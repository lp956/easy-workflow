// This file owns PostgreSQL instance-projection normalization, keyset execution, decoding, and page construction.
// It serves Initiated only; it does not define public data contracts or task-query cursor and relation rules.
// Queries borrow the caller-owned concurrent pool, retain no input, and return detached values.
// Source text is UTF-8 and contains no platform-specific encoding or generated regions.
package postgres

import (
	"context"
	"fmt"
	"strconv"
	"time"

	workflow "github.com/lvpeng/easy-workflow"
)

// initiatedSQL selects running and terminal executions by trusted initiator with an instance-only keyset.
// Parameters are actor, scope, cursor presence, cursor time, instance, and limit in that order.
const initiatedSQL = `
	SELECT definition_id, definition_version::text, instance_id, instance_status,
	       initiator, current_node_id, started_at, last_audit_at, order_at
	FROM easy_workflow_instance_projection
	WHERE initiator = $1
	  AND ($2::text[] IS NULL OR instance_id = ANY($2::text[]))
	  AND (
	      NOT $3::boolean OR order_at < $4 OR
	      (order_at = $4 AND instance_id > $5)
	  )
	ORDER BY order_at DESC, instance_id ASC
	LIMIT $6`

// instanceProjectionKeyset is the complete stable ordering position owned by Initiated.
type instanceProjectionKeyset struct {
	// at is the PostgreSQL-normalized primary descending order timestamp.
	at time.Time
	// instanceID is the ascending identity tie-breaker after at.
	instanceID workflow.InstanceID
}

// instanceProjectionQuery is one fully prepared Initiated request ready for parameterized SQL execution.
type instanceProjectionQuery struct {
	projectionQueryBoundary
	// after is nil for the first page or a validated complete instance keyset for exclusive continuation.
	after *instanceProjectionKeyset
}

// instanceProjectionResult is the private detached Initiated page before public cursor or token adaptation.
type instanceProjectionResult struct {
	// items contains caller-visible rows in stable instance-family order.
	items []InstanceProjection
	// next is nil on the final page or the exact keyset of the last visible row.
	next *instanceProjectionKeyset
}

// Initiated returns running and terminal instances started by one trusted actor without moving history tables.
//
// ctx must be non-nil; cancellation is checked before pool acquisition and remains discoverable through the error.
// query.ActorID must be non-empty, Scope is the exact host-authorized instance set, and Page uses last audit time
// descending then InstanceID ascending. The detached result accepts only instance cursors without TaskID. All values
// are PostgreSQL parameters; errors preserve ErrInvalidProjectionQuery classification and context or database causes.
//
// Deprecated: use InitiatedPage with ContinuationQuery. This method remains a compatibility adapter for Cursor through
// the current major version and delegates to the same instance query-family implementation.
func (p *Projection) Initiated(ctx context.Context, query ActorQuery) (Page[InstanceProjection], error) {
	prepared, err := prepareLegacyInstanceProjectionQuery(p, ctx, query)
	if err != nil {
		return Page[InstanceProjection]{}, err
	}
	result, err := p.queryInstanceProjection(ctx, prepared)
	if err != nil {
		return Page[InstanceProjection]{}, err
	}
	page := Page[InstanceProjection]{Items: result.items}
	if result.next != nil {
		page.Next = &Cursor{At: result.next.at, InstanceID: result.next.instanceID}
	}
	return page, nil
}

// InitiatedPage returns running and terminal instances started by one actor with an opaque instance continuation.
//
// ctx must remain active through execution. query contains a trusted actor, exact host scope, zero or [1, 200] limit,
// and an optional unchanged token returned by InitiatedPage. Results are detached and ordered by audit time descending,
// then InstanceID ascending. Task-family or malformed tokens return ErrInvalidProjectionQuery before pool acquisition;
// every SQL value remains parameterized.
func (p *Projection) InitiatedPage(
	ctx context.Context,
	query ContinuationQuery,
) (ContinuationPage[InstanceProjection], error) {
	prepared, err := prepareContinuedInstanceProjectionQuery(p, ctx, query)
	if err != nil {
		return ContinuationPage[InstanceProjection]{}, err
	}
	result, err := p.queryInstanceProjection(ctx, prepared)
	if err != nil {
		return ContinuationPage[InstanceProjection]{}, err
	}
	page := ContinuationPage[InstanceProjection]{Items: result.items}
	if result.next != nil {
		page.Next, err = encodeInstanceContinuation(*result.next)
		if err != nil {
			return ContinuationPage[InstanceProjection]{}, err
		}
	}
	return page, nil
}

// queryInstanceProjection executes Initiated's parameterized keyset, decoding, and look-ahead page contract.
//
// query must be prepared by one instance-family boundary and owns actor, scope, limit, and optional keyset. The method
// returns at most limit detached rows plus a private next keyset. It interprets no public cursor or token; errors preserve
// context, database, scanning, and version-parsing causes.
func (p *Projection) queryInstanceProjection(
	ctx context.Context,
	query instanceProjectionQuery,
) (instanceProjectionResult, error) {
	// A non-nil empty scope is authoritative deny-all input and therefore needs no database snapshot.
	if query.scope != nil && len(query.scope) == 0 {
		return instanceProjectionResult{items: []InstanceProjection{}}, nil
	}
	after := query.after
	hasAfter := after != nil
	if after == nil {
		// SQL ignores placeholder cursor fields while hasAfter is false, preserving a fixed parameter list.
		after = &instanceProjectionKeyset{}
	}
	fetchLimit := query.limit + 1 // The extra row determines Next within the same database snapshot.

	// Use one parameterized look-ahead query so page membership and continuation come from one snapshot.
	rows, err := p.pool.Query(
		ctx,
		initiatedSQL,
		query.actorID,
		query.scope,
		hasAfter,
		after.at,
		after.instanceID,
		fetchLimit,
	)
	if err != nil {
		// Query failure preserves its database or cancellation cause and yields no partial page.
		return instanceProjectionResult{}, fmt.Errorf("postgres: query initiated instances: %w", err)
	}
	// The driver rows lifetime ends with this call even when scanning or decoding fails early.
	defer rows.Close()

	// Decode instance rows while retaining the database-normalized order timestamp used only by continuation cursors.
	items := make([]InstanceProjection, 0, fetchLimit)
	orderTimes := make([]time.Time, 0, fetchLimit)
	for rows.Next() {
		var item InstanceProjection
		var definitionVersion string
		var orderAt time.Time
		if err := rows.Scan(
			&item.DefinitionID,
			&definitionVersion,
			&item.InstanceID,
			&item.InstanceStatus,
			&item.Initiator,
			&item.CurrentNodeID,
			&item.StartedAt,
			&item.LastAuditAt,
			&orderAt,
		); err != nil {
			// One undecodable row invalidates the page because pagination cannot safely advance past it.
			return instanceProjectionResult{}, fmt.Errorf("postgres: scan initiated instance: %w", err)
		}
		version, err := strconv.ParseUint(definitionVersion, projectionVersionBase, projectionVersionBits)
		if err != nil {
			// Frozen definition versions must round-trip without truncation before they become public data.
			return instanceProjectionResult{}, fmt.Errorf("postgres: parse initiated definition version %q: %w", definitionVersion, err)
		}
		item.DefinitionVersion = version
		items = append(items, item)
		orderTimes = append(orderTimes, orderAt)
	}
	if err := rows.Err(); err != nil {
		// Late driver failures discard accumulated rows rather than presenting an incomplete successful page.
		return instanceProjectionResult{}, fmt.Errorf("postgres: iterate initiated instances: %w", err)
	}

	// Trim the look-ahead row and retain the private keyset of the last caller-visible instance.
	result := instanceProjectionResult{items: items}
	// More than query.limit rows means the final decoded row is look-ahead rather than caller-visible data.
	if len(items) > query.limit {
		lastIndex := query.limit - 1 // The visible page ends immediately before the look-ahead row.
		last := items[lastIndex]
		result.items = items[:query.limit]
		result.next = &instanceProjectionKeyset{at: orderTimes[lastIndex], instanceID: last.InstanceID}
	}
	return result, nil
}

// prepareLegacyInstanceProjectionQuery validates and converts one deprecated public instance cursor request.
//
// Shared adapter, cancellation, actor, scope, and limit semantics are prepared first. After, when present, must supply a
// complete instance keyset and no TaskID. The returned request owns scope and keyset values; errors precede pool access.
func prepareLegacyInstanceProjectionQuery(
	p *Projection,
	ctx context.Context,
	query ActorQuery,
) (instanceProjectionQuery, error) {
	boundary, err := prepareProjectionQuery(p, ctx, query.ActorID, query.Scope.InstanceIDs, query.Page.Limit)
	if err != nil {
		return instanceProjectionQuery{}, err
	}
	prepared := instanceProjectionQuery{projectionQueryBoundary: boundary}
	if query.Page.After != nil {
		if query.Page.After.TaskID != "" {
			return instanceProjectionQuery{}, fmt.Errorf(
				"%w: instance cursor belongs to a task query",
				ErrInvalidProjectionQuery,
			)
		}
		keyset := instanceProjectionKeyset{at: query.Page.After.At, instanceID: query.Page.After.InstanceID}
		if err := validateInstanceProjectionKeyset(keyset); err != nil {
			return instanceProjectionQuery{}, err
		}
		prepared.after = &keyset
	}
	return prepared, nil
}

// prepareContinuedInstanceProjectionQuery validates and decodes one opaque Initiated continuation request.
//
// Shared adapter, cancellation, actor, scope, and limit semantics are prepared first. A non-empty token must decode to
// the current instance-family schema and complete keyset. The returned request owns all values and errors before pool access.
func prepareContinuedInstanceProjectionQuery(
	p *Projection,
	ctx context.Context,
	query ContinuationQuery,
) (instanceProjectionQuery, error) {
	boundary, err := prepareProjectionQuery(p, ctx, query.ActorID, query.Scope.InstanceIDs, query.Page.Limit)
	if err != nil {
		return instanceProjectionQuery{}, err
	}
	prepared := instanceProjectionQuery{projectionQueryBoundary: boundary}
	if query.Page.After != "" {
		keyset, err := decodeInstanceContinuation(query.Page.After)
		if err != nil {
			return instanceProjectionQuery{}, err
		}
		prepared.after = &keyset
	}
	return prepared, nil
}

// validateInstanceProjectionKeyset enforces Initiated's complete two-component ordering position.
//
// keyset requires a non-zero time and non-empty InstanceID. Errors wrap ErrInvalidProjectionQuery and the function
// performs no allocation or I/O.
func validateInstanceProjectionKeyset(keyset instanceProjectionKeyset) error {
	// Both ordering components are required because either omission destabilizes equal-time pagination.
	if keyset.at.IsZero() || keyset.instanceID == "" {
		return fmt.Errorf("%w: instance continuation keyset is incomplete", ErrInvalidProjectionQuery)
	}
	return nil
}

// encodeInstanceContinuation validates and encodes one Initiated keyset without exposing its components.
//
// keyset must identify the final visible instance row. The token belongs only to InitiatedPage, carries no actor or
// authorization scope, and must be combined with fresh trusted query input on the next call.
func encodeInstanceContinuation(keyset instanceProjectionKeyset) (Continuation, error) {
	if err := validateInstanceProjectionKeyset(keyset); err != nil {
		return "", err
	}
	return encodeContinuationEnvelope(continuationEnvelope{
		Version:    continuationEncodingVersion,
		Family:     instanceContinuationFamily,
		At:         keyset.at.UTC(),
		InstanceID: keyset.instanceID,
	})
}

// decodeInstanceContinuation decodes and validates one opaque token as a complete Initiated keyset.
//
// token must use the current encoding version and instance family with no TaskID. Task-family or structurally incomplete
// values return ErrInvalidProjectionQuery. The function retains no token data and performs no database I/O.
func decodeInstanceContinuation(token Continuation) (instanceProjectionKeyset, error) {
	envelope, err := decodeContinuationEnvelope(token)
	if err != nil {
		return instanceProjectionKeyset{}, err
	}
	// Version, family, and absent TaskID jointly prevent a task position from entering the instance query family.
	if envelope.Version != continuationEncodingVersion || envelope.Family != instanceContinuationFamily || envelope.TaskID != "" {
		return instanceProjectionKeyset{}, fmt.Errorf(
			"%w: continuation does not belong to instance queries",
			ErrInvalidProjectionQuery,
		)
	}
	keyset := instanceProjectionKeyset{at: envelope.At, instanceID: envelope.InstanceID}
	if err := validateInstanceProjectionKeyset(keyset); err != nil {
		return instanceProjectionKeyset{}, err
	}
	return keyset, nil
}
