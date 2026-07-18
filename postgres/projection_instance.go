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

// Initiated returns running and terminal instances started by one trusted actor without moving history tables.
//
// ctx must be non-nil; cancellation is checked before pool acquisition and remains discoverable through the error.
// query.ActorID must be non-empty, Scope is the exact host-authorized instance set, and Page uses last audit time
// descending then InstanceID ascending. The detached result accepts only instance cursors without TaskID. All values
// are PostgreSQL parameters; errors preserve ErrInvalidProjectionQuery classification and context or database causes.
func (p *Projection) Initiated(ctx context.Context, query ActorQuery) (Page[InstanceProjection], error) {
	// Normalize the complete instance-family boundary before acquiring a pool connection.
	limit, err := validateActorInstanceQuery(p, ctx, query)
	if err != nil {
		// Invalid identity, scope-independent pagination, or cancellation stops before pool acquisition.
		return Page[InstanceProjection]{}, err
	}
	scope := projectionScope(query.Scope.InstanceIDs)
	// A non-nil empty scope is authoritative deny-all input and therefore needs no database snapshot.
	if scope != nil && len(scope) == 0 {
		return Page[InstanceProjection]{Items: []InstanceProjection{}}, nil
	}
	after := query.Page.After
	hasAfter := after != nil
	if after == nil {
		// SQL ignores placeholder cursor fields while hasAfter is false, preserving a fixed parameter list.
		after = &Cursor{}
	}
	fetchLimit := limit + 1 // The extra row determines Next within the same database snapshot.

	// Use one parameterized look-ahead query so page membership and continuation come from one snapshot.
	rows, err := p.pool.Query(
		ctx,
		initiatedSQL,
		query.ActorID,
		scope,
		hasAfter,
		after.At,
		after.InstanceID,
		fetchLimit,
	)
	if err != nil {
		// Query failure preserves its database or cancellation cause and yields no partial page.
		return Page[InstanceProjection]{}, fmt.Errorf("postgres: query initiated instances: %w", err)
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
			return Page[InstanceProjection]{}, fmt.Errorf("postgres: scan initiated instance: %w", err)
		}
		version, err := strconv.ParseUint(definitionVersion, projectionVersionBase, projectionVersionBits)
		if err != nil {
			// Frozen definition versions must round-trip without truncation before they become public data.
			return Page[InstanceProjection]{}, fmt.Errorf("postgres: parse initiated definition version %q: %w", definitionVersion, err)
		}
		item.DefinitionVersion = version
		items = append(items, item)
		orderTimes = append(orderTimes, orderAt)
	}
	if err := rows.Err(); err != nil {
		// Late driver failures discard accumulated rows rather than presenting an incomplete successful page.
		return Page[InstanceProjection]{}, fmt.Errorf("postgres: iterate initiated instances: %w", err)
	}

	// Trim the look-ahead row and expose an exclusive cursor for the last item the caller received.
	page := Page[InstanceProjection]{Items: items}
	// More than limit rows means the final decoded row is look-ahead rather than caller-visible data.
	if len(items) > limit {
		lastIndex := limit - 1 // The visible page ends immediately before the look-ahead row.
		last := items[lastIndex]
		page.Items = items[:limit]
		page.Next = &Cursor{At: orderTimes[lastIndex], InstanceID: last.InstanceID}
	}
	return page, nil
}

// validateActorInstanceQuery normalizes an initiated-instance page and rejects task or incomplete cursors.
//
// ctx, adapter ownership, actor identity, and limit use the shared Projection boundary. A valid continuation has
// non-zero At, non-empty InstanceID, and empty TaskID. Errors occur before database acquisition.
func validateActorInstanceQuery(p *Projection, ctx context.Context, query ActorQuery) (int, error) {
	limit, err := normalizeProjectionQuery(p, ctx, query)
	if err != nil {
		// Shared boundary failures retain their cancellation or invalid-query classification.
		return 0, err
	}
	// Instance pagination needs its two ordering keys and rejects the extra task key from another query family.
	if query.Page.After != nil && (query.Page.After.At.IsZero() || query.Page.After.InstanceID == "" || query.Page.After.TaskID != "") {
		return 0, fmt.Errorf("%w: instance cursor is incomplete or belongs to a task query", ErrInvalidProjectionQuery)
	}
	return limit, nil
}
