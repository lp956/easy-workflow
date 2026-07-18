// This file owns PostgreSQL task-projection normalization, keyset execution, decoding, and page construction.
// It serves Worklist and Participated only; it does not define public data contracts or instance-query cursor rules.
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

// worklistSQL selects concrete active assignments and applies host scope plus a deterministic task keyset.
// Parameters are actor, scope, cursor presence, cursor time, instance, task, and limit in that order.
const worklistSQL = `
	SELECT i.definition_id, i.definition_version::text, i.instance_id, i.instance_status,
	       i.initiator, p.node_id, p.task_id, p.actor_id, p.task_status, p.outcome,
	       i.started_at, i.last_audit_at, i.order_at
	FROM easy_workflow_participation_projection AS p
	JOIN easy_workflow_instance_projection AS i ON i.instance_id = p.instance_id
	WHERE p.actor_id = $1
	  AND p.task_status = 'active'
	  AND i.instance_status = 'running'
	  AND ($2::text[] IS NULL OR i.instance_id = ANY($2::text[]))
	  AND (
	      NOT $3::boolean OR i.order_at < $4 OR
	      (i.order_at = $4 AND i.instance_id > $5) OR
	      (i.order_at = $4 AND i.instance_id = $5 AND p.task_id > $6)
	  )
	ORDER BY i.order_at DESC, i.instance_id ASC, p.task_id ASC
	LIMIT $7`

// participatedSQL selects retained completed and closed assignments with the same task cursor ordering.
// It shares worklistSQL's seven-parameter contract so both task views use identical pagination orchestration.
const participatedSQL = `
	SELECT i.definition_id, i.definition_version::text, i.instance_id, i.instance_status,
	       i.initiator, p.node_id, p.task_id, p.actor_id, p.task_status, p.outcome,
	       i.started_at, i.last_audit_at, i.order_at
	FROM easy_workflow_participation_projection AS p
	JOIN easy_workflow_instance_projection AS i ON i.instance_id = p.instance_id
	WHERE p.actor_id = $1
	  AND p.task_status IN ('completed', 'closed')
	  AND ($2::text[] IS NULL OR i.instance_id = ANY($2::text[]))
	  AND (
	      NOT $3::boolean OR i.order_at < $4 OR
	      (i.order_at = $4 AND i.instance_id > $5) OR
	      (i.order_at = $4 AND i.instance_id = $5 AND p.task_id > $6)
	  )
	ORDER BY i.order_at DESC, i.instance_id ASC, p.task_id ASC
	LIMIT $7`

// taskProjectionKeyset is the complete stable ordering position shared by Worklist and Participated.
type taskProjectionKeyset struct {
	// at is the PostgreSQL-normalized primary descending order timestamp.
	at time.Time
	// instanceID is the ascending identity tie-breaker after at.
	instanceID workflow.InstanceID
	// taskID is the final ascending assignment tie-breaker.
	taskID workflow.TaskID
}

// taskProjectionQuery is one fully prepared task-family request ready for parameterized SQL execution.
type taskProjectionQuery struct {
	projectionQueryBoundary
	// after is nil for the first page or a validated complete task keyset for exclusive continuation.
	after *taskProjectionKeyset
}

// taskProjectionResult is the private detached task-family page before public cursor or token adaptation.
type taskProjectionResult struct {
	// items contains the caller-visible rows in stable task-family order.
	items []TaskProjection
	// next is nil on the final page or the exact keyset of the last visible row.
	next *taskProjectionKeyset
}

// Worklist returns active tasks frozen for one actor in running workflow instances.
//
// ctx must be non-nil; cancellation is checked before pool acquisition and remains discoverable through the error.
// query.ActorID must be non-empty and must originate from trusted host identity. Scope is applied exactly as supplied,
// Limit is zero or in [1, 200], and After must be a task cursor. The detached result is ordered by last audit time
// descending then InstanceID and TaskID ascending. All filters are parameters; errors preserve validation and causes.
//
// Deprecated: use WorklistPage with ContinuationQuery. This method remains a compatibility adapter for Cursor through
// the current major version and delegates to the same task query-family implementation.
func (p *Projection) Worklist(ctx context.Context, query ActorQuery) (Page[TaskProjection], error) {
	return p.queryLegacyTaskProjection(ctx, query, worklistSQL, "worklist", RelationCandidate)
}

// WorklistPage returns active frozen tasks with an opaque task-family continuation.
//
// ctx must remain active through query execution. query contains a trusted non-empty actor, exact host scope, zero or
// [1, 200] limit, and an optional unchanged token from WorklistPage or ParticipatedPage. Results are detached and ordered
// by audit time descending, then InstanceID and TaskID ascending. All SQL values are parameters; malformed or instance
// tokens return ErrInvalidProjectionQuery before pool acquisition.
func (p *Projection) WorklistPage(
	ctx context.Context,
	query ContinuationQuery,
) (ContinuationPage[TaskProjection], error) {
	return p.queryContinuedTaskProjection(ctx, query, worklistSQL, "worklist", RelationCandidate)
}

// Participated returns completed or closed frozen assignments for one actor, including retained terminal instances.
//
// ctx must be non-nil; cancellation is checked before pool acquisition and remains discoverable through the error.
// query follows Worklist's trusted actor, host scope, page limit, cursor, ordering, and parameterization contract. The
// detached result retains both completed decisions and tasks closed by sibling or lifecycle transitions. The method
// performs no history migration; errors preserve validation classification and context or database causes.
//
// Deprecated: use ParticipatedPage with ContinuationQuery. This compatibility adapter preserves Cursor behavior and
// delegates to the same task query-family implementation through the current major version.
func (p *Projection) Participated(ctx context.Context, query ActorQuery) (Page[TaskProjection], error) {
	return p.queryLegacyTaskProjection(ctx, query, participatedSQL, "participation", RelationParticipant)
}

// ParticipatedPage returns completed or closed frozen assignments with an opaque task-family continuation.
//
// query follows WorklistPage's actor, scope, limit, token, ordering, and parameterization contract. Tokens are compatible
// across both task views because they encode the same complete keyset. The result owns detached rows; errors retain
// validation, cancellation, parsing, and database causes.
func (p *Projection) ParticipatedPage(
	ctx context.Context,
	query ContinuationQuery,
) (ContinuationPage[TaskProjection], error) {
	return p.queryContinuedTaskProjection(ctx, query, participatedSQL, "participation", RelationParticipant)
}

// queryLegacyTaskProjection adapts one deprecated Cursor request to the deep task query-family result.
//
// statement and operation are package constants and relation is a package-owned classification. The method preserves
// every existing validation, scope, ordering, and nil-Next behavior while performing no duplicate SQL interpretation.
func (p *Projection) queryLegacyTaskProjection(
	ctx context.Context,
	query ActorQuery,
	statement string,
	operation string,
	relation Relation,
) (Page[TaskProjection], error) {
	prepared, err := prepareLegacyTaskProjectionQuery(p, ctx, query)
	if err != nil {
		return Page[TaskProjection]{}, err
	}
	result, err := p.queryTaskProjection(ctx, prepared, statement, operation, relation)
	if err != nil {
		return Page[TaskProjection]{}, err
	}
	page := Page[TaskProjection]{Items: result.items}
	if result.next != nil {
		page.Next = &Cursor{At: result.next.at, InstanceID: result.next.instanceID, TaskID: result.next.taskID}
	}
	return page, nil
}

// queryContinuedTaskProjection decodes one opaque request and encodes the deep task query-family result.
//
// statement and operation are package constants and relation is package-owned. Token decoding completes before pool
// acquisition; encoding uses the last visible row from the same look-ahead query. No raw keyset leaves this method.
func (p *Projection) queryContinuedTaskProjection(
	ctx context.Context,
	query ContinuationQuery,
	statement string,
	operation string,
	relation Relation,
) (ContinuationPage[TaskProjection], error) {
	prepared, err := prepareContinuedTaskProjectionQuery(p, ctx, query)
	if err != nil {
		return ContinuationPage[TaskProjection]{}, err
	}
	result, err := p.queryTaskProjection(ctx, prepared, statement, operation, relation)
	if err != nil {
		return ContinuationPage[TaskProjection]{}, err
	}
	page := ContinuationPage[TaskProjection]{Items: result.items}
	if result.next != nil {
		page.Next, err = encodeTaskContinuation(*result.next)
		if err != nil {
			return ContinuationPage[TaskProjection]{}, err
		}
	}
	return page, nil
}

// queryTaskProjection executes the shared parameterized keyset contract for candidate and participant views.
//
// statement and operation are package-owned constants, never caller input; relation is attached only after a
// committed task row is scanned. query must be prepared by one task-family boundary and owns actor, scope, limit, and
// optional keyset. The method fetches one look-ahead row and returns at most limit items plus a private next keyset.
// Errors preserve context, parsing, and database causes; no public cursor or token is interpreted here.
func (p *Projection) queryTaskProjection(
	ctx context.Context,
	query taskProjectionQuery,
	statement string,
	operation string,
	relation Relation,
) (taskProjectionResult, error) {
	// A non-nil empty scope is the host's deny-all decision, so no database snapshot can add a visible row.
	if query.scope != nil && len(query.scope) == 0 {
		return taskProjectionResult{items: []TaskProjection{}}, nil
	}
	after := query.after
	hasAfter := after != nil
	if after == nil {
		// Placeholder values are ignored by SQL while hasAfter is false and keep one fixed parameter shape.
		after = &taskProjectionKeyset{}
	}
	fetchLimit := query.limit + 1 // The extra row determines Next within the same database snapshot.

	// Fetch one extra row in the same parameterized query so continuation never requires a racing existence query.
	rows, err := p.pool.Query(
		ctx,
		statement,
		query.actorID,
		query.scope,
		hasAfter,
		after.at,
		after.instanceID,
		after.taskID,
		fetchLimit,
	)
	if err != nil {
		// Query failure exposes its driver or context cause without returning a partial page.
		return taskProjectionResult{}, fmt.Errorf("postgres: query %s: %w", operation, err)
	}
	// The driver rows lifetime ends with this call even when scanning or decoding fails early.
	defer rows.Close()

	// Decode rows into public types while retaining the database-normalized order time for cursor construction.
	items := make([]TaskProjection, 0, fetchLimit)
	orderTimes := make([]time.Time, 0, fetchLimit)
	for rows.Next() {
		var item TaskProjection
		var definitionVersion string
		var orderAt time.Time
		if err := rows.Scan(
			&item.DefinitionID,
			&definitionVersion,
			&item.InstanceID,
			&item.InstanceStatus,
			&item.Initiator,
			&item.NodeID,
			&item.TaskID,
			&item.ActorID,
			&item.TaskStatus,
			&item.Outcome,
			&item.StartedAt,
			&item.LastAuditAt,
			&orderAt,
		); err != nil {
			// A malformed durable row invalidates the whole page because its stable position cannot be skipped safely.
			return taskProjectionResult{}, fmt.Errorf("postgres: scan %s row: %w", operation, err)
		}
		version, err := strconv.ParseUint(definitionVersion, projectionVersionBase, projectionVersionBits)
		if err != nil {
			// Lossless version parsing is required before exposing frozen definition identity.
			return taskProjectionResult{}, fmt.Errorf(
				"postgres: parse %s definition version %q: %w",
				operation,
				definitionVersion,
				err,
			)
		}
		item.DefinitionVersion = version
		item.Relation = relation
		items = append(items, item)
		orderTimes = append(orderTimes, orderAt)
	}
	if err := rows.Err(); err != nil {
		// Late iteration errors discard accumulated rows rather than returning an incomplete page as successful.
		return taskProjectionResult{}, fmt.Errorf("postgres: iterate %s: %w", operation, err)
	}

	// Exclude the look-ahead row and retain the exact private keyset of the final caller-visible row.
	result := taskProjectionResult{items: items}
	// More than query.limit rows means the final decoded row is look-ahead rather than caller-visible data.
	if len(items) > query.limit {
		lastIndex := query.limit - 1 // The visible page ends immediately before the look-ahead row.
		last := items[lastIndex]
		result.items = items[:query.limit]
		result.next = &taskProjectionKeyset{
			at:         orderTimes[lastIndex],
			instanceID: last.InstanceID,
			taskID:     last.TaskID,
		}
	}
	return result, nil
}

// prepareLegacyTaskProjectionQuery validates and converts one deprecated public task cursor request.
//
// Shared adapter, cancellation, actor, scope, and limit semantics are prepared first. After, when present, must supply the
// complete task keyset. The returned request owns scope and keyset values; errors occur before database acquisition.
func prepareLegacyTaskProjectionQuery(
	p *Projection,
	ctx context.Context,
	query ActorQuery,
) (taskProjectionQuery, error) {
	boundary, err := prepareProjectionQuery(p, ctx, query.ActorID, query.Scope.InstanceIDs, query.Page.Limit)
	if err != nil {
		return taskProjectionQuery{}, err
	}
	prepared := taskProjectionQuery{projectionQueryBoundary: boundary}
	if query.Page.After != nil {
		keyset := taskProjectionKeyset{
			at:         query.Page.After.At,
			instanceID: query.Page.After.InstanceID,
			taskID:     query.Page.After.TaskID,
		}
		if err := validateTaskProjectionKeyset(keyset); err != nil {
			return taskProjectionQuery{}, err
		}
		prepared.after = &keyset
	}
	return prepared, nil
}

// prepareContinuedTaskProjectionQuery validates and decodes one opaque task-family continuation request.
//
// Shared adapter, cancellation, actor, scope, and limit semantics are prepared first. A non-empty token must decode to
// the current task-family schema and complete keyset. The returned request owns all values and errors before pool access.
func prepareContinuedTaskProjectionQuery(
	p *Projection,
	ctx context.Context,
	query ContinuationQuery,
) (taskProjectionQuery, error) {
	boundary, err := prepareProjectionQuery(p, ctx, query.ActorID, query.Scope.InstanceIDs, query.Page.Limit)
	if err != nil {
		return taskProjectionQuery{}, err
	}
	prepared := taskProjectionQuery{projectionQueryBoundary: boundary}
	if query.Page.After != "" {
		keyset, err := decodeTaskContinuation(query.Page.After)
		if err != nil {
			return taskProjectionQuery{}, err
		}
		prepared.after = &keyset
	}
	return prepared, nil
}

// validateTaskProjectionKeyset enforces the complete three-component ordering position used by both task views.
//
// keyset requires a non-zero time plus non-empty InstanceID and TaskID. Errors wrap ErrInvalidProjectionQuery and the
// function performs no allocation or I/O.
func validateTaskProjectionKeyset(keyset taskProjectionKeyset) error {
	// All three ordering components are required because omitting any one can skip or duplicate equal-time rows.
	if keyset.at.IsZero() || keyset.instanceID == "" || keyset.taskID == "" {
		return fmt.Errorf("%w: task continuation keyset is incomplete", ErrInvalidProjectionQuery)
	}
	return nil
}

// encodeTaskContinuation validates and encodes one task-family keyset without exposing its components.
//
// keyset must identify the final visible task row. The token is compatible with WorklistPage and ParticipatedPage,
// carries no actor or authorization scope, and must be combined with fresh trusted query input on the next call.
func encodeTaskContinuation(keyset taskProjectionKeyset) (Continuation, error) {
	if err := validateTaskProjectionKeyset(keyset); err != nil {
		return "", err
	}
	return encodeContinuationEnvelope(continuationEnvelope{
		Version:    continuationEncodingVersion,
		Family:     taskContinuationFamily,
		At:         keyset.at.UTC(),
		InstanceID: keyset.instanceID,
		TaskID:     keyset.taskID,
	})
}

// decodeTaskContinuation decodes and validates one opaque token as a complete task-family keyset.
//
// token must use the current encoding version and shared task family. Instance-family or structurally incomplete values
// return ErrInvalidProjectionQuery. The function retains no token data and performs no database I/O.
func decodeTaskContinuation(token Continuation) (taskProjectionKeyset, error) {
	envelope, err := decodeContinuationEnvelope(token)
	if err != nil {
		return taskProjectionKeyset{}, err
	}
	// Version and family must match together so no other wire schema can be reinterpreted as a task position.
	if envelope.Version != continuationEncodingVersion || envelope.Family != taskContinuationFamily {
		return taskProjectionKeyset{}, fmt.Errorf("%w: continuation does not belong to task queries", ErrInvalidProjectionQuery)
	}
	keyset := taskProjectionKeyset{at: envelope.At, instanceID: envelope.InstanceID, taskID: envelope.TaskID}
	if err := validateTaskProjectionKeyset(keyset); err != nil {
		return taskProjectionKeyset{}, err
	}
	return keyset, nil
}
