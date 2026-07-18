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

// Worklist returns active tasks frozen for one actor in running workflow instances.
//
// ctx must be non-nil; cancellation is checked before pool acquisition and remains discoverable through the error.
// query.ActorID must be non-empty and must originate from trusted host identity. Scope is applied exactly as supplied,
// Limit is zero or in [1, 200], and After must be a task cursor. The detached result is ordered by last audit time
// descending then InstanceID and TaskID ascending. All filters are parameters; errors preserve validation and causes.
func (p *Projection) Worklist(ctx context.Context, query ActorQuery) (Page[TaskProjection], error) {
	return p.queryTaskProjection(ctx, query, worklistSQL, "worklist", RelationCandidate)
}

// Participated returns completed or closed frozen assignments for one actor, including retained terminal instances.
//
// ctx must be non-nil; cancellation is checked before pool acquisition and remains discoverable through the error.
// query follows Worklist's trusted actor, host scope, page limit, cursor, ordering, and parameterization contract. The
// detached result retains both completed decisions and tasks closed by sibling or lifecycle transitions. The method
// performs no history migration; errors preserve validation classification and context or database causes.
func (p *Projection) Participated(ctx context.Context, query ActorQuery) (Page[TaskProjection], error) {
	return p.queryTaskProjection(ctx, query, participatedSQL, "participation", RelationParticipant)
}

// queryTaskProjection executes the shared parameterized keyset contract for candidate and participant views.
//
// statement and operation are package-owned constants, never caller input; relation is attached only after a
// committed task row is scanned. The method validates actor, scope, limit, and task cursor, fetches one look-ahead
// row, and returns at most Limit items. Errors preserve validation, context, parsing, and database causes.
func (p *Projection) queryTaskProjection(
	ctx context.Context,
	query ActorQuery,
	statement string,
	operation string,
	relation Relation,
) (Page[TaskProjection], error) {
	// Normalize the complete task-family boundary before acquiring a pool connection.
	limit, err := validateActorTaskQuery(p, ctx, query)
	if err != nil {
		// Boundary failures must not acquire a connection or reinterpret host authorization scope.
		return Page[TaskProjection]{}, err
	}
	scope := projectionScope(query.Scope.InstanceIDs)
	// A non-nil empty scope is the host's deny-all decision, so no database snapshot can add a visible row.
	if scope != nil && len(scope) == 0 {
		return Page[TaskProjection]{Items: []TaskProjection{}}, nil
	}
	after := query.Page.After
	hasAfter := after != nil
	if after == nil {
		// Placeholder values are ignored by SQL while hasAfter is false and keep one fixed parameter shape.
		after = &Cursor{}
	}
	fetchLimit := limit + 1 // The extra row determines Next within the same database snapshot.

	// Fetch one extra row in the same parameterized query so continuation never requires a racing existence query.
	rows, err := p.pool.Query(
		ctx,
		statement,
		query.ActorID,
		scope,
		hasAfter,
		after.At,
		after.InstanceID,
		after.TaskID,
		fetchLimit,
	)
	if err != nil {
		// Query failure exposes its driver or context cause without returning a partial page.
		return Page[TaskProjection]{}, fmt.Errorf("postgres: query %s: %w", operation, err)
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
			return Page[TaskProjection]{}, fmt.Errorf("postgres: scan %s row: %w", operation, err)
		}
		version, err := strconv.ParseUint(definitionVersion, projectionVersionBase, projectionVersionBits)
		if err != nil {
			// Lossless version parsing is required before exposing frozen definition identity.
			return Page[TaskProjection]{}, fmt.Errorf(
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
		return Page[TaskProjection]{}, fmt.Errorf("postgres: iterate %s: %w", operation, err)
	}

	// Exclude the look-ahead row and resume strictly after the last row visible to the caller.
	page := Page[TaskProjection]{Items: items}
	// More than limit rows means the final decoded row is look-ahead rather than caller-visible data.
	if len(items) > limit {
		lastIndex := limit - 1 // The visible page ends immediately before the look-ahead row.
		last := items[lastIndex]
		page.Items = items[:limit]
		page.Next = &Cursor{At: orderTimes[lastIndex], InstanceID: last.InstanceID, TaskID: last.TaskID}
	}
	return page, nil
}

// validateActorTaskQuery normalizes one task page and rejects incomplete task cursors.
//
// ctx, adapter ownership, actor identity, and limit use the shared Projection boundary. A valid task continuation
// supplies non-zero At plus non-empty InstanceID and TaskID. Errors occur before database acquisition.
func validateActorTaskQuery(p *Projection, ctx context.Context, query ActorQuery) (int, error) {
	limit, err := normalizeProjectionQuery(p, ctx, query)
	if err != nil {
		// Shared boundary failures retain their cancellation or invalid-query classification.
		return 0, err
	}
	// A task keyset is usable only when every ordering component is present.
	if query.Page.After != nil && (query.Page.After.At.IsZero() || query.Page.After.InstanceID == "" || query.Page.After.TaskID == "") {
		return 0, fmt.Errorf("%w: task cursor is incomplete", ErrInvalidProjectionQuery)
	}
	return limit, nil
}
