// This file exposes PostgreSQL-specific, read-only workflow projections with stable keyset pagination.
// It does not execute commands, discover tenants, authorize actors, own the pool, or extend workflow.Store.
package postgres

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	workflow "github.com/lvpeng/easy-workflow"
)

const (
	// defaultPageLimit bounds an omitted page size while keeping ordinary inbox reads useful.
	defaultPageLimit = 50
	// maximumPageLimit prevents one query from returning an unbounded projection result.
	maximumPageLimit = 200
)

var (
	// ErrInvalidProjectionQuery classifies missing dependencies, identities, limits, scopes, and cursors.
	ErrInvalidProjectionQuery = errors.New("postgres: invalid projection query")
)

// Projection provides read-only PostgreSQL queries over transactionally maintained workflow facts.
//
// Projection borrows a caller-owned pool, performs no I/O at construction, and is safe for concurrent use under
// pgxpool's concurrency contract. Host applications must authenticate actors and calculate tenant/business scope
// before each call; Projection applies but does not discover or authorize that scope.
type Projection struct {
	// pool supplies read-only query execution and remains owned by the host application.
	pool *pgxpool.Pool
}

// QueryScope restricts a projection query to host-authorized workflow instances.
//
// A nil InstanceIDs slice means no additional instance constraint. A non-nil empty slice means the caller is
// authorized for no instances and therefore receives an empty page. Values are always sent as PostgreSQL parameters.
type QueryScope struct {
	// InstanceIDs is the complete host-authorized instance set for this query, or nil when no scope is required.
	InstanceIDs []workflow.InstanceID
}

// PageRequest selects a bounded page after an optional keyset cursor.
type PageRequest struct {
	// Limit is the requested item count in [1, 200]; zero selects the documented default of 50.
	Limit int
	// After resumes strictly after a cursor returned by the same query family and ordering.
	After *Cursor
}

// Cursor identifies one stable projection row in descending audit-time order.
//
// Callers must treat fields as opaque continuation data and return them unchanged. TaskID is required for task
// projections and may continue either task view; instance projections leave it empty. Structurally invalid cursors
// or task cursors supplied to an instance query return ErrInvalidProjectionQuery.
type Cursor struct {
	// At is the row's normalized last-audit timestamp used as the primary descending sort key.
	At time.Time
	// InstanceID is the deterministic ascending tie-breaker after At.
	InstanceID workflow.InstanceID
	// TaskID is the final ascending tie-breaker for task projection rows.
	TaskID workflow.TaskID
}

// ActorQuery selects one trusted actor, host-authorized instance scope, and stable page.
type ActorQuery struct {
	// ActorID is the authenticated business principal whose assignments or initiations are requested.
	ActorID workflow.ActorID
	// Scope contains the host's tenant or business authorization result; Projection never broadens it.
	Scope QueryScope
	// Page bounds the result and optionally resumes a prior query.
	Page PageRequest
}

// Page contains one result slice and an optional cursor for the following page.
//
// Items is non-nil on success. Next is nil exactly when no later row was observed in the same database query.
type Page[T any] struct {
	// Items contains at most the normalized requested limit in stable query order.
	Items []T
	// Next resumes after the final returned item when another row exists.
	Next *Cursor
}

// Relation classifies how one committed assignment fact appears in a host-facing query view.
type Relation string

const (
	// RelationCandidate means a concrete frozen task currently accepts a command from its actor.
	RelationCandidate Relation = "candidate"
	// RelationParticipant means a concrete frozen task reached completed or closed status.
	RelationParticipant Relation = "participant"
)

// TaskProjection joins frozen definition identity, instance state, assignment state, and key audit timestamps.
//
// Values are detached read-model data owned by the caller. Relation comes only from committed Task status; no
// directory lookup or mutable core Task extension participates in reconstruction.
type TaskProjection struct {
	// DefinitionID is the stable identifier frozen into the workflow instance at start.
	DefinitionID string
	// DefinitionVersion is the exact frozen version and preserves the full uint64 range.
	DefinitionVersion uint64
	// InstanceID identifies the workflow execution that owns this assignment.
	InstanceID workflow.InstanceID
	// InstanceStatus is the lifecycle state observed in the same committed projection snapshot.
	InstanceStatus workflow.InstanceStatus
	// Initiator is the trusted actor recorded when the instance started.
	Initiator workflow.ActorID
	// NodeID identifies the frozen definition node that created the task.
	NodeID string
	// TaskID uniquely identifies this assignment within its instance.
	TaskID workflow.TaskID
	// ActorID is the concrete frozen assignee; empty directory candidates never produce rows.
	ActorID workflow.ActorID
	// TaskStatus is active for worklist rows and completed or closed for participation rows.
	TaskStatus workflow.TaskStatus
	// Outcome is handler-owned decision text and remains empty until a task records one.
	Outcome string
	// Relation classifies an active task as candidate and a completed or closed task as participant.
	Relation Relation
	// StartedAt is the first non-zero instance.started audit time, or nil when unavailable.
	StartedAt *time.Time
	// LastAuditAt is the final non-zero audit timestamp at projection commit, or nil when unavailable.
	LastAuditAt *time.Time
}

// InstanceProjection joins frozen definition identity, lifecycle state, initiator, current node, and audit times.
//
// Values are detached caller-owned data read from the same transactionally maintained projection as task views.
type InstanceProjection struct {
	// DefinitionID is the stable identifier frozen into the workflow instance at start.
	DefinitionID string
	// DefinitionVersion is the exact frozen version and preserves the full uint64 range.
	DefinitionVersion uint64
	// InstanceID uniquely identifies the projected workflow execution.
	InstanceID workflow.InstanceID
	// InstanceStatus includes running and every retained terminal lifecycle state.
	InstanceStatus workflow.InstanceStatus
	// Initiator is the trusted actor used by Initiated filtering.
	Initiator workflow.ActorID
	// CurrentNodeID is the aggregate's committed current node, including a terminal node after completion.
	CurrentNodeID string
	// StartedAt is the first non-zero instance.started audit time, or nil when unavailable.
	StartedAt *time.Time
	// LastAuditAt is the final non-zero audit timestamp at projection commit, or nil when unavailable.
	LastAuditAt *time.Time
}

// NewProjection constructs a read-only PostgreSQL projection adapter without connecting or applying migrations.
//
// pool remains caller-owned and may be nil; query methods return ErrInvalidProjectionQuery when it is absent.
func NewProjection(pool *pgxpool.Pool) *Projection {
	return &Projection{pool: pool}
}

// worklistSQL selects concrete active assignments and applies host scope plus a deterministic task keyset.
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

// initiatedSQL selects running and terminal executions by trusted initiator with an instance-only keyset.
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

// Worklist returns active tasks frozen for one actor in running workflow instances.
//
// query.ActorID must be non-empty and must originate from trusted host identity. Scope is applied exactly as
// supplied, Limit is zero or in [1, 200], and After must be a task cursor. Results are ordered by last audit time
// descending then InstanceID and TaskID ascending. All filters are parameters; errors preserve context/database causes.
func (p *Projection) Worklist(ctx context.Context, query ActorQuery) (Page[TaskProjection], error) {
	return p.queryTaskProjection(ctx, query, worklistSQL, "worklist", RelationCandidate)
}

// Participated returns completed or closed frozen assignments for one actor, including retained terminal instances.
//
// query follows Worklist's trusted actor, host scope, page limit, cursor, ordering, and parameterization contract.
// Both completed decisions and tasks closed by sibling or lifecycle transitions remain in place; the method performs
// no history migration. Errors preserve context cancellation, validation classification, and database causes.
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
	// Normalize the boundary and cursor before acquiring a pool connection.
	limit, err := validateActorTaskQuery(p, ctx, query)
	if err != nil {
		// Boundary failures must not acquire a connection or reinterpret host authorization scope.
		return Page[TaskProjection]{}, err
	}
	scope := projectionScope(query.Scope.InstanceIDs)
	after := query.Page.After
	hasAfter := after != nil
	if after == nil {
		// Placeholder values are ignored by SQL while hasAfter is false and keep one fixed parameter shape.
		after = &Cursor{}
	}

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
		limit+1,
	)
	if err != nil {
		// Query failure exposes its driver or context cause without returning a partial page.
		return Page[TaskProjection]{}, fmt.Errorf("postgres: query %s: %w", operation, err)
	}
	defer rows.Close()

	// Decode rows into public types while retaining the database-normalized order time for cursor construction.
	items := make([]TaskProjection, 0, limit+1)
	orderTimes := make([]time.Time, 0, limit+1)
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
		version, err := strconv.ParseUint(definitionVersion, 10, 64)
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
	if len(items) > limit {
		last := items[limit-1]
		page.Items = items[:limit]
		page.Next = &Cursor{At: orderTimes[limit-1], InstanceID: last.InstanceID, TaskID: last.TaskID}
	}
	return page, nil
}

// Initiated returns running and terminal instances started by one trusted actor without moving history tables.
//
// query.ActorID must be non-empty, Scope is the exact host-authorized instance set, and Page uses last audit time
// descending then InstanceID ascending. Instance cursors require no TaskID. All values are PostgreSQL parameters;
// errors preserve context cancellation, ErrInvalidProjectionQuery classification, and database causes.
func (p *Projection) Initiated(ctx context.Context, query ActorQuery) (Page[InstanceProjection], error) {
	// Validate the instance-query cursor separately because TaskID must remain empty for this query family.
	limit, err := validateActorInstanceQuery(p, ctx, query)
	if err != nil {
		// Invalid identity, scope-independent pagination, or cancellation stops before pool acquisition.
		return Page[InstanceProjection]{}, err
	}
	scope := projectionScope(query.Scope.InstanceIDs)
	after := query.Page.After
	hasAfter := after != nil
	if after == nil {
		// SQL ignores placeholder cursor fields while hasAfter is false, preserving a fixed parameter list.
		after = &Cursor{}
	}

	// Use one parameterized look-ahead query so page membership and continuation come from one snapshot.
	rows, err := p.pool.Query(
		ctx,
		initiatedSQL,
		query.ActorID,
		scope,
		hasAfter,
		after.At,
		after.InstanceID,
		limit+1,
	)
	if err != nil {
		// Query failure preserves its database or cancellation cause and yields no partial page.
		return Page[InstanceProjection]{}, fmt.Errorf("postgres: query initiated instances: %w", err)
	}
	defer rows.Close()

	// Decode instance rows while retaining the database-normalized order timestamp used only by continuation cursors.
	items := make([]InstanceProjection, 0, limit+1)
	orderTimes := make([]time.Time, 0, limit+1)
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
		version, err := strconv.ParseUint(definitionVersion, 10, 64)
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
	if len(items) > limit {
		last := items[limit-1]
		page.Items = items[:limit]
		page.Next = &Cursor{At: orderTimes[limit-1], InstanceID: last.InstanceID}
	}
	return page, nil
}

// validateActorTaskQuery normalizes one task page and rejects unusable identities, dependencies, or cursors.
//
// ctx is checked before pool use. A zero limit becomes 50; valid explicit limits are [1, 200]. The returned limit
// is safe for limit+1 look-ahead. Errors wrap context cancellation or ErrInvalidProjectionQuery without database I/O.
func validateActorTaskQuery(p *Projection, ctx context.Context, query ActorQuery) (int, error) {
	if err := ctx.Err(); err != nil {
		// Cancellation takes precedence because the caller can no longer consume validation or query results.
		return 0, fmt.Errorf("postgres: projection query: %w", err)
	}
	// Query execution requires a concrete adapter, borrowed pool, and trusted actor identity together.
	if p == nil || p.pool == nil || query.ActorID == "" {
		return 0, fmt.Errorf("%w: projection, pool, or actor is empty", ErrInvalidProjectionQuery)
	}
	limit := query.Page.Limit
	if limit == 0 {
		// Zero is the documented request sentinel, not an empty-page request.
		limit = defaultPageLimit
	}
	// Explicit limits outside the bounded public range are rejected instead of silently clamped.
	if limit < 1 || limit > maximumPageLimit {
		return 0, fmt.Errorf("%w: page limit %d is outside [1, %d]", ErrInvalidProjectionQuery, limit, maximumPageLimit)
	}
	// A task keyset is usable only when every ordering component is present.
	if query.Page.After != nil && (query.Page.After.At.IsZero() || query.Page.After.InstanceID == "" || query.Page.After.TaskID == "") {
		return 0, fmt.Errorf("%w: task cursor is incomplete", ErrInvalidProjectionQuery)
	}
	return limit, nil
}

// validateActorInstanceQuery normalizes an initiated-instance page and rejects task or incomplete cursors.
//
// ctx is checked before pool use. A zero limit becomes 50; explicit limits are [1, 200]. A valid continuation has
// non-zero At, non-empty InstanceID, and empty TaskID. Errors require no database acquisition.
func validateActorInstanceQuery(p *Projection, ctx context.Context, query ActorQuery) (int, error) {
	if err := ctx.Err(); err != nil {
		// Cancellation takes precedence because no subsequent query result can be consumed.
		return 0, fmt.Errorf("postgres: projection query: %w", err)
	}
	// Instance queries share the same dependency and trusted-identity boundary as task queries.
	if p == nil || p.pool == nil || query.ActorID == "" {
		return 0, fmt.Errorf("%w: projection, pool, or actor is empty", ErrInvalidProjectionQuery)
	}
	limit := query.Page.Limit
	if limit == 0 {
		// Zero requests the documented default rather than an intentionally empty page.
		limit = defaultPageLimit
	}
	// Rejecting out-of-range limits keeps memory and database work predictably bounded.
	if limit < 1 || limit > maximumPageLimit {
		return 0, fmt.Errorf("%w: page limit %d is outside [1, %d]", ErrInvalidProjectionQuery, limit, maximumPageLimit)
	}
	// Instance pagination needs its two ordering keys and rejects the extra task key from another query family.
	if query.Page.After != nil && (query.Page.After.At.IsZero() || query.Page.After.InstanceID == "" || query.Page.After.TaskID != "") {
		return 0, fmt.Errorf("%w: instance cursor is incomplete or belongs to a task query", ErrInvalidProjectionQuery)
	}
	return limit, nil
}

// projectionScope converts host-owned workflow identities into detached PostgreSQL text-array parameters.
//
// nil remains nil to mean no scope constraint, while a non-nil empty slice remains empty to deny every instance.
// The conversion performs no I/O and prevents driver encoding from depending on named Go string types.
func projectionScope(ids []workflow.InstanceID) []string {
	if ids == nil {
		// Preserve nil so SQL can distinguish unrestricted scope from an explicitly empty authorized set.
		return nil
	}
	values := make([]string, len(ids))
	for index := range ids {
		values[index] = string(ids[index])
	}
	return values
}
