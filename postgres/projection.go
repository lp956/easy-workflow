// This file exposes PostgreSQL-specific, read-only workflow projection contracts and adapter construction.
// It does not execute commands, discover tenants, authorize actors, own the pool, or define query-family internals.
package postgres

import (
	"errors"
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
