// This file defines persisted runtime state and commands shared by the engine and node extensions.
// Runtime values are data-only snapshots; handlers cannot use them to access storage or force graph jumps.
package workflow

import (
	"context"
	"encoding/json"
	"slices"
	"time"
)

// ActorID is the business system's stable identifier for a person or service principal.
type ActorID string

// InstanceID uniquely identifies one execution of a workflow definition.
type InstanceID string

// TaskID uniquely identifies one actionable assignment within an instance.
type TaskID string

// InstanceStatus describes the lifecycle of a workflow instance.
type InstanceStatus string

const (
	// InstanceStatusUnknown is the zero value and is never persisted by a valid engine operation.
	InstanceStatusUnknown InstanceStatus = ""
	// InstanceStatusRunning means the instance currently waits at or advances through an active node.
	InstanceStatusRunning InstanceStatus = "running"
	// InstanceStatusCompleted means the instance reached a successful end node.
	InstanceStatusCompleted InstanceStatus = "completed"
	// InstanceStatusRejected means a node rejected the instance as a terminal decision.
	InstanceStatusRejected InstanceStatus = "rejected"
	// InstanceStatusWithdrawn means a host-authorized actor ended the instance while it was running.
	InstanceStatusWithdrawn InstanceStatus = "withdrawn"
)

// TaskStatus describes whether an assignment can still accept a command.
type TaskStatus string

const (
	// TaskStatusUnknown is the zero value and is invalid for persisted tasks.
	TaskStatusUnknown TaskStatus = ""
	// TaskStatusActive means the assignee may act on the task.
	TaskStatusActive TaskStatus = "active"
	// TaskStatusCompleted means the assignee supplied the decision recorded in Outcome.
	TaskStatusCompleted TaskStatus = "completed"
	// TaskStatusClosed means the node ended before this assignee needed to decide.
	TaskStatusClosed TaskStatus = "closed"
)

// Task is one assignment produced by the active node handler.
//
// The engine owns ID, NodeID, Assignee, and status persistence. During command handling, a handler may leave an active
// task active, complete it with a non-empty Outcome, or close it without inventing an Outcome. Completed and closed tasks
// are immutable history; reassignment requires Engine.Transfer. Outcome is handler-defined data such as "approved" or
// "rejected".
type Task struct {
	ID       TaskID     `json:"id"`
	NodeID   string     `json:"nodeId"`
	Assignee ActorID    `json:"assignee"`
	Status   TaskStatus `json:"status"`
	Outcome  string     `json:"outcome,omitempty"`
}

// AuditRecord is an immutable description of one accepted state transition.
//
// Action is a stable machine-readable name. ActorID is empty for engine-driven transitions. NodeState optionally
// preserves opaque source-node state on lifecycle history records. At is UTC; sequence order is the slice order
// within Instance and remains authoritative if timestamps are equal.
type AuditRecord struct {
	Action string `json:"action"`
	// InstanceID identifies the aggregate explicitly when an audit fact may be consumed outside its owning snapshot.
	InstanceID InstanceID `json:"instanceId,omitempty"`
	NodeID     string     `json:"nodeId,omitempty"`
	// TargetNodeID identifies the explicit destination of a lifecycle transition when one exists.
	TargetNodeID string  `json:"targetNodeId,omitempty"`
	TaskID       TaskID  `json:"taskId,omitempty"`
	ActorID      ActorID `json:"actorId,omitempty"`
	// PreviousAssignee identifies the assignment owner before a task transfer.
	PreviousAssignee ActorID `json:"previousAssignee,omitempty"`
	// NewAssignee identifies the assignment owner selected by a task transfer.
	NewAssignee ActorID `json:"newAssignee,omitempty"`
	// Reason preserves the host-provided lifecycle justification verbatim.
	Reason string `json:"reason,omitempty"`
	// NodeState preserves opaque source-node bytes as a string before a lifecycle transition replaces current state.
	NodeState string    `json:"nodeState,omitempty"`
	At        time.Time `json:"at"`
}

// Instance is a durable snapshot of workflow execution, active assignments, and audit history.
//
// Definition is frozen at start so later definition changes cannot alter a running instance. Version is
// an optimistic concurrency token incremented exactly once per accepted transition. Data and NodeState are
// opaque JSON owned by the business module and current node handler respectively.
type Instance struct {
	ID            InstanceID      `json:"id"`
	Definition    Definition      `json:"definition"`
	Status        InstanceStatus  `json:"status"`
	Initiator     ActorID         `json:"initiator"`
	CurrentNodeID string          `json:"currentNodeId"`
	Data          json.RawMessage `json:"data,omitempty"`
	NodeState     json.RawMessage `json:"nodeState,omitempty"`
	Tasks         []Task          `json:"tasks"`
	Audit         []AuditRecord   `json:"audit"`
	Version       uint64          `json:"version"`
}

// StartRequest contains caller-owned identity and business data for a new instance.
//
// ID and Initiator must be non-empty. Data must be nil or valid JSON; the core preserves it without
// interpreting its schema.
type StartRequest struct {
	ID        InstanceID      `json:"id"`
	Initiator ActorID         `json:"initiator"`
	Data      json.RawMessage `json:"data,omitempty"`
}

// WithdrawRequest identifies one running instance and its trusted lifecycle actor.
//
// ActorID must come from host-established identity rather than an untrusted request body. The request carries
// no authorization decision; Engine always delegates that decision to an explicit WithdrawalPolicy.
type WithdrawRequest struct {
	// InstanceID identifies the aggregate to withdraw and must be non-empty.
	InstanceID InstanceID `json:"instanceId"`
	// ActorID identifies the authenticated host principal requesting withdrawal and must be non-empty.
	ActorID ActorID `json:"actorId"`
}

// WithdrawalPolicy authorizes a trusted actor against the current durable instance snapshot.
//
// Engine supplies a defensive copy before changing status, tasks, audit, or version. Implementations may use
// host-owned identity, tenant, or business rules and must return a non-nil error to deny withdrawal. They must
// honor context cancellation for blocking work and must be safe for the host's Engine concurrency model.
type WithdrawalPolicy interface {
	// AuthorizeWithdrawal returns nil only when actor may withdraw the supplied pre-transition snapshot.
	AuthorizeWithdrawal(ctx context.Context, actor ActorID, instance *Instance) error
}

// ReturnRequest identifies one running instance, trusted actor, explicit historical target, and audit reason.
//
// TargetNodeID is never inferred from Definition order. ActorID must come from host-established identity, while
// Reason is persisted verbatim for audit. Engine validates graph membership and execution history before policy.
type ReturnRequest struct {
	// InstanceID identifies the aggregate to return and must be non-empty.
	InstanceID InstanceID `json:"instanceId"`
	// ActorID identifies the authenticated host principal requesting return and must be non-empty.
	ActorID ActorID `json:"actorId"`
	// TargetNodeID identifies one previously entered non-control node and must be non-empty.
	TargetNodeID string `json:"targetNodeId"`
	// Reason explains the return for audit and must contain at least one non-whitespace character.
	Reason string `json:"reason"`
}

// ReturnPolicy authorizes an explicit return after Engine validates its graph and history constraints.
//
// Engine supplies a defensive pre-transition snapshot and the validated request. Implementations may apply
// host-owned identity, tenant, and source-target rules; a non-nil error denies return. Implementations must honor
// context cancellation for blocking work and be safe for the host's Engine concurrency model.
type ReturnPolicy interface {
	// AuthorizeReturn returns nil only when the actor may perform this exact source-target return.
	AuthorizeReturn(ctx context.Context, request ReturnRequest, instance *Instance) error
}

// TransferRequest identifies one active assignment, trusted operator, replacement assignee, and audit reason.
//
// ActorID must come from host-established identity rather than an untrusted request body. NewAssignee is a concrete
// host identity, while Reason is persisted verbatim after Engine rejects blank input. The request carries no
// authorization decision; Engine delegates operator and target eligibility to TransferPolicy.
type TransferRequest struct {
	// InstanceID identifies the running aggregate that owns the assignment and must be non-empty.
	InstanceID InstanceID `json:"instanceId"`
	// TaskID identifies the current active assignment and must be non-empty.
	TaskID TaskID `json:"taskId"`
	// ActorID identifies the authenticated host principal requesting transfer and must be non-empty.
	ActorID ActorID `json:"actorId"`
	// NewAssignee identifies the concrete replacement owner and must be non-empty.
	NewAssignee ActorID `json:"newAssignee"`
	// Reason explains the transfer for audit and must contain at least one non-whitespace character.
	Reason string `json:"reason"`
}

// TransferPolicy authorizes one operator and replacement owner against the current active assignment.
//
// Engine supplies the validated request, a value copy of the current task, and a defensive pre-transition aggregate.
// Implementations own host identity, tenant, delegation, and assignee-validity rules; a non-nil error denies transfer.
// They must honor context cancellation for blocking work and be safe for the host's Engine concurrency model.
type TransferPolicy interface {
	// AuthorizeTransfer returns nil only when the operator may transfer the task to the requested assignee.
	AuthorizeTransfer(ctx context.Context, request TransferRequest, task Task, instance *Instance) error
}

// Command asks the handler for the current node to process one task action.
//
// Name is a stable handler-defined command such as "approve". Payload is optional JSON delivered to the handler as a
// defensive copy. The engine verifies instance and task ownership before committing the returned NodeResult.
type Command struct {
	InstanceID InstanceID      `json:"instanceId"`
	TaskID     TaskID          `json:"taskId"`
	ActorID    ActorID         `json:"actorId"`
	Name       string          `json:"name"`
	Payload    json.RawMessage `json:"payload,omitempty"`
}

// cloneCommand detaches the optional JSON payload before a command crosses a handler boundary.
func cloneCommand(source Command) Command {
	cloned := source
	cloned.Payload = slices.Clone(source.Payload)
	return cloned
}
