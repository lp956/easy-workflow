// This file defines persisted runtime state and commands shared by the engine and node extensions.
// Runtime values are data-only snapshots; handlers cannot use them to access storage or force graph jumps.
package workflow

import (
	"context"
	"encoding/json"
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
// The engine owns ID, NodeID, and status persistence. A handler may propose task states in NodeResult,
// but it cannot write them directly. Outcome is handler-defined data such as "approved" or "rejected".
type Task struct {
	ID       TaskID     `json:"id"`
	NodeID   string     `json:"nodeId"`
	Assignee ActorID    `json:"assignee"`
	Status   TaskStatus `json:"status"`
	Outcome  string     `json:"outcome,omitempty"`
}

// AuditRecord is an immutable description of one accepted state transition.
//
// Action is a stable machine-readable name. ActorID is empty for engine-driven transitions. At is UTC;
// sequence order is the slice order within Instance and remains authoritative if timestamps are equal.
type AuditRecord struct {
	Action  string    `json:"action"`
	NodeID  string    `json:"nodeId,omitempty"`
	TaskID  TaskID    `json:"taskId,omitempty"`
	ActorID ActorID   `json:"actorId,omitempty"`
	At      time.Time `json:"at"`
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

// Command asks the handler for the current node to process one task action.
//
// Name is a stable handler-defined command such as "approve". Payload is optional JSON owned by that
// handler. The engine verifies instance and task ownership before committing the returned NodeResult.
type Command struct {
	InstanceID InstanceID      `json:"instanceId"`
	TaskID     TaskID          `json:"taskId"`
	ActorID    ActorID         `json:"actorId"`
	Name       string          `json:"name"`
	Payload    json.RawMessage `json:"payload,omitempty"`
}
