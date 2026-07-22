// This file owns package-internal mutations of workflow instance, task, and audit facts.
// It does not authorize commands, invoke handlers, navigate definitions, or perform Store I/O; each value is request-local.
package workflow

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"slices"
	"time"
)

const (
	// auditInstanceStarted is the stable action persisted when Engine accepts a new instance.
	auditInstanceStarted = "instance.started"
	// auditNodeEntered is the stable action persisted before a business or terminal node becomes current.
	auditNodeEntered = "node.entered"
	// auditNodeRejected is the stable action persisted when rejection follows a declared outcome edge.
	auditNodeRejected = "node.rejected"
	// auditInstanceCompleted is the stable action persisted when execution reaches an end node.
	auditInstanceCompleted = "instance.completed"
	// auditInstanceRejected is the stable action persisted for a terminal rejection without an outcome edge.
	auditInstanceRejected = "instance.rejected"
	// auditInstanceWithdrawn is the stable action persisted for an authorized withdrawal.
	auditInstanceWithdrawn = "instance.withdrawn"
	// auditInstanceReturned is the stable action persisted before entering an explicit historical target.
	auditInstanceReturned = "instance.returned"
	// auditTaskTransferred is the stable action persisted before replacing one active assignment.
	auditTaskTransferred = "task.transferred"
	// reservedTaskCommandTransferred is the handler command suffix reserved by auditTaskTransferred.
	reservedTaskCommandTransferred = "transferred"
	// auditTaskActionPrefix namespaces accepted handler commands while preserving the handler-owned command name.
	auditTaskActionPrefix = "task."
)

// instanceFacts is the sole package-internal mutation boundary for one caller-owned Instance candidate.
//
// The wrapper neither clones nor caches the aggregate: Engine gives it one detached snapshot for one command attempt.
// It is not safe for concurrent use, but separate wrappers share no mutable state and may run concurrently.
type instanceFacts struct {
	// instance is the detached candidate discarded in full when preparation, transition, or persistence fails.
	instance *Instance
}

// startInstanceFacts creates the initial running snapshot and records its attributed start fact.
//
// definition must be a compiler-owned snapshot, request must have passed Engine boundary validation, and startNodeID
// must identify the compiled control entry. The returned wrapper owns a new aggregate with version one; it performs no
// Store I/O and shares no mutable Definition, business data, task, or audit slices with its inputs.
func startInstanceFacts(definition Definition, request StartRequest, startNodeID string) *instanceFacts {
	// Freeze caller-owned runtime data while constructing the only candidate that Start may persist.
	facts := newInstanceFacts(&Instance{
		ID:            request.ID,
		Definition:    cloneDefinition(definition),
		Status:        InstanceStatusRunning,
		Initiator:     request.Initiator,
		CurrentNodeID: startNodeID,
		Data:          slices.Clone(request.Data),
		Version:       1, // A created aggregate starts at the first CAS-visible version.
	})

	// Starting attribution precedes the first routed node entry in the authoritative audit order.
	facts.appendAudit(AuditRecord{
		Action:  auditInstanceStarted,
		NodeID:  startNodeID,
		ActorID: request.Initiator,
	})
	return facts
}

// newInstanceFacts wraps one detached candidate without copying or mutating it.
func newInstanceFacts(instance *Instance) *instanceFacts {
	return &instanceFacts{instance: instance}
}

// candidate returns the request-local aggregate that Engine may pass to Store after every fact operation succeeds.
func (f *instanceFacts) candidate() *Instance {
	return f.instance
}

// appendAudit stamps one accepted fact in UTC and appends it after the immutable durable prefix.
//
// record contains transition attribution selected by the owning fact method. The method mutates only the request-local
// candidate and performs no I/O; slice order remains authoritative when clock resolution produces equal timestamps.
func (f *instanceFacts) appendAudit(record AuditRecord) {
	record.At = time.Now().UTC()
	f.instance.Audit = append(f.instance.Audit, record)
}

// recordTaskCommand appends the accepted handler command before applying its returned NodeResult.
//
// name is the validated handler-owned action name; nodeID, taskID, and actorID identify the exact accepted decision.
// The method stamps one audit suffix record and performs no I/O. Callers must invoke it only after handler success.
func (f *instanceFacts) recordTaskCommand(name, nodeID string, taskID TaskID, actorID ActorID) {
	f.appendAudit(AuditRecord{
		Action:  auditTaskActionPrefix + name,
		NodeID:  nodeID,
		TaskID:  taskID,
		ActorID: actorID,
	})
}

// recordWithdrawal appends actor attribution against the source node before terminal withdrawal changes.
//
// actorID is the already authorized host principal. The method captures the current source node, stamps one audit
// suffix record, and performs no I/O; withdraw must follow before the candidate is eligible for persistence.
func (f *instanceFacts) recordWithdrawal(actorID ActorID) {
	f.appendAudit(AuditRecord{
		Action:  auditInstanceWithdrawn,
		NodeID:  f.instance.CurrentNodeID,
		ActorID: actorID,
	})
}

// recordReturn captures source node state and the explicit destination before return replaces either value.
//
// targetNodeID is the validated historical target, actorID is the authorized principal, and reason is preserved
// verbatim. The method stamps one audit suffix record and performs no I/O; returnTo must follow on the same candidate.
func (f *instanceFacts) recordReturn(targetNodeID string, actorID ActorID, reason string) {
	f.appendAudit(AuditRecord{
		Action:       auditInstanceReturned,
		NodeID:       f.instance.CurrentNodeID,
		TargetNodeID: targetNodeID,
		ActorID:      actorID,
		Reason:       reason,
		NodeState:    string(f.instance.NodeState),
	})
}

// recordTransfer appends the complete assignment change attribution before the source task is closed.
//
// current is the authorized active source task, actorID identifies the operator, newAssignee is the concrete successor
// owner, and reason is preserved verbatim. The method stamps one audit suffix record and performs no I/O.
func (f *instanceFacts) recordTransfer(current Task, actorID, newAssignee ActorID, reason string) {
	f.appendAudit(AuditRecord{
		Action:           auditTaskTransferred,
		InstanceID:       f.instance.ID,
		NodeID:           current.NodeID,
		TaskID:           current.ID,
		ActorID:          actorID,
		PreviousAssignee: current.Assignee,
		NewAssignee:      newAssignee,
		Reason:           reason,
	})
}

// enterNode makes nodeID current, clears prior node state, and records entry in execution order.
//
// nodeID must come from the compiled Definition. The method appends one UTC audit fact after changing only the private
// candidate; callers set handler state and tasks afterward, then persist the complete candidate or discard it.
func (f *instanceFacts) enterNode(nodeID string) {
	f.instance.CurrentNodeID = nodeID
	f.instance.NodeState = nil
	f.appendAudit(AuditRecord{Action: auditNodeEntered, NodeID: nodeID})
}

// setNodeState stores a detached copy of the opaque state returned by the active node handler.
//
// state may be nil or valid handler-owned JSON already accepted at the NodeResult seam. The method retains no
// caller-owned bytes and performs no I/O.
func (f *instanceFacts) setNodeState(state []byte) {
	f.instance.NodeState = slices.Clone(state)
}

// complete marks the current end node successful and records the terminal fact.
//
// The current node must be the compiled end node selected by Engine. The method updates status and appends one UTC
// audit suffix record; both remain private until the enclosing Create or Save succeeds.
func (f *instanceFacts) complete() {
	f.instance.Status = InstanceStatusCompleted
	f.appendAudit(AuditRecord{Action: auditInstanceCompleted, NodeID: f.instance.CurrentNodeID})
}

// rejectNode records a non-terminal rejection whose declared outcome continues graph navigation.
//
// Engine must have validated that the handler supplied a non-empty declared outcome. The method appends one UTC audit
// suffix record without changing status; the caller enters the resolved target next or discards the candidate.
func (f *instanceFacts) rejectNode() {
	f.appendAudit(AuditRecord{Action: auditNodeRejected, NodeID: f.instance.CurrentNodeID})
}

// rejectInstance marks the current node's outcome as a terminal rejection and records it.
//
// Engine calls this only for an empty rejection outcome. The method updates status and appends one UTC audit suffix
// record; both changes remain private until the enclosing Create or Save succeeds.
func (f *instanceFacts) rejectInstance() {
	f.instance.Status = InstanceStatusRejected
	f.appendAudit(AuditRecord{Action: auditInstanceRejected, NodeID: f.instance.CurrentNodeID})
}

// withdraw closes every active assignment and marks the candidate withdrawn without rewriting historical decisions.
//
// recordWithdrawal must already have captured source attribution. The method changes only running status and active
// tasks; completed and previously closed tasks retain every field, and no Store I/O occurs.
func (f *instanceFacts) withdraw() {
	// The terminal status and task closures form one candidate committed by the caller's single Store CAS.
	f.instance.Status = InstanceStatusWithdrawn
	for index := range f.instance.Tasks {
		if f.instance.Tasks[index].Status == TaskStatusActive {
			f.instance.Tasks[index].Status = TaskStatusClosed
		}
	}
}

// returnTo closes source-active work and appends one validated fresh task round at an authorized historical target.
//
// targetNodeID and result must come from prepareReturnNodeResult against this candidate's defensive snapshot. Historical
// tasks retain order and values. Identity-generation errors cause the caller to discard the complete candidate.
func (f *instanceFacts) returnTo(targetNodeID string, result NodeResult) error {
	// Close only the source node's active round, preserving completed decisions and every earlier node's history.
	sourceNodeID := f.instance.CurrentNodeID
	for index := range f.instance.Tasks {
		if f.instance.Tasks[index].NodeID == sourceNodeID && f.instance.Tasks[index].Status == TaskStatusActive {
			f.instance.Tasks[index].Status = TaskStatusClosed
		}
	}

	// Target entry follows the return audit, then the new state and task round become visible together.
	f.enterNode(targetNodeID)
	f.setNodeState(result.State)
	return f.activateTasks(targetNodeID, result.Tasks)
}

// transfer closes one exact active assignment and appends its new active successor with a fresh identity.
//
// current must be the policy-authorized task resolved from the same pre-transition snapshot. newAssignee must be a
// concrete host identity. A missing source returns ErrTaskNotTransferable; identity generation errors retain their
// cause. Existing task order and fields remain unchanged except for the source status.
func (f *instanceFacts) transfer(current Task, newAssignee ActorID) error {
	// Allocate the successor before changing the candidate so generator failure leaves even the private task view intact.
	id, err := newTaskID()
	if err != nil {
		return err
	}
	replacement := Task{
		ID:       id,
		NodeID:   current.NodeID,
		Assignee: newAssignee,
		Status:   TaskStatusActive,
	}

	// Preserve the source as closed history and append the replacement as the next assignment fact.
	for index := range f.instance.Tasks {
		if f.instance.Tasks[index].ID == current.ID {
			f.instance.Tasks[index].Status = TaskStatusClosed
			f.instance.Tasks = append(f.instance.Tasks, replacement)
			return nil // Aggregate-unique task IDs make the first match authoritative.
		}
	}
	return fmt.Errorf("%w: task %q disappeared before transition", ErrTaskNotTransferable, current.ID)
}

// applyNodeTaskReplacements writes one prevalidated command task view without changing other-node task records.
//
// replacements must come from prepareNodeTaskReplacements for this exact candidate. The method performs only factual
// writes, cannot fail, preserves aggregate order, and performs no validation, allocation, or I/O.
func (f *instanceFacts) applyNodeTaskReplacements(replacements []nodeTaskReplacement) {
	for _, replacement := range replacements {
		f.instance.Tasks[replacement.index] = replacement.task
	}
}

// activateTasks binds one node and fresh cryptographic identities to prevalidated active task drafts.
//
// tasks must have passed validateActivationTaskDrafts. Engine overwrites draft NodeID for backward compatibility with
// existing handlers. Generated IDs are aggregate identities returned to callers and persisted by Store; entropy errors
// cause the enclosing candidate to be discarded.
func (f *instanceFacts) activateTasks(nodeID string, tasks []Task) error {
	// Activation appends a new round after all historical tasks rather than replacing prior node-owned records.
	for _, task := range tasks {
		id, err := newTaskID()
		if err != nil {
			return err
		}
		task.ID = id
		task.NodeID = nodeID
		f.instance.Tasks = append(f.instance.Tasks, task)
	}
	return nil
}

// nodeTasks returns a defensive copy of every historical or current task owned by nodeID.
//
// nodeID may own multiple rounds after a return, all of which remain in aggregate order. The returned slice shares no
// backing storage with tasks and can be passed to a handler without exposing the candidate's task collection.
func nodeTasks(tasks []Task, nodeID string) []Task {
	result := make([]Task, 0, len(tasks))
	for _, task := range tasks {
		if task.NodeID == nodeID {
			result = append(result, task)
		}
	}
	return result
}

// advanceVersion increments the candidate's CAS token exactly once after every transition fact succeeds.
//
// executeInstanceCommand owns call timing and invokes this immediately before its single Save attempt. The method does
// not persist or retry; a failed CAS discards the increment together with the rest of the private candidate. A maximum
// uint64 token is rejected rather than wrapping to zero and reusing an older CAS value.
func (f *instanceFacts) advanceVersion() error {
	if f == nil || f.instance == nil {
		return fmt.Errorf("%w: instance facts are incomplete", ErrInvalidStoreInput)
	}
	if f.instance.Version == ^uint64(0) {
		return ErrVersionOverflow
	}
	f.instance.Version++
	return nil
}

// newTaskID generates a process-independent task identifier using 128 random bits.
//
// The identifier is suitable for aggregate-wide task identity without shared mutable generators. Entropy failures
// retain their cause and produce no identifier. The function is safe for concurrent calls through crypto/rand.
func newTaskID() (TaskID, error) {
	const randomBytes = 16 // Sixteen bytes provide the documented 128-bit collision-resistant identity.
	buffer := make([]byte, randomBytes)
	if _, err := rand.Read(buffer); err != nil {
		return "", fmt.Errorf("workflow: generate task id: %w", err)
	}
	return TaskID("task_" + hex.EncodeToString(buffer)), nil
}
