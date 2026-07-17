// This file implements graph navigation, optimistic command handling, and immutable audit recording.
// Business decisions stay in handlers; Engine alone applies accepted results to durable instance state.
package workflow

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"time"
)

var (
	// ErrInvalidEngine means an operation cannot run because required Engine collaborators are absent.
	ErrInvalidEngine = errors.New("workflow: invalid engine")
	// ErrInvalidStartRequest means instance identity, initiator, business data, or published identity is invalid.
	ErrInvalidStartRequest = errors.New("workflow: invalid start request")
	// ErrInvalidCommand means a command violates the active instance, task, or handler contract.
	ErrInvalidCommand = errors.New("workflow: invalid command")
	// ErrInvalidWithdrawRequest means withdrawal lacks identity or an explicit host policy.
	ErrInvalidWithdrawRequest = errors.New("workflow: invalid withdraw request")
	// ErrInstanceNotRunning means a lifecycle operation targeted an already terminal instance.
	ErrInstanceNotRunning = errors.New("workflow: instance is not running")
	// ErrInvalidNodeResult means a handler returned state the engine cannot safely persist.
	ErrInvalidNodeResult = errors.New("workflow: invalid node result")
)

// Engine coordinates validated definitions, node handlers, storage, and audit records.
//
// Engine is safe for concurrent use when its Store and handlers honor their concurrency contracts. It
// owns no mutable instance cache; every command loads and conditionally saves one durable snapshot.
type Engine struct {
	store    Store
	registry *Registry
}

// NewEngine constructs an engine with explicit persistence and handler dependencies.
//
// store and registry must be non-nil. Invalid dependencies are detected by Start or Handle and returned
// as errors rather than causing package initialization side effects.
func NewEngine(store Store, registry *Registry) *Engine {
	return &Engine{store: store, registry: registry}
}

// Start validates and freezes definition, enters its start path, and atomically creates one instance.
//
// request.ID and Initiator must be non-empty; Data must be absent or valid JSON. Handler activation runs
// before persistence and may observe context cancellation. The returned snapshot is detached from Store.
func (e *Engine) Start(ctx context.Context, definition *Definition, request StartRequest) (*Instance, error) {
	// Required collaborators must exist before validation can resolve handlers or persist the new aggregate.
	if e == nil || e.store == nil || e.registry == nil {
		return nil, fmt.Errorf("%w: start dependencies are nil", ErrInvalidEngine)
	}
	// External identity and JSON syntax are checked before potentially expensive Definition compilation.
	if request.ID == "" || request.Initiator == "" || !validJSON(request.Data) {
		return nil, fmt.Errorf("%w: id, initiator, or data is invalid", ErrInvalidStartRequest)
	}
	// Compilation freezes a trusted execution plan before any instance state becomes durable.
	plan, err := compileDefinition(definition, e.registry)
	if err != nil {
		return nil, err
	}

	// Use the compiler-owned snapshot so caller mutation cannot invalidate the execution plan or new instance.
	instance := &Instance{
		ID:         request.ID,
		Definition: cloneDefinition(plan.definition),
		Status:     InstanceStatusRunning,
		Initiator:  request.Initiator,
		Data:       slices.Clone(request.Data),
		Version:    1,
	}
	start, err := plan.startNode()
	if err != nil {
		return nil, err
	}
	instance.CurrentNodeID = start.ID
	e.appendAudit(instance, AuditRecord{Action: "instance.started", NodeID: start.ID, ActorID: request.Initiator})

	// Start nodes are control-only, so execution immediately follows their unconditional outgoing edge.
	if err := e.advance(ctx, instance, plan, ""); err != nil {
		return nil, err
	}
	if err := e.store.Create(ctx, instance); err != nil {
		return nil, fmt.Errorf("workflow: persist new instance: %w", err)
	}
	return cloneInstance(instance), nil
}

// StartPublished resolves one exact immutable Definition version and starts an instance from its snapshot.
//
// definitions must be a non-nil reader; definitionID must be non-empty and version must be positive. The
// reader's not-found and context errors remain in the returned error chain. Startup then follows Start's
// validation, handler, persistence, and ownership contract. No latest-version fallback is attempted.
func (e *Engine) StartPublished(
	ctx context.Context,
	definitions DefinitionReader,
	definitionID string,
	version uint64,
	request StartRequest,
) (*Instance, error) {
	// Exact-version startup requires a complete repository identity and never infers missing values.
	if definitions == nil || definitionID == "" || version == 0 {
		return nil, fmt.Errorf("%w: definition reader or identity is invalid", ErrInvalidStartRequest)
	}

	// Resolve through the immutable repository seam before Start freezes another copy into Instance storage.
	definition, err := definitions.Load(ctx, definitionID, version)
	// Preserve repository sentinels such as ErrDefinitionNotFound for caller error dispatch.
	if err != nil {
		return nil, fmt.Errorf("workflow: start published definition %q version %d: %w", definitionID, version, err)
	}
	return e.Start(ctx, definition, request)
}

// Handle processes one actor command against the current node and commits it with optimistic concurrency.
//
// The task must belong to the current node and actor. Handler errors leave durable state unchanged. A stale
// concurrent command returns ErrVersionConflict from Store; callers may reload before deciding whether to retry.
func (e *Engine) Handle(ctx context.Context, command Command) (*Instance, error) {
	// Reject dependency and boundary-input errors before loading or mutating durable instance state.
	if err := e.validateCommand(command); err != nil {
		return nil, err
	}

	// Load one caller-owned aggregate and preserve its version for the final compare-and-swap.
	instance, err := e.store.Load(ctx, command.InstanceID)
	if err != nil {
		return nil, fmt.Errorf("workflow: handle command: %w", err)
	}
	if instance.Status != InstanceStatusRunning {
		return nil, fmt.Errorf("%w: instance is %q", ErrInvalidCommand, instance.Status)
	}
	plan, err := compileDefinition(&instance.Definition, e.registry)
	if err != nil {
		return nil, err
	}
	expectedVersion := instance.Version
	node, err := plan.node(instance.CurrentNodeID)
	if err != nil {
		return nil, err
	}
	handler, err := e.registry.handler(node.Kind)
	if err != nil {
		return nil, err
	}

	// Limit handler visibility to its current node, preventing accidental mutation of historical assignments.
	result, err := handler.Handle(ctx, CommandInput{
		Config:  slices.Clone(node.Config),
		Data:    slices.Clone(instance.Data),
		State:   slices.Clone(instance.NodeState),
		Tasks:   tasksForNode(instance.Tasks, node.ID),
		Command: command,
	})
	if err != nil {
		return nil, fmt.Errorf("workflow: handle node %q command: %w", node.ID, err)
	}
	// Record the causal command before its resulting node transitions so audit order mirrors execution order.
	e.appendAudit(instance, AuditRecord{
		Action:  "task." + command.Name,
		NodeID:  node.ID,
		TaskID:  command.TaskID,
		ActorID: command.ActorID,
	})
	if err := e.applyResult(ctx, instance, plan, node, result); err != nil {
		return nil, err
	}
	instance.Version++
	if err := e.store.Save(ctx, instance, expectedVersion); err != nil {
		return nil, fmt.Errorf("workflow: persist command: %w", err)
	}
	return cloneInstance(instance), nil
}

// Withdraw authorizes and atomically ends one running instance without executing its active node handler.
//
// request identities and policy must be non-empty, and ActorID must originate from trusted host context. Policy
// receives a defensive pre-transition snapshot and may deny with any host-owned error. A successful withdrawal
// closes every active task, appends one actor-attributed audit record, increments Version once, and commits the
// full aggregate through Store.Save CAS. Load, policy, cancellation, terminal-state, and CAS errors leave durable
// state unchanged; returned snapshots are detached from Store ownership.
func (e *Engine) Withdraw(
	ctx context.Context,
	request WithdrawRequest,
	policy WithdrawalPolicy,
) (*Instance, error) {
	// Withdrawal needs persistence and complete trusted identities before any durable state is loaded.
	if e == nil || e.store == nil {
		return nil, fmt.Errorf("%w: withdraw store is nil", ErrInvalidEngine)
	}
	if request.InstanceID == "" || request.ActorID == "" || policy == nil {
		return nil, fmt.Errorf("%w: instance, actor, or policy is empty", ErrInvalidWithdrawRequest)
	}

	// Load one caller-owned aggregate and reject terminal states before invoking host authorization logic.
	instance, err := e.store.Load(ctx, request.InstanceID)
	if err != nil {
		return nil, fmt.Errorf("workflow: withdraw instance: %w", err)
	}
	if instance.Status != InstanceStatusRunning {
		return nil, fmt.Errorf("%w: instance %q is %q", ErrInstanceNotRunning, instance.ID, instance.Status)
	}
	expectedVersion := instance.Version

	// Host policy observes an isolated pre-transition snapshot and remains the sole authorization authority.
	if err := policy.AuthorizeWithdrawal(ctx, request.ActorID, cloneInstance(instance)); err != nil {
		return nil, fmt.Errorf("workflow: authorize withdrawal: %w", err)
	}

	// Apply the whole lifecycle transition in memory before issuing its single aggregate CAS write.
	instance.Status = InstanceStatusWithdrawn
	for index := range instance.Tasks {
		if instance.Tasks[index].Status == TaskStatusActive {
			instance.Tasks[index].Status = TaskStatusClosed
		}
	}
	e.appendAudit(instance, AuditRecord{
		Action:  "instance.withdrawn",
		NodeID:  instance.CurrentNodeID,
		ActorID: request.ActorID,
	})
	instance.Version++

	// Store.Save atomically commits status, tasks, audit, and version or preserves the prior full snapshot.
	if err := e.store.Save(ctx, instance, expectedVersion); err != nil {
		return nil, fmt.Errorf("workflow: persist withdrawal: %w", err)
	}
	return cloneInstance(instance), nil
}

// validateCommand checks Engine dependencies and the complete external command boundary before Store access.
//
// command identity fields must be non-empty and Payload must be absent or valid JSON. The method performs no
// I/O and returns ErrInvalidEngine or ErrInvalidCommand so callers can classify setup and request failures.
func (e *Engine) validateCommand(command Command) error {
	// Missing collaborators make every command unsafe regardless of its input fields.
	if e == nil || e.store == nil || e.registry == nil {
		return fmt.Errorf("%w: handle dependencies are nil", ErrInvalidEngine)
	}
	// All four identities jointly bind the command to one task, actor, and handler operation.
	if command.InstanceID == "" || command.TaskID == "" || command.ActorID == "" || command.Name == "" {
		return fmt.Errorf("%w: required command field is empty", ErrInvalidCommand)
	}
	// Invalid JSON cannot be delegated because handlers own schema validation only after syntax is trustworthy.
	if !validJSON(command.Payload) {
		return fmt.Errorf("%w: payload is not valid json", ErrInvalidCommand)
	}
	return nil
}

// advance follows compiled routes and activates nodes until execution waits or reaches a terminal state.
//
// instance is the caller-owned aggregate being prepared for one atomic Store write. plan must be compiled
// from instance.Definition, and outcome selects the first route; an empty outcome is unconditional. Handler
// or routing errors leave durable state unchanged because callers persist only after this method succeeds.
func (e *Engine) advance(ctx context.Context, instance *Instance, plan *compiledDefinition, outcome string) error {
	for {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("workflow: advance instance: %w", err)
		}
		next, err := plan.nextNode(instance.CurrentNodeID, outcome)
		if err != nil {
			return err
		}
		instance.CurrentNodeID = next.ID
		instance.NodeState = nil
		e.appendAudit(instance, AuditRecord{Action: "node.entered", NodeID: next.ID})

		// End nodes complete immediately; other business nodes decide whether to wait or continue again.
		if next.Kind == KindEnd {
			instance.Status = InstanceStatusCompleted
			e.appendAudit(instance, AuditRecord{Action: "instance.completed", NodeID: next.ID})
			return nil
		}
		handler, err := e.registry.handler(next.Kind)
		if err != nil {
			return err
		}
		result, err := handler.Activate(ctx, ActivationInput{
			Config: slices.Clone(next.Config),
			Data:   slices.Clone(instance.Data),
		})
		if err != nil {
			return fmt.Errorf("workflow: activate node %q: %w", next.ID, err)
		}
		if err := e.applyNodeTasks(instance, next.ID, result.Tasks, false); err != nil {
			return err
		}
		instance.NodeState = slices.Clone(result.State)
		switch result.Disposition {
		case DispositionWaiting:
			return nil
		case DispositionContinue:
			outcome = result.Outcome
		case DispositionReject:
			// Synchronous handlers use the same explicit outcome contract as command-driven handlers.
			if result.Outcome != "" {
				e.appendAudit(instance, AuditRecord{Action: "node.rejected", NodeID: next.ID})
				outcome = result.Outcome
				continue
			}

			// Empty rejection outcomes retain the global terminal behavior.
			instance.Status = InstanceStatusRejected
			e.appendAudit(instance, AuditRecord{Action: "instance.rejected", NodeID: next.ID})
			return nil
		case DispositionUnknown:
			return fmt.Errorf("%w: node %q returned an empty disposition", ErrInvalidNodeResult, next.ID)
		default:
			return fmt.Errorf("%w: node %q returned disposition %q", ErrInvalidNodeResult, next.ID, result.Disposition)
		}
	}
}

// applyResult validates and applies one handler result to the caller-owned aggregate.
//
// plan and node must belong to instance.Definition. Waiting replaces current tasks, continue requires the
// result outcome route, and reject either terminates with an empty outcome or requires its outcome route.
// Errors prevent the enclosing Handle operation from saving the mutated snapshot; this method performs no
// Store I/O itself.
func (e *Engine) applyResult(
	ctx context.Context,
	instance *Instance,
	plan *compiledDefinition,
	node *NodeDefinition,
	result NodeResult,
) error {
	if err := e.applyNodeTasks(instance, node.ID, result.Tasks, true); err != nil {
		return err
	}
	instance.NodeState = slices.Clone(result.State)
	switch result.Disposition {
	case DispositionWaiting:
		return nil
	case DispositionContinue:
		return e.advance(ctx, instance, plan, result.Outcome)
	case DispositionReject:
		// A handler may name only an outcome; the compiled Definition remains the sole owner of its target node.
		if result.Outcome != "" {
			// Record rejection before target entry; callers discard both mutations when route resolution fails.
			e.appendAudit(instance, AuditRecord{Action: "node.rejected", NodeID: node.ID})
			return e.advance(ctx, instance, plan, result.Outcome)
		}

		// An empty outcome preserves the original terminal rejection contract.
		instance.Status = InstanceStatusRejected
		e.appendAudit(instance, AuditRecord{Action: "instance.rejected", NodeID: node.ID})
		return nil
	case DispositionUnknown:
		return fmt.Errorf("%w: node %q returned an empty disposition", ErrInvalidNodeResult, node.ID)
	default:
		return fmt.Errorf("%w: node %q returned disposition %q", ErrInvalidNodeResult, node.ID, result.Disposition)
	}
}

// applyNodeTasks assigns IDs to activation drafts or replaces the exact task set returned after a command.
func (e *Engine) applyNodeTasks(instance *Instance, nodeID string, tasks []Task, isReplacement bool) error {
	if isReplacement {
		// Replace by ID while preserving task order and all historical tasks from other nodes.
		updates := make(map[TaskID]Task, len(tasks))
		for _, task := range tasks {
			if task.ID == "" || task.NodeID != nodeID {
				return fmt.Errorf("%w: replacement task identity is invalid", ErrInvalidNodeResult)
			}
			updates[task.ID] = task
		}
		for i := range instance.Tasks {
			if instance.Tasks[i].NodeID != nodeID {
				continue
			}
			updated, exists := updates[instance.Tasks[i].ID]
			if !exists {
				return fmt.Errorf("%w: handler omitted task %q", ErrInvalidNodeResult, instance.Tasks[i].ID)
			}
			instance.Tasks[i] = updated
			delete(updates, updated.ID)
		}
		if len(updates) != 0 {
			return fmt.Errorf("%w: handler introduced unknown task", ErrInvalidNodeResult)
		}
		return nil
	}

	// Activation tasks are drafts: the engine binds node ownership and creates collision-resistant IDs.
	for _, task := range tasks {
		if task.ID != "" || task.Assignee == "" || task.Status != TaskStatusActive {
			return fmt.Errorf("%w: activation task is invalid", ErrInvalidNodeResult)
		}
		id, err := newTaskID()
		if err != nil {
			return err
		}
		task.ID = id
		task.NodeID = nodeID
		instance.Tasks = append(instance.Tasks, task)
	}
	return nil
}

// appendAudit stamps and appends one immutable transition record in execution order.
func (e *Engine) appendAudit(instance *Instance, record AuditRecord) {
	record.At = time.Now().UTC()
	instance.Audit = append(instance.Audit, record)
}

// tasksForNode returns a defensive copy of every historical or current task owned by nodeID.
func tasksForNode(tasks []Task, nodeID string) []Task {
	result := make([]Task, 0, len(tasks))
	for _, task := range tasks {
		if task.NodeID == nodeID {
			result = append(result, task)
		}
	}
	return result
}

// newTaskID generates a process-independent task identifier using 128 bits from crypto/rand.
func newTaskID() (TaskID, error) {
	const randomBytes = 16 // 128 random bits make collisions negligible without a shared generator.
	buffer := make([]byte, randomBytes)
	if _, err := rand.Read(buffer); err != nil {
		return "", fmt.Errorf("workflow: generate task id: %w", err)
	}
	return TaskID("task_" + hex.EncodeToString(buffer)), nil
}
