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
	// ErrInvalidCommand means a command violates the active instance, task, or handler contract.
	ErrInvalidCommand = errors.New("workflow: invalid command")
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
	if e == nil || e.store == nil || e.registry == nil {
		return nil, fmt.Errorf("workflow: start: engine dependencies are nil")
	}
	if request.ID == "" || request.Initiator == "" || !validJSON(request.Data) {
		return nil, fmt.Errorf("workflow: start: id, initiator, or data is invalid")
	}
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

// Handle processes one actor command against the current node and commits it with optimistic concurrency.
//
// The task must belong to the current node and actor. Handler errors leave durable state unchanged. A stale
// concurrent command returns ErrVersionConflict from Store; callers may reload before deciding whether to retry.
func (e *Engine) Handle(ctx context.Context, command Command) (*Instance, error) {
	if e == nil || e.store == nil || e.registry == nil {
		return nil, fmt.Errorf("workflow: handle: engine dependencies are nil")
	}
	if command.InstanceID == "" || command.TaskID == "" || command.ActorID == "" || command.Name == "" {
		return nil, fmt.Errorf("%w: required command field is empty", ErrInvalidCommand)
	}
	if !validJSON(command.Payload) {
		return nil, fmt.Errorf("%w: payload is not valid json", ErrInvalidCommand)
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

// advance follows one declared outcome and activates control or business nodes until execution must wait or ends.
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

// applyResult validates one command result, replaces current tasks, and performs its requested disposition.
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
