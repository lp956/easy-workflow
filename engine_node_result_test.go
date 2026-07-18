// This file verifies Engine task-set validation through public NodeHandler and Store seams.
// It treats the returned aggregate as observable behavior and does not assert internal fact-module organization.
package workflow_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	workflow "github.com/lvpeng/easy-workflow"
)

// taskSetHandlerKind is the isolated extension key shared by malformed task-set scenarios.
const taskSetHandlerKind = "task-set-result-test"

// taskSetViolation selects one malformed complete task view returned by the test handler.
type taskSetViolation string

const (
	// violationUnknownTask appends an identity absent from the current aggregate.
	violationUnknownTask taskSetViolation = "unknown-task"
	// violationOmittedTask removes the required current-node assignment.
	violationOmittedTask taskSetViolation = "omitted-task"
	// violationOtherNodeTask proposes a task value owned by a different node.
	violationOtherNodeTask taskSetViolation = "other-node-task"
)

// taskSetResultHandler activates one valid assignment, then returns one selected malformed complete task view.
//
// The adapter is stateless and safe for concurrent tests. It exercises only the public NodeHandler boundary; Engine
// remains responsible for validating task ownership and persisting an accepted candidate atomically.
type taskSetResultHandler struct {
	// violation determines which task-set invariant Handle attempts to break.
	violation taskSetViolation
}

// Validate accepts any test configuration without retaining caller-owned bytes.
// It is side-effect free, concurrency-safe, and always returns nil because these scenarios target runtime facts.
func (taskSetResultHandler) Validate(json.RawMessage) error {
	return nil
}

// Activate creates one valid task draft whose identity and node ownership are completed by Engine.
// It ignores immutable input, returns detached data, performs no external I/O, and is concurrency-safe.
func (taskSetResultHandler) Activate(context.Context, workflow.ActivationInput) (workflow.NodeResult, error) {
	return workflow.NodeResult{
		Disposition: workflow.DispositionWaiting,
		Tasks:       []workflow.Task{{Assignee: "owner-a", Status: workflow.TaskStatusActive}},
	}, nil
}

// Handle returns a detached current task view containing the selected ownership or completeness violation.
//
// input must contain the activation task. The method returns no error so Engine remains the observable classifier,
// retains no caller data, performs no external I/O, and is safe for concurrent calls.
func (h taskSetResultHandler) Handle(_ context.Context, input workflow.CommandInput) (workflow.NodeResult, error) {
	tasks := append([]workflow.Task(nil), input.Tasks...)
	switch h.violation {
	case violationUnknownTask:
		tasks = append(tasks, workflow.Task{
			ID:       "unknown-task",
			NodeID:   "decision",
			Assignee: "owner-b",
			Status:   workflow.TaskStatusActive,
		})
	case violationOmittedTask:
		tasks = nil
	case violationOtherNodeTask:
		tasks = append(tasks, workflow.Task{
			ID:       "other-node-task",
			NodeID:   "other-node",
			Assignee: "owner-b",
			Status:   workflow.TaskStatusActive,
		})
	}
	return workflow.NodeResult{Disposition: workflow.DispositionWaiting, Tasks: tasks}, nil
}

// startTaskSetResultInstance builds and starts one isolated Engine scenario for task-set validation.
//
// violation selects the test handler behavior and id must be a non-empty aggregate identity unique to the caller. The
// helper fails its test on setup errors and returns an Engine, its MemoryStore inspection seam, and a detached snapshot.
func startTaskSetResultInstance(
	t *testing.T,
	violation taskSetViolation,
	id workflow.InstanceID,
) (*workflow.Engine, *workflow.MemoryStore, *workflow.Instance) {
	t.Helper()

	// Register the selected handler and build the smallest waiting task graph accepted by the compiler.
	registry := workflow.NewRegistry()
	if err := registry.Register(taskSetHandlerKind, taskSetResultHandler{violation: violation}); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	builder := workflow.NewBuilder("task-set-result-validation")
	builder.Start("start")
	builder.Node("decision", taskSetHandlerKind, nil)
	builder.Node("other-node", taskSetHandlerKind, nil)
	builder.End("end")
	builder.Connect("start", "decision", "")
	builder.Connect("decision", "other-node", "accepted")
	builder.Connect("other-node", "end", "accepted")
	definition, err := builder.Build()
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}

	// Start through the public persistence seam so rejection assertions compare durable detached snapshots.
	store := workflow.NewMemoryStore()
	engine := workflow.NewEngine(store, registry)
	instance, err := engine.Start(t.Context(), definition, workflow.StartRequest{ID: id, Initiator: "requester-a"})
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if violation == violationOtherNodeTask {
		// Seed an existing task for the declared non-current node through Store's public snapshot contract.
		expectedVersion := instance.Version
		instance.Tasks = append(instance.Tasks, workflow.Task{
			ID:       "other-node-task",
			NodeID:   "other-node",
			Assignee: "historical-owner",
			Status:   workflow.TaskStatusCompleted,
			Outcome:  "accepted",
		})
		instance.Version++
		if err := store.Save(t.Context(), instance, expectedVersion); err != nil {
			t.Fatalf("Save(other-node task) error = %v", err)
		}
	}
	return engine, store, instance
}

// TestEngineRejectsMalformedTaskDecisionSets verifies ownership and completeness without persisting partial facts.
func TestEngineRejectsMalformedTaskDecisionSets(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		violation taskSetViolation
	}{
		{name: "unknown task", violation: violationUnknownTask},
		{name: "omitted current task", violation: violationOmittedTask},
		{name: "task owned by another node", violation: violationOtherNodeTask},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			// Each malformed result reaches Engine through an independent handler, aggregate, and Store snapshot.
			id := workflow.InstanceID("task-set-" + string(test.violation))
			engine, store, before := startTaskSetResultInstance(t, test.violation, id)
			_, err := engine.Handle(t.Context(), workflow.Command{
				InstanceID: before.ID,
				TaskID:     before.Tasks[0].ID,
				ActorID:    before.Tasks[0].Assignee,
				Name:       "decide",
			})
			if !errors.Is(err, workflow.ErrInvalidNodeResult) {
				t.Fatalf("Handle() error = %v, want ErrInvalidNodeResult", err)
			}

			// Candidate rejection preserves tasks, audit, node state, status, and Version as one durable snapshot.
			after, err := store.Load(t.Context(), before.ID)
			if err != nil {
				t.Fatalf("Load() error = %v", err)
			}
			if !reflect.DeepEqual(after, before) {
				t.Errorf("stored instance changed after invalid task result: before=%#v after=%#v", before, after)
			}
		})
	}
}
