// This file verifies Engine task-set validation through public NodeHandler and Store seams.
// It treats the returned aggregate as observable behavior and does not assert internal fact-module organization.
package workflow_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"sync"
	"testing"

	workflow "github.com/lvpeng/easy-workflow"
)

// taskSetHandlerKind is the isolated extension key shared by malformed task-set scenarios.
const taskSetHandlerKind = "task-set-result-test"

// malformedResultHandlerKind is the isolated extension key used to exercise stage-specific NodeResult validation.
const malformedResultHandlerKind = "malformed-node-result-test"

var (
	// errInvalidReturnRole classifies malformed role configuration supplied to the return-result test handler.
	errInvalidReturnRole = errors.New("test handler: invalid role")
)

// taskSetViolation selects one malformed complete task view returned by the test handler.
type taskSetViolation string

const (
	// violationUnknownTask appends an identity absent from the current aggregate.
	violationUnknownTask taskSetViolation = "unknown-task"
	// violationOmittedTask removes the required current-node assignment.
	violationOmittedTask taskSetViolation = "omitted-task"
	// violationOtherNodeTask proposes a task value owned by a different node.
	violationOtherNodeTask taskSetViolation = "other-node-task"
	// violationMutatedHistoricalTask rewrites one completed task from an earlier round at the current node.
	violationMutatedHistoricalTask taskSetViolation = "mutated-historical-task"
	// violationReassignedActiveTask changes an active task owner without using Engine.Transfer.
	violationReassignedActiveTask taskSetViolation = "reassigned-active-task"
	// violationWaitingWithoutActiveTask leaves the instance waiting after closing its final actionable task.
	violationWaitingWithoutActiveTask taskSetViolation = "waiting-without-active-task"
	// violationContinueWithActiveTask routes away while retaining an actionable task at the old node.
	violationContinueWithActiveTask taskSetViolation = "continue-with-active-task"
	// violationRejectWithActiveTask rejects while retaining an actionable task at the terminal node.
	violationRejectWithActiveTask taskSetViolation = "reject-with-active-task"
)

// malformedResultViolation selects one invalid cross-field combination returned at a public handler seam.
type malformedResultViolation string

const (
	// violationInvalidState returns bytes that cannot be persisted as one JSON value.
	violationInvalidState malformedResultViolation = "invalid-state"
	// violationMissingDisposition omits the mandatory transition decision.
	violationMissingDisposition malformedResultViolation = "missing-disposition"
	// violationWaitingOutcome attaches an ignored route selector to a waiting result.
	violationWaitingOutcome malformedResultViolation = "waiting-outcome"
	// violationRoutedActivationTasks attaches task drafts to a result that immediately leaves its node.
	violationRoutedActivationTasks malformedResultViolation = "routed-activation-tasks"
)

// malformedResultHandler returns one selected invalid result during activation or command handling.
//
// The adapter owns no external resources. Command-mode instances first create one valid assignment, while activation
// mode fails before Store.Create. Returned slices are detached, and separate handler values are safe for parallel tests.
type malformedResultHandler struct {
	// activation selects whether the malformed result is emitted by Activate rather than Handle.
	activation bool
	// violation identifies the result invariant under test.
	violation malformedResultViolation
}

// Validate accepts the empty test configuration without retaining its bytes.
func (malformedResultHandler) Validate(json.RawMessage) error {
	return nil
}

// Activate either returns the selected malformed proposal or creates one valid command-test assignment.
func (h malformedResultHandler) Activate(context.Context, workflow.ActivationInput) (workflow.NodeResult, error) {
	if h.activation {
		return malformedResult(h.violation, nil), nil
	}
	return workflow.NodeResult{
		Disposition: workflow.DispositionWaiting,
		Tasks:       []workflow.Task{{Assignee: "owner-a", Status: workflow.TaskStatusActive}},
	}, nil
}

// Handle returns the selected malformed proposal over the complete detached current-node task view.
func (h malformedResultHandler) Handle(_ context.Context, input workflow.CommandInput) (workflow.NodeResult, error) {
	return malformedResult(h.violation, slices.Clone(input.Tasks)), nil
}

// malformedResult constructs one invalid proposal while keeping unrelated fields valid for its test stage.
//
// violation must be one of the declared test cases. tasks is nil for activation failures and the complete current-node
// view for command failures. The return value owns its task slice and performs no I/O.
func malformedResult(violation malformedResultViolation, tasks []workflow.Task) workflow.NodeResult {
	result := workflow.NodeResult{Disposition: workflow.DispositionWaiting, Tasks: slices.Clone(tasks)}
	switch violation {
	case violationInvalidState:
		result.State = json.RawMessage(`{"incomplete"`)
	case violationMissingDisposition:
		result.Disposition = workflow.DispositionUnknown
	case violationWaitingOutcome:
		result.Outcome = "done"
	case violationRoutedActivationTasks:
		result.Disposition = workflow.DispositionContinue
		result.Outcome = "done"
		result.Tasks = []workflow.Task{{Assignee: "owner-a", Status: workflow.TaskStatusActive}}
	}
	return result
}

// returnResultHandler activates a valid historical target once, then returns a malformed proposal on reactivation.
//
// A mutex protects the target activation count so the adapter remains valid under Engine's concurrent handler contract.
// The handler retains no input, performs no external I/O, and scopes mutable state to one test Engine.
type returnResultHandler struct {
	// mu protects targetActivations across initial activation and explicit return.
	mu sync.Mutex
	// targetActivations counts accepted calls for the node configured as the return target.
	targetActivations int
	// violation selects the malformed proposal returned by the target's second activation.
	violation malformedResultViolation
}

// Validate requires one JSON string naming either the target or source role.
func (*returnResultHandler) Validate(config json.RawMessage) error {
	var role string
	if err := json.Unmarshal(config, &role); err != nil {
		return fmt.Errorf("test handler decode return role: %w", err)
	}
	if role != "target" && role != "source" {
		return errInvalidReturnRole
	}
	return nil
}

// Activate creates valid target/source assignments except for the selected malformed return-target reactivation.
func (h *returnResultHandler) Activate(_ context.Context, input workflow.ActivationInput) (workflow.NodeResult, error) {
	var role string
	if err := json.Unmarshal(input.Config, &role); err != nil {
		return workflow.NodeResult{}, fmt.Errorf("test handler decode activation role: %w", err)
	}
	if role == "target" {
		h.mu.Lock()
		h.targetActivations++
		activation := h.targetActivations
		h.mu.Unlock()
		if activation > 1 {
			return malformedResult(h.violation, nil), nil
		}
	}
	return workflow.NodeResult{
		Disposition: workflow.DispositionWaiting,
		Tasks:       []workflow.Task{{Assignee: workflow.ActorID(role + "-owner"), Status: workflow.TaskStatusActive}},
	}, nil
}

// Handle completes the target assignment and follows the declared route to the source node.
func (*returnResultHandler) Handle(_ context.Context, input workflow.CommandInput) (workflow.NodeResult, error) {
	tasks := slices.Clone(input.Tasks)
	for index := range tasks {
		if tasks[index].ID == input.TaskID {
			tasks[index].Status = workflow.TaskStatusCompleted
			tasks[index].Outcome = "done"
		}
	}
	return workflow.NodeResult{Disposition: workflow.DispositionContinue, Outcome: "done", Tasks: tasks}, nil
}

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

// Handle returns a detached current task view containing the selected ownership, lifecycle, or completeness violation.
//
// input must contain the activation task and may contain a seeded historical task. The method returns no error so Engine
// remains the observable classifier, retains no caller data, performs no external I/O, and is safe for concurrent calls.
func (h taskSetResultHandler) Handle(_ context.Context, input workflow.CommandInput) (workflow.NodeResult, error) {
	tasks := append([]workflow.Task(nil), input.Tasks...)
	disposition := workflow.DispositionWaiting
	outcome := ""
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
	case violationMutatedHistoricalTask:
		for index := range tasks {
			if tasks[index].ID == "historical-task" {
				tasks[index].Outcome = "rewritten"
			}
		}
	case violationReassignedActiveTask:
		tasks[0].Assignee = "owner-b"
	case violationWaitingWithoutActiveTask:
		tasks[0].Status = workflow.TaskStatusClosed
	case violationContinueWithActiveTask:
		disposition = workflow.DispositionContinue
		outcome = "accepted"
	case violationRejectWithActiveTask:
		disposition = workflow.DispositionReject
	}
	return workflow.NodeResult{Disposition: disposition, Outcome: outcome, Tasks: tasks}, nil
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
	if violation == violationOtherNodeTask || violation == violationMutatedHistoricalTask {
		// Seed the exact durable history needed by cross-node ownership or same-node immutability scenarios.
		expectedVersion := instance.Version
		if violation == violationOtherNodeTask {
			instance.Tasks = append(instance.Tasks, workflow.Task{
				ID:       "other-node-task",
				NodeID:   "other-node",
				Assignee: "historical-owner",
				Status:   workflow.TaskStatusCompleted,
				Outcome:  "accepted",
			})
		} else {
			instance.Tasks = append(instance.Tasks, workflow.Task{
				ID:       "historical-task",
				NodeID:   "decision",
				Assignee: "historical-owner",
				Status:   workflow.TaskStatusCompleted,
				Outcome:  "accepted",
			})
		}
		instance.Version++
		if err := store.Save(t.Context(), instance, expectedVersion); err != nil {
			t.Fatalf("Save(seeded task history) error = %v", err)
		}
	}
	return engine, store, instance
}

// TestEngineRejectsMalformedTaskDecisionSets verifies task ownership, history, and lifecycle invariants atomically.
func TestEngineRejectsMalformedTaskDecisionSets(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		violation taskSetViolation
	}{
		{name: "unknown task", violation: violationUnknownTask},
		{name: "omitted current task", violation: violationOmittedTask},
		{name: "task owned by another node", violation: violationOtherNodeTask},
		{name: "mutated historical task", violation: violationMutatedHistoricalTask},
		{name: "reassigned active task", violation: violationReassignedActiveTask},
		{name: "waiting without active task", violation: violationWaitingWithoutActiveTask},
		{name: "continue with active task", violation: violationContinueWithActiveTask},
		{name: "reject with active task", violation: violationRejectWithActiveTask},
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

// TestEngineRejectsMalformedActivationResultsAtomically verifies activation rules fail before instance creation.
func TestEngineRejectsMalformedActivationResultsAtomically(t *testing.T) {
	t.Parallel()

	violations := []malformedResultViolation{
		violationInvalidState,
		violationMissingDisposition,
		violationWaitingOutcome,
		violationRoutedActivationTasks,
	}
	for _, violation := range violations {
		t.Run(string(violation), func(t *testing.T) {
			t.Parallel()

			// Compile the same valid graph for every case so only the handler result changes observable behavior.
			registry := workflow.NewRegistry()
			if err := registry.Register(malformedResultHandlerKind, malformedResultHandler{
				activation: true,
				violation:  violation,
			}); err != nil {
				t.Fatalf("Register() error = %v", err)
			}
			builder := workflow.NewBuilder("malformed-activation-" + string(violation))
			builder.Start("start")
			builder.Node("decision", malformedResultHandlerKind, nil)
			builder.End("end")
			builder.Connect("start", "decision", "")
			builder.Connect("decision", "end", "done")
			definition, err := builder.Build()
			if err != nil {
				t.Fatalf("Build() error = %v", err)
			}

			// Invalid activation data must be classified by Engine and leave no partially created aggregate.
			store := workflow.NewMemoryStore()
			instanceID := workflow.InstanceID("malformed-activation-" + string(violation))
			_, err = workflow.NewEngine(store, registry).Start(t.Context(), definition, workflow.StartRequest{
				ID:        instanceID,
				Initiator: "requester-a",
			})
			if !errors.Is(err, workflow.ErrInvalidNodeResult) {
				t.Fatalf("Start() error = %v, want ErrInvalidNodeResult", err)
			}
			if _, loadErr := store.Load(t.Context(), instanceID); !errors.Is(loadErr, workflow.ErrInstanceNotFound) {
				t.Fatalf("Load() error = %v, want ErrInstanceNotFound", loadErr)
			}
		})
	}
}

// TestEngineRejectsMalformedCommandResultsAtomically verifies command rules preserve the durable pre-command snapshot.
func TestEngineRejectsMalformedCommandResultsAtomically(t *testing.T) {
	t.Parallel()

	violations := []malformedResultViolation{
		violationInvalidState,
		violationMissingDisposition,
		violationWaitingOutcome,
	}
	for _, violation := range violations {
		t.Run(string(violation), func(t *testing.T) {
			t.Parallel()

			// Start a valid waiting instance before asking the same handler to return one malformed command proposal.
			registry := workflow.NewRegistry()
			if err := registry.Register(malformedResultHandlerKind, malformedResultHandler{violation: violation}); err != nil {
				t.Fatalf("Register() error = %v", err)
			}
			builder := workflow.NewBuilder("malformed-command-" + string(violation))
			builder.Start("start")
			builder.Node("decision", malformedResultHandlerKind, nil)
			builder.End("end")
			builder.Connect("start", "decision", "")
			builder.Connect("decision", "end", "done")
			definition, err := builder.Build()
			if err != nil {
				t.Fatalf("Build() error = %v", err)
			}
			store := workflow.NewMemoryStore()
			engine := workflow.NewEngine(store, registry)
			before, err := engine.Start(t.Context(), definition, workflow.StartRequest{
				ID:        workflow.InstanceID("malformed-command-" + string(violation)),
				Initiator: "requester-a",
			})
			if err != nil {
				t.Fatalf("Start() error = %v", err)
			}

			// Reject the proposal without committing its audit, state, tasks, status, or Version changes.
			_, err = engine.Handle(t.Context(), workflow.Command{
				InstanceID: before.ID,
				TaskID:     before.Tasks[0].ID,
				ActorID:    before.Tasks[0].Assignee,
				Name:       "decide",
			})
			if !errors.Is(err, workflow.ErrInvalidNodeResult) {
				t.Fatalf("Handle() error = %v, want ErrInvalidNodeResult", err)
			}
			after, loadErr := store.Load(t.Context(), before.ID)
			if loadErr != nil {
				t.Fatalf("Load() error = %v", loadErr)
			}
			if !reflect.DeepEqual(after, before) {
				t.Errorf("stored instance changed after invalid command result: before=%#v after=%#v", before, after)
			}
		})
	}
}

// TestEngineRejectsMalformedReturnResultsAtomically verifies return reactivation uses the same strict result boundary.
func TestEngineRejectsMalformedReturnResultsAtomically(t *testing.T) {
	t.Parallel()

	violations := []malformedResultViolation{
		violationInvalidState,
		violationMissingDisposition,
		violationWaitingOutcome,
		violationRoutedActivationTasks,
	}
	for _, violation := range violations {
		t.Run(string(violation), func(t *testing.T) {
			t.Parallel()

			// Move from the historical target to a waiting source before exercising target reactivation.
			handler := &returnResultHandler{violation: violation}
			registry := workflow.NewRegistry()
			if err := registry.Register(malformedResultHandlerKind, handler); err != nil {
				t.Fatalf("Register() error = %v", err)
			}
			builder := workflow.NewBuilder("malformed-return-" + string(violation))
			builder.Start("start")
			builder.Node("target", malformedResultHandlerKind, "target")
			builder.Node("source", malformedResultHandlerKind, "source")
			builder.End("end")
			builder.Connect("start", "target", "")
			builder.Connect("target", "source", "done")
			builder.Connect("source", "end", "done")
			definition, err := builder.Build()
			if err != nil {
				t.Fatalf("Build() error = %v", err)
			}
			store := workflow.NewMemoryStore()
			engine := workflow.NewEngine(store, registry)
			instance, err := engine.Start(t.Context(), definition, workflow.StartRequest{
				ID:        workflow.InstanceID("malformed-return-" + string(violation)),
				Initiator: "requester-a",
			})
			if err != nil {
				t.Fatalf("Start() error = %v", err)
			}
			before, err := engine.Handle(t.Context(), workflow.Command{
				InstanceID: instance.ID,
				TaskID:     instance.Tasks[0].ID,
				ActorID:    instance.Tasks[0].Assignee,
				Name:       "decide",
			})
			if err != nil {
				t.Fatalf("Handle() error = %v", err)
			}

			// Preserve both the legacy invalid-target classification and the new malformed-result cause during migration.
			_, err = engine.Return(t.Context(), workflow.ReturnRequest{
				InstanceID:   before.ID,
				ActorID:      "operator-a",
				TargetNodeID: "target",
				Reason:       "repeat target validation",
			}, returnPolicyFunc(func(context.Context, workflow.ReturnRequest, *workflow.Instance) error { return nil }))
			if !errors.Is(err, workflow.ErrInvalidNodeResult) || !errors.Is(err, workflow.ErrInvalidReturnTarget) {
				t.Fatalf("Return() error = %v, want ErrInvalidNodeResult and ErrInvalidReturnTarget", err)
			}
			after, loadErr := store.Load(t.Context(), before.ID)
			if loadErr != nil {
				t.Fatalf("Load() error = %v", loadErr)
			}
			if !reflect.DeepEqual(after, before) {
				t.Errorf("stored instance changed after invalid return result: before=%#v after=%#v", before, after)
			}
		})
	}
}
