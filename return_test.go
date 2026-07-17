// This file verifies explicit instance return through Engine's public lifecycle API.
// It observes authorization, execution history, task rounds, audit order, and atomic snapshots without internals.
package workflow_test

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"testing"

	workflow "github.com/lvpeng/easy-workflow"
	"github.com/lvpeng/easy-workflow/approval"
	"github.com/lvpeng/easy-workflow/condition"
)

var (
	// errReturnDenied is the host-owned sentinel returned by authorization-denial tests.
	errReturnDenied = errors.New("host: return denied")
)

// returnPolicyFunc adapts a test function to the host return authorization contract.
type returnPolicyFunc func(context.Context, workflow.ReturnRequest, *workflow.Instance) error

// AuthorizeReturn delegates one return authorization decision while preserving its returned error.
func (f returnPolicyFunc) AuthorizeReturn(
	ctx context.Context,
	request workflow.ReturnRequest,
	instance *workflow.Instance,
) error {
	return f(ctx, request, instance)
}

// startReturnFlow advances a two-level approval instance to its countersign source node.
//
// store must be fresh and id unique within it. The frozen Definition includes one structurally reachable but
// unvisited approval node so target-history validation can be exercised independently of graph membership.
func startReturnFlow(
	t *testing.T,
	store workflow.Store,
	id workflow.InstanceID,
) (*workflow.Engine, *workflow.Instance) {
	t.Helper()

	// Register Approval and build the historical target, current source, and unvisited alternative branch.
	registry := workflow.NewRegistry()
	if err := registry.Register(approval.Kind, approval.NewHandler()); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	engine := workflow.NewEngine(store, registry)
	builder := workflow.NewBuilder("explicit-return")
	builder.Start("start")
	builder.Node("first-approval", approval.Kind, approval.Config{
		Mode:      approval.ModeAny,
		Assignees: []workflow.ActorID{"reviewer-a"},
	})
	builder.Node("second-approval", approval.Kind, approval.Config{
		Mode:      approval.ModeAll,
		Assignees: []workflow.ActorID{"reviewer-b", "reviewer-c"},
	})
	builder.Node("unvisited-approval", approval.Kind, approval.Config{
		Mode:      approval.ModeAny,
		Assignees: []workflow.ActorID{"reviewer-d"},
	})
	builder.End("end")
	builder.Connect("start", "first-approval", "")
	builder.Connect("first-approval", "second-approval", approval.OutcomeApproved)
	builder.Connect("first-approval", "unvisited-approval", "alternative")
	builder.Connect("second-approval", "end", approval.OutcomeApproved)
	builder.Connect("unvisited-approval", "end", approval.OutcomeApproved)
	definition, err := builder.Build()
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}

	// Complete the first approval so the instance waits at the countersign source with recorded target history.
	instance, err := engine.Start(t.Context(), definition, workflow.StartRequest{
		ID:        id,
		Initiator: "employee-a",
	})
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	instance, err = engine.Handle(t.Context(), workflow.Command{
		InstanceID: instance.ID,
		TaskID:     instance.Tasks[0].ID,
		ActorID:    instance.Tasks[0].Assignee,
		Name:       approval.CommandApprove,
	})
	if err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	return engine, instance
}

// TestEngineReturnsToVisitedApprovalNode verifies one authorized explicit return creates a fresh task round.
func TestEngineReturnsToVisitedApprovalNode(t *testing.T) {
	t.Parallel()

	// Start a two-level approval flow and complete the first node so it becomes an eligible historical target.
	store := workflow.NewMemoryStore()
	engine, instance := startReturnFlow(t, store, "explicit-return-1")
	expectedVersion := instance.Version
	instance.NodeState = []byte(`{"round":"second"}`)
	instance.Version++
	if err := store.Save(t.Context(), instance, expectedVersion); err != nil {
		t.Fatalf("Save(source state) error = %v", err)
	}
	oldTasks := slices.Clone(instance.Tasks)
	oldAudit := slices.Clone(instance.Audit)

	// Host policy receives the explicit request and a defensive pre-transition snapshot at the source node.
	request := workflow.ReturnRequest{
		InstanceID:   instance.ID,
		ActorID:      "operator-a",
		TargetNodeID: "first-approval",
		Reason:       "missing supporting document",
	}
	policy := returnPolicyFunc(func(
		_ context.Context,
		gotRequest workflow.ReturnRequest,
		gotInstance *workflow.Instance,
	) error {
		if gotRequest != request {
			t.Errorf("policy request = %#v, want %#v", gotRequest, request)
		}
		if gotInstance.CurrentNodeID != "second-approval" || gotInstance.Version != instance.Version {
			t.Errorf("policy instance = %#v, want second-approval version %d", gotInstance, instance.Version)
		}
		return nil
	})
	returned, err := engine.Return(t.Context(), request, policy)
	if err != nil {
		t.Fatalf("Return() error = %v", err)
	}

	// Historical tasks remain ordered and decided outcomes immutable; only source-active tasks close.
	if returned.Status != workflow.InstanceStatusRunning || returned.CurrentNodeID != request.TargetNodeID {
		t.Errorf("returned status/node = %q/%q, want running/%q", returned.Status, returned.CurrentNodeID, request.TargetNodeID)
	}
	if returned.Version != instance.Version+1 {
		t.Errorf("returned.Version = %d, want %d", returned.Version, instance.Version+1)
	}
	if len(returned.Tasks) != len(oldTasks)+1 {
		t.Fatalf("len(returned.Tasks) = %d, want %d", len(returned.Tasks), len(oldTasks)+1)
	}
	if returned.Tasks[0] != oldTasks[0] {
		t.Errorf("historical decided task = %#v, want unchanged %#v", returned.Tasks[0], oldTasks[0])
	}
	for index := 1; index < len(oldTasks); index++ {
		want := oldTasks[index]
		want.Status = workflow.TaskStatusClosed
		if returned.Tasks[index] != want {
			t.Errorf("source task %d = %#v, want %#v", index, returned.Tasks[index], want)
		}
	}
	newTask := returned.Tasks[len(returned.Tasks)-1]
	if newTask.ID == oldTasks[0].ID || newTask.NodeID != request.TargetNodeID ||
		newTask.Assignee != "reviewer-a" || newTask.Status != workflow.TaskStatusActive {
		t.Errorf("new task round = %#v, want fresh active first-approval assignment", newTask)
	}

	// Return audit follows the immutable prefix and precedes target entry with complete causal metadata.
	if len(returned.Audit) != len(oldAudit)+2 || !slices.Equal(returned.Audit[:len(oldAudit)], oldAudit) {
		t.Fatalf("returned audit = %#v, want unchanged prefix plus return and entry records", returned.Audit)
	}
	returnAudit := returned.Audit[len(oldAudit)]
	if returnAudit.Action != "instance.returned" || returnAudit.ActorID != request.ActorID ||
		returnAudit.NodeID != "second-approval" || returnAudit.TargetNodeID != request.TargetNodeID ||
		returnAudit.Reason != request.Reason || returnAudit.NodeState != `{"round":"second"}` ||
		returnAudit.At.IsZero() {
		t.Errorf("return audit = %#v, want actor, source, target, reason, and timestamp", returnAudit)
	}
	if entered := returned.Audit[len(oldAudit)+1]; entered.Action != "node.entered" || entered.NodeID != request.TargetNodeID {
		t.Errorf("target entry audit = %#v, want node.entered for %q", entered, request.TargetNodeID)
	}
}

// TestEngineReturnRejectsInvalidTargets verifies graph and history constraints run before host policy.
func TestEngineReturnRejectsInvalidTargets(t *testing.T) {
	t.Parallel()

	targets := []string{"start", "end", "second-approval", "missing", "unvisited-approval"}
	for _, target := range targets {
		t.Run(target, func(t *testing.T) {
			t.Parallel()

			store := workflow.NewMemoryStore()
			engine, instance := startReturnFlow(t, store, workflow.InstanceID("invalid-target-"+target))
			policy := returnPolicyFunc(func(context.Context, workflow.ReturnRequest, *workflow.Instance) error {
				t.Fatal("AuthorizeReturn() called for invalid target")
				return nil
			})

			_, err := engine.Return(t.Context(), workflow.ReturnRequest{
				InstanceID:   instance.ID,
				ActorID:      "operator-a",
				TargetNodeID: target,
				Reason:       "target validation",
			}, policy)
			if !errors.Is(err, workflow.ErrInvalidReturnTarget) {
				t.Fatalf("Return() error = %v, want ErrInvalidReturnTarget", err)
			}
			stored, loadErr := store.Load(t.Context(), instance.ID)
			if loadErr != nil {
				t.Fatalf("Load() error = %v", loadErr)
			}
			if !reflect.DeepEqual(stored, instance) {
				t.Errorf("stored instance = %#v, want unchanged %#v", stored, instance)
			}
		})
	}
}

// TestEngineReturnRejectsUnauthorizedActor verifies policy denial and mutation cannot alter durable state.
func TestEngineReturnRejectsUnauthorizedActor(t *testing.T) {
	t.Parallel()

	store := workflow.NewMemoryStore()
	engine, instance := startReturnFlow(t, store, "unauthorized-return")
	policy := returnPolicyFunc(func(
		_ context.Context,
		_ workflow.ReturnRequest,
		snapshot *workflow.Instance,
	) error {
		snapshot.CurrentNodeID = "first-approval"
		snapshot.Tasks = nil
		snapshot.Audit = nil
		return errReturnDenied
	})

	_, err := engine.Return(t.Context(), workflow.ReturnRequest{
		InstanceID:   instance.ID,
		ActorID:      instance.Initiator,
		TargetNodeID: "first-approval",
		Reason:       "claimed initiator is insufficient",
	}, policy)
	if !errors.Is(err, errReturnDenied) {
		t.Fatalf("Return() error = %v, want host policy error", err)
	}
	stored, loadErr := store.Load(t.Context(), instance.ID)
	if loadErr != nil {
		t.Fatalf("Load() error = %v", loadErr)
	}
	if !reflect.DeepEqual(stored, instance) {
		t.Errorf("stored instance = %#v, want unchanged %#v", stored, instance)
	}
}

// TestEngineReturnPreservesSnapshotOnVersionConflict verifies stale CAS cannot partially create a task round.
func TestEngineReturnPreservesSnapshotOnVersionConflict(t *testing.T) {
	t.Parallel()

	memoryStore := workflow.NewMemoryStore()
	store := &versionConflictStore{Store: memoryStore}
	engine, instance := startReturnFlow(t, store, "conflicting-return")
	store.rejectSaves = true
	store.saveCalls = 0
	policy := returnPolicyFunc(func(context.Context, workflow.ReturnRequest, *workflow.Instance) error { return nil })

	_, err := engine.Return(t.Context(), workflow.ReturnRequest{
		InstanceID:   instance.ID,
		ActorID:      "operator-a",
		TargetNodeID: "first-approval",
		Reason:       "retry earlier review",
	}, policy)
	if !errors.Is(err, workflow.ErrVersionConflict) {
		t.Fatalf("Return() error = %v, want ErrVersionConflict", err)
	}
	if store.saveCalls != 1 {
		t.Errorf("Save() calls = %d, want 1", store.saveCalls)
	}
	stored, loadErr := memoryStore.Load(t.Context(), instance.ID)
	if loadErr != nil {
		t.Fatalf("Load() error = %v", loadErr)
	}
	if !reflect.DeepEqual(stored, instance) {
		t.Errorf("stored instance = %#v, want unchanged %#v", stored, instance)
	}
}

// TestEngineReturnsAlongExecutedConditionBranch verifies return history remains correct after explicit branching.
func TestEngineReturnsAlongExecutedConditionBranch(t *testing.T) {
	t.Parallel()

	// Compose Approval and Condition so one frozen Definition contains both taken and untaken business branches.
	store := workflow.NewMemoryStore()
	registry := workflow.NewRegistry()
	if err := registry.Register(approval.Kind, approval.NewHandler()); err != nil {
		t.Fatalf("Register(approval) error = %v", err)
	}
	if err := registry.Register(condition.Kind, condition.NewHandler()); err != nil {
		t.Fatalf("Register(condition) error = %v", err)
	}
	engine := workflow.NewEngine(store, registry)
	builder := workflow.NewBuilder("branched-return")
	builder.Start("start")
	builder.Node("initial-approval", approval.Kind, approval.Config{
		Mode: approval.ModeAny, Assignees: []workflow.ActorID{"reviewer-initial"},
	})
	builder.Node("route", condition.Kind, condition.Config{
		Rules: []condition.Rule{{
			Match:   condition.MatchAll,
			Outcome: "a",
			Conditions: []condition.Expression{{
				Field: "/route", Type: condition.TypeString, Operator: condition.OperatorEqual, Value: "a",
			}},
		}},
		DefaultOutcome: "b",
	})
	builder.Node("branch-a", approval.Kind, approval.Config{
		Mode: approval.ModeAny, Assignees: []workflow.ActorID{"reviewer-a"},
	})
	builder.Node("branch-b", approval.Kind, approval.Config{
		Mode: approval.ModeAny, Assignees: []workflow.ActorID{"reviewer-b"},
	})
	builder.Node("final-approval", approval.Kind, approval.Config{
		Mode: approval.ModeAny, Assignees: []workflow.ActorID{"reviewer-final"},
	})
	builder.End("end")
	builder.Connect("start", "initial-approval", "")
	builder.Connect("initial-approval", "route", approval.OutcomeApproved)
	builder.Connect("route", "branch-a", "a")
	builder.Connect("route", "branch-b", "b")
	builder.Connect("branch-a", "final-approval", approval.OutcomeApproved)
	builder.Connect("branch-b", "final-approval", approval.OutcomeApproved)
	builder.Connect("final-approval", "end", approval.OutcomeApproved)
	definition, err := builder.Build()
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	instance, err := engine.Start(t.Context(), definition, workflow.StartRequest{
		ID: "branched-return-1", Initiator: "employee-a", Data: []byte(`{"route":"a"}`),
	})
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	// Approve the initial and selected branch tasks so final approval becomes the return source.
	for _, taskIndex := range []int{0, 1} {
		instance, err = engine.Handle(t.Context(), workflow.Command{
			InstanceID: instance.ID,
			TaskID:     instance.Tasks[taskIndex].ID,
			ActorID:    instance.Tasks[taskIndex].Assignee,
			Name:       approval.CommandApprove,
		})
		if err != nil {
			t.Fatalf("Handle(task %d) error = %v", taskIndex, err)
		}
	}
	oldTaskCount := len(instance.Tasks)
	policy := returnPolicyFunc(func(context.Context, workflow.ReturnRequest, *workflow.Instance) error { return nil })
	_, err = engine.Return(t.Context(), workflow.ReturnRequest{
		InstanceID: instance.ID, ActorID: "operator-a", TargetNodeID: "route", Reason: "invalid control evaluation target",
	}, policy)
	if !errors.Is(err, workflow.ErrInvalidReturnTarget) {
		t.Fatalf("Return(condition target) error = %v, want ErrInvalidReturnTarget", err)
	}
	returned, err := engine.Return(t.Context(), workflow.ReturnRequest{
		InstanceID: instance.ID, ActorID: "operator-a", TargetNodeID: "branch-a", Reason: "repeat selected review",
	}, policy)
	if err != nil {
		t.Fatalf("Return() error = %v", err)
	}

	// The taken branch receives a fresh round while the untaken branch remains absent from task history.
	if returned.CurrentNodeID != "branch-a" || len(returned.Tasks) != oldTaskCount+1 {
		t.Fatalf("returned node/tasks = %q/%d, want branch-a/%d", returned.CurrentNodeID, len(returned.Tasks), oldTaskCount+1)
	}
	for _, task := range returned.Tasks {
		if task.NodeID == "branch-b" {
			t.Errorf("unexpected task for unexecuted branch: %#v", task)
		}
	}
	newTask := returned.Tasks[len(returned.Tasks)-1]
	if newTask.NodeID != "branch-a" || newTask.Status != workflow.TaskStatusActive {
		t.Errorf("new branch task = %#v, want active branch-a task", newTask)
	}
}

// TestEngineReturnsAfterRejectedOutcomeRoute verifies rejected history survives a return from its routed node.
func TestEngineReturnsAfterRejectedOutcomeRoute(t *testing.T) {
	t.Parallel()

	// Build the explicit rejected route introduced by Approval without exposing either target to the command.
	store := workflow.NewMemoryStore()
	registry := workflow.NewRegistry()
	if err := registry.Register(approval.Kind, approval.NewHandler()); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	engine := workflow.NewEngine(store, registry)
	builder := workflow.NewBuilder("rejected-route-return")
	builder.Start("start")
	builder.Node("manager-approval", approval.Kind, approval.Config{
		Mode: approval.ModeAny, Assignees: []workflow.ActorID{"manager-a"}, RejectedOutcome: approval.OutcomeRejected,
	})
	builder.Node("correction", approval.Kind, approval.Config{
		Mode: approval.ModeAll, Assignees: []workflow.ActorID{"employee-a", "employee-b"},
	})
	builder.End("end")
	builder.Connect("start", "manager-approval", "")
	builder.Connect("manager-approval", "end", approval.OutcomeApproved)
	builder.Connect("manager-approval", "correction", approval.OutcomeRejected)
	builder.Connect("correction", "end", approval.OutcomeApproved)
	definition, err := builder.Build()
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	instance, err := engine.Start(t.Context(), definition, workflow.StartRequest{
		ID: "rejected-route-return-1", Initiator: "employee-a",
	})
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	instance, err = engine.Handle(t.Context(), workflow.Command{
		InstanceID: instance.ID,
		TaskID:     instance.Tasks[0].ID,
		ActorID:    instance.Tasks[0].Assignee,
		Name:       approval.CommandReject,
	})
	if err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	rejectedTask := instance.Tasks[0]
	oldTaskCount := len(instance.Tasks)

	// Return from correction to the explicitly named historical approval node.
	policy := returnPolicyFunc(func(context.Context, workflow.ReturnRequest, *workflow.Instance) error { return nil })
	returned, err := engine.Return(t.Context(), workflow.ReturnRequest{
		InstanceID: instance.ID, ActorID: "operator-a", TargetNodeID: "manager-approval", Reason: "manager re-review",
	}, policy)
	if err != nil {
		t.Fatalf("Return() error = %v", err)
	}
	if returned.Tasks[0] != rejectedTask {
		t.Errorf("rejected task = %#v, want unchanged %#v", returned.Tasks[0], rejectedTask)
	}
	if len(returned.Tasks) != oldTaskCount+1 {
		t.Fatalf("len(returned.Tasks) = %d, want %d", len(returned.Tasks), oldTaskCount+1)
	}
	for _, task := range returned.Tasks[1:oldTaskCount] {
		if task.Status != workflow.TaskStatusClosed {
			t.Errorf("correction task = %#v, want closed", task)
		}
	}
	newTask := returned.Tasks[len(returned.Tasks)-1]
	if newTask.NodeID != "manager-approval" || newTask.Status != workflow.TaskStatusActive {
		t.Errorf("new manager task = %#v, want active manager-approval task", newTask)
	}
}

// TestEngineReturnValidatesBoundaryInput verifies missing dependencies and request fields fail before Load.
func TestEngineReturnValidatesBoundaryInput(t *testing.T) {
	t.Parallel()

	policy := returnPolicyFunc(func(context.Context, workflow.ReturnRequest, *workflow.Instance) error { return nil })
	validRequest := workflow.ReturnRequest{
		InstanceID: "instance", ActorID: "actor", TargetNodeID: "target", Reason: "reason",
	}
	tests := []struct {
		name    string
		engine  *workflow.Engine
		request workflow.ReturnRequest
		policy  workflow.ReturnPolicy
		wantErr error
	}{
		{name: "nil engine", request: validRequest, policy: policy, wantErr: workflow.ErrInvalidEngine},
		{
			name: "empty instance", engine: workflow.NewEngine(workflow.NewMemoryStore(), workflow.NewRegistry()),
			request: workflow.ReturnRequest{ActorID: "actor", TargetNodeID: "target", Reason: "reason"},
			policy:  policy, wantErr: workflow.ErrInvalidReturnRequest,
		},
		{
			name: "empty actor", engine: workflow.NewEngine(workflow.NewMemoryStore(), workflow.NewRegistry()),
			request: workflow.ReturnRequest{InstanceID: "instance", TargetNodeID: "target", Reason: "reason"},
			policy:  policy, wantErr: workflow.ErrInvalidReturnRequest,
		},
		{
			name: "empty target", engine: workflow.NewEngine(workflow.NewMemoryStore(), workflow.NewRegistry()),
			request: workflow.ReturnRequest{InstanceID: "instance", ActorID: "actor", Reason: "reason"},
			policy:  policy, wantErr: workflow.ErrInvalidReturnRequest,
		},
		{
			name: "blank reason", engine: workflow.NewEngine(workflow.NewMemoryStore(), workflow.NewRegistry()),
			request: workflow.ReturnRequest{InstanceID: "instance", ActorID: "actor", TargetNodeID: "target", Reason: " \t"},
			policy:  policy, wantErr: workflow.ErrInvalidReturnRequest,
		},
		{
			name: "nil policy", engine: workflow.NewEngine(workflow.NewMemoryStore(), workflow.NewRegistry()),
			request: validRequest, wantErr: workflow.ErrInvalidReturnRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := tt.engine.Return(t.Context(), tt.request, tt.policy)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("Return() error = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

// TestEngineReturnRejectsTerminalInstances verifies lifecycle return accepts only running aggregates.
func TestEngineReturnRejectsTerminalInstances(t *testing.T) {
	t.Parallel()

	for _, status := range []workflow.InstanceStatus{
		workflow.InstanceStatusCompleted,
		workflow.InstanceStatusRejected,
		workflow.InstanceStatusWithdrawn,
	} {
		t.Run(string(status), func(t *testing.T) {
			t.Parallel()

			store := workflow.NewMemoryStore()
			instance := &workflow.Instance{
				ID: workflow.InstanceID("terminal-return-" + status), Status: status, CurrentNodeID: "end", Version: 2,
			}
			if err := store.Create(t.Context(), instance); err != nil {
				t.Fatalf("Create() error = %v", err)
			}
			policy := returnPolicyFunc(func(context.Context, workflow.ReturnRequest, *workflow.Instance) error {
				t.Fatal("AuthorizeReturn() called for terminal instance")
				return nil
			})

			_, err := workflow.NewEngine(store, workflow.NewRegistry()).Return(t.Context(), workflow.ReturnRequest{
				InstanceID: instance.ID, ActorID: "operator-a", TargetNodeID: "target", Reason: "terminal check",
			}, policy)
			if !errors.Is(err, workflow.ErrInstanceNotRunning) {
				t.Fatalf("Return() error = %v, want ErrInstanceNotRunning", err)
			}
		})
	}
}

// TestEngineConcurrentReturnsCommitOnce verifies competing returns share one aggregate CAS winner.
func TestEngineConcurrentReturnsCommitOnce(t *testing.T) {
	t.Parallel()

	store := workflow.NewMemoryStore()
	engine, instance := startReturnFlow(t, store, "concurrent-return")
	ready := make(chan struct{}, 2)
	release := make(chan struct{})
	policy := returnPolicyFunc(func(context.Context, workflow.ReturnRequest, *workflow.Instance) error {
		ready <- struct{}{}
		<-release
		return nil
	})
	request := workflow.ReturnRequest{
		InstanceID: instance.ID, ActorID: "operator-a", TargetNodeID: "first-approval", Reason: "concurrent retry",
	}
	type result struct {
		instance *workflow.Instance
		err      error
	}
	results := make(chan result, 2)

	// Hold both commands after their Load and history validation so they race with the same expected version.
	for range 2 {
		go func() {
			returned, err := engine.Return(t.Context(), request, policy)
			results <- result{instance: returned, err: err}
		}()
	}
	<-ready
	<-ready
	close(release)

	// Exactly one candidate becomes durable; the loser observes the Store's stable version-conflict sentinel.
	var committed *workflow.Instance
	conflicts := 0
	for range 2 {
		outcome := <-results
		switch {
		case outcome.err == nil:
			if committed != nil {
				t.Fatal("Return() succeeded more than once")
			}
			committed = outcome.instance
		case errors.Is(outcome.err, workflow.ErrVersionConflict):
			conflicts++
		default:
			t.Fatalf("Return() error = %v, want nil or ErrVersionConflict", outcome.err)
		}
	}
	if committed == nil || conflicts != 1 {
		t.Fatalf("committed = %v, conflicts = %d; want one of each", committed != nil, conflicts)
	}
	stored, err := store.Load(t.Context(), instance.ID)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if !reflect.DeepEqual(stored, committed) {
		t.Errorf("stored instance = %#v, want committed %#v", stored, committed)
	}
}
