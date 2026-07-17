// This file verifies instance withdrawal through Engine's public lifecycle API.
// It treats Store, authorization policy, tasks, and audit as observable boundaries without inspecting internals.
package workflow_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"testing"

	workflow "github.com/lvpeng/easy-workflow"
	"github.com/lvpeng/easy-workflow/approval"
)

var (
	// errWithdrawalDenied is the host-owned sentinel returned by authorization-denial tests.
	errWithdrawalDenied = errors.New("host: withdrawal denied")
)

// withdrawalPolicyFunc adapts a test function to the host withdrawal authorization contract.
type withdrawalPolicyFunc func(context.Context, workflow.ActorID, *workflow.Instance) error

// versionConflictStore records the candidate Save and rejects it as a stale aggregate without mutating storage.
type versionConflictStore struct {
	// Store supplies the unchanged Create and Load behavior used to inspect failure atomicity.
	workflow.Store

	// saveCalls counts aggregate CAS attempts made through the wrapper.
	saveCalls int
	// rejectSaves switches the wrapper from pass-through setup behavior to deterministic CAS failure.
	rejectSaves bool
}

// AuthorizeWithdrawal delegates one authorization decision while preserving its returned error.
func (f withdrawalPolicyFunc) AuthorizeWithdrawal(
	ctx context.Context,
	actor workflow.ActorID,
	instance *workflow.Instance,
) error {
	return f(ctx, actor, instance)
}

// Save delegates setup writes until rejectSaves is enabled, then simulates a compare-and-swap loss.
//
// The wrapper counts every attempt. Pass-through failures retain their Store sentinel through wrapping; rejection
// returns ErrVersionConflict without mutating the embedded Store so lifecycle atomicity can be asserted afterward.
func (s *versionConflictStore) Save(
	ctx context.Context,
	instance *workflow.Instance,
	expectedVersion uint64,
) error {
	s.saveCalls++
	if !s.rejectSaves {
		if err := s.Store.Save(ctx, instance, expectedVersion); err != nil {
			return fmt.Errorf("save setup aggregate: %w", err)
		}
		return nil
	}
	return workflow.ErrVersionConflict
}

// startWithdrawableInstance creates one running countersign instance through the public Engine API.
//
// store must be a fresh adapter. id identifies the isolated aggregate. The returned Engine shares store and
// has the official Approval handler registered; failures stop the calling test.
func startWithdrawableInstance(
	t *testing.T,
	store workflow.Store,
	id workflow.InstanceID,
) (*workflow.Engine, *workflow.Instance) {
	t.Helper()

	// Register Approval and build one node with two active assignments for task-closure assertions.
	registry := workflow.NewRegistry()
	if err := registry.Register(approval.Kind, approval.NewHandler()); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	engine := workflow.NewEngine(store, registry)
	builder := workflow.NewBuilder("withdrawal")
	builder.Start("start")
	builder.Node("approval", approval.Kind, approval.Config{
		Mode:      approval.ModeAll,
		Assignees: []workflow.ActorID{"reviewer-a", "reviewer-b", "reviewer-c"},
	})
	builder.End("end")
	builder.Connect("start", "approval", "")
	builder.Connect("approval", "end", approval.OutcomeApproved)
	definition, err := builder.Build()
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}

	// Start persists the baseline snapshot consumed by each withdrawal scenario.
	started, err := engine.Start(t.Context(), definition, workflow.StartRequest{
		ID:        id,
		Initiator: "employee-a",
	})
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	return engine, started
}

// TestEngineWithdrawsAuthorizedRunningInstance verifies the complete accepted withdrawal transition.
func TestEngineWithdrawsAuthorizedRunningInstance(t *testing.T) {
	t.Parallel()

	// Start a countersign node so withdrawal must close more than one active assignment atomically.
	store := workflow.NewMemoryStore()
	engine, started := startWithdrawableInstance(t, store, "withdrawal-1")
	started, err := engine.Handle(t.Context(), workflow.Command{
		InstanceID: started.ID,
		TaskID:     started.Tasks[0].ID,
		ActorID:    started.Tasks[0].Assignee,
		Name:       approval.CommandApprove,
	})
	if err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	originalAudit := slices.Clone(started.Audit)

	// Host policy receives trusted actor identity and the current pre-transition snapshot.
	policy := withdrawalPolicyFunc(func(
		_ context.Context,
		actor workflow.ActorID,
		instance *workflow.Instance,
	) error {
		if actor != "operator-a" {
			t.Errorf("policy actor = %q, want %q", actor, "operator-a")
		}
		if instance.Status != workflow.InstanceStatusRunning || instance.Version != started.Version {
			t.Errorf("policy instance = %#v, want running version %d", instance, started.Version)
		}
		return nil
	})
	withdrawn, err := engine.Withdraw(t.Context(), workflow.WithdrawRequest{
		InstanceID: started.ID,
		ActorID:    "operator-a",
	}, policy)
	if err != nil {
		t.Fatalf("Withdraw() error = %v", err)
	}

	// The accepted transition preserves audit history, closes every active task, and advances one CAS version.
	if withdrawn.Status != workflow.InstanceStatusWithdrawn {
		t.Errorf("withdrawn.Status = %q, want %q", withdrawn.Status, workflow.InstanceStatusWithdrawn)
	}
	if withdrawn.Version != started.Version+1 {
		t.Errorf("withdrawn.Version = %d, want %d", withdrawn.Version, started.Version+1)
	}
	if withdrawn.Tasks[0].Status != workflow.TaskStatusCompleted ||
		withdrawn.Tasks[0].Outcome != approval.OutcomeApproved {
		t.Errorf("decided task = %#v, want preserved completed approval", withdrawn.Tasks[0])
	}
	for _, task := range withdrawn.Tasks[1:] {
		if task.Status != workflow.TaskStatusClosed {
			t.Errorf("task %q status = %q, want %q", task.ID, task.Status, workflow.TaskStatusClosed)
		}
	}
	if len(withdrawn.Audit) != len(originalAudit)+1 || !slices.Equal(withdrawn.Audit[:len(originalAudit)], originalAudit) {
		t.Fatalf("withdrawn audit = %#v, want unchanged prefix plus one record", withdrawn.Audit)
	}
	lastAudit := withdrawn.Audit[len(withdrawn.Audit)-1]
	if lastAudit.Action != "instance.withdrawn" || lastAudit.ActorID != "operator-a" ||
		lastAudit.NodeID != started.CurrentNodeID || lastAudit.At.IsZero() {
		t.Errorf("withdrawal audit = %#v, want action, actor, node, and timestamp", lastAudit)
	}

	// The returned aggregate and durable snapshot agree, and the old task cannot accept another command.
	stored, err := store.Load(t.Context(), started.ID)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if !reflect.DeepEqual(stored, withdrawn) {
		t.Errorf("stored instance = %#v, want %#v", stored, withdrawn)
	}
	_, err = engine.Handle(t.Context(), workflow.Command{
		InstanceID: started.ID,
		TaskID:     started.Tasks[1].ID,
		ActorID:    started.Tasks[1].Assignee,
		Name:       approval.CommandApprove,
	})
	if !errors.Is(err, workflow.ErrInvalidCommand) {
		t.Errorf("Handle() error = %v, want ErrInvalidCommand", err)
	}
}

// TestEngineWithdrawValidatesBoundaryInput verifies missing dependencies, identities, and policy fail before Load.
func TestEngineWithdrawValidatesBoundaryInput(t *testing.T) {
	t.Parallel()

	policy := withdrawalPolicyFunc(func(context.Context, workflow.ActorID, *workflow.Instance) error { return nil })
	tests := []struct {
		name    string
		engine  *workflow.Engine
		request workflow.WithdrawRequest
		policy  workflow.WithdrawalPolicy
		wantErr error
	}{
		{
			name:    "nil engine",
			request: workflow.WithdrawRequest{InstanceID: "instance", ActorID: "actor"},
			policy:  policy,
			wantErr: workflow.ErrInvalidEngine,
		},
		{
			name:    "empty instance",
			engine:  workflow.NewEngine(workflow.NewMemoryStore(), nil),
			request: workflow.WithdrawRequest{ActorID: "actor"},
			policy:  policy,
			wantErr: workflow.ErrInvalidWithdrawRequest,
		},
		{
			name:    "empty actor",
			engine:  workflow.NewEngine(workflow.NewMemoryStore(), nil),
			request: workflow.WithdrawRequest{InstanceID: "instance"},
			policy:  policy,
			wantErr: workflow.ErrInvalidWithdrawRequest,
		},
		{
			name:    "nil policy",
			engine:  workflow.NewEngine(workflow.NewMemoryStore(), nil),
			request: workflow.WithdrawRequest{InstanceID: "instance", ActorID: "actor"},
			wantErr: workflow.ErrInvalidWithdrawRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := tt.engine.Withdraw(t.Context(), tt.request, tt.policy)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("Withdraw() error = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

// TestEngineWithdrawRejectsUnauthorizedActor verifies policy denial and mutation cannot alter durable state.
func TestEngineWithdrawRejectsUnauthorizedActor(t *testing.T) {
	t.Parallel()

	store := workflow.NewMemoryStore()
	engine, started := startWithdrawableInstance(t, store, "unauthorized-withdrawal")

	// Mutating the policy snapshot proves authorization cannot modify Engine's pending or durable aggregate.
	policy := withdrawalPolicyFunc(func(
		_ context.Context,
		_ workflow.ActorID,
		instance *workflow.Instance,
	) error {
		instance.Status = workflow.InstanceStatusWithdrawn
		instance.Tasks[0].Status = workflow.TaskStatusClosed
		instance.Audit = nil
		return errWithdrawalDenied
	})
	_, err := engine.Withdraw(t.Context(), workflow.WithdrawRequest{
		InstanceID: started.ID,
		ActorID:    started.Initiator,
	}, policy)
	if !errors.Is(err, errWithdrawalDenied) {
		t.Fatalf("Withdraw() error = %v, want host policy error", err)
	}

	// A claimed initiator identity alone grants nothing and the stored snapshot remains byte-for-byte equivalent.
	stored, err := store.Load(t.Context(), started.ID)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if !reflect.DeepEqual(stored, started) {
		t.Errorf("stored instance = %#v, want unchanged %#v", stored, started)
	}
}

// TestEngineWithdrawRejectsTerminalInstances verifies every non-running lifecycle state has one stable error.
func TestEngineWithdrawRejectsTerminalInstances(t *testing.T) {
	t.Parallel()

	for _, status := range []workflow.InstanceStatus{
		workflow.InstanceStatusCompleted,
		workflow.InstanceStatusRejected,
		workflow.InstanceStatusWithdrawn,
	} {
		t.Run(string(status), func(t *testing.T) {
			t.Parallel()

			// Store a terminal aggregate directly because withdrawal must reject it before policy evaluation.
			store := workflow.NewMemoryStore()
			instance := &workflow.Instance{
				ID:            workflow.InstanceID("terminal-" + status),
				Status:        status,
				CurrentNodeID: "end",
				Version:       2,
			}
			if err := store.Create(t.Context(), instance); err != nil {
				t.Fatalf("Create() error = %v", err)
			}
			policy := withdrawalPolicyFunc(func(context.Context, workflow.ActorID, *workflow.Instance) error {
				t.Fatal("AuthorizeWithdrawal() called for terminal instance")
				return nil
			})

			_, err := workflow.NewEngine(store, nil).Withdraw(t.Context(), workflow.WithdrawRequest{
				InstanceID: instance.ID,
				ActorID:    "operator-a",
			}, policy)
			if !errors.Is(err, workflow.ErrInstanceNotRunning) {
				t.Fatalf("Withdraw() error = %v, want ErrInstanceNotRunning", err)
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

// TestEngineWithdrawPreservesSnapshotOnVersionConflict verifies stale CAS cannot partially withdraw an instance.
func TestEngineWithdrawPreservesSnapshotOnVersionConflict(t *testing.T) {
	t.Parallel()

	// The wrapper exposes normal Create and Load behavior but rejects Engine's sole withdrawal Save.
	memoryStore := workflow.NewMemoryStore()
	store := &versionConflictStore{Store: memoryStore, rejectSaves: true}
	engine, started := startWithdrawableInstance(t, store, "conflicting-withdrawal")
	policy := withdrawalPolicyFunc(func(context.Context, workflow.ActorID, *workflow.Instance) error { return nil })

	_, err := engine.Withdraw(t.Context(), workflow.WithdrawRequest{
		InstanceID: started.ID,
		ActorID:    "operator-a",
	}, policy)
	if !errors.Is(err, workflow.ErrVersionConflict) {
		t.Fatalf("Withdraw() error = %v, want ErrVersionConflict", err)
	}
	if store.saveCalls != 1 {
		t.Errorf("Save() calls = %d, want 1", store.saveCalls)
	}
	stored, loadErr := memoryStore.Load(t.Context(), started.ID)
	if loadErr != nil {
		t.Fatalf("Load() error = %v", loadErr)
	}
	if !reflect.DeepEqual(stored, started) {
		t.Errorf("stored instance = %#v, want unchanged %#v", stored, started)
	}
}
