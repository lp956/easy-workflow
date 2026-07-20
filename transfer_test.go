// This file verifies active-task transfer through Engine's public lifecycle API.
// It observes host authorization, assignment history, audit metadata, and aggregate atomicity without internals.
package workflow_test

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"testing"

	workflow "github.com/lvpeng/easy-workflow"
	"github.com/lvpeng/easy-workflow/approval"
)

var (
	// errTransferDenied is the host-owned sentinel returned by transfer authorization-denial tests.
	errTransferDenied = errors.New("host: transfer denied")
)

// transferPolicyFunc adapts a test function to the host transfer authorization contract.
type transferPolicyFunc func(context.Context, workflow.TransferRequest, workflow.Task, *workflow.Instance) error

// AuthorizeTransfer delegates one transfer authorization decision while preserving its returned error.
func (f transferPolicyFunc) AuthorizeTransfer(
	ctx context.Context,
	request workflow.TransferRequest,
	task workflow.Task,
	instance *workflow.Instance,
) error {
	return f(ctx, request, task, instance)
}

// startTransferableInstance creates one running approval instance through the public Engine API.
//
// store must be fresh and id unique within it. mode controls whether the active assignment set uses or-sign or
// countersign semantics; assignees must satisfy Approval's frozen assignment contract.
func startTransferableInstance(
	t *testing.T,
	store workflow.Store,
	id workflow.InstanceID,
	mode approval.Mode,
	assignees []workflow.ActorID,
) (*workflow.Engine, *workflow.Instance) {
	t.Helper()

	// Register the official Approval handler and build one immutable approval round.
	registry := workflow.NewRegistry()
	if err := registry.Register(approval.Kind, approval.NewHandler()); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	engine := workflow.NewEngine(store, registry)
	builder := workflow.NewBuilder("task-transfer")
	builder.Start("start")
	builder.Node("approval", approval.Kind, approval.Config{Mode: mode, Assignees: assignees})
	builder.End("end")
	builder.Connect("start", "approval", "")
	builder.Connect("approval", "end", approval.OutcomeApproved)
	definition, err := builder.Build()
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}

	// Start persists the active assignment snapshot consumed by each transfer scenario.
	instance, err := engine.Start(t.Context(), definition, workflow.StartRequest{ID: id, Initiator: "employee-a"})
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	return engine, instance
}

// TestEngineTransfersActiveOrSignTask verifies an authorized transfer replaces ownership with a new assignment.
func TestEngineTransfersActiveOrSignTask(t *testing.T) {
	t.Parallel()

	memoryStore := workflow.NewMemoryStore()
	store := &versionConflictStore{Store: memoryStore}
	engine, started := startTransferableInstance(
		t,
		store,
		"transfer-any",
		approval.ModeAny,
		[]workflow.ActorID{"reviewer-a"},
	)
	originalDefinition := started.Definition
	originalAudit := slices.Clone(started.Audit)
	oldTask := started.Tasks[0]
	request := workflow.TransferRequest{
		InstanceID:  started.ID,
		TaskID:      oldTask.ID,
		ActorID:     oldTask.Assignee,
		NewAssignee: "reviewer-b",
		Reason:      "cover planned leave",
	}

	// Policy receives the trusted request, current assignment, and detached pre-transition aggregate.
	policy := transferPolicyFunc(func(
		_ context.Context,
		gotRequest workflow.TransferRequest,
		gotTask workflow.Task,
		gotInstance *workflow.Instance,
	) error {
		if gotRequest != request || gotTask != oldTask {
			t.Errorf("policy input = (%#v, %#v), want (%#v, %#v)", gotRequest, gotTask, request, oldTask)
		}
		if gotInstance.Version != started.Version || gotInstance.CurrentNodeID != oldTask.NodeID {
			t.Errorf("policy instance = %#v, want active source snapshot", gotInstance)
		}
		return nil
	})
	transferred, err := engine.Transfer(t.Context(), request, policy)
	if err != nil {
		t.Fatalf("Transfer() error = %v", err)
	}

	// Transfer closes the historical assignment, appends a fresh active assignment, and advances one CAS version.
	if transferred.Version != started.Version+1 || len(transferred.Tasks) != 2 {
		t.Fatalf("transferred version/tasks = %d/%d, want %d/2", transferred.Version, len(transferred.Tasks), started.Version+1)
	}
	if store.saveCalls != 1 {
		t.Errorf("Save() calls = %d, want one aggregate CAS", store.saveCalls)
	}
	wantOldTask := oldTask
	wantOldTask.Status = workflow.TaskStatusClosed
	if transferred.Tasks[0] != wantOldTask {
		t.Errorf("old task = %#v, want closed history %#v", transferred.Tasks[0], wantOldTask)
	}
	newTask := transferred.Tasks[1]
	if newTask.ID == "" || newTask.ID == oldTask.ID || newTask.NodeID != oldTask.NodeID ||
		newTask.Assignee != request.NewAssignee || newTask.Status != workflow.TaskStatusActive {
		t.Errorf("new task = %#v, want fresh active assignment for %q", newTask, request.NewAssignee)
	}
	if !reflect.DeepEqual(transferred.Definition, originalDefinition) {
		t.Errorf("Definition changed during transfer")
	}

	// The appended audit record carries complete transfer attribution without rewriting its immutable prefix.
	if len(transferred.Audit) != len(originalAudit)+1 || !slices.Equal(transferred.Audit[:len(originalAudit)], originalAudit) {
		t.Fatalf("transfer audit = %#v, want unchanged prefix plus one record", transferred.Audit)
	}
	audit := transferred.Audit[len(originalAudit)]
	if audit.Action != "task.transferred" || audit.InstanceID != started.ID || audit.NodeID != oldTask.NodeID ||
		audit.TaskID != oldTask.ID || audit.ActorID != request.ActorID || audit.PreviousAssignee != oldTask.Assignee ||
		audit.NewAssignee != request.NewAssignee || audit.Reason != request.Reason || audit.At.IsZero() {
		t.Errorf("transfer audit = %#v, want complete transfer attribution", audit)
	}
}

// TestEngineTransfersCountersignTask verifies replacement ownership participates in the existing approval round.
func TestEngineTransfersCountersignTask(t *testing.T) {
	t.Parallel()

	store := workflow.NewMemoryStore()
	engine, started := startTransferableInstance(
		t,
		store,
		"transfer-all",
		approval.ModeAll,
		[]workflow.ActorID{"reviewer-a", "reviewer-b"},
	)
	oldTask := started.Tasks[0]
	transferred, err := engine.Transfer(t.Context(), workflow.TransferRequest{
		InstanceID:  started.ID,
		TaskID:      oldTask.ID,
		ActorID:     "team-lead",
		NewAssignee: "reviewer-c",
		Reason:      "rebalance active review",
	}, transferPolicyFunc(func(context.Context, workflow.TransferRequest, workflow.Task, *workflow.Instance) error {
		return nil
	}))
	if err != nil {
		t.Fatalf("Transfer() error = %v", err)
	}
	newTask := transferred.Tasks[len(transferred.Tasks)-1]

	// Historical ownership cannot act after transfer, while the replacement can contribute the required decision.
	_, err = engine.Handle(t.Context(), workflow.Command{
		InstanceID: transferred.ID,
		TaskID:     oldTask.ID,
		ActorID:    oldTask.Assignee,
		Name:       approval.CommandApprove,
	})
	if !errors.Is(err, workflow.ErrInvalidCommand) {
		t.Errorf("old assignee Handle() error = %v, want ErrInvalidCommand", err)
	}
	transferred, err = engine.Handle(t.Context(), workflow.Command{
		InstanceID: transferred.ID,
		TaskID:     newTask.ID,
		ActorID:    newTask.Assignee,
		Name:       approval.CommandApprove,
	})
	if err != nil {
		t.Fatalf("new assignee Handle() error = %v", err)
	}
	if transferred.Status != workflow.InstanceStatusRunning || transferred.Tasks[len(transferred.Tasks)-1].Status != workflow.TaskStatusCompleted {
		t.Errorf("replacement decision = %#v, want completed task in running countersign", transferred)
	}

	// The untouched countersigner completes the same frozen round and advances the original Definition route.
	completed, err := engine.Handle(t.Context(), workflow.Command{
		InstanceID: transferred.ID,
		TaskID:     transferred.Tasks[1].ID,
		ActorID:    transferred.Tasks[1].Assignee,
		Name:       approval.CommandApprove,
	})
	if err != nil {
		t.Fatalf("remaining assignee Handle() error = %v", err)
	}
	if completed.Status != workflow.InstanceStatusCompleted {
		t.Errorf("completed.Status = %q, want %q", completed.Status, workflow.InstanceStatusCompleted)
	}
}

// TestEngineTransferRejectsUnauthorizedRequest verifies policy denial cannot mutate assignment or audit state.
func TestEngineTransferRejectsUnauthorizedRequest(t *testing.T) {
	t.Parallel()

	store := workflow.NewMemoryStore()
	engine, started := startTransferableInstance(
		t,
		store,
		"transfer-denied",
		approval.ModeAny,
		[]workflow.ActorID{"reviewer-a"},
	)
	request := workflow.TransferRequest{
		InstanceID:  started.ID,
		TaskID:      started.Tasks[0].ID,
		ActorID:     "untrusted-operator",
		NewAssignee: "unknown-reviewer",
		Reason:      "unauthorized delegation",
	}
	policy := transferPolicyFunc(func(
		_ context.Context,
		_ workflow.TransferRequest,
		_ workflow.Task,
		snapshot *workflow.Instance,
	) error {
		// Mutating the detached authorization view proves policy cannot create partial candidate state.
		snapshot.Tasks[0].Assignee = request.NewAssignee
		snapshot.Audit = nil
		snapshot.Definition.Nodes = nil
		return errTransferDenied
	})

	_, err := engine.Transfer(t.Context(), request, policy)
	if !errors.Is(err, errTransferDenied) {
		t.Fatalf("Transfer() error = %v, want host policy error", err)
	}
	stored, err := store.Load(t.Context(), started.ID)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if !reflect.DeepEqual(stored, started) {
		t.Errorf("stored instance = %#v, want unchanged %#v", stored, started)
	}
}

// TestEngineTransferRejectsNonActiveTasks verifies historical and non-current assignments cannot be transferred.
func TestEngineTransferRejectsNonActiveTasks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		status workflow.TaskStatus
		nodeID string
	}{
		{name: "completed task", status: workflow.TaskStatusCompleted, nodeID: "approval"},
		{name: "closed task", status: workflow.TaskStatusClosed, nodeID: "approval"},
		{name: "historical active task", status: workflow.TaskStatusActive, nodeID: "previous-approval"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Store the exact aggregate shape needed to isolate transferability from handler behavior.
			store := workflow.NewMemoryStore()
			instance := &workflow.Instance{
				ID:            workflow.InstanceID("non-active-" + tt.name),
				Status:        workflow.InstanceStatusRunning,
				CurrentNodeID: "approval",
				Tasks: []workflow.Task{{
					ID: "task-a", NodeID: tt.nodeID, Assignee: "reviewer-a", Status: tt.status,
				}},
				Version: 3,
			}
			if err := store.Create(t.Context(), instance); err != nil {
				t.Fatalf("Create() error = %v", err)
			}
			policy := transferPolicyFunc(func(context.Context, workflow.TransferRequest, workflow.Task, *workflow.Instance) error {
				t.Fatal("AuthorizeTransfer() called for non-transferable task")
				return nil
			})

			_, err := workflow.NewEngine(store, nil).Transfer(t.Context(), workflow.TransferRequest{
				InstanceID:  instance.ID,
				TaskID:      instance.Tasks[0].ID,
				ActorID:     "operator-a",
				NewAssignee: "reviewer-b",
				Reason:      "invalid historical transfer",
			}, policy)
			if !errors.Is(err, workflow.ErrTaskNotTransferable) {
				t.Fatalf("Transfer() error = %v, want ErrTaskNotTransferable", err)
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

// TestEngineTransferPreservesSnapshotOnVersionConflict verifies stale CAS cannot partially replace an assignment.
func TestEngineTransferPreservesSnapshotOnVersionConflict(t *testing.T) {
	t.Parallel()

	memoryStore := workflow.NewMemoryStore()
	store := &versionConflictStore{Store: memoryStore}
	engine, started := startTransferableInstance(
		t,
		store,
		"transfer-conflict",
		approval.ModeAny,
		[]workflow.ActorID{"reviewer-a"},
	)
	store.rejectSaves = true
	store.saveCalls = 0
	policy := transferPolicyFunc(func(context.Context, workflow.TransferRequest, workflow.Task, *workflow.Instance) error {
		return nil
	})

	_, err := engine.Transfer(t.Context(), workflow.TransferRequest{
		InstanceID:  started.ID,
		TaskID:      started.Tasks[0].ID,
		ActorID:     "operator-a",
		NewAssignee: "reviewer-b",
		Reason:      "concurrent transfer",
	}, policy)
	if !errors.Is(err, workflow.ErrVersionConflict) {
		t.Fatalf("Transfer() error = %v, want ErrVersionConflict", err)
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

// TestEngineTransferRejectsTerminalInstances verifies ended workflows cannot transfer otherwise active task data.
func TestEngineTransferRejectsTerminalInstances(t *testing.T) {
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
				ID:            workflow.InstanceID("terminal-transfer-" + status),
				Status:        status,
				CurrentNodeID: "approval",
				Tasks: []workflow.Task{{
					ID: "task-a", NodeID: "approval", Assignee: "reviewer-a", Status: workflow.TaskStatusActive,
				}},
				Version: 2,
			}
			if err := store.Create(t.Context(), instance); err != nil {
				t.Fatalf("Create() error = %v", err)
			}
			policy := transferPolicyFunc(func(context.Context, workflow.TransferRequest, workflow.Task, *workflow.Instance) error {
				t.Fatal("AuthorizeTransfer() called for terminal instance")
				return nil
			})

			_, err := workflow.NewEngine(store, nil).Transfer(t.Context(), workflow.TransferRequest{
				InstanceID:  instance.ID,
				TaskID:      instance.Tasks[0].ID,
				ActorID:     "operator-a",
				NewAssignee: "reviewer-b",
				Reason:      "terminal transfer",
			}, policy)
			if !errors.Is(err, workflow.ErrInstanceNotRunning) {
				t.Fatalf("Transfer() error = %v, want ErrInstanceNotRunning", err)
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

// TestEngineTransferValidatesBoundaryInput verifies incomplete trusted requests fail before Store.Load or policy.
func TestEngineTransferValidatesBoundaryInput(t *testing.T) {
	t.Parallel()

	validRequest := workflow.TransferRequest{
		InstanceID: "instance", TaskID: "task", ActorID: "operator", NewAssignee: "reviewer", Reason: "coverage",
	}
	policy := transferPolicyFunc(func(context.Context, workflow.TransferRequest, workflow.Task, *workflow.Instance) error {
		return nil
	})
	tests := []struct {
		name    string
		engine  *workflow.Engine
		request workflow.TransferRequest
		policy  workflow.TransferPolicy
		wantErr error
	}{
		{name: "nil engine", request: validRequest, policy: policy, wantErr: workflow.ErrInvalidEngine},
		{name: "nil store", engine: workflow.NewEngine(nil, nil), request: validRequest, policy: policy, wantErr: workflow.ErrInvalidEngine},
		{name: "empty instance", engine: workflow.NewEngine(workflow.NewMemoryStore(), nil), request: workflow.TransferRequest{TaskID: "task", ActorID: "operator", NewAssignee: "reviewer", Reason: "coverage"}, policy: policy, wantErr: workflow.ErrInvalidTransferRequest},
		{name: "empty task", engine: workflow.NewEngine(workflow.NewMemoryStore(), nil), request: workflow.TransferRequest{InstanceID: "instance", ActorID: "operator", NewAssignee: "reviewer", Reason: "coverage"}, policy: policy, wantErr: workflow.ErrInvalidTransferRequest},
		{name: "empty operator", engine: workflow.NewEngine(workflow.NewMemoryStore(), nil), request: workflow.TransferRequest{InstanceID: "instance", TaskID: "task", NewAssignee: "reviewer", Reason: "coverage"}, policy: policy, wantErr: workflow.ErrInvalidTransferRequest},
		{name: "empty target", engine: workflow.NewEngine(workflow.NewMemoryStore(), nil), request: workflow.TransferRequest{InstanceID: "instance", TaskID: "task", ActorID: "operator", Reason: "coverage"}, policy: policy, wantErr: workflow.ErrInvalidTransferRequest},
		{name: "blank reason", engine: workflow.NewEngine(workflow.NewMemoryStore(), nil), request: workflow.TransferRequest{InstanceID: "instance", TaskID: "task", ActorID: "operator", NewAssignee: "reviewer", Reason: "  "}, policy: policy, wantErr: workflow.ErrInvalidTransferRequest},
		{name: "nil policy", engine: workflow.NewEngine(workflow.NewMemoryStore(), nil), request: validRequest, wantErr: workflow.ErrInvalidTransferRequest},
		{name: "typed nil policy", engine: workflow.NewEngine(workflow.NewMemoryStore(), nil), request: validRequest, policy: transferPolicyFunc(nil), wantErr: workflow.ErrInvalidTransferRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := tt.engine.Transfer(t.Context(), tt.request, tt.policy)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("Transfer() error = %v, want %v", err, tt.wantErr)
			}
		})
	}
}
