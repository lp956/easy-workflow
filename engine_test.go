// Package workflow_test exercises the engine only through contracts available to library consumers.
// The scenarios use the in-memory adapter but assert workflow behavior rather than storage internals.
package workflow_test

import (
	"context"
	"slices"
	"testing"

	workflow "github.com/lvpeng/easy-workflow"
	"github.com/lvpeng/easy-workflow/approval"
)

// TestLeaveRequestAnyApproval verifies that the first decision completes an or-sign approval node.
func TestLeaveRequestAnyApproval(t *testing.T) {
	t.Parallel()

	// Register the official approval extension explicitly; the core has no hidden node-type globals.
	registry := workflow.NewRegistry()
	if err := registry.Register(approval.Kind, approval.NewHandler()); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	engine := workflow.NewEngine(workflow.NewMemoryStore(), registry)

	// Define a minimal leave workflow with two eligible managers and or-sign semantics.
	builder := workflow.NewBuilder("leave-request")
	builder.Start("start")
	builder.Node("manager-approval", approval.Kind, approval.Config{
		Mode:      approval.ModeAny,
		Assignees: []workflow.ActorID{"manager-a", "manager-b"},
	})
	builder.End("end")
	builder.Connect("start", "manager-approval", "")
	builder.Connect("manager-approval", "end", approval.OutcomeApproved)
	definition, err := builder.Build()
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}

	instance, err := engine.Start(context.Background(), definition, workflow.StartRequest{
		ID:        "leave-1",
		Initiator: "employee-a",
	})
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if instance.Status != workflow.InstanceStatusRunning {
		t.Fatalf("instance.Status = %q, want %q", instance.Status, workflow.InstanceStatusRunning)
	}
	if len(instance.Tasks) != 2 {
		t.Fatalf("len(instance.Tasks) = %d, want 2", len(instance.Tasks))
	}

	// The first manager decides the or-sign node; the sibling task must close without another decision.
	instance, err = engine.Handle(context.Background(), workflow.Command{
		InstanceID: instance.ID,
		TaskID:     instance.Tasks[0].ID,
		ActorID:    instance.Tasks[0].Assignee,
		Name:       approval.CommandApprove,
	})
	if err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	if instance.Status != workflow.InstanceStatusCompleted {
		t.Errorf("instance.Status = %q, want %q", instance.Status, workflow.InstanceStatusCompleted)
	}
	if instance.Tasks[0].Status != workflow.TaskStatusCompleted {
		t.Errorf("decided task status = %q, want %q", instance.Tasks[0].Status, workflow.TaskStatusCompleted)
	}
	if instance.Tasks[1].Status != workflow.TaskStatusClosed {
		t.Errorf("sibling task status = %q, want %q", instance.Tasks[1].Status, workflow.TaskStatusClosed)
	}
	if len(instance.Audit) < 3 {
		t.Errorf("len(instance.Audit) = %d, want at least 3", len(instance.Audit))
	}
	if last := instance.Audit[len(instance.Audit)-1].Action; last != "instance.completed" {
		t.Errorf("last audit action = %q, want %q", last, "instance.completed")
	}
}

// TestLeaveRequestAllApproval verifies that countersign waits for every frozen assignee.
func TestLeaveRequestAllApproval(t *testing.T) {
	t.Parallel()

	// Compose a fresh engine so the scenario remains independent and safe for parallel execution.
	registry := workflow.NewRegistry()
	if err := registry.Register(approval.Kind, approval.NewHandler()); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	engine := workflow.NewEngine(workflow.NewMemoryStore(), registry)

	// Use the same leave graph as or-sign while changing only the official approval mode.
	builder := workflow.NewBuilder("leave-request")
	builder.Start("start")
	builder.Node("manager-approval", approval.Kind, approval.Config{
		Mode:      approval.ModeAll,
		Assignees: []workflow.ActorID{"manager-a", "manager-b"},
	})
	builder.End("end")
	builder.Connect("start", "manager-approval", "")
	builder.Connect("manager-approval", "end", approval.OutcomeApproved)
	definition, err := builder.Build()
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}

	instance, err := engine.Start(context.Background(), definition, workflow.StartRequest{
		ID:        "leave-all-1",
		Initiator: "employee-a",
	})
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	// The first approval is recorded, but one active frozen assignee keeps the instance running.
	instance, err = engine.Handle(context.Background(), workflow.Command{
		InstanceID: instance.ID,
		TaskID:     instance.Tasks[0].ID,
		ActorID:    instance.Tasks[0].Assignee,
		Name:       approval.CommandApprove,
	})
	if err != nil {
		t.Fatalf("first Handle() error = %v", err)
	}
	if instance.Status != workflow.InstanceStatusRunning {
		t.Fatalf("status after first approval = %q, want %q", instance.Status, workflow.InstanceStatusRunning)
	}
	if instance.Tasks[1].Status != workflow.TaskStatusActive {
		t.Fatalf("second task status = %q, want %q", instance.Tasks[1].Status, workflow.TaskStatusActive)
	}

	// The final frozen assignee completes the countersign node and advances to the end node.
	instance, err = engine.Handle(context.Background(), workflow.Command{
		InstanceID: instance.ID,
		TaskID:     instance.Tasks[1].ID,
		ActorID:    instance.Tasks[1].Assignee,
		Name:       approval.CommandApprove,
	})
	if err != nil {
		t.Fatalf("second Handle() error = %v", err)
	}
	if instance.Status != workflow.InstanceStatusCompleted {
		t.Errorf("status after all approvals = %q, want %q", instance.Status, workflow.InstanceStatusCompleted)
	}
}

// TestLeaveRequestRejection verifies that one rejection terminates either approval mode without a rejection edge.
func TestLeaveRequestRejection(t *testing.T) {
	t.Parallel()

	for _, mode := range []approval.Mode{approval.ModeAny, approval.ModeAll} {
		t.Run(string(mode), func(t *testing.T) {
			t.Parallel()

			// Compose a fresh engine because each mode must own an independent task and audit history.
			registry := workflow.NewRegistry()
			if err := registry.Register(approval.Kind, approval.NewHandler()); err != nil {
				t.Fatalf("Register() error = %v", err)
			}
			engine := workflow.NewEngine(workflow.NewMemoryStore(), registry)

			// Without a rejected edge, the same approval decision remains terminal in both modes.
			builder := workflow.NewBuilder("leave-request")
			builder.Start("start")
			builder.Node("manager-approval", approval.Kind, approval.Config{
				Mode:      mode,
				Assignees: []workflow.ActorID{"manager-a", "manager-b"},
			})
			builder.End("end")
			builder.Connect("start", "manager-approval", "")
			builder.Connect("manager-approval", "end", approval.OutcomeApproved)
			definition, err := builder.Build()
			if err != nil {
				t.Fatalf("Build() error = %v", err)
			}
			instance, err := engine.Start(context.Background(), definition, workflow.StartRequest{
				ID:        "leave-rejected-1",
				Initiator: "employee-a",
			})
			if err != nil {
				t.Fatalf("Start() error = %v", err)
			}

			// One manager's rejection closes the sibling assignment and ends the whole instance.
			instance, err = engine.Handle(context.Background(), workflow.Command{
				InstanceID: instance.ID,
				TaskID:     instance.Tasks[0].ID,
				ActorID:    instance.Tasks[0].Assignee,
				Name:       approval.CommandReject,
			})
			if err != nil {
				t.Fatalf("Handle() error = %v", err)
			}
			if instance.Status != workflow.InstanceStatusRejected {
				t.Errorf("instance.Status = %q, want %q", instance.Status, workflow.InstanceStatusRejected)
			}
			if instance.Tasks[0].Outcome != approval.OutcomeRejected {
				t.Errorf("deciding task outcome = %q, want %q", instance.Tasks[0].Outcome, approval.OutcomeRejected)
			}
			if instance.Tasks[1].Status != workflow.TaskStatusClosed {
				t.Errorf("sibling task status = %q, want %q", instance.Tasks[1].Status, workflow.TaskStatusClosed)
			}
			if last := instance.Audit[len(instance.Audit)-1].Action; last != "instance.rejected" {
				t.Errorf("last audit action = %q, want %q", last, "instance.rejected")
			}
		})
	}
}

// TestLeaveRequestApprovalRoutesRejection verifies that an explicit rejected edge continues either approval mode.
func TestLeaveRequestApprovalRoutesRejection(t *testing.T) {
	t.Parallel()

	for _, mode := range []approval.Mode{approval.ModeAny, approval.ModeAll} {
		t.Run(string(mode), func(t *testing.T) {
			t.Parallel()

			// Compose a fresh engine and correction node for each independent approval mode.
			registry := workflow.NewRegistry()
			if err := registry.Register(approval.Kind, approval.NewHandler()); err != nil {
				t.Fatalf("Register() error = %v", err)
			}
			engine := workflow.NewEngine(workflow.NewMemoryStore(), registry)

			// The rejected outcome names an edge; neither the command nor the handler receives its target node ID.
			builder := workflow.NewBuilder("routed-rejection")
			builder.Start("start")
			builder.Node("manager-approval", approval.Kind, approval.Config{
				Mode:            mode,
				Assignees:       []workflow.ActorID{"manager-a", "manager-b"},
				RejectedOutcome: approval.OutcomeRejected,
			})
			builder.Node("correction", approval.Kind, approval.Config{
				Mode:      approval.ModeAny,
				Assignees: []workflow.ActorID{"employee-a"},
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
			instance, err := engine.Start(context.Background(), definition, workflow.StartRequest{
				ID:        "routed-rejection-1",
				Initiator: "employee-a",
			})
			if err != nil {
				t.Fatalf("Start() error = %v", err)
			}

			// Rejecting completes the deciding task, closes its sibling, and activates the edge-selected correction node.
			instance, err = engine.Handle(context.Background(), workflow.Command{
				InstanceID: instance.ID,
				TaskID:     instance.Tasks[0].ID,
				ActorID:    instance.Tasks[0].Assignee,
				Name:       approval.CommandReject,
			})
			if err != nil {
				t.Fatalf("Handle() error = %v", err)
			}
			if instance.Status != workflow.InstanceStatusRunning {
				t.Errorf("instance.Status = %q, want %q", instance.Status, workflow.InstanceStatusRunning)
			}
			if instance.CurrentNodeID != "correction" {
				t.Errorf("instance.CurrentNodeID = %q, want %q", instance.CurrentNodeID, "correction")
			}
			if instance.Tasks[0].Status != workflow.TaskStatusCompleted || instance.Tasks[0].Outcome != approval.OutcomeRejected {
				t.Errorf("deciding task = %#v, want completed rejected task", instance.Tasks[0])
			}
			if instance.Tasks[1].Status != workflow.TaskStatusClosed {
				t.Errorf("sibling task status = %q, want %q", instance.Tasks[1].Status, workflow.TaskStatusClosed)
			}
			if len(instance.Tasks) != 3 || instance.Tasks[2].NodeID != "correction" || instance.Tasks[2].Status != workflow.TaskStatusActive {
				t.Fatalf("tasks = %#v, want one active correction task after two historical approval tasks", instance.Tasks)
			}

			// Accepted-command audit precedes the explicit rejection-route marker and target-node activation.
			auditCount := len(instance.Audit)
			if auditCount < 3 {
				t.Fatalf("len(instance.Audit) = %d, want at least 3", auditCount)
			}
			gotActions := []string{
				instance.Audit[auditCount-3].Action,
				instance.Audit[auditCount-2].Action,
				instance.Audit[auditCount-1].Action,
			}
			wantActions := []string{"task.reject", "node.rejected", "node.entered"}
			if !slices.Equal(gotActions, wantActions) {
				t.Errorf("trailing audit actions = %v, want %v", gotActions, wantActions)
			}
		})
	}
}
