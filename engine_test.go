// Package workflow_test exercises the engine only through contracts available to library consumers.
// The scenarios use the in-memory adapter but assert workflow behavior rather than storage internals.
package workflow_test

import (
	"context"
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

// TestLeaveRequestRejection verifies that one rejection terminates approval without a rejection edge.
func TestLeaveRequestRejection(t *testing.T) {
	t.Parallel()

	// Compose the official extension explicitly for an isolated rejection scenario.
	registry := workflow.NewRegistry()
	if err := registry.Register(approval.Kind, approval.NewHandler()); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	engine := workflow.NewEngine(workflow.NewMemoryStore(), registry)

	// Rejection is a terminal node result, so only the approved outcome needs a graph edge.
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
	if instance.Tasks[1].Status != workflow.TaskStatusClosed {
		t.Errorf("sibling task status = %q, want %q", instance.Tasks[1].Status, workflow.TaskStatusClosed)
	}
	if last := instance.Audit[len(instance.Audit)-1].Action; last != "instance.rejected" {
		t.Errorf("last audit action = %q, want %q", last, "instance.rejected")
	}
}
