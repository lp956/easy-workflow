// This file verifies Approval's optional request-local prepared-config contract against its legacy public behavior.
// It compares only public results and errors and does not inspect Approval's decoded configuration representation.
package approval_test

import (
	"encoding/json"
	"reflect"
	"testing"

	workflow "github.com/lvpeng/easy-workflow"
	"github.com/lvpeng/easy-workflow/approval"
)

// TestApprovalPreparedConfigMatchesLegacyExecution verifies one decoded config serves activation and command handling.
func TestApprovalPreparedConfigMatchesLegacyExecution(t *testing.T) {
	t.Parallel()

	// Encode canonical static Approval data and require the official handler to expose the optional preparation seam.
	config, err := json.Marshal(approval.Config{
		Mode:      approval.ModeAny,
		Assignees: []workflow.ActorID{"reviewer-a"},
	})
	if err != nil {
		t.Fatalf("json.Marshal(config) error = %v", err)
	}
	handler := approval.NewHandler()
	preparer, ok := any(handler).(workflow.NodeHandlerConfigPreparer)
	if !ok {
		t.Fatal("Approval handler does not implement NodeHandlerConfigPreparer")
	}
	prepared, err := preparer.PrepareConfig(config)
	if err != nil {
		t.Fatalf("PrepareConfig() error = %v", err)
	}

	// Prepared and legacy activation must produce the same detached task drafts and transition decision.
	legacyActivation, err := handler.Activate(t.Context(), workflow.ActivationInput{Config: config})
	if err != nil {
		t.Fatalf("Activate() error = %v", err)
	}
	preparedActivation, err := prepared.ActivatePrepared(t.Context(), workflow.PreparedActivationInput{})
	if err != nil {
		t.Fatalf("ActivatePrepared() error = %v", err)
	}
	if !reflect.DeepEqual(preparedActivation, legacyActivation) {
		t.Errorf("ActivatePrepared() = %#v, want legacy %#v", preparedActivation, legacyActivation)
	}

	// Supply Engine-shaped task identity and compare the command path without exposing decoded config to either caller.
	tasks := []workflow.Task{{
		ID:       "task-a",
		NodeID:   "approval",
		Assignee: "reviewer-a",
		Status:   workflow.TaskStatusActive,
	}}
	command := workflow.Command{TaskID: "task-a", ActorID: "reviewer-a", Name: approval.CommandApprove}
	legacyCommand, err := handler.Handle(t.Context(), workflow.CommandInput{
		Command: command,
		Config:  config,
		Tasks:   tasks,
	})
	if err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	preparedCommand, err := prepared.HandlePrepared(t.Context(), workflow.PreparedCommandInput{
		Command: command,
		Tasks:   tasks,
	})
	if err != nil {
		t.Fatalf("HandlePrepared() error = %v", err)
	}
	if !reflect.DeepEqual(preparedCommand, legacyCommand) {
		t.Errorf("HandlePrepared() = %#v, want legacy %#v", preparedCommand, legacyCommand)
	}
}
