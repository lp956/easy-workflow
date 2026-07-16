// Package workflow_test verifies Definition compilation through the public package contract.
// Tests observe validation and canonical JSON only; execution-plan indexes remain package internals.
package workflow_test

import (
	"encoding/json"
	"testing"

	workflow "github.com/lvpeng/easy-workflow"
	"github.com/lvpeng/easy-workflow/approval"
)

// TestCompileDefinitionAcceptsBuilderAndJSON verifies both authoring paths enter the same compiler.
func TestCompileDefinitionAcceptsBuilderAndJSON(t *testing.T) {
	t.Parallel()

	// Build one canonical graph through the code authoring API.
	builder := workflow.NewBuilder("leave-request")
	builder.Start("start")
	builder.Node("manager-approval", approval.Kind, approval.Config{
		Mode:      approval.ModeAny,
		Assignees: []workflow.ActorID{"manager-a"},
	})
	builder.End("end")
	builder.Connect("start", "manager-approval", "")
	builder.Connect("manager-approval", "end", approval.OutcomeApproved)
	built, err := builder.Build()
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}

	// Parse the same canonical bytes to exercise the Web-authored JSON path.
	data, err := json.Marshal(built)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	parsed, err := workflow.ParseDefinition(data)
	if err != nil {
		t.Fatalf("ParseDefinition() error = %v", err)
	}

	// Both definitions compile against the same registered handler contract.
	registry := workflow.NewRegistry()
	if err := registry.Register(approval.Kind, approval.NewHandler()); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	if err := workflow.CompileDefinition(built, registry); err != nil {
		t.Errorf("CompileDefinition(builder) error = %v", err)
	}
	if err := workflow.CompileDefinition(parsed, registry); err != nil {
		t.Errorf("CompileDefinition(JSON) error = %v", err)
	}
}
