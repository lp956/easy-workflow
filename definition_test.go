// Package workflow_test verifies the public workflow definition contract.
// It intentionally avoids internal helpers so implementations can evolve without breaking behavior tests.
package workflow_test

import (
	"encoding/json"
	"errors"
	"testing"

	workflow "github.com/lvpeng/easy-workflow"
)

// TestBuilderJSONRoundTrip verifies that code-built definitions retain their graph and node configuration through JSON.
func TestBuilderJSONRoundTrip(t *testing.T) {
	t.Parallel()

	// Build the smallest useful leave flow through the same public API used by library consumers.
	builder := workflow.NewBuilder("leave-request")
	builder.Start("start")
	builder.Node("manager-approval", "approval", struct {
		Mode string `json:"mode"`
	}{Mode: "any"})
	builder.End("end")
	builder.Connect("start", "manager-approval", "")
	builder.Connect("manager-approval", "end", "approved")

	definition, err := builder.Build()
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}

	// Round-trip through JSON because code and web editors must share one canonical definition format.
	data, err := json.Marshal(definition)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	loaded, err := workflow.ParseDefinition(data)
	if err != nil {
		t.Fatalf("ParseDefinition() error = %v", err)
	}

	if loaded.ID != "leave-request" {
		t.Errorf("loaded.ID = %q, want %q", loaded.ID, "leave-request")
	}
	if len(loaded.Nodes) != 3 {
		t.Fatalf("len(loaded.Nodes) = %d, want 3", len(loaded.Nodes))
	}
	if got := string(loaded.Nodes[1].Config); got != `{"mode":"any"}` {
		t.Errorf("approval config = %s, want %s", got, `{"mode":"any"}`)
	}
	if len(loaded.Edges) != 2 {
		t.Fatalf("len(loaded.Edges) = %d, want 2", len(loaded.Edges))
	}
}

// TestParseDefinitionRejectsAmbiguousJSON verifies web-authored definitions use a closed, single-value schema.
//
// Every fixture would otherwise decode into a structurally valid start-to-end graph after unknown or duplicate data was
// discarded. ParseDefinition must reject the bytes before those silent rewrites acquire canonical workflow meaning.
func TestParseDefinitionRejectsAmbiguousJSON(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		data string
	}{
		{
			name: "unknown edge field",
			data: `{"id":"strict-unknown","nodes":[{"id":"start","kind":"start"},{"id":"end","kind":"end"}],` +
				`"edges":[{"from":"start","to":"end","outcome":"","outcomee":"never"}]}`,
		},
		{
			name: "duplicate root field",
			data: `{"id":"discarded","id":"strict-duplicate","nodes":[{"id":"start","kind":"start"},` +
				`{"id":"end","kind":"end"}],"edges":[{"from":"start","to":"end","outcome":""}]}`,
		},
		{
			name: "trailing value",
			data: `{"id":"strict-trailing","nodes":[{"id":"start","kind":"start"},{"id":"end","kind":"end"}],` +
				`"edges":[{"from":"start","to":"end","outcome":""}]} {}`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			if _, err := workflow.ParseDefinition([]byte(test.data)); err == nil {
				t.Fatal("ParseDefinition() error = nil, want strict JSON rejection")
			}
		})
	}
}

// TestBuilderRejectsCycle verifies that definition-time validation enforces the agreed DAG boundary.
func TestBuilderRejectsCycle(t *testing.T) {
	t.Parallel()

	// A backward edge from approval to start creates a cycle even though an end node also exists.
	builder := workflow.NewBuilder("cyclic")
	builder.Start("start")
	builder.Node("approval", "approval", nil)
	builder.End("end")
	builder.Connect("start", "approval", "")
	builder.Connect("approval", "start", "returned")
	builder.Connect("approval", "end", "approved")

	_, err := builder.Build()
	if !errors.Is(err, workflow.ErrInvalidDefinition) {
		t.Fatalf("Build() error = %v, want ErrInvalidDefinition", err)
	}
}

// TestBuilderRejectsUnreachableNode verifies that every declared node belongs to the executable graph.
func TestBuilderRejectsUnreachableNode(t *testing.T) {
	t.Parallel()

	// The orphan has no path from start and would otherwise become dead configuration in web-authored JSON.
	builder := workflow.NewBuilder("unreachable")
	builder.Start("start")
	builder.Node("orphan", "approval", nil)
	builder.End("end")
	builder.Connect("start", "end", "")

	_, err := builder.Build()
	if !errors.Is(err, workflow.ErrInvalidDefinition) {
		t.Fatalf("Build() error = %v, want ErrInvalidDefinition", err)
	}
}

// TestBuilderRejectsDeadEndBranch verifies that every executable branch can reach a declared end node.
func TestBuilderRejectsDeadEndBranch(t *testing.T) {
	t.Parallel()

	// The escalation branch is reachable from start but has no route to a successful terminal node.
	builder := workflow.NewBuilder("dead-end")
	builder.Start("start")
	builder.Node("approval", "approval", nil)
	builder.Node("escalation", "approval", nil)
	builder.End("end")
	builder.Connect("start", "approval", "")
	builder.Connect("approval", "end", "approved")
	builder.Connect("approval", "escalation", "escalated")

	_, err := builder.Build()
	if !errors.Is(err, workflow.ErrInvalidDefinition) {
		t.Fatalf("Build() error = %v, want ErrInvalidDefinition", err)
	}
}

// TestBuilderRejectsAmbiguousRejectedOutcome verifies that rejected cannot select multiple targets.
func TestBuilderRejectsAmbiguousRejectedOutcome(t *testing.T) {
	t.Parallel()

	// Both terminal edges use the same outcome, so runtime routing would depend on slice order.
	builder := workflow.NewBuilder("ambiguous")
	builder.Start("start")
	builder.Node("approval", "approval", nil)
	builder.End("accepted")
	builder.End("archived")
	builder.Connect("start", "approval", "")
	builder.Connect("approval", "accepted", "rejected")
	builder.Connect("approval", "archived", "rejected")

	_, err := builder.Build()
	if !errors.Is(err, workflow.ErrInvalidDefinition) || !errors.Is(err, workflow.ErrAmbiguousRoute) {
		t.Fatalf("Build() error = %v, want ErrInvalidDefinition and ErrAmbiguousRoute", err)
	}
}

// TestBuilderRejectsOutgoingEndEdge verifies that terminal nodes cannot hide ignored transitions.
func TestBuilderRejectsOutgoingEndEdge(t *testing.T) {
	t.Parallel()

	// Engine completion stops at the first end node, so an outgoing edge would never execute.
	builder := workflow.NewBuilder("non-terminal-end")
	builder.Start("start")
	builder.End("end")
	builder.End("ignored-end")
	builder.Connect("start", "end", "")
	builder.Connect("end", "ignored-end", "")

	_, err := builder.Build()
	if !errors.Is(err, workflow.ErrInvalidDefinition) {
		t.Fatalf("Build() error = %v, want ErrInvalidDefinition", err)
	}
}
