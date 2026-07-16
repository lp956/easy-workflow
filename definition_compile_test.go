// Package workflow_test verifies Definition compilation through the public package contract.
// Tests observe validation and canonical JSON only; execution-plan indexes remain package internals.
package workflow_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
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
	afterCompile, err := json.Marshal(built)
	if err != nil {
		t.Fatalf("json.Marshal(after compile) error = %v", err)
	}
	if !bytes.Equal(afterCompile, data) {
		t.Errorf("canonical JSON changed during compilation: got %s, want %s", afterCompile, data)
	}

	// Execute both authored forms and compare only stable public behavior, excluding generated identities and time.
	builtInstance, err := workflow.NewEngine(workflow.NewMemoryStore(), registry).Start(
		context.Background(),
		built,
		workflow.StartRequest{ID: "builder-instance", Initiator: "employee-a"},
	)
	if err != nil {
		t.Fatalf("Start(builder) error = %v", err)
	}
	parsedInstance, err := workflow.NewEngine(workflow.NewMemoryStore(), registry).Start(
		context.Background(),
		parsed,
		workflow.StartRequest{ID: "json-instance", Initiator: "employee-a"},
	)
	if err != nil {
		t.Fatalf("Start(JSON) error = %v", err)
	}
	if builtInstance.Status != parsedInstance.Status || builtInstance.CurrentNodeID != parsedInstance.CurrentNodeID {
		t.Errorf(
			"execution state differs: builder=(%q, %q), JSON=(%q, %q)",
			builtInstance.Status,
			builtInstance.CurrentNodeID,
			parsedInstance.Status,
			parsedInstance.CurrentNodeID,
		)
	}
	if len(builtInstance.Tasks) != 1 || len(parsedInstance.Tasks) != 1 {
		t.Fatalf("task counts differ from one activation task: builder=%d, JSON=%d", len(builtInstance.Tasks), len(parsedInstance.Tasks))
	}
	if builtInstance.Tasks[0].Assignee != parsedInstance.Tasks[0].Assignee ||
		builtInstance.Tasks[0].Status != parsedInstance.Tasks[0].Status {
		t.Errorf("execution tasks differ: builder=%v, JSON=%v", builtInstance.Tasks, parsedInstance.Tasks)
	}
}

// TestEngineHandleRejectsMissingOutcomeRoute verifies runtime outcomes use a stable missing-route error.
func TestEngineHandleRejectsMissingOutcomeRoute(t *testing.T) {
	t.Parallel()

	// Compile a valid graph whose declared outcome intentionally differs from Approval's actual result.
	registry := workflow.NewRegistry()
	if err := registry.Register(approval.Kind, approval.NewHandler()); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	builder := workflow.NewBuilder("missing-route")
	builder.Start("start")
	builder.Node("manager-approval", approval.Kind, approval.Config{
		Mode:      approval.ModeAny,
		Assignees: []workflow.ActorID{"manager-a"},
	})
	builder.End("end")
	builder.Connect("start", "manager-approval", "")
	builder.Connect("manager-approval", "end", "unexpected")
	definition, err := builder.Build()
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	engine := workflow.NewEngine(workflow.NewMemoryStore(), registry)
	instance, err := engine.Start(context.Background(), definition, workflow.StartRequest{
		ID:        "missing-route-1",
		Initiator: "employee-a",
	})
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	// Approval emits "approved", which is absent from the compiled route index.
	_, err = engine.Handle(context.Background(), workflow.Command{
		InstanceID: instance.ID,
		TaskID:     instance.Tasks[0].ID,
		ActorID:    instance.Tasks[0].Assignee,
		Name:       approval.CommandApprove,
	})
	if !errors.Is(err, workflow.ErrRouteNotFound) {
		t.Fatalf("Handle() error = %v, want ErrRouteNotFound", err)
	}
	if !strings.Contains(err.Error(), `definition "missing-route"`) ||
		!strings.Contains(err.Error(), `node "manager-approval"`) ||
		!strings.Contains(err.Error(), `outcome "approved"`) {
		t.Errorf("Handle() error = %v, want definition, node, and outcome context", err)
	}
}

// TestCompileDefinitionRejectsInvalidNodeConfig verifies handler failures have a stable classification and location.
func TestCompileDefinitionRejectsInvalidNodeConfig(t *testing.T) {
	t.Parallel()

	// Keep the graph valid so the compiler reaches the approval handler's configuration contract.
	builder := workflow.NewBuilder("invalid-approval")
	builder.Start("start")
	builder.Node("manager-approval", approval.Kind, approval.Config{
		Mode:      approval.Mode("unsupported"),
		Assignees: []workflow.ActorID{"manager-a"},
	})
	builder.End("end")
	builder.Connect("start", "manager-approval", "")
	builder.Connect("manager-approval", "end", approval.OutcomeApproved)
	definition, err := builder.Build()
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	registry := workflow.NewRegistry()
	if err := registry.Register(approval.Kind, approval.NewHandler()); err != nil {
		t.Fatalf("Register() error = %v", err)
	}

	// Callers can classify the rule failure while retaining the exact Definition and node location.
	err = workflow.CompileDefinition(definition, registry)
	if !errors.Is(err, workflow.ErrInvalidNodeConfig) {
		t.Fatalf("CompileDefinition() error = %v, want ErrInvalidNodeConfig", err)
	}
	if !strings.Contains(err.Error(), `definition "invalid-approval"`) ||
		!strings.Contains(err.Error(), `node "manager-approval"`) {
		t.Errorf("CompileDefinition() error = %v, want definition and node context", err)
	}
}

// TestCompileDefinitionRejectsUnregisteredHandler verifies missing extensions are classified and located.
func TestCompileDefinitionRejectsUnregisteredHandler(t *testing.T) {
	t.Parallel()

	// The canonical graph is structurally valid but references a handler absent from this registry.
	builder := workflow.NewBuilder("missing-extension")
	builder.Start("start")
	builder.Node("external-review", "external-review", map[string]any{})
	builder.End("end")
	builder.Connect("start", "external-review", "")
	builder.Connect("external-review", "end", "approved")
	definition, err := builder.Build()
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}

	// The error remains an invalid Definition while exposing the specific missing-handler cause.
	err = workflow.CompileDefinition(definition, workflow.NewRegistry())
	if !errors.Is(err, workflow.ErrInvalidDefinition) || !errors.Is(err, workflow.ErrHandlerNotFound) {
		t.Fatalf("CompileDefinition() error = %v, want ErrInvalidDefinition and ErrHandlerNotFound", err)
	}
	if !strings.Contains(err.Error(), `definition "missing-extension"`) ||
		!strings.Contains(err.Error(), `node "external-review"`) {
		t.Errorf("CompileDefinition() error = %v, want definition and node context", err)
	}
}

// TestCompileDefinitionRejectsAmbiguousRoute verifies duplicate outcome selectors have a stable cause.
func TestCompileDefinitionRejectsAmbiguousRoute(t *testing.T) {
	t.Parallel()

	// Construct canonical data directly because Builder intentionally rejects this graph before return.
	definition := &workflow.Definition{
		ID:      "ambiguous-routing",
		Version: 1,
		Nodes: []workflow.NodeDefinition{
			{ID: "start", Kind: workflow.KindStart},
			{ID: "review", Kind: approval.Kind, Config: json.RawMessage(`{"mode":"any","assignees":["manager-a"]}`)},
			{ID: "accepted", Kind: workflow.KindEnd},
			{ID: "archived", Kind: workflow.KindEnd},
		},
		Edges: []workflow.Edge{
			{From: "start", To: "review"},
			{From: "review", To: "accepted", Outcome: approval.OutcomeApproved},
			{From: "review", To: "archived", Outcome: approval.OutcomeApproved},
		},
	}

	// Route ambiguity is independently classifiable and identifies the complete selector location.
	err := workflow.CompileDefinition(definition, workflow.NewRegistry())
	if !errors.Is(err, workflow.ErrInvalidDefinition) || !errors.Is(err, workflow.ErrAmbiguousRoute) {
		t.Fatalf("CompileDefinition() error = %v, want ErrInvalidDefinition and ErrAmbiguousRoute", err)
	}
	if !strings.Contains(err.Error(), `definition "ambiguous-routing"`) ||
		!strings.Contains(err.Error(), `node "review"`) ||
		!strings.Contains(err.Error(), `outcome "approved"`) {
		t.Errorf("CompileDefinition() error = %v, want definition, node, and outcome context", err)
	}
}

// TestCompileDefinitionGraphErrorIdentifiesDefinition verifies structural failures retain their owner.
func TestCompileDefinitionGraphErrorIdentifiesDefinition(t *testing.T) {
	t.Parallel()

	// This cycle is structurally invalid before registry or handler validation can run.
	definition := &workflow.Definition{
		ID: "cyclic-review",
		Nodes: []workflow.NodeDefinition{
			{ID: "start", Kind: workflow.KindStart},
			{ID: "review", Kind: approval.Kind},
			{ID: "end", Kind: workflow.KindEnd},
		},
		Edges: []workflow.Edge{
			{From: "start", To: "review"},
			{From: "review", To: "start", Outcome: "retry"},
			{From: "review", To: "end", Outcome: approval.OutcomeApproved},
		},
	}

	// Even whole-graph failures identify the Definition that must be corrected.
	err := workflow.CompileDefinition(definition, workflow.NewRegistry())
	if !errors.Is(err, workflow.ErrInvalidDefinition) {
		t.Fatalf("CompileDefinition() error = %v, want ErrInvalidDefinition", err)
	}
	if !strings.Contains(err.Error(), `definition "cyclic-review"`) {
		t.Errorf("CompileDefinition() error = %v, want definition context", err)
	}
}

// TestCompileDefinitionRejectsMissingStartRoute verifies the control entry has its required unconditional edge.
func TestCompileDefinitionRejectsMissingStartRoute(t *testing.T) {
	t.Parallel()

	// A named start outcome is structurally connected but cannot be selected by Engine startup semantics.
	definition := &workflow.Definition{
		ID: "missing-start-route",
		Nodes: []workflow.NodeDefinition{
			{ID: "start", Kind: workflow.KindStart},
			{ID: "end", Kind: workflow.KindEnd},
		},
		Edges: []workflow.Edge{
			{From: "start", To: "end", Outcome: "unexpected"},
		},
	}

	// Compilation rejects the absent unconditional selector before an Engine can create state.
	err := workflow.CompileDefinition(definition, workflow.NewRegistry())
	if !errors.Is(err, workflow.ErrInvalidDefinition) || !errors.Is(err, workflow.ErrRouteNotFound) {
		t.Fatalf("CompileDefinition() error = %v, want ErrInvalidDefinition and ErrRouteNotFound", err)
	}
	if !strings.Contains(err.Error(), `definition "missing-start-route"`) ||
		!strings.Contains(err.Error(), `node "start"`) ||
		!strings.Contains(err.Error(), `outcome ""`) {
		t.Errorf("CompileDefinition() error = %v, want definition, start node, and empty outcome context", err)
	}
}

// TestEngineStartDoesNotPersistCompileFailure verifies invalid definitions cannot create Instance state.
func TestEngineStartDoesNotPersistCompileFailure(t *testing.T) {
	t.Parallel()

	// Use invalid Approval configuration so failure occurs in the unified compiler before activation.
	registry := workflow.NewRegistry()
	if err := registry.Register(approval.Kind, approval.NewHandler()); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	builder := workflow.NewBuilder("invalid-before-create")
	builder.Start("start")
	builder.Node("manager-approval", approval.Kind, approval.Config{
		Mode:      approval.Mode("unsupported"),
		Assignees: []workflow.ActorID{"manager-a"},
	})
	builder.End("end")
	builder.Connect("start", "manager-approval", "")
	builder.Connect("manager-approval", "end", approval.OutcomeApproved)
	definition, err := builder.Build()
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	store := workflow.NewMemoryStore()
	engine := workflow.NewEngine(store, registry)

	// Compilation must fail before Store.Create can make the requested identifier durable.
	_, err = engine.Start(context.Background(), definition, workflow.StartRequest{
		ID:        "compile-failure-1",
		Initiator: "employee-a",
	})
	if !errors.Is(err, workflow.ErrInvalidNodeConfig) {
		t.Fatalf("Start() error = %v, want ErrInvalidNodeConfig", err)
	}
	if _, err := store.Load(context.Background(), "compile-failure-1"); !errors.Is(err, workflow.ErrInstanceNotFound) {
		t.Errorf("Load() error = %v, want ErrInstanceNotFound", err)
	}
}
