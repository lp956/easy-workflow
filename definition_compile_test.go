// Package workflow_test verifies Definition compilation through the public package contract.
// Tests observe validation and canonical JSON only; execution-plan indexes remain package internals.
package workflow_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	workflow "github.com/lvpeng/easy-workflow"
	"github.com/lvpeng/easy-workflow/approval"
)

var (
	// errTestHandlerMutatedConfig classifies compiled-plan aliasing observed by the test extension.
	errTestHandlerMutatedConfig = errors.New("test handler received mutated config")
)

// configMutatingHandler probes whether compilation exposes plan-owned configuration to extension code.
// It retains no state, mutates only the Validate argument, and is safe for the test's concurrent package execution.
type configMutatingHandler struct{}

// Validate deliberately corrupts its input to verify the compiler supplied disposable bytes.
func (configMutatingHandler) Validate(config json.RawMessage) error {
	if len(config) > 0 {
		config[0] = '[' // Changing the object opener makes aliasing visible when Activate receives plan config.
	}
	return nil
}

// Activate continues only when plan-owned configuration survived Validate unchanged.
func (configMutatingHandler) Activate(_ context.Context, input workflow.ActivationInput) (workflow.NodeResult, error) {
	// Any difference proves Validate received bytes aliased with the immutable compiled snapshot.
	if string(input.Config) != `{"outcome":"done"}` {
		return workflow.NodeResult{}, fmt.Errorf("%w: %q", errTestHandlerMutatedConfig, input.Config)
	}
	return workflow.NodeResult{Disposition: workflow.DispositionContinue, Outcome: "done"}, nil
}

// Handle is unused because this test handler always completes during activation.
func (configMutatingHandler) Handle(context.Context, workflow.CommandInput) (workflow.NodeResult, error) {
	return workflow.NodeResult{}, workflow.ErrInvalidCommand
}

// definitionMutatingHandler changes caller-owned graph data during activation to probe plan snapshot isolation.
// It is scoped to one synchronous Engine.Start call and must not be shared across concurrent operations.
type definitionMutatingHandler struct {
	// definition is the caller-owned graph mutated only after the compiler has frozen its operation snapshot.
	definition *workflow.Definition
}

// Validate accepts the fixed empty test configuration without retaining compiler-owned bytes.
func (*definitionMutatingHandler) Validate(json.RawMessage) error {
	return nil
}

// Activate mutates the original Definition after compilation, then requests the plan's declared outcome.
func (h *definitionMutatingHandler) Activate(context.Context, workflow.ActivationInput) (workflow.NodeResult, error) {
	h.definition.Edges[1].To = "caller-mutated-target" // The plan must keep the pre-activation target snapshot.
	return workflow.NodeResult{Disposition: workflow.DispositionContinue, Outcome: "done"}, nil
}

// Handle is unused because this test handler always completes during activation.
func (*definitionMutatingHandler) Handle(context.Context, workflow.CommandInput) (workflow.NodeResult, error) {
	return workflow.NodeResult{}, workflow.ErrInvalidCommand
}

// definitionCorruptingStore simulates an adapter returning a damaged historical Definition snapshot.
// It delegates durable behavior to Store and is scoped to one serial command test; Load corrupts only caller-owned results.
type definitionCorruptingStore struct {
	// Store supplies the real immutable snapshot and records any delegated durable mutation.
	workflow.Store

	// saveCalls counts serial CAS attempts made after a corrupted snapshot was loaded.
	saveCalls int
}

// Load returns a detached snapshot whose graph is deliberately no longer executable.
func (s *definitionCorruptingStore) Load(ctx context.Context, id workflow.InstanceID) (*workflow.Instance, error) {
	instance, err := s.Store.Load(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("test corrupting store load: %w", err)
	}
	instance.Definition.Edges = nil
	return instance, nil
}

// Save records every attempted durable mutation before delegating the Store CAS contract.
func (s *definitionCorruptingStore) Save(
	ctx context.Context,
	instance *workflow.Instance,
	expectedVersion uint64,
) error {
	s.saveCalls++
	if err := s.Store.Save(ctx, instance, expectedVersion); err != nil {
		return fmt.Errorf("test corrupting store save: %w", err)
	}
	return nil
}

// buildDefinitionFixture validates one canonical fixture through the public Builder authoring seam.
// definition must be non-nil; node config bytes are passed through json.RawMessage semantics. Structural or
// config-encoding failures preserve Builder's public error chain, while successful rebuilt data is intentionally discarded.
func buildDefinitionFixture(definition *workflow.Definition) error {
	// Recreate every declared node with the control-node helpers used by code-authored definitions.
	builder := workflow.NewBuilder(definition.ID)
	for _, node := range definition.Nodes {
		switch node.Kind {
		case workflow.KindStart:
			builder.Start(node.ID)
		case workflow.KindEnd:
			builder.End(node.ID)
		default:
			builder.Node(node.ID, node.Kind, node.Config)
		}
	}

	// Preserve serialized edge order so each public seam receives the same graph relationships.
	for _, edge := range definition.Edges {
		builder.Connect(edge.From, edge.To, edge.Outcome)
	}
	_, err := builder.Build()
	if err != nil {
		return fmt.Errorf("build definition fixture: %w", err)
	}
	return nil
}

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

// TestEngineStartProtectsPlanConfigFromHandlerValidation verifies extension validation cannot mutate a compiled plan.
func TestEngineStartProtectsPlanConfigFromHandlerValidation(t *testing.T) {
	t.Parallel()

	// Build one synchronous business node whose handler deliberately corrupts every Validate input.
	builder := workflow.NewBuilder("defensive-plan-config")
	builder.Start("start")
	builder.Node("mutator", "mutator", map[string]string{"outcome": "done"})
	builder.End("end")
	builder.Connect("start", "mutator", "")
	builder.Connect("mutator", "end", "done")
	definition, err := builder.Build()
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	registry := workflow.NewRegistry()
	if err := registry.Register("mutator", configMutatingHandler{}); err != nil {
		t.Fatalf("Register() error = %v", err)
	}

	// Start must activate and complete from the frozen bytes even after Validate mutates its disposable argument.
	instance, err := workflow.NewEngine(workflow.NewMemoryStore(), registry).Start(
		context.Background(),
		definition,
		workflow.StartRequest{ID: "defensive-plan-config-1", Initiator: "employee-a"},
	)
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if got := string(instance.Definition.Nodes[1].Config); got != `{"outcome":"done"}` {
		t.Errorf("frozen config = %s, want original canonical bytes", got)
	}
}

// TestEngineStartUsesOneFrozenGraphDuringActivation verifies caller mutation cannot change an in-flight plan.
func TestEngineStartUsesOneFrozenGraphDuringActivation(t *testing.T) {
	t.Parallel()

	// The custom handler changes the caller's route only after complete compilation has returned its plan.
	builder := workflow.NewBuilder("frozen-operation-graph")
	builder.Start("start")
	builder.Node("mutator", "definition-mutator", nil)
	builder.End("end")
	builder.Connect("start", "mutator", "")
	builder.Connect("mutator", "end", "done")
	definition, err := builder.Build()
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	registry := workflow.NewRegistry()
	handler := &definitionMutatingHandler{definition: definition}
	if err := registry.Register("definition-mutator", handler); err != nil {
		t.Fatalf("Register() error = %v", err)
	}

	// Execution follows and persists the frozen target even though caller-owned canonical data changed mid-operation.
	instance, err := workflow.NewEngine(workflow.NewMemoryStore(), registry).Start(
		context.Background(),
		definition,
		workflow.StartRequest{ID: "frozen-operation-graph-1", Initiator: "employee-a"},
	)
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if definition.Edges[1].To != "caller-mutated-target" {
		t.Fatal("test handler did not mutate the caller-owned Definition")
	}
	if instance.Status != workflow.InstanceStatusCompleted || instance.CurrentNodeID != "end" ||
		instance.Definition.Edges[1].To != "end" {
		t.Errorf("frozen instance = status %q node %q route %q, want completed at original end route", instance.Status, instance.CurrentNodeID, instance.Definition.Edges[1].To)
	}
}

// TestEngineRoutingIgnoresEdgeDeclarationOrder verifies outcome selection depends only on the compiled selector.
func TestEngineRoutingIgnoresEdgeDeclarationOrder(t *testing.T) {
	t.Parallel()

	registry := workflow.NewRegistry()
	if err := registry.Register(approval.Kind, approval.NewHandler()); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	tests := []struct {
		name  string
		edges []workflow.Edge
	}{
		{
			name: "entry edge first",
			edges: []workflow.Edge{
				{From: "start", To: "approval"},
				{From: "approval", To: "end", Outcome: approval.OutcomeApproved},
			},
		},
		{
			name: "outcome edge first",
			edges: []workflow.Edge{
				{From: "approval", To: "end", Outcome: approval.OutcomeApproved},
				{From: "start", To: "approval"},
			},
		},
	}

	for testIndex, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Both fixtures contain the same graph semantics and differ only in serialized edge order.
			definition := &workflow.Definition{
				ID: fmt.Sprintf("edge-order-%d", testIndex),
				Nodes: []workflow.NodeDefinition{
					{ID: "start", Kind: workflow.KindStart},
					{
						ID:     "approval",
						Kind:   approval.Kind,
						Config: json.RawMessage(`{"mode":"any","assignees":["manager-a"]}`),
					},
					{ID: "end", Kind: workflow.KindEnd},
				},
				Edges: tt.edges,
			}
			engine := workflow.NewEngine(workflow.NewMemoryStore(), registry)
			instance, err := engine.Start(context.Background(), definition, workflow.StartRequest{
				ID:        workflow.InstanceID(fmt.Sprintf("edge-order-%d", testIndex)),
				Initiator: "employee-a",
			})
			if err != nil {
				t.Fatalf("Start() error = %v", err)
			}

			// Approval's stable outcome must resolve to the same end node for either declaration order.
			instance, err = engine.Handle(context.Background(), workflow.Command{
				InstanceID: instance.ID,
				TaskID:     instance.Tasks[0].ID,
				ActorID:    instance.Tasks[0].Assignee,
				Name:       approval.CommandApprove,
			})
			if err != nil {
				t.Fatalf("Handle() error = %v", err)
			}
			if instance.Status != workflow.InstanceStatusCompleted || instance.CurrentNodeID != "end" {
				t.Errorf("instance = status %q node %q, want completed at end", instance.Status, instance.CurrentNodeID)
			}
		})
	}
}

// TestEngineCommandsRejectCorruptedDefinitionBeforeSave verifies loaded graph failure is atomic across command paths.
func TestEngineCommandsRejectCorruptedDefinitionBeforeSave(t *testing.T) {
	t.Parallel()

	// Start one valid waiting instance in the durable delegate before injecting corruption into future Load results.
	registry := workflow.NewRegistry()
	if err := registry.Register(approval.Kind, approval.NewHandler()); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	builder := workflow.NewBuilder("corrupted-loaded-definition")
	builder.Start("start")
	builder.Node("approval", approval.Kind, approval.Config{
		Mode:      approval.ModeAny,
		Assignees: []workflow.ActorID{"manager-a"},
	})
	builder.End("end")
	builder.Connect("start", "approval", "")
	builder.Connect("approval", "end", approval.OutcomeApproved)
	definition, err := builder.Build()
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	ctx := context.Background()
	durable := workflow.NewMemoryStore()
	instance, err := workflow.NewEngine(durable, registry).Start(ctx, definition, workflow.StartRequest{
		ID:        "corrupted-loaded-definition-1",
		Initiator: "employee-a",
	})
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	store := &definitionCorruptingStore{Store: durable}
	engine := workflow.NewEngine(store, registry)

	// Handle must reject the damaged graph before invoking the handler transition or Store.Save.
	_, err = engine.Handle(ctx, workflow.Command{
		InstanceID: instance.ID,
		TaskID:     instance.Tasks[0].ID,
		ActorID:    instance.Tasks[0].Assignee,
		Name:       approval.CommandApprove,
	})
	if !errors.Is(err, workflow.ErrInvalidDefinition) {
		t.Fatalf("Handle() error = %v, want ErrInvalidDefinition", err)
	}

	// Return follows the same complete-compilation gate and must not reach authorization or persistence.
	_, err = engine.Return(ctx, workflow.ReturnRequest{
		InstanceID:   instance.ID,
		ActorID:      "manager-a",
		TargetNodeID: "approval",
		Reason:       "retry after correction",
	}, returnPolicyFunc(func(context.Context, workflow.ReturnRequest, *workflow.Instance) error {
		return nil
	}))
	if !errors.Is(err, workflow.ErrInvalidDefinition) {
		t.Fatalf("Return() error = %v, want ErrInvalidDefinition", err)
	}
	if calls := store.saveCalls; calls != 0 {
		t.Fatalf("Store.Save() calls = %d, want 0 for rejected corrupted snapshots", calls)
	}

	// The adapter corrupted only caller-owned loads, so the durable aggregate remains the original valid version.
	persisted, err := durable.Load(ctx, instance.ID)
	if err != nil {
		t.Fatalf("durable Load() error = %v", err)
	}
	if persisted.Version != instance.Version || len(persisted.Definition.Edges) != 2 {
		t.Errorf("durable snapshot = version %d edges %d, want version %d with 2 edges", persisted.Version, len(persisted.Definition.Edges), instance.Version)
	}
}

// TestInvalidGraphClassificationMatchesAcrossPublicSeams verifies structural rules cannot drift by authoring path.
func TestInvalidGraphClassificationMatchesAcrossPublicSeams(t *testing.T) {
	t.Parallel()

	// Each fixture isolates one structural invariant while remaining valid enough to reach that invariant's pass.
	tests := []struct {
		name          string               // name identifies the structural invariant exercised by the subtest.
		definition    *workflow.Definition // definition is the canonical fixture reused by every public seam.
		specificCause error                // specificCause is nil when no narrower public sentinel exists.
	}{
		{
			name: "cycle",
			definition: &workflow.Definition{
				ID: "matrix-cycle",
				Nodes: []workflow.NodeDefinition{
					{ID: "start", Kind: workflow.KindStart},
					{ID: "review", Kind: "review"},
					{ID: "end", Kind: workflow.KindEnd},
				},
				Edges: []workflow.Edge{
					{From: "start", To: "review"},
					{From: "review", To: "start", Outcome: "retry"},
					{From: "review", To: "end", Outcome: "done"},
				},
			},
		},
		{
			name: "unreachable node",
			definition: &workflow.Definition{
				ID: "matrix-unreachable",
				Nodes: []workflow.NodeDefinition{
					{ID: "start", Kind: workflow.KindStart},
					{ID: "orphan", Kind: "review"},
					{ID: "end", Kind: workflow.KindEnd},
				},
				Edges: []workflow.Edge{{From: "start", To: "end"}},
			},
		},
		{
			name: "dead-end branch",
			definition: &workflow.Definition{
				ID: "matrix-dead-end",
				Nodes: []workflow.NodeDefinition{
					{ID: "start", Kind: workflow.KindStart},
					{ID: "review", Kind: "review"},
					{ID: "dead-end", Kind: "review"},
					{ID: "end", Kind: workflow.KindEnd},
				},
				Edges: []workflow.Edge{
					{From: "start", To: "review"},
					{From: "review", To: "end", Outcome: "done"},
					{From: "review", To: "dead-end", Outcome: "retry"},
				},
			},
		},
		{
			name: "ambiguous outcome",
			definition: &workflow.Definition{
				ID: "matrix-ambiguous",
				Nodes: []workflow.NodeDefinition{
					{ID: "start", Kind: workflow.KindStart},
					{ID: "review", Kind: "review"},
					{ID: "accepted", Kind: workflow.KindEnd},
					{ID: "archived", Kind: workflow.KindEnd},
				},
				Edges: []workflow.Edge{
					{From: "start", To: "review"},
					{From: "review", To: "accepted", Outcome: "done"},
					{From: "review", To: "archived", Outcome: "done"},
				},
			},
			specificCause: workflow.ErrAmbiguousRoute,
		},
	}

	for testIndex, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Send the same graph through code authoring, JSON authoring, publication, and Engine startup.
			builderErr := buildDefinitionFixture(tt.definition)
			data, err := json.Marshal(tt.definition)
			if err != nil {
				t.Fatalf("json.Marshal() error = %v", err)
			}
			_, parseErr := workflow.ParseDefinition(data)
			_, publishErr := workflow.NewDefinitionPublisher(
				workflow.NewMemoryDefinitionStore(),
				workflow.NewRegistry(),
			).Publish(context.Background(), tt.definition)
			_, startErr := workflow.NewEngine(workflow.NewMemoryStore(), workflow.NewRegistry()).Start(
				context.Background(),
				tt.definition,
				workflow.StartRequest{
					ID:        workflow.InstanceID(fmt.Sprintf("invalid-matrix-%d", testIndex)),
					Initiator: "employee-a",
				},
			)
			errorsBySeam := []struct {
				name string
				err  error
			}{
				{name: "Builder.Build", err: builderErr},
				{name: "ParseDefinition", err: parseErr},
				{name: "DefinitionPublisher.Publish", err: publishErr},
				{name: "Engine.Start", err: startErr},
			}

			// Every structural entry point retains the common class and any narrower sentinel exposed for the invariant.
			for _, result := range errorsBySeam {
				if !errors.Is(result.err, workflow.ErrInvalidDefinition) {
					t.Errorf("%s error = %v, want ErrInvalidDefinition", result.name, result.err)
				}
				if tt.specificCause != nil && !errors.Is(result.err, tt.specificCause) {
					t.Errorf("%s error = %v, want specific cause %v", result.name, result.err, tt.specificCause)
				}
			}
		})
	}

	// A named-only start route is structurally valid but fails the two complete-compilation seams consistently.
	missingStartRoute := &workflow.Definition{
		ID: "matrix-missing-start-route",
		Nodes: []workflow.NodeDefinition{
			{ID: "start", Kind: workflow.KindStart},
			{ID: "end", Kind: workflow.KindEnd},
		},
		Edges: []workflow.Edge{{From: "start", To: "end", Outcome: "named"}},
	}
	if err := buildDefinitionFixture(missingStartRoute); err != nil {
		t.Fatalf("Builder.Build(missing start route) error = %v, want structural success", err)
	}
	data, err := json.Marshal(missingStartRoute)
	if err != nil {
		t.Fatalf("json.Marshal(missing start route) error = %v", err)
	}
	if _, err := workflow.ParseDefinition(data); err != nil {
		t.Fatalf("ParseDefinition(missing start route) error = %v, want structural success", err)
	}
	_, publishErr := workflow.NewDefinitionPublisher(
		workflow.NewMemoryDefinitionStore(),
		workflow.NewRegistry(),
	).Publish(context.Background(), missingStartRoute)
	_, startErr := workflow.NewEngine(workflow.NewMemoryStore(), workflow.NewRegistry()).Start(
		context.Background(),
		missingStartRoute,
		workflow.StartRequest{ID: "missing-start-route-matrix-1", Initiator: "employee-a"},
	)
	for seam, err := range map[string]error{"DefinitionPublisher.Publish": publishErr, "Engine.Start": startErr} {
		if !errors.Is(err, workflow.ErrInvalidDefinition) || !errors.Is(err, workflow.ErrRouteNotFound) {
			t.Errorf("%s error = %v, want ErrInvalidDefinition and ErrRouteNotFound", seam, err)
		}
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

// TestEngineHandleRejectsMissingRejectedOutcomeRoute verifies configured rejection routing never falls back to terminal.
func TestEngineHandleRejectsMissingRejectedOutcomeRoute(t *testing.T) {
	t.Parallel()

	// Configure Approval to emit rejected while deliberately declaring only its approved edge.
	registry := workflow.NewRegistry()
	if err := registry.Register(approval.Kind, approval.NewHandler()); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	builder := workflow.NewBuilder("missing-rejected-route")
	builder.Start("start")
	builder.Node("manager-approval", approval.Kind, approval.Config{
		Mode:            approval.ModeAny,
		Assignees:       []workflow.ActorID{"manager-a"},
		RejectedOutcome: approval.OutcomeRejected,
	})
	builder.End("end")
	builder.Connect("start", "manager-approval", "")
	builder.Connect("manager-approval", "end", approval.OutcomeApproved)
	definition, err := builder.Build()
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	engine := workflow.NewEngine(workflow.NewMemoryStore(), registry)
	instance, err := engine.Start(context.Background(), definition, workflow.StartRequest{
		ID:        "missing-rejected-route-1",
		Initiator: "employee-a",
	})
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	// Configured routing requires the exact rejected selector and must surface its absence with full context.
	_, err = engine.Handle(context.Background(), workflow.Command{
		InstanceID: instance.ID,
		TaskID:     instance.Tasks[0].ID,
		ActorID:    instance.Tasks[0].Assignee,
		Name:       approval.CommandReject,
	})
	if !errors.Is(err, workflow.ErrRouteNotFound) {
		t.Fatalf("Handle() error = %v, want ErrRouteNotFound", err)
	}
	if !strings.Contains(err.Error(), `definition "missing-rejected-route"`) ||
		!strings.Contains(err.Error(), `node "manager-approval"`) ||
		!strings.Contains(err.Error(), `outcome "rejected"`) {
		t.Errorf("Handle() error = %v, want definition, node, and rejected outcome context", err)
	}
}

// TestCompileDefinitionRejectsInvalidNodeConfig verifies Approval failures have a stable classification and location.
func TestCompileDefinitionRejectsInvalidNodeConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		config approval.Config
	}{
		{
			name: "unsupported mode",
			config: approval.Config{
				Mode:      approval.Mode("unsupported"),
				Assignees: []workflow.ActorID{"manager-a"},
			},
		},
		{
			name: "arbitrary rejected outcome",
			config: approval.Config{
				Mode:            approval.ModeAny,
				Assignees:       []workflow.ActorID{"manager-a"},
				RejectedOutcome: "arbitrary-node-or-outcome",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Keep the graph valid so compilation reaches the Approval-owned configuration contract.
			builder := workflow.NewBuilder("invalid-approval")
			builder.Start("start")
			builder.Node("manager-approval", approval.Kind, tt.config)
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
		})
	}
}

// TestCompileDefinitionRejectsInvalidControlNodeConfig verifies complete compilation validates all canonical config bytes.
func TestCompileDefinitionRejectsInvalidControlNodeConfig(t *testing.T) {
	t.Parallel()

	// Control nodes need no handler, but their RawMessage values must still remain valid canonical JSON.
	definition := &workflow.Definition{
		ID: "invalid-control-config",
		Nodes: []workflow.NodeDefinition{
			{ID: "start", Kind: workflow.KindStart, Config: json.RawMessage(`{"invalid"`)},
			{ID: "end", Kind: workflow.KindEnd},
		},
		Edges: []workflow.Edge{{From: "start", To: "end"}},
	}

	// Complete compilation classifies malformed control config without requiring a registered handler.
	err := workflow.CompileDefinition(definition, workflow.NewRegistry())
	if !errors.Is(err, workflow.ErrInvalidDefinition) || !errors.Is(err, workflow.ErrInvalidNodeConfig) {
		t.Fatalf("CompileDefinition() error = %v, want ErrInvalidDefinition and ErrInvalidNodeConfig", err)
	}
	if !strings.Contains(err.Error(), `definition "invalid-control-config"`) ||
		!strings.Contains(err.Error(), `node "start"`) {
		t.Errorf("CompileDefinition() error = %v, want definition and control-node context", err)
	}
}

// TestCompileDefinitionValidatesJSONBeforeRegistry verifies complete compilation applies its validation phases consistently.
func TestCompileDefinitionValidatesJSONBeforeRegistry(t *testing.T) {
	t.Parallel()

	// The earlier business node lacks a handler while the later control node contains malformed canonical JSON.
	definition := &workflow.Definition{
		ID: "validation-order",
		Nodes: []workflow.NodeDefinition{
			{ID: "start", Kind: workflow.KindStart},
			{ID: "external-review", Kind: "external-review", Config: json.RawMessage(`{}`)},
			{ID: "end", Kind: workflow.KindEnd, Config: json.RawMessage(`{"invalid"`)},
		},
		Edges: []workflow.Edge{
			{From: "start", To: "external-review"},
			{From: "external-review", To: "end", Outcome: "done"},
		},
	}

	// Syntax is a canonical-data phase, so it must fail before Registry membership is consulted.
	err := workflow.CompileDefinition(definition, workflow.NewRegistry())
	if !errors.Is(err, workflow.ErrInvalidNodeConfig) {
		t.Fatalf("CompileDefinition() error = %v, want ErrInvalidNodeConfig", err)
	}
	if errors.Is(err, workflow.ErrHandlerNotFound) {
		t.Fatalf("CompileDefinition() error = %v, must validate all JSON before Registry membership", err)
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

// TestCompileDefinitionRejectsAdditionalStartBranch verifies startup cannot publish semantically dead selectors.
//
// Builder-level graph analysis sees both terminal nodes as reachable, but Engine can emit only the empty Start outcome.
// Complete compilation must therefore reject the additional named selector before publication or instance creation.
func TestCompileDefinitionRejectsAdditionalStartBranch(t *testing.T) {
	t.Parallel()

	definition := &workflow.Definition{
		ID: "additional-start-branch",
		Nodes: []workflow.NodeDefinition{
			{ID: "start", Kind: workflow.KindStart},
			{ID: "live-end", Kind: workflow.KindEnd},
			{ID: "dead-end", Kind: workflow.KindEnd},
		},
		Edges: []workflow.Edge{
			{From: "start", To: "live-end"},
			{From: "start", To: "dead-end", Outcome: "never"},
		},
	}

	err := workflow.CompileDefinition(definition, workflow.NewRegistry())
	if !errors.Is(err, workflow.ErrInvalidDefinition) {
		t.Fatalf("CompileDefinition() error = %v, want ErrInvalidDefinition", err)
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
