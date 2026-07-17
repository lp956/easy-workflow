// Package approval_test verifies dynamic assignment through the public Approval and workflow contracts.
// Tests provide in-memory organization adapters; they do not inspect Approval internals or external directories.
package approval_test

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"testing"

	workflow "github.com/lvpeng/easy-workflow"
	"github.com/lvpeng/easy-workflow/approval"
)

// errDirectoryUnavailable is the stable host-adapter failure used to verify wrapped causes.
var errDirectoryUnavailable = errors.New("directory unavailable")

// roleResolverFunc adapts one test-owned role lookup to the public organization adapter boundary.
type roleResolverFunc func(context.Context, string, json.RawMessage) ([]workflow.ActorID, error)

// ResolveRole delegates one role lookup and preserves the adapter result and error unchanged.
func (f roleResolverFunc) ResolveRole(
	ctx context.Context,
	role string,
	data json.RawMessage,
) ([]workflow.ActorID, error) {
	return f(ctx, role, data)
}

// TestDynamicRoleAssignmentRejectsDuplicatesAtomically verifies invalid directory results create no instance.
func TestDynamicRoleAssignmentRejectsDuplicatesAtomically(t *testing.T) {
	t.Parallel()

	// Return the same stable identity twice to model overlapping organization membership.
	resolver := roleResolverFunc(func(
		_ context.Context,
		_ string,
		_ json.RawMessage,
	) ([]workflow.ActorID, error) {
		return []workflow.ActorID{"reviewer-a", "reviewer-a"}, nil
	})
	registry := workflow.NewRegistry()
	if err := registry.Register(approval.Kind, approval.NewHandlerWithOrganization(resolver)); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	definition := dynamicApprovalDefinition(t, "duplicate-assignment", approval.ModeAll)
	store := workflow.NewMemoryStore()

	// Resolution and validation finish before Engine creates the aggregate, so failure leaves no partial state.
	_, err := workflow.NewEngine(store, registry).Start(context.Background(), definition, workflow.StartRequest{
		ID:        "duplicate-assignment-1",
		Initiator: "requester",
	})
	if !errors.Is(err, approval.ErrDuplicateAssignee) {
		t.Fatalf("Start() error = %v, want ErrDuplicateAssignee", err)
	}
	if _, loadErr := store.Load(context.Background(), "duplicate-assignment-1"); !errors.Is(loadErr, workflow.ErrInstanceNotFound) {
		t.Errorf("Load() error = %v, want ErrInstanceNotFound", loadErr)
	}
}

// TestDynamicRoleAssignmentRejectsInvalidActorsAtomically verifies unusable directory identities never persist.
func TestDynamicRoleAssignmentRejectsInvalidActorsAtomically(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		resolved   []workflow.ActorID
		wantErr    error
		instanceID workflow.InstanceID
	}{
		{
			name:       "empty result",
			resolved:   nil,
			wantErr:    approval.ErrNoAssignees,
			instanceID: "empty-assignment-1",
		},
		{
			name:       "empty actor",
			resolved:   []workflow.ActorID{"reviewer-a", ""},
			wantErr:    approval.ErrInvalidAssignee,
			instanceID: "empty-actor-1",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			// Each case exposes one invalid directory result through the host adapter boundary.
			resolver := roleResolverFunc(func(
				_ context.Context,
				_ string,
				_ json.RawMessage,
			) ([]workflow.ActorID, error) {
				return slices.Clone(test.resolved), nil
			})
			registry := workflow.NewRegistry()
			if err := registry.Register(approval.Kind, approval.NewHandlerWithOrganization(resolver)); err != nil {
				t.Fatalf("Register() error = %v", err)
			}
			store := workflow.NewMemoryStore()

			// Invalid identities are rejected before any aggregate can cross the Store creation boundary.
			_, err := workflow.NewEngine(store, registry).Start(
				context.Background(),
				dynamicApprovalDefinition(t, "invalid-directory-result", approval.ModeAny),
				workflow.StartRequest{ID: test.instanceID, Initiator: "requester"},
			)
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("Start() error = %v, want %v", err, test.wantErr)
			}
			if _, loadErr := store.Load(context.Background(), test.instanceID); !errors.Is(loadErr, workflow.ErrInstanceNotFound) {
				t.Errorf("Load() error = %v, want ErrInstanceNotFound", loadErr)
			}
		})
	}
}

// TestDynamicRoleAssignmentPreservesAdapterErrorsAtomically verifies lookup causes remain classifiable.
func TestDynamicRoleAssignmentPreservesAdapterErrorsAtomically(t *testing.T) {
	t.Parallel()

	resolver := roleResolverFunc(func(
		_ context.Context,
		_ string,
		_ json.RawMessage,
	) ([]workflow.ActorID, error) {
		return nil, errDirectoryUnavailable
	})
	registry := workflow.NewRegistry()
	if err := registry.Register(approval.Kind, approval.NewHandlerWithOrganization(resolver)); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	store := workflow.NewMemoryStore()

	// The Approval classification and host cause must both survive Engine context wrapping.
	_, err := workflow.NewEngine(store, registry).Start(
		context.Background(),
		dynamicApprovalDefinition(t, "adapter-failure", approval.ModeAny),
		workflow.StartRequest{ID: "adapter-failure-1", Initiator: "requester"},
	)
	if !errors.Is(err, approval.ErrAssignmentResolution) || !errors.Is(err, errDirectoryUnavailable) {
		t.Fatalf("Start() error = %v, want ErrAssignmentResolution and adapter cause", err)
	}
	if _, loadErr := store.Load(context.Background(), "adapter-failure-1"); !errors.Is(loadErr, workflow.ErrInstanceNotFound) {
		t.Errorf("Load() error = %v, want ErrInstanceNotFound", loadErr)
	}
}

// TestDynamicRoleAssignmentPropagatesCancellationAtomically verifies cancellation reaches a blocking adapter.
func TestDynamicRoleAssignmentPropagatesCancellationAtomically(t *testing.T) {
	t.Parallel()

	entered := make(chan struct{})
	resolver := roleResolverFunc(func(
		ctx context.Context,
		_ string,
		_ json.RawMessage,
	) ([]workflow.ActorID, error) {
		close(entered)
		<-ctx.Done()
		return nil, ctx.Err()
	})
	registry := workflow.NewRegistry()
	if err := registry.Register(approval.Kind, approval.NewHandlerWithOrganization(resolver)); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	store := workflow.NewMemoryStore()
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	definition := dynamicApprovalDefinition(t, "cancelled-assignment", approval.ModeAny)

	// Start resolves synchronously, so run it separately to cancel only after the adapter receives the request context.
	go func() {
		_, err := workflow.NewEngine(store, registry).Start(
			ctx,
			definition,
			workflow.StartRequest{ID: "cancelled-assignment-1", Initiator: "requester"},
		)
		result <- err
	}()
	<-entered
	cancel()
	err := <-result

	// Cancellation remains recognizable through Approval and Engine wrapping and cannot leave partial state.
	if !errors.Is(err, context.Canceled) || !errors.Is(err, approval.ErrAssignmentResolution) {
		t.Fatalf("Start() error = %v, want context.Canceled and ErrAssignmentResolution", err)
	}
	if _, loadErr := store.Load(context.Background(), "cancelled-assignment-1"); !errors.Is(loadErr, workflow.ErrInstanceNotFound) {
		t.Errorf("Load() error = %v, want ErrInstanceNotFound", loadErr)
	}
}

// TestDynamicRoleAssignmentRequiresExplicitAdapter verifies static composition has no implicit directory fallback.
func TestDynamicRoleAssignmentRequiresExplicitAdapter(t *testing.T) {
	t.Parallel()

	registry := workflow.NewRegistry()
	if err := registry.Register(approval.Kind, approval.NewHandler()); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	store := workflow.NewMemoryStore()

	// A dynamic definition may deserialize without side effects, but activation requires an explicit host adapter.
	_, err := workflow.NewEngine(store, registry).Start(
		context.Background(),
		dynamicApprovalDefinition(t, "missing-adapter", approval.ModeAny),
		workflow.StartRequest{ID: "missing-adapter-1", Initiator: "requester"},
	)
	if !errors.Is(err, approval.ErrOrganizationAdapterRequired) {
		t.Fatalf("Start() error = %v, want ErrOrganizationAdapterRequired", err)
	}
	if _, loadErr := store.Load(context.Background(), "missing-adapter-1"); !errors.Is(loadErr, workflow.ErrInstanceNotFound) {
		t.Errorf("Load() error = %v, want ErrInstanceNotFound", loadErr)
	}
}

// TestDynamicRoleAssignmentFreezesActorsForAnyAndAllModes verifies directory drift cannot rewrite active rounds.
func TestDynamicRoleAssignmentFreezesActorsForAnyAndAllModes(t *testing.T) {
	t.Parallel()

	for _, mode := range []approval.Mode{approval.ModeAny, approval.ModeAll} {
		t.Run(string(mode), func(t *testing.T) {
			t.Parallel()

			// Membership changes after Start affect only later adapter calls, not the concrete tasks already returned.
			members := []workflow.ActorID{"reviewer-a", "reviewer-b"}
			resolver := roleResolverFunc(func(
				_ context.Context,
				_ string,
				_ json.RawMessage,
			) ([]workflow.ActorID, error) {
				return slices.Clone(members), nil
			})
			registry := workflow.NewRegistry()
			if err := registry.Register(approval.Kind, approval.NewHandlerWithOrganization(resolver)); err != nil {
				t.Fatalf("Register() error = %v", err)
			}
			engine := workflow.NewEngine(workflow.NewMemoryStore(), registry)
			instance, err := engine.Start(
				context.Background(),
				dynamicApprovalDefinition(t, "frozen-"+string(mode), mode),
				workflow.StartRequest{ID: workflow.InstanceID("frozen-" + string(mode)), Initiator: "requester"},
			)
			if err != nil {
				t.Fatalf("Start() error = %v", err)
			}
			members = []workflow.ActorID{"replacement-reviewer"}

			// The first frozen actor follows the same command state machine used by static Approval configuration.
			instance, err = engine.Handle(context.Background(), workflow.Command{
				InstanceID: instance.ID,
				TaskID:     instance.Tasks[0].ID,
				ActorID:    "reviewer-a",
				Name:       approval.CommandApprove,
			})
			if err != nil {
				t.Fatalf("first Handle() error = %v", err)
			}
			if mode == approval.ModeAny {
				if instance.Status != workflow.InstanceStatusCompleted || instance.Tasks[1].Status != workflow.TaskStatusClosed {
					t.Fatalf("any-sign instance = %#v, want completed with closed sibling", instance)
				}
				return
			}
			if instance.Status != workflow.InstanceStatusRunning || instance.Tasks[1].Status != workflow.TaskStatusActive {
				t.Fatalf("first countersign instance = %#v, want running with active sibling", instance)
			}

			// Countersign completes only when the second originally frozen actor approves.
			instance, err = engine.Handle(context.Background(), workflow.Command{
				InstanceID: instance.ID,
				TaskID:     instance.Tasks[1].ID,
				ActorID:    "reviewer-b",
				Name:       approval.CommandApprove,
			})
			if err != nil {
				t.Fatalf("second Handle() error = %v", err)
			}
			if instance.Status != workflow.InstanceStatusCompleted {
				t.Errorf("countersign status = %q, want %q", instance.Status, workflow.InstanceStatusCompleted)
			}
		})
	}
}

// TestDynamicRoleAssignmentFailureDoesNotPersistTransition verifies later activation failure is fully atomic.
func TestDynamicRoleAssignmentFailureDoesNotPersistTransition(t *testing.T) {
	t.Parallel()

	resolver := roleResolverFunc(func(
		_ context.Context,
		_ string,
		_ json.RawMessage,
	) ([]workflow.ActorID, error) {
		return nil, errDirectoryUnavailable
	})
	registry := workflow.NewRegistry()
	if err := registry.Register(approval.Kind, approval.NewHandlerWithOrganization(resolver)); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	builder := workflow.NewBuilder("atomic-dynamic-transition")
	builder.Start("start")
	builder.Node("initial-review", approval.Kind, approval.Config{
		Mode:      approval.ModeAny,
		Assignees: []workflow.ActorID{"initial-reviewer"},
	})
	builder.Node("dynamic-review", approval.Kind, approval.Config{
		Mode: approval.ModeAny,
		Policy: &approval.AssignmentPolicy{
			Role: "finance-reviewer",
		},
	})
	builder.End("end")
	builder.Connect("start", "initial-review", "")
	builder.Connect("initial-review", "dynamic-review", approval.OutcomeApproved)
	builder.Connect("dynamic-review", "end", approval.OutcomeApproved)
	definition, err := builder.Build()
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	store := workflow.NewMemoryStore()
	engine := workflow.NewEngine(store, registry)
	before, err := engine.Start(context.Background(), definition, workflow.StartRequest{
		ID:        "atomic-dynamic-transition-1",
		Initiator: "requester",
	})
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	// Approving the first node attempts dynamic activation, whose failure must abort the entire CAS candidate.
	_, err = engine.Handle(context.Background(), workflow.Command{
		InstanceID: before.ID,
		TaskID:     before.Tasks[0].ID,
		ActorID:    before.Tasks[0].Assignee,
		Name:       approval.CommandApprove,
	})
	if !errors.Is(err, approval.ErrAssignmentResolution) || !errors.Is(err, errDirectoryUnavailable) {
		t.Fatalf("Handle() error = %v, want ErrAssignmentResolution and adapter cause", err)
	}
	after, err := store.Load(context.Background(), before.ID)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if after.Version != before.Version || after.CurrentNodeID != before.CurrentNodeID ||
		!slices.Equal(after.Tasks, before.Tasks) || !slices.Equal(after.Audit, before.Audit) {
		t.Errorf("stored instance changed after failed activation: before=%#v after=%#v", before, after)
	}
}

// dynamicApprovalDefinition builds one valid role-assigned approval graph for activation scenarios.
//
// id must be non-empty and mode must be a supported Approval mode. The helper fails its test on malformed graph
// construction and returns a caller-owned Definition configured for the fixed "finance-reviewer" role.
func dynamicApprovalDefinition(t *testing.T, id string, mode approval.Mode) *workflow.Definition {
	t.Helper()

	builder := workflow.NewBuilder(id)
	builder.Start("start")
	builder.Node("review", approval.Kind, approval.Config{
		Mode: mode,
		Policy: &approval.AssignmentPolicy{
			Role: "finance-reviewer",
		},
	})
	builder.End("end")
	builder.Connect("start", "review", "")
	builder.Connect("review", "end", approval.OutcomeApproved)
	definition, err := builder.Build()
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	return definition
}

// TestDynamicRoleAssignmentSupportsBuilderAndJSON verifies both authoring paths freeze resolved actors as tasks.
func TestDynamicRoleAssignmentSupportsBuilderAndJSON(t *testing.T) {
	t.Parallel()

	// The host owns role membership and returns concrete workflow identities at activation time.
	resolveCalls := 0
	resolver := roleResolverFunc(func(
		_ context.Context,
		role string,
		_ json.RawMessage,
	) ([]workflow.ActorID, error) {
		resolveCalls++
		if role != "finance-reviewer" {
			t.Fatalf("ResolveRole() role = %q, want %q", role, "finance-reviewer")
		}
		return []workflow.ActorID{"finance-a", "finance-b"}, nil
	})
	registry := workflow.NewRegistry()
	if err := registry.Register(approval.Kind, approval.NewHandlerWithOrganization(resolver)); err != nil {
		t.Fatalf("Register() error = %v", err)
	}

	// Builder and raw JSON express the same serializable role assignment policy.
	builder := workflow.NewBuilder("dynamic-builder")
	builder.Start("start")
	builder.Node("review", approval.Kind, approval.Config{
		Mode: approval.ModeAny,
		Policy: &approval.AssignmentPolicy{
			Role: "finance-reviewer",
		},
	})
	builder.End("end")
	builder.Connect("start", "review", "")
	builder.Connect("review", "end", approval.OutcomeApproved)
	built, err := builder.Build()
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	parsed, err := workflow.ParseDefinition([]byte(`{
		"id":"dynamic-json",
		"version":1,
		"nodes":[
			{"id":"start","kind":"start"},
			{"id":"review","kind":"approval","config":{"mode":"any","assignmentPolicy":{"role":"finance-reviewer"}}},
			{"id":"end","kind":"end"}
		],
		"edges":[
			{"from":"start","to":"review"},
			{"from":"review","to":"end","outcome":"approved"}
		]
	}`))
	if err != nil {
		t.Fatalf("ParseDefinition() error = %v", err)
	}
	for index, definition := range []*workflow.Definition{built, parsed} {
		if err := workflow.CompileDefinition(definition, registry); err != nil {
			t.Fatalf("CompileDefinition(definition %d) error = %v", index, err)
		}
	}
	if resolveCalls != 0 {
		t.Fatalf("organization adapter calls before activation = %d, want 0", resolveCalls)
	}

	// Each authoring form must resolve to the same frozen active task population.
	for index, definition := range []*workflow.Definition{built, parsed} {
		instance, startErr := workflow.NewEngine(workflow.NewMemoryStore(), registry).Start(
			context.Background(),
			definition,
			workflow.StartRequest{ID: workflow.InstanceID(definition.ID), Initiator: "requester"},
		)
		if startErr != nil {
			t.Fatalf("Start(definition %d) error = %v", index, startErr)
		}
		if len(instance.Tasks) != 2 {
			t.Fatalf("Start(definition %d) task count = %d, want 2", index, len(instance.Tasks))
		}
		assignees := []workflow.ActorID{instance.Tasks[0].Assignee, instance.Tasks[1].Assignee}
		if !slices.Equal(assignees, []workflow.ActorID{"finance-a", "finance-b"}) {
			t.Errorf("Start(definition %d) assignees = %v, want [finance-a finance-b]", index, assignees)
		}
	}
	if resolveCalls != 2 {
		t.Errorf("organization adapter activation calls = %d, want 2", resolveCalls)
	}
}

// TestAssignmentPolicyValidationRejectsAmbiguousSources verifies dynamic configuration has one clear source.
func TestAssignmentPolicyValidationRejectsAmbiguousSources(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		config approval.Config
	}{
		{
			name: "missing source",
			config: approval.Config{
				Mode: approval.ModeAny,
			},
		},
		{
			name: "static and dynamic sources",
			config: approval.Config{
				Mode:      approval.ModeAny,
				Assignees: []workflow.ActorID{"reviewer-a"},
				Policy:    &approval.AssignmentPolicy{Role: "finance-reviewer"},
			},
		},
		{
			name: "empty role",
			config: approval.Config{
				Mode:   approval.ModeAny,
				Policy: &approval.AssignmentPolicy{},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			// Marshal the public type so validation covers the same JSON contract used by Builder and Web authors.
			data, err := json.Marshal(test.config)
			if err != nil {
				t.Fatalf("json.Marshal() error = %v", err)
			}
			if err := approval.NewHandler().Validate(data); !errors.Is(err, approval.ErrInvalidConfig) {
				t.Fatalf("Validate() error = %v, want ErrInvalidConfig", err)
			}
		})
	}
}
