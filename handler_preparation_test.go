// This file verifies request-local handler configuration preparation through public compiler and Engine seams.
// It does not inspect compiled-plan internals; counters observe only documented handler and prepared-handler calls.
package workflow_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"sync"
	"testing"

	workflow "github.com/lvpeng/easy-workflow"
)

const (
	// preparedProbeKind is the registry key for the optional prepared-config handler contract.
	preparedProbeKind = "prepared-config-probe"
	// legacyProbeKind is the registry key for a handler that implements only the original NodeHandler contract.
	legacyProbeKind = "legacy-config-probe"
)

var (
	// errLegacyPreparedPath identifies an accidental fallback from a prepared execution plan to legacy handler methods.
	errLegacyPreparedPath = errors.New("test handler: legacy path called")
	// errUnsupportedPreparedRole classifies configuration outside the prepared probe's two executable roles.
	errUnsupportedPreparedRole = errors.New("test handler: unsupported prepared role")
	// errUnexpectedLegacyConfig classifies canonical bytes changed before legacy validation.
	errUnexpectedLegacyConfig = errors.New("test handler: unexpected legacy config")
	// errMissingLegacyActivationConfig classifies canonical bytes omitted or changed before legacy activation.
	errMissingLegacyActivationConfig = errors.New("test handler: missing legacy activation config")
	// errMissingLegacyCommandConfig classifies canonical bytes omitted or changed before legacy command handling.
	errMissingLegacyCommandConfig = errors.New("test handler: missing legacy command config")
)

// preparedProbeHandler records compilation and prepared-execution calls without retaining Engine input.
//
// A mutex protects counters because one registered handler may serve concurrent Engine operations. PrepareConfig returns
// request-local executors; the legacy methods deliberately fail so successful execution proves the prepared seam was used.
type preparedProbeHandler struct {
	// mu protects every counter map and legacyCalls.
	mu sync.Mutex
	// prepares counts config preparation by the stable role encoded in each node's canonical JSON.
	prepares map[string]int
	// activations counts prepared activation by role.
	activations map[string]int
	// handles counts prepared commands by role.
	handles map[string]int
	// legacyCalls counts any accidental Validate, Activate, or Handle fallback.
	legacyCalls int
}

// preparedProbeExecution is one immutable request-local executable config returned by preparedProbeHandler.
type preparedProbeExecution struct {
	// owner receives concurrency-safe observation counters and owns no request data.
	owner *preparedProbeHandler
	// role is the decoded handler configuration reused by every call in the compiled plan.
	role string
}

// typedNilPreparedProbeHandler returns a typed-nil executor to exercise the public compilation boundary.
// It is stateless, performs no I/O, and its legacy methods must never execute after PrepareConfig is selected.
type typedNilPreparedProbeHandler struct{}

// Validate accepts test configuration; the preparer interface replaces this legacy path during compilation.
func (typedNilPreparedProbeHandler) Validate(json.RawMessage) error {
	return nil
}

// Activate reports an unreachable legacy call if compilation incorrectly bypasses PrepareConfig.
func (typedNilPreparedProbeHandler) Activate(context.Context, workflow.ActivationInput) (workflow.NodeResult, error) {
	return workflow.NodeResult{}, errLegacyPreparedPath
}

// Handle reports an unreachable legacy call if compilation incorrectly constructs a compatibility executor.
func (typedNilPreparedProbeHandler) Handle(context.Context, workflow.CommandInput) (workflow.NodeResult, error) {
	return workflow.NodeResult{}, errLegacyPreparedPath
}

// PrepareConfig returns a typed-nil PreparedNodeHandler without retaining the supplied canonical bytes.
func (typedNilPreparedProbeHandler) PrepareConfig(json.RawMessage) (workflow.PreparedNodeHandler, error) {
	var prepared *preparedProbeExecution
	return prepared, nil
}

// Validate fails deliberately because a ConfigPreparer must replace legacy validation with one preparation call.
func (h *preparedProbeHandler) Validate(json.RawMessage) error {
	h.mu.Lock()
	h.legacyCalls++
	h.mu.Unlock()
	return errLegacyPreparedPath
}

// Activate fails deliberately because prepared plans must invoke ActivatePrepared instead.
func (h *preparedProbeHandler) Activate(context.Context, workflow.ActivationInput) (workflow.NodeResult, error) {
	h.mu.Lock()
	h.legacyCalls++
	h.mu.Unlock()
	return workflow.NodeResult{}, errLegacyPreparedPath
}

// Handle fails deliberately because prepared plans must invoke HandlePrepared instead.
func (h *preparedProbeHandler) Handle(context.Context, workflow.CommandInput) (workflow.NodeResult, error) {
	h.mu.Lock()
	h.legacyCalls++
	h.mu.Unlock()
	return workflow.NodeResult{}, errLegacyPreparedPath
}

// PrepareConfig validates and decodes one role, then returns a request-local immutable executor.
//
// config must encode the string "first" or "second". The method retains no raw bytes, performs no external I/O, and
// increments exactly one concurrency-safe observation counter. Errors reject compilation before execution or persistence.
func (h *preparedProbeHandler) PrepareConfig(config json.RawMessage) (workflow.PreparedNodeHandler, error) {
	var role string
	if err := json.Unmarshal(config, &role); err != nil {
		return nil, fmt.Errorf("test prepared handler decode role: %w", err)
	}
	if role != "first" && role != "second" {
		return nil, errUnsupportedPreparedRole
	}
	h.mu.Lock()
	if h.prepares == nil {
		h.prepares = make(map[string]int)
	}
	h.prepares[role]++
	h.mu.Unlock()
	return &preparedProbeExecution{owner: h, role: role}, nil
}

// ActivatePrepared uses the decoded role without parsing canonical config again.
//
// input contains detached business data only. The first role waits with one task; the second routes synchronously to
// "done". The executor retains no input and performs no external I/O.
func (e *preparedProbeExecution) ActivatePrepared(
	_ context.Context,
	_ workflow.PreparedActivationInput,
) (workflow.NodeResult, error) {
	e.owner.mu.Lock()
	if e.owner.activations == nil {
		e.owner.activations = make(map[string]int)
	}
	e.owner.activations[e.role]++
	e.owner.mu.Unlock()
	if e.role == "first" {
		return workflow.NodeResult{
			Disposition: workflow.DispositionWaiting,
			Tasks:       []workflow.Task{{Assignee: "owner-a", Status: workflow.TaskStatusActive}},
		}, nil
	}
	return workflow.NodeResult{Disposition: workflow.DispositionContinue, Outcome: "done"}, nil
}

// HandlePrepared completes the first role's selected task and routes to the second role.
//
// input must contain the complete detached current-node task view. The returned view owns its slice; the method performs
// no external I/O, and an unexpected second-role command returns workflow.ErrInvalidCommand.
func (e *preparedProbeExecution) HandlePrepared(
	_ context.Context,
	input workflow.PreparedCommandInput,
) (workflow.NodeResult, error) {
	e.owner.mu.Lock()
	if e.owner.handles == nil {
		e.owner.handles = make(map[string]int)
	}
	e.owner.handles[e.role]++
	e.owner.mu.Unlock()
	if e.role != "first" {
		return workflow.NodeResult{}, workflow.ErrInvalidCommand
	}
	tasks := slices.Clone(input.Tasks)
	for index := range tasks {
		if tasks[index].ID == input.TaskID {
			tasks[index].Status = workflow.TaskStatusCompleted
			tasks[index].Outcome = "next"
		}
	}
	return workflow.NodeResult{Disposition: workflow.DispositionContinue, Outcome: "next", Tasks: tasks}, nil
}

// counts returns detached observation maps and the legacy fallback count under one lock acquisition.
func (h *preparedProbeHandler) counts() (map[string]int, map[string]int, map[string]int, int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return cloneIntMap(h.prepares), cloneIntMap(h.activations), cloneIntMap(h.handles), h.legacyCalls
}

// legacyProbeHandler implements only NodeHandler and records raw-config validation and execution calls.
//
// A mutex protects counters under the original concurrent handler contract. The adapter retains no input and verifies
// that compatibility execution continues to receive the canonical config bytes on Activate and Handle.
type legacyProbeHandler struct {
	// mu protects validation, activation, and handle counters.
	mu sync.Mutex
	// validations counts complete compilations of this handler's node config.
	validations int
	// activations counts legacy node entries.
	activations int
	// handles counts legacy commands.
	handles int
}

// Validate accepts only the canonical legacy probe string and records one compilation call.
func (h *legacyProbeHandler) Validate(config json.RawMessage) error {
	if string(config) != `"legacy"` {
		return errUnexpectedLegacyConfig
	}
	h.mu.Lock()
	h.validations++
	h.mu.Unlock()
	return nil
}

// Activate verifies raw config delivery and creates one valid waiting assignment.
func (h *legacyProbeHandler) Activate(_ context.Context, input workflow.ActivationInput) (workflow.NodeResult, error) {
	if string(input.Config) != `"legacy"` {
		return workflow.NodeResult{}, errMissingLegacyActivationConfig
	}
	h.mu.Lock()
	h.activations++
	h.mu.Unlock()
	return workflow.NodeResult{
		Disposition: workflow.DispositionWaiting,
		Tasks:       []workflow.Task{{Assignee: "legacy-owner", Status: workflow.TaskStatusActive}},
	}, nil
}

// Handle verifies raw config delivery, completes the selected task, and follows the declared outcome.
func (h *legacyProbeHandler) Handle(_ context.Context, input workflow.CommandInput) (workflow.NodeResult, error) {
	if string(input.Config) != `"legacy"` {
		return workflow.NodeResult{}, errMissingLegacyCommandConfig
	}
	h.mu.Lock()
	h.handles++
	h.mu.Unlock()
	tasks := slices.Clone(input.Tasks)
	for index := range tasks {
		if tasks[index].ID == input.TaskID {
			tasks[index].Status = workflow.TaskStatusCompleted
			tasks[index].Outcome = "done"
		}
	}
	return workflow.NodeResult{Disposition: workflow.DispositionContinue, Outcome: "done", Tasks: tasks}, nil
}

// counts returns the legacy adapter's three counters atomically.
func (h *legacyProbeHandler) counts() (validations int, activations int, handles int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.validations, h.activations, h.handles
}

// cloneIntMap returns a detached copy of one counter map, preserving nil when no calls occurred.
func cloneIntMap(source map[string]int) map[string]int {
	if source == nil {
		return nil
	}
	result := make(map[string]int, len(source))
	maps.Copy(result, source)
	return result
}

// TestHandlerBoundariesRejectTypedNilImplementations verifies interface wrappers cannot bypass nil dependency checks.
//
// Registration and prepared-executor construction must return their documented classification errors without invoking a
// method on either typed-nil value. The table uses stateless probes and performs no external I/O.
func TestHandlerBoundariesRejectTypedNilImplementations(t *testing.T) {
	t.Parallel()

	t.Run("registry handler", func(t *testing.T) {
		t.Parallel()

		var handler *legacyProbeHandler
		err := workflow.NewRegistry().Register("typed-nil-handler", handler)
		if !errors.Is(err, workflow.ErrInvalidHandler) {
			t.Fatalf("Register() error = %v, want ErrInvalidHandler", err)
		}
	})

	t.Run("prepared executor", func(t *testing.T) {
		t.Parallel()

		registry := workflow.NewRegistry()
		if err := registry.Register("typed-nil-prepared", typedNilPreparedProbeHandler{}); err != nil {
			t.Fatalf("Register() error = %v", err)
		}
		builder := workflow.NewBuilder("typed-nil-prepared")
		builder.Start("start")
		builder.Node("task", "typed-nil-prepared", nil)
		builder.End("end")
		builder.Connect("start", "task", "")
		builder.Connect("task", "end", "done")
		definition, err := builder.Build()
		if err != nil {
			t.Fatalf("Build() error = %v", err)
		}

		_, err = workflow.NewEngine(workflow.NewMemoryStore(), registry).Start(t.Context(), definition, workflow.StartRequest{
			ID:        "typed-nil-prepared-1",
			Initiator: "requester-a",
		})
		if !errors.Is(err, workflow.ErrInvalidHandler) {
			t.Fatalf("Start() error = %v, want ErrInvalidHandler", err)
		}
	})
}

// TestEnginePreparesHandlerConfigOncePerExecutablePlan verifies Start and Handle each build one reusable prepared plan.
func TestEnginePreparesHandlerConfigOncePerExecutablePlan(t *testing.T) {
	t.Parallel()

	// Build two differently configured nodes so one Handle plan uses prepared command and activation executors together.
	handler := &preparedProbeHandler{}
	registry := workflow.NewRegistry()
	if err := registry.Register(preparedProbeKind, handler); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	builder := workflow.NewBuilder("prepared-handler-plan")
	builder.Start("start")
	builder.Node("first", preparedProbeKind, "first")
	builder.Node("second", preparedProbeKind, "second")
	builder.End("end")
	builder.Connect("start", "first", "")
	builder.Connect("first", "second", "next")
	builder.Connect("second", "end", "done")
	definition, err := builder.Build()
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	canonicalJSON, err := json.Marshal(definition)
	if err != nil {
		t.Fatalf("json.Marshal(definition) error = %v", err)
	}

	// Start prepares both node configs once and activates the first through its prepared executor.
	store := workflow.NewMemoryStore()
	engine := workflow.NewEngine(store, registry)
	instance, err := engine.Start(t.Context(), definition, workflow.StartRequest{
		ID:        "prepared-handler-plan-1",
		Initiator: "requester-a",
	})
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	// Handle defensively recompiles the persisted snapshot, then reuses that plan for first.Handle and second.Activate.
	instance, err = engine.Handle(t.Context(), workflow.Command{
		InstanceID: instance.ID,
		TaskID:     instance.Tasks[0].ID,
		ActorID:    instance.Tasks[0].Assignee,
		Name:       "decide",
	})
	if err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	if instance.Status != workflow.InstanceStatusCompleted || instance.CurrentNodeID != "end" {
		t.Fatalf("instance status/node = %q/%q, want completed/end", instance.Status, instance.CurrentNodeID)
	}

	// Each Engine operation prepared each config once; runtime used only the executors produced for that operation.
	prepares, activations, handles, legacyCalls := handler.counts()
	if !reflect.DeepEqual(prepares, map[string]int{"first": 2, "second": 2}) {
		t.Errorf("PrepareConfig() counts = %#v, want first=2 second=2", prepares)
	}
	if !reflect.DeepEqual(activations, map[string]int{"first": 1, "second": 1}) {
		t.Errorf("ActivatePrepared() counts = %#v, want first=1 second=1", activations)
	}
	if !reflect.DeepEqual(handles, map[string]int{"first": 1}) {
		t.Errorf("HandlePrepared() counts = %#v, want first=1", handles)
	}
	if legacyCalls != 0 {
		t.Errorf("legacy handler calls = %d, want 0", legacyCalls)
	}

	// Prepared executors remain request-local: the durable Definition still contains only the original canonical JSON.
	persistedJSON, err := json.Marshal(instance.Definition)
	if err != nil {
		t.Fatalf("json.Marshal(instance.Definition) error = %v", err)
	}
	if !slices.Equal(persistedJSON, canonicalJSON) {
		t.Errorf("persisted Definition JSON = %s, want canonical %s", persistedJSON, canonicalJSON)
	}
}

// TestEngineKeepsLegacyNodeHandlerConfigCompatible verifies existing custom adapters need no immediate migration.
func TestEngineKeepsLegacyNodeHandlerConfigCompatible(t *testing.T) {
	t.Parallel()

	// Register a handler that knows nothing about prepared execution and run its complete waiting-to-end lifecycle.
	handler := &legacyProbeHandler{}
	registry := workflow.NewRegistry()
	if err := registry.Register(legacyProbeKind, handler); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	builder := workflow.NewBuilder("legacy-handler-plan")
	builder.Start("start")
	builder.Node("legacy", legacyProbeKind, "legacy")
	builder.End("end")
	builder.Connect("start", "legacy", "")
	builder.Connect("legacy", "end", "done")
	definition, err := builder.Build()
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	engine := workflow.NewEngine(workflow.NewMemoryStore(), registry)
	instance, err := engine.Start(t.Context(), definition, workflow.StartRequest{
		ID:        "legacy-handler-plan-1",
		Initiator: "requester-a",
	})
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	instance, err = engine.Handle(t.Context(), workflow.Command{
		InstanceID: instance.ID,
		TaskID:     instance.Tasks[0].ID,
		ActorID:    instance.Tasks[0].Assignee,
		Name:       "decide",
	})
	if err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	if instance.Status != workflow.InstanceStatusCompleted {
		t.Fatalf("instance.Status = %q, want completed", instance.Status)
	}

	// Defensive compilation validates once per Engine operation and the compatibility executor supplies raw config.
	validations, activations, handles := handler.counts()
	if validations != 2 || activations != 1 || handles != 1 {
		t.Errorf(
			"legacy counts = validate:%d activate:%d handle:%d, want 2/1/1",
			validations,
			activations,
			handles,
		)
	}
}
