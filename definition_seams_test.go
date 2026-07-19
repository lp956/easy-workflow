// Package workflow_test verifies publication and startup orchestration at their public dependency seams.
// Repository adapter behavior belongs to definitiontest; these tests cover only call ordering and error propagation.
package workflow_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"

	workflow "github.com/lvpeng/easy-workflow"
)

var (
	// errReaderUnavailable is the stable repository failure used to verify StartPublished error propagation.
	errReaderUnavailable = errors.New("reader unavailable")
	// errUnexpectedLoadLatest classifies an exact-version startup that incorrectly falls back to latest lookup.
	errUnexpectedLoadLatest = errors.New("unexpected LoadLatest call")
)

// observingDefinitionWriter records public writer calls and returns detached version-one snapshots.
//
// It is confined to one test goroutine and intentionally implements no repository policy beyond the behavior needed
// to observe Publisher ordering and candidate ownership at the DefinitionVersionWriter boundary.
type observingDefinitionWriter struct {
	// calls counts completed seam entries; tests use it to prove invalid publication never reaches persistence.
	calls int
	// received owns the most recent candidate snapshot so later caller mutation cannot change the observation.
	received *workflow.Definition
}

// observingDefinitionReader records exact identities and supplies a configured detached snapshot or error.
//
// It is confined to one test goroutine. definition remains test-owned; every successful Load returns a deep copy so
// StartPublished must establish its own frozen ownership boundary independently of the reader fake.
type observingDefinitionReader struct {
	// definition is the successful exact snapshot template; nil is allowed when err is configured.
	definition *workflow.Definition
	// err is returned from lookup unchanged to verify errors.Is propagation through StartPublished.
	err error
	// calls counts exact Load entries and excludes LoadLatest, which startup must never use.
	calls int
	// id records the most recent exact Definition identity requested by Engine.
	id string
	// version records the most recent exact positive version requested by Engine.
	version uint64
	// returned retains the exact caller-owned snapshot supplied to Engine so the test can mutate that boundary value.
	returned *workflow.Definition
}

// observingInstanceStore counts Create calls while rejecting unrelated Store operations.
//
// The fake is confined to the reader-error test. Its purpose is to prove command-side persistence is not attempted;
// it neither retains aggregates nor emulates MemoryStore behavior.
type observingInstanceStore struct {
	// createCalls counts attempts to make an Instance durable after Definition lookup.
	createCalls int
}

// TestDefinitionPublisherCompilesBeforeWriterInvocation verifies invalid input never crosses the writer seam.
//
// A structurally valid graph with an unregistered business kind must fail compilation without allocating a version.
// A later valid graph reaches the same writer once with cleared authoring Version metadata.
func TestDefinitionPublisherCompilesBeforeWriterInvocation(t *testing.T) {
	writer := &observingDefinitionWriter{}
	publisher := workflow.NewDefinitionPublisher(writer, workflow.NewRegistry())

	// The missing business handler makes compilation fail before persistence becomes eligible.
	invalidBuilder := workflow.NewBuilder("publisher-seam")
	invalidBuilder.Start("start")
	invalidBuilder.Node("task", "unregistered", nil)
	invalidBuilder.End("end")
	invalidBuilder.Connect("start", "task", "")
	invalidBuilder.Connect("task", "end", "done")
	invalid, err := invalidBuilder.Build()
	if err != nil {
		t.Fatalf("invalid Build() error = %v", err)
	}
	if _, err := publisher.Publish(t.Context(), invalid); !errors.Is(err, workflow.ErrInvalidDefinition) {
		t.Fatalf("Publish(invalid) error = %v, want ErrInvalidDefinition", err)
	}
	if writer.calls != 0 {
		t.Fatalf("writer calls after invalid publication = %d, want 0", writer.calls)
	}

	// A complete control-only graph compiles and crosses the writer boundary exactly once.
	validBuilder := workflow.NewBuilder("publisher-seam")
	validBuilder.Start("start")
	validBuilder.End("end")
	validBuilder.Connect("start", "end", "")
	valid, err := validBuilder.Build()
	if err != nil {
		t.Fatalf("valid Build() error = %v", err)
	}
	valid.Version = 42
	published, err := publisher.Publish(t.Context(), valid)
	if err != nil {
		t.Fatalf("Publish(valid) error = %v", err)
	}
	if writer.calls != 1 || writer.received == nil || writer.received.Version != 0 || published.Version != 1 {
		t.Fatalf(
			"Publish(valid) writer calls = %d candidate = %#v result version = %d, want one call with candidate version 0 and result version 1",
			writer.calls,
			writer.received,
			published.Version,
		)
	}
}

// TestEngineStartPublishedStopsOnReaderError verifies lookup failure precedes Instance Store creation.
//
// StartPublished must preserve the reader's stable error cause and must not attempt any command-side persistence when
// the exact Definition snapshot is absent or unavailable.
func TestEngineStartPublishedStopsOnReaderError(t *testing.T) {
	reader := &observingDefinitionReader{err: errReaderUnavailable}
	instances := &observingInstanceStore{}
	engine := workflow.NewEngine(instances, workflow.NewRegistry())

	_, err := engine.StartPublished(t.Context(), reader, "definition-a", 7, workflow.StartRequest{
		ID:        "instance-a",
		Initiator: "initiator-a",
	})
	if !errors.Is(err, errReaderUnavailable) {
		t.Fatalf("StartPublished() error = %v, want reader failure", err)
	}
	if reader.calls != 1 || reader.id != "definition-a" || reader.version != 7 {
		t.Fatalf("reader observation = calls %d ID %q version %d, want one exact lookup", reader.calls, reader.id, reader.version)
	}
	if instances.createCalls != 0 {
		t.Fatalf("Instance Store Create() calls after reader failure = %d, want 0", instances.createCalls)
	}
}

// TestEngineStartPublishedFreezesExactReaderSnapshot verifies startup embeds the exact loaded version defensively.
//
// The reader-owned Definition is mutated after startup; the returned and persisted Instance snapshots must retain the
// requested identity, version, nodes, and edges selected through DefinitionReader.Load.
func TestEngineStartPublishedFreezesExactReaderSnapshot(t *testing.T) {
	definition := startableDefinition("definition-frozen", 7, "end-original")
	reader := &observingDefinitionReader{definition: definition}
	instances := workflow.NewMemoryStore()
	engine := workflow.NewEngine(instances, workflow.NewRegistry())

	started, err := engine.StartPublished(t.Context(), reader, "definition-frozen", 7, workflow.StartRequest{
		ID:        "instance-frozen",
		Initiator: "initiator-a",
	})
	if err != nil {
		t.Fatalf("StartPublished() error = %v", err)
	}
	reader.returned.Nodes[1].ID = "mutated-reader-result"
	reader.returned.Edges[0].To = "mutated-reader-target"
	persisted, err := instances.Load(t.Context(), started.ID)
	if err != nil {
		t.Fatalf("Instance Store Load() error = %v", err)
	}
	if reader.calls != 1 || reader.id != "definition-frozen" || reader.version != 7 ||
		started.Definition.Version != 7 || started.Definition.Nodes[1].ID != "end-original" ||
		persisted.Definition.Version != 7 || persisted.Definition.Nodes[1].ID != "end-original" {
		t.Fatalf("StartPublished() reader = (%d, %q, %d), started = %#v persisted = %#v", reader.calls, reader.id, reader.version, started, persisted)
	}
}

// TestPublicDependencySeamsRejectTypedNil verifies interface-wrapped nil collaborators fail with stable boundary errors.
//
// The fixtures use valid control-only definitions and complete request identities so only dependency validation determines
// the result. No typed-nil method may be called, and no instance or definition persistence may be attempted.
func TestPublicDependencySeamsRejectTypedNil(t *testing.T) {
	t.Parallel()

	definition := startableDefinition("typed-nil-dependencies", 1, "end")
	t.Run("instance store", func(t *testing.T) {
		t.Parallel()

		var store *workflow.MemoryStore
		_, err := workflow.NewEngine(store, workflow.NewRegistry()).Start(t.Context(), definition, workflow.StartRequest{
			ID:        "typed-nil-store-1",
			Initiator: "initiator-a",
		})
		if !errors.Is(err, workflow.ErrInvalidEngine) {
			t.Fatalf("Start() error = %v, want ErrInvalidEngine", err)
		}
	})

	t.Run("definition reader", func(t *testing.T) {
		t.Parallel()

		var reader *observingDefinitionReader
		_, err := workflow.NewEngine(workflow.NewMemoryStore(), workflow.NewRegistry()).StartPublished(
			t.Context(),
			reader,
			definition.ID,
			definition.Version,
			workflow.StartRequest{ID: "typed-nil-reader-1", Initiator: "initiator-a"},
		)
		if !errors.Is(err, workflow.ErrInvalidStartRequest) {
			t.Fatalf("StartPublished() error = %v, want ErrInvalidStartRequest", err)
		}
	})

	t.Run("definition writer", func(t *testing.T) {
		t.Parallel()

		var writer *observingDefinitionWriter
		_, err := workflow.NewDefinitionPublisher(writer, workflow.NewRegistry()).Publish(t.Context(), definition)
		if !errors.Is(err, workflow.ErrInvalidPublisher) {
			t.Fatalf("Publish() error = %v, want ErrInvalidPublisher", err)
		}
	})
}

// CreateVersion records a detached candidate and returns a separately owned version-one snapshot.
//
// ctx cancellation is preserved. definition must be non-nil for this publisher seam test; the fake performs no
// validation or durable I/O and is not safe for concurrent use.
func (w *observingDefinitionWriter) CreateVersion(ctx context.Context, definition *workflow.Definition) (*workflow.Definition, error) {
	// Respect an abandoned request before recording any observable writer call.
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("test definition writer context: %w", err)
	}
	// Capture the candidate independently, then return another snapshot representing writer-assigned identity.
	w.calls++
	w.received = cloneTestDefinition(definition)
	published := cloneTestDefinition(definition)
	published.Version = 1
	return published, nil
}

// Load records one exact identity and returns the configured detached snapshot or error.
//
// ctx cancellation takes precedence. id and version are accepted verbatim so the test can verify Engine supplied the
// requested values; the fake performs no fallback and is not safe for concurrent use.
func (r *observingDefinitionReader) Load(ctx context.Context, id string, version uint64) (*workflow.Definition, error) {
	// Cancellation prevents the fake from recording a lookup that a real adapter must abandon.
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("test definition reader context: %w", err)
	}
	// Record the exact request before selecting the configured success or failure path.
	r.calls++
	r.id = id
	r.version = version
	if r.err != nil {
		return nil, r.err
	}
	r.returned = cloneTestDefinition(r.definition)
	return r.returned, nil
}

// LoadLatest rejects unexpected fallback because StartPublished may use only exact lookup.
func (r *observingDefinitionReader) LoadLatest(context.Context, string) (*workflow.Definition, error) {
	return nil, errUnexpectedLoadLatest
}

// Create records an unexpected persistence attempt without retaining caller-owned Instance data.
func (s *observingInstanceStore) Create(context.Context, *workflow.Instance) error {
	s.createCalls++
	return nil
}

// Load rejects unsupported observation because the reader-error test never creates an Instance.
func (s *observingInstanceStore) Load(context.Context, workflow.InstanceID) (*workflow.Instance, error) {
	return nil, workflow.ErrInstanceNotFound
}

// Save rejects unsupported mutation because the reader-error test never reaches command execution.
func (s *observingInstanceStore) Save(context.Context, *workflow.Instance, uint64) error {
	return workflow.ErrInvalidStoreInput
}

// startableDefinition returns a complete control-only graph with the requested immutable repository identity.
//
// id and endID must be non-empty, and version must be positive. The returned node and edge slices are caller-owned;
// the graph completes synchronously and requires no registered business handler.
func startableDefinition(id string, version uint64, endID string) *workflow.Definition {
	return &workflow.Definition{
		ID:      id,
		Version: version,
		Nodes: []workflow.NodeDefinition{
			{ID: "start", Kind: workflow.KindStart},
			{ID: endID, Kind: workflow.KindEnd},
		},
		Edges: []workflow.Edge{{From: "start", To: endID}},
	}
}

// cloneTestDefinition returns a detached Definition for seam fakes while preserving nil input.
//
// The clone owns node, edge, and configuration slices. It performs no validation and exists only to keep test-double
// ownership from masking Publisher or Engine behavior under observation.
func cloneTestDefinition(source *workflow.Definition) *workflow.Definition {
	// Preserve absence so a fake cannot fabricate a successful repository result.
	if source == nil {
		return nil
	}
	// Clone every mutable layer that crosses the public Definition ownership boundary.
	cloned := *source
	cloned.Nodes = slices.Clone(source.Nodes)
	for index := range cloned.Nodes {
		cloned.Nodes[index].Config = slices.Clone(source.Nodes[index].Config)
	}
	cloned.Edges = slices.Clone(source.Edges)
	return &cloned
}
