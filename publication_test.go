// Package workflow_test verifies definition publication through public authoring and repository seams.
// Tests intentionally avoid repository internals so durable adapters can preserve the same contract.
package workflow_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"

	workflow "github.com/lvpeng/easy-workflow"
	"github.com/lvpeng/easy-workflow/approval"
)

// publicationMutationHandlerKind identifies the handler used to probe publication snapshot isolation.
const publicationMutationHandlerKind = "publication-mutation-test"

var (
	// errUnexpectedPublicationExecution reports that publication invoked a runtime-only handler method.
	errUnexpectedPublicationExecution = errors.New("publication mutation handler: unexpected execution")
)

// publicationMutationHandler mutates caller-owned authoring data during validation.
//
// The handler is scoped to one synchronous publication test. It retains the supplied definition only for that call,
// performs no I/O, and demonstrates that persistence must use the compiler-owned frozen snapshot.
type publicationMutationHandler struct {
	// definition is the caller-owned graph deliberately changed after compilation freezes its candidate.
	definition *workflow.Definition
}

// Validate corrupts the caller-owned graph while accepting the compiler-owned detached configuration.
func (h *publicationMutationHandler) Validate(json.RawMessage) error {
	h.definition.Nodes[len(h.definition.Nodes)-1].ID = "mutated-end"
	return nil
}

// Activate is unused because publication compiles but does not execute a definition.
func (*publicationMutationHandler) Activate(context.Context, workflow.ActivationInput) (workflow.NodeResult, error) {
	return workflow.NodeResult{}, errUnexpectedPublicationExecution
}

// Handle is unused because publication compiles but does not execute a definition.
func (*publicationMutationHandler) Handle(context.Context, workflow.CommandInput) (workflow.NodeResult, error) {
	return workflow.NodeResult{}, errUnexpectedPublicationExecution
}

// TestDefinitionPublisherAssignsVersionsAcrossAuthoringPaths verifies Builder and JSON inputs share one version sequence.
//
// The publisher owns Version: caller-supplied values are ignored, the first successful publication receives version 1,
// and each later publication for the same stable ID receives the next version. Failures are reported through testing.T.
func TestDefinitionPublisherAssignsVersionsAcrossAuthoringPaths(t *testing.T) {
	ctx := context.Background()
	definitions := workflow.NewMemoryDefinitionStore()
	publisher := workflow.NewDefinitionPublisher(definitions, workflow.NewRegistry())

	// Publish code-authored canonical data through the Definition seam.
	builder := workflow.NewBuilder("leave-request")
	builder.Start("start")
	builder.End("end")
	builder.Connect("start", "end", "")
	definition, err := builder.Build()
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	first, err := publisher.Publish(ctx, definition)
	if err != nil {
		t.Fatalf("Publish(builder) error = %v", err)
	}
	if first.ID != "leave-request" || first.Version != 1 {
		t.Fatalf("Publish(builder) identity = (%q, %d), want (%q, 1)", first.ID, first.Version, "leave-request")
	}

	// Re-publish equivalent web-authored JSON; its untrusted version must not influence allocation.
	definition.Version = 99
	data, err := json.Marshal(definition)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	second, err := publisher.PublishJSON(ctx, data)
	if err != nil {
		t.Fatalf("PublishJSON() error = %v", err)
	}
	if second.ID != "leave-request" || second.Version != 2 {
		t.Fatalf("PublishJSON() identity = (%q, %d), want (%q, 2)", second.ID, second.Version, "leave-request")
	}
}

// TestMemoryDefinitionStoreReadsImmutableVersions verifies exact and latest reads cannot mutate stored snapshots.
//
// Each returned Definition must be caller-owned, including node slices and RawMessage configuration bytes.
// Missing versions return ErrDefinitionNotFound rather than falling back to the latest stored version.
func TestMemoryDefinitionStoreReadsImmutableVersions(t *testing.T) {
	ctx := context.Background()
	registry := workflow.NewRegistry()
	if err := registry.Register(approval.Kind, approval.NewHandler()); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	definitions := workflow.NewMemoryDefinitionStore()
	publisher := workflow.NewDefinitionPublisher(definitions, registry)

	// Publish two distinguishable snapshots under the same stable ID.
	firstBuilder := workflow.NewBuilder("leave-request")
	firstBuilder.Start("start")
	firstBuilder.Node("approval", approval.Kind, approval.Config{
		Mode:      approval.ModeAny,
		Assignees: []workflow.ActorID{"manager-a"},
	})
	firstBuilder.End("end-v1")
	firstBuilder.Connect("start", "approval", "")
	firstBuilder.Connect("approval", "end-v1", approval.OutcomeApproved)
	firstDefinition, err := firstBuilder.Build()
	if err != nil {
		t.Fatalf("first Build() error = %v", err)
	}
	first, err := publisher.Publish(ctx, firstDefinition)
	if err != nil {
		t.Fatalf("first Publish() error = %v", err)
	}
	secondBuilder := workflow.NewBuilder("leave-request")
	secondBuilder.Start("start")
	secondBuilder.Node("approval", approval.Kind, approval.Config{
		Mode:      approval.ModeAny,
		Assignees: []workflow.ActorID{"manager-b"},
	})
	secondBuilder.End("end-v2")
	secondBuilder.Connect("start", "approval", "")
	secondBuilder.Connect("approval", "end-v2", approval.OutcomeApproved)
	secondDefinition, err := secondBuilder.Build()
	if err != nil {
		t.Fatalf("second Build() error = %v", err)
	}
	if _, err := publisher.Publish(ctx, secondDefinition); err != nil {
		t.Fatalf("second Publish() error = %v", err)
	}

	// Mutating inputs and publication results must not alter the exact stored version.
	firstDefinition.Nodes[2].ID = "mutated-input"
	firstDefinition.Nodes[1].Config[0] = 'x'
	first.Nodes[2].ID = "mutated-result"
	first.Nodes[1].Config[0] = 'y'
	loaded, err := definitions.Load(ctx, "leave-request", 1)
	if err != nil {
		t.Fatalf("Load(version 1) error = %v", err)
	}
	if loaded.Nodes[2].ID != "end-v1" || !json.Valid(loaded.Nodes[1].Config) {
		t.Fatalf("Load(version 1) snapshot = nodes %v, want intact end and approval config", loaded.Nodes)
	}

	// A loaded value is also defensive, while latest resolves only the greatest assigned version.
	loaded.Nodes[2].ID = "mutated-load"
	loaded.Nodes[1].Config[0] = 'z'
	reloaded, err := definitions.Load(ctx, "leave-request", 1)
	if err != nil {
		t.Fatalf("second Load(version 1) error = %v", err)
	}
	if reloaded.Nodes[2].ID != "end-v1" || !json.Valid(reloaded.Nodes[1].Config) {
		t.Fatalf("second Load(version 1) snapshot = nodes %v, want intact end and approval config", reloaded.Nodes)
	}
	latest, err := definitions.LoadLatest(ctx, "leave-request")
	if err != nil {
		t.Fatalf("LoadLatest() error = %v", err)
	}
	if latest.Version != 2 || latest.Nodes[2].ID != "end-v2" {
		t.Fatalf("LoadLatest() = version %d end %q, want version 2 end %q", latest.Version, latest.Nodes[2].ID, "end-v2")
	}
	if _, err := definitions.Load(ctx, "leave-request", 3); !errors.Is(err, workflow.ErrDefinitionNotFound) {
		t.Fatalf("Load(missing version) error = %v, want ErrDefinitionNotFound", err)
	}
}

// TestDefinitionPublisherRejectsInvalidDefinitionWithoutConsumingVersion verifies compilation precedes persistence.
//
// A definition whose business node has no registered handler must not become readable or advance the ID's
// version sequence. The next valid publication must still receive version 1 and no partial record may remain.
func TestDefinitionPublisherRejectsInvalidDefinitionWithoutConsumingVersion(t *testing.T) {
	ctx := context.Background()
	definitions := workflow.NewMemoryDefinitionStore()
	publisher := workflow.NewDefinitionPublisher(definitions, workflow.NewRegistry())

	// This graph is structurally valid but cannot compile because its business handler is absent.
	invalidBuilder := workflow.NewBuilder("leave-request")
	invalidBuilder.Start("start")
	invalidBuilder.Node("unregistered", "unregistered", nil)
	invalidBuilder.End("end")
	invalidBuilder.Connect("start", "unregistered", "")
	invalidBuilder.Connect("unregistered", "end", "done")
	invalid, err := invalidBuilder.Build()
	if err != nil {
		t.Fatalf("invalid Build() error = %v", err)
	}
	if _, err := publisher.Publish(ctx, invalid); !errors.Is(err, workflow.ErrInvalidDefinition) {
		t.Fatalf("Publish(invalid) error = %v, want ErrInvalidDefinition", err)
	}
	if _, err := definitions.LoadLatest(ctx, "leave-request"); !errors.Is(err, workflow.ErrDefinitionNotFound) {
		t.Fatalf("LoadLatest(after invalid publish) error = %v, want ErrDefinitionNotFound", err)
	}

	// A later executable graph proves the failed attempt left both storage and allocation untouched.
	validBuilder := workflow.NewBuilder("leave-request")
	validBuilder.Start("start")
	validBuilder.End("end")
	validBuilder.Connect("start", "end", "")
	valid, err := validBuilder.Build()
	if err != nil {
		t.Fatalf("valid Build() error = %v", err)
	}
	published, err := publisher.Publish(ctx, valid)
	if err != nil {
		t.Fatalf("Publish(valid) error = %v", err)
	}
	if published.Version != 1 {
		t.Fatalf("Publish(valid) version = %d, want 1", published.Version)
	}
}

// TestDefinitionPublisherPersistsCompiledSnapshot verifies validation-time mutations cannot bypass compilation.
func TestDefinitionPublisherPersistsCompiledSnapshot(t *testing.T) {
	t.Parallel()

	builder := workflow.NewBuilder("publication-snapshot")
	builder.Start("start")
	builder.Node("business", publicationMutationHandlerKind, nil)
	builder.End("end")
	builder.Connect("start", "business", "")
	builder.Connect("business", "end", "done")
	definition, err := builder.Build()
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}

	registry := workflow.NewRegistry()
	if err := registry.Register(publicationMutationHandlerKind, &publicationMutationHandler{definition: definition}); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	definitions := workflow.NewMemoryDefinitionStore()
	published, err := workflow.NewDefinitionPublisher(definitions, registry).Publish(t.Context(), definition)
	if err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	if published.Nodes[len(published.Nodes)-1].ID != "end" {
		t.Fatalf("published end node = %q, want compiler snapshot %q", published.Nodes[len(published.Nodes)-1].ID, "end")
	}
	if err := workflow.CompileDefinition(published, registry); err != nil {
		t.Fatalf("CompileDefinition(published) error = %v", err)
	}
}

// TestEngineStartsPublishedVersionAndFreezesSnapshot verifies startup resolves an exact immutable definition version.
//
// Publishing version 2 after version 1 starts must not alter the durable version-1 instance, its assigned task,
// or its embedded canonical Definition. A separate version-2 start must observe only version-2 configuration.
func TestEngineStartsPublishedVersionAndFreezesSnapshot(t *testing.T) {
	ctx := context.Background()
	registry := workflow.NewRegistry()
	if err := registry.Register(approval.Kind, approval.NewHandler()); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	definitions := workflow.NewMemoryDefinitionStore()
	publisher := workflow.NewDefinitionPublisher(definitions, registry)
	instances := workflow.NewMemoryStore()
	engine := workflow.NewEngine(instances, registry)

	// Version 1 assigns manager-a and starts an instance from that exact published identity.
	firstBuilder := workflow.NewBuilder("leave-request")
	firstBuilder.Start("start")
	firstBuilder.Node("approval", approval.Kind, approval.Config{
		Mode:      approval.ModeAny,
		Assignees: []workflow.ActorID{"manager-a"},
	})
	firstBuilder.End("end")
	firstBuilder.Connect("start", "approval", "")
	firstBuilder.Connect("approval", "end", approval.OutcomeApproved)
	firstDefinition, err := firstBuilder.Build()
	if err != nil {
		t.Fatalf("first Build() error = %v", err)
	}
	if _, err := publisher.Publish(ctx, firstDefinition); err != nil {
		t.Fatalf("first Publish() error = %v", err)
	}
	firstInstance, err := engine.StartPublished(ctx, definitions, "leave-request", 1, workflow.StartRequest{
		ID:        "leave-v1",
		Initiator: "employee-a",
	})
	if err != nil {
		t.Fatalf("StartPublished(version 1) error = %v", err)
	}

	// Version 2 changes the assignee without sharing mutable data with the already persisted instance.
	secondBuilder := workflow.NewBuilder("leave-request")
	secondBuilder.Start("start")
	secondBuilder.Node("approval", approval.Kind, approval.Config{
		Mode:      approval.ModeAny,
		Assignees: []workflow.ActorID{"manager-b"},
	})
	secondBuilder.End("end")
	secondBuilder.Connect("start", "approval", "")
	secondBuilder.Connect("approval", "end", approval.OutcomeApproved)
	secondDefinition, err := secondBuilder.Build()
	if err != nil {
		t.Fatalf("second Build() error = %v", err)
	}
	if _, err := publisher.Publish(ctx, secondDefinition); err != nil {
		t.Fatalf("second Publish() error = %v", err)
	}
	persistedFirst, err := instances.Load(ctx, firstInstance.ID)
	if err != nil {
		t.Fatalf("Load(version-1 instance) error = %v", err)
	}
	if persistedFirst.Definition.Version != 1 || len(persistedFirst.Tasks) != 1 || persistedFirst.Tasks[0].Assignee != "manager-a" {
		t.Fatalf(
			"version-1 instance snapshot = definition %d tasks %v, want version 1 assigned to manager-a",
			persistedFirst.Definition.Version,
			persistedFirst.Tasks,
		)
	}

	// An explicit version-2 start resolves the new snapshot rather than silently selecting another version.
	secondInstance, err := engine.StartPublished(ctx, definitions, "leave-request", 2, workflow.StartRequest{
		ID:        "leave-v2",
		Initiator: "employee-b",
	})
	if err != nil {
		t.Fatalf("StartPublished(version 2) error = %v", err)
	}
	if secondInstance.Definition.Version != 2 || len(secondInstance.Tasks) != 1 || secondInstance.Tasks[0].Assignee != "manager-b" {
		t.Fatalf(
			"version-2 instance snapshot = definition %d tasks %v, want version 2 assigned to manager-b",
			secondInstance.Definition.Version,
			secondInstance.Tasks,
		)
	}
}

// TestDefinitionPublisherAllocatesUniqueConcurrentVersions verifies atomic monotonic allocation per stable ID.
//
// Concurrent publications may complete in any order, but their assigned versions must form one gap-free sequence
// with no duplicates. The test shares a read-only Definition because Publish promises not to mutate caller data.
func TestDefinitionPublisherAllocatesUniqueConcurrentVersions(t *testing.T) {
	ctx := context.Background()
	definitions := workflow.NewMemoryDefinitionStore()
	publisher := workflow.NewDefinitionPublisher(definitions, workflow.NewRegistry())
	builder := workflow.NewBuilder("leave-request")
	builder.Start("start")
	builder.End("end")
	builder.Connect("start", "end", "")
	definition, err := builder.Build()
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}

	// Start all publications without serial coordination; the adapter owns allocation ordering.
	const publicationCount = 16
	versions := make(chan uint64, publicationCount)
	errorsSeen := make(chan error, publicationCount)
	var waitGroup sync.WaitGroup
	waitGroup.Add(publicationCount)
	for range publicationCount {
		go func() {
			defer waitGroup.Done()
			published, err := publisher.Publish(ctx, definition)
			if err != nil {
				errorsSeen <- err
				return
			}
			versions <- published.Version
		}()
	}
	waitGroup.Wait()
	close(versions)
	close(errorsSeen)

	// The assigned set, independent of completion order, must be exactly versions 1 through publicationCount.
	for err := range errorsSeen {
		t.Errorf("Publish() error = %v", err)
	}
	seen := make(map[uint64]bool, publicationCount)
	for version := range versions {
		seen[version] = true
	}
	for version := uint64(1); version <= publicationCount; version++ {
		if !seen[version] {
			t.Errorf("assigned versions = %v, missing %d", seen, version)
		}
	}
}
