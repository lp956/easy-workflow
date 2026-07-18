// Package definitiontest verifies Definition repository adapters through public capability seams.
// It provides test infrastructure only: adapters own setup and cleanup, and production repository APIs stay unchanged.
package definitiontest

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"

	workflow "github.com/lvpeng/easy-workflow"
)

// WriterFactory creates an isolated DefinitionVersionWriter for one contract subtest.
//
// The testing handle identifies the subtest that owns adapter resources. Every invocation must return an empty,
// concurrency-safe writer whose state is not shared with other invocations; cleanup may be registered on t.
type WriterFactory func(t *testing.T) workflow.DefinitionVersionWriter

// ReaderFactory creates an isolated DefinitionReader preloaded with ordered fixture snapshots.
//
// fixtures contain explicit positive versions and are ordered by publication within each Definition ID. The factory
// must persist detached copies, return a concurrency-safe reader, and may register adapter cleanup on t.
type ReaderFactory func(t *testing.T, fixtures []*workflow.Definition) workflow.DefinitionReader

// RepositoryFactory creates isolated writer and reader capabilities backed by the same repository state.
//
// Each invocation must return concurrency-safe capabilities that share one initially empty adapter. They may be the
// same concrete value, but the contract intentionally exposes no combined production interface.
type RepositoryFactory func(t *testing.T) (workflow.DefinitionVersionWriter, workflow.DefinitionReader)

// RunWriter applies the reusable observable writer contract to a Definition repository adapter.
//
// factory must return a fresh empty writer for every invocation. Contract failures are reported through nested
// subtests, and no assertion depends on adapter storage, locks, transactions, or schema details.
func RunWriter(t *testing.T, factory WriterFactory) {
	t.Helper()

	// Each behavior receives isolated version state so subtest order cannot influence allocation.
	t.Run("monotonic versions per definition", func(t *testing.T) {
		runWriterVersionAllocationContract(t, factory)
	})
	t.Run("validation and failure atomicity", func(t *testing.T) {
		runWriterFailureAtomicityContract(t, factory)
	})
	t.Run("detached input and result", func(t *testing.T) {
		runWriterOwnershipContract(t, factory)
	})
	t.Run("concurrent unique versions", func(t *testing.T) {
		runWriterConcurrencyContract(t, factory)
	})
}

// RunReader applies the reusable observable reader contract to a Definition repository adapter.
//
// factory must return a fresh reader containing exactly the supplied fixtures for every invocation. Contract
// failures are reported through nested subtests without relying on the adapter's write API or implementation.
func RunReader(t *testing.T, factory ReaderFactory) {
	t.Helper()

	// Each reader behavior receives its own seeded adapter so mutations and concurrency cannot cross subtests.
	t.Run("exact and latest lookup", func(t *testing.T) {
		runReaderLookupContract(t, factory)
	})
	t.Run("missing identity", func(t *testing.T) {
		runReaderMissingContract(t, factory)
	})
	t.Run("context cancellation", func(t *testing.T) {
		runReaderCancellationContract(t, factory)
	})
	t.Run("detached results", func(t *testing.T) {
		runReaderOwnershipContract(t, factory)
	})
	t.Run("concurrent reads", func(t *testing.T) {
		runReaderConcurrencyContract(t, factory)
	})
}

// RunRepository applies the combined publication-read lifecycle contract to one Definition repository adapter.
//
// factory must return fresh capabilities over shared empty state for every invocation. The suite verifies behavior
// observable only when writer and reader are composed, without introducing a production repository abstraction.
func RunRepository(t *testing.T, factory RepositoryFactory) {
	t.Helper()

	// Each lifecycle behavior owns an empty repository so version history and mutations cannot leak across subtests.
	t.Run("publish and read lifecycle", func(t *testing.T) {
		runRepositoryLifecycleContract(t, factory)
	})
	t.Run("failed publication leaves no gap", func(t *testing.T) {
		runRepositoryFailureAtomicityContract(t, factory)
	})
	t.Run("caller mutation isolation", func(t *testing.T) {
		runRepositoryOwnershipContract(t, factory)
	})
	t.Run("reads during publication", func(t *testing.T) {
		runRepositoryConcurrentLifecycleContract(t, factory)
	})
}

// runRepositoryLifecycleContract verifies published snapshots are available through exact and latest lookup.
//
// Two versions for one ID and one version for another establish monotonic history, exact selection, latest selection,
// and independent sequences through capabilities that share only their adapter's public state.
func runRepositoryLifecycleContract(t *testing.T, factory RepositoryFactory) {
	t.Helper()
	t.Parallel()

	writer, reader := factory(t)
	first, err := writer.CreateVersion(t.Context(), contractDefinition("lifecycle-a", "end-a-v1"))
	if err != nil {
		t.Fatalf("first CreateVersion() error = %v", err)
	}
	second, err := writer.CreateVersion(t.Context(), contractDefinition("lifecycle-a", "end-a-v2"))
	if err != nil {
		t.Fatalf("second CreateVersion() error = %v", err)
	}
	independent, err := writer.CreateVersion(t.Context(), contractDefinition("lifecycle-b", "end-b-v1"))
	if err != nil {
		t.Fatalf("independent CreateVersion() error = %v", err)
	}
	assertContractDefinition(t, first, "lifecycle-a", 1, "end-a-v1")
	assertContractDefinition(t, second, "lifecycle-a", 2, "end-a-v2")
	assertContractDefinition(t, independent, "lifecycle-b", 1, "end-b-v1")

	// Read every durable identity back through the reader rather than inferring persistence from writer results.
	exact, err := reader.Load(t.Context(), "lifecycle-a", 1)
	if err != nil {
		t.Fatalf("Load(version 1) error = %v", err)
	}
	assertContractDefinition(t, exact, "lifecycle-a", 1, "end-a-v1")
	latest, err := reader.LoadLatest(t.Context(), "lifecycle-a")
	if err != nil {
		t.Fatalf("LoadLatest() error = %v", err)
	}
	assertContractDefinition(t, latest, "lifecycle-a", 2, "end-a-v2")
}

// runRepositoryFailureAtomicityContract verifies a canceled publication changes neither storage nor allocation.
//
// The reader must report absence immediately after failure, and the next successful publication must be readable as
// version one. Both errors retain their stable context and repository sentinels through errors.Is.
func runRepositoryFailureAtomicityContract(t *testing.T, factory RepositoryFactory) {
	t.Helper()
	t.Parallel()

	writer, reader := factory(t)
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := writer.CreateVersion(canceled, contractDefinition("repository-failure", "end-failed")); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled CreateVersion() error = %v, want context.Canceled", err)
	}
	if _, err := reader.LoadLatest(t.Context(), "repository-failure"); !errors.Is(err, workflow.ErrDefinitionNotFound) {
		t.Fatalf("LoadLatest() after failed publication error = %v, want ErrDefinitionNotFound", err)
	}

	// Successful retry proves the failed call reserved neither a version nor a partial readable snapshot.
	if _, err := writer.CreateVersion(t.Context(), contractDefinition("repository-failure", "end-success")); err != nil {
		t.Fatalf("CreateVersion() after failure error = %v", err)
	}
	loaded, err := reader.Load(t.Context(), "repository-failure", 1)
	if err != nil {
		t.Fatalf("Load(version 1) after failure error = %v", err)
	}
	assertContractDefinition(t, loaded, "repository-failure", 1, "end-success")
}

// runRepositoryOwnershipContract verifies every write and read boundary returns detached mutable data.
//
// The fixture input, writer result, exact result, and latest result are each mutated before a fresh read. Repository
// history must retain its original node slice, edge slice, configuration bytes, identity, and assigned version.
func runRepositoryOwnershipContract(t *testing.T, factory RepositoryFactory) {
	t.Helper()
	t.Parallel()

	writer, reader := factory(t)
	input := contractDefinition("repository-ownership", "end-original")
	published, err := writer.CreateVersion(t.Context(), input)
	if err != nil {
		t.Fatalf("CreateVersion() error = %v", err)
	}
	mutateContractDefinition(input)
	mutateContractDefinition(published)
	assertRepositorySnapshot(t, reader, "repository-ownership", 1, "end-original")

	// Exact and latest results must be independent from both storage and each other.
	exact, err := reader.Load(t.Context(), "repository-ownership", 1)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	mutateContractDefinition(exact)
	latest, err := reader.LoadLatest(t.Context(), "repository-ownership")
	if err != nil {
		t.Fatalf("LoadLatest() error = %v", err)
	}
	mutateContractDefinition(latest)
	assertRepositorySnapshot(t, reader, "repository-ownership", 1, "end-original")
}

// runRepositoryConcurrentLifecycleContract verifies reads observe complete snapshots while publications append.
//
// One seed guarantees every read can succeed; bounded writers then append identical immutable content while readers
// alternate latest and exact lookup. Results may observe any committed version but never partial or mismatched data.
func runRepositoryConcurrentLifecycleContract(t *testing.T, factory RepositoryFactory) {
	t.Helper()
	t.Parallel()

	// Counts create overlapping work while keeping remote or database adapter contract runs predictably bounded.
	const publicationCount = 16
	const readerCount = 16
	const readsPerReader = 16

	writer, reader := factory(t)
	if _, err := writer.CreateVersion(t.Context(), contractDefinition("concurrent-lifecycle", "end-stable")); err != nil {
		t.Fatalf("seed CreateVersion() error = %v", err)
	}
	start := make(chan struct{})
	operationErrors := make(chan error, publicationCount+readerCount)
	var operations sync.WaitGroup
	operations.Add(publicationCount - 1 + readerCount) // Version one is seeded; remaining writers allocate through 16.
	for index := 1; index < publicationCount; index++ {
		// Writers publish independently owned values after the same start signal as concurrent readers.
		go func() {
			defer operations.Done()
			<-start
			if _, err := writer.CreateVersion(t.Context(), contractDefinition("concurrent-lifecycle", "end-stable")); err != nil {
				operationErrors <- err
			}
		}()
	}
	for index := 0; index < readerCount; index++ {
		// Each reader pins the observed latest identity with an exact follow-up read.
		go func() {
			defer operations.Done()
			<-start
			for attempt := 0; attempt < readsPerReader; attempt++ {
				latest, err := reader.LoadLatest(t.Context(), "concurrent-lifecycle")
				if err != nil {
					operationErrors <- err
					return
				}
				if err := validateConcurrentSnapshot(latest, publicationCount); err != nil {
					operationErrors <- err
					return
				}
				exact, err := reader.Load(t.Context(), latest.ID, latest.Version)
				if err != nil {
					operationErrors <- err
					return
				}
				if err := validateConcurrentSnapshot(exact, publicationCount); err != nil {
					operationErrors <- err
					return
				}
			}
		}()
	}
	close(start)
	operations.Wait()
	close(operationErrors)

	// Assert after all goroutines exit, then verify the final append-only version is gap-free and complete.
	for err := range operationErrors {
		t.Fatalf("concurrent repository operation error = %v", err)
	}
	latest, err := reader.LoadLatest(t.Context(), "concurrent-lifecycle")
	if err != nil {
		t.Fatalf("final LoadLatest() error = %v", err)
	}
	assertContractDefinition(t, latest, "concurrent-lifecycle", publicationCount, "end-stable")
}

// runReaderLookupContract verifies exact lookup never falls forward and latest selects the greatest version.
//
// The fixtures include two versions for one ID and one independent ID. Assertions use literal identities and content
// so the contract does not reproduce an adapter's indexing or selection algorithm.
func runReaderLookupContract(t *testing.T, factory ReaderFactory) {
	t.Helper()
	t.Parallel()

	reader := factory(t, []*workflow.Definition{
		versionedContractDefinition("reader-a", 1, "end-a-v1"),
		versionedContractDefinition("reader-a", 2, "end-a-v2"),
		versionedContractDefinition("reader-b", 1, "end-b-v1"),
	})
	exact, err := reader.Load(t.Context(), "reader-a", 1)
	if err != nil {
		t.Fatalf("Load(version 1) error = %v", err)
	}
	assertContractDefinition(t, exact, "reader-a", 1, "end-a-v1")
	latest, err := reader.LoadLatest(t.Context(), "reader-a")
	if err != nil {
		t.Fatalf("LoadLatest() error = %v", err)
	}
	assertContractDefinition(t, latest, "reader-a", 2, "end-a-v2")
	independent, err := reader.LoadLatest(t.Context(), "reader-b")
	if err != nil {
		t.Fatalf("independent LoadLatest() error = %v", err)
	}
	assertContractDefinition(t, independent, "reader-b", 1, "end-b-v1")

	// Requesting the next absent version must report absence instead of returning the current latest snapshot.
	if _, err := reader.Load(t.Context(), "reader-a", 3); !errors.Is(err, workflow.ErrDefinitionNotFound) {
		t.Fatalf("Load(missing version) error = %v, want ErrDefinitionNotFound", err)
	}
}

// runReaderMissingContract verifies unknown, empty, and zero-version identities share one sentinel.
//
// The public contract classifies every absent repository identity as ErrDefinitionNotFound while allowing adapters
// to wrap the sentinel with operation-specific context.
func runReaderMissingContract(t *testing.T, factory ReaderFactory) {
	t.Helper()
	t.Parallel()

	reader := factory(t, nil)
	checks := []struct {
		name string
		load func() error
	}{
		{name: "unknown exact", load: func() error { _, err := reader.Load(t.Context(), "missing", 1); return err }},
		{name: "zero version", load: func() error { _, err := reader.Load(t.Context(), "missing", 0); return err }},
		{name: "empty exact ID", load: func() error { _, err := reader.Load(t.Context(), "", 1); return err }},
		{name: "unknown latest", load: func() error { _, err := reader.LoadLatest(t.Context(), "missing"); return err }},
		{name: "empty latest ID", load: func() error { _, err := reader.LoadLatest(t.Context(), ""); return err }},
	}
	for _, check := range checks {
		if err := check.load(); !errors.Is(err, workflow.ErrDefinitionNotFound) {
			t.Errorf("%s error = %v, want ErrDefinitionNotFound", check.name, err)
		}
	}
}

// runReaderCancellationContract verifies exact and latest reads preserve a canceled context cause.
//
// The seeded snapshot proves cancellation wins over otherwise successful lookup. Reads have no repository side
// effects, and subsequent background-context calls must still retrieve the original data.
func runReaderCancellationContract(t *testing.T, factory ReaderFactory) {
	t.Helper()
	t.Parallel()

	reader := factory(t, []*workflow.Definition{
		versionedContractDefinition("reader-cancellation", 1, "end-original"),
	})
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := reader.Load(canceled, "reader-cancellation", 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Load() error = %v, want context.Canceled", err)
	}
	if _, err := reader.LoadLatest(canceled, "reader-cancellation"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled LoadLatest() error = %v, want context.Canceled", err)
	}
	loaded, err := reader.Load(t.Context(), "reader-cancellation", 1)
	if err != nil {
		t.Fatalf("Load() after cancellation error = %v", err)
	}
	assertContractDefinition(t, loaded, "reader-cancellation", 1, "end-original")
}

// runReaderOwnershipContract verifies seeding, exact results, and latest results never share mutable storage.
//
// Nodes, edges, and configuration bytes are mutated at every caller-owned boundary, then observed again only through
// DefinitionReader. This keeps the test portable across memory, database, filesystem, and remote adapters.
func runReaderOwnershipContract(t *testing.T, factory ReaderFactory) {
	t.Helper()
	t.Parallel()

	fixture := versionedContractDefinition("reader-ownership", 1, "end-original")
	reader := factory(t, []*workflow.Definition{fixture})
	mutateContractDefinition(fixture)

	// The seed input may be released or reused immediately after factory setup without rewriting repository history.
	exact, err := reader.Load(t.Context(), "reader-ownership", 1)
	if err != nil {
		t.Fatalf("Load() after seed mutation error = %v", err)
	}
	assertContractDefinition(t, exact, "reader-ownership", 1, "end-original")
	mutateContractDefinition(exact)
	reloaded, err := reader.Load(t.Context(), "reader-ownership", 1)
	if err != nil {
		t.Fatalf("Load() after exact-result mutation error = %v", err)
	}
	assertContractDefinition(t, reloaded, "reader-ownership", 1, "end-original")

	// Latest returns a separately owned snapshot even when it selects the same stored version as exact lookup.
	latest, err := reader.LoadLatest(t.Context(), "reader-ownership")
	if err != nil {
		t.Fatalf("LoadLatest() error = %v", err)
	}
	mutateContractDefinition(latest)
	latestAgain, err := reader.LoadLatest(t.Context(), "reader-ownership")
	if err != nil {
		t.Fatalf("LoadLatest() after result mutation error = %v", err)
	}
	assertContractDefinition(t, latestAgain, "reader-ownership", 1, "end-original")
}

// runReaderConcurrencyContract verifies simultaneous exact and latest loads return complete snapshots.
//
// The bounded readers start together and report errors through a channel. The contract requires concurrency safety
// while leaving connection pools, transactions, read locks, and snapshot-isolation choices to each adapter.
func runReaderConcurrencyContract(t *testing.T, factory ReaderFactory) {
	t.Helper()
	t.Parallel()

	// This fixed count creates overlapping reads without making durable-adapter suites unbounded.
	const readerCount = 16

	reader := factory(t, []*workflow.Definition{
		versionedContractDefinition("concurrent-reader", 1, "end-v1"),
		versionedContractDefinition("concurrent-reader", 2, "end-v2"),
	})
	start := make(chan struct{})
	readErrors := make(chan error, readerCount)
	var readers sync.WaitGroup
	readers.Add(readerCount)
	for index := 0; index < readerCount; index++ {
		// Every reader checks both lookup modes after the shared gate opens.
		go func() {
			defer readers.Done()
			<-start
			exact, err := reader.Load(t.Context(), "concurrent-reader", 1)
			if err != nil {
				readErrors <- err
				return
			}
			latest, err := reader.LoadLatest(t.Context(), "concurrent-reader")
			if err != nil {
				readErrors <- err
				return
			}
			if exact.Version != 1 || exact.Nodes[2].ID != "end-v1" || latest.Version != 2 || latest.Nodes[2].ID != "end-v2" {
				readErrors <- fmt.Errorf("incomplete snapshots: exact %#v latest %#v", exact, latest)
			}
		}()
	}
	close(start)
	readers.Wait()
	close(readErrors)
	for err := range readErrors {
		t.Fatalf("concurrent reader error = %v", err)
	}
}

// runWriterFailureAtomicityContract verifies invalid and canceled attempts consume no version.
//
// The adapter must classify invalid input with ErrInvalidDefinitionStore and preserve context.Canceled. After every
// rejected attempt, the first valid publication for that ID must still receive version one.
func runWriterFailureAtomicityContract(t *testing.T, factory WriterFactory) {
	t.Helper()
	t.Parallel()

	writer := factory(t)
	if _, err := writer.CreateVersion(t.Context(), nil); !errors.Is(err, workflow.ErrInvalidDefinitionStore) {
		t.Fatalf("CreateVersion(nil) error = %v, want ErrInvalidDefinitionStore", err)
	}
	if _, err := writer.CreateVersion(t.Context(), contractDefinition("", "end")); !errors.Is(err, workflow.ErrInvalidDefinitionStore) {
		t.Fatalf("CreateVersion(empty ID) error = %v, want ErrInvalidDefinitionStore", err)
	}

	// Cancellation is checked before allocation, so an abandoned request cannot reserve the first version.
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := writer.CreateVersion(canceled, contractDefinition("failure-atomicity", "end-canceled")); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled CreateVersion() error = %v, want context.Canceled", err)
	}
	published, err := writer.CreateVersion(t.Context(), contractDefinition("failure-atomicity", "end-success"))
	if err != nil {
		t.Fatalf("CreateVersion() after failures error = %v", err)
	}
	if published.Version != 1 {
		t.Fatalf("CreateVersion() after failures version = %d, want 1", published.Version)
	}
}

// runWriterOwnershipContract verifies CreateVersion does not share mutable data with its input or result.
//
// The test mutates nodes, edges, and configuration bytes on each side of the call. Immediate comparisons prove the
// returned snapshot and input own distinct storage without reaching into repository internals.
func runWriterOwnershipContract(t *testing.T, factory WriterFactory) {
	t.Helper()
	t.Parallel()

	writer := factory(t)
	input := contractDefinition("writer-ownership", "end-original")
	published, err := writer.CreateVersion(t.Context(), input)
	if err != nil {
		t.Fatalf("CreateVersion() error = %v", err)
	}

	// Mutating the caller input after success must not alter the writer result snapshot.
	mutateContractDefinition(input)
	assertContractDefinition(t, published, "writer-ownership", 1, "end-original")

	// Mutating the returned snapshot must not flow backward into the caller's already distinct data.
	published.Nodes[0].ID = "mutated-result-start"
	published.Nodes[1].Config[0] = '['
	published.Edges[0].To = "mutated-result-target"
	if input.Nodes[0].ID != "mutated-start" || input.Nodes[1].Config[0] != '[' || input.Edges[0].To != "mutated-target" {
		t.Fatalf("CreateVersion() result mutation changed input = %#v", input)
	}
}

// runWriterConcurrencyContract verifies simultaneous calls allocate one gap-free version set for a stable ID.
//
// The contract constrains only observable results: implementation-specific locks, transactions, and retry policies
// remain free. Every goroutine starts at one gate and reports through a buffered channel before assertions run.
func runWriterConcurrencyContract(t *testing.T, factory WriterFactory) {
	t.Helper()
	t.Parallel()

	// This fixed count is large enough to create contention while keeping every adapter contract run bounded.
	const publicationCount = 16

	writer := factory(t)
	start := make(chan struct{})
	versions := make(chan uint64, publicationCount)
	errorsByCall := make(chan error, publicationCount)
	var publishers sync.WaitGroup
	publishers.Add(publicationCount)
	for index := 0; index < publicationCount; index++ {
		// Each publication owns its Definition and waits at the shared gate before crossing the writer seam.
		go func() {
			defer publishers.Done()
			<-start
			published, err := writer.CreateVersion(t.Context(), contractDefinition("concurrent-writer", "end"))
			if err != nil {
				errorsByCall <- err
				return
			}
			versions <- published.Version
		}()
	}
	close(start)
	publishers.Wait()
	close(versions)
	close(errorsByCall)

	// Collect only after every goroutine exits so test failure reporting remains single-threaded.
	for err := range errorsByCall {
		t.Fatalf("concurrent CreateVersion() error = %v", err)
	}
	actual := make([]uint64, 0, publicationCount)
	for version := range versions {
		actual = append(actual, version)
	}
	sort.Slice(actual, func(i int, j int) bool { return actual[i] < actual[j] })
	if len(actual) != publicationCount {
		t.Fatalf("concurrent CreateVersion() result count = %d, want %d", len(actual), publicationCount)
	}
	for index, version := range actual {
		want := uint64(index + 1) // Sorted position zero represents the first positive, gap-free version.
		if version != want {
			t.Fatalf("concurrent CreateVersion() versions = %v, want gap-free 1..%d", actual, publicationCount)
		}
	}
}

// runWriterVersionAllocationContract verifies positive monotonic versions and independent per-ID sequences.
//
// factory supplies an empty adapter. The test observes only CreateVersion results and compares them with literal
// contract versions; adapter-specific allocation mechanisms remain unconstrained.
func runWriterVersionAllocationContract(t *testing.T, factory WriterFactory) {
	t.Helper()
	t.Parallel()

	writer := factory(t)
	first, err := writer.CreateVersion(t.Context(), contractDefinition("definition-a", "end-a-v1"))
	if err != nil {
		t.Fatalf("first CreateVersion() error = %v", err)
	}
	second, err := writer.CreateVersion(t.Context(), contractDefinition("definition-a", "end-a-v2"))
	if err != nil {
		t.Fatalf("second CreateVersion() error = %v", err)
	}
	independent, err := writer.CreateVersion(t.Context(), contractDefinition("definition-b", "end-b-v1"))
	if err != nil {
		t.Fatalf("independent CreateVersion() error = %v", err)
	}
	if first.Version != 1 || second.Version != 2 || independent.Version != 1 {
		t.Fatalf(
			"CreateVersion() versions = (%d, %d, %d), want (1, 2, 1)",
			first.Version,
			second.Version,
			independent.Version,
		)
	}
}

// contractDefinition returns an independently allocated canonical snapshot with mutable nodes, edges, and config.
//
// id is the repository identity and endID distinguishes versions. Version is deliberately non-zero so the writer
// contract proves allocation ignores caller metadata. The caller owns every returned slice and JSON byte.
func contractDefinition(id string, endID string) *workflow.Definition {
	return &workflow.Definition{
		ID:      id,
		Version: 99,
		Nodes: []workflow.NodeDefinition{
			{ID: "start", Kind: workflow.KindStart},
			{ID: "task", Kind: "contract-task", Config: []byte(`{"assignee":"reviewer"}`)},
			{ID: endID, Kind: workflow.KindEnd},
		},
		Edges: []workflow.Edge{
			{From: "start", To: "task"},
			{From: "task", To: endID, Outcome: "done"},
		},
	}
}

// versionedContractDefinition returns one reader fixture with an explicit repository version.
//
// id and endID retain contractDefinition ownership semantics. version must be positive and contiguous within the
// fixture sequence for one ID because ReaderFactory implementations seed snapshots in publication order.
func versionedContractDefinition(id string, version uint64, endID string) *workflow.Definition {
	definition := contractDefinition(id, endID)
	definition.Version = version
	return definition
}

// mutateContractDefinition changes every mutable field exercised by the repository ownership contract.
//
// definition must come from contractDefinition or a conforming repository result. Mutations stay in bounds by
// construction and make shared node, edge, or configuration storage visible through a later public observation.
func mutateContractDefinition(definition *workflow.Definition) {
	definition.Nodes[0].ID = "mutated-start"
	definition.Nodes[1].Config[0] = '['
	definition.Edges[0].To = "mutated-target"
}

// assertContractDefinition compares a repository result with independently known identity and content literals.
//
// actual remains caller-owned. id, version, and endID describe the expected published snapshot; mismatches are
// reported through t without inspecting adapter state or retaining the snapshot beyond the current subtest.
func assertContractDefinition(t *testing.T, actual *workflow.Definition, id string, version uint64, endID string) {
	t.Helper()

	// Reject absence and partial aggregate shapes before indexing any adapter-owned result fields.
	if actual == nil {
		t.Fatal("Definition snapshot = nil, want non-nil")
	}
	if len(actual.Nodes) != 3 || len(actual.Edges) != 2 || len(actual.Nodes[1].Config) == 0 {
		t.Fatalf("Definition snapshot shape = %d nodes, %d edges, task config length %d; want 3, 2, and non-empty", len(actual.Nodes), len(actual.Edges), definitionConfigLength(actual))
	}
	// Compare identity and mutable content with literals independent from repository selection logic.
	if actual.ID != id || actual.Version != version || actual.Nodes[0].ID != "start" ||
		actual.Nodes[1].Config[0] != '{' || actual.Nodes[2].ID != endID || actual.Edges[0].To != "task" {
		t.Fatalf(
			"Definition snapshot = %#v, want ID %q version %d with intact mutable content ending at %q",
			actual,
			id,
			version,
			endID,
		)
	}
}

// definitionConfigLength safely reports the task fixture's configuration length for failure diagnostics.
//
// definition may contain fewer than two nodes; zero then denotes that no task configuration can be observed. The
// helper performs no mutation and exists only to keep a malformed adapter result from panicking a contract test.
func definitionConfigLength(definition *workflow.Definition) int {
	if definition == nil || len(definition.Nodes) < 2 {
		return 0
	}
	return len(definition.Nodes[1].Config)
}

// assertRepositorySnapshot loads one exact snapshot and compares it with independent contract literals.
//
// reader is the only observation path. id, version, and endID name the expected immutable history entry; failures
// report through t and do not retain or mutate the adapter result.
func assertRepositorySnapshot(
	t *testing.T,
	reader workflow.DefinitionReader,
	id string,
	version uint64,
	endID string,
) {
	t.Helper()

	actual, err := reader.Load(t.Context(), id, version)
	if err != nil {
		t.Fatalf("Load(%q, %d) error = %v", id, version, err)
	}
	assertContractDefinition(t, actual, id, version, endID)
}

// validateConcurrentSnapshot checks one read-during-write result without using testing.T from a goroutine.
//
// definition must be a caller-owned snapshot for the concurrent lifecycle fixture. maxVersion is the inclusive
// bounded publication count; nil, partial, out-of-range, or cross-version content returns a descriptive error.
func validateConcurrentSnapshot(definition *workflow.Definition, maxVersion uint64) error {
	// Nil is never a complete committed repository snapshot.
	if definition == nil {
		return errors.New("concurrent Definition snapshot is nil")
	}
	// A complete snapshot has the fixture's full graph and a version committed within the bounded publication set.
	if definition.ID != "concurrent-lifecycle" || definition.Version == 0 || definition.Version > maxVersion ||
		len(definition.Nodes) != 3 || len(definition.Edges) != 2 || len(definition.Nodes[1].Config) == 0 ||
		definition.Nodes[0].ID != "start" || definition.Nodes[1].Config[0] != '{' ||
		definition.Nodes[2].ID != "end-stable" || definition.Edges[0].To != "task" {
		return fmt.Errorf("incomplete concurrent Definition snapshot: %#v", definition)
	}
	return nil
}
