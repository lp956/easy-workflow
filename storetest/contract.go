// Package storetest verifies workflow Store adapters through their public persistence contract.
// It does not inspect adapter internals or prescribe database setup; callers own adapter lifecycle and cleanup.
package storetest

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	workflow "github.com/lvpeng/easy-workflow"
)

// Factory creates an isolated Store for one contract subtest.
//
// The supplied testing handle identifies the subtest that owns the adapter and may be used to register cleanup.
// Each invocation must return an empty, usable Store whose state is not shared with other invocations.
type Factory func(t *testing.T) workflow.Store

// Run applies the reusable observable behavior contract to a Store adapter.
//
// factory must return a fresh Store for each invocation and may use t.Cleanup for external resources.
// Contract failures are reported through nested subtests; Run does not access adapter-specific state.
func Run(t *testing.T, factory Factory) {
	t.Helper()

	// Each behavior receives an isolated adapter so failures cannot depend on subtest order.
	t.Run("create and load", func(t *testing.T) { runCreateAndLoadContract(t, factory) })
	t.Run("snapshot ownership", func(t *testing.T) { runSnapshotOwnershipContract(t, factory) })
	t.Run("compare and swap", func(t *testing.T) { runCompareAndSwapContract(t, factory) })
	t.Run("append-only audit", func(t *testing.T) { runAppendOnlyAuditContract(t, factory) })
	t.Run("context cancellation", func(t *testing.T) { runContextCancellationContract(t, factory) })
}

// runCreateAndLoadContract verifies insert-only creation and stable lookup errors.
//
// factory supplies an isolated adapter. Failures identify operation-level behavior through the public Store seam.
func runCreateAndLoadContract(t *testing.T, factory Factory) {
	t.Helper()
	t.Parallel()

	store := factory(t)
	instance := contractInstance("create-and-load", 1)
	if err := store.Create(t.Context(), instance); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	loaded, err := store.Load(t.Context(), instance.ID)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if !reflect.DeepEqual(loaded, contractInstance(instance.ID, 1)) {
		t.Fatalf("Load() = %#v, want created instance", loaded)
	}

	// Duplicate creation and missing lookup must expose portable errors without adapter-specific matching.
	if err := store.Create(t.Context(), contractInstance(instance.ID, 1)); !errors.Is(err, workflow.ErrInstanceExists) {
		t.Fatalf("duplicate Create() error = %v, want ErrInstanceExists", err)
	}
	if _, err := store.Load(t.Context(), "missing-instance"); !errors.Is(err, workflow.ErrInstanceNotFound) {
		t.Fatalf("missing Load() error = %v, want ErrInstanceNotFound", err)
	}
	if err := store.Save(t.Context(), contractInstance("missing-instance", 2), 1); !errors.Is(err, workflow.ErrInstanceNotFound) {
		t.Fatalf("Save(missing instance) error = %v, want ErrInstanceNotFound", err)
	}
}

// runSnapshotOwnershipContract verifies that Create, Load, and Save never share caller-mutable storage.
//
// The checks cover instance JSON, nested definition JSON, node and edge slices, tasks, and audit records.
func runSnapshotOwnershipContract(t *testing.T, factory Factory) {
	t.Helper()
	t.Parallel()

	// Mutating the Create input after success must not change the stored snapshot.
	store := factory(t)
	created := contractInstance("create-ownership", 1)
	if err := store.Create(t.Context(), created); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	mutateContractInstance(created)
	assertStoredInstance(t, store, contractInstance("create-ownership", 1))

	// Mutating one Load result must not change either storage or a later Load result.
	loaded, err := store.Load(t.Context(), "create-ownership")
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	mutateContractInstance(loaded)
	assertStoredInstance(t, store, contractInstance("create-ownership", 1))

	// Mutating the Save input after success must not change the committed replacement snapshot.
	saved := contractInstance("create-ownership", 2)
	if err := store.Save(t.Context(), saved, 1); err != nil {
		t.Fatalf("Save() error = %v", err)
	}
	mutateContractInstance(saved)
	assertStoredInstance(t, store, contractInstance("create-ownership", 2))
}

// runCompareAndSwapContract verifies successful replacement, stale-write rejection, and atomic contenders.
//
// A failed stale write must preserve the winning version and aggregate fields. Concurrent writers must produce
// exactly one success because both compare against the same durable version.
func runCompareAndSwapContract(t *testing.T, factory Factory) {
	t.Helper()
	t.Parallel()

	store := factory(t)
	original := contractInstance("compare-and-swap", 1)
	if err := store.Create(t.Context(), original); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	winner := contractInstance(original.ID, 2)
	winner.Data = []byte(`{"writer":"first"}`)
	if err := store.Save(t.Context(), winner, 1); err != nil {
		t.Fatalf("first Save() error = %v", err)
	}
	stale := contractInstance(original.ID, 2)
	stale.Data = []byte(`{"writer":"stale"}`)
	if err := store.Save(t.Context(), stale, 1); !errors.Is(err, workflow.ErrVersionConflict) {
		t.Fatalf("stale Save() error = %v, want ErrVersionConflict", err)
	}
	assertStoredInstance(t, store, winner)

	// A fresh adapter isolates the concurrent CAS result from the sequential stale-write check above.
	concurrentStore := factory(t)
	concurrentOriginal := contractInstance("concurrent-compare-and-swap", 1)
	if err := concurrentStore.Create(t.Context(), concurrentOriginal); err != nil {
		t.Fatalf("concurrent Create() error = %v", err)
	}
	assertConcurrentCAS(t, concurrentStore, concurrentOriginal.ID)
}

// runAppendOnlyAuditContract verifies that Save may append audit records but cannot rewrite committed history.
//
// A rejected rewrite must preserve the prior aggregate and durable version. A valid suffix is committed in order
// through the same public Save operation used for all other aggregate fields.
func runAppendOnlyAuditContract(t *testing.T, factory Factory) {
	t.Helper()
	t.Parallel()

	store := factory(t)
	original := contractInstance("append-only-audit", 1)
	if err := store.Create(t.Context(), original); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	rewrite := contractInstance(original.ID, 2)
	rewrite.Audit[0].Action = "rewritten"
	if err := store.Save(t.Context(), rewrite, 1); !errors.Is(err, workflow.ErrInvalidStoreInput) {
		t.Fatalf("audit rewrite Save() error = %v, want ErrInvalidStoreInput", err)
	}
	assertStoredInstance(t, store, original)

	// A new record extends rather than replaces the authoritative existing prefix.
	appended := contractInstance(original.ID, 2)
	appended.Audit = append(appended.Audit, workflow.AuditRecord{
		Action:  "approved",
		NodeID:  "approval",
		TaskID:  "task-1",
		ActorID: "reviewer-1",
		At:      time.Date(2026, 1, 2, 3, 5, 0, 0, time.UTC),
	})
	if err := store.Save(t.Context(), appended, 1); err != nil {
		t.Fatalf("append audit Save() error = %v", err)
	}
	assertStoredInstance(t, store, appended)
}

// runContextCancellationContract verifies that abandoned Create, Load, and Save calls retain context.Canceled.
//
// Every canceled mutation must leave the durable snapshot unchanged; the checks use a fresh adapter to avoid
// dependence on any adapter-specific rollback or transaction implementation.
func runContextCancellationContract(t *testing.T, factory Factory) {
	t.Helper()
	t.Parallel()

	store := factory(t)
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	instance := contractInstance("context-cancellation", 1)
	if err := store.Create(canceled, instance); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Create() error = %v, want context.Canceled", err)
	}
	if _, err := store.Load(t.Context(), instance.ID); !errors.Is(err, workflow.ErrInstanceNotFound) {
		t.Fatalf("Load() after canceled Create error = %v, want ErrInstanceNotFound", err)
	}

	if err := store.Create(t.Context(), instance); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if _, err := store.Load(canceled, instance.ID); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Load() error = %v, want context.Canceled", err)
	}
	replacement := contractInstance(instance.ID, 2)
	if err := store.Save(canceled, replacement, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Save() error = %v, want context.Canceled", err)
	}
	assertStoredInstance(t, store, instance)
}

// contractInstance returns an independently allocated aggregate containing every mutable Store field.
//
// id identifies the fixture and version is its optimistic concurrency token. Every call owns distinct slices,
// including Definition node configuration bytes, so expected values cannot alias adapter inputs.
func contractInstance(id workflow.InstanceID, version uint64) *workflow.Instance {
	return &workflow.Instance{
		ID: id,
		Definition: workflow.Definition{
			ID:      "definition-1",
			Version: 3,
			Nodes: []workflow.NodeDefinition{
				{ID: "start", Kind: workflow.KindStart},
				{ID: "approval", Kind: "approval", Config: []byte(`{"minimum":2}`)},
				{ID: "end", Kind: workflow.KindEnd},
			},
			Edges: []workflow.Edge{
				{From: "start", To: "approval"},
				{From: "approval", To: "end", Outcome: "approved"},
			},
		},
		Status:        workflow.InstanceStatusRunning,
		Initiator:     "initiator-1",
		CurrentNodeID: "approval",
		Data:          []byte(`{"amount":42}`),
		NodeState:     []byte(`{"approved":1}`),
		Tasks: []workflow.Task{
			{ID: "task-1", NodeID: "approval", Assignee: "reviewer-1", Status: workflow.TaskStatusActive},
		},
		Audit: []workflow.AuditRecord{
			{Action: "started", NodeID: "start", ActorID: "initiator-1", At: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)},
		},
		Version: version,
	}
}

// mutateContractInstance changes every mutable field used by the ownership fixture.
//
// instance must come from contractInstance or a conforming Store Load result. The mutation remains in bounds
// by construction and makes any shared backing storage visible through a later public Load.
func mutateContractInstance(instance *workflow.Instance) {
	instance.Definition.Nodes[0].ID = "mutated-start"
	instance.Definition.Nodes[1].Config[0] = '['
	instance.Definition.Edges[0].To = "mutated-target"
	instance.Data[0] = '['
	instance.NodeState[0] = '['
	instance.Tasks[0].Assignee = "mutated-reviewer"
	instance.Audit[0].Action = "mutated-action"
}

// assertStoredInstance loads id through Store and compares it with an independently allocated expected snapshot.
//
// It reports both the adapter error and the complete mismatched aggregate through t. The helper never inspects
// adapter internals and is therefore reusable for in-memory and durable implementations.
func assertStoredInstance(t *testing.T, store workflow.Store, expected *workflow.Instance) {
	t.Helper()

	actual, err := store.Load(t.Context(), expected.ID)
	if err != nil {
		t.Fatalf("Load(%q) error = %v", expected.ID, err)
	}
	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("Load(%q) = %#v, want %#v", expected.ID, actual, expected)
	}
}

// assertConcurrentCAS verifies that two replacements competing on version one commit exactly one snapshot.
//
// store must already contain id at version one. The helper starts both Save calls together, waits for completion,
// requires one ErrVersionConflict, and verifies the durable snapshot equals the successful candidate.
func assertConcurrentCAS(t *testing.T, store workflow.Store, id workflow.InstanceID) {
	t.Helper()

	first := contractInstance(id, 2)
	first.Data = []byte(`{"writer":"concurrent-first"}`)
	second := contractInstance(id, 2)
	second.Data = []byte(`{"writer":"concurrent-second"}`)
	type result struct {
		instance *workflow.Instance
		err      error
	}
	start := make(chan struct{})
	results := make(chan result, 2)
	var writers sync.WaitGroup
	writers.Add(2)
	for _, candidate := range []*workflow.Instance{first, second} {
		// Each writer waits at the shared gate so both CAS calls contend on the same durable version.
		go func() {
			defer writers.Done()
			<-start
			results <- result{instance: candidate, err: store.Save(t.Context(), candidate, 1)}
		}()
	}
	close(start)
	writers.Wait()
	close(results)

	// Classify both outcomes only after writers finish so test reporting never races with an active goroutine.
	var committed *workflow.Instance
	conflicts := 0
	for outcome := range results {
		switch {
		case outcome.err == nil:
			if committed != nil {
				t.Fatal("concurrent Save() succeeded more than once")
			}
			committed = outcome.instance
		case errors.Is(outcome.err, workflow.ErrVersionConflict):
			conflicts++
		default:
			t.Fatalf("concurrent Save() error = %v, want nil or ErrVersionConflict", outcome.err)
		}
	}
	if committed == nil || conflicts != 1 {
		t.Fatalf("concurrent Save() committed = %v, conflicts = %d; want one of each", committed != nil, conflicts)
	}
	assertStoredInstance(t, store, committed)
}
