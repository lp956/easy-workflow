// Package workflow_test verifies cancellation at process-local persistence ownership boundaries.
// The tests use public adapters and deterministic contexts rather than reaching into implementation locks.
package workflow_test

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	workflow "github.com/lvpeng/easy-workflow"
)

// cancelOnErrContext becomes canceled on a configured call to Err.
//
// The context is concurrency-safe, performs no I/O, and closes Done exactly once when the configured observation is
// reached. Deadline and Value remain empty because this probe controls only cancellation observations.
type cancelOnErrContext struct {
	// cancelOn is the one-based Err call that observes cancellation.
	cancelOn int32
	// calls counts concurrent Err observations.
	calls atomic.Int32
	// done closes when cancellation becomes observable.
	done chan struct{}
	// cancel protects the one-time Done transition.
	cancel sync.Once
}

// newCancelOnThirdErrContext creates a context that remains active for exactly two Err observations.
func newCancelOnThirdErrContext() *cancelOnErrContext {
	return &cancelOnErrContext{cancelOn: 3, done: make(chan struct{})}
}

// Deadline reports that the deterministic cancellation probe has no time-based deadline.
func (*cancelOnErrContext) Deadline() (time.Time, bool) {
	return time.Time{}, false
}

// Done exposes the cancellation notification closed by the configured Err observation.
func (c *cancelOnErrContext) Done() <-chan struct{} {
	return c.done
}

// Err returns context.Canceled from the configured observation onward.
func (c *cancelOnErrContext) Err() error {
	if c.calls.Add(1) < c.cancelOn {
		return nil
	}
	c.cancel.Do(func() { close(c.done) })
	return context.Canceled
}

// Value reports no request-scoped values because persistence tests need only cancellation behavior.
func (*cancelOnErrContext) Value(any) any {
	return nil
}

// TestMemoryStoreHonorsCancellationAtOwnershipBoundaries verifies canceled clones never commit or escape.
func TestMemoryStoreHonorsCancellationAtOwnershipBoundaries(t *testing.T) {
	t.Parallel()

	t.Run("create", func(t *testing.T) {
		t.Parallel()

		store := workflow.NewMemoryStore()
		instance := &workflow.Instance{ID: "cancel-create", Version: 1, Data: []byte(`{"state":"candidate"}`)}
		if err := store.Create(newCancelOnThirdErrContext(), instance); !errors.Is(err, context.Canceled) {
			t.Fatalf("Create() error = %v, want context.Canceled", err)
		}
		if _, err := store.Load(t.Context(), instance.ID); !errors.Is(err, workflow.ErrInstanceNotFound) {
			t.Fatalf("Load(after canceled Create) error = %v, want ErrInstanceNotFound", err)
		}
	})

	t.Run("load", func(t *testing.T) {
		t.Parallel()

		store := workflow.NewMemoryStore()
		instance := &workflow.Instance{ID: "cancel-load", Version: 1, Data: []byte(`{"state":"stored"}`)}
		if err := store.Create(t.Context(), instance); err != nil {
			t.Fatalf("Create() error = %v", err)
		}
		if _, err := store.Load(newCancelOnThirdErrContext(), instance.ID); !errors.Is(err, context.Canceled) {
			t.Fatalf("Load() error = %v, want context.Canceled", err)
		}
	})

	t.Run("save", func(t *testing.T) {
		t.Parallel()

		store := workflow.NewMemoryStore()
		stored := &workflow.Instance{ID: "cancel-save", Version: 1, Data: []byte(`{"state":"stored"}`)}
		if err := store.Create(t.Context(), stored); err != nil {
			t.Fatalf("Create() error = %v", err)
		}
		candidate := &workflow.Instance{ID: stored.ID, Version: 2, Data: []byte(`{"state":"candidate"}`)}
		if err := store.Save(newCancelOnThirdErrContext(), candidate, stored.Version); !errors.Is(err, context.Canceled) {
			t.Fatalf("Save() error = %v, want context.Canceled", err)
		}
		actual, err := store.Load(t.Context(), stored.ID)
		if err != nil {
			t.Fatalf("Load() error = %v", err)
		}
		if !reflect.DeepEqual(actual, stored) {
			t.Fatalf("stored instance changed after canceled Save: got %#v, want %#v", actual, stored)
		}
	})
}

// TestMemoryDefinitionStoreHonorsCancellationAtOwnershipBoundaries verifies version storage never outlives its context.
func TestMemoryDefinitionStoreHonorsCancellationAtOwnershipBoundaries(t *testing.T) {
	t.Parallel()

	definition := &workflow.Definition{
		ID:    "cancel-definition",
		Nodes: []workflow.NodeDefinition{{ID: "start", Kind: workflow.KindStart}, {ID: "end", Kind: workflow.KindEnd}},
		Edges: []workflow.Edge{{From: "start", To: "end"}},
	}

	t.Run("create version", func(t *testing.T) {
		t.Parallel()

		store := workflow.NewMemoryDefinitionStore()
		if _, err := store.CreateVersion(newCancelOnThirdErrContext(), definition); !errors.Is(err, context.Canceled) {
			t.Fatalf("CreateVersion() error = %v, want context.Canceled", err)
		}
		if _, err := store.LoadLatest(t.Context(), definition.ID); !errors.Is(err, workflow.ErrDefinitionNotFound) {
			t.Fatalf("LoadLatest(after canceled CreateVersion) error = %v, want ErrDefinitionNotFound", err)
		}
	})

	t.Run("load exact", func(t *testing.T) {
		t.Parallel()

		store := workflow.NewMemoryDefinitionStore()
		if _, err := store.CreateVersion(t.Context(), definition); err != nil {
			t.Fatalf("CreateVersion() error = %v", err)
		}
		if _, err := store.Load(newCancelOnThirdErrContext(), definition.ID, 1); !errors.Is(err, context.Canceled) {
			t.Fatalf("Load() error = %v, want context.Canceled", err)
		}
	})

	t.Run("load latest", func(t *testing.T) {
		t.Parallel()

		store := workflow.NewMemoryDefinitionStore()
		if _, err := store.CreateVersion(t.Context(), definition); err != nil {
			t.Fatalf("CreateVersion() error = %v", err)
		}
		if _, err := store.LoadLatest(newCancelOnThirdErrContext(), definition.ID); !errors.Is(err, context.Canceled) {
			t.Fatalf("LoadLatest() error = %v, want context.Canceled", err)
		}
	})
}
