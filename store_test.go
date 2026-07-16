// Package workflow_test verifies persistence behavior through the public Store contract.
// These tests treat MemoryStore as the reference semantics future durable adapters must match.
package workflow_test

import (
	"context"
	"errors"
	"testing"

	workflow "github.com/lvpeng/easy-workflow"
)

// TestMemoryStoreRejectsStaleSave verifies atomic optimistic concurrency for competing snapshots.
func TestMemoryStoreRejectsStaleSave(t *testing.T) {
	t.Parallel()

	store := workflow.NewMemoryStore()
	original := &workflow.Instance{ID: "instance-1", Version: 1}
	if err := store.Create(context.Background(), original); err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	// Load two snapshots at the same version to model commands racing in separate application requests.
	first, err := store.Load(context.Background(), original.ID)
	if err != nil {
		t.Fatalf("first Load() error = %v", err)
	}
	second, err := store.Load(context.Background(), original.ID)
	if err != nil {
		t.Fatalf("second Load() error = %v", err)
	}

	// The first writer advances the durable version; the stale writer must not overwrite it.
	first.Version++
	if err := store.Save(context.Background(), first, original.Version); err != nil {
		t.Fatalf("first Save() error = %v", err)
	}
	second.Version++
	if err := store.Save(context.Background(), second, original.Version); !errors.Is(err, workflow.ErrVersionConflict) {
		t.Fatalf("stale Save() error = %v, want ErrVersionConflict", err)
	}
}
