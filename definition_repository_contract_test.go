// Package workflow_test applies the reusable Definition repository contract to the reference adapter.
// Adapter-neutral behavior lives in definitiontest; this file owns only MemoryDefinitionStore wiring.
package workflow_test

import (
	"testing"

	workflow "github.com/lvpeng/easy-workflow"
	"github.com/lvpeng/easy-workflow/definitiontest"
)

// TestMemoryDefinitionStoreWriterContract verifies the reference adapter through the public writer seam.
//
// Each contract subtest receives an empty store so version allocation and cancellation assertions remain isolated.
func TestMemoryDefinitionStoreWriterContract(t *testing.T) {
	definitiontest.RunWriter(t, func(t *testing.T) workflow.DefinitionVersionWriter {
		t.Helper()
		return workflow.NewMemoryDefinitionStore()
	})
}

// TestMemoryDefinitionStoreReaderContract verifies the reference adapter through the public reader seam.
//
// The factory publishes requested fixtures in order and returns only DefinitionReader, keeping contract assertions
// independent from the adapter's writer capability and internal storage representation.
func TestMemoryDefinitionStoreReaderContract(t *testing.T) {
	definitiontest.RunReader(t, func(t *testing.T, fixtures []*workflow.Definition) workflow.DefinitionReader {
		t.Helper()
		store := workflow.NewMemoryDefinitionStore()
		for _, fixture := range fixtures {
			published, err := store.CreateVersion(t.Context(), fixture)
			if err != nil {
				t.Fatalf("seed CreateVersion() error = %v", err)
			}
			if published.Version != fixture.Version {
				t.Fatalf("seed CreateVersion() version = %d, want fixture version %d", published.Version, fixture.Version)
			}
		}
		return store
	})
}

// TestMemoryDefinitionStoreRepositoryContract verifies the writer-reader lifecycle on one reference adapter.
//
// Both capabilities share one fresh MemoryDefinitionStore per subtest so the combined contract can observe only
// public publication and lookup behavior across the repository boundary.
func TestMemoryDefinitionStoreRepositoryContract(t *testing.T) {
	definitiontest.RunRepository(t, func(t *testing.T) (workflow.DefinitionVersionWriter, workflow.DefinitionReader) {
		t.Helper()
		store := workflow.NewMemoryDefinitionStore()
		return store, store
	})
}
