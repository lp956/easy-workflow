// Package workflow_test verifies persistence behavior through the public Store contract.
// These tests treat MemoryStore as the reference semantics future durable adapters must match.
package workflow_test

import (
	"testing"

	workflow "github.com/lvpeng/easy-workflow"
	"github.com/lvpeng/easy-workflow/storetest"
)

// TestMemoryStoreContract applies the public Store contract to the process-local adapter.
func TestMemoryStoreContract(t *testing.T) {
	t.Parallel()

	storetest.Run(t, func(*testing.T) workflow.Store {
		return workflow.NewMemoryStore()
	})
}
