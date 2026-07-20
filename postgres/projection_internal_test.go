// This file verifies private transport and transaction-cleanup invariants that cannot be observed without database I/O.
package postgres

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	workflow "github.com/lvpeng/easy-workflow"
)

// TestEncodeContinuationEnvelopeRejectsOversizedOutput verifies the encoder never returns an unusable token.
func TestEncodeContinuationEnvelopeRejectsOversizedOutput(t *testing.T) {
	t.Parallel()

	_, err := encodeContinuationEnvelope(continuationEnvelope{
		Version:    continuationEncodingVersion,
		Family:     instanceContinuationFamily,
		At:         time.Date(2026, 7, 20, 1, 2, 3, 0, time.UTC),
		InstanceID: workflow.InstanceID(strings.Repeat("i", maximumContinuationLength)),
	})
	if !errors.Is(err, ErrInvalidProjectionQuery) {
		t.Fatalf("encodeContinuationEnvelope() error = %v, want ErrInvalidProjectionQuery", err)
	}
}

// TestValidateProjectionContinuationKeysRejectsUnpageableRows verifies invalid identities fail before projection I/O.
func TestValidateProjectionContinuationKeysRejectsUnpageableRows(t *testing.T) {
	t.Parallel()

	orderAt := time.Date(2026, 7, 20, 1, 2, 3, 0, time.UTC)
	tests := []struct {
		name     string
		instance *workflow.Instance
	}{
		{
			name:     "instance identity",
			instance: &workflow.Instance{ID: workflow.InstanceID(strings.Repeat("i", maximumContinuationLength))},
		},
		{
			name: "task identity",
			instance: &workflow.Instance{
				ID: "instance-a",
				Tasks: []workflow.Task{{
					ID:       workflow.TaskID(strings.Repeat("t", maximumContinuationLength)),
					NodeID:   "review",
					Assignee: "reviewer-a",
					Status:   workflow.TaskStatusActive,
				}},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			if err := validateProjectionContinuationKeys(test.instance, orderAt); !errors.Is(err, workflow.ErrInvalidStoreInput) {
				t.Fatalf("validateProjectionContinuationKeys() error = %v, want ErrInvalidStoreInput", err)
			}
		})
	}
}

// TestNewTransactionRollbackContextIsIndependentAndBounded verifies cleanup survives cancellation without running forever.
func TestNewTransactionRollbackContextIsIndependentAndBounded(t *testing.T) {
	t.Parallel()

	parent, cancelParent := context.WithCancel(context.Background())
	cancelParent()
	rollbackContext, cancelRollback := newTransactionRollbackContext(parent)
	defer cancelRollback()
	if err := rollbackContext.Err(); err != nil {
		t.Fatalf("rollback context error = %v, want active independent cleanup", err)
	}
	deadline, exists := rollbackContext.Deadline()
	if !exists {
		t.Fatal("rollback context deadline is absent")
	}
	remaining := time.Until(deadline)
	if remaining <= 0 || remaining > transactionRollbackTimeout {
		t.Fatalf("rollback context remaining duration = %v, want within (0, %v]", remaining, transactionRollbackTimeout)
	}
}
