// This file verifies the reference Store leaves mutex waits when callers cancel.
package workflow

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestMemoryStoreReturnsWhileWriteLockIsHeld verifies cancellation is not delayed by an unrelated writer.
func TestMemoryStoreReturnsWhileWriteLockIsHeld(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore()
	instance := &Instance{ID: "lock-cancellation", Version: 1}
	if err := store.Create(t.Context(), instance); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	store.mu.Lock()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		done <- store.Save(ctx, &Instance{ID: instance.ID, Version: 2}, instance.Version)
	}()
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Save() error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Save() did not return after cancellation while write lock was held")
	}
	store.mu.Unlock()
}

// TestMemoryStoreReturnsWhileReadLockIsHeld verifies read operations do not wait indefinitely for a writer.
func TestMemoryStoreReturnsWhileReadLockIsHeld(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore()
	instance := &Instance{ID: "read-lock-cancellation", Version: 1}
	if err := store.Create(t.Context(), instance); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	store.mu.Lock()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		_, err := store.Load(ctx, instance.ID)
		done <- err
	}()
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Load() error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Load() did not return after cancellation while write lock was held")
	}
	store.mu.Unlock()
}
