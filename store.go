// This file defines the persistence port consumed by Engine and its in-memory reference adapter.
// The adapter is intended for tests and examples; production databases implement the same CAS contract.
package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"slices"
	"sync"

	"github.com/lvpeng/easy-workflow/internal/jsonstrict"
)

var (
	// ErrInvalidStoreInput means a Store dependency, required aggregate input, or immutable audit prefix is invalid.
	ErrInvalidStoreInput = errors.New("workflow: invalid store input")
	// ErrInstanceNotFound means no workflow instance exists for the requested identifier.
	ErrInstanceNotFound = errors.New("workflow: instance not found")
	// ErrInstanceExists means Create received an identifier that is already durable.
	ErrInstanceExists = errors.New("workflow: instance already exists")
	// ErrVersionConflict means optimistic concurrency rejected a stale instance snapshot.
	ErrVersionConflict = errors.New("workflow: version conflict")
	// ErrVersionOverflow means an accepted transition cannot advance the uint64 instance CAS token.
	ErrVersionOverflow = errors.New("workflow: version overflow")
)

// Store persists complete aggregate snapshots with optimistic concurrency.
//
// Create is insert-only. Load returns a caller-owned snapshot. Save must atomically persist the instance,
// its tasks, and audit records only when the stored version equals expectedVersion. Existing audit records are
// immutable and Save may only append a suffix. Implementations must propagate context cancellation and must not
// retain caller-mutable slices.
type Store interface {
	Create(ctx context.Context, instance *Instance) error
	Load(ctx context.Context, id InstanceID) (*Instance, error)
	Save(ctx context.Context, instance *Instance, expectedVersion uint64) error
}

// MemoryStore is a concurrency-safe, process-local Store for tests, examples, and prototypes.
//
// It keeps defensive copies under a mutex and therefore provides the same atomic snapshot and version
// semantics expected from durable adapters. Data is lost when the process exits.
type MemoryStore struct {
	mu        sync.RWMutex
	instances map[InstanceID]*Instance
}

var _ Store = (*MemoryStore)(nil)

// NewMemoryStore creates an empty process-local store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{instances: make(map[InstanceID]*Instance)}
}

// Create atomically inserts a detached instance snapshot and rejects duplicate identifiers.
//
// instance and its ID must be non-empty, and ctx must remain active until the in-memory commit completes.
// The store retains no caller-owned slices. Errors wrap context cancellation, ErrInvalidStoreInput, or
// ErrInstanceExists; a failed call leaves the process-local snapshot map unchanged.
func (s *MemoryStore) Create(ctx context.Context, instance *Instance) error {
	// Cancellation takes precedence because no caller can consume a successful write after abandoning it.
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("workflow: create instance: %w", err)
	}
	// A missing store, aggregate, or identity cannot form a durable ownership boundary.
	if s == nil || instance == nil || instance.ID == "" {
		return fmt.Errorf("%w: create requires store and instance identity", ErrInvalidStoreInput)
	}

	// Hold the exclusive lock across lazy initialization, duplicate detection, and insertion.
	if err := acquireMemoryStoreLock(ctx, s.mu.TryLock, s.mu.Unlock); err != nil {
		return fmt.Errorf("workflow: create instance: %w", err)
	}
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("workflow: create instance: %w", err)
	}
	if s.instances == nil {
		s.instances = make(map[InstanceID]*Instance)
	}
	// Insert-only semantics prevent a second creator from overwriting an existing execution snapshot.
	if _, exists := s.instances[instance.ID]; exists {
		return fmt.Errorf("%w: %q", ErrInstanceExists, instance.ID)
	}
	snapshot := cloneInstance(instance)
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("workflow: create instance: %w", err)
	}
	s.instances[instance.ID] = snapshot
	return nil
}

// Load returns a caller-owned snapshot for one exact instance ID.
//
// ctx must remain active through the read. The returned aggregate deep-copies every mutable field and may be
// changed freely by its caller. Errors wrap context cancellation, ErrInvalidStoreInput, or ErrInstanceNotFound.
func (s *MemoryStore) Load(ctx context.Context, id InstanceID) (*Instance, error) {
	// Avoid lock acquisition when the caller can no longer consume the loaded aggregate.
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("workflow: load instance: %w", err)
	}
	// A nil adapter has no implicit global store fallback.
	if s == nil {
		return nil, fmt.Errorf("%w: load requires a store", ErrInvalidStoreInput)
	}

	// Clone while holding the read lock so every field comes from one consistent stored pointer.
	if err := acquireMemoryStoreLock(ctx, s.mu.TryRLock, s.mu.RUnlock); err != nil {
		return nil, fmt.Errorf("workflow: load instance: %w", err)
	}
	defer s.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("workflow: load instance: %w", err)
	}
	instance, exists := s.instances[id]
	// Missing identity never falls back to another or newly created instance.
	if !exists {
		return nil, fmt.Errorf("%w: %q", ErrInstanceNotFound, id)
	}
	snapshot := cloneInstance(instance)
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("workflow: load instance: %w", err)
	}
	return snapshot, nil
}

// Save atomically replaces a snapshot only when expectedVersion matches the durable version.
//
// instance must be non-nil and ctx must remain active through the compare-and-swap. The store retains a deep
// copy rather than caller-owned slices. Errors wrap cancellation, ErrInvalidStoreInput, ErrInstanceNotFound,
// or ErrVersionConflict; every failed path preserves the previously stored aggregate.
func (s *MemoryStore) Save(ctx context.Context, instance *Instance, expectedVersion uint64) error {
	// Cancellation prevents a write whose result the caller has already abandoned.
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("workflow: save instance: %w", err)
	}
	// CAS requires both a concrete adapter and an aggregate carrying the candidate identity and version.
	if s == nil || instance == nil {
		return fmt.Errorf("%w: save requires store and instance", ErrInvalidStoreInput)
	}

	// Hold one exclusive lock across comparison and replacement so CAS is atomic for concurrent commands.
	if err := acquireMemoryStoreLock(ctx, s.mu.TryLock, s.mu.Unlock); err != nil {
		return fmt.Errorf("workflow: save instance: %w", err)
	}
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("workflow: save instance: %w", err)
	}
	stored, exists := s.instances[instance.ID]
	// Save never creates a missing aggregate because creation has a separate insert-only contract.
	if !exists {
		return fmt.Errorf("%w: %q", ErrInstanceNotFound, instance.ID)
	}
	// Reject stale callers at the comparison point so no field can be partially replaced.
	if stored.Version != expectedVersion {
		return fmt.Errorf("%w: expected %d, got %d", ErrVersionConflict, expectedVersion, stored.Version)
	}
	// Audit order is authoritative, so a candidate may extend but never remove or rewrite the durable prefix.
	if len(instance.Audit) < len(stored.Audit) || !slices.Equal(instance.Audit[:len(stored.Audit)], stored.Audit) {
		return fmt.Errorf("%w: save cannot rewrite audit history for %q", ErrInvalidStoreInput, instance.ID)
	}
	snapshot := cloneInstance(instance)
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("workflow: save instance: %w", err)
	}
	s.instances[instance.ID] = snapshot
	return nil
}

// cloneInstance deep-copies every mutable field crossing the Store ownership boundary.
func cloneInstance(source *Instance) *Instance {
	if source == nil {
		return nil
	}
	cloned := *source
	cloned.Definition = cloneDefinition(source.Definition)
	cloned.Data = slices.Clone(source.Data)
	cloned.NodeState = slices.Clone(source.NodeState)
	cloned.Tasks = slices.Clone(source.Tasks)
	cloned.Audit = slices.Clone(source.Audit)
	return &cloned
}

// acquireMemoryStoreLock waits for a lock without blocking past context cancellation.
//
// tryLock and unlock must refer to the same synchronization primitive. The loop yields between failed attempts so a
// canceled caller can leave while another operation holds the mutex; the caller owns the successful unlock afterward.
func acquireMemoryStoreLock(ctx context.Context, tryLock func() bool, unlock func()) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if tryLock() {
			if err := ctx.Err(); err != nil {
				unlock()
				return err
			}
			return nil
		}
		// Yield rather than sleeping so short critical sections retain low latency without monopolizing a scheduler thread.
		runtime.Gosched()
	}
}

// validJSON reports whether optional raw data is either absent or one complete JSON value.
func validJSON(data json.RawMessage) bool {
	return len(data) == 0 || jsonstrict.Validate(data) == nil
}
