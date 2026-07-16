// This file defines the persistence port consumed by Engine and its in-memory reference adapter.
// The adapter is intended for tests and examples; production databases implement the same CAS contract.
package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync"
)

var (
	// ErrInstanceNotFound means no workflow instance exists for the requested identifier.
	ErrInstanceNotFound = errors.New("workflow: instance not found")
	// ErrInstanceExists means Create received an identifier that is already durable.
	ErrInstanceExists = errors.New("workflow: instance already exists")
	// ErrVersionConflict means optimistic concurrency rejected a stale instance snapshot.
	ErrVersionConflict = errors.New("workflow: version conflict")
)

// Store persists complete aggregate snapshots with optimistic concurrency.
//
// Create is insert-only. Load returns a caller-owned snapshot. Save must atomically persist the instance,
// its tasks, and audit records only when the stored version equals expectedVersion. Implementations must
// propagate context cancellation and must not retain caller-mutable slices.
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

// Create inserts a new instance and rejects duplicate identifiers.
func (s *MemoryStore) Create(ctx context.Context, instance *Instance) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("workflow: create instance: %w", err)
	}
	if s == nil || instance == nil || instance.ID == "" {
		return fmt.Errorf("workflow: create instance: invalid input")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.instances == nil {
		s.instances = make(map[InstanceID]*Instance)
	}
	if _, exists := s.instances[instance.ID]; exists {
		return fmt.Errorf("%w: %q", ErrInstanceExists, instance.ID)
	}
	s.instances[instance.ID] = cloneInstance(instance)
	return nil
}

// Load returns a defensive snapshot for id or ErrInstanceNotFound.
func (s *MemoryStore) Load(ctx context.Context, id InstanceID) (*Instance, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("workflow: load instance: %w", err)
	}
	if s == nil {
		return nil, fmt.Errorf("workflow: load instance: store is nil")
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	instance, exists := s.instances[id]
	if !exists {
		return nil, fmt.Errorf("%w: %q", ErrInstanceNotFound, id)
	}
	return cloneInstance(instance), nil
}

// Save replaces a snapshot only when expectedVersion matches the durable version.
func (s *MemoryStore) Save(ctx context.Context, instance *Instance, expectedVersion uint64) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("workflow: save instance: %w", err)
	}
	if s == nil || instance == nil {
		return fmt.Errorf("workflow: save instance: invalid input")
	}

	// Hold one exclusive lock across comparison and replacement so CAS is atomic for concurrent commands.
	s.mu.Lock()
	defer s.mu.Unlock()
	stored, exists := s.instances[instance.ID]
	if !exists {
		return fmt.Errorf("%w: %q", ErrInstanceNotFound, instance.ID)
	}
	if stored.Version != expectedVersion {
		return fmt.Errorf("%w: expected %d, got %d", ErrVersionConflict, expectedVersion, stored.Version)
	}
	s.instances[instance.ID] = cloneInstance(instance)
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

// validJSON reports whether optional raw data is either absent or one complete JSON value.
func validJSON(data json.RawMessage) bool {
	return len(data) == 0 || json.Valid(data)
}
