// This file publishes validated canonical definitions as immutable, monotonically versioned snapshots.
// It defines the publication boundary and its process-local adapter; instance persistence remains in Store.
package workflow

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

var (
	// ErrDefinitionNotFound means no immutable snapshot exists for the requested definition identity.
	ErrDefinitionNotFound = errors.New("workflow: definition not found")
	// ErrInvalidPublisher means publication collaborators are absent or returned an unusable result.
	ErrInvalidPublisher = errors.New("workflow: invalid definition publisher")
	// ErrInvalidDefinitionStore means a version adapter or candidate Definition cannot be used safely.
	ErrInvalidDefinitionStore = errors.New("workflow: invalid definition store")
)

// DefinitionVersionWriter atomically assigns and persists the next version of one canonical Definition.
//
// CreateVersion receives a fully compiled caller-owned definition whose Version is not authoritative.
// Implementations must assign a positive version greater than every existing version for the same ID,
// persist a detached immutable snapshot, and return another caller-owned snapshot. A returned error must
// leave both the version sequence and stored definitions unchanged. Implementations must support concurrent calls.
type DefinitionVersionWriter interface {
	CreateVersion(ctx context.Context, definition *Definition) (*Definition, error)
}

// DefinitionReader retrieves caller-owned snapshots without exposing adapter-owned mutable data.
//
// Load selects one exact positive version and never falls back. LoadLatest selects the greatest stored
// version for an ID. Both methods return ErrDefinitionNotFound when the requested identity does not exist,
// preserve context cancellation, and must be safe for concurrent use.
type DefinitionReader interface {
	Load(ctx context.Context, id string, version uint64) (*Definition, error)
	LoadLatest(ctx context.Context, id string) (*Definition, error)
}

// DefinitionPublisher compiles canonical definitions before handing them to atomic version storage.
//
// The publisher is safe for concurrent use when its writer and Registry are safe for concurrent calls.
// It owns no definition cache, so successful publication is durable exactly to the writer's guarantees.
type DefinitionPublisher struct {
	// writer owns atomic version allocation and immutable persistence after compilation succeeds.
	writer DefinitionVersionWriter
	// registry supplies every business node's configuration validator during publication.
	registry *Registry
}

// NewDefinitionPublisher constructs the single publication path for code-authored and JSON definitions.
//
// writer atomically allocates immutable versions; registry resolves and validates every business node kind.
// Nil dependencies are accepted at construction and reported by Publish, keeping setup free of side effects.
func NewDefinitionPublisher(writer DefinitionVersionWriter, registry *Registry) *DefinitionPublisher {
	return &DefinitionPublisher{writer: writer, registry: registry}
}

// Publish compiles one canonical definition and persists it under the next store-assigned version.
//
// definition remains caller-owned and its Version is ignored. Compilation validates the graph and every
// registered node configuration before any storage call. The returned definition is detached from both
// input and storage. Errors do not consume a version or leave a partial definition in a conforming writer.
func (p *DefinitionPublisher) Publish(ctx context.Context, definition *Definition) (*Definition, error) {
	// Publication cannot validate or persist safely when either configured collaborator is absent.
	if p == nil || p.writer == nil || p.registry == nil {
		return nil, fmt.Errorf("%w: dependencies are nil", ErrInvalidPublisher)
	}
	// Compilation must finish before the first storage call so invalid attempts cannot reserve a version.
	if err := CompileDefinition(definition, p.registry); err != nil {
		return nil, err
	}

	// Clear authoring metadata so the writer is the only authority that can allocate a published version.
	candidate := cloneDefinition(*definition)
	candidate.Version = 0
	published, err := p.writer.CreateVersion(ctx, &candidate)
	// Writer errors preserve their cause while adding the stable Definition identity for operations.
	if err != nil {
		return nil, fmt.Errorf("workflow: publish definition %q: %w", candidate.ID, err)
	}
	// A successful writer must return the identity it persisted; nil would make publication unverifiable.
	if published == nil {
		return nil, fmt.Errorf("%w: writer returned nil for definition %q", ErrInvalidPublisher, candidate.ID)
	}
	return cloneDefinitionPointer(published), nil
}

// PublishJSON parses one web-authored canonical Definition and publishes it through the same compiled path.
//
// data must contain exactly one valid Definition JSON object. Caller-supplied Version is treated as
// untrusted authoring metadata and replaced during publication. Parse, compile, and storage errors are returned.
func (p *DefinitionPublisher) PublishJSON(ctx context.Context, data []byte) (*Definition, error) {
	definition, err := ParseDefinition(data)
	// Malformed or structurally invalid JSON never reaches compilation or version storage.
	if err != nil {
		return nil, err
	}
	return p.Publish(ctx, definition)
}

// MemoryDefinitionStore is a concurrency-safe, process-local immutable definition writer.
//
// It is intended for tests, examples, and single-process deployments. Snapshots live for the store lifetime,
// and every ownership boundary deep-copies mutable slices and JSON bytes.
type MemoryDefinitionStore struct {
	// mu keeps version selection, insertion, and defensive cloning in one consistent process-local view.
	mu sync.RWMutex
	// definitions stores gap-free snapshots by stable ID; slice position zero represents published version 1.
	definitions map[string][]Definition
}

var _ DefinitionVersionWriter = (*MemoryDefinitionStore)(nil)
var _ DefinitionReader = (*MemoryDefinitionStore)(nil)

// NewMemoryDefinitionStore creates an empty process-local definition store.
func NewMemoryDefinitionStore() *MemoryDefinitionStore {
	return &MemoryDefinitionStore{definitions: make(map[string][]Definition)}
}

// CreateVersion atomically assigns the next positive version and stores an immutable snapshot.
//
// definition must be non-nil, fully compiled, and retain a stable non-empty ID; Version is ignored.
// The method serializes allocation and insertion under one lock, returns a defensive copy, and performs
// no external I/O. Context cancellation before lock acquisition leaves the store unchanged.
func (s *MemoryDefinitionStore) CreateVersion(ctx context.Context, definition *Definition) (*Definition, error) {
	// Reject unusable ownership boundaries before touching the adapter's synchronization state.
	if s == nil || definition == nil || definition.ID == "" {
		return nil, fmt.Errorf("%w: store or definition is invalid", ErrInvalidDefinitionStore)
	}
	// A context already canceled by the caller must leave allocation unchanged.
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("workflow: create definition version: %w", err)
	}

	// Allocation and insertion share one critical section so concurrent publishers cannot reuse a version.
	s.mu.Lock()
	defer s.mu.Unlock()
	// Recheck after waiting for the lock so cancellation cannot race with the durable in-memory mutation.
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("workflow: create definition version: %w", err)
	}
	versions := s.definitions[definition.ID]
	snapshot := cloneDefinition(*definition)
	// The append-only slice length equals the last gap-free version, so one selects the next positive version.
	snapshot.Version = uint64(len(versions)) + 1
	s.definitions[definition.ID] = append(versions, snapshot)
	return cloneDefinitionPointer(&snapshot), nil
}

// Load returns a defensive copy of one exact immutable definition version.
//
// id must be non-empty and version must be positive. Missing IDs and versions return ErrDefinitionNotFound;
// canceled contexts retain their cause. The returned snapshot can be freely mutated by its caller.
func (s *MemoryDefinitionStore) Load(ctx context.Context, id string, version uint64) (*Definition, error) {
	// Invalid identities are indistinguishable from absent persisted versions at the repository boundary.
	if s == nil || id == "" || version == 0 {
		return nil, fmt.Errorf("%w: id %q version %d", ErrDefinitionNotFound, id, version)
	}
	// Avoid lock acquisition when the caller can no longer consume the snapshot.
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("workflow: load definition: %w", err)
	}

	// Hold the read lock while selecting and cloning so no append can replace the backing slice mid-read.
	s.mu.RLock()
	defer s.mu.RUnlock()
	versions := s.definitions[id]
	// Exact lookup never falls forward to another version when the requested position is absent.
	if version > uint64(len(versions)) {
		return nil, fmt.Errorf("%w: id %q version %d", ErrDefinitionNotFound, id, version)
	}
	snapshot := versions[version-1] // Published version 1 occupies zero-based slice position 0.
	return cloneDefinitionPointer(&snapshot), nil
}

// LoadLatest returns a defensive copy of the greatest immutable version stored for one ID.
//
// id must be non-empty. An unknown or empty ID returns ErrDefinitionNotFound; canceled contexts retain
// their cause. The lookup is read-only and safe to call concurrently with publication.
func (s *MemoryDefinitionStore) LoadLatest(ctx context.Context, id string) (*Definition, error) {
	// Empty identities do not name an implicit default Definition.
	if s == nil || id == "" {
		return nil, fmt.Errorf("%w: id %q", ErrDefinitionNotFound, id)
	}
	// Avoid lock acquisition when the caller can no longer consume the snapshot.
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("workflow: load latest definition: %w", err)
	}

	// Clone under the read lock so the selected latest position and its bytes come from one snapshot.
	s.mu.RLock()
	defer s.mu.RUnlock()
	versions := s.definitions[id]
	// An empty append-only sequence has no latest-version fallback to return.
	if len(versions) == 0 {
		return nil, fmt.Errorf("%w: id %q", ErrDefinitionNotFound, id)
	}
	snapshot := versions[len(versions)-1] // The append-only final element is the greatest published version.
	return cloneDefinitionPointer(&snapshot), nil
}

// cloneDefinitionPointer returns a detached Definition while preserving nil input as nil.
func cloneDefinitionPointer(source *Definition) *Definition {
	if source == nil {
		return nil // Nil stays nil so callers can validate optional adapter results without a fabricated snapshot.
	}
	cloned := cloneDefinition(*source)
	return &cloned
}
