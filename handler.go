// This file defines the constrained protocol between the graph engine and pluggable node behavior.
// Handlers return declarative results; they never receive a Store or choose arbitrary target nodes.
package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
)

var (
	// ErrInvalidHandler means registration received a missing registry, kind, or handler implementation.
	ErrInvalidHandler = errors.New("workflow: invalid node handler")
	// ErrHandlerNotFound means a definition references a node kind absent from the engine registry.
	ErrHandlerNotFound = errors.New("workflow: node handler not found")
	// ErrHandlerExists means a registry already contains the requested stable node kind.
	ErrHandlerExists = errors.New("workflow: node handler already registered")
)

// Disposition tells the core whether a node remains active, follows an edge, or rejects the instance.
type Disposition string

const (
	// DispositionUnknown is invalid and prevents accidental zero-value transitions.
	DispositionUnknown Disposition = ""
	// DispositionWaiting keeps the current node active with the returned tasks and state.
	DispositionWaiting Disposition = "waiting"
	// DispositionContinue follows the edge selected by NodeResult.Outcome.
	DispositionContinue Disposition = "continue"
	// DispositionReject terminates when Outcome is empty and otherwise requires its explicit Definition edge.
	DispositionReject Disposition = "reject"
)

// ActivationInput supplies immutable definition and business data when a node becomes active.
type ActivationInput struct {
	Config json.RawMessage
	Data   json.RawMessage
}

// CommandInput supplies current node-owned state and tasks for one actor command.
//
// Tasks is a defensive copy limited to the current node. Handlers return updated copies in NodeResult;
// mutating this slice cannot change the stored instance unless the engine accepts the result.
type CommandInput struct {
	Command

	Config json.RawMessage
	Data   json.RawMessage
	State  json.RawMessage
	Tasks  []Task
}

// NodeResult is the only channel through which a handler proposes runtime changes.
//
// Waiting results must contain the node's complete task view. Continue results require one declared edge
// selected by Outcome. Reject results terminate with an empty outcome or require a matching declared edge
// for a non-empty outcome. State is opaque JSON persisted for the active node.
type NodeResult struct {
	Disposition Disposition
	Outcome     string
	State       json.RawMessage
	Tasks       []Task
}

// NodeHandler implements one stable node kind without controlling graph navigation or persistence.
//
// Validate runs before an instance is created. Activate runs whenever execution enters the node. Handle
// processes commands while it is active. Implementations must be safe for concurrent calls from different
// instances and must honor context cancellation for any blocking work.
type NodeHandler interface {
	Validate(config json.RawMessage) error
	Activate(ctx context.Context, input ActivationInput) (NodeResult, error)
	Handle(ctx context.Context, input CommandInput) (NodeResult, error)
}

// Registry maps stable node-kind names to explicitly registered handlers.
//
// Registry is safe for concurrent registration and lookup. Engines do not copy it, so applications should
// normally finish registration before serving commands to keep available behavior predictable.
type Registry struct {
	mu       sync.RWMutex
	handlers map[string]NodeHandler
}

// NewRegistry creates an empty handler registry with no implicit global registrations.
func NewRegistry() *Registry {
	return &Registry{handlers: make(map[string]NodeHandler)}
}

// Register binds kind to handler exactly once.
//
// kind must be non-empty and handler must be non-nil. Duplicate registration returns ErrHandlerExists so
// application composition cannot silently replace behavior used by persisted definitions.
func (r *Registry) Register(kind string, handler NodeHandler) error {
	// Registration requires a concrete receiver, stable lookup key, and executable implementation.
	if r == nil || kind == "" || handler == nil {
		return fmt.Errorf("%w: registry, kind, or handler is empty", ErrInvalidHandler)
	}

	// Serialize lazy map creation, duplicate detection, and insertion as one registry mutation.
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.handlers == nil {
		r.handlers = make(map[string]NodeHandler)
	}
	// Insert-only registration prevents running instances from observing handler replacement.
	if _, exists := r.handlers[kind]; exists {
		return fmt.Errorf("%w: %q", ErrHandlerExists, kind)
	}
	r.handlers[kind] = handler
	return nil
}

// handler returns the registered implementation for kind without exposing the mutable registry map.
func (r *Registry) handler(kind string) (NodeHandler, error) {
	if r == nil {
		return nil, fmt.Errorf("%w: %q", ErrHandlerNotFound, kind)
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	handler, exists := r.handlers[kind]
	if !exists {
		return nil, fmt.Errorf("%w: %q", ErrHandlerNotFound, kind)
	}
	return handler, nil
}
