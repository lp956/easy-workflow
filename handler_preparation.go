// This file adapts registered NodeHandler implementations into request-local prepared execution objects.
// It does not own Registry state, canonical Definition data, graph routing, persistence, or cross-request caches.
// Prepared values live only inside one package-internal compiled Definition plan.
package workflow

import (
	"context"
	"fmt"
	"slices"

	"github.com/lvpeng/easy-workflow/internal/nilguard"
)

// legacyPreparedNodeHandler adapts the original raw-config NodeHandler contract to prepared plan execution.
//
// The adapter owns one defensive config copy for one compiled node. It performs no parsing or global caching and remains
// only until the enclosing CompileDefinition validation or Engine operation ends.
type legacyPreparedNodeHandler struct {
	// handler is the registered concurrency-safe implementation retained by the Registry and host application.
	handler NodeHandler
	// config is the canonical node JSON supplied defensively to every legacy runtime call.
	config []byte
}

// prepareRegisteredNodeHandler validates or prepares one registered handler's canonical node configuration.
//
// handler must be non-nil and config must already have valid JSON syntax. Preparers receive one detached copy and replace
// legacy Validate; other handlers are validated once and wrapped with a request-local raw-config compatibility executor.
// A nil prepared executor returns ErrInvalidHandler. The function performs no persistence or cross-request caching.
func prepareRegisteredNodeHandler(handler NodeHandler, config []byte) (PreparedNodeHandler, error) {
	if nilguard.IsNil(handler) {
		return nil, fmt.Errorf("%w: handler is nil", ErrInvalidHandler)
	}
	if preparer, ok := handler.(NodeHandlerConfigPreparer); ok {
		prepared, err := preparer.PrepareConfig(slices.Clone(config))
		if err != nil {
			return nil, fmt.Errorf("workflow: prepare node handler config: %w", err)
		}
		if nilguard.IsNil(prepared) {
			return nil, fmt.Errorf("%w: config preparer returned nil", ErrInvalidHandler)
		}
		return prepared, nil
	}

	// Legacy validation still runs exactly once per complete compilation before its executor can be published.
	if err := handler.Validate(slices.Clone(config)); err != nil {
		return nil, fmt.Errorf("workflow: validate legacy node handler config: %w", err)
	}
	return &legacyPreparedNodeHandler{handler: handler, config: slices.Clone(config)}, nil
}

// ActivatePrepared forwards one prepared-plan activation through the original NodeHandler input contract.
//
// input.Data is detached again alongside config so a legacy handler cannot mutate plan or aggregate ownership. The
// method retains no input, performs no I/O itself, and preserves the handler's error and cancellation behavior.
func (h *legacyPreparedNodeHandler) ActivatePrepared(
	ctx context.Context,
	input PreparedActivationInput,
) (NodeResult, error) {
	result, err := h.handler.Activate(ctx, ActivationInput{
		Config: slices.Clone(h.config),
		Data:   slices.Clone(input.Data),
	})
	if err != nil {
		return NodeResult{}, fmt.Errorf("workflow: activate legacy node handler: %w", err)
	}
	return result, nil
}

// HandlePrepared forwards one prepared-plan command through the original NodeHandler input contract.
//
// Every mutable input is detached from the compiled plan and candidate aggregate. The method retains no input, performs
// no I/O itself, and preserves the handler's complete task-view, error, and cancellation semantics.
func (h *legacyPreparedNodeHandler) HandlePrepared(
	ctx context.Context,
	input PreparedCommandInput,
) (NodeResult, error) {
	result, err := h.handler.Handle(ctx, CommandInput{
		Command: cloneCommand(input.Command),
		Config:  slices.Clone(h.config),
		Data:    slices.Clone(input.Data),
		State:   slices.Clone(input.State),
		Tasks:   slices.Clone(input.Tasks),
	})
	if err != nil {
		return NodeResult{}, fmt.Errorf("workflow: handle legacy node handler: %w", err)
	}
	return result, nil
}
