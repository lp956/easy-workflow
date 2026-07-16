// This file compiles canonical workflow data into a package-internal execution plan.
// Plans are immutable request-local indexes; they are never serialized or persisted with Definition JSON.
package workflow

import (
	"encoding/json"
	"fmt"
)

// compiledDefinition owns a frozen Definition and its deterministic node and outcome lookup indexes.
// It is immutable after construction and safe for concurrent reads within one Engine operation.
type compiledDefinition struct {
	definition Definition
	startID    string
	nodes      map[string]int
	routes     map[edgeSelector]string
}

// CompileDefinition performs complete graph and registered-handler validation without persisting state.
//
// definition must be non-nil and registry must contain every non-control node kind. The function builds
// the same internal execution plan consumed by Engine, then discards it after validation. Returned errors
// identify the failing Definition and, where applicable, its node. It has no persistence side effects and
// is safe for concurrent calls when callers do not mutate definition during compilation.
func CompileDefinition(definition *Definition, registry *Registry) error {
	_, err := compileDefinition(definition, registry)
	return err
}

// compileDefinition validates and freezes one Definition for indexed Engine execution.
//
// The returned plan owns all mutable Definition data. registry is read only during compilation; missing
// handlers and invalid handler configuration fail before a plan is returned.
func compileDefinition(definition *Definition, registry *Registry) (*compiledDefinition, error) {
	if err := definition.Validate(); err != nil {
		return nil, err
	}

	// Freeze canonical data before indexing so later caller mutation cannot invalidate plan lookups.
	frozen := cloneDefinition(*definition)
	plan := &compiledDefinition{
		definition: frozen,
		nodes:      make(map[string]int, len(frozen.Nodes)),
		routes:     make(map[edgeSelector]string, len(frozen.Edges)),
	}

	// Index every node and validate all business configuration against its registered owner.
	for index := range frozen.Nodes {
		node := &frozen.Nodes[index]
		plan.nodes[node.ID] = index
		if node.Kind == KindStart {
			plan.startID = node.ID
			continue
		}
		if node.Kind == KindEnd {
			continue
		}
		if len(node.Config) > 0 && !json.Valid(node.Config) {
			return nil, fmt.Errorf("%w: definition %q node %q config is not valid json", ErrInvalidDefinition, frozen.ID, node.ID)
		}
		handler, err := registry.handler(node.Kind)
		if err != nil {
			return nil, fmt.Errorf("definition %q node %q: %w", frozen.ID, node.ID, err)
		}
		if err := handler.Validate(node.Config); err != nil {
			return nil, fmt.Errorf("%w: definition %q node %q config: %w", ErrInvalidDefinition, frozen.ID, node.ID, err)
		}
	}

	// Materialize outcome routing once; graph validation has already proved every selector is unique.
	for _, edge := range frozen.Edges {
		plan.routes[edgeSelector{source: edge.From, outcome: edge.Outcome}] = edge.To
	}
	return plan, nil
}
