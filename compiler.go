// This file compiles canonical workflow data into a package-internal execution plan.
// Plans are immutable request-local indexes; they are never serialized or persisted with Definition JSON.
package workflow

import (
	"encoding/json"
	"errors"
	"fmt"
)

// ErrInvalidNodeConfig identifies configuration rejected by JSON validation or its registered handler.
var ErrInvalidNodeConfig = errors.New("workflow: invalid node config")

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
		definitionID := ""
		if definition != nil {
			definitionID = definition.ID
		}
		return nil, fmt.Errorf("definition %q: %w", definitionID, err)
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
			return nil, fmt.Errorf(
				"%w: %w: definition %q node %q config is not valid json",
				ErrInvalidDefinition,
				ErrInvalidNodeConfig,
				frozen.ID,
				node.ID,
			)
		}
		handler, err := registry.handler(node.Kind)
		if err != nil {
			return nil, fmt.Errorf("%w: definition %q node %q: %w", ErrInvalidDefinition, frozen.ID, node.ID, err)
		}
		if err := handler.Validate(node.Config); err != nil {
			return nil, fmt.Errorf(
				"%w: %w: definition %q node %q config: %w",
				ErrInvalidDefinition,
				ErrInvalidNodeConfig,
				frozen.ID,
				node.ID,
				err,
			)
		}
	}

	// Materialize outcome routing once; graph validation has already proved every selector is unique.
	for _, edge := range frozen.Edges {
		plan.routes[edgeSelector{source: edge.From, outcome: edge.Outcome}] = edge.To
	}
	return plan, nil
}

// startNode returns the single compiled entry node.
//
// A successfully compiled plan always contains startID. An error therefore identifies corrupted package
// state rather than caller-authored graph input. The returned pointer is read-only and owned by plan.
func (p *compiledDefinition) startNode() (*NodeDefinition, error) {
	return p.node(p.startID)
}

// node resolves one node ID through the compiled immutable index.
//
// id must belong to the frozen Definition. The returned pointer is read-only and remains valid for the
// plan lifetime; missing IDs return ErrInvalidDefinition with Definition and node context.
func (p *compiledDefinition) node(id string) (*NodeDefinition, error) {
	index, exists := p.nodes[id]
	if !exists {
		return nil, fmt.Errorf("%w: definition %q node %q not found", ErrInvalidDefinition, p.definition.ID, id)
	}
	return &p.definition.Nodes[index], nil
}

// nextNode resolves exactly one compiled outcome route from source to its target node.
//
// source and outcome form the complete route selector; an empty outcome denotes an unconditional edge.
// Missing selectors return ErrRouteNotFound with Definition, node, and outcome context. The returned node
// is read-only and owned by the plan.
func (p *compiledDefinition) nextNode(source, outcome string) (*NodeDefinition, error) {
	target, exists := p.routes[edgeSelector{source: source, outcome: outcome}]
	if !exists {
		return nil, fmt.Errorf(
			"%w: definition %q node %q outcome %q",
			ErrRouteNotFound,
			p.definition.ID,
			source,
			outcome,
		)
	}
	return p.node(target)
}
