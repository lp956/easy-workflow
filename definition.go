// This file defines the canonical workflow graph shared by code builders and web-authored JSON.
// Definitions contain data only: executable callbacks are resolved separately by stable handler names.
package workflow

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"

	"github.com/lvpeng/easy-workflow/internal/jsonstrict"
)

const (
	// KindStart identifies the single entry node required by every definition.
	KindStart = "start"
	// KindEnd identifies a successful terminal node; a definition may contain multiple end nodes.
	KindEnd = "end"
)

var (
	// ErrInvalidDefinition identifies a malformed or internally inconsistent workflow graph.
	ErrInvalidDefinition = errors.New("workflow: invalid definition")
	// ErrAmbiguousRoute identifies one node outcome that selects more than one target.
	ErrAmbiguousRoute = errors.New("workflow: ambiguous route")
	// ErrRouteNotFound identifies an outcome for which the current node declares no target.
	ErrRouteNotFound = errors.New("workflow: route not found")
)

// Definition is the canonical, JSON-serializable workflow graph.
//
// ID is a stable business identifier. Nodes and Edges are ordered for deterministic JSON output,
// but execution semantics never depend on their slice positions. Callers may construct a Definition
// directly, although Builder is safer for code-authored flows. Validate must succeed before execution.
type Definition struct {
	ID      string           `json:"id"`
	Version uint64           `json:"version"`
	Nodes   []NodeDefinition `json:"nodes"`
	Edges   []Edge           `json:"edges"`
}

// NodeDefinition describes one graph node without embedding executable Go behavior.
//
// Kind is a stable registry key such as "approval". Config is owned and decoded by that node handler;
// the workflow core only validates that it is JSON and preserves its bytes across persistence.
type NodeDefinition struct {
	ID     string          `json:"id"`
	Kind   string          `json:"kind"`
	Config json.RawMessage `json:"config,omitempty"`
}

// Edge connects two nodes for a handler outcome.
//
// Outcome is empty for unconditional transitions. For a given source node, each outcome may select
// at most one target so runtime routing remains deterministic.
type Edge struct {
	From    string `json:"from"`
	To      string `json:"to"`
	Outcome string `json:"outcome,omitempty"`
}

// edgeSelector is the deterministic routing key formed by a source node and one handler outcome.
type edgeSelector struct {
	source  string
	outcome string
}

// Builder incrementally creates a Definition while deferring validation until Build.
//
// Builder is not safe for concurrent use. Methods retain the first configuration error so fluent
// construction can continue without ignored return values; Build reports that error.
type Builder struct {
	definition Definition
	err        error
}

// NewBuilder creates an empty version-one definition with the supplied stable identifier.
//
// The identifier must be non-empty; validation is deferred to Build so code-authored and parsed
// definitions follow the same error contract.
func NewBuilder(id string) *Builder {
	return &Builder{definition: Definition{ID: id, Version: 1}}
}

// Start appends the definition's entry node.
//
// id must be unique and non-empty. A second start node is rejected when Build validates the graph.
func (b *Builder) Start(id string) *Builder {
	return b.Node(id, KindStart, nil)
}

// End appends a successful terminal node.
//
// id must be unique and non-empty. End nodes may not have outgoing edges once the full DAG validator
// is applied.
func (b *Builder) End(id string) *Builder {
	return b.Node(id, KindEnd, nil)
}

// Node appends a node whose configuration can be encoded as JSON.
//
// id and kind must be non-empty and id must be unique within the definition. config is a true
// serialization boundary: nil means no configuration, while any other value is marshaled immediately.
// Marshal failures are retained and returned by Build. This method has no side effects outside b.
func (b *Builder) Node(id, kind string, config any) *Builder {
	if b.err != nil {
		return b
	}

	// Encode configuration at definition time so invalid business config cannot leak into persistence.
	var raw json.RawMessage
	if config != nil {
		data, err := json.Marshal(config)
		if err != nil {
			b.err = fmt.Errorf("workflow: marshal node %q config: %w", id, err)
			return b
		}
		raw = data
	}

	b.definition.Nodes = append(b.definition.Nodes, NodeDefinition{ID: id, Kind: kind, Config: raw})
	return b
}

// Connect appends a directed edge selected by outcome.
//
// from and to are node identifiers in this builder. An empty outcome represents an unconditional
// edge. Reference and determinism checks are performed by Build after all nodes have been declared.
func (b *Builder) Connect(from, to, outcome string) *Builder {
	if b.err != nil {
		return b
	}
	b.definition.Edges = append(b.definition.Edges, Edge{From: from, To: to, Outcome: outcome})
	return b
}

// Build validates the accumulated graph and returns a definition detached from builder-owned slices.
//
// The returned Definition can be safely modified without changing the Builder. ErrInvalidDefinition
// is present in the error chain for graph violations; configuration encoding errors retain their
// original cause.
func (b *Builder) Build() (*Definition, error) {
	if b.err != nil {
		return nil, b.err
	}

	// Clone every mutable field before exposing the result to prevent later builder reuse from aliasing it.
	definition := cloneDefinition(b.definition)
	if err := definition.Validate(); err != nil {
		return nil, err
	}
	return &definition, nil
}

// ParseDefinition decodes and validates a canonical definition received from JSON.
//
// data must contain one JSON object. Invalid JSON and graph violations are returned without fallback;
// successful results own their decoded slices and config bytes.
func ParseDefinition(data []byte) (*Definition, error) {
	var definition Definition
	if err := jsonstrict.Decode(data, &definition); err != nil {
		return nil, fmt.Errorf("workflow: parse definition: %w", err)
	}
	if err := definition.Validate(); err != nil {
		return nil, err
	}
	return &definition, nil
}

// Validate checks every canonical graph invariant without resolving registered business handlers.
//
// Validation delegates to the compiler's shared graph analysis so Builder, JSON parsing, publication, and
// Engine compilation cannot drift on structural rules. It is read-only and safe for concurrent calls when
// callers do not mutate the Definition; every domain-validation error wraps ErrInvalidDefinition.
func (d *Definition) Validate() error {
	_, err := analyzeDefinition(d)
	return err
}

// cloneDefinition returns a deep-enough copy for all mutable definition fields.
func cloneDefinition(source Definition) Definition {
	cloned := source
	cloned.Nodes = slices.Clone(source.Nodes)
	for i := range cloned.Nodes {
		cloned.Nodes[i].Config = slices.Clone(source.Nodes[i].Config)
	}
	cloned.Edges = slices.Clone(source.Edges)
	return cloned
}
