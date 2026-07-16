// This file defines the canonical workflow graph shared by code builders and web-authored JSON.
// Definitions contain data only: executable callbacks are resolved separately by stable handler names.
package workflow

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
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
	if err := json.Unmarshal(data, &definition); err != nil {
		return nil, fmt.Errorf("workflow: parse definition: %w", err)
	}
	if err := definition.Validate(); err != nil {
		return nil, err
	}
	return &definition, nil
}

// Validate checks the definition's identity, node uniqueness, entry/terminal cardinality, and edge references.
//
// Validation is read-only and safe to call concurrently when the Definition is not being mutated.
// ErrInvalidDefinition is present in every returned domain-validation error.
func (d *Definition) Validate() error {
	if d == nil {
		return fmt.Errorf("%w: definition is nil", ErrInvalidDefinition)
	}
	if d.ID == "" {
		return fmt.Errorf("%w: id is empty", ErrInvalidDefinition)
	}

	// Index nodes once so uniqueness, cardinality, and edge checks share the same source of truth.
	nodes := make(map[string]NodeDefinition, len(d.Nodes))
	startCount := 0
	startID := ""
	endCount := 0
	for _, node := range d.Nodes {
		if node.ID == "" || node.Kind == "" {
			return fmt.Errorf("%w: node id and kind must be non-empty", ErrInvalidDefinition)
		}
		if _, exists := nodes[node.ID]; exists {
			return fmt.Errorf("%w: duplicate node %q", ErrInvalidDefinition, node.ID)
		}
		nodes[node.ID] = node
		switch node.Kind {
		case KindStart:
			startCount++
			startID = node.ID
		case KindEnd:
			endCount++
		}
	}
	if startCount != 1 {
		return fmt.Errorf("%w: expected one start node, got %d", ErrInvalidDefinition, startCount)
	}
	if endCount == 0 {
		return fmt.Errorf("%w: expected at least one end node", ErrInvalidDefinition)
	}

	// Resolve every edge only after all nodes are indexed, allowing declarations in any order.
	selectors := make(map[edgeSelector]struct{}, len(d.Edges))
	for _, edge := range d.Edges {
		if _, exists := nodes[edge.From]; !exists {
			return fmt.Errorf("%w: edge source %q does not exist", ErrInvalidDefinition, edge.From)
		}
		if _, exists := nodes[edge.To]; !exists {
			return fmt.Errorf("%w: edge target %q does not exist", ErrInvalidDefinition, edge.To)
		}
		if nodes[edge.From].Kind == KindEnd {
			return fmt.Errorf("%w: end node %q has an outgoing edge", ErrInvalidDefinition, edge.From)
		}
		selector := edgeSelector{source: edge.From, outcome: edge.Outcome}
		// Duplicate selectors would make target choice depend on declaration order.
		if _, exists := selectors[selector]; exists {
			return fmt.Errorf(
				"%w: %w: definition %q node %q outcome %q selects multiple targets",
				ErrInvalidDefinition,
				ErrAmbiguousRoute,
				d.ID,
				edge.From,
				edge.Outcome,
			)
		}
		selectors[selector] = struct{}{}
	}
	if err := validateAcyclic(nodes, d.Edges); err != nil {
		return err
	}
	if err := validateReachable(nodes, d.Edges, startID); err != nil {
		return err
	}
	if err := validateCanReachEnd(nodes, d.Edges); err != nil {
		return err
	}
	return nil
}

// validateAcyclic uses Kahn's algorithm to reject any cycle without recursive stack growth.
//
// nodes must contain every edge endpoint. The function allocates adjacency and indegree indexes
// proportional to the graph size and returns ErrInvalidDefinition when no topological ordering exists.
func validateAcyclic(nodes map[string]NodeDefinition, edges []Edge) error {
	// Build graph indexes once; indegree records how many predecessors must be removed before a node is ready.
	adjacency := make(map[string][]string, len(nodes))
	indegree := make(map[string]int, len(nodes))
	for id := range nodes {
		indegree[id] = 0
	}
	for _, edge := range edges {
		adjacency[edge.From] = append(adjacency[edge.From], edge.To)
		indegree[edge.To]++
	}

	// Seed all roots so disconnected acyclic components are still counted rather than mistaken for cycles.
	queue := make([]string, 0, len(nodes))
	for id, degree := range indegree {
		if degree == 0 {
			queue = append(queue, id)
		}
	}

	// Removing each ready node simulates a topological ordering and exposes its newly ready successors.
	visited := 0
	for len(queue) > 0 {
		id := queue[0]
		queue = queue[1:]
		visited++
		for _, target := range adjacency[id] {
			indegree[target]--
			if indegree[target] == 0 {
				queue = append(queue, target)
			}
		}
	}
	if visited != len(nodes) {
		return fmt.Errorf("%w: graph contains a cycle", ErrInvalidDefinition)
	}
	return nil
}

// validateReachable rejects dead nodes that cannot be entered from the single start node.
//
// startID must identify the validated start node. The breadth-first scan follows edge direction because
// a node reachable only by a backward reference is not executable from a new instance.
func validateReachable(nodes map[string]NodeDefinition, edges []Edge, startID string) error {
	// Build outgoing adjacency independently from cycle validation to keep each invariant locally auditable.
	adjacency := make(map[string][]string, len(nodes))
	for _, edge := range edges {
		adjacency[edge.From] = append(adjacency[edge.From], edge.To)
	}

	// Visit each node at most once so converging branches do not inflate work or enqueue duplicates.
	visited := map[string]bool{startID: true}
	queue := []string{startID}
	for len(queue) > 0 {
		id := queue[0]
		queue = queue[1:]
		for _, target := range adjacency[id] {
			if visited[target] {
				continue
			}
			visited[target] = true
			queue = append(queue, target)
		}
	}
	if len(visited) != len(nodes) {
		return fmt.Errorf("%w: graph contains an unreachable node", ErrInvalidDefinition)
	}
	return nil
}

// validateCanReachEnd rejects reachable branches that can never arrive at a successful end node.
//
// The reverse scan starts from every end node and follows predecessor links. Nodes absent from the final
// visited set are dead ends even when they are reachable from start.
func validateCanReachEnd(nodes map[string]NodeDefinition, edges []Edge) error {
	// Reverse edges so one traversal answers whether each node has any forward path to an end node.
	predecessors := make(map[string][]string, len(nodes))
	queue := make([]string, 0)
	visited := make(map[string]bool, len(nodes))
	for id, node := range nodes {
		if node.Kind == KindEnd {
			queue = append(queue, id)
			visited[id] = true
		}
	}
	for _, edge := range edges {
		predecessors[edge.To] = append(predecessors[edge.To], edge.From)
	}

	// Mark every predecessor that can eventually flow into any valid terminal node.
	for len(queue) > 0 {
		id := queue[0]
		queue = queue[1:]
		for _, source := range predecessors[id] {
			if visited[source] {
				continue
			}
			visited[source] = true
			queue = append(queue, source)
		}
	}
	if len(visited) != len(nodes) {
		return fmt.Errorf("%w: graph contains a branch that cannot reach an end node", ErrInvalidDefinition)
	}
	return nil
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
