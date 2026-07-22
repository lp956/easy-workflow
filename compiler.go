// This file compiles canonical workflow data into a package-internal execution plan.
// Plans are immutable request-local indexes; they are never serialized or persisted with Definition JSON.
package workflow

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"

	"github.com/lvpeng/easy-workflow/internal/nilguard"
)

// ErrInvalidNodeConfig identifies configuration rejected by JSON validation or its registered handler.
var ErrInvalidNodeConfig = errors.New("workflow: invalid node config")

// definitionAnalysis owns all graph-derived data needed by structural validation and execution-plan compilation.
// It is request-local and immutable after successful analysis; its maps and slices can be transferred into a
// compiled plan without retaining the analyzed Definition. It does not validate handlers or their configuration.
type definitionAnalysis struct {
	// startID identifies the only validated start node.
	startID string
	// endIDs contains every validated end node used to seed reverse reachability.
	endIDs []string
	// nodes maps each node ID to its canonical slice position.
	nodes map[string]int
	// routes maps each deterministic source-and-outcome selector to its target node ID.
	routes map[edgeSelector]string
	// adjacency contains forward traversal relationships for cycle and start-reachability checks.
	adjacency map[string][]string
	// predecessors contains reverse traversal relationships for end-reachability checks.
	predecessors map[string][]string
	// indegree records each node's validated incoming-edge count for topological analysis.
	indegree map[string]int
}

// compiledNode binds one canonical node position to its request-local prepared runtime executor.
//
// handler is nil only for start and end control nodes. Values are immutable after compilation and are never serialized,
// persisted, or reused across complete compilation operations.
type compiledNode struct {
	// index is the node's stable position in compiledDefinition.definition.Nodes.
	index int
	// handler owns prepared business-node configuration for this one executable plan.
	handler PreparedNodeHandler
}

// compiledDefinition owns a frozen Definition, deterministic graph indexes, and prepared node executors.
// It is immutable after construction and safe for concurrent reads within one Engine operation.
type compiledDefinition struct {
	// definition is the canonical snapshot whose slice order remains available for persistence compatibility.
	definition Definition
	// startID identifies the single control entry validated for unconditional routing.
	startID string
	// nodes maps each stable node ID to its canonical position and optional prepared business-node executor.
	nodes map[string]compiledNode
	// routes maps a source-and-outcome selector to its single validated target node ID.
	routes map[edgeSelector]string
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
// The returned plan owns all mutable Definition data. registry is read only during compilation; missing handlers,
// invalid handler configuration, and Start selectors that runtime cannot produce fail before a plan is returned.
func compileDefinition(definition *Definition, registry *Registry) (*compiledDefinition, error) {
	// Freeze canonical data first so analysis, handler validation, and runtime indexes share one stable snapshot.
	definitionID := ""
	var frozenDefinition *Definition
	if definition != nil {
		definitionID = definition.ID
		frozen := cloneDefinition(*definition)
		frozenDefinition = &frozen
	}
	analysis, err := analyzeDefinition(frozenDefinition)
	// Structural failure prevents handler lookup and execution-plan publication.
	if err != nil {
		return nil, fmt.Errorf("definition %q: %w", definitionID, err)
	}

	// Transfer analyzed positions into runtime nodes without retaining analysis or exposing executable data canonically.
	compiledNodes := make(map[string]compiledNode, len(analysis.nodes))
	for id, index := range analysis.nodes {
		compiledNodes[id] = compiledNode{index: index}
	}
	plan := &compiledDefinition{
		definition: *frozenDefinition,
		startID:    analysis.startID,
		nodes:      compiledNodes,
		routes:     analysis.routes,
	}

	// Validate every canonical JSON value before Registry state can affect the reported failure.
	for index := range plan.definition.Nodes {
		node := &plan.definition.Nodes[index]
		// Malformed RawMessage values cannot participate in stable canonical JSON, including on control nodes.
		if len(node.Config) > 0 && !json.Valid(node.Config) {
			return nil, fmt.Errorf(
				"%w: %w: definition %q node %q config is not valid json",
				ErrInvalidDefinition,
				ErrInvalidNodeConfig,
				plan.definition.ID,
				node.ID,
			)
		}
	}

	// Engine startup always selects the empty outcome, so a named-only start edge is not executable.
	if _, exists := plan.routes[edgeSelector{source: plan.startID}]; !exists {
		return nil, fmt.Errorf(
			"%w: %w: definition %q node %q outcome %q",
			ErrInvalidDefinition,
			ErrRouteNotFound,
			plan.definition.ID,
			plan.startID,
			"",
		)
	}
	// The unconditional selector is the sole runtime Start result, so every additional outgoing edge would be dead config.
	if len(analysis.adjacency[plan.startID]) != 1 {
		return nil, fmt.Errorf(
			"%w: definition %q start node %q has %d outgoing routes; want one unconditional route",
			ErrInvalidDefinition,
			plan.definition.ID,
			plan.startID,
			len(analysis.adjacency[plan.startID]),
		)
	}

	// Resolve and prepare handler-owned configuration only after canonical syntax is known to be sound.
	for index := range plan.definition.Nodes {
		node := &plan.definition.Nodes[index]
		if node.Kind == KindStart {
			continue // Control nodes have no registered handler configuration contract.
		}
		if node.Kind == KindEnd {
			continue // Terminal control nodes likewise require no handler lookup.
		}
		handler, err := registry.handler(node.Kind)
		// A missing handler makes the node non-executable regardless of graph validity.
		if err != nil {
			return nil, fmt.Errorf(
				"%w: definition %q node %q: %w",
				ErrInvalidDefinition,
				plan.definition.ID,
				node.ID,
				err,
			)
		}
		// Preparation performs complete validation once and yields the only executor retained by this request-local plan.
		prepared, err := prepareRegisteredNodeHandler(handler, node.Config)
		if err != nil {
			return nil, fmt.Errorf(
				"%w: %w: definition %q node %q config: %w",
				ErrInvalidDefinition,
				ErrInvalidNodeConfig,
				plan.definition.ID,
				node.ID,
				err,
			)
		}
		compiled := plan.nodes[node.ID]
		compiled.handler = prepared
		plan.nodes[node.ID] = compiled
	}

	return plan, nil
}

// analyzeDefinition derives and validates every structural graph index in one pass over nodes and one pass over edges.
//
// definition must contain a stable ID, exactly one start, at least one end, unique node IDs, and deterministic valid edge
// references. The returned analysis owns all derived maps and slices; node positions refer to the supplied canonical order
// without retaining definition. Complete compilation separately enforces the executable Start selector contract.
func analyzeDefinition(definition *Definition) (*definitionAnalysis, error) {
	// Reject incomplete identity before deriving allocation sizes or indexing caller-owned graph data.
	if definition == nil {
		return nil, fmt.Errorf("%w: definition is nil", ErrInvalidDefinition)
	}
	// An empty stable identity cannot own published versions or persisted instances.
	if definition.ID == "" {
		return nil, fmt.Errorf("%w: id is empty", ErrInvalidDefinition)
	}

	// Build the complete node index and control-node seeds once for every later validation pass.
	analysis := &definitionAnalysis{
		endIDs:       make([]string, 0),
		nodes:        make(map[string]int, len(definition.Nodes)),
		routes:       make(map[edgeSelector]string, len(definition.Edges)),
		adjacency:    make(map[string][]string, len(definition.Nodes)),
		predecessors: make(map[string][]string, len(definition.Nodes)),
		indegree:     make(map[string]int, len(definition.Nodes)),
	}
	if err := analysis.indexNodes(definition); err != nil {
		return nil, err
	}
	if err := analysis.indexEdges(definition); err != nil {
		return nil, err
	}

	// Reuse the derived graph for every invariant so no validation pass reconstructs edge relationships.
	// A cyclic graph has no meaningful forward or reverse reachability result to preserve.
	if err := analysis.validateAcyclic(); err != nil {
		return nil, err
	}
	// A node outside the start traversal cannot participate in executable end reachability.
	if err := analysis.validateReachable(); err != nil {
		return nil, err
	}
	// A reachable dead end is the final structural failure before the analysis can be shared.
	if err := analysis.validateCanReachEnd(); err != nil {
		return nil, err
	}
	return analysis, nil
}

// indexNodes validates canonical node identity and records control-node and topological seeds in one pass.
//
// definition must be non-nil and a must be freshly allocated with empty indexes. The method records canonical slice
// positions, requires exactly one Start and at least one End, and returns ErrInvalidDefinition without retaining node data.
func (a *definitionAnalysis) indexNodes(definition *Definition) error {
	startCount := 0
	for index, node := range definition.Nodes {
		// Both fields participate in persistence and runtime lookup, so neither has a useful zero value.
		if node.ID == "" || node.Kind == "" {
			return fmt.Errorf("%w: node id and kind must be non-empty", ErrInvalidDefinition)
		}
		// Duplicate IDs would make execution depend on declaration order.
		if _, exists := a.nodes[node.ID]; exists {
			return fmt.Errorf("%w: duplicate node %q", ErrInvalidDefinition, node.ID)
		}
		a.nodes[node.ID] = index
		a.indegree[node.ID] = 0
		switch node.Kind {
		case KindStart:
			startCount++
			a.startID = node.ID
		case KindEnd:
			a.endIDs = append(a.endIDs, node.ID)
		default:
			// Business node kinds are validated through Registry during complete compilation.
		}
	}
	// Exactly one start keeps startup routing deterministic.
	if startCount != 1 {
		return fmt.Errorf("%w: expected one start node, got %d", ErrInvalidDefinition, startCount)
	}
	// At least one end is required for every validated branch to terminate successfully.
	if len(a.endIDs) == 0 {
		return fmt.Errorf("%w: expected at least one end node", ErrInvalidDefinition)
	}
	return nil
}

// indexEdges validates canonical references and derives routing, traversal, and topological indexes in one pass.
//
// definition nodes must already be indexed in a. Every source and target must resolve, End nodes cannot have successors,
// and each source-outcome selector must be unique. Errors wrap ErrInvalidDefinition and no caller-owned data is mutated.
func (a *definitionAnalysis) indexEdges(definition *Definition) error {
	for _, edge := range definition.Edges {
		sourceIndex, sourceExists := a.nodes[edge.From]
		// Every source must resolve before its kind can be checked safely.
		if !sourceExists {
			return fmt.Errorf("%w: edge source %q does not exist", ErrInvalidDefinition, edge.From)
		}
		// Every target must be part of the same canonical Definition snapshot.
		if _, targetExists := a.nodes[edge.To]; !targetExists {
			return fmt.Errorf("%w: edge target %q does not exist", ErrInvalidDefinition, edge.To)
		}
		// Terminal nodes cannot hide executable successors behind serialized edges.
		if definition.Nodes[sourceIndex].Kind == KindEnd {
			return fmt.Errorf("%w: end node %q has an outgoing edge", ErrInvalidDefinition, edge.From)
		}
		selector := edgeSelector{source: edge.From, outcome: edge.Outcome}
		// Duplicate selectors would make target choice depend on declaration order.
		if _, exists := a.routes[selector]; exists {
			return fmt.Errorf(
				"%w: %w: definition %q node %q outcome %q selects multiple targets",
				ErrInvalidDefinition,
				ErrAmbiguousRoute,
				definition.ID,
				edge.From,
				edge.Outcome,
			)
		}
		a.routes[selector] = edge.To
		a.adjacency[edge.From] = append(a.adjacency[edge.From], edge.To)
		a.predecessors[edge.To] = append(a.predecessors[edge.To], edge.From)
		a.indegree[edge.To]++
	}
	return nil
}

// validateAcyclic uses the analyzed indegree and adjacency data to reject cycles without recursive stack growth.
// It clones only integer counters, leaves the reusable analysis immutable, and returns ErrInvalidDefinition when
// no topological ordering can contain every node.
func (a *definitionAnalysis) validateAcyclic() error {
	// Seed every root from a disposable counter map so disconnected components still participate in cycle detection.
	remaining := maps.Clone(a.indegree)
	queue := make([]string, 0, len(a.nodes))
	for id, degree := range remaining {
		if degree == 0 {
			queue = append(queue, id)
		}
	}

	// Remove ready nodes in topological order and expose successors when their final predecessor is removed.
	visited := 0
	for len(queue) > 0 {
		id := queue[0]    // The queue front is the next node whose incoming dependencies are satisfied.
		queue = queue[1:] // Advancing one element preserves FIFO traversal without rebuilding graph indexes.
		visited++
		for _, target := range a.adjacency[id] {
			remaining[target]--
			if remaining[target] == 0 {
				queue = append(queue, target)
			}
		}
	}
	// A smaller ordering means at least one component retained a cyclic incoming dependency.
	if visited != len(a.nodes) {
		return fmt.Errorf("%w: graph contains a cycle", ErrInvalidDefinition)
	}
	return nil
}

// validateReachable verifies every analyzed node can be entered from the single start node.
// The breadth-first scan reads the shared forward adjacency, allocates request-local visit state, and returns
// ErrInvalidDefinition when canonical data contains any unreachable node.
func (a *definitionAnalysis) validateReachable() error {
	// Visit each node at most once so converging branches do not inflate work or enqueue duplicates.
	visited := map[string]bool{a.startID: true}
	queue := []string{a.startID}
	for len(queue) > 0 {
		id := queue[0]    // The queue front preserves breadth-first traversal from the start node.
		queue = queue[1:] // Removing the processed front keeps every remaining node pending exactly once.
		for _, target := range a.adjacency[id] {
			if visited[target] {
				continue // Converging routes do not require a second traversal of the same successor.
			}
			visited[target] = true
			queue = append(queue, target)
		}
	}
	// Any node outside the start traversal is dead canonical configuration.
	if len(visited) != len(a.nodes) {
		return fmt.Errorf("%w: graph contains an unreachable node", ErrInvalidDefinition)
	}
	return nil
}

// validateCanReachEnd verifies every analyzed node has a forward path to at least one end node.
// The reverse breadth-first scan reads shared predecessor data and returns ErrInvalidDefinition for reachable
// dead-end branches; it does not mutate the reusable analysis.
func (a *definitionAnalysis) validateCanReachEnd() error {
	// Seed every terminal node, then walk predecessors to mark all nodes that can eventually terminate.
	queue := append([]string(nil), a.endIDs...)
	visited := make(map[string]bool, len(a.nodes))
	for _, id := range a.endIDs {
		visited[id] = true
	}
	for len(queue) > 0 {
		id := queue[0]    // The queue front is the next node with a proven path to an end.
		queue = queue[1:] // Removing the processed front leaves the remaining reverse frontier intact.
		for _, source := range a.predecessors[id] {
			if visited[source] {
				continue // Multiple terminal paths still require only one proof for each predecessor.
			}
			visited[source] = true
			queue = append(queue, source)
		}
	}
	// A node absent from the reverse traversal cannot finish successfully along any outgoing branch.
	if len(visited) != len(a.nodes) {
		return fmt.Errorf("%w: graph contains a branch that cannot reach an end node", ErrInvalidDefinition)
	}
	return nil
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
	compiled, exists := p.nodes[id]
	// A miss indicates a corrupted runtime snapshot because compilation indexed every declared node.
	if !exists {
		return nil, fmt.Errorf("%w: definition %q node %q not found", ErrInvalidDefinition, p.definition.ID, id)
	}
	return &p.definition.Nodes[compiled.index], nil
}

// preparedHandler returns one business node's request-local executable config without consulting Registry again.
//
// id must identify a compiled non-control node. The returned executor is owned by the plan, is never persisted, and may
// be used only during the enclosing Engine operation. Missing or control-node handlers return ErrHandlerNotFound.
func (p *compiledDefinition) preparedHandler(id string) (PreparedNodeHandler, error) {
	compiled, exists := p.nodes[id]
	if !exists || nilguard.IsNil(compiled.handler) {
		return nil, fmt.Errorf("%w: %q", ErrHandlerNotFound, id)
	}
	return compiled.handler, nil
}

// nextNode resolves exactly one compiled outcome route from source to its target node.
//
// source and outcome form the complete route selector; an empty outcome denotes an unconditional edge.
// Missing selectors return ErrRouteNotFound with Definition, node, and outcome context. The returned node
// is read-only and owned by the plan.
func (p *compiledDefinition) nextNode(source, outcome string) (*NodeDefinition, error) {
	target, exists := p.routes[edgeSelector{source: source, outcome: outcome}]
	// Fail at the exact selector so handlers cannot fall through to slice-order-dependent routing.
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
