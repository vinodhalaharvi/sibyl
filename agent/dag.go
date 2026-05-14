// Package agent — dag.go defines a static directed-acyclic-graph executor
// that runs heterogeneous weft.Arrow nodes with topological scheduling and
// per-level parallelism.
//
// Why a DAG?
//
//	A weft.Pipe is a straight line: A -> B -> C. A supervisor is a tree
//	one level deep: parent fans out to N siblings, joins. A DAG is the
//	general case: arbitrary node-to-node dependencies, with selective
//	fan-in (a node depends on a SUBSET of upstreams) and heterogeneous
//	node types. See README "DAG agent" section for use cases.
//
// Why type-erased nodes?
//
//	Each node has its own (Input, Output) types, but we need one
//	executor. Go generics aren't expressive enough to type-track a
//	whole graph without making the API painful. So node arrows speak
//	`any` at the boundary; users wrap typed arrows with `Node` builders
//	that handle the conversion. Type safety lives in the builder, not
//	the executor.
package agent

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/vinodhalaharvi/weft/weft"
)

// AnyArrow is the type-erased shape every DAG node arrow takes.
// Inputs and outputs are `any`; the Node builder handles conversion.
type AnyArrow = weft.Arrow[any, any]

// NodeID is a user-supplied identifier for a DAG node. Two nodes with
// the same ID is a configuration error.
type NodeID string

// Node is one vertex in a DAG. It holds the arrow that produces this
// node's output and the list of upstream node IDs whose outputs are
// required before this node can run.
type Node struct {
	ID       NodeID
	Arrow    AnyArrow
	Requires []NodeID
}

// NodeOption customizes a Node. Use DependsOn to declare dependencies.
type NodeOption func(*Node)

// DependsOn adds upstream IDs to a node's requirement list.
func DependsOn(ids ...NodeID) NodeOption {
	return func(n *Node) { n.Requires = append(n.Requires, ids...) }
}

// DAG is a static directed acyclic graph of weft Arrows.
//
// Construction: AddNode for each node, declaring its dependencies via
// DependsOn. The graph is validated (no cycles, all deps exist) at
// Compile() time, not Execute() time, so configuration errors surface
// early.
//
// Execution: Execute walks the graph in topological order. Nodes whose
// dependencies are all satisfied run in parallel, gated only by their
// upstream completion. The first node failure cancels the rest via
// context cancellation.
//
// Outputs: each node's output is stored in the result map keyed by its
// NodeID. Downstream nodes pull what they need from this map via the
// NodeInputs they receive.
type DAG struct {
	nodes []Node
	// indexed by NodeID for fast lookup after Compile
	byID map[NodeID]int
}

// NewDAG returns an empty DAG. Use AddNode to populate it.
func NewDAG() *DAG {
	return &DAG{
		byID: make(map[NodeID]int),
	}
}

// AddNode registers a node. Returns the NodeID so it can be used in
// later DependsOn() calls.
//
// AddTypedNode is the better API for most callers — it takes a typed
// arrow and handles the type erasure for you. AddNode is for cases
// where you want to construct an AnyArrow by hand.
func (d *DAG) AddNode(id NodeID, arrow AnyArrow, opts ...NodeOption) NodeID {
	n := Node{ID: id, Arrow: arrow}
	for _, opt := range opts {
		opt(&n)
	}
	d.nodes = append(d.nodes, n)
	d.byID[id] = len(d.nodes) - 1
	return id
}

// AddTypedNode is the typed convenience builder. Given a typed weft.Arrow
// and a function that constructs the node's input from upstream outputs,
// it wraps both in the type-erased shape the executor expects.
//
// The InputBuilder receives a NodeInputs map: keys are the upstream
// node IDs, values are the actual outputs (typed as `any`). The builder
// is responsible for asserting those into concrete types.
//
//	// Example: node B depends on A's []string output and produces an int.
//	bID := dag.AddTypedNode(
//	    "b",
//	    countWordsArrow,                                // weft.Arrow[[]string, int]
//	    func(in agent.NodeInputs) ([]string, error) {
//	        return in.MustGet("a").([]string), nil
//	    },
//	    agent.DependsOn("a"),
//	)
//
// The type-assertion discipline is on the caller. We trade some safety
// for executor simplicity. In practice users wrap this in helpers for
// their own type schemas; the executor stays generic.
func AddTypedNode[I, O any](
	d *DAG,
	id NodeID,
	arrow weft.Arrow[I, O],
	build func(NodeInputs) (I, error),
	opts ...NodeOption,
) NodeID {
	erased := AnyArrow(func(ctx context.Context, in any) (any, error) {
		inputs, ok := in.(NodeInputs)
		if !ok {
			return nil, fmt.Errorf("dag: node %q: expected NodeInputs, got %T", id, in)
		}
		typed, err := build(inputs)
		if err != nil {
			return nil, fmt.Errorf("dag: node %q input builder: %w", id, err)
		}
		out, err := arrow(ctx, typed)
		if err != nil {
			return nil, err
		}
		return any(out), nil
	})
	return d.AddNode(id, erased, opts...)
}

// NodeInputs is the type-erased map of upstream outputs passed to each
// node's input builder. Keys are NodeIDs of upstream nodes, values are
// their outputs.
type NodeInputs map[NodeID]any

// Get returns the value for id and whether it was present.
func (n NodeInputs) Get(id NodeID) (any, bool) {
	v, ok := n[id]
	return v, ok
}

// MustGet returns the value for id, panicking if missing. Use this in
// input builders when the dependency is statically known to exist
// (because you declared it via DependsOn). A missing entry indicates
// a programming error, not a runtime condition.
func (n NodeInputs) MustGet(id NodeID) any {
	v, ok := n[id]
	if !ok {
		panic(fmt.Sprintf("dag: node input %q not found; check DependsOn declarations", id))
	}
	return v
}

// CompiledDAG is the result of validating a DAG. The validation work
// (cycle detection, missing-dependency check, topological order) is
// done once; Execute can be called multiple times on the same compiled
// instance.
type CompiledDAG struct {
	dag *DAG
	// order is the topological order: order[i] is a list of node indices
	// (into dag.nodes) that have no remaining dependencies once layers
	// 0..i-1 have completed. Within a layer, nodes run in parallel.
	order [][]int
}

// Compile validates the DAG and returns a CompiledDAG ready to execute.
// Returns an error if:
//   - a node has a dependency on a non-existent NodeID
//   - the graph contains a cycle
//   - duplicate NodeIDs were added
func (d *DAG) Compile() (*CompiledDAG, error) {
	if err := d.validateUnique(); err != nil {
		return nil, err
	}
	if err := d.validateDeps(); err != nil {
		return nil, err
	}
	order, err := d.topoLayers()
	if err != nil {
		return nil, err
	}
	return &CompiledDAG{dag: d, order: order}, nil
}

func (d *DAG) validateUnique() error {
	seen := make(map[NodeID]struct{}, len(d.nodes))
	for _, n := range d.nodes {
		if _, dup := seen[n.ID]; dup {
			return fmt.Errorf("dag: duplicate node id %q", n.ID)
		}
		seen[n.ID] = struct{}{}
	}
	return nil
}

func (d *DAG) validateDeps() error {
	for _, n := range d.nodes {
		for _, dep := range n.Requires {
			if _, ok := d.byID[dep]; !ok {
				return fmt.Errorf("dag: node %q depends on unknown node %q", n.ID, dep)
			}
			if dep == n.ID {
				return fmt.Errorf("dag: node %q depends on itself", n.ID)
			}
		}
	}
	return nil
}

// topoLayers groups nodes into execution "layers". Layer 0 has all
// nodes with no dependencies; layer i has all nodes whose dependencies
// are all in layers 0..i-1. This gives us a Kahn's-algorithm-style
// topological order with maximal per-layer parallelism.
//
// Returns an error if a cycle is detected.
func (d *DAG) topoLayers() ([][]int, error) {
	// in-degree count per node index
	indegree := make([]int, len(d.nodes))
	// reverse adjacency: deps[i] = nodes that depend on i
	dependents := make(map[int][]int, len(d.nodes))

	for i, n := range d.nodes {
		indegree[i] = len(n.Requires)
		for _, dep := range n.Requires {
			depIdx := d.byID[dep]
			dependents[depIdx] = append(dependents[depIdx], i)
		}
	}

	var layers [][]int
	remaining := len(d.nodes)
	for remaining > 0 {
		// Collect all nodes with zero remaining in-degree.
		var layer []int
		for i, deg := range indegree {
			if deg == 0 {
				layer = append(layer, i)
			}
		}
		if len(layer) == 0 {
			return nil, errors.New("dag: cycle detected (no nodes with zero in-degree but work remains)")
		}
		// Sort the layer by NodeID for deterministic execution order
		// within a layer. The arrows still run in parallel, but the
		// SCHEDULE order is stable, which helps debugging.
		sort.Slice(layer, func(a, b int) bool {
			return d.nodes[layer[a]].ID < d.nodes[layer[b]].ID
		})
		layers = append(layers, layer)

		// Mark these nodes done: set indegree[-1] (out of consideration)
		// and decrement dependents' in-degree.
		for _, idx := range layer {
			indegree[idx] = -1 // never picked up again
			for _, dependent := range dependents[idx] {
				indegree[dependent]--
			}
		}
		remaining -= len(layer)
	}
	return layers, nil
}

// NodeError wraps an error from a specific node so callers can identify
// which node failed in a DAG run.
type NodeError struct {
	NodeID NodeID
	Err    error
}

func (e *NodeError) Error() string {
	return fmt.Sprintf("dag: node %q failed: %v", e.NodeID, e.Err)
}
func (e *NodeError) Unwrap() error { return e.Err }

// Execute runs the DAG to completion. Returns a map from NodeID to
// each node's output (typed as `any`; callers cast at retrieval).
//
// Concurrency: nodes within a layer run in parallel goroutines. Layers
// are sequential. The first node failure cancels the context for all
// in-flight siblings; they may complete their current operation but
// won't start new work. The returned error is a *NodeError pointing at
// the first failure.
//
// If a node fails, the result map contains outputs for all nodes that
// completed before the failure was observed. Downstream nodes never
// run. This is the "fail-fast within a DAG" policy — for "swallow
// failures" semantics like the supervisor, wrap nodes individually
// with arrow-level retry/recover logic.
func (c *CompiledDAG) Execute(ctx context.Context, seed map[NodeID]any) (map[NodeID]any, error) {
	results := make(map[NodeID]any, len(c.dag.nodes))
	// Seed values let callers pre-populate nodes that don't have an
	// arrow — e.g. a literal "input" node. These nodes can be referenced
	// in DependsOn but won't actually run. (Not used in the simple API;
	// reserved for an Input/Output node convention.)
	for k, v := range seed {
		results[k] = v
	}

	// Mutex protects results map; goroutines write their outputs into it.
	var mu sync.Mutex

	for layerIdx, layer := range c.order {
		// Run this layer's nodes in parallel. If any fail, capture the
		// first error; don't start subsequent layers.
		errCh := make(chan error, len(layer))
		var wg sync.WaitGroup

		// Per-layer context that we'll cancel on first failure to abort
		// concurrent siblings still running.
		layerCtx, cancelLayer := context.WithCancel(ctx)

		for _, idx := range layer {
			n := c.dag.nodes[idx]
			wg.Add(1)
			go func(n Node) {
				defer wg.Done()
				// Skip nodes that have already been seeded by the caller.
				mu.Lock()
				_, alreadyHave := results[n.ID]
				mu.Unlock()
				if alreadyHave {
					return
				}

				// Assemble the NodeInputs map from upstream outputs.
				inputs := make(NodeInputs, len(n.Requires))
				mu.Lock()
				for _, dep := range n.Requires {
					inputs[dep] = results[dep]
				}
				mu.Unlock()

				out, err := n.Arrow(layerCtx, inputs)
				if err != nil {
					errCh <- &NodeError{NodeID: n.ID, Err: err}
					cancelLayer()
					return
				}
				mu.Lock()
				results[n.ID] = out
				mu.Unlock()
			}(n)
		}

		wg.Wait()
		cancelLayer() // release the per-layer context regardless of outcome
		close(errCh)

		// Return the first error encountered in this layer.
		if err, ok := <-errCh; ok {
			return results, err
		}

		_ = layerIdx // reserved for tracing if we add it
	}
	return results, nil
}

// MustCompile is Compile that panics on error. Useful for graphs defined
// at init time where any validation error is a programming bug.
func (d *DAG) MustCompile() *CompiledDAG {
	c, err := d.Compile()
	if err != nil {
		panic(err)
	}
	return c
}
