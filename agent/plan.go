// Package agent — plan.go defines a serializable execution plan: a
// directed acyclic graph whose nodes reference activities by *name*
// (not by closure) so the whole graph can cross the Temporal
// workflow/activity boundary as plain data.
//
// Why a Plan (and not the old in-process DAG)?
//
//	Temporal dispatches activities by registered name with serializable
//	arguments; it cannot ship Go closures across the workflow/activity
//	boundary, and workflow replay requires activity code to live in
//	pre-registered activities. So the durable execution graph must be
//	*data*: node IDs, activity names, string arguments, and dependency
//	edges. PlanWorkflow walks this data and dispatches the named
//	activities. This is the lowering target for the AgentScript
//	translator — a compiled pipeline becomes a Plan.
//
// I/O contract (first cut): every plan activity takes a NodeInput and
// returns a string. Outputs are threaded to dependents by node ID. This
// mirrors Unix-pipe semantics — a stage's stdout becomes the next
// stage's stdin — and keeps everything trivially serializable. Richer
// typed payloads can come later without changing the Plan shape.
package agent

import (
	"fmt"
	"sort"
)

// PlanNodeID identifies a node within a Plan. Unique per plan.
type PlanNodeID string

// PlanNode is one vertex of a Plan: a named activity to run, the static
// arguments declared in source, and the upstream nodes whose outputs
// this node consumes.
type PlanNode struct {
	// ID is the node's unique identifier within the plan.
	ID PlanNodeID `json:"id"`
	// Activity is the registered Temporal activity name to dispatch
	// (e.g. "echo"). It must resolve to an activity registered on the
	// worker; Plan.Validate checks the graph shape but not registration
	// (that's the worker's responsibility at dispatch time).
	Activity string `json:"activity"`
	// Args are the static arguments from source (a builtin's literal
	// arguments). Passed to the activity alongside upstream outputs.
	Args []string `json:"args,omitempty"`
	// Requires lists upstream node IDs whose outputs feed this node.
	// Empty for source nodes.
	Requires []PlanNodeID `json:"requires,omitempty"`
}

// Plan is a serializable DAG of named-activity nodes. It is the durable
// execution graph PlanWorkflow runs and the AgentScript translator's
// lowering target. Being plain data, it serializes to JSON and crosses
// the Temporal boundary intact.
type Plan struct {
	// Nodes are the plan's vertices. Order is not significant;
	// execution order is derived from Requires edges.
	Nodes []PlanNode `json:"nodes"`
}

// NodeInput is what a plan activity receives: its own static Args plus
// the outputs of its upstream nodes, keyed by upstream node ID. This is
// the serializable envelope every plan activity accepts.
type NodeInput struct {
	// NodeID is the ID of the node being executed (useful for logging
	// and for activities that vary behavior by position).
	NodeID string `json:"node_id"`
	// Args are the node's static arguments from source.
	Args []string `json:"args,omitempty"`
	// Upstream maps each required upstream node ID to its string output.
	Upstream map[string]string `json:"upstream,omitempty"`
}

// === Validation ============================================================

// Validate checks the plan is well-formed: non-empty, unique node IDs,
// every Requires edge points at an existing node, every node names an
// activity, and the graph is acyclic. It does NOT check that activities
// are registered on a worker — that surfaces at dispatch time.
//
// Validation runs before submission so malformed plans (including ones a
// translator or LLM might produce) fail fast with a clear error rather
// than partway through execution.
func (p Plan) Validate() error {
	if len(p.Nodes) == 0 {
		return fmt.Errorf("plan: no nodes")
	}

	ids := make(map[PlanNodeID]struct{}, len(p.Nodes))
	for _, n := range p.Nodes {
		if n.ID == "" {
			return fmt.Errorf("plan: node with empty ID")
		}
		if _, dup := ids[n.ID]; dup {
			return fmt.Errorf("plan: duplicate node ID %q", n.ID)
		}
		if n.Activity == "" {
			return fmt.Errorf("plan: node %q has no activity", n.ID)
		}
		ids[n.ID] = struct{}{}
	}

	for _, n := range p.Nodes {
		for _, req := range n.Requires {
			if _, ok := ids[req]; !ok {
				return fmt.Errorf("plan: node %q requires unknown node %q", n.ID, req)
			}
			if req == n.ID {
				return fmt.Errorf("plan: node %q requires itself", n.ID)
			}
		}
	}

	if _, err := p.topoLayers(); err != nil {
		return err
	}
	return nil
}

// topoLayers returns node IDs grouped into topological layers: layer 0
// has all nodes with no dependencies, layer k has nodes whose
// dependencies are all satisfied by layers < k. Nodes within a layer are
// independent and may run in parallel. Within each layer IDs are sorted
// for deterministic scheduling. Returns an error if the graph has a
// cycle.
func (p Plan) topoLayers() ([][]PlanNodeID, error) {
	indegree := make(map[PlanNodeID]int, len(p.Nodes))
	dependents := make(map[PlanNodeID][]PlanNodeID, len(p.Nodes))
	for _, n := range p.Nodes {
		indegree[n.ID] = len(n.Requires)
		for _, req := range n.Requires {
			dependents[req] = append(dependents[req], n.ID)
		}
	}

	done := make(map[PlanNodeID]bool, len(p.Nodes))
	var layers [][]PlanNodeID
	remaining := len(p.Nodes)

	for remaining > 0 {
		var layer []PlanNodeID
		for id, deg := range indegree {
			if deg == 0 && !done[id] {
				layer = append(layer, id)
			}
		}
		if len(layer) == 0 {
			return nil, fmt.Errorf("plan: cycle detected (no runnable nodes but %d remain)", remaining)
		}
		sort.Slice(layer, func(a, b int) bool { return layer[a] < layer[b] })
		layers = append(layers, layer)
		for _, id := range layer {
			done[id] = true
			for _, dep := range dependents[id] {
				indegree[dep]--
			}
		}
		remaining -= len(layer)
	}
	return layers, nil
}

// node returns the PlanNode with the given ID, or false.
func (p Plan) node(id PlanNodeID) (PlanNode, bool) {
	for _, n := range p.Nodes {
		if n.ID == id {
			return n, true
		}
	}
	return PlanNode{}, false
}

// leaves returns node IDs that no other node depends on — the plan's
// terminal outputs. A well-formed pipeline has exactly one leaf; a
// fan-out without a join may have several.
func (p Plan) leaves() []PlanNodeID {
	hasDependents := make(map[PlanNodeID]bool, len(p.Nodes))
	for _, n := range p.Nodes {
		for _, req := range n.Requires {
			hasDependents[req] = true
		}
	}
	var out []PlanNodeID
	for _, n := range p.Nodes {
		if !hasDependents[n.ID] {
			out = append(out, n.ID)
		}
	}
	sort.Slice(out, func(a, b int) bool { return out[a] < out[b] })
	return out
}
