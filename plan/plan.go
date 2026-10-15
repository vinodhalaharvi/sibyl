// Package plan is a serializable execution plan, a directed acyclic graph
// whose nodes name Temporal activities, and the one workflow that runs any
// such plan.
//
// It depends on the Temporal SDK and nothing else, so a program that only
// needs durable, auditable pipelines (a build service, a release process)
// can import it without the agent package's LLM clients, Slack, metrics or
// cgo.
//
// Why a plan is data, not closures:
//
//	Temporal dispatches activities by registered name with serializable
//	arguments; it cannot ship Go closures across the workflow/activity
//	boundary, and workflow replay requires activity code to live in
//	pre-registered activities. So the durable execution graph must be
//	data: node IDs, activity names, string arguments, dependency edges,
//	and how each node is run. Workflow walks this data and dispatches the
//	named activities. A translator (AgentScript, or a build description)
//	lowers to a Plan.
//
// I/O contract: every plan activity takes an Input and returns a string.
// Outputs are threaded to dependents by node ID, as a stage's stdout
// becomes the next stage's stdin. Everything stays serializable, and small:
// Temporal keeps every input and output in the workflow's history, so a
// node that produces something large (a file, an image) stores it elsewhere
// and returns where it put it.
package plan

import (
	"fmt"
	"sort"
	"time"
)

// NodeID identifies a node within a Plan. Unique per plan.
type NodeID string

// Node is one vertex of a Plan: a named activity to run, the static
// arguments declared in source, the upstream nodes whose outputs it
// consumes, and how it is run.
type Node struct {
	// ID is the node's unique identifier within the plan.
	ID NodeID `json:"id"`
	// Activity is the registered Temporal activity name to dispatch
	// (e.g. "Echo"). Validate checks the graph shape but not
	// registration; that surfaces at dispatch time.
	Activity string `json:"activity"`
	// Args are the static arguments from source, passed to the activity
	// alongside upstream outputs.
	Args []string `json:"args,omitempty"`
	// Requires lists upstream node IDs whose outputs feed this node.
	// Empty for source nodes.
	Requires []NodeID `json:"requires,omitempty"`
	// Options override the plan's Defaults for this node.
	Options Options `json:"options,omitzero"`
}

// Options say how a node is run. A zero field means "not set here": a
// node's Options override the plan's Defaults field by field, and what
// neither sets comes from the workflow's own defaults (DefaultOptions).
type Options struct {
	// TaskQueue sends the node to the workers polling this queue, so
	// different kinds of work run on different machines (a GPU, a build
	// host, a lab bench). "": the workflow's own queue.
	TaskQueue string `json:"taskQueue,omitempty"`
	// Timeout bounds one attempt (Temporal's StartToClose timeout).
	Timeout time.Duration `json:"timeout,omitempty"`
	// Heartbeat, if set, fails an attempt that has not heartbeated for
	// this long, so a hung long-running node is retried before Timeout.
	Heartbeat time.Duration `json:"heartbeat,omitempty"`
	// MaxAttempts is how many times the node is tried, the first
	// included.
	MaxAttempts int32 `json:"maxAttempts,omitempty"`
	// NonRetryable are application error types that fail the node at
	// once: a retry cannot change the answer.
	NonRetryable []string `json:"nonRetryable,omitempty"`
}

// DefaultOptions are what a node gets when neither it nor the plan says
// otherwise.
var DefaultOptions = Options{
	Timeout:      3 * time.Minute,
	MaxAttempts:  3,
	NonRetryable: []string{"InvalidInput", "InvalidPlan", "ConfigurationError"},
}

// Over is o with every field that over sets replaced. NonRetryable is
// added to, not replaced: a type that is never worth retrying stays so.
func (o Options) Over(over Options) Options {
	if over.TaskQueue != "" {
		o.TaskQueue = over.TaskQueue
	}
	if over.Timeout != 0 {
		o.Timeout = over.Timeout
	}
	if over.Heartbeat != 0 {
		o.Heartbeat = over.Heartbeat
	}
	if over.MaxAttempts != 0 {
		o.MaxAttempts = over.MaxAttempts
	}
	if len(over.NonRetryable) > 0 {
		seen := make(map[string]bool, len(o.NonRetryable)+len(over.NonRetryable))
		var all []string
		for _, t := range append(append([]string{}, o.NonRetryable...), over.NonRetryable...) {
			if !seen[t] {
				seen[t] = true
				all = append(all, t)
			}
		}
		o.NonRetryable = all
	}
	return o
}

func (o Options) check() error {
	switch {
	case o.Timeout < 0:
		return fmt.Errorf("negative timeout %s", o.Timeout)
	case o.Heartbeat < 0:
		return fmt.Errorf("negative heartbeat %s", o.Heartbeat)
	case o.MaxAttempts < 0:
		return fmt.Errorf("negative maxAttempts %d", o.MaxAttempts)
	}
	return nil
}

// Plan is a serializable DAG of named-activity nodes: what Workflow runs.
// Being plain data, it serializes to JSON and crosses the Temporal
// boundary intact.
type Plan struct {
	// Nodes are the plan's vertices. Order is not significant;
	// execution order is derived from Requires edges.
	Nodes []Node `json:"nodes"`
	// Defaults apply to every node, over DefaultOptions and under the
	// node's own Options.
	Defaults Options `json:"defaults,omitzero"`
}

// Input is what a plan activity receives: its own static Args plus the
// outputs of its upstream nodes, keyed by upstream node ID.
type Input struct {
	// NodeID is the ID of the node being executed (useful for logging
	// and for activities that vary behavior by position).
	NodeID string `json:"node_id"`
	// Args are the node's static arguments from source.
	Args []string `json:"args,omitempty"`
	// Upstream maps each required upstream node ID to its string output.
	Upstream map[string]string `json:"upstream,omitempty"`
}

// OptionsFor is how node n of p is run: DefaultOptions, then p.Defaults,
// then n.Options.
func (p Plan) OptionsFor(n Node) Options {
	return DefaultOptions.Over(p.Defaults).Over(n.Options)
}

// === Validation ============================================================

// Validate checks the plan is well-formed: non-empty, unique node IDs,
// every Requires edge points at an existing node, every node names an
// activity, no option is negative, and the graph is acyclic. It does NOT
// check that activities are registered on a worker; that surfaces at
// dispatch time.
//
// Validation runs before submission so malformed plans (including ones a
// translator or LLM might produce) fail fast with a clear error rather
// than partway through execution.
func (p Plan) Validate() error {
	if len(p.Nodes) == 0 {
		return fmt.Errorf("plan: no nodes")
	}
	if err := p.Defaults.check(); err != nil {
		return fmt.Errorf("plan: defaults: %w", err)
	}

	ids := make(map[NodeID]struct{}, len(p.Nodes))
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
		if err := n.Options.check(); err != nil {
			return fmt.Errorf("plan: node %q: %w", n.ID, err)
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

	if _, err := p.Layers(); err != nil {
		return err
	}
	return nil
}

// Layers returns node IDs grouped into topological layers: layer 0 has
// all nodes with no dependencies, layer k has nodes whose dependencies are
// all satisfied by layers < k. Nodes within a layer are independent and
// may run in parallel. Within each layer IDs are sorted for deterministic
// scheduling. Returns an error if the graph has a cycle.
func (p Plan) Layers() ([][]NodeID, error) {
	indegree := make(map[NodeID]int, len(p.Nodes))
	dependents := make(map[NodeID][]NodeID, len(p.Nodes))
	for _, n := range p.Nodes {
		indegree[n.ID] = len(n.Requires)
		for _, req := range n.Requires {
			dependents[req] = append(dependents[req], n.ID)
		}
	}

	done := make(map[NodeID]bool, len(p.Nodes))
	var layers [][]NodeID
	remaining := len(p.Nodes)

	for remaining > 0 {
		var layer []NodeID
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

// Node returns the node with the given ID, or false.
func (p Plan) Node(id NodeID) (Node, bool) {
	for _, n := range p.Nodes {
		if n.ID == id {
			return n, true
		}
	}
	return Node{}, false
}

// Leaves returns node IDs that no other node depends on: the plan's
// terminal outputs. A well-formed pipeline has exactly one leaf; a fan-out
// without a join may have several.
func (p Plan) Leaves() []NodeID {
	hasDependents := make(map[NodeID]bool, len(p.Nodes))
	for _, n := range p.Nodes {
		for _, req := range n.Requires {
			hasDependents[req] = true
		}
	}
	var out []NodeID
	for _, n := range p.Nodes {
		if !hasDependents[n.ID] {
			out = append(out, n.ID)
		}
	}
	sort.Slice(out, func(a, b int) bool { return out[a] < out[b] })
	return out
}
