// Package agents provides the agent registration and behavior-composition
// primitives for Sibyl.
//
// # The shape
//
// An Agent is a typed wrapper:
//
//	Agent[Req, Resp] {
//	    ID       string         // stable identifier, used in logs/metrics/HITL
//	    Vendors  []string       // declared vendor dependencies, used by auth
//	    Run      weft.Arrow     // the actual logic
//	}
//
// You construct one by combining a raw arrow with zero or more Behaviors:
//
//	oktaScanner := agents.Register(agents.Spec[Input, Output]{
//	    ID:      "OktaScanner",
//	    Vendors: []string{"okta"},
//	    Run:     rawOktaScannerArrow,
//	    Behaviors: []agents.Behavior[Input, Output]{
//	        agents.WithHumanVerdict(agents.SeverityPolicy{Min: channels.SeverityCritical}),
//	    },
//	})
//
// At Run time the behaviors are applied in declared order, wrapping the raw
// arrow with new logic. Each Behavior is itself a `weft.Arrow` combinator,
// so the unit of composition is uniform — auth, HITL, rate-limiting, cost
// budgets, etc. all share the same shape.
//
// # Why this lives in Sibyl
//
// Behaviors are cross-cutting concerns that apply to *any* agent regardless
// of what domain it works in. Sentry's OktaScanner needs HITL on CRITICAL
// findings; a future ComplianceChecker has the same need; a SupportTriage
// agent has the same need. Centralizing the behaviors in Sibyl means each
// concern is built once, tested once, hardened once.
package agents

import (
	"context"
	"fmt"

	"github.com/vinodhalaharvi/weft/weft"
)

// Agent is a registered, composed agent ready to run.
type Agent[Req, Resp any] struct {
	// ID is a stable identifier used in logs, metrics, audit trails, and
	// HITL routing (when policies need to know which agent produced an output).
	ID string

	// Vendors lists the external systems this agent needs to talk to (e.g.
	// "okta", "slack", "github"). At credential-resolution time the runtime
	// looks up credentials for each vendor based on the workflow context.
	Vendors []string

	// Run is the composed arrow. Behaviors have already been applied; this
	// is what you actually invoke.
	Run weft.Arrow[Req, Resp]
}

// Spec describes how to construct an Agent before behaviors are applied.
type Spec[Req, Resp any] struct {
	// ID is the agent's stable identifier. Required, unique within a Registry.
	ID string

	// Vendors declares which external systems the agent talks to. Optional
	// today; future auth-as-arrow work uses this to resolve credentials.
	Vendors []string

	// Run is the raw arrow before any behaviors are applied. Required.
	Run weft.Arrow[Req, Resp]

	// Behaviors is an ordered list of combinators to apply. They wrap the
	// Run arrow in order: Behaviors[0] is innermost (closest to Run),
	// Behaviors[len-1] is outermost. Optional.
	Behaviors []Behavior[Req, Resp]
}

// Behavior is a generic arrow combinator — it takes an inner arrow and
// returns a new arrow that wraps it. Auth, HITL, rate-limiting, cost
// budgets, dry-run, tracing — all are Behaviors.
//
// Behaviors must be safe to apply at registration time; they should not
// hold per-request state outside the closure they construct.
type Behavior[Req, Resp any] func(inner weft.Arrow[Req, Resp]) weft.Arrow[Req, Resp]

// Register constructs an Agent from a Spec, applying behaviors in order.
//
// Returns an error if Spec.ID or Spec.Run are missing. Behaviors are
// applied with Behaviors[0] innermost — i.e. the *last* behavior in the
// slice is the *first* to receive a request. This matches HTTP middleware
// conventions: each behavior decides whether to call its inner.
func Register[Req, Resp any](spec Spec[Req, Resp]) (Agent[Req, Resp], error) {
	if spec.ID == "" {
		return Agent[Req, Resp]{}, fmt.Errorf("agents.Register: Spec.ID is required")
	}
	if spec.Run == nil {
		return Agent[Req, Resp]{}, fmt.Errorf("agents.Register: Spec.Run is required")
	}
	composed := spec.Run
	for _, b := range spec.Behaviors {
		if b == nil {
			return Agent[Req, Resp]{}, fmt.Errorf("agents.Register: Behavior cannot be nil")
		}
		composed = b(composed)
	}
	return Agent[Req, Resp]{
		ID:      spec.ID,
		Vendors: spec.Vendors,
		Run:     composed,
	}, nil
}

// MustRegister is Register that panics on error. For startup-time
// configuration where misregistration is a programming bug.
func MustRegister[Req, Resp any](spec Spec[Req, Resp]) Agent[Req, Resp] {
	a, err := Register(spec)
	if err != nil {
		panic(err)
	}
	return a
}

// === Context plumbing ===
//
// Some behaviors (HITL especially) need context about *who* is running the
// agent and *which workflow* it's part of. Behaviors retrieve this from
// the context via the helpers below; callers populate it before invoking
// the agent.

// agentContext carries per-run metadata that behaviors consume.
type agentContextKey struct{}

// AgentContext is the per-invocation metadata behaviors can read from.
type AgentContext struct {
	// AgentID is set by Register so behaviors can see which agent they're
	// wrapping. Useful for HITL policies that filter by agent.
	AgentID string

	// WorkflowID is the originating Temporal workflow ID. Behaviors that
	// post to channels use this to build WorkflowRef back-links.
	WorkflowID string

	// RunID is the Temporal run ID. Same purpose as WorkflowID.
	RunID string

	// InvokedBy is the canonical identity of the human (or agent) that
	// triggered this run. Used by audit-trail and authorization behaviors.
	InvokedBy string

	// Extra is free-form metadata behaviors may consume.
	Extra map[string]string
}

// WithAgentContext attaches an AgentContext to the parent context.
func WithAgentContext(parent context.Context, ac AgentContext) context.Context {
	return context.WithValue(parent, agentContextKey{}, ac)
}

// AgentContextFrom retrieves the AgentContext from ctx, or a zero value
// if none was set. Behaviors should treat the zero value gracefully —
// e.g. by skipping the behavior or using a default policy.
func AgentContextFrom(ctx context.Context) AgentContext {
	if v, ok := ctx.Value(agentContextKey{}).(AgentContext); ok {
		return v
	}
	return AgentContext{}
}
