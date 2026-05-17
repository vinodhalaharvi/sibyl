package agents

import (
	"context"
	"fmt"
	"time"

	"github.com/vinodhalaharvi/weft/weft"

	"github.com/vinodhalaharvi/sibyl/channels"
)

// VerdictPolicy decides, per agent output, whether to ask a human for a
// verdict and where to send it. Policies are themselves composable values;
// callers can build policies that look at the output, the agent context,
// or any other state available to them.
//
// Methods are called in this order during HITL execution:
//
//  1. Required(ctx, output) -> bool
//     If false, HITL is skipped entirely; output flows through.
//
//  2. Render(ctx, output) -> channels.Message
//     The output is converted to a Message the channel can post.
//
//  3. AwaitOpts(ctx, output) -> channels.AwaitOpts
//     Configures the wait — timeout, allowed verdicts.
//
//  4. OnVerdict(ctx, output, verdict) -> (Resp, error)
//     The verdict shapes the final response. Common patterns: reject ->
//     return zero-value, snooze -> return error, accept -> pass output
//     through.
type VerdictPolicy[Resp any] interface {
	// Required returns true if a human verdict should be solicited for
	// this output. Returning false makes HITL a no-op for this output.
	Required(ctx context.Context, output Resp) bool

	// Render produces the channels.Message for posting. The default
	// implementations in this package handle common cases (severity,
	// finding-shaped outputs); custom policies can produce arbitrary
	// messages.
	Render(ctx context.Context, output Resp) channels.Message

	// AwaitOpts configures the wait — timeout and allowed verdict choices.
	AwaitOpts(ctx context.Context, output Resp) channels.AwaitOpts

	// OnVerdict shapes the final response based on the verdict received
	// (or absence of one, if Verdict.Choice is empty). Implementations
	// decide what reject/snooze/timeout mean for their domain.
	OnVerdict(ctx context.Context, output Resp, verdict channels.Verdict) (Resp, error)
}

// WithHumanVerdict is a Behavior that wraps an agent's output with HITL.
//
// For each agent invocation, the wrapped arrow runs Run normally; then
// if policy.Required(output) is true, posts via dispatcher, awaits a
// verdict, and routes the result through policy.OnVerdict.
//
// If policy.Required returns false for a given output, the output flows
// through unchanged — no post, no wait, no policy.OnVerdict call.
//
// This is the per-agent HITL semantics: each agent declares its own
// policy at registration time. The workflow does not branch on
// "should we wait"; it just runs agents and the policies decide.
func WithHumanVerdict[Req, Resp any](
	dispatcher *channels.Dispatcher,
	policy VerdictPolicy[Resp],
) Behavior[Req, Resp] {
	return func(inner weft.Arrow[Req, Resp]) weft.Arrow[Req, Resp] {
		return func(ctx context.Context, req Req) (Resp, error) {
			// Run the inner arrow first to produce the candidate output.
			output, err := inner(ctx, req)
			if err != nil {
				return output, err
			}

			// Policy decides whether to engage HITL for this specific output.
			if !policy.Required(ctx, output) {
				return output, nil
			}

			// Build the message; attach workflow back-reference if available.
			msg := policy.Render(ctx, output)
			if ac := AgentContextFrom(ctx); ac.WorkflowID != "" {
				if msg.WorkflowRef.WorkflowID == "" {
					msg.WorkflowRef = channels.WorkflowRef{
						WorkflowID: ac.WorkflowID,
						RunID:      ac.RunID,
					}
				}
				if msg.Metadata == nil {
					msg.Metadata = map[string]string{}
				}
				if _, ok := msg.Metadata["agent_id"]; !ok && ac.AgentID != "" {
					msg.Metadata["agent_id"] = ac.AgentID
				}
			}

			// Post to the channel(s).
			result, err := dispatcher.Notify(ctx, msg)
			if err != nil {
				// If routing produced no targets, that's a configuration issue
				// in the workflow's setup, not the agent's fault. Surface it
				// but don't drop the output — the policy may want to handle.
				return policy.OnVerdict(ctx, output, channels.Verdict{})
			}
			if len(result.Receipts) == 0 {
				return policy.OnVerdict(ctx, output, channels.Verdict{})
			}

			// Wait for a verdict.
			opts := policy.AwaitOpts(ctx, output)
			verdict, err := dispatcher.Await(ctx, result.Receipts, opts)
			if err == channels.ErrTimeout {
				verdict = channels.Verdict{} // empty Choice signals timeout
			} else if err != nil {
				return output, fmt.Errorf("hitl await: %w", err)
			}

			// Hand the verdict back to the policy for final shaping.
			return policy.OnVerdict(ctx, output, verdict)
		}
	}
}

// === Built-in policies ===

// AlwaysAskPolicy asks for human verdict on every output.
//
// Render is configurable so callers can shape the message; the other hooks
// have sane defaults: 30-minute timeout, all three standard choices allowed,
// accept passes the output through, reject returns a zero value with no
// error, snooze returns ErrSnoozed.
type AlwaysAskPolicy[Resp any] struct {
	// RenderFn converts Resp to a channels.Message. Required.
	RenderFn func(ctx context.Context, output Resp) channels.Message

	// Timeout is the wait duration. Default: 30 minutes.
	Timeout time.Duration

	// AllowedChoices, if set, restricts which Action.IDs count. Default:
	// accept, reject, snooze.
	AllowedChoices []string

	// OnReject decides the response value when a human rejects. Default
	// returns a zero-value Resp and no error.
	OnReject func(ctx context.Context, output Resp) (Resp, error)

	// OnSnooze decides the response when a human defers. Default returns
	// the output with ErrSnoozed so callers can branch.
	OnSnooze func(ctx context.Context, output Resp) (Resp, error)

	// OnTimeout decides the response when no verdict arrived. Default
	// returns the output as-is (no error) — i.e. fail-open. Override to
	// fail-closed by returning an error.
	OnTimeout func(ctx context.Context, output Resp) (Resp, error)
}

// ErrSnoozed is the conventional error indicating a human deferred a verdict.
// Workflows can branch on errors.Is to put the finding into a retry queue.
var ErrSnoozed = fmt.Errorf("agents: verdict deferred (snooze)")

func (p AlwaysAskPolicy[Resp]) Required(_ context.Context, _ Resp) bool { return true }

func (p AlwaysAskPolicy[Resp]) Render(ctx context.Context, output Resp) channels.Message {
	if p.RenderFn == nil {
		return channels.Message{Title: "Agent output requires review"}
	}
	return p.RenderFn(ctx, output)
}

func (p AlwaysAskPolicy[Resp]) AwaitOpts(_ context.Context, _ Resp) channels.AwaitOpts {
	timeout := p.Timeout
	if timeout <= 0 {
		timeout = 30 * time.Minute
	}
	options := p.AllowedChoices
	if len(options) == 0 {
		options = []string{"accept", "reject", "snooze"}
	}
	return channels.AwaitOpts{Timeout: timeout, Options: options}
}

func (p AlwaysAskPolicy[Resp]) OnVerdict(ctx context.Context, output Resp, v channels.Verdict) (Resp, error) {
	switch v.Choice {
	case "accept":
		return output, nil
	case "reject":
		if p.OnReject != nil {
			return p.OnReject(ctx, output)
		}
		var zero Resp
		return zero, nil
	case "snooze":
		if p.OnSnooze != nil {
			return p.OnSnooze(ctx, output)
		}
		return output, ErrSnoozed
	default:
		// Empty Choice = timeout.
		if p.OnTimeout != nil {
			return p.OnTimeout(ctx, output)
		}
		return output, nil // fail-open by default
	}
}

// NeverAskPolicy makes HITL a no-op — useful when you want to compose
// many agents through the same Behavior pipeline but only some of them
// need HITL. Equivalent to omitting WithHumanVerdict entirely, but
// sometimes clearer at the registration site.
type NeverAskPolicy[Resp any] struct{}

func (NeverAskPolicy[Resp]) Required(_ context.Context, _ Resp) bool { return false }
func (NeverAskPolicy[Resp]) Render(_ context.Context, _ Resp) channels.Message {
	return channels.Message{}
}
func (NeverAskPolicy[Resp]) AwaitOpts(_ context.Context, _ Resp) channels.AwaitOpts {
	return channels.AwaitOpts{}
}
func (NeverAskPolicy[Resp]) OnVerdict(_ context.Context, output Resp, _ channels.Verdict) (Resp, error) {
	return output, nil
}

// === Composing policies ===

// PolicyPredicate is a function that decides whether to require HITL for a
// given output without depending on the rest of the policy machinery.
// Useful for building custom Required logic in a small policy.
type PolicyPredicate[Resp any] func(ctx context.Context, output Resp) bool

// WithPredicate wraps a VerdictPolicy with a custom Required predicate.
// When the predicate returns false, HITL is skipped entirely (the inner
// policy's other methods never fire). When true, the inner policy's
// methods are used.
//
// Useful for "only ask in some cases" patterns layered on top of an
// AlwaysAskPolicy:
//
//	agents.WithPredicate(
//	    agents.AlwaysAskPolicy[Finding]{ RenderFn: renderFinding },
//	    func(_ context.Context, f Finding) bool {
//	        return f.Severity == channels.SeverityCritical
//	    },
//	)
func WithPredicate[Resp any](inner VerdictPolicy[Resp], required PolicyPredicate[Resp]) VerdictPolicy[Resp] {
	return predicatePolicy[Resp]{inner: inner, required: required}
}

type predicatePolicy[Resp any] struct {
	inner    VerdictPolicy[Resp]
	required PolicyPredicate[Resp]
}

func (p predicatePolicy[Resp]) Required(ctx context.Context, output Resp) bool {
	if p.required == nil {
		return p.inner.Required(ctx, output)
	}
	return p.required(ctx, output)
}
func (p predicatePolicy[Resp]) Render(ctx context.Context, output Resp) channels.Message {
	return p.inner.Render(ctx, output)
}
func (p predicatePolicy[Resp]) AwaitOpts(ctx context.Context, output Resp) channels.AwaitOpts {
	return p.inner.AwaitOpts(ctx, output)
}
func (p predicatePolicy[Resp]) OnVerdict(ctx context.Context, output Resp, v channels.Verdict) (Resp, error) {
	return p.inner.OnVerdict(ctx, output, v)
}

// AnyOfPredicate returns a predicate that's true if any of the provided
// predicates is true. Short-circuits on first true.
func AnyOfPredicate[Resp any](preds ...PolicyPredicate[Resp]) PolicyPredicate[Resp] {
	return func(ctx context.Context, output Resp) bool {
		for _, p := range preds {
			if p != nil && p(ctx, output) {
				return true
			}
		}
		return false
	}
}

// AllOfPredicate returns a predicate that's true only if every provided
// predicate is true.
func AllOfPredicate[Resp any](preds ...PolicyPredicate[Resp]) PolicyPredicate[Resp] {
	return func(ctx context.Context, output Resp) bool {
		for _, p := range preds {
			if p == nil || !p(ctx, output) {
				return false
			}
		}
		return true
	}
}
