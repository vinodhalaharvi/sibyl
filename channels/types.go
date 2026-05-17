// Package channels provides a polymorphic, function-seam-based abstraction
// for posting messages from agents to human-facing channels (Slack, email,
// Teams, etc.) and awaiting human verdicts in response.
//
// # Design
//
// A Channel is a struct of three function fields:
//
//	Notify   — post a message; return Receipts so it can be looked up later
//	Await    — wait for a verdict on previously-posted Receipts (durable)
//	Converse — open a multi-turn conversation (replies, evidence requests)
//
// Each field may be nil; capabilities are advertised by presence. There are
// no interfaces and no type assertions — the function signatures are the
// contract. Adapters (slack, email, teams) compose Channel{Notify, Await,
// Converse} from closures over their vendor SDKs.
//
// Routing, identity resolution, and rendering are also function seams:
//
//	Router            picks which Channel and which target for a given Message
//	IdentityResolver  maps channel-native user IDs to canonical Identity values
//	Renderer          turns a Message into a channel-native render target
//
// This lets the package stay domain-agnostic. Sibyl Sentry uses it; so can
// any other Sibyl application that needs a human review loop.
//
// # Durability
//
// The channels package itself is just data + closures. Durability comes
// from running the Notify/Await calls as Temporal activities — see the
// channels/temporal subpackage for the integration helpers. An AwaitVerdict
// activity heartbeats while it polls for reactions; worker restarts mid-wait
// are recovered transparently.
package channels

import (
	"context"
	"errors"
	"time"
)

// Severity is a normalized severity level for messages.
// Adapters render it however their channel idiom prefers (color, prefix, etc.).
type Severity string

// Severity levels in increasing urgency.
const (
	SeverityInfo     Severity = "INFO"
	SeverityLow      Severity = "LOW"
	SeverityMedium   Severity = "MEDIUM"
	SeverityHigh     Severity = "HIGH"
	SeverityCritical Severity = "CRITICAL"
)

// Message is the neutral, channel-agnostic payload an agent posts.
// Adapters render this into Slack Block Kit, email HTML, Teams cards, etc.
type Message struct {
	// Subject identifies what this message is about. Subject.Kind is a
	// domain-defined string ("finding", "code-review-comment", "approval-
	// request"). Subject.ID is a stable, opaque identifier the consumer
	// chooses; the channels package treats it as a string.
	Subject Subject

	// Title is a short headline shown prominently. One line.
	Title string

	// Body is the main content. May contain markdown; adapters convert as
	// needed for their channel's text format.
	Body string

	// Severity sets the visual urgency level. Renderers map this to color,
	// emoji, channel-side prefix, etc.
	Severity Severity

	// Evidence is structured supporting data. Each item is rendered as a
	// "kind: description" line in a context block (or equivalent). Optional.
	Evidence []Evidence

	// Actions are the verdict options offered to the human. Each Action.ID
	// is the value that will be returned in Verdict.Choice if a human picks
	// that option. Adapters render Actions however their channel allows —
	// reactions, buttons, replies — driven by Renderer's choices.
	Actions []Action

	// Links are non-interactive URL references (e.g. "Open in Temporal").
	// Adapters render these as link buttons or hyperlinks.
	Links []Link

	// WorkflowRef ties this message back to the originating Temporal
	// workflow. Adapters surface this as a footer link or context line so
	// clicking a message in Slack jumps to its full execution trace.
	WorkflowRef WorkflowRef

	// Metadata is free-form provider-specific data carried alongside the
	// message. Channels package treats it as opaque.
	Metadata map[string]string
}

// Subject identifies what a message is about.
type Subject struct {
	Kind string // e.g. "finding", "code-review", "incident"
	ID   string // stable identifier within Kind
}

// Evidence is one piece of supporting data attached to a Message.
type Evidence struct {
	Kind        string // free-form, e.g. "api_field", "log_line", "screenshot"
	Description string // human-readable
	Location    string // optional pointer (URL, path, etc.)
}

// Action represents a verdict option offered to the human.
//
// The ID is the value that comes back in Verdict.Choice when a human picks
// this Action. Standard IDs the framework understands and surfaces in
// dashboards: "accept", "reject", "snooze". Adapters and consumers can
// define their own IDs; everything in channels stays string-typed.
type Action struct {
	ID    string // e.g. "accept", "reject", "snooze", "escalate"
	Label string // human-facing text shown next to the affordance
	Style string // optional hint: "primary", "danger", "default"
}

// Link is a non-interactive URL reference rendered with the message.
type Link struct {
	Label string
	URL   string
}

// WorkflowRef ties a message back to the originating Temporal workflow.
type WorkflowRef struct {
	WorkflowID string
	RunID      string
	TraceURL   string // pre-built deep link to the Temporal Web UI; optional
}

// Receipt is what a channel returns after a successful Notify.
//
// Receipts are the durable handle that AwaitVerdict uses to find the
// message it's polling. They survive across worker restarts because the
// values themselves are recorded in workflow history; on replay the
// AwaitVerdict activity is re-invoked with the same Receipts and resumes
// polling against them.
type Receipt struct {
	Channel string            // adapter name, e.g. "slack"
	Target  string            // channel-native target, e.g. "C0AFZ2T51LG"
	Native  string            // adapter-defined opaque ID; e.g. Slack ts
	Meta    map[string]string // optional vendor-specific extras
}

// Verdict is the human's response on a previously-posted Message.
type Verdict struct {
	// Choice is the Action.ID the human picked. Empty if no clear choice
	// could be resolved (e.g. timeout, malformed reaction).
	Choice string

	// Actor identifies the human who made the choice, in canonical form.
	// See IdentityResolver for how channel-native IDs get normalized.
	Actor Identity

	// Channel is the adapter that produced this verdict ("slack", "email").
	Channel string

	// At is when the verdict was recorded (server-side time when polled).
	At time.Time

	// Raw is the channel-native shape of the response (e.g. the raw
	// reactions.get JSON for Slack). Optional; useful for advanced
	// rendering or post-hoc audit.
	Raw map[string]any
}

// Identity is a canonical user identifier across systems.
//
// The Canonical field is the system-of-record form: "slack:U0AEYCJM8NP",
// "okta:00u7g8h9j2K1L", "github:42". Equality compares Canonical strings.
// Source helps debug where the identity came from (which IdentityResolver).
type Identity struct {
	Canonical   string            // namespaced, system-of-record form
	DisplayName string            // optional human-readable name
	Email       string            // optional email if known
	Source      string            // e.g. "slack-passthrough", "okta-resolver"
	Raw         map[string]string // optional additional fields
}

// AwaitOpts configures a verdict wait.
type AwaitOpts struct {
	// Timeout caps how long the await blocks. Zero means use the channel's
	// default (typically 30 minutes; see adapter docs).
	Timeout time.Duration

	// Options, if set, restricts which Action.IDs count as valid verdicts.
	// Anything outside this set is ignored. Empty means accept any choice
	// produced by the adapter's render mapping.
	Options []string
}

// Errors returned by channel primitives.
//
// All wait paths that exit without a verdict return ErrTimeout. Activity
// callers should treat ErrTimeout as a successful terminal outcome (the
// "no human responded" branch of a HITL flow), not as a failure to retry.
var (
	// ErrTimeout indicates a wait completed without a valid verdict.
	ErrTimeout = errors.New("channels: await timed out before verdict")

	// ErrNotSupported indicates a channel doesn't implement a capability
	// (e.g. calling Await on a Notify-only channel).
	ErrNotSupported = errors.New("channels: capability not supported")

	// ErrNoTarget indicates a router produced no channel/target for a
	// message (e.g. severity didn't reach any configured route).
	ErrNoTarget = errors.New("channels: no route for message")
)

// Channel is the polymorphic communication primitive.
//
// All three function fields are optional; nil means "not supported." The
// Dispatcher checks at call time and returns ErrNotSupported if the
// requested capability isn't present.
//
// Adapters construct Channel values from their internal closures. See
// channels/slack for the canonical example.
type Channel struct {
	// Name is the adapter identifier (e.g. "slack", "email", "teams").
	// Used for routing and for Receipt.Channel tagging.
	Name string

	// Notify posts a Message to the given target (channel-native, e.g.
	// "C0AFZ2T51LG" for Slack). Returns a Receipt that can later be
	// passed to Await. nil if this channel doesn't support posting.
	Notify func(ctx context.Context, target string, msg Message) (Receipt, error)

	// Await waits for a verdict on one or more previously-posted
	// Receipts. Returns the first valid Verdict produced by any of them,
	// or ErrTimeout. Implementations must be polling-friendly: they may
	// be invoked inside a Temporal activity that heartbeats periodically.
	// nil if this channel doesn't support waiting.
	Await func(ctx context.Context, receipts []Receipt, opts AwaitOpts) (Verdict, error)

	// Converse opens a multi-turn conversation thread anchored at a
	// Receipt. Reserved for future expansion; nil today on all adapters.
	Converse func(ctx context.Context, receipt Receipt) (Conversation, error)
}

// Conversation is a placeholder for the future multi-turn surface.
// Adapters may return any value implementing this interface. Defined here
// so the Channel.Converse signature has a concrete return type today.
type Conversation interface {
	// Send posts a follow-up message in the thread.
	Send(ctx context.Context, msg Message) (Receipt, error)
	// Replies returns received replies since the last call.
	Replies(ctx context.Context) ([]Reply, error)
	// Close ends the conversation (releases resources, marks as resolved).
	Close(ctx context.Context) error
}

// Reply is one inbound message in a Conversation.
type Reply struct {
	Author Identity
	Text   string
	At     time.Time
}
