// Package slack is the Slack adapter for the channels package.
//
// It exposes:
//
//   - New(name, client, config) (channels.Channel, error)
//     constructs a Channel{Notify, Await, ...} backed by a Slack Client.
//
//   - Client interface
//     the four operations the adapter needs from Slack (post, reactions
//     read, users lookup). Production code uses NewSlackGoClient; tests
//     swap in a fake.
//
//   - DefaultRender(msg) PostOptions
//     turns a channels.Message into a Slack Block Kit layout. Override via
//     Config.Render to customize the look.
//
// The adapter is intentionally small: it composes three closures over the
// Client interface and the Render function, and returns a channels.Channel.
// No state outside the closures.
package slack

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/vinodhalaharvi/sibyl/channels"
)

// Client is the minimum surface the adapter needs from Slack.
//
// In tests this is a fake; in production it's NewSlackGoClient backed by
// the slack-go SDK. Keeping this interface narrow keeps adapter tests
// simple and prevents the adapter from leaking SDK types upward.
type Client interface {
	// Post sends a message to the given channel ID and returns its ts.
	Post(ctx context.Context, channel string, opts PostOptions) (ts string, err error)

	// GetReactions returns the reactions on a message identified by
	// (channel, ts). Each Reaction.Users is a list of native Slack user IDs.
	GetReactions(ctx context.Context, channel, ts string) ([]Reaction, error)

	// GetUserInfo returns display info for a Slack user. Optional; used by
	// the Await path to enrich the Verdict.Actor with display name + email.
	// Adapters may return ErrNotSupported if the bot doesn't have users:read.
	GetUserInfo(ctx context.Context, userID string) (UserInfo, error)
}

// PostOptions is the rendered form of a Message ready for slack.Post.
type PostOptions struct {
	// Text is the plain-text fallback (shown in notifications). Required.
	Text string
	// Blocks is the Block Kit layout. Optional; if empty, Text is used alone.
	Blocks []Block
}

// Block is a neutral representation of a Block Kit block. The slack-go
// client implementation translates these into the SDK's types.
type Block struct {
	Kind     BlockKind
	Text     string   // for Header, Section
	Elements []string // for Context (each rendered as plain_text)
	Buttons  []Button // for Actions
}

// BlockKind identifies which Block Kit block to render.
type BlockKind int

// Supported Block Kit blocks.
const (
	BlockHeader BlockKind = iota
	BlockSection
	BlockContext
	BlockActions
)

// Button is a Block Kit button. URL-only buttons are non-interactive
// (clicking just opens the URL); buttons with ActionID need an
// Interactivity URL configured on the Slack app to be functional. The
// default render uses URL buttons for Links and a reaction-hint context
// line for Actions, avoiding the "this app is not configured to handle
// interactive responses" warning.
type Button struct {
	ActionID string // interactive buttons; leave empty for URL buttons
	Label    string
	Style    string // "primary" | "danger" | "default"
	URL      string // populated means a URL button
}

// Reaction is one emoji reaction on a posted message.
type Reaction struct {
	Name  string   // e.g. "white_check_mark", "x", "zzz"
	Users []string // native Slack user IDs who reacted
	Count int
}

// UserInfo is the subset of Slack user data the adapter consumes.
type UserInfo struct {
	ID          string
	DisplayName string
	Email       string
	RealName    string
}

// Config configures the Slack adapter's behavior.
type Config struct {
	// PollInterval is how often Await polls Slack's reactions.get API.
	// Default: 5 seconds. Production-friendly; reduce for snappier demos.
	PollInterval time.Duration

	// VerdictByReaction maps Slack reaction names (without colons) to
	// channels.Action.IDs. The default maps:
	//
	//	"white_check_mark" -> "accept"
	//	"x"                -> "reject"
	//	"zzz"              -> "snooze"
	//
	// Override this for custom action vocabularies.
	VerdictByReaction map[string]string

	// Render turns a channels.Message into a PostOptions. Defaults to
	// DefaultRender. Override for a custom Block Kit layout.
	Render func(channels.Message) PostOptions
}

func (c *Config) applyDefaults() {
	if c.PollInterval <= 0 {
		c.PollInterval = 5 * time.Second
	}
	if c.VerdictByReaction == nil {
		c.VerdictByReaction = map[string]string{
			"white_check_mark": "accept",
			"x":                "reject",
			"zzz":              "snooze",
		}
	}
	if c.Render == nil {
		c.Render = DefaultRender
	}
}

// New constructs a Slack-backed channels.Channel.
//
// name is the adapter identifier (typically "slack"); it appears in
// Receipt.Channel and Verdict.Channel for routing.
func New(name string, client Client, cfg Config) (channels.Channel, error) {
	if name == "" {
		return channels.Channel{}, fmt.Errorf("slack.New: name is required")
	}
	if client == nil {
		return channels.Channel{}, fmt.Errorf("slack.New: client is required")
	}
	cfg.applyDefaults()

	notify := func(ctx context.Context, target string, msg channels.Message) (channels.Receipt, error) {
		opts := cfg.Render(msg)
		ts, err := client.Post(ctx, target, opts)
		if err != nil {
			return channels.Receipt{}, fmt.Errorf("slack.Post: %w", err)
		}
		return channels.Receipt{
			Channel: name,
			Target:  target,
			Native:  ts,
		}, nil
	}

	await := func(ctx context.Context, receipts []channels.Receipt, opts channels.AwaitOpts) (channels.Verdict, error) {
		timeout := opts.Timeout
		if timeout <= 0 {
			timeout = 30 * time.Minute
		}
		deadline := time.Now().Add(timeout)

		allowed := allowedSet(opts.Options)

		ticker := time.NewTicker(cfg.PollInterval)
		defer ticker.Stop()

		for {
			for _, r := range receipts {
				if r.Channel != name {
					continue
				}
				rs, err := client.GetReactions(ctx, r.Target, r.Native)
				if err != nil {
					// Transient errors don't fail the wait; just retry on next tick.
					// Caller-visible failure happens only via context cancel or timeout.
					_ = err
					continue
				}
				// Find the first reaction that maps to a valid action.
				for _, reaction := range rs {
					action, ok := cfg.VerdictByReaction[reaction.Name]
					if !ok {
						continue
					}
					if allowed != nil {
						if _, ok := allowed[action]; !ok {
							continue
						}
					}
					if len(reaction.Users) == 0 {
						continue
					}
					userID := reaction.Users[0]
					actor := lookupActor(ctx, client, name, userID)
					return channels.Verdict{
						Choice:  action,
						Actor:   actor,
						Channel: name,
						At:      time.Now(),
						Raw: map[string]any{
							"reaction": reaction.Name,
							"count":    reaction.Count,
							"users":    reaction.Users,
						},
					}, nil
				}
			}

			if time.Now().After(deadline) {
				return channels.Verdict{}, channels.ErrTimeout
			}
			select {
			case <-ctx.Done():
				return channels.Verdict{}, ctx.Err()
			case <-ticker.C:
			}
		}
	}

	return channels.Channel{
		Name:   name,
		Notify: notify,
		Await:  await,
	}, nil
}

// allowedSet returns nil if no restriction; otherwise a set membership.
func allowedSet(opts []string) map[string]struct{} {
	if len(opts) == 0 {
		return nil
	}
	out := make(map[string]struct{}, len(opts))
	for _, o := range opts {
		out[o] = struct{}{}
	}
	return out
}

// lookupActor resolves a Slack user ID to a channels.Identity. Falls back
// to a passthrough identity if user info isn't available.
func lookupActor(ctx context.Context, client Client, channelName, userID string) channels.Identity {
	id := channels.Identity{
		Canonical: channelName + ":" + userID,
		Source:    "slack-passthrough",
	}
	info, err := client.GetUserInfo(ctx, userID)
	if err != nil {
		return id
	}
	id.DisplayName = info.DisplayName
	if id.DisplayName == "" {
		id.DisplayName = info.RealName
	}
	id.Email = info.Email
	id.Source = "slack-lookup"
	return id
}

// DefaultRender produces a Block Kit layout for a Message:
//
//	header  : "[SEVERITY] Title"
//	section : Body
//	context : Evidence
//	context : "React: ✅ Confirm · ❌ Reject · 💤 Defer"  (if Actions present)
//	actions : URL buttons for Links
//	context : "Workflow: <id> — Open in Temporal"
//
// Action buttons are intentionally NOT rendered as interactive Block Kit
// buttons (those require a public Interactivity URL). Reactions are the
// affordance instead — production-friendly and require no public endpoint.
func DefaultRender(m channels.Message) PostOptions {
	var blocks []Block

	if m.Title != "" {
		title := m.Title
		if m.Severity != "" {
			title = fmt.Sprintf("[%s] %s", m.Severity, m.Title)
		}
		blocks = append(blocks, Block{Kind: BlockHeader, Text: title})
	}
	if m.Body != "" {
		blocks = append(blocks, Block{Kind: BlockSection, Text: m.Body})
	}
	if len(m.Evidence) > 0 {
		var elems []string
		for _, e := range m.Evidence {
			label := e.Description
			if label == "" {
				label = e.Location
			}
			if e.Kind != "" {
				label = fmt.Sprintf("%s: %s", e.Kind, label)
			}
			elems = append(elems, label)
		}
		blocks = append(blocks, Block{Kind: BlockContext, Elements: elems})
	}
	if hint := actionReactionHint(m.Actions); hint != "" {
		blocks = append(blocks, Block{Kind: BlockContext, Elements: []string{hint}})
	}
	if len(m.Links) > 0 {
		var btns []Button
		for _, l := range m.Links {
			btns = append(btns, Button{Label: l.Label, URL: l.URL})
		}
		blocks = append(blocks, Block{Kind: BlockActions, Buttons: btns})
	}
	if m.WorkflowRef.WorkflowID != "" {
		ref := fmt.Sprintf("Workflow: `%s`", m.WorkflowRef.WorkflowID)
		if m.WorkflowRef.TraceURL != "" {
			ref += " — <" + m.WorkflowRef.TraceURL + "|Open in Temporal>"
		}
		blocks = append(blocks, Block{Kind: BlockContext, Elements: []string{ref}})
	}

	text := m.Title
	if text == "" {
		text = m.Body
	}
	return PostOptions{Blocks: blocks, Text: text}
}

// actionReactionHint produces the "React: ✅ Confirm · ❌ Reject · 💤 Defer"
// context line from a Message's Actions.
func actionReactionHint(actions []channels.Action) string {
	emojiByAction := map[string]string{
		"accept": ":white_check_mark:",
		"reject": ":x:",
		"snooze": ":zzz:",
	}
	if len(actions) == 0 {
		return ""
	}
	parts := make([]string, 0, len(actions))
	for _, a := range actions {
		label := a.Label
		if label == "" {
			label = a.ID
		}
		if emoji, ok := emojiByAction[a.ID]; ok {
			parts = append(parts, emoji+" "+label)
		} else {
			parts = append(parts, label)
		}
	}
	return "React: " + strings.Join(parts, "  ·  ")
}
