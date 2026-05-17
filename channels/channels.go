package channels

import (
	"context"
	"fmt"
)

// Dispatcher routes Messages to one or more Channels and aggregates the
// results. Construct one with New(...); thread it through your workflow as
// the single point of human-facing communication.
//
// Dispatcher is safe for concurrent use after construction.
type Dispatcher struct {
	channels map[string]Channel
	router   Router
	resolver IdentityResolver
}

// Option configures a Dispatcher.
type Option func(*Dispatcher) error

// WithChannel registers a Channel under its declared Name. Multiple calls
// add multiple channels. Channels with duplicate names are rejected.
func WithChannel(c Channel) Option {
	return func(d *Dispatcher) error {
		if c.Name == "" {
			return fmt.Errorf("channels.WithChannel: Channel.Name is required")
		}
		if _, exists := d.channels[c.Name]; exists {
			return fmt.Errorf("channels.WithChannel: duplicate channel name %q", c.Name)
		}
		d.channels[c.Name] = c
		return nil
	}
}

// WithRouter installs a routing strategy. If not set, the Dispatcher
// errors on Notify (no default — explicit configuration is safer).
func WithRouter(r Router) Option {
	return func(d *Dispatcher) error {
		if r == nil {
			return fmt.Errorf("channels.WithRouter: router cannot be nil")
		}
		d.router = r
		return nil
	}
}

// WithIdentityResolver installs an identity-resolution strategy. If not
// set, a PassthroughResolver is used (raw channel:nativeID).
func WithIdentityResolver(r IdentityResolver) Option {
	return func(d *Dispatcher) error {
		d.resolver = r
		return nil
	}
}

// New constructs a Dispatcher from the given options.
func New(opts ...Option) (*Dispatcher, error) {
	d := &Dispatcher{
		channels: make(map[string]Channel),
		resolver: PassthroughResolver,
	}
	for _, opt := range opts {
		if err := opt(d); err != nil {
			return nil, err
		}
	}
	if len(d.channels) == 0 {
		return nil, fmt.Errorf("channels.New: at least one channel must be registered")
	}
	if d.router == nil {
		return nil, fmt.Errorf("channels.New: a router must be set with WithRouter(...)")
	}
	return d, nil
}

// Notify dispatches a Message to all channels chosen by the router.
//
// Returns one Receipt per successful post and one error per failed post.
// A returned (nil, ErrNoTarget) means the router selected no channels;
// callers can decide whether that's an error or expected silence.
//
// Notify is NOT itself a Temporal activity; the channels/temporal
// subpackage wraps this call as one so the receipts are recorded in
// workflow history.
func (d *Dispatcher) Notify(ctx context.Context, msg Message) (PostResult, error) {
	targets, err := d.router(ctx, d.channelNames(), msg)
	if err != nil {
		return PostResult{}, fmt.Errorf("router: %w", err)
	}
	if len(targets) == 0 {
		return PostResult{}, ErrNoTarget
	}

	var receipts []Receipt
	var errs []string
	for _, t := range targets {
		ch, ok := d.channels[t.Channel]
		if !ok {
			errs = append(errs, fmt.Sprintf("%s: unknown channel", t.Channel))
			continue
		}
		if ch.Notify == nil {
			errs = append(errs, fmt.Sprintf("%s: %v", t.Channel, ErrNotSupported))
			continue
		}
		r, err := ch.Notify(ctx, t.Target, msg)
		if err != nil {
			errs = append(errs, fmt.Sprintf("%s: %v", t.Channel, err))
			continue
		}
		receipts = append(receipts, r)
	}
	return PostResult{Receipts: receipts, Errors: errs}, nil
}

// Await waits for a verdict across the given Receipts. The first valid
// Verdict wins; remaining waits are cancelled via context.
//
// Each receipt's Channel determines which adapter's Await closure runs.
// If multiple receipts target the same channel, the adapter receives them
// as a batch and decides how to multiplex.
func (d *Dispatcher) Await(ctx context.Context, receipts []Receipt, opts AwaitOpts) (Verdict, error) {
	if len(receipts) == 0 {
		return Verdict{}, fmt.Errorf("channels.Await: no receipts provided")
	}

	// Group receipts by channel name.
	groups := make(map[string][]Receipt)
	for _, r := range receipts {
		groups[r.Channel] = append(groups[r.Channel], r)
	}

	// Single-channel fast path — most common case.
	if len(groups) == 1 {
		for name, group := range groups {
			ch, ok := d.channels[name]
			if !ok {
				return Verdict{}, fmt.Errorf("channels.Await: unknown channel %q", name)
			}
			if ch.Await == nil {
				return Verdict{}, fmt.Errorf("channels.Await: channel %q: %w", name, ErrNotSupported)
			}
			v, err := ch.Await(ctx, group, opts)
			if err != nil {
				return Verdict{}, err
			}
			return d.normalize(v), nil
		}
	}

	// Multi-channel: race the adapters via goroutines. First valid verdict
	// wins; ctx cancellation propagates to losers.
	type result struct {
		v   Verdict
		err error
	}
	out := make(chan result, len(groups))
	raceCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	for name, group := range groups {
		ch, ok := d.channels[name]
		if !ok {
			return Verdict{}, fmt.Errorf("channels.Await: unknown channel %q", name)
		}
		if ch.Await == nil {
			continue // skip channels that can't wait
		}
		go func(name string, group []Receipt) {
			v, err := ch.Await(raceCtx, group, opts)
			out <- result{v: v, err: err}
		}(name, group)
	}

	// Wait for first non-timeout result.
	timeouts := 0
	expected := 0
	for _, group := range groups {
		if d.channels[group[0].Channel].Await != nil {
			expected++
		}
	}

	for i := 0; i < expected; i++ {
		select {
		case <-ctx.Done():
			return Verdict{}, ctx.Err()
		case r := <-out:
			if r.err == nil && r.v.Choice != "" {
				return d.normalize(r.v), nil
			}
			if r.err == ErrTimeout {
				timeouts++
			}
		}
	}
	if timeouts == expected {
		return Verdict{}, ErrTimeout
	}
	return Verdict{}, fmt.Errorf("channels.Await: all waiters failed without verdict")
}

// normalize resolves the verdict's Actor through the IdentityResolver.
func (d *Dispatcher) normalize(v Verdict) Verdict {
	if v.Actor.Canonical != "" {
		// Already normalized by the adapter; trust it.
		return v
	}
	resolved, err := d.resolver(context.Background(), v.Channel, v.Actor.Canonical, v.Actor.Raw)
	if err == nil {
		v.Actor = resolved
	}
	return v
}

// channelNames returns a stable list of registered channel names for the router.
func (d *Dispatcher) channelNames() []string {
	names := make([]string, 0, len(d.channels))
	for n := range d.channels {
		names = append(names, n)
	}
	return names
}

// PostResult is the aggregate return from Notify.
type PostResult struct {
	Receipts []Receipt // one per successful post; may be empty
	Errors   []string  // one per failed channel; may be empty
}

// Target identifies a channel-and-target pair the router selects.
type Target struct {
	Channel string // matches Channel.Name
	Target  string // channel-native target, e.g. "C0AFZ2T51LG" for Slack
}

// Router decides which (Channel, Target) pairs a Message should be sent to.
//
// Routers are pure functions over (registered channel names, message).
// They never call channels themselves — they just return routing decisions.
// Multiple targets means fan-out; empty slice means "drop this message."
type Router func(ctx context.Context, channels []string, msg Message) ([]Target, error)

// FixedRouter always routes every message to a single (channel, target).
// Useful for the simplest case: "send everything to this Slack channel."
func FixedRouter(channel, target string) Router {
	return func(_ context.Context, _ []string, _ Message) ([]Target, error) {
		return []Target{{Channel: channel, Target: target}}, nil
	}
}

// SeverityRouter routes by Message.Severity. Map from severity → target;
// messages with severities not in the map are dropped (route to nothing).
//
// Used when different urgency levels go to different channels — e.g. CRITICAL
// to a paging channel, HIGH to a general security channel, lower severities
// suppressed.
func SeverityRouter(channel string, byLevel map[Severity]string) Router {
	return func(_ context.Context, _ []string, msg Message) ([]Target, error) {
		target, ok := byLevel[msg.Severity]
		if !ok {
			return nil, nil
		}
		return []Target{{Channel: channel, Target: target}}, nil
	}
}

// MultiRouter composes routers — every selection from every sub-router
// flows through. Use for "post to Slack AND email" patterns.
func MultiRouter(rs ...Router) Router {
	return func(ctx context.Context, channels []string, msg Message) ([]Target, error) {
		var out []Target
		for _, r := range rs {
			ts, err := r(ctx, channels, msg)
			if err != nil {
				return nil, err
			}
			out = append(out, ts...)
		}
		return out, nil
	}
}

// IdentityResolver maps a channel-native user identifier to a canonical
// Identity. The default PassthroughResolver returns the raw form
// ("slack:U0AEYCJM8NP") without external lookup; production systems will
// typically swap in an Okta-backed resolver via WithIdentityResolver.
//
// raw is optional channel-specific extras (display name, email, etc.) that
// the adapter may have available without a separate lookup.
type IdentityResolver func(ctx context.Context, channel, nativeID string, raw map[string]string) (Identity, error)

// PassthroughResolver is the default — produces an Identity with the raw
// channel:nativeID as Canonical, no external lookup.
var PassthroughResolver IdentityResolver = func(_ context.Context, channel, nativeID string, raw map[string]string) (Identity, error) {
	display := ""
	email := ""
	if raw != nil {
		display = raw["display_name"]
		email = raw["email"]
	}
	return Identity{
		Canonical:   channel + ":" + nativeID,
		DisplayName: display,
		Email:       email,
		Source:      "passthrough",
		Raw:         raw,
	}, nil
}
