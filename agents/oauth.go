// Package agents — oauth.go provides the WithOAuth Behavior: per-agent
// OAuth credential resolution and injection composed entirely from
// weft arrows.
//
// # The shape
//
// A typical agent in a multi-user, multi-vendor system needs:
//
//  1. To act on behalf of a specific identity (often the workflow's
//     InvokedBy user).
//  2. A fresh, non-expired OAuth token for the vendor it's about to call.
//  3. Transparent refresh-and-retry if the token is rejected mid-call.
//
// WithOAuth bundles (1)–(3) into a Behavior so individual agents in a DAG
// opt in by listing it in their Spec.Behaviors. The agent's Run arrow
// stays vendor-aware but auth-unaware: it receives a Req with the token
// already injected by the policy's Inject arrow.
//
// # Arrow composition, not callbacks
//
// Both seams an OAuthPolicy exposes — ResolveIdentity and Inject — are
// weft.Arrow values, not method callbacks. This matches the style of
// auth/oauth/provider.go's Provider type (struct of arrows, not
// interface). Composing with the rest of the framework remains uniform:
// any weft combinator (retry, log, timeout) applies to a policy seam
// the same way it applies to anything else.
//
// # Reuse over reimplementation
//
// The load-call-refresh-retry loop is NOT reimplemented here. It lives
// in oauth.WithRefresh, and WithOAuth delegates to it for every
// invocation. We're a thin adapter that turns a typed weft pipeline
// into the (TokenCall, Identity, Provider) shape WithRefresh accepts.
package agents

import (
	"context"
	"errors"
	"fmt"

	"github.com/vinodhalaharvi/weft/weft"

	"github.com/vinodhalaharvi/sibyl/auth/oauth"
)

// InjectInput is the input to OAuthPolicy.Inject. Carries the original
// request alongside the freshly-loaded token so the injector can
// produce a Req with the token material attached in whatever shape the
// downstream call expects (bearer header on an *http.Request, struct
// field, ctx value, ...).
type InjectInput[Req any] struct {
	Req   Req
	Token oauth.TokenPair
}

// OAuthPolicy is the per-agent OAuth policy. Both ResolveIdentity and
// Inject are weft.Arrow values so they compose uniformly with the rest
// of the framework — no interface methods, no hidden seams.
type OAuthPolicy[Req any] struct {
	// Provider is the registered oauth provider name (matches Provider.Name
	// in the oauth registry, e.g. "okta", "github"). Required.
	Provider string

	// ResolveIdentity produces the canonical identity to act as for this
	// request. Default constructors (InvokedByIdentity, FixedIdentity)
	// cover the common cases. Required.
	ResolveIdentity weft.Arrow[Req, oauth.Identity]

	// Inject attaches the freshly-loaded TokenPair to the request and
	// returns the new Req value. Shape is caller's choice — bearer
	// header, struct field, ctx value. Required.
	Inject weft.Arrow[InjectInput[Req], Req]
}

// WithOAuth wraps an inner arrow with per-agent OAuth credential
// resolution.
//
// Per-invocation pipeline:
//
//	policy.ResolveIdentity >>> oauth.WithRefresh(
//	    inner-call = policy.Inject >>> inner,
//	    refresh, store, identity, policy.Provider,
//	)
//
// The retry-on-AuthenticationError logic lives entirely in
// oauth.WithRefresh; this behavior just supplies the typed pieces.
//
// Errors surface unchanged:
//
//   - ResolveIdentity may return MissingIdentityError when the default
//     resolver finds no InvokedBy in AgentContext.
//   - oauth.WithRefresh wraps oauth.ErrTokenNotFound when no token
//     exists for (identity, provider); WithOAuth converts that into
//     MissingCredentialError (hard) so callers don't have to
//     errors.Is on a low-level sentinel.
//   - Refresh failures, inject failures, and inner failures all bubble
//     up with wrapping.
func WithOAuth[Req, Resp any](
	store oauth.TokenStore,
	refresh oauth.RefreshArrow,
	policy OAuthPolicy[Req],
) Behavior[Req, Resp] {
	if store == nil {
		panic("agents.WithOAuth: store is nil")
	}
	if refresh == nil {
		panic("agents.WithOAuth: refresh is nil")
	}
	if policy.Provider == "" {
		panic("agents.WithOAuth: policy.Provider is empty")
	}
	if policy.ResolveIdentity == nil {
		panic("agents.WithOAuth: policy.ResolveIdentity is nil")
	}
	if policy.Inject == nil {
		panic("agents.WithOAuth: policy.Inject is nil")
	}

	return func(inner weft.Arrow[Req, Resp]) weft.Arrow[Req, Resp] {
		return func(ctx context.Context, req Req) (Resp, error) {
			var zero Resp

			// Step 1: resolve identity. We do this once per invocation,
			// not once per retry, because the identity is a property of
			// the caller, not of the token.
			identity, err := policy.ResolveIdentity(ctx, req)
			if err != nil {
				return zero, fmt.Errorf("agents.WithOAuth: resolve identity: %w", err)
			}

			// Step 2: build the TokenCall that oauth.WithRefresh expects.
			// The closure runs Inject + inner together. Whatever token the
			// wrapper supplies (initial load OR refreshed) is the one
			// Inject sees.
			call := func(ctx context.Context, req Req, token oauth.TokenPair) (Resp, error) {
				var zero Resp
				injected, err := policy.Inject(ctx, InjectInput[Req]{Req: req, Token: token})
				if err != nil {
					return zero, fmt.Errorf("agents.WithOAuth: inject: %w", err)
				}
				return inner(ctx, injected)
			}

			// Step 3: delegate the load-call-refresh-retry to
			// oauth.WithRefresh. We don't reimplement that logic; we
			// just supply the typed pieces.
			wrapped := oauth.WithRefresh(call, refresh, store, identity, policy.Provider)
			resp, err := wrapped(ctx, req)
			if err != nil {
				// Translate the low-level "token not found" sentinel into
				// our typed hard error so behaviors stacked above this
				// one can match on it via errors.As. WithRefresh wraps
				// the sentinel in its own message, so check via Is.
				if errors.Is(err, oauth.ErrTokenNotFound) {
					return zero, MissingCredentialError{
						Identity: identity,
						Provider: policy.Provider,
						Wrapped:  err,
					}
				}
				return zero, err
			}
			return resp, nil
		}
	}
}

// === Built-in ResolveIdentity arrows ===

// InvokedByIdentity is the default ResolveIdentity arrow: it reads
// AgentContext.InvokedBy from the context and returns it as the
// canonical identity for the given provider.
//
// If AgentContext is absent or InvokedBy is empty, the arrow returns
// MissingIdentityError. This is intentional: an agent that needs to
// act as a user must have a user; silently falling back to a service
// identity would be a confused-deputy hazard.
//
// The returned Identity has Canonical set to InvokedBy and Provider
// set to provider; ProviderID/Email/DisplayName are left blank because
// this resolver only sees the canonical address. Whoami flows that
// produced the token are the source of truth for the richer fields;
// the store will yield those when the token is looked up by canonical
// address.
func InvokedByIdentity[Req any](provider string) weft.Arrow[Req, oauth.Identity] {
	if provider == "" {
		panic("agents.InvokedByIdentity: provider is empty")
	}
	return func(ctx context.Context, _ Req) (oauth.Identity, error) {
		ac := AgentContextFrom(ctx)
		if ac.InvokedBy == "" {
			return oauth.Identity{}, MissingIdentityError{Provider: provider}
		}
		return oauth.Identity{
			Canonical: ac.InvokedBy,
			Provider:  provider,
		}, nil
	}
}

// FixedIdentity returns a ResolveIdentity arrow that always yields the
// supplied identity, regardless of context.
//
// Use this for service-account agents — agents that act as a fixed
// non-user identity (e.g. "service:slack-bot") regardless of which
// human triggered the surrounding workflow. The identity's Canonical
// is the addressing key the TokenStore was keyed on at provisioning
// time; the same identity that Whoami returned when the service
// account was authorized.
func FixedIdentity[Req any](identity oauth.Identity) weft.Arrow[Req, oauth.Identity] {
	if identity.Canonical == "" {
		panic("agents.FixedIdentity: identity.Canonical is empty")
	}
	return func(_ context.Context, _ Req) (oauth.Identity, error) {
		return identity, nil
	}
}

// === Built-in Inject arrows ===

// StructFieldInjector wraps a struct-field mutator into the
// weft.Arrow shape Inject expects.
//
// The mutator runs against a copy of the Req (Go's pass-by-value
// semantics for non-pointer types), so callers writing to a struct's
// fields don't mutate the original input. For pointer receivers, the
// pointer itself is copied; the pointee is shared, so a *Req mutator
// IS observed by the caller — use this knowledge or avoid pointer
// types when you need pristine inputs preserved.
//
// Common usage: an agent whose Req is a struct with a Token field of
// type oauth.TokenPair:
//
//	policy.Inject = agents.StructFieldInjector(
//	    func(req *MyReq, tok oauth.TokenPair) { req.Token = tok },
//	)
func StructFieldInjector[Req any](mutate func(*Req, oauth.TokenPair)) weft.Arrow[InjectInput[Req], Req] {
	if mutate == nil {
		panic("agents.StructFieldInjector: mutate is nil")
	}
	return func(_ context.Context, in InjectInput[Req]) (Req, error) {
		req := in.Req // copy
		mutate(&req, in.Token)
		return req, nil
	}
}

// === Typed errors ===

// MissingIdentityError is returned by InvokedByIdentity when
// AgentContext.InvokedBy is empty. Indicates a workflow configuration
// problem (the caller forgot to attach AgentContext) rather than an
// authentication problem.
type MissingIdentityError struct {
	Provider string
}

// Error implements error.
func (e MissingIdentityError) Error() string {
	return fmt.Sprintf("agents: no InvokedBy identity in context for provider %q", e.Provider)
}

// MissingCredentialError is returned when no token exists in the store
// for (identity, provider). Per the pilot's design call: this is a
// HARD error — the agent does not proceed, and the workflow is
// expected to surface "needs auth" to the human. Recovery via
// re-authentication is a workflow concern, not a behavior concern.
type MissingCredentialError struct {
	Identity oauth.Identity
	Provider string
	Wrapped  error
}

// Error implements error.
func (e MissingCredentialError) Error() string {
	return fmt.Sprintf(
		"agents: no credential for identity %q on provider %q (re-authentication required)",
		e.Identity.Canonical, e.Provider,
	)
}

// Unwrap implements errors.Unwrap so consumers can errors.Is against
// oauth.ErrTokenNotFound if they want the low-level sentinel.
func (e MissingCredentialError) Unwrap() error { return e.Wrapped }
