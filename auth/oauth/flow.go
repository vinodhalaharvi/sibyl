// Package oauth — flow.go provides the high-level flow composition
// and the WithRefresh combinator for keeping API calls authenticated.
//
// Most consumers of this package interact with three things:
//
//  1. The OAuthFlowWorkflow (in workflow.go) — runs the durable
//     "authenticate the user" dance.
//  2. WithRefresh — wraps API calls so they automatically refresh
//     tokens on 401 and retry.
//  3. The TokenStore — Get the current token, occasionally Put a
//     new one (handled by WithRefresh automatically).
//
// Direct arrow composition via Flow is rarely needed; it's the
// building block the workflow uses internally.
package oauth

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/vinodhalaharvi/weft/weft"
)

// Flow composes a Provider's arrows into a single arrow that takes
// AuthCode (from the callback) and produces a Session. The
// authorization step is NOT part of this composition — that's owned
// by OAuthFlowWorkflow which awaits the callback signal.
//
// In other words: Flow describes the synchronous part of the dance
// (exchange the code for tokens, identify the user, build a session).
// The asynchronous part (waiting for the user to click Allow) is
// the workflow's job.
//
// Conceptually:
//
//	Flow = Exchange >>> (Whoami &&& id) >>> assembleSession
//
// In practice we use a closure for the assembly because weft.Pipe
// doesn't have a built-in "&&&" (fanout) combinator and it's not
// worth the type gymnastics to fake one.
type Flow = weft.Arrow[FlowInput, Session]

// FlowInput is what the workflow passes to the Flow arrow after the
// callback arrives.
type FlowInput struct {
	Code         AuthCode
	CodeVerifier string
	RedirectURI  string
}

// NewFlow builds a composed Flow for the given Provider. The store
// is captured so the session-issuing step can persist the resulting
// tokens automatically — callers don't have to remember to Put.
func NewFlow(p Provider, store TokenStore) Flow {
	return func(ctx context.Context, in FlowInput) (Session, error) {
		// Step 1: exchange code for tokens.
		// FlowInput and ExchangeRequest currently have identical fields;
		// the conversion makes the layer boundary explicit without the
		// repetition of a field-by-field literal.
		tokens, err := p.Exchange(ctx, ExchangeRequest(in))
		if err != nil {
			return Session{}, fmt.Errorf("oauth: exchange: %w", err)
		}

		// Step 2: identify the user.
		identity, err := p.Whoami(ctx, tokens)
		if err != nil {
			return Session{}, fmt.Errorf("oauth: whoami: %w", err)
		}

		// Step 3: persist the token pair.
		if store != nil {
			if err := store.Put(ctx, identity, p.Name, tokens); err != nil {
				return Session{}, fmt.Errorf("oauth: store.Put: %w", err)
			}
		}

		// Step 4: assemble the session.
		return Session{
			Identity:  identity,
			Token:     tokens,
			IssuedAt:  time.Now(),
			ExpiresAt: tokens.ExpiresAt,
		}, nil
	}
}

// WithRefresh wraps a TokenPair-consuming arrow so that, on
// authentication failure (returned through the refresh signal), it
// transparently refreshes and retries.
//
// Usage pattern:
//
//	// The base call takes a TokenPair and returns whatever the API
//	// returns. The token will be passed by the wrapper.
//	listIssues := func(ctx context.Context, in ListIssuesInput) ([]Issue, error) {
//	    return githubClient.ListIssues(ctx, in.Token.AccessToken, in.Query)
//	}
//
//	wrapped := oauth.WithRefresh(
//	    listIssues, provider.Refresh, store, identity,
//	)
//
//	// Now the caller doesn't think about refresh at all:
//	issues, err := wrapped(ctx, ListIssuesInput{Query: ...})
//
// The wrapper invokes the underlying arrow once; if it returns
// AuthenticationError, the wrapper calls Refresh, persists the new
// tokens, and retries the underlying arrow exactly once.
//
// "AuthenticationError" is the sentinel produced by Provider
// implementations when a 401 is observed. Wrapping callers that
// don't return this sentinel will not benefit from refresh.
func WithRefresh[I, O any](
	call weft.Arrow[I, O],
	refresh RefreshArrow,
	store TokenStore,
	identity Identity,
	provider string,
) weft.Arrow[I, O] {
	if call == nil || refresh == nil || store == nil {
		panic("oauth: WithRefresh: nil dependency")
	}
	return func(ctx context.Context, in I) (O, error) {
		var zero O

		out, err := call(ctx, in)
		if err == nil {
			return out, nil
		}
		var authErr AuthenticationError
		if !errors.As(err, &authErr) {
			// Not an auth failure; return as-is. No retry.
			return zero, err
		}

		// Refresh: load current tokens, refresh them, store new ones.
		current, gerr := store.Get(ctx, identity, provider)
		if gerr != nil {
			return zero, fmt.Errorf("oauth: WithRefresh: load tokens: %w", gerr)
		}
		fresh, rerr := refresh(ctx, current)
		if rerr != nil {
			return zero, fmt.Errorf("oauth: WithRefresh: refresh: %w", rerr)
		}
		if perr := store.Put(ctx, identity, provider, fresh); perr != nil {
			return zero, fmt.Errorf("oauth: WithRefresh: store refreshed tokens: %w", perr)
		}

		// Retry once with the refreshed tokens.
		out, err = call(ctx, in)
		if err != nil {
			return zero, fmt.Errorf("oauth: WithRefresh: retry after refresh: %w", err)
		}
		return out, nil
	}
}

// AuthenticationError is the sentinel a Provider arrow returns when
// the underlying API replied with 401 Unauthorized (or moral equivalent).
// WithRefresh checks for this via errors.As; providers should wrap a
// concrete error with this type.
//
// Construct via: errors.As-friendly value wrapper.
type AuthenticationError struct {
	// Wrapped is the original error if useful for logging.
	Wrapped error
	// Status is the HTTP status that triggered the classification.
	// Useful for distinguishing 401 from 403 when needed.
	Status int
}

// Error implements error.
func (e AuthenticationError) Error() string {
	if e.Wrapped != nil {
		return fmt.Sprintf("oauth: authentication required (status %d): %v", e.Status, e.Wrapped)
	}
	return fmt.Sprintf("oauth: authentication required (status %d)", e.Status)
}

// Unwrap implements errors.Unwrap.
func (e AuthenticationError) Unwrap() error { return e.Wrapped }
