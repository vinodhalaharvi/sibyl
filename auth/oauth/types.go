// Package oauth provides an OAuth 2.0 (authorization-code-with-PKCE)
// implementation built on weft arrows and Temporal workflows.
//
// Why this package exists:
//
//	Every multi-step interaction with an external system has the same
//	shape — authenticate, exchange, use, refresh, revoke. OAuth is the
//	prototypical example. Implementing it once on Sibyl's primitives
//	(weft arrows, Temporal-durable activities, the workflow lift)
//	gives consumers a reusable pattern that handles the production
//	realities: durable wait for user interaction, automatic token
//	refresh on 401, transparent retry on transient failures, all
//	without bespoke per-provider code.
//
// Design constraints:
//
//   - PKCE-only. We do not support OAuth 2.0 flows without PKCE.
//     RFC 9700 (OAuth 2.1) makes PKCE mandatory for all clients; we
//     are aligned with that direction.
//
//   - Authorization-code grant only. Other flows (device, client
//     credentials) are out of scope for v1 and would live in sibling
//     packages.
//
//   - One identity, one provider, one active token pair per (identity,
//     provider). Multiple concurrent sessions for the same identity
//     would land tokens in the same store key; the most recent Put
//     wins.
//
//   - Token material is encrypted at rest in every store. There is
//     no "without encryption" mode. Tests use NoOpCipher and that
//     name carries its own warning.
//
// The four primary types in this package:
//
//	AuthRequest    The "I want a session" input
//	TokenPair      What an OAuth provider returns
//	Identity       Who the token belongs to (canonical form)
//	Session        The two combined — what consumers actually want
package oauth

import (
	"errors"
	"time"
)

// AuthRequest describes a new OAuth flow. The PKCE fields are
// mandatory; populate them via NewAuthRequest, which generates
// cryptographically random values for State and the PKCE pair.
type AuthRequest struct {
	// Provider identifies which registered Provider to use.
	// Must match Provider.Name (e.g. "okta", "github").
	Provider string

	// Scopes is the OAuth scope list to request. Format follows
	// the provider's conventions: "openid email profile" for OIDC,
	// "repo user:email" for GitHub, etc.
	Scopes []string

	// RedirectURI is the absolute URL the provider redirects to
	// after the user clicks "Allow". Must be registered with the
	// provider out-of-band.
	RedirectURI string

	// State is the opaque CSRF token. We generate this and verify
	// it against the value echoed back from the callback.
	State string

	// CodeVerifier is the PKCE secret (43-128 chars). Never sent
	// to the provider during the authorize step; sent during the
	// exchange step to prove possession.
	CodeVerifier string

	// CodeChallenge is base64url(sha256(CodeVerifier)). Sent during
	// the authorize step. We use S256 challenge method always (the
	// "plain" method is forbidden by RFC 9700).
	CodeChallenge string
}

// AuthorizeRedirect is what the AuthorizeArrow produces. Build it
// at workflow-start time; persist the State and CodeVerifier in
// workflow state until the callback fires; emit URL to the UI so
// the user can be redirected.
type AuthorizeRedirect struct {
	// URL is the provider's /authorize endpoint with all required
	// query params: client_id, redirect_uri, response_type=code,
	// state, scope, code_challenge, code_challenge_method=S256.
	URL string

	// State is the CSRF token, echoed for the caller's convenience.
	State string

	// CodeVerifier is what the caller must keep until exchange
	// (or what the workflow keeps in its state).
	CodeVerifier string
}

// AuthCode is the short-lived authorization code returned to the
// redirect URI after the user clicks "Allow". It is one-shot;
// exchange it immediately.
type AuthCode struct {
	// Code is the value of the "code" query param from the redirect.
	Code string

	// State is the value of the "state" query param. The caller MUST
	// verify this matches AuthRequest.State before proceeding —
	// mismatched state is a CSRF attack signal.
	State string
}

// TokenPair is what /token returns and what gets stored, refreshed,
// and consumed downstream.
type TokenPair struct {
	// AccessToken is the bearer token used in API calls.
	AccessToken string

	// RefreshToken is used to obtain a new TokenPair when AccessToken
	// expires. May be empty for providers that don't issue refresh
	// tokens (some scopes, some configurations).
	RefreshToken string

	// TokenType is usually "Bearer" but we preserve whatever the
	// provider returned in case some custom type appears.
	TokenType string

	// ExpiresAt is the absolute expiration time, computed at receipt
	// from the provider's expires_in (seconds). Using absolute time
	// here means downstream "is this expired" checks don't depend on
	// "when did we receive this token" bookkeeping.
	ExpiresAt time.Time

	// Scope is the granted scope set (may be smaller than requested).
	// Space-separated per RFC 6749; we keep it as the raw string and
	// let callers parse if needed.
	Scope string

	// Raw is the unparsed provider response, preserved verbatim.
	// OIDC providers include id_token here; some providers add
	// vendor-specific fields. Consumers that need these reach into
	// Raw with full provider knowledge.
	Raw map[string]any
}

// Identity is the "who is this person" answer. Canonical IDs follow
// the format "<provider>:<provider-specific-id>" — e.g. "okta:00u123",
// "github:42", "google:108429..." — which is the framework-wide
// addressing scheme that future IdentityResolver implementations will
// map across platforms.
type Identity struct {
	// Canonical is the universal address.
	Canonical string

	// Provider is "okta", "github", etc.
	Provider string

	// ProviderID is the raw ID inside the provider (e.g. "00u123abc").
	ProviderID string

	// Email is the user's email if the provider returned one. May
	// be empty for providers that don't issue email scope by default.
	Email string

	// DisplayName is a human-friendly name. Best-effort; varies wildly
	// by provider in quality.
	DisplayName string

	// Raw is the unparsed userinfo response. Custom claims (Okta
	// groups, GitHub orgs, Google org_unit) live here.
	Raw map[string]any
}

// Session combines an identity with the token pair used to act on
// their behalf. It's a snapshot — consumers should call TokenStore.Get
// for the current token state rather than relying on the Session value
// across multiple API calls.
type Session struct {
	Identity  Identity
	Token     TokenPair
	IssuedAt  time.Time
	ExpiresAt time.Time
}

// Errors --------------------------------------------------------------

// ErrTokenNotFound is returned by TokenStore.Get when no token exists
// for the given (identity, provider). Use errors.Is to test.
var ErrTokenNotFound = errors.New("oauth: token not found")

// ErrStateMismatch is returned when an AuthCode's State doesn't match
// the originating AuthRequest.State. Indicates either a stale callback
// or a CSRF attempt.
var ErrStateMismatch = errors.New("oauth: state mismatch (csrf protection)")

// ErrProviderNotRegistered is returned when an arrow refers to a
// provider name that has no corresponding registered Provider.
var ErrProviderNotRegistered = errors.New("oauth: provider not registered")

// ErrTokenExpired is returned by helpers that explicitly check expiry.
// Consumers usually use Token.ExpiresAt directly; this sentinel is
// available for WithRefresh-style middleware that wants to test
// expiry imperatively.
var ErrTokenExpired = errors.New("oauth: token expired")

// IsExpired reports whether the token pair has passed its ExpiresAt.
// Tokens with ExpiresAt set to the zero value are treated as never-
// expiring (some providers issue these explicitly; they're rare and
// usually a misconfiguration but we shouldn't crash on them).
func (t TokenPair) IsExpired() bool {
	if t.ExpiresAt.IsZero() {
		return false
	}
	return time.Now().After(t.ExpiresAt)
}

// IsExpiringWithin reports whether the token will expire within d.
// Useful for proactive refresh before a long-running operation.
func (t TokenPair) IsExpiringWithin(d time.Duration) bool {
	if t.ExpiresAt.IsZero() {
		return false
	}
	return time.Now().Add(d).After(t.ExpiresAt)
}
