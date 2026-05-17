// Package oauth — provider.go defines the Provider type: a bundle of
// weft.Arrow values that describe one OAuth provider's behavior.
//
// Why a struct of arrows instead of an interface?
//
//	With an interface, the contract is "implement these methods."
//	With a struct of arrows, the contract is "supply these values" —
//	and arrows compose. You can wrap any single arrow with weft
//	combinators (retry, log, timeout) without re-implementing the
//	provider. Middleware applies surgically.
//
// Concrete providers (Okta, GitHub, Google) live in subpackages of
// providers/. Each exports a NewProvider() function that returns a
// Provider value with all arrows pre-bound to the provider's HTTP
// endpoints and config.
package oauth

import (
	"context"

	"github.com/vinodhalaharvi/weft/weft"
)

// AuthorizeArrow produces the URL the user should be redirected to,
// plus the State and CodeVerifier the caller must persist until the
// callback fires.
//
// This arrow does NOT wait for the callback. It is purely a URL builder.
// The actual "wait for user to click Allow" step is owned by
// OAuthFlowWorkflow via a Temporal signal channel.
type AuthorizeArrow = weft.Arrow[AuthRequest, AuthorizeRedirect]

// ExchangeArrow swaps a freshly-issued AuthCode for a TokenPair.
// The ExchangeRequest carries the code plus the original CodeVerifier
// and RedirectURI (which the provider re-validates).
type ExchangeArrow = weft.Arrow[ExchangeRequest, TokenPair]

// RefreshArrow swaps an old TokenPair for a new one using its
// RefreshToken. If the input TokenPair has no RefreshToken, the
// implementation should return an error rather than silently
// re-issuing a guest token.
type RefreshArrow = weft.Arrow[TokenPair, TokenPair]

// WhoamiArrow returns canonical identity for a TokenPair. For OIDC
// providers, the implementation may inspect the id_token in
// TokenPair.Raw and avoid a separate userinfo HTTP call. For non-OIDC
// providers, the implementation calls the provider's userinfo endpoint.
type WhoamiArrow = weft.Arrow[TokenPair, Identity]

// RevokeArrow invalidates a TokenPair server-side. Returns struct{}
// because there's nothing meaningful in the response — the result is
// the side-effect.
//
// Providers that don't implement revocation should leave this field
// nil in their Provider value. Consumers must check before calling
// (helper: ProviderHasRevoke).
type RevokeArrow = weft.Arrow[TokenPair, struct{}]

// ExchangeRequest is the input to ExchangeArrow.
type ExchangeRequest struct {
	// Code is the value just returned by the provider's redirect.
	Code AuthCode

	// CodeVerifier is the PKCE verifier from the originating
	// AuthRequest. The provider validates this matches the
	// challenge sent during authorize.
	CodeVerifier string

	// RedirectURI is the same URI passed during authorize. The
	// provider re-validates this for the security property that
	// the same client is on both ends of the flow.
	RedirectURI string
}

// Provider bundles the arrows that describe one OAuth provider's
// behavior. Provider values are constructed by per-provider packages
// and registered with the OAuth worker at startup.
//
// Fields are values, not interface methods, so each can be wrapped
// independently with weft middleware. For example:
//
//	p := okta.NewProvider(cfg)
//	p.Exchange = weft.WithRetry(p.Exchange, 3, time.Second)  // wrap one arrow
//
// The Provider value is held in the worker's registry; arrows are
// invoked through that registry rather than directly by consumers.
type Provider struct {
	// Name uniquely identifies this provider; must match the
	// AuthRequest.Provider value passed by clients.
	Name string

	Authorize AuthorizeArrow
	Exchange  ExchangeArrow
	Refresh   RefreshArrow
	Whoami    WhoamiArrow

	// Revoke is optional. nil if the provider does not support
	// token revocation (RFC 7009). Callers check via ProviderHasRevoke.
	Revoke RevokeArrow
}

// ProviderHasRevoke reports whether p has a non-nil Revoke arrow.
// Helpful for "best-effort revoke on logout" code that should
// silently skip when the provider doesn't support it.
func ProviderHasRevoke(p Provider) bool {
	return p.Revoke != nil
}

// Registry holds the Providers a worker knows about. Lookups are
// safe for concurrent use; registration is typically done at startup
// before activities begin.
type Registry struct {
	providers map[string]Provider
}

// NewRegistry creates an empty Registry.
func NewRegistry() *Registry {
	return &Registry{providers: make(map[string]Provider)}
}

// Register adds a Provider to the registry. Returns an error if a
// Provider with the same Name is already registered.
func (r *Registry) Register(p Provider) error {
	if p.Name == "" {
		return ErrProviderNotRegistered
	}
	if _, exists := r.providers[p.Name]; exists {
		return ErrProviderNotRegistered
	}
	if p.Authorize == nil || p.Exchange == nil || p.Refresh == nil || p.Whoami == nil {
		return ErrProviderNotRegistered
	}
	r.providers[p.Name] = p
	return nil
}

// MustRegister panics on error. For startup-time registration.
func (r *Registry) MustRegister(p Provider) {
	if err := r.Register(p); err != nil {
		panic(err)
	}
}

// Get returns the provider with the given name, or ErrProviderNotRegistered.
func (r *Registry) Get(name string) (Provider, error) {
	p, ok := r.providers[name]
	if !ok {
		return Provider{}, ErrProviderNotRegistered
	}
	return p, nil
}

// Names returns the registered provider names. Useful for diagnostics.
func (r *Registry) Names() []string {
	names := make([]string, 0, len(r.providers))
	for name := range r.providers {
		names = append(names, name)
	}
	return names
}

// Use is a tiny composition helper that returns an arrow over the
// registry: given a provider name and an arrow factory, look up the
// provider and invoke the factory.
//
// Most callers won't need this — Flow and WithRefresh handle the
// common cases. It exists for callers building bespoke compositions
// over the registry.
func Use[I, O any](r *Registry, providerName string, choose func(Provider) weft.Arrow[I, O]) weft.Arrow[I, O] {
	return func(ctx context.Context, in I) (O, error) {
		var zero O
		p, err := r.Get(providerName)
		if err != nil {
			return zero, err
		}
		arrow := choose(p)
		if arrow == nil {
			return zero, ErrProviderNotRegistered
		}
		return arrow(ctx, in)
	}
}
