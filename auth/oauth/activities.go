// Package oauth — activities.go implements the Temporal activities the
// OAuthFlowWorkflow dispatches to: BuildAuthorizeURL, Exchange, Whoami,
// StoreTokens, StoreStateMapping.
//
// All activities live on an Activities struct that holds the Provider
// Registry, the TokenStore registry, and the StateMapper. The struct
// is constructed once at worker startup and its methods are registered
// as activities.
package oauth

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"sync"

	"go.temporal.io/sdk/temporal"
)

// Activities bundles the implementations of the workflow's activities.
// One per worker process. Construct via NewActivities.
type Activities struct {
	// Providers holds the registered OAuth providers (one per name).
	Providers *Registry

	// Stores maps a string key to a TokenStore. Most deployments have
	// one store keyed "default". The workflow's StoreKey input picks
	// which store to use.
	Stores map[string]TokenStore

	// StateMapper holds the state-token -> workflow-id mapping used by
	// the HTTP callback handler to route incoming callbacks.
	StateMapper StateMapper

	// ClientConfigs holds per-provider config (client_id, endpoints).
	// Looked up by provider name. The provider implementations need
	// these to actually call HTTP endpoints; we keep them out of the
	// Provider struct so configuration changes don't ripple through
	// the arrow definitions.
	ClientConfigs map[string]ClientConfig
}

// ClientConfig is the per-provider HTTP-level config the activities
// use to actually call /authorize, /token, /userinfo.
type ClientConfig struct {
	// ClientID is the OAuth client ID issued by the provider.
	ClientID string

	// AuthorizeEndpoint, TokenEndpoint, UserInfoEndpoint, RevokeEndpoint
	// are the absolute URLs of each OAuth endpoint. For OIDC discovery
	// providers, fill these from the well-known config at construction
	// time.
	AuthorizeEndpoint string
	TokenEndpoint     string
	UserInfoEndpoint  string
	RevokeEndpoint    string // may be empty

	// Issuer is the OIDC issuer string (used for id_token validation
	// when applicable). May be empty for non-OIDC providers.
	Issuer string
}

// NewActivities constructs an Activities value with sensible defaults.
func NewActivities(reg *Registry, stores map[string]TokenStore, mapper StateMapper, configs map[string]ClientConfig) *Activities {
	if reg == nil {
		reg = NewRegistry()
	}
	if stores == nil {
		stores = map[string]TokenStore{}
	}
	if mapper == nil {
		mapper = NewMemoryStateMapper()
	}
	if configs == nil {
		configs = map[string]ClientConfig{}
	}
	return &Activities{
		Providers:     reg,
		Stores:        stores,
		StateMapper:   mapper,
		ClientConfigs: configs,
	}
}

// Exchange is the ExchangeActivityName activity.
func (a *Activities) Exchange(ctx context.Context, in ExchangeActivityInput) (TokenPair, error) {
	provider, err := a.Providers.Get(in.Provider)
	if err != nil {
		return TokenPair{}, temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("provider %q not registered", in.Provider),
			"ConfigurationError", nil)
	}
	return provider.Exchange(ctx, ExchangeRequest{
		Code:         in.Code,
		CodeVerifier: in.CodeVerifier,
		RedirectURI:  in.RedirectURI,
	})
}

// Whoami is the WhoamiActivityName activity.
func (a *Activities) Whoami(ctx context.Context, in WhoamiActivityInput) (Identity, error) {
	provider, err := a.Providers.Get(in.Provider)
	if err != nil {
		return Identity{}, temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("provider %q not registered", in.Provider),
			"ConfigurationError", nil)
	}
	return provider.Whoami(ctx, in.Tokens)
}

// StoreTokens is the StoreTokensActivityName activity.
func (a *Activities) StoreTokens(ctx context.Context, in StoreTokensActivityInput) error {
	store, ok := a.Stores[in.StoreKey]
	if !ok {
		return temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("token store %q not registered", in.StoreKey),
			"ConfigurationError", nil)
	}
	return store.Put(ctx, in.Identity, in.Provider, in.Tokens)
}

// StateMapper resolves OAuth state values back to the workflow ID
// that initiated the flow. The callback handler uses this to know
// which workflow to signal.
type StateMapper interface {
	// Put records a (state, workflowID) mapping.
	Put(ctx context.Context, state, workflowID string) error
	// Get returns the workflowID for a state value, or empty + ok=false.
	Get(ctx context.Context, state string) (workflowID string, ok bool)
	// Delete removes a state mapping (typically after the callback
	// fires successfully).
	Delete(ctx context.Context, state string) error
}

// MemoryStateMapper is the in-process StateMapper for single-worker
// deployments. Lost on restart.
type MemoryStateMapper struct {
	mu    sync.RWMutex
	table map[string]string
}

// NewMemoryStateMapper creates an empty MemoryStateMapper.
func NewMemoryStateMapper() *MemoryStateMapper {
	return &MemoryStateMapper{table: make(map[string]string)}
}

// Put implements StateMapper.
func (m *MemoryStateMapper) Put(_ context.Context, state, workflowID string) error {
	if state == "" {
		return errors.New("oauth: state must not be empty")
	}
	m.mu.Lock()
	m.table[state] = workflowID
	m.mu.Unlock()
	return nil
}

// Get implements StateMapper.
func (m *MemoryStateMapper) Get(_ context.Context, state string) (string, bool) {
	m.mu.RLock()
	v, ok := m.table[state]
	m.mu.RUnlock()
	return v, ok
}

// Delete implements StateMapper.
func (m *MemoryStateMapper) Delete(_ context.Context, state string) error {
	m.mu.Lock()
	delete(m.table, state)
	m.mu.Unlock()
	return nil
}

// SnapshotForTests returns a copy of the state -> workflow-id map.
// Exposed for tests that need to find the generated state value to
// signal back the right workflow. Not part of the StateMapper interface.
func (m *MemoryStateMapper) SnapshotForTests() map[string]string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make(map[string]string, len(m.table))
	for k, v := range m.table {
		out[k] = v
	}
	return out
}

// ParseCallback parses an HTTP request's query string into an AuthCode.
// Helper for HTTP callback handlers — keep it in the package so all
// consumers extract callbacks the same way.
func ParseCallback(rawQuery string) (AuthCode, error) {
	v, err := url.ParseQuery(rawQuery)
	if err != nil {
		return AuthCode{}, fmt.Errorf("oauth: parse callback query: %w", err)
	}
	if errMsg := v.Get("error"); errMsg != "" {
		desc := v.Get("error_description")
		return AuthCode{}, fmt.Errorf("oauth: provider returned error %q: %s", errMsg, desc)
	}
	code := v.Get("code")
	state := v.Get("state")
	if code == "" || state == "" {
		return AuthCode{}, errors.New("oauth: callback missing code or state")
	}
	return AuthCode{Code: code, State: state}, nil
}
