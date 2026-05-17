package worker_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/vinodhalaharvi/sibyl/auth/oauth"
	oauthworker "github.com/vinodhalaharvi/sibyl/auth/oauth/worker"
)

// TestRegister_PanicsOnMissingConfig verifies that Register's panic
// preconditions fire correctly. We can't easily verify the happy-path
// registration without a real worker.Worker (the testsuite's
// TestWorkflowEnvironment isn't a worker.Worker), so this smoke test
// covers the surface that can be tested without integration.
func TestRegister_PanicsOnMissingConfig(t *testing.T) {
	provider := makeMinimalProvider("test")
	reg := oauth.NewRegistry()
	require.NoError(t, reg.Register(provider))

	store := oauth.NewMemoryTokenStore(oauth.NoOpCipher{})
	defer store.Close()
	mapper := oauth.NewMemoryStateMapper()

	// nil worker is fine here — we never reach the registration calls
	// because the config validation panics first.
	require.Panics(t, func() {
		oauthworker.Register(nil, oauthworker.Config{
			Stores: map[string]oauth.TokenStore{"default": store},
			Mapper: mapper,
		})
	}, "should panic when Providers is nil")

	require.Panics(t, func() {
		oauthworker.Register(nil, oauthworker.Config{
			Providers: reg,
			Mapper:    mapper,
		})
	}, "should panic when Stores is nil")

	require.Panics(t, func() {
		oauthworker.Register(nil, oauthworker.Config{
			Providers: reg,
			Stores:    map[string]oauth.TokenStore{"default": store},
		})
	}, "should panic when Mapper is nil")
}

// makeMinimalProvider returns a Provider with placeholder arrows that
// satisfy Registry's "all four required" check. Useful for tests that
// don't actually exercise the arrows.
func makeMinimalProvider(name string) oauth.Provider {
	return oauth.Provider{
		Name: name,
		Authorize: func(_ context.Context, _ oauth.AuthRequest) (oauth.AuthorizeRedirect, error) {
			return oauth.AuthorizeRedirect{}, nil
		},
		Exchange: func(_ context.Context, _ oauth.ExchangeRequest) (oauth.TokenPair, error) {
			return oauth.TokenPair{}, nil
		},
		Refresh: func(_ context.Context, _ oauth.TokenPair) (oauth.TokenPair, error) {
			return oauth.TokenPair{}, nil
		},
		Whoami: func(_ context.Context, _ oauth.TokenPair) (oauth.Identity, error) {
			return oauth.Identity{}, nil
		},
	}
}
