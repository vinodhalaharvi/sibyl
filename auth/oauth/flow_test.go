package oauth_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/vinodhalaharvi/weft/weft"

	"github.com/vinodhalaharvi/sibyl/auth/oauth"
)

// --- Flow ----------------------------------------------------------------

func TestFlow_HappyPath(t *testing.T) {
	store := oauth.NewMemoryTokenStore(oauth.NoOpCipher{})
	defer store.Close()
	provider := makeFakeProvider("test")
	flow := oauth.NewFlow(provider, store)

	session, err := flow(context.Background(), oauth.FlowInput{
		Code:         oauth.AuthCode{Code: "code-abc", State: "state-xyz"},
		CodeVerifier: "verifier",
		RedirectURI:  "http://localhost/cb",
	})
	require.NoError(t, err)
	require.Equal(t, "fake-code-abc", session.Token.AccessToken)
	require.Equal(t, "test:test-user", session.Identity.Canonical)
	require.False(t, session.IssuedAt.IsZero())

	// Tokens persisted in the store.
	stored, err := store.Get(context.Background(), session.Identity, "test")
	require.NoError(t, err)
	require.Equal(t, session.Token.AccessToken, stored.AccessToken)
}

func TestFlow_ExchangeFailureSurfaces(t *testing.T) {
	store := oauth.NewMemoryTokenStore(oauth.NoOpCipher{})
	defer store.Close()
	provider := makeFakeProvider("test")
	// Override exchange to return an error.
	provider.Exchange = func(_ context.Context, _ oauth.ExchangeRequest) (oauth.TokenPair, error) {
		return oauth.TokenPair{}, errors.New("provider down")
	}
	flow := oauth.NewFlow(provider, store)

	_, err := flow(context.Background(), oauth.FlowInput{
		Code:         oauth.AuthCode{Code: "any", State: "any"},
		CodeVerifier: "v",
		RedirectURI:  "u",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "provider down")
}

func TestFlow_WhoamiFailureSurfaces(t *testing.T) {
	store := oauth.NewMemoryTokenStore(oauth.NoOpCipher{})
	defer store.Close()
	provider := makeFakeProvider("test")
	provider.Whoami = func(_ context.Context, _ oauth.TokenPair) (oauth.Identity, error) {
		return oauth.Identity{}, errors.New("userinfo 500")
	}
	flow := oauth.NewFlow(provider, store)
	_, err := flow(context.Background(), oauth.FlowInput{
		Code: oauth.AuthCode{Code: "c", State: "s"},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "userinfo 500")
}

func TestFlow_NilStoreSkipsPersistence(t *testing.T) {
	// If the caller passes nil for the store, the flow still completes
	// but obviously doesn't persist. Useful for testing the flow itself
	// without store side-effects.
	provider := makeFakeProvider("test")
	flow := oauth.NewFlow(provider, nil)
	session, err := flow(context.Background(), oauth.FlowInput{
		Code: oauth.AuthCode{Code: "c", State: "s"},
	})
	require.NoError(t, err)
	require.NotEmpty(t, session.Token.AccessToken)
}

// --- WithRefresh ---------------------------------------------------------

func TestWithRefresh_PassesThroughOnSuccess(t *testing.T) {
	store := oauth.NewMemoryTokenStore(oauth.NoOpCipher{})
	defer store.Close()
	id := sampleIdentity()
	require.NoError(t, store.Put(context.Background(), id, "okta", sampleTokens()))

	var callCount atomic.Int32
	innerCall := weft.Arrow[string, string](func(_ context.Context, in string) (string, error) {
		callCount.Add(1)
		return "got: " + in, nil
	})
	refresh := makeFakeProvider("okta").Refresh

	wrapped := oauth.WithRefresh(innerCall, refresh, store, id, "okta")
	out, err := wrapped(context.Background(), "hello")
	require.NoError(t, err)
	require.Equal(t, "got: hello", out)
	require.EqualValues(t, 1, callCount.Load(), "call should run once on success")
}

func TestWithRefresh_RetriesOnAuthenticationError(t *testing.T) {
	store := oauth.NewMemoryTokenStore(oauth.NoOpCipher{})
	defer store.Close()
	id := sampleIdentity()
	require.NoError(t, store.Put(context.Background(), id, "okta", oauth.TokenPair{
		AccessToken: "old", RefreshToken: "rt", ExpiresAt: time.Now().Add(time.Hour),
	}))

	var attempts atomic.Int32
	innerCall := weft.Arrow[string, string](func(_ context.Context, in string) (string, error) {
		n := attempts.Add(1)
		if n == 1 {
			return "", oauth.AuthenticationError{Status: 401}
		}
		return "retry-success: " + in, nil
	})
	refresh := func(_ context.Context, _ oauth.TokenPair) (oauth.TokenPair, error) {
		return oauth.TokenPair{AccessToken: "new", RefreshToken: "rt2", ExpiresAt: time.Now().Add(time.Hour)}, nil
	}

	wrapped := oauth.WithRefresh(innerCall, refresh, store, id, "okta")
	out, err := wrapped(context.Background(), "hi")
	require.NoError(t, err)
	require.Equal(t, "retry-success: hi", out)
	require.EqualValues(t, 2, attempts.Load(), "should call once, then retry after refresh")

	// New token in store.
	got, _ := store.Get(context.Background(), id, "okta")
	require.Equal(t, "new", got.AccessToken)
}

func TestWithRefresh_NonAuthErrorDoesNotRetry(t *testing.T) {
	store := oauth.NewMemoryTokenStore(oauth.NoOpCipher{})
	defer store.Close()
	id := sampleIdentity()
	require.NoError(t, store.Put(context.Background(), id, "okta", sampleTokens()))

	var attempts atomic.Int32
	innerCall := weft.Arrow[string, string](func(_ context.Context, _ string) (string, error) {
		attempts.Add(1)
		return "", errors.New("500 internal")
	})
	refresh := makeFakeProvider("okta").Refresh

	wrapped := oauth.WithRefresh(innerCall, refresh, store, id, "okta")
	_, err := wrapped(context.Background(), "hi")
	require.Error(t, err)
	require.Contains(t, err.Error(), "500 internal")
	require.EqualValues(t, 1, attempts.Load(), "non-auth errors should not trigger refresh+retry")
}

func TestWithRefresh_RefreshFailurePropagates(t *testing.T) {
	store := oauth.NewMemoryTokenStore(oauth.NoOpCipher{})
	defer store.Close()
	id := sampleIdentity()
	require.NoError(t, store.Put(context.Background(), id, "okta", sampleTokens()))

	innerCall := weft.Arrow[string, string](func(_ context.Context, _ string) (string, error) {
		return "", oauth.AuthenticationError{Status: 401}
	})
	refresh := func(_ context.Context, _ oauth.TokenPair) (oauth.TokenPair, error) {
		return oauth.TokenPair{}, errors.New("refresh endpoint down")
	}

	wrapped := oauth.WithRefresh(innerCall, refresh, store, id, "okta")
	_, err := wrapped(context.Background(), "hi")
	require.Error(t, err)
	require.Contains(t, err.Error(), "refresh endpoint down")
}

func TestWithRefresh_NilDependenciesPanic(t *testing.T) {
	require.Panics(t, func() {
		oauth.WithRefresh[string, string](nil, makeFakeProvider("okta").Refresh, oauth.NewMemoryTokenStore(oauth.NoOpCipher{}), sampleIdentity(), "okta")
	})
}

// --- AuthenticationError -------------------------------------------------

func TestAuthenticationError_ErrorsAs(t *testing.T) {
	root := oauth.AuthenticationError{Status: 401, Wrapped: errors.New("bad token")}
	wrapped := errors.Join(errors.New("context: "), root)
	var ae oauth.AuthenticationError
	require.True(t, errors.As(wrapped, &ae))
	require.Equal(t, 401, ae.Status)
}
