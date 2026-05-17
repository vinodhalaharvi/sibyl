package oauth_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

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
	innerCall := func(_ context.Context, in string, _ oauth.TokenPair) (string, error) {
		callCount.Add(1)
		return "got: " + in, nil
	}
	refresh := makeFakeProvider("okta").Refresh

	wrapped := oauth.WithRefresh[string, string](innerCall, refresh, store, id, "okta")
	out, err := wrapped(context.Background(), "hello")
	require.NoError(t, err)
	require.Equal(t, "got: hello", out)
	require.EqualValues(t, 1, callCount.Load(), "call should run once on success")
}

// TestWithRefresh_PassesCurrentTokenFromStore verifies that the inner
// call receives the token loaded from the store, not a captured one.
// This is the core of the bug fix in Patch I.
func TestWithRefresh_PassesCurrentTokenFromStore(t *testing.T) {
	store := oauth.NewMemoryTokenStore(oauth.NoOpCipher{})
	defer store.Close()
	id := sampleIdentity()
	stored := oauth.TokenPair{AccessToken: "stored-in-store", RefreshToken: "rt"}
	require.NoError(t, store.Put(context.Background(), id, "okta", stored))

	var seenToken string
	innerCall := func(_ context.Context, _ struct{}, tok oauth.TokenPair) (string, error) {
		seenToken = tok.AccessToken
		return "ok", nil
	}
	refresh := makeFakeProvider("okta").Refresh

	wrapped := oauth.WithRefresh[struct{}, string](innerCall, refresh, store, id, "okta")
	_, err := wrapped(context.Background(), struct{}{})
	require.NoError(t, err)
	require.Equal(t, "stored-in-store", seenToken,
		"inner call must receive the token from the store, not a stale closure capture")
}

func TestWithRefresh_RetryUsesRefreshedToken(t *testing.T) {
	// This is the bug-fix regression test. After refresh, the retry
	// invocation must receive the NEW token, not the original one.
	store := oauth.NewMemoryTokenStore(oauth.NoOpCipher{})
	defer store.Close()
	id := sampleIdentity()
	require.NoError(t, store.Put(context.Background(), id, "okta", oauth.TokenPair{
		AccessToken: "old-access", RefreshToken: "rt",
	}))

	seenTokens := []string{}
	innerCall := func(_ context.Context, _ struct{}, tok oauth.TokenPair) (string, error) {
		seenTokens = append(seenTokens, tok.AccessToken)
		if len(seenTokens) == 1 {
			return "", oauth.AuthenticationError{Status: 401}
		}
		return "second-attempt-ok", nil
	}
	refresh := func(_ context.Context, _ oauth.TokenPair) (oauth.TokenPair, error) {
		return oauth.TokenPair{AccessToken: "fresh-access", RefreshToken: "rt2"}, nil
	}

	wrapped := oauth.WithRefresh[struct{}, string](innerCall, refresh, store, id, "okta")
	out, err := wrapped(context.Background(), struct{}{})
	require.NoError(t, err)
	require.Equal(t, "second-attempt-ok", out)
	require.Equal(t, []string{"old-access", "fresh-access"}, seenTokens,
		"retry must use the refreshed token, not the original")
}

func TestWithRefresh_RetriesOnAuthenticationError(t *testing.T) {
	store := oauth.NewMemoryTokenStore(oauth.NoOpCipher{})
	defer store.Close()
	id := sampleIdentity()
	require.NoError(t, store.Put(context.Background(), id, "okta", oauth.TokenPair{
		AccessToken: "old", RefreshToken: "rt", ExpiresAt: time.Now().Add(time.Hour),
	}))

	var attempts atomic.Int32
	innerCall := func(_ context.Context, in string, _ oauth.TokenPair) (string, error) {
		n := attempts.Add(1)
		if n == 1 {
			return "", oauth.AuthenticationError{Status: 401}
		}
		return "retry-success: " + in, nil
	}
	refresh := func(_ context.Context, _ oauth.TokenPair) (oauth.TokenPair, error) {
		return oauth.TokenPair{AccessToken: "new", RefreshToken: "rt2", ExpiresAt: time.Now().Add(time.Hour)}, nil
	}

	wrapped := oauth.WithRefresh[string, string](innerCall, refresh, store, id, "okta")
	out, err := wrapped(context.Background(), "hi")
	require.NoError(t, err)
	require.Equal(t, "retry-success: hi", out)
	require.EqualValues(t, 2, attempts.Load(), "should call once, then retry after refresh")

	got, _ := store.Get(context.Background(), id, "okta")
	require.Equal(t, "new", got.AccessToken)
}

func TestWithRefresh_NonAuthErrorDoesNotRetry(t *testing.T) {
	store := oauth.NewMemoryTokenStore(oauth.NoOpCipher{})
	defer store.Close()
	id := sampleIdentity()
	require.NoError(t, store.Put(context.Background(), id, "okta", sampleTokens()))

	var attempts atomic.Int32
	innerCall := func(_ context.Context, _ string, _ oauth.TokenPair) (string, error) {
		attempts.Add(1)
		return "", errors.New("500 internal")
	}
	refresh := makeFakeProvider("okta").Refresh

	wrapped := oauth.WithRefresh[string, string](innerCall, refresh, store, id, "okta")
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

	innerCall := func(_ context.Context, _ string, _ oauth.TokenPair) (string, error) {
		return "", oauth.AuthenticationError{Status: 401}
	}
	refresh := func(_ context.Context, _ oauth.TokenPair) (oauth.TokenPair, error) {
		return oauth.TokenPair{}, errors.New("refresh endpoint down")
	}

	wrapped := oauth.WithRefresh[string, string](innerCall, refresh, store, id, "okta")
	_, err := wrapped(context.Background(), "hi")
	require.Error(t, err)
	require.Contains(t, err.Error(), "refresh endpoint down")
}

func TestWithRefresh_LoadTokensFailurePropagates(t *testing.T) {
	// New behavior: WithRefresh now loads tokens at the start of every
	// invocation. A missing token should surface clearly, not as a
	// "no auth" 500 from the wrapped call.
	store := oauth.NewMemoryTokenStore(oauth.NoOpCipher{})
	defer store.Close()
	id := sampleIdentity() // no token Put — store will return ErrTokenNotFound

	innerCall := func(_ context.Context, _ string, _ oauth.TokenPair) (string, error) {
		t.Fatal("inner call should not run when store load fails")
		return "", nil
	}
	refresh := makeFakeProvider("okta").Refresh

	wrapped := oauth.WithRefresh[string, string](innerCall, refresh, store, id, "okta")
	_, err := wrapped(context.Background(), "hi")
	require.Error(t, err)
	require.Contains(t, err.Error(), "load tokens")
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
