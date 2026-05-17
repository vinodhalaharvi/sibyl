package oauth_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/vinodhalaharvi/sibyl/auth/oauth"
)

// --- CurrentSession ------------------------------------------------------

func TestCurrentSession_ReturnsStoredTokensWhenFresh(t *testing.T) {
	store := oauth.NewMemoryTokenStore(oauth.NoOpCipher{})
	defer store.Close()
	id := sampleIdentity()
	tokens := oauth.TokenPair{
		AccessToken:  "fresh-token",
		RefreshToken: "rt",
		ExpiresAt:    time.Now().Add(time.Hour),
	}
	require.NoError(t, store.Put(context.Background(), id, "okta", tokens))

	// Refresh should NOT be called.
	refresh := func(_ context.Context, _ oauth.TokenPair) (oauth.TokenPair, error) {
		t.Fatal("refresh should not run when token is fresh")
		return oauth.TokenPair{}, nil
	}

	session, err := oauth.CurrentSession(context.Background(), store, refresh, id, "okta", 5*time.Minute)
	require.NoError(t, err)
	require.Equal(t, "fresh-token", session.Token.AccessToken)
	require.Equal(t, id, session.Identity)
}

func TestCurrentSession_RefreshesWhenExpired(t *testing.T) {
	store := oauth.NewMemoryTokenStore(oauth.NoOpCipher{})
	defer store.Close()
	id := sampleIdentity()
	old := oauth.TokenPair{
		AccessToken:  "old-expired",
		RefreshToken: "rt",
		ExpiresAt:    time.Now().Add(-time.Minute),
	}
	require.NoError(t, store.Put(context.Background(), id, "okta", old))

	refreshCalled := false
	refresh := func(_ context.Context, _ oauth.TokenPair) (oauth.TokenPair, error) {
		refreshCalled = true
		return oauth.TokenPair{
			AccessToken:  "newly-refreshed",
			RefreshToken: "rt2",
			ExpiresAt:    time.Now().Add(time.Hour),
		}, nil
	}

	session, err := oauth.CurrentSession(context.Background(), store, refresh, id, "okta", 0)
	require.NoError(t, err)
	require.True(t, refreshCalled, "refresh must run when token is expired")
	require.Equal(t, "newly-refreshed", session.Token.AccessToken)

	stored, _ := store.Get(context.Background(), id, "okta")
	require.Equal(t, "newly-refreshed", stored.AccessToken)
}

func TestCurrentSession_RefreshesWhenWithinLeeway(t *testing.T) {
	store := oauth.NewMemoryTokenStore(oauth.NoOpCipher{})
	defer store.Close()
	id := sampleIdentity()
	require.NoError(t, store.Put(context.Background(), id, "okta", oauth.TokenPair{
		AccessToken:  "expiring-soon",
		RefreshToken: "rt",
		ExpiresAt:    time.Now().Add(30 * time.Second),
	}))

	refreshCalled := false
	refresh := func(_ context.Context, _ oauth.TokenPair) (oauth.TokenPair, error) {
		refreshCalled = true
		return oauth.TokenPair{AccessToken: "refreshed-proactively", ExpiresAt: time.Now().Add(time.Hour)}, nil
	}

	session, err := oauth.CurrentSession(context.Background(), store, refresh, id, "okta", time.Minute)
	require.NoError(t, err)
	require.True(t, refreshCalled)
	require.Equal(t, "refreshed-proactively", session.Token.AccessToken)
}

func TestCurrentSession_LoadFailureReturnsError(t *testing.T) {
	store := oauth.NewMemoryTokenStore(oauth.NoOpCipher{})
	defer store.Close()
	id := sampleIdentity()

	_, err := oauth.CurrentSession(context.Background(), store, makeFakeProvider("okta").Refresh, id, "okta", 0)
	require.Error(t, err)
	require.ErrorIs(t, errors.Unwrap(err), oauth.ErrTokenNotFound)
}

func TestCurrentSession_ExpiredWithNilRefreshFails(t *testing.T) {
	store := oauth.NewMemoryTokenStore(oauth.NoOpCipher{})
	defer store.Close()
	id := sampleIdentity()
	require.NoError(t, store.Put(context.Background(), id, "okta", oauth.TokenPair{
		AccessToken: "expired", ExpiresAt: time.Now().Add(-time.Minute),
	}))

	_, err := oauth.CurrentSession(context.Background(), store, nil, id, "okta", 0)
	require.Error(t, err)
	require.Contains(t, err.Error(), "no refresh arrow")
}

// --- StartFlow validation surface ----------------------------------------
//
// StartFlow's happy path needs a real Temporal client. These tests
// cover the validation that runs before any client.Client call.

func TestStartFlow_RejectsEmptyProvider(t *testing.T) {
	reg := oauth.NewRegistry()
	mapper := oauth.NewMemoryStateMapper()
	_, err := oauth.StartFlow(context.Background(), nil, reg, mapper, oauth.StartFlowInput{
		RedirectURI: "u", StoreKey: "default", TaskQueue: "tq",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "Provider")
}

func TestStartFlow_RejectsEmptyRedirectURI(t *testing.T) {
	reg := oauth.NewRegistry()
	mapper := oauth.NewMemoryStateMapper()
	_, err := oauth.StartFlow(context.Background(), nil, reg, mapper, oauth.StartFlowInput{
		Provider: "okta", StoreKey: "default", TaskQueue: "tq",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "RedirectURI")
}

func TestStartFlow_RejectsNilRegistry(t *testing.T) {
	mapper := oauth.NewMemoryStateMapper()
	_, err := oauth.StartFlow(context.Background(), nil, nil, mapper, oauth.StartFlowInput{
		Provider: "okta", RedirectURI: "u", StoreKey: "default", TaskQueue: "tq",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "registry is nil")
}

func TestStartFlow_RejectsNilMapper(t *testing.T) {
	reg := oauth.NewRegistry()
	_, err := oauth.StartFlow(context.Background(), nil, reg, nil, oauth.StartFlowInput{
		Provider: "okta", RedirectURI: "u", StoreKey: "default", TaskQueue: "tq",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "mapper is nil")
}

func TestStartFlow_UnknownProvider(t *testing.T) {
	reg := oauth.NewRegistry()
	mapper := oauth.NewMemoryStateMapper()
	_, err := oauth.StartFlow(context.Background(), nil, reg, mapper, oauth.StartFlowInput{
		Provider: "no-such-provider", RedirectURI: "u", StoreKey: "default", TaskQueue: "tq",
	})
	require.Error(t, err)
	require.ErrorIs(t, err, oauth.ErrProviderNotRegistered)
}

// --- Callback handler validation -----------------------------------------
//
// End-to-end testing of NewCallbackHandler against a live Temporal
// server is covered by workflow_test.go's HappyPath case (which uses
// the testsuite to inject signals). Here we verify the handler factory's
// preconditions.

func TestNewCallbackHandler_NilTemporalClientPanics(t *testing.T) {
	mapper := oauth.NewMemoryStateMapper()
	require.Panics(t, func() {
		oauth.NewCallbackHandler(nil, mapper, oauth.CallbackHandlerOptions{})
	})
}
