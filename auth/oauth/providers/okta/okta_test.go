package okta_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/vinodhalaharvi/sibyl/auth/oauth"
	"github.com/vinodhalaharvi/sibyl/auth/oauth/providers/okta"
)

// mockOktaServer is a tiny httptest.Server that mimics enough of
// Okta's OAuth surface to exercise the provider arrows. It records
// each request for assertions in the calling test.
type mockOktaServer struct {
	server *httptest.Server

	// What the next /token call returns. Tests set these.
	tokenResponse map[string]any
	tokenStatus   int

	// What the next /userinfo call returns.
	userInfoResponse map[string]any
	userInfoStatus   int

	// What the next /revoke call returns.
	revokeStatus int

	// Recorded requests.
	tokenCalls    []map[string]string
	userInfoCalls int
	revokeCalls   []map[string]string
}

func newMockOkta(t *testing.T) *mockOktaServer {
	m := &mockOktaServer{
		tokenStatus:    200,
		userInfoStatus: 200,
		revokeStatus:   200,
		tokenResponse: map[string]any{
			"access_token":  "default-access",
			"refresh_token": "default-refresh",
			"token_type":    "Bearer",
			"expires_in":    3600,
			"scope":         "openid email",
		},
		userInfoResponse: map[string]any{
			"sub":   "00u-default",
			"email": "default@example.com",
			"name":  "Default User",
		},
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/token", m.handleToken)
	mux.HandleFunc("/v1/userinfo", m.handleUserInfo)
	mux.HandleFunc("/v1/revoke", m.handleRevoke)
	m.server = httptest.NewServer(mux)
	t.Cleanup(m.server.Close)
	return m
}

func (m *mockOktaServer) handleToken(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	call := map[string]string{}
	for k := range r.PostForm {
		call[k] = r.PostForm.Get(k)
	}
	m.tokenCalls = append(m.tokenCalls, call)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(m.tokenStatus)
	_ = json.NewEncoder(w).Encode(m.tokenResponse)
}

func (m *mockOktaServer) handleUserInfo(w http.ResponseWriter, _ *http.Request) {
	m.userInfoCalls++
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(m.userInfoStatus)
	_ = json.NewEncoder(w).Encode(m.userInfoResponse)
}

func (m *mockOktaServer) handleRevoke(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	call := map[string]string{}
	for k := range r.PostForm {
		call[k] = r.PostForm.Get(k)
	}
	m.revokeCalls = append(m.revokeCalls, call)
	w.WriteHeader(m.revokeStatus)
}

// --- Tests ---------------------------------------------------------------

func newProvider(t *testing.T, server *httptest.Server) oauth.Provider {
	p, err := okta.NewProvider(okta.Config{
		ClientID: "test-client",
		Issuer:   server.URL,
	})
	require.NoError(t, err)
	return p
}

func TestOkta_AuthorizeBuildsCorrectURL(t *testing.T) {
	mock := newMockOkta(t)
	p := newProvider(t, mock.server)

	req, err := oauth.NewAuthRequest("okta", "http://localhost:8090/cb", []string{"openid", "email"})
	require.NoError(t, err)

	redirect, err := p.Authorize(context.Background(), req)
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(redirect.URL, mock.server.URL+"/v1/authorize?"))
	require.Contains(t, redirect.URL, "client_id=test-client")
	require.Contains(t, redirect.URL, "response_type=code")
	require.Contains(t, redirect.URL, "code_challenge_method=S256")
	require.Contains(t, redirect.URL, "code_challenge="+req.CodeChallenge)
	require.Contains(t, redirect.URL, "state="+req.State)
	require.Equal(t, req.State, redirect.State)
	require.Equal(t, req.CodeVerifier, redirect.CodeVerifier)
}

func TestOkta_ExchangeSendsCorrectFormAndParsesTokens(t *testing.T) {
	mock := newMockOkta(t)
	p := newProvider(t, mock.server)

	tokens, err := p.Exchange(context.Background(), oauth.ExchangeRequest{
		Code:         oauth.AuthCode{Code: "auth-code-xyz", State: "st"},
		CodeVerifier: "the-pkce-verifier",
		RedirectURI:  "http://localhost:8090/cb",
	})
	require.NoError(t, err)
	require.Equal(t, "default-access", tokens.AccessToken)
	require.Equal(t, "default-refresh", tokens.RefreshToken)
	require.Equal(t, "Bearer", tokens.TokenType)
	require.False(t, tokens.ExpiresAt.IsZero())
	require.True(t, tokens.ExpiresAt.After(time.Now()))

	// Verify the form sent to /token.
	require.Len(t, mock.tokenCalls, 1)
	form := mock.tokenCalls[0]
	require.Equal(t, "authorization_code", form["grant_type"])
	require.Equal(t, "test-client", form["client_id"])
	require.Equal(t, "auth-code-xyz", form["code"])
	require.Equal(t, "the-pkce-verifier", form["code_verifier"])
	require.Equal(t, "http://localhost:8090/cb", form["redirect_uri"])
}

func TestOkta_RefreshSendsCorrectForm(t *testing.T) {
	mock := newMockOkta(t)
	p := newProvider(t, mock.server)

	_, err := p.Refresh(context.Background(), oauth.TokenPair{
		AccessToken: "old-access", RefreshToken: "old-refresh",
	})
	require.NoError(t, err)
	require.Len(t, mock.tokenCalls, 1)
	form := mock.tokenCalls[0]
	require.Equal(t, "refresh_token", form["grant_type"])
	require.Equal(t, "old-refresh", form["refresh_token"])
}

func TestOkta_RefreshFailsWithoutRefreshToken(t *testing.T) {
	mock := newMockOkta(t)
	p := newProvider(t, mock.server)

	_, err := p.Refresh(context.Background(), oauth.TokenPair{
		AccessToken: "x", RefreshToken: "",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "no refresh token")
}

func TestOkta_Whoami_BuildsCanonicalIdentity(t *testing.T) {
	mock := newMockOkta(t)
	mock.userInfoResponse = map[string]any{
		"sub":   "00u-abc-123",
		"email": "alice@example.com",
		"name":  "Alice Smith",
	}
	p := newProvider(t, mock.server)

	id, err := p.Whoami(context.Background(), oauth.TokenPair{AccessToken: "any"})
	require.NoError(t, err)
	require.Equal(t, "okta:00u-abc-123", id.Canonical)
	require.Equal(t, "okta", id.Provider)
	require.Equal(t, "00u-abc-123", id.ProviderID)
	require.Equal(t, "alice@example.com", id.Email)
	require.Equal(t, "Alice Smith", id.DisplayName)
}

func TestOkta_Whoami_UnauthorizedReturnsAuthError(t *testing.T) {
	mock := newMockOkta(t)
	mock.userInfoStatus = 401
	p := newProvider(t, mock.server)

	_, err := p.Whoami(context.Background(), oauth.TokenPair{AccessToken: "stale"})
	require.Error(t, err)
	var authErr oauth.AuthenticationError
	require.True(t, errors.As(err, &authErr), "should be classified as AuthenticationError")
	require.Equal(t, 401, authErr.Status)
}

func TestOkta_Whoami_MissingSubFails(t *testing.T) {
	mock := newMockOkta(t)
	mock.userInfoResponse = map[string]any{"email": "x@y.com"} // no sub
	p := newProvider(t, mock.server)

	_, err := p.Whoami(context.Background(), oauth.TokenPair{AccessToken: "any"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "missing sub")
}

func TestOkta_Revoke_PostsCorrectForm(t *testing.T) {
	mock := newMockOkta(t)
	p := newProvider(t, mock.server)

	_, err := p.Revoke(context.Background(), oauth.TokenPair{
		AccessToken: "to-be-revoked",
	})
	require.NoError(t, err)
	require.Len(t, mock.revokeCalls, 1)
	form := mock.revokeCalls[0]
	require.Equal(t, "test-client", form["client_id"])
	require.Equal(t, "to-be-revoked", form["token"])
}

func TestOkta_TokenEndpoint5xxBubblesUp(t *testing.T) {
	mock := newMockOkta(t)
	mock.tokenStatus = 502
	mock.tokenResponse = map[string]any{"error": "bad_gateway"}
	p := newProvider(t, mock.server)

	_, err := p.Exchange(context.Background(), oauth.ExchangeRequest{
		Code: oauth.AuthCode{Code: "x"},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "status 502")
}

func TestOkta_RejectsEmptyConfig(t *testing.T) {
	_, err := okta.NewProvider(okta.Config{})
	require.Error(t, err)
}

func TestOkta_RegistryIntegration(t *testing.T) {
	mock := newMockOkta(t)
	p := newProvider(t, mock.server)

	reg := oauth.NewRegistry()
	require.NoError(t, reg.Register(p))

	got, err := reg.Get("okta")
	require.NoError(t, err)
	require.Equal(t, "okta", got.Name)
	require.True(t, oauth.ProviderHasRevoke(got))
}
