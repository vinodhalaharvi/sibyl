// Package okta is the OAuth Provider implementation for Okta.
//
// Okta's OAuth surface is standard OIDC with one wrinkle: Okta wants
// the issuer URL as a base for all endpoints. The well-known config
// at <issuer>/.well-known/openid-configuration enumerates the
// /authorize, /token, /userinfo endpoints.
//
// For simplicity, NewProvider takes a Config with all endpoints
// pre-resolved. A future enhancement could auto-discover them.
package okta

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/vinodhalaharvi/sibyl/auth/oauth"
)

// Config configures the Okta provider's HTTP client and endpoints.
type Config struct {
	// ClientID is the Okta app's client ID.
	ClientID string

	// Issuer is the Okta issuer URL (e.g. "https://dev-123.okta.com/oauth2/default").
	// Used as the base for endpoint URLs and for id_token verification (later).
	Issuer string

	// HTTPClient is optional; defaults to a 30s-timeout client.
	HTTPClient *http.Client
}

// NewProvider returns an oauth.Provider value with all arrows bound
// to the Okta endpoints derived from cfg.Issuer.
func NewProvider(cfg Config) (oauth.Provider, error) {
	if cfg.ClientID == "" {
		return oauth.Provider{}, errors.New("okta: ClientID is required")
	}
	if cfg.Issuer == "" {
		return oauth.Provider{}, errors.New("okta: Issuer is required")
	}
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = &http.Client{Timeout: 30 * time.Second}
	}

	endpoints := struct {
		Authorize string
		Token     string
		UserInfo  string
		Revoke    string
	}{
		Authorize: strings.TrimRight(cfg.Issuer, "/") + "/v1/authorize",
		Token:     strings.TrimRight(cfg.Issuer, "/") + "/v1/token",
		UserInfo:  strings.TrimRight(cfg.Issuer, "/") + "/v1/userinfo",
		Revoke:    strings.TrimRight(cfg.Issuer, "/") + "/v1/revoke",
	}

	return oauth.Provider{
		Name: "okta",
		Authorize: func(_ context.Context, in oauth.AuthRequest) (oauth.AuthorizeRedirect, error) {
			q := url.Values{}
			q.Set("client_id", cfg.ClientID)
			q.Set("response_type", "code")
			q.Set("redirect_uri", in.RedirectURI)
			q.Set("scope", strings.Join(in.Scopes, " "))
			q.Set("state", in.State)
			q.Set("code_challenge", in.CodeChallenge)
			q.Set("code_challenge_method", "S256")
			return oauth.AuthorizeRedirect{
				URL:          endpoints.Authorize + "?" + q.Encode(),
				State:        in.State,
				CodeVerifier: in.CodeVerifier,
			}, nil
		},
		Exchange: makeExchangeArrow(cfg.HTTPClient, endpoints.Token, cfg.ClientID),
		Refresh:  makeRefreshArrow(cfg.HTTPClient, endpoints.Token, cfg.ClientID),
		Whoami:   makeWhoamiArrow(cfg.HTTPClient, endpoints.UserInfo),
		Revoke:   makeRevokeArrow(cfg.HTTPClient, endpoints.Revoke, cfg.ClientID),
	}, nil
}

func makeExchangeArrow(client *http.Client, tokenEndpoint, clientID string) oauth.ExchangeArrow {
	return func(ctx context.Context, in oauth.ExchangeRequest) (oauth.TokenPair, error) {
		form := url.Values{}
		form.Set("grant_type", "authorization_code")
		form.Set("client_id", clientID)
		form.Set("code", in.Code.Code)
		form.Set("code_verifier", in.CodeVerifier)
		form.Set("redirect_uri", in.RedirectURI)
		return postForm(ctx, client, tokenEndpoint, form)
	}
}

func makeRefreshArrow(client *http.Client, tokenEndpoint, clientID string) oauth.RefreshArrow {
	return func(ctx context.Context, tokens oauth.TokenPair) (oauth.TokenPair, error) {
		if tokens.RefreshToken == "" {
			return oauth.TokenPair{}, errors.New("okta: refresh: no refresh token in pair")
		}
		form := url.Values{}
		form.Set("grant_type", "refresh_token")
		form.Set("client_id", clientID)
		form.Set("refresh_token", tokens.RefreshToken)
		return postForm(ctx, client, tokenEndpoint, form)
	}
}

func makeWhoamiArrow(client *http.Client, userInfoEndpoint string) oauth.WhoamiArrow {
	return func(ctx context.Context, tokens oauth.TokenPair) (oauth.Identity, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, userInfoEndpoint, nil)
		if err != nil {
			return oauth.Identity{}, fmt.Errorf("okta whoami: build request: %w", err)
		}
		req.Header.Set("Authorization", "Bearer "+tokens.AccessToken)
		req.Header.Set("Accept", "application/json")
		resp, err := client.Do(req)
		if err != nil {
			return oauth.Identity{}, fmt.Errorf("okta whoami: http: %w", err)
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
			return oauth.Identity{}, oauth.AuthenticationError{
				Wrapped: fmt.Errorf("okta whoami: status %d: %s", resp.StatusCode, string(body)),
				Status:  resp.StatusCode,
			}
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return oauth.Identity{}, fmt.Errorf("okta whoami: status %d: %s", resp.StatusCode, string(body))
		}
		var raw map[string]any
		if err := json.Unmarshal(body, &raw); err != nil {
			return oauth.Identity{}, fmt.Errorf("okta whoami: parse json: %w", err)
		}
		sub, _ := raw["sub"].(string)
		email, _ := raw["email"].(string)
		name, _ := raw["name"].(string)
		if sub == "" {
			return oauth.Identity{}, errors.New("okta whoami: response missing sub")
		}
		return oauth.Identity{
			Canonical:   "okta:" + sub,
			Provider:    "okta",
			ProviderID:  sub,
			Email:       email,
			DisplayName: name,
			Raw:         raw,
		}, nil
	}
}

func makeRevokeArrow(client *http.Client, revokeEndpoint, clientID string) oauth.RevokeArrow {
	return func(ctx context.Context, tokens oauth.TokenPair) (struct{}, error) {
		form := url.Values{}
		form.Set("client_id", clientID)
		form.Set("token", tokens.AccessToken)
		form.Set("token_type_hint", "access_token")

		req, err := http.NewRequestWithContext(ctx, http.MethodPost, revokeEndpoint, strings.NewReader(form.Encode()))
		if err != nil {
			return struct{}{}, fmt.Errorf("okta revoke: build request: %w", err)
		}
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		resp, err := client.Do(req)
		if err != nil {
			return struct{}{}, fmt.Errorf("okta revoke: http: %w", err)
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		// Per RFC 7009, 200 is success. Some providers return 204.
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return struct{}{}, fmt.Errorf("okta revoke: status %d: %s", resp.StatusCode, string(body))
		}
		return struct{}{}, nil
	}
}

// postForm is the shared "POST application/x-www-form-urlencoded to
// the token endpoint and parse the response" helper. Used by Exchange
// and Refresh.
func postForm(ctx context.Context, client *http.Client, endpoint string, form url.Values) (oauth.TokenPair, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return oauth.TokenPair{}, fmt.Errorf("okta token: build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return oauth.TokenPair{}, fmt.Errorf("okta token: http: %w", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode == http.StatusUnauthorized {
		return oauth.TokenPair{}, oauth.AuthenticationError{
			Wrapped: fmt.Errorf("okta token: status 401: %s", string(body)),
			Status:  resp.StatusCode,
		}
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return oauth.TokenPair{}, fmt.Errorf("okta token: status %d: %s", resp.StatusCode, string(body))
	}

	var raw map[string]any
	if err := json.Unmarshal(body, &raw); err != nil {
		return oauth.TokenPair{}, fmt.Errorf("okta token: parse json: %w", err)
	}
	pair := oauth.TokenPair{
		AccessToken:  stringField(raw, "access_token"),
		RefreshToken: stringField(raw, "refresh_token"),
		TokenType:    stringField(raw, "token_type"),
		Scope:        stringField(raw, "scope"),
		Raw:          raw,
	}
	if pair.AccessToken == "" {
		return oauth.TokenPair{}, errors.New("okta token: response missing access_token")
	}
	// expires_in is a JSON number; compute absolute expiry.
	if expiresIn := numberField(raw, "expires_in"); expiresIn > 0 {
		pair.ExpiresAt = time.Now().Add(time.Duration(expiresIn) * time.Second)
	}
	return pair, nil
}

func stringField(m map[string]any, key string) string {
	v, ok := m[key]
	if !ok {
		return ""
	}
	s, _ := v.(string)
	return s
}

func numberField(m map[string]any, key string) int {
	v, ok := m[key]
	if !ok {
		return 0
	}
	switch n := v.(type) {
	case float64:
		return int(n)
	case int:
		return n
	case string:
		i, err := strconv.Atoi(n)
		if err == nil {
			return i
		}
	}
	return 0
}
