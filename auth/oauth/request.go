// Package oauth — request.go provides constructors that fill in the
// security-critical random fields (State, CodeVerifier, CodeChallenge)
// from a cryptographic source.
//
// Callers should never set these fields manually. Use NewAuthRequest
// (and its variants) so the entropy is correct and the PKCE encoding
// is right.
package oauth

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
)

// NewAuthRequest builds an AuthRequest with cryptographically random
// State and PKCE values. Returns an error if the system CSPRNG fails
// (extremely unlikely; would indicate a broken crypto environment).
//
// The State is 32 bytes of entropy, base64url-encoded (43 chars).
// The CodeVerifier is 32 bytes of entropy, base64url-encoded (43 chars,
// within the RFC 7636 range of 43-128).
// The CodeChallenge is base64url(sha256(CodeVerifier)) per S256.
func NewAuthRequest(provider, redirectURI string, scopes []string) (AuthRequest, error) {
	state, err := randomBase64URL(32)
	if err != nil {
		return AuthRequest{}, fmt.Errorf("oauth: generate state: %w", err)
	}
	verifier, err := randomBase64URL(32)
	if err != nil {
		return AuthRequest{}, fmt.Errorf("oauth: generate code_verifier: %w", err)
	}
	challenge := pkceS256Challenge(verifier)

	return AuthRequest{
		Provider:      provider,
		Scopes:        scopes,
		RedirectURI:   redirectURI,
		State:         state,
		CodeVerifier:  verifier,
		CodeChallenge: challenge,
	}, nil
}

// pkceS256Challenge computes base64url(sha256(verifier)) per RFC 7636.
// This is the only challenge method we support (RFC 9700 forbids
// "plain").
func pkceS256Challenge(verifier string) string {
	sum := sha256.Sum256([]byte(verifier))
	return base64.RawURLEncoding.EncodeToString(sum[:])
}

// randomBase64URL returns n bytes of CSPRNG entropy, base64url-encoded
// without padding (per RFC 7636 / RFC 6749 conventions).
func randomBase64URL(n int) (string, error) {
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(buf), nil
}
