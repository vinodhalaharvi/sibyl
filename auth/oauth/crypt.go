// Package oauth — crypt.go provides at-rest encryption for token
// material in TokenStore implementations.
//
// Design:
//
//   - AES-256-GCM. Authenticated encryption (catches tampering),
//     widely-supported, in Go's standard library.
//   - Nonces are random per-encryption (12 bytes, prepended to
//     ciphertext). Reusing a nonce with the same key would be
//     catastrophic; random per-call eliminates the risk.
//   - Keys are 32 bytes (AES-256). NewAESGCMCipher rejects others.
//   - Keys come from the application; we provide one helper to read
//     from an environment variable for convenience.
//
// What this is NOT:
//
//   - Not a key management system. The application provides the key.
//   - Not envelope encryption (KMS-wrapped data keys). That's a v2
//     concern. The TokenCipher interface gives a clean upgrade path.
//   - Not for password storage. Use bcrypt/argon2 for password hashes.
//     This is for symmetric encryption of token material at rest.
package oauth

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
)

// TokenCipher encrypts and decrypts token material before it lands
// in a TokenStore's backing storage. All TokenStore implementations
// take a TokenCipher; tests use NoOpCipher.
type TokenCipher interface {
	Encrypt(plaintext []byte) ([]byte, error)
	Decrypt(ciphertext []byte) ([]byte, error)
}

// AESGCMCipher is the production cipher. It holds a 32-byte AES-256
// key in memory; use NewAESGCMCipherFromEnv or NewAESGCMCipher.
type AESGCMCipher struct {
	aead cipher.AEAD
}

// NewAESGCMCipher returns an AES-GCM cipher for the given 32-byte key.
// Returns an error if the key is the wrong length.
func NewAESGCMCipher(key []byte) (*AESGCMCipher, error) {
	if len(key) != 32 {
		return nil, fmt.Errorf("oauth: AES-256-GCM requires 32-byte key, got %d", len(key))
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("oauth: aes.NewCipher: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("oauth: cipher.NewGCM: %w", err)
	}
	return &AESGCMCipher{aead: aead}, nil
}

// NewAESGCMCipherFromEnv reads SIBYL_OAUTH_KEY (base64-encoded 32
// bytes) and returns a cipher. The standard base64 alphabet is
// accepted (with or without padding) for convenience.
//
// Generate a key with: openssl rand -base64 32
func NewAESGCMCipherFromEnv() (*AESGCMCipher, error) {
	raw := os.Getenv("SIBYL_OAUTH_KEY")
	if raw == "" {
		return nil, errors.New("oauth: SIBYL_OAUTH_KEY is not set")
	}
	// Try standard base64; fall back to base64-url.
	key, err := base64.StdEncoding.DecodeString(raw)
	if err != nil {
		key, err = base64.RawStdEncoding.DecodeString(raw)
		if err != nil {
			key, err = base64.URLEncoding.DecodeString(raw)
			if err != nil {
				return nil, fmt.Errorf("oauth: SIBYL_OAUTH_KEY base64 decode: %w", err)
			}
		}
	}
	return NewAESGCMCipher(key)
}

// Encrypt seals plaintext with a fresh random nonce. The output is
// nonce || ciphertext || tag. The 12-byte nonce is prepended so
// Decrypt can read it back.
func (c *AESGCMCipher) Encrypt(plaintext []byte) ([]byte, error) {
	nonce := make([]byte, c.aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("oauth: nonce gen: %w", err)
	}
	// Seal appends the tag automatically.
	sealed := c.aead.Seal(nil, nonce, plaintext, nil)
	// Prepend nonce so Decrypt can split it back out.
	out := make([]byte, 0, len(nonce)+len(sealed))
	out = append(out, nonce...)
	out = append(out, sealed...)
	return out, nil
}

// Decrypt verifies and decrypts the output of Encrypt. Returns an
// error if the tag check fails (tampering or wrong key).
func (c *AESGCMCipher) Decrypt(ciphertext []byte) ([]byte, error) {
	nonceSize := c.aead.NonceSize()
	if len(ciphertext) < nonceSize {
		return nil, errors.New("oauth: ciphertext too short")
	}
	nonce, sealed := ciphertext[:nonceSize], ciphertext[nonceSize:]
	plaintext, err := c.aead.Open(nil, nonce, sealed, nil)
	if err != nil {
		return nil, fmt.Errorf("oauth: AES-GCM open: %w", err)
	}
	return plaintext, nil
}

// NoOpCipher passes plaintext through unchanged. ONLY for tests.
// Production code that constructs this is a configuration bug — the
// SQLite store will refuse to start with it once we add that check.
// For now, the explicit name is the warning.
type NoOpCipher struct{}

// Encrypt returns the input unchanged.
func (NoOpCipher) Encrypt(p []byte) ([]byte, error) { return p, nil }

// Decrypt returns the input unchanged.
func (NoOpCipher) Decrypt(p []byte) ([]byte, error) { return p, nil }
