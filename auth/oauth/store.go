// Package oauth — store.go defines the TokenStore interface and the
// in-memory implementation.
//
// Three concrete implementations ship with the package:
//
//   - MemoryTokenStore  (this file)         — tests and ephemeral use
//   - SQLiteTokenStore  (store_sqlite.go)   — single-instance demos and production
//   - PostgresTokenStore (future)            — multi-instance production
//
// All three encrypt tokens at rest via a TokenCipher injected at
// construction time. The Memory store still uses the cipher because:
//
//   - Tests that intentionally exercise encryption need a real cipher.
//   - Code that runs identically with Memory or SQLite should observe
//     consistent semantics (a Get can fail with corruption if the
//     wrong key is used in either backend).
//
// Token addressing: (Identity.Canonical, Provider) is the composite
// key. We treat Canonical as the source of truth — it carries the
// provider prefix already. The Provider string in the API is for
// disambiguation when the same canonical ID could conceivably refer
// to multiple providers' tokens (rare in practice but cheap to support).
package oauth

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// TokenStore is the persistence boundary. Implementations encrypt
// token material before persisting and decrypt on read; the interface
// itself operates on plaintext TokenPair values.
//
// All methods must be safe for concurrent use.
type TokenStore interface {
	// Get retrieves the token pair for (identity, provider). Returns
	// (TokenPair{}, ErrTokenNotFound) if no token exists.
	Get(ctx context.Context, identity Identity, provider string) (TokenPair, error)

	// Put stores or replaces a token pair. Subsequent Get returns the
	// pair as supplied (modulo encryption round-trip). No version
	// history; the most recent Put wins.
	Put(ctx context.Context, identity Identity, provider string, tokens TokenPair) error

	// Delete removes the stored token. Returns nil whether the token
	// existed or not — the caller wanted it gone, it's gone.
	Delete(ctx context.Context, identity Identity, provider string) error

	// List returns lightweight refs to all stored tokens. Does NOT
	// return token material; that requires a per-ref Get.
	List(ctx context.Context) ([]TokenRef, error)

	// Close releases any backing resources (DB connections, file
	// handles). Safe to call multiple times.
	Close() error
}

// TokenRef is the lightweight tuple returned by List. No token material
// in this type — use Get for that.
type TokenRef struct {
	Identity  Identity
	Provider  string
	ExpiresAt time.Time
	UpdatedAt time.Time
}

// MemoryTokenStore holds tokens in a map. Lost on restart. Cipher is
// still applied so behavior matches persistent stores.
type MemoryTokenStore struct {
	cipher TokenCipher
	mu     sync.RWMutex
	tokens map[storeKey]storedEntry
}

// storeKey is the composite map key.
type storeKey struct {
	Canonical string
	Provider  string
}

// storedEntry is the in-memory record. Token is encrypted.
type storedEntry struct {
	Identity  Identity // stored plaintext; not sensitive
	Provider  string
	Token     []byte    // encrypted TokenPair JSON
	ExpiresAt time.Time // stored plaintext for List ordering
	UpdatedAt time.Time
}

// NewMemoryTokenStore returns a new in-memory store. The cipher MUST
// be non-nil; pass NoOpCipher{} for tests that want plaintext semantics.
func NewMemoryTokenStore(cipher TokenCipher) *MemoryTokenStore {
	if cipher == nil {
		// Programmer error; fail loudly.
		panic("oauth: MemoryTokenStore requires a non-nil TokenCipher (use NoOpCipher for tests)")
	}
	return &MemoryTokenStore{
		cipher: cipher,
		tokens: make(map[storeKey]storedEntry),
	}
}

// Get implements TokenStore.
func (m *MemoryTokenStore) Get(ctx context.Context, identity Identity, provider string) (TokenPair, error) {
	if err := ctx.Err(); err != nil {
		return TokenPair{}, err
	}
	m.mu.RLock()
	entry, ok := m.tokens[storeKey{identity.Canonical, provider}]
	m.mu.RUnlock()
	if !ok {
		return TokenPair{}, ErrTokenNotFound
	}
	plaintext, err := m.cipher.Decrypt(entry.Token)
	if err != nil {
		return TokenPair{}, fmt.Errorf("oauth: memory store decrypt: %w", err)
	}
	var tokens TokenPair
	if err := json.Unmarshal(plaintext, &tokens); err != nil {
		return TokenPair{}, fmt.Errorf("oauth: memory store unmarshal: %w", err)
	}
	return tokens, nil
}

// Put implements TokenStore.
func (m *MemoryTokenStore) Put(ctx context.Context, identity Identity, provider string, tokens TokenPair) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	plaintext, err := json.Marshal(tokens)
	if err != nil {
		return fmt.Errorf("oauth: memory store marshal: %w", err)
	}
	ciphertext, err := m.cipher.Encrypt(plaintext)
	if err != nil {
		return fmt.Errorf("oauth: memory store encrypt: %w", err)
	}
	now := time.Now()
	m.mu.Lock()
	m.tokens[storeKey{identity.Canonical, provider}] = storedEntry{
		Identity:  identity,
		Provider:  provider,
		Token:     ciphertext,
		ExpiresAt: tokens.ExpiresAt,
		UpdatedAt: now,
	}
	m.mu.Unlock()
	return nil
}

// Delete implements TokenStore. Returns nil whether the token existed
// or not.
func (m *MemoryTokenStore) Delete(ctx context.Context, identity Identity, provider string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	delete(m.tokens, storeKey{identity.Canonical, provider})
	m.mu.Unlock()
	return nil
}

// List implements TokenStore.
func (m *MemoryTokenStore) List(ctx context.Context) ([]TokenRef, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	refs := make([]TokenRef, 0, len(m.tokens))
	for _, entry := range m.tokens {
		refs = append(refs, TokenRef{
			Identity:  entry.Identity,
			Provider:  entry.Provider,
			ExpiresAt: entry.ExpiresAt,
			UpdatedAt: entry.UpdatedAt,
		})
	}
	return refs, nil
}

// Close is a no-op for the memory store.
func (m *MemoryTokenStore) Close() error { return nil }
