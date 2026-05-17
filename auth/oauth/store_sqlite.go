// Package oauth — store_sqlite.go provides a SQLite-backed TokenStore.
//
// Schema:
//
//	CREATE TABLE oauth_tokens (
//	  canonical_id  TEXT NOT NULL,    -- "okta:00u..."
//	  provider      TEXT NOT NULL,    -- "okta"
//	  identity_json TEXT NOT NULL,    -- full Identity, plaintext (no secrets)
//	  token_blob    BLOB NOT NULL,    -- encrypted TokenPair JSON
//	  expires_at    INTEGER NOT NULL, -- unix seconds; 0 = no expiry
//	  updated_at    INTEGER NOT NULL, -- unix seconds
//	  PRIMARY KEY (canonical_id, provider)
//	);
//
// Why SQLite for tokens?
//
//   - No infrastructure for hackathon demos.
//   - File-based; trivial to back up (cp the file).
//   - Survives restarts.
//   - Single-instance only — for multi-worker production, use the
//     Postgres backend (future patch).
//
// Encryption: all token material is encrypted via the injected
// TokenCipher before INSERT. Reads decrypt on the way out. The
// identity_json and expires_at columns are plaintext — they're
// not sensitive and we use them for indexing/sorting.
package oauth

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	// CGO SQLite driver; we already depend on this for the cache.
	_ "github.com/mattn/go-sqlite3"
)

// SQLiteTokenStore is a TokenStore backed by a SQLite database file.
type SQLiteTokenStore struct {
	db     *sql.DB
	cipher TokenCipher
}

// NewSQLiteTokenStore opens (or creates) a SQLite token store at the
// given path. The schema is created if it doesn't exist; existing
// schemas are validated.
//
// cipher MUST be non-nil. Pass NoOpCipher{} only for tests.
func NewSQLiteTokenStore(path string, cipher TokenCipher) (*SQLiteTokenStore, error) {
	if cipher == nil {
		return nil, errors.New("oauth: SQLiteTokenStore requires a non-nil TokenCipher")
	}
	// _journal=WAL improves concurrent read+write throughput; _busy_timeout
	// makes the busy retry behavior less surprising under contention.
	dsn := fmt.Sprintf("file:%s?_journal=WAL&_busy_timeout=5000&_synchronous=NORMAL", path)
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, fmt.Errorf("oauth sqlite: open %q: %w", path, err)
	}
	// Single writer for SQLite. Multiple readers OK with WAL mode.
	db.SetMaxOpenConns(1)

	s := &SQLiteTokenStore{db: db, cipher: cipher}
	if err := s.ensureSchema(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

func (s *SQLiteTokenStore) ensureSchema() error {
	const ddl = `
CREATE TABLE IF NOT EXISTS oauth_tokens (
  canonical_id  TEXT    NOT NULL,
  provider      TEXT    NOT NULL,
  identity_json TEXT    NOT NULL,
  token_blob    BLOB    NOT NULL,
  expires_at    INTEGER NOT NULL,
  updated_at    INTEGER NOT NULL,
  PRIMARY KEY (canonical_id, provider)
);
`
	_, err := s.db.Exec(ddl)
	if err != nil {
		return fmt.Errorf("oauth sqlite: ensure schema: %w", err)
	}
	return nil
}

// Get implements TokenStore.
func (s *SQLiteTokenStore) Get(ctx context.Context, identity Identity, provider string) (TokenPair, error) {
	const q = `SELECT token_blob FROM oauth_tokens WHERE canonical_id = ? AND provider = ?`
	var blob []byte
	err := s.db.QueryRowContext(ctx, q, identity.Canonical, provider).Scan(&blob)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return TokenPair{}, ErrTokenNotFound
	case err != nil:
		return TokenPair{}, fmt.Errorf("oauth sqlite: get: %w", err)
	}
	plaintext, err := s.cipher.Decrypt(blob)
	if err != nil {
		return TokenPair{}, fmt.Errorf("oauth sqlite: decrypt: %w", err)
	}
	var tokens TokenPair
	if err := json.Unmarshal(plaintext, &tokens); err != nil {
		return TokenPair{}, fmt.Errorf("oauth sqlite: unmarshal: %w", err)
	}
	return tokens, nil
}

// Put implements TokenStore.
func (s *SQLiteTokenStore) Put(ctx context.Context, identity Identity, provider string, tokens TokenPair) error {
	plaintext, err := json.Marshal(tokens)
	if err != nil {
		return fmt.Errorf("oauth sqlite: marshal tokens: %w", err)
	}
	ciphertext, err := s.cipher.Encrypt(plaintext)
	if err != nil {
		return fmt.Errorf("oauth sqlite: encrypt: %w", err)
	}
	identityJSON, err := json.Marshal(identity)
	if err != nil {
		return fmt.Errorf("oauth sqlite: marshal identity: %w", err)
	}

	var expiresUnix int64
	if !tokens.ExpiresAt.IsZero() {
		expiresUnix = tokens.ExpiresAt.Unix()
	}
	now := time.Now().Unix()

	const q = `
INSERT INTO oauth_tokens (canonical_id, provider, identity_json, token_blob, expires_at, updated_at)
VALUES (?, ?, ?, ?, ?, ?)
ON CONFLICT (canonical_id, provider) DO UPDATE SET
  identity_json = excluded.identity_json,
  token_blob    = excluded.token_blob,
  expires_at    = excluded.expires_at,
  updated_at    = excluded.updated_at
`
	if _, err := s.db.ExecContext(ctx, q,
		identity.Canonical, provider,
		string(identityJSON), ciphertext,
		expiresUnix, now); err != nil {
		return fmt.Errorf("oauth sqlite: put: %w", err)
	}
	return nil
}

// Delete implements TokenStore. No error on missing row.
func (s *SQLiteTokenStore) Delete(ctx context.Context, identity Identity, provider string) error {
	const q = `DELETE FROM oauth_tokens WHERE canonical_id = ? AND provider = ?`
	if _, err := s.db.ExecContext(ctx, q, identity.Canonical, provider); err != nil {
		return fmt.Errorf("oauth sqlite: delete: %w", err)
	}
	return nil
}

// List implements TokenStore.
func (s *SQLiteTokenStore) List(ctx context.Context) ([]TokenRef, error) {
	const q = `SELECT identity_json, provider, expires_at, updated_at FROM oauth_tokens ORDER BY updated_at DESC`
	rows, err := s.db.QueryContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("oauth sqlite: list: %w", err)
	}
	defer rows.Close()

	var refs []TokenRef
	for rows.Next() {
		var (
			identityJSON string
			provider     string
			expiresUnix  int64
			updatedUnix  int64
		)
		if err := rows.Scan(&identityJSON, &provider, &expiresUnix, &updatedUnix); err != nil {
			return nil, fmt.Errorf("oauth sqlite: scan: %w", err)
		}
		var identity Identity
		if err := json.Unmarshal([]byte(identityJSON), &identity); err != nil {
			return nil, fmt.Errorf("oauth sqlite: unmarshal identity: %w", err)
		}
		ref := TokenRef{
			Identity:  identity,
			Provider:  provider,
			UpdatedAt: time.Unix(updatedUnix, 0),
		}
		if expiresUnix > 0 {
			ref.ExpiresAt = time.Unix(expiresUnix, 0)
		}
		refs = append(refs, ref)
	}
	return refs, rows.Err()
}

// Close implements TokenStore.
func (s *SQLiteTokenStore) Close() error {
	return s.db.Close()
}
