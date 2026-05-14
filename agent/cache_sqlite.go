package agent

import (
	"database/sql"
	"errors"
	"fmt"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// SQLiteCache is a persistent Cache backed by a single-file SQLite database.
//
// Compared to MemoryCache:
//   - Persists across worker restarts. A cache hit from yesterday is still
//     a hit today, as long as TTL hasn't elapsed.
//   - Shared across worker processes pointing at the same DB file. Good for
//     a small cluster; for many workers, swap to a centralized Redis-backed
//     Cache instead.
//   - Lazy expiration: stale entries are deleted on Get rather than by a
//     background cleaner. Simpler, no goroutines. Cost: disk space lingers
//     until entries are touched. Bounded by ~unique-prompts × value-size.
//
// Build requirements: this package imports github.com/mattn/go-sqlite3,
// which requires CGO. Set CGO_ENABLED=1 when cross-compiling.
type SQLiteCache struct {
	db  *sql.DB
	ttl time.Duration
}

// NewSQLiteCache opens (or creates) the SQLite database at path and returns
// a ready-to-use Cache. The TTL applies to all entries; a zero or negative
// TTL disables expiration.
//
// The schema is one table:
//
//	CREATE TABLE entries (
//	    k          TEXT PRIMARY KEY,
//	    v          BLOB NOT NULL,
//	    expires_at INTEGER NOT NULL  -- unix seconds; 0 means "never"
//	)
//
// pragmas are set for sensible defaults: WAL mode for concurrent reads
// alongside writes, synchronous=NORMAL for a good crash-safety/throughput
// balance, busy_timeout for graceful handling of contended writes.
func NewSQLiteCache(path string, ttl time.Duration) (*SQLiteCache, error) {
	if path == "" {
		return nil, errors.New("SQLiteCache: path is required")
	}
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, fmt.Errorf("sqlite open: %w", err)
	}
	// One connection at a time avoids "database is locked" surprises in the
	// face of mixed read+write traffic. The cache itself sees little load,
	// so we don't lose meaningful throughput.
	db.SetMaxOpenConns(1)

	// Pragmas. Run before schema setup so any DDL benefits from them.
	pragmas := []string{
		"PRAGMA journal_mode = WAL",
		"PRAGMA synchronous = NORMAL",
		"PRAGMA busy_timeout = 5000",
	}
	for _, p := range pragmas {
		if _, err := db.Exec(p); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("sqlite pragma %q: %w", p, err)
		}
	}

	const schema = `
CREATE TABLE IF NOT EXISTS entries (
    k          TEXT PRIMARY KEY,
    v          BLOB NOT NULL,
    expires_at INTEGER NOT NULL
)`
	if _, err := db.Exec(schema); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("sqlite schema: %w", err)
	}

	return &SQLiteCache{db: db, ttl: ttl}, nil
}

// Close releases the underlying database handle. Always defer Close().
func (c *SQLiteCache) Close() error {
	return c.db.Close()
}

// Get returns the cached value for key if present and not expired.
//
// On a stale hit, the entry is deleted opportunistically. This bounds
// disk growth without needing a separate cleaner.
func (c *SQLiteCache) Get(key string) (string, bool) {
	const q = `SELECT v, expires_at FROM entries WHERE k = ?`
	var (
		value     []byte
		expiresAt int64
	)
	err := c.db.QueryRow(q, key).Scan(&value, &expiresAt)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false
	}
	if err != nil {
		// Soft failure: treat as miss. The fallback is a (potentially
		// duplicated) inner call, which is cheaper than crashing.
		return "", false
	}
	if expiresAt != 0 && time.Now().Unix() > expiresAt {
		_, _ = c.db.Exec(`DELETE FROM entries WHERE k = ?`, key)
		return "", false
	}
	return string(value), true
}

// Set stores value under key. Existing entries are replaced.
func (c *SQLiteCache) Set(key, value string) {
	var expiresAt int64
	if c.ttl > 0 {
		expiresAt = time.Now().Add(c.ttl).Unix()
	}
	const q = `INSERT OR REPLACE INTO entries (k, v, expires_at) VALUES (?, ?, ?)`
	_, _ = c.db.Exec(q, key, []byte(value), expiresAt)
}

// Len reports the number of entries in the cache (including expired
// entries that haven't been swept yet). Useful for metrics and tests.
func (c *SQLiteCache) Len() int {
	var n int
	_ = c.db.QueryRow(`SELECT COUNT(*) FROM entries`).Scan(&n)
	return n
}

// Vacuum deletes all expired entries proactively. Optional — called by
// nobody in the framework; expose for callers who want a periodic sweep.
func (c *SQLiteCache) Vacuum() error {
	if c.ttl <= 0 {
		return nil
	}
	_, err := c.db.Exec(`DELETE FROM entries WHERE expires_at != 0 AND expires_at < ?`, time.Now().Unix())
	return err
}
