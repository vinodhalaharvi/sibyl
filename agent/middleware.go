package agent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"math/rand"
	"sync"
	"time"
)

// middleware.go provides three reusable Middleware values that wrap a
// CompleteFunc. Constructors take config and return a Middleware. Compose
// with Chain:
//
//	complete := Chain(
//	    realClient.Complete,
//	    WithCache(NewMemoryCache(time.Hour)),
//	    WithRetry(3, 200*time.Millisecond),
//	    WithLogging(slog.Default()),
//	)
//
// Temporal already retries failed activities. WithRetry retries INSIDE
// one activity invocation, so brief transient errors get absorbed
// without bumping Temporal's attempt counter. Stack both for two
// layers of resilience.

// --- WithLogging ------------------------------------------------------------

// WithLogging logs the start, end, and duration of every CompleteFunc call.
// Errors are logged at WARN; successes at DEBUG. The log fields include
// prompt and response *lengths* but never the content itself — keeping
// logs cheap and free of sensitive data.
func WithLogging(logger *slog.Logger) Middleware {
	if logger == nil {
		logger = slog.Default()
	}
	return func(next CompleteFunc) CompleteFunc {
		return func(ctx context.Context, system, user string) (string, error) {
			start := time.Now()
			logger.DebugContext(ctx, "llm call start",
				"system_len", len(system),
				"user_len", len(user),
			)
			out, err := next(ctx, system, user)
			dur := time.Since(start)
			if err != nil {
				logger.WarnContext(ctx, "llm call failed",
					"err", err,
					"took", dur,
				)
				return out, err
			}
			logger.DebugContext(ctx, "llm call ok",
				"took", dur,
				"output_len", len(out),
			)
			return out, nil
		}
	}
}

// --- WithRetry --------------------------------------------------------------

// WithRetry retries the inner CompleteFunc on error, up to attempts-1
// times, with exponential backoff plus jitter. Context cancellation
// aborts immediately.
func WithRetry(attempts int, initial time.Duration) Middleware {
	if attempts < 1 {
		attempts = 1
	}
	if initial <= 0 {
		initial = 100 * time.Millisecond
	}
	return func(next CompleteFunc) CompleteFunc {
		return func(ctx context.Context, system, user string) (string, error) {
			var lastErr error
			delay := initial
			for i := 0; i < attempts; i++ {
				out, err := next(ctx, system, user)
				if err == nil {
					return out, nil
				}
				lastErr = err
				if i == attempts-1 {
					break
				}
				jitter := time.Duration(rand.Int63n(int64(delay) / 2))
				select {
				case <-ctx.Done():
					return "", ctx.Err()
				case <-time.After(delay + jitter):
				}
				delay *= 2
			}
			return "", fmt.Errorf("after %d attempts: %w", attempts, lastErr)
		}
	}
}

// --- WithCache --------------------------------------------------------------

// Cache is the storage interface for WithCache. Implementations must be
// safe for concurrent use.
type Cache interface {
	Get(key string) (string, bool)
	Set(key, value string)
}

// MemoryCache is a TTL-bounded in-memory cache. Concurrent-safe.
// Memory is unbounded — appropriate for moderate volume; use Redis
// or similar for production.
type MemoryCache struct {
	ttl time.Duration

	mu    sync.RWMutex
	store map[string]memoryCacheEntry
}

type memoryCacheEntry struct {
	value   string
	expires time.Time
}

// NewMemoryCache returns a fresh in-memory cache with the given TTL.
// A zero or negative TTL disables expiration.
func NewMemoryCache(ttl time.Duration) *MemoryCache {
	return &MemoryCache{
		ttl:   ttl,
		store: make(map[string]memoryCacheEntry),
	}
}

// Get returns the cached value for key if present and not expired.
func (c *MemoryCache) Get(key string) (string, bool) {
	c.mu.RLock()
	entry, ok := c.store[key]
	c.mu.RUnlock()
	if !ok {
		return "", false
	}
	if c.ttl > 0 && time.Now().After(entry.expires) {
		c.mu.Lock()
		delete(c.store, key)
		c.mu.Unlock()
		return "", false
	}
	return entry.value, true
}

// Set stores value under key.
func (c *MemoryCache) Set(key, value string) {
	var expires time.Time
	if c.ttl > 0 {
		expires = time.Now().Add(c.ttl)
	}
	c.mu.Lock()
	c.store[key] = memoryCacheEntry{value: value, expires: expires}
	c.mu.Unlock()
}

// Len reports the current number of cached entries.
func (c *MemoryCache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.store)
}

// WithCache memoizes CompleteFunc calls keyed by (system, user).
// Cache hits skip the inner call entirely. Errors are NOT cached.
//
// CAUTION: caching LLM responses across workflow executions is only
// safe if your prompts are truly idempotent. Within a single workflow
// replay, Temporal's event history already provides exactly-once
// semantics — this middleware is for cross-workflow reuse.
func WithCache(c Cache) Middleware {
	return func(next CompleteFunc) CompleteFunc {
		return func(ctx context.Context, system, user string) (string, error) {
			key := cacheKey(system, user)
			if hit, ok := c.Get(key); ok {
				return hit, nil
			}
			out, err := next(ctx, system, user)
			if err != nil {
				return out, err
			}
			c.Set(key, out)
			return out, nil
		}
	}
}

// cacheKey deterministically hashes the prompt pair. The separator byte
// prevents (system="a", user="b") from colliding with (system="a\x00b", user="").
func cacheKey(system, user string) string {
	h := sha256.New()
	h.Write([]byte(system))
	h.Write([]byte{0})
	h.Write([]byte(user))
	return hex.EncodeToString(h.Sum(nil))
}
