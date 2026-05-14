package agent_test

import (
	"context"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/vinodhalaharvi/sibyl/agent"
)

// newTempSQLite creates a fresh on-disk SQLite cache in t.TempDir().
// Returns the cache and a t.Cleanup-registered close.
func newTempSQLite(t *testing.T, ttl time.Duration) *agent.SQLiteCache {
	t.Helper()
	path := filepath.Join(t.TempDir(), "cache.db")
	c, err := agent.NewSQLiteCache(path, ttl)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func TestSQLiteCache_BasicGetSet(t *testing.T) {
	c := newTempSQLite(t, 0)
	c.Set("k", "v")
	got, ok := c.Get("k")
	require.True(t, ok)
	require.Equal(t, "v", got)
	require.Equal(t, 1, c.Len())
}

func TestSQLiteCache_MissReturnsFalse(t *testing.T) {
	c := newTempSQLite(t, 0)
	_, ok := c.Get("nope")
	require.False(t, ok)
}

func TestSQLiteCache_OverwriteOnSet(t *testing.T) {
	c := newTempSQLite(t, 0)
	c.Set("k", "v1")
	c.Set("k", "v2")
	got, ok := c.Get("k")
	require.True(t, ok)
	require.Equal(t, "v2", got)
	require.Equal(t, 1, c.Len())
}

func TestSQLiteCache_TTLExpiry(t *testing.T) {
	// 1s TTL -> immediate hit, 2s wait -> miss.
	// Smaller TTLs are unsafe here because we store unix seconds.
	c := newTempSQLite(t, 1*time.Second)
	c.Set("k", "v")

	got, ok := c.Get("k")
	require.True(t, ok)
	require.Equal(t, "v", got)

	time.Sleep(2 * time.Second)
	_, ok = c.Get("k")
	require.False(t, ok, "expired entry should not be returned")
	require.Equal(t, 0, c.Len(), "expired entry should be deleted on Get")
}

func TestSQLiteCache_PersistsAcrossOpens(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cache.db")

	// First handle: write.
	c1, err := agent.NewSQLiteCache(path, 0)
	require.NoError(t, err)
	c1.Set("k", "persists")
	require.NoError(t, c1.Close())

	// Second handle: read.
	c2, err := agent.NewSQLiteCache(path, 0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c2.Close() })

	got, ok := c2.Get("k")
	require.True(t, ok, "value should survive close+reopen")
	require.Equal(t, "persists", got)
}

func TestSQLiteCache_EmptyPathErrors(t *testing.T) {
	_, err := agent.NewSQLiteCache("", 0)
	require.Error(t, err)
}

func TestSQLiteCache_ConcurrentSafe(t *testing.T) {
	c := newTempSQLite(t, time.Hour)
	var wg sync.WaitGroup
	const iter = 50
	for i := 0; i < iter; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			c.Set("k", "v")
		}()
		go func() {
			defer wg.Done()
			c.Get("k")
		}()
	}
	wg.Wait()
	got, ok := c.Get("k")
	require.True(t, ok)
	require.Equal(t, "v", got)
}

func TestSQLiteCache_Vacuum(t *testing.T) {
	c := newTempSQLite(t, 1*time.Second)
	c.Set("a", "1")
	c.Set("b", "2")
	require.Equal(t, 2, c.Len())

	time.Sleep(2 * time.Second)
	require.NoError(t, c.Vacuum())
	require.Equal(t, 0, c.Len(), "Vacuum should delete expired entries")
}

// Integration: SQLiteCache plugs into WithCache the same way MemoryCache does.
func TestSQLiteCache_AsWithCacheBackend(t *testing.T) {
	c := newTempSQLite(t, time.Hour)
	var innerCalls atomic.Int64

	inner := agent.CompleteFunc(func(_ context.Context, _, _ string) (string, error) {
		innerCalls.Add(1)
		return "result", nil
	})
	wrapped := agent.WithCache(c)(inner)

	out, err := wrapped(context.Background(), "sys", "user")
	require.NoError(t, err)
	require.Equal(t, "result", out)
	require.EqualValues(t, 1, innerCalls.Load())

	// Same prompts -> hit, no inner call.
	out, err = wrapped(context.Background(), "sys", "user")
	require.NoError(t, err)
	require.Equal(t, "result", out)
	require.EqualValues(t, 1, innerCalls.Load())
}
