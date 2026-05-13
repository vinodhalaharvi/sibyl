package agent_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/vinodhalaharvi/sibyl/agent"
)

// --- WithLogging ------------------------------------------------------------

func TestWithLogging_LogsSuccess(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	inner := agent.CompleteFunc(func(_ context.Context, _, _ string) (string, error) {
		return "ok", nil
	})
	wrapped := agent.WithLogging(logger)(inner)

	out, err := wrapped(context.Background(), "sys-prompt", "user-msg")
	require.NoError(t, err)
	require.Equal(t, "ok", out)

	logged := buf.String()
	require.Contains(t, logged, "llm call start")
	require.Contains(t, logged, "llm call ok")
	require.Contains(t, logged, "took=")
	// Content should NOT appear in logs.
	require.NotContains(t, logged, "sys-prompt")
	require.NotContains(t, logged, "user-msg")
}

func TestWithLogging_LogsFailure(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))

	boom := errors.New("provider down")
	inner := agent.CompleteFunc(func(_ context.Context, _, _ string) (string, error) {
		return "", boom
	})
	wrapped := agent.WithLogging(logger)(inner)

	_, err := wrapped(context.Background(), "", "")
	require.ErrorIs(t, err, boom)

	logged := buf.String()
	require.Contains(t, logged, "llm call failed")
	require.Contains(t, logged, "provider down")
	require.Contains(t, logged, "level=WARN")
}

func TestWithLogging_NilLoggerUsesDefault(t *testing.T) {
	inner := agent.CompleteFunc(func(_ context.Context, _, _ string) (string, error) {
		return "ok", nil
	})
	wrapped := agent.WithLogging(nil)(inner)
	out, err := wrapped(context.Background(), "", "")
	require.NoError(t, err)
	require.Equal(t, "ok", out)
}

// --- WithRetry --------------------------------------------------------------

func TestWithRetry_SucceedsFirstAttempt(t *testing.T) {
	var calls atomic.Int64
	inner := agent.CompleteFunc(func(_ context.Context, _, _ string) (string, error) {
		calls.Add(1)
		return "ok", nil
	})
	wrapped := agent.WithRetry(3, 10*time.Millisecond)(inner)

	out, err := wrapped(context.Background(), "", "")
	require.NoError(t, err)
	require.Equal(t, "ok", out)
	require.EqualValues(t, 1, calls.Load(), "no retries on success")
}

func TestWithRetry_RetriesOnTransientErrorThenSucceeds(t *testing.T) {
	var calls atomic.Int64
	inner := agent.CompleteFunc(func(_ context.Context, _, _ string) (string, error) {
		n := calls.Add(1)
		if n < 3 {
			return "", errors.New("transient")
		}
		return "ok-after-retries", nil
	})
	wrapped := agent.WithRetry(5, 1*time.Millisecond)(inner)

	out, err := wrapped(context.Background(), "", "")
	require.NoError(t, err)
	require.Equal(t, "ok-after-retries", out)
	require.EqualValues(t, 3, calls.Load())
}

func TestWithRetry_GivesUpAfterMaxAttempts(t *testing.T) {
	var calls atomic.Int64
	inner := agent.CompleteFunc(func(_ context.Context, _, _ string) (string, error) {
		calls.Add(1)
		return "", errors.New("always fails")
	})
	wrapped := agent.WithRetry(3, 1*time.Millisecond)(inner)

	_, err := wrapped(context.Background(), "", "")
	require.Error(t, err)
	require.Contains(t, err.Error(), "after 3 attempts")
	require.EqualValues(t, 3, calls.Load())
}

func TestWithRetry_RespectsContextCancellation(t *testing.T) {
	var calls atomic.Int64
	inner := agent.CompleteFunc(func(_ context.Context, _, _ string) (string, error) {
		calls.Add(1)
		return "", errors.New("transient")
	})
	wrapped := agent.WithRetry(10, 500*time.Millisecond)(inner)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	_, err := wrapped(ctx, "", "")
	require.ErrorIs(t, err, context.Canceled)
	require.Less(t, calls.Load(), int64(10))
}

// --- WithCache + MemoryCache ------------------------------------------------

func TestMemoryCache_BasicGetSet(t *testing.T) {
	c := agent.NewMemoryCache(0)
	c.Set("k", "v")
	got, ok := c.Get("k")
	require.True(t, ok)
	require.Equal(t, "v", got)
	require.Equal(t, 1, c.Len())
}

func TestMemoryCache_TTLExpiry(t *testing.T) {
	c := agent.NewMemoryCache(10 * time.Millisecond)
	c.Set("k", "v")

	got, ok := c.Get("k")
	require.True(t, ok)
	require.Equal(t, "v", got)

	time.Sleep(20 * time.Millisecond)
	_, ok = c.Get("k")
	require.False(t, ok)
	require.Equal(t, 0, c.Len(), "expired entry should be evicted on Get")
}

func TestMemoryCache_ConcurrentSafe(t *testing.T) {
	c := agent.NewMemoryCache(time.Hour)
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
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
}

func TestWithCache_FirstCallMissesSecondHits(t *testing.T) {
	cache := agent.NewMemoryCache(time.Hour)
	var innerCalls atomic.Int64

	inner := agent.CompleteFunc(func(_ context.Context, _, _ string) (string, error) {
		innerCalls.Add(1)
		return "result", nil
	})
	wrapped := agent.WithCache(cache)(inner)

	out1, err := wrapped(context.Background(), "sys", "user")
	require.NoError(t, err)
	require.Equal(t, "result", out1)
	require.EqualValues(t, 1, innerCalls.Load())

	out2, err := wrapped(context.Background(), "sys", "user")
	require.NoError(t, err)
	require.Equal(t, "result", out2)
	require.EqualValues(t, 1, innerCalls.Load(), "cache hit should skip inner call")
}

func TestWithCache_DifferentPromptsAreDifferentKeys(t *testing.T) {
	cache := agent.NewMemoryCache(time.Hour)
	var innerCalls atomic.Int64

	inner := agent.CompleteFunc(func(_ context.Context, _, user string) (string, error) {
		innerCalls.Add(1)
		return "out:" + user, nil
	})
	wrapped := agent.WithCache(cache)(inner)

	out1, _ := wrapped(context.Background(), "sys", "a")
	out2, _ := wrapped(context.Background(), "sys", "b")
	require.Equal(t, "out:a", out1)
	require.Equal(t, "out:b", out2)
	require.EqualValues(t, 2, innerCalls.Load())

	_, _ = wrapped(context.Background(), "sys2", "a")
	require.EqualValues(t, 3, innerCalls.Load())
}

func TestWithCache_DoesNotCacheErrors(t *testing.T) {
	cache := agent.NewMemoryCache(time.Hour)
	var innerCalls atomic.Int64

	inner := agent.CompleteFunc(func(_ context.Context, _, _ string) (string, error) {
		n := innerCalls.Add(1)
		if n == 1 {
			return "", errors.New("first call fails")
		}
		return "ok", nil
	})
	wrapped := agent.WithCache(cache)(inner)

	_, err := wrapped(context.Background(), "", "")
	require.Error(t, err)

	out, err := wrapped(context.Background(), "", "")
	require.NoError(t, err)
	require.Equal(t, "ok", out)
	require.EqualValues(t, 2, innerCalls.Load())
}

// --- Composition test (Chain across all three) ------------------------------

func TestChain_LoggingRetryCacheTogether(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	cache := agent.NewMemoryCache(time.Hour)
	var innerCalls atomic.Int64

	inner := agent.CompleteFunc(func(_ context.Context, _, _ string) (string, error) {
		n := innerCalls.Add(1)
		if n == 1 {
			return "", errors.New("first call transient")
		}
		return "result", nil
	})

	complete := agent.Chain(
		inner,
		agent.WithCache(cache),
		agent.WithRetry(3, 1*time.Millisecond),
		agent.WithLogging(logger),
	)

	// First call: log start -> retry (1 fail + 1 success) -> cache set
	out, err := complete(context.Background(), "sys", "user")
	require.NoError(t, err)
	require.Equal(t, "result", out)
	require.EqualValues(t, 2, innerCalls.Load(), "retried once after transient")

	// Second call: cache hit, skips retry+inner entirely.
	out, err = complete(context.Background(), "sys", "user")
	require.NoError(t, err)
	require.Equal(t, "result", out)
	require.EqualValues(t, 2, innerCalls.Load(), "second call hit cache")

	logged := buf.String()
	require.True(t, strings.Contains(logged, "llm call start"))
	require.True(t, strings.Contains(logged, "llm call ok"))
}
