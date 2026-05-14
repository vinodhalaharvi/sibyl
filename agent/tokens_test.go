package agent_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/vinodhalaharvi/sibyl/agent"
)

func TestTokenUsage_TotalAndAdd(t *testing.T) {
	u := agent.TokenUsage{Input: 10, Output: 5}
	require.Equal(t, 15, u.Total())

	u.Add(agent.TokenUsage{Input: 3, Output: 7})
	require.Equal(t, agent.TokenUsage{Input: 13, Output: 12}, u)
}

func TestAtomicTokenSink_RecordsAcrossCalls(t *testing.T) {
	sink := &agent.AtomicTokenSink{}
	sink.RecordCall(context.Background(), agent.TokenUsage{Input: 100, Output: 50})
	sink.RecordCall(context.Background(), agent.TokenUsage{Input: 200, Output: 0})

	in, out, calls := sink.Snapshot()
	require.EqualValues(t, 300, in)
	require.EqualValues(t, 50, out)
	require.EqualValues(t, 2, calls)
	require.EqualValues(t, 350, sink.Total())
}

func TestAtomicTokenSink_ConcurrentSafe(t *testing.T) {
	sink := &agent.AtomicTokenSink{}
	var wg sync.WaitGroup
	const goroutines = 50
	const callsEach = 20
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < callsEach; j++ {
				sink.RecordCall(context.Background(), agent.TokenUsage{Input: 1, Output: 1})
			}
		}()
	}
	wg.Wait()
	_, _, calls := sink.Snapshot()
	require.EqualValues(t, goroutines*callsEach, calls)
	require.EqualValues(t, 2*goroutines*callsEach, sink.Total())
}

func TestWithTokenAccounting_NilSinkIsNoop(t *testing.T) {
	inner := agent.CompleteFunc(func(_ context.Context, _, _ string) (string, error) {
		return "x", nil
	})
	wrapped := agent.WithTokenAccounting(nil)(inner)

	out, err := wrapped(context.Background(), "sys", "user")
	require.NoError(t, err)
	require.Equal(t, "x", out)
	// No panic, no error — that's the whole assertion.
}

func TestWithTokenAccounting_ReportsApproxTokens(t *testing.T) {
	sink := &agent.AtomicTokenSink{}
	// system has 8 chars -> ~2 tokens; user has 4 chars -> ~1 token;
	// response is "world" = 5 chars -> ~2 tokens (rounded up).
	inner := agent.CompleteFunc(func(_ context.Context, _, _ string) (string, error) {
		return "world", nil
	})
	wrapped := agent.WithTokenAccounting(sink)(inner)

	_, err := wrapped(context.Background(), "12345678", "abcd")
	require.NoError(t, err)

	in, out, calls := sink.Snapshot()
	require.EqualValues(t, 1, calls)
	// 8/4 + 4/4 = 2 + 1 = 3
	require.EqualValues(t, 3, in)
	// (5+3)/4 = 2
	require.EqualValues(t, 2, out)
}

func TestWithTokenAccounting_ReportsEvenOnError(t *testing.T) {
	sink := &agent.AtomicTokenSink{}
	boom := errors.New("provider down")
	inner := agent.CompleteFunc(func(_ context.Context, _, _ string) (string, error) {
		return "", boom
	})
	wrapped := agent.WithTokenAccounting(sink)(inner)

	_, err := wrapped(context.Background(), "12345678", "abcd")
	require.ErrorIs(t, err, boom)

	in, out, calls := sink.Snapshot()
	require.EqualValues(t, 1, calls, "should record the call even though it failed")
	require.EqualValues(t, 3, in, "input cost was real (prompt was constructed)")
	require.EqualValues(t, 0, out, "no output produced")
}

func TestWithTokenAccounting_EmptyPromptsCountZero(t *testing.T) {
	sink := &agent.AtomicTokenSink{}
	inner := agent.CompleteFunc(func(_ context.Context, _, _ string) (string, error) {
		return "", nil
	})
	wrapped := agent.WithTokenAccounting(sink)(inner)

	_, err := wrapped(context.Background(), "", "")
	require.NoError(t, err)

	in, out, _ := sink.Snapshot()
	require.EqualValues(t, 0, in)
	require.EqualValues(t, 0, out)
}

// Composition test: token accounting works alongside cache.
// Cache hits should NOT be counted as new token spend.
func TestWithTokenAccounting_DoesNotCountCacheHits(t *testing.T) {
	sink := &agent.AtomicTokenSink{}
	cache := agent.NewMemoryCache(0)

	inner := agent.CompleteFunc(func(_ context.Context, _, _ string) (string, error) {
		return "result", nil
	})

	// Order matters: cache outermost so hits skip token accounting too.
	complete := agent.Chain(inner,
		agent.WithCache(cache),
		agent.WithTokenAccounting(sink),
	)

	// First call: miss, counts tokens.
	_, _ = complete(context.Background(), "sys", "user")
	in1, out1, calls1 := sink.Snapshot()

	// Second call: hit, should NOT add to sink (cache short-circuits).
	_, _ = complete(context.Background(), "sys", "user")
	in2, out2, calls2 := sink.Snapshot()

	require.Equal(t, in1, in2, "cache hit shouldn't increase input tokens")
	require.Equal(t, out1, out2, "cache hit shouldn't increase output tokens")
	require.Equal(t, calls1, calls2, "cache hit shouldn't increase call count")
}
