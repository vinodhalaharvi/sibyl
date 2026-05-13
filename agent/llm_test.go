package agent_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/vinodhalaharvi/sibyl/agent"
)

func TestChain_OrderAndComposition(t *testing.T) {
	var trace []string

	inner := agent.CompleteFunc(func(_ context.Context, _, _ string) (string, error) {
		trace = append(trace, "inner")
		return "result", nil
	})

	mw := func(label string) agent.Middleware {
		return func(next agent.CompleteFunc) agent.CompleteFunc {
			return func(ctx context.Context, sys, user string) (string, error) {
				trace = append(trace, "before:"+label)
				out, err := next(ctx, sys, user)
				trace = append(trace, "after:"+label)
				return out, err
			}
		}
	}

	chained := agent.Chain(inner, mw("A"), mw("B"))

	out, err := chained(context.Background(), "sys", "user")
	require.NoError(t, err)
	require.Equal(t, "result", out)

	// A wraps B wraps inner — so A sees the call first and the response last.
	require.Equal(t, []string{
		"before:A",
		"before:B",
		"inner",
		"after:B",
		"after:A",
	}, trace)
}

func TestChain_EmptyMiddlewareReturnsInnerUnchanged(t *testing.T) {
	called := false
	inner := agent.CompleteFunc(func(_ context.Context, _, _ string) (string, error) {
		called = true
		return "x", nil
	})

	out, err := agent.Chain(inner)(context.Background(), "", "")
	require.NoError(t, err)
	require.Equal(t, "x", out)
	require.True(t, called)
}

func TestChain_MiddlewareCanShortCircuit(t *testing.T) {
	innerCalls := atomic.Int64{}
	inner := agent.CompleteFunc(func(_ context.Context, _, _ string) (string, error) {
		innerCalls.Add(1)
		return "should not see this", nil
	})

	blocker := func(next agent.CompleteFunc) agent.CompleteFunc {
		return func(_ context.Context, _, _ string) (string, error) {
			return "", errors.New("blocked")
		}
	}

	out, err := agent.Chain(inner, blocker)(context.Background(), "", "")
	require.Error(t, err)
	require.Equal(t, "", out)
	require.EqualValues(t, 0, innerCalls.Load(),
		"a short-circuiting middleware should prevent inner from running")
}

// TestCompleteFunc_AsClosureNoStruct shows that test doubles do not need to be
// a struct type — a closure is a CompleteFunc. This is the ergonomic win of
// using a function-typed seam.
func TestCompleteFunc_AsClosureNoStruct(t *testing.T) {
	var f agent.CompleteFunc = func(_ context.Context, sys, user string) (string, error) {
		return fmt.Sprintf("sys=%s user=%s", sys, user), nil
	}

	out, err := f(context.Background(), "S", "U")
	require.NoError(t, err)
	require.Equal(t, "sys=S user=U", out)
}
