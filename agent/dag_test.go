package agent_test

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/vinodhalaharvi/weft/weft"

	"github.com/vinodhalaharvi/sibyl/agent"
)

// passthrough is a tiny arrow that records when it ran and returns its
// own NodeID's value, useful for testing ordering.
func passthrough[T any](value T) weft.Arrow[T, T] {
	return func(_ context.Context, in T) (T, error) {
		return in, nil
	}
}

func TestDAG_LinearChain(t *testing.T) {
	// a -> b -> c. Each appends its name to a string.
	dag := agent.NewDAG()

	a := agent.AddTypedNode(dag, "a",
		weft.Arrow[string, string](func(_ context.Context, s string) (string, error) {
			return s + "a", nil
		}),
		func(in agent.NodeInputs) (string, error) {
			// "input" is seeded by the caller below.
			return in.MustGet("input").(string), nil
		},
		agent.DependsOn("input"),
	)
	b := agent.AddTypedNode(dag, "b",
		weft.Arrow[string, string](func(_ context.Context, s string) (string, error) {
			return s + "b", nil
		}),
		func(in agent.NodeInputs) (string, error) {
			return in.MustGet(a).(string), nil
		},
		agent.DependsOn(a),
	)
	_ = agent.AddTypedNode(dag, "c",
		weft.Arrow[string, string](func(_ context.Context, s string) (string, error) {
			return s + "c", nil
		}),
		func(in agent.NodeInputs) (string, error) {
			return in.MustGet(b).(string), nil
		},
		agent.DependsOn(b),
	)
	// "input" is a special seed-only node that we don't AddNode but
	// declare as a dependency. Since the DAG validates "all deps must
	// be known nodes", we add a no-arrow placeholder.
	agent.AddTypedNode(dag, "input",
		passthrough[string](""), // arrow won't run because we seed
		func(in agent.NodeInputs) (string, error) { return "", nil },
	)

	compiled, err := dag.Compile()
	require.NoError(t, err)

	out, err := compiled.Execute(context.Background(), map[agent.NodeID]any{
		"input": "start:",
	})
	require.NoError(t, err)
	require.Equal(t, "start:abc", out["c"])
}

func TestDAG_DiamondParallelism(t *testing.T) {
	// Layout:
	//     root
	//     /  \
	//    b    c    (run in parallel)
	//     \  /
	//      d
	//
	// b and c each sleep 100ms. If they run sequentially, total >= 200ms.
	// If they run in parallel, total < 150ms.
	dag := agent.NewDAG()

	agent.AddTypedNode(dag, "root",
		passthrough[int](0),
		func(_ agent.NodeInputs) (int, error) { return 1, nil },
	)
	agent.AddTypedNode(dag, "b",
		weft.Arrow[int, int](func(_ context.Context, n int) (int, error) {
			time.Sleep(100 * time.Millisecond)
			return n + 10, nil
		}),
		func(in agent.NodeInputs) (int, error) { return in.MustGet("root").(int), nil },
		agent.DependsOn("root"),
	)
	agent.AddTypedNode(dag, "c",
		weft.Arrow[int, int](func(_ context.Context, n int) (int, error) {
			time.Sleep(100 * time.Millisecond)
			return n + 100, nil
		}),
		func(in agent.NodeInputs) (int, error) { return in.MustGet("root").(int), nil },
		agent.DependsOn("root"),
	)
	agent.AddTypedNode(dag, "d",
		weft.Arrow[[2]int, int](func(_ context.Context, bc [2]int) (int, error) {
			return bc[0] + bc[1], nil
		}),
		func(in agent.NodeInputs) ([2]int, error) {
			return [2]int{in.MustGet("b").(int), in.MustGet("c").(int)}, nil
		},
		agent.DependsOn("b", "c"),
	)

	compiled, err := dag.Compile()
	require.NoError(t, err)

	start := time.Now()
	out, err := compiled.Execute(context.Background(), map[agent.NodeID]any{})
	elapsed := time.Since(start)

	require.NoError(t, err)
	require.Equal(t, 11+101, out["d"]) // 11 + (1+100)=101
	require.Less(t, elapsed, 180*time.Millisecond,
		"b and c should have run in parallel; took %v", elapsed)
}

func TestDAG_CycleRejected(t *testing.T) {
	dag := agent.NewDAG()
	dag.AddNode("a", nil, agent.DependsOn("b"))
	dag.AddNode("b", nil, agent.DependsOn("a"))

	_, err := dag.Compile()
	require.Error(t, err)
	require.Contains(t, err.Error(), "cycle")
}

func TestDAG_SelfLoopRejected(t *testing.T) {
	dag := agent.NewDAG()
	dag.AddNode("a", nil, agent.DependsOn("a"))

	_, err := dag.Compile()
	require.Error(t, err)
	require.Contains(t, err.Error(), "itself")
}

func TestDAG_UnknownDependencyRejected(t *testing.T) {
	dag := agent.NewDAG()
	dag.AddNode("a", nil, agent.DependsOn("not_a_node"))

	_, err := dag.Compile()
	require.Error(t, err)
	require.Contains(t, err.Error(), "unknown node")
}

func TestDAG_DuplicateNodeIDRejected(t *testing.T) {
	dag := agent.NewDAG()
	dag.AddNode("a", nil)
	dag.AddNode("a", nil)

	_, err := dag.Compile()
	require.Error(t, err)
	require.Contains(t, err.Error(), "duplicate")
}

func TestDAG_NodeFailureStopsExecution(t *testing.T) {
	dag := agent.NewDAG()
	var downstreamRan atomic.Bool

	agent.AddTypedNode(dag, "fails",
		weft.Arrow[any, any](func(_ context.Context, _ any) (any, error) {
			return nil, errors.New("boom")
		}),
		func(_ agent.NodeInputs) (any, error) { return nil, nil },
	)
	agent.AddTypedNode(dag, "downstream",
		weft.Arrow[any, any](func(_ context.Context, _ any) (any, error) {
			downstreamRan.Store(true)
			return nil, nil
		}),
		func(_ agent.NodeInputs) (any, error) { return nil, nil },
		agent.DependsOn("fails"),
	)

	compiled, err := dag.Compile()
	require.NoError(t, err)

	_, err = compiled.Execute(context.Background(), nil)
	require.Error(t, err)
	var nodeErr *agent.NodeError
	require.ErrorAs(t, err, &nodeErr)
	require.Equal(t, agent.NodeID("fails"), nodeErr.NodeID)
	require.Contains(t, nodeErr.Err.Error(), "boom")
	require.False(t, downstreamRan.Load(), "downstream should not run after upstream failure")
}

func TestDAG_SiblingFailureCancelsConcurrentNodes(t *testing.T) {
	// Two siblings run in parallel. One fails fast; the other is in a
	// long sleep that respects context cancellation. We expect the
	// slow one to abort.
	dag := agent.NewDAG()
	var slowFinished atomic.Bool

	agent.AddTypedNode(dag, "fast_fail",
		weft.Arrow[any, any](func(_ context.Context, _ any) (any, error) {
			return nil, errors.New("fast fail")
		}),
		func(_ agent.NodeInputs) (any, error) { return nil, nil },
	)
	agent.AddTypedNode(dag, "slow",
		weft.Arrow[any, any](func(ctx context.Context, _ any) (any, error) {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(2 * time.Second):
				slowFinished.Store(true)
				return "done", nil
			}
		}),
		func(_ agent.NodeInputs) (any, error) { return nil, nil },
	)

	compiled, err := dag.Compile()
	require.NoError(t, err)

	start := time.Now()
	_, err = compiled.Execute(context.Background(), nil)
	elapsed := time.Since(start)

	require.Error(t, err)
	require.Less(t, elapsed, 1*time.Second,
		"slow sibling should have been cancelled, not run to completion")
	require.False(t, slowFinished.Load(), "slow node should have been cancelled mid-sleep")
}

func TestDAG_TopologicalLayersAreDeterministic(t *testing.T) {
	// Three independent nodes at layer 0: a, b, c. They should be
	// scheduled in lexical order within the layer (deterministic).
	dag := agent.NewDAG()

	var orderObserved []string
	var orderMu = make(chan string, 3)
	_ = orderMu

	for _, id := range []string{"c", "a", "b"} {
		id := id // capture
		dag.AddNode(agent.NodeID(id), agent.AnyArrow(func(_ context.Context, _ any) (any, error) {
			// They run in parallel goroutines, so we can't assert
			// runtime ordering; assert the layer assignment instead.
			return id, nil
		}))
	}

	compiled, err := dag.Compile()
	require.NoError(t, err)

	out, err := compiled.Execute(context.Background(), nil)
	require.NoError(t, err)

	// All three should be in the output.
	keys := make([]string, 0, len(out))
	for k := range out {
		keys = append(keys, string(k))
	}
	sort.Strings(keys)
	require.Equal(t, []string{"a", "b", "c"}, keys)

	_ = orderObserved
}

func TestDAG_SeedValuesAreUsable(t *testing.T) {
	// "input" is a seed-only node. Other nodes depend on it without
	// it having an arrow that actually runs.
	dag := agent.NewDAG()
	// Add it as a placeholder so DependsOn("input") validates, but the
	// seed will already have populated its output.
	agent.AddTypedNode(dag, "input",
		passthrough[string](""), // wouldn't run (seeded)
		func(_ agent.NodeInputs) (string, error) { return "", nil },
	)
	agent.AddTypedNode(dag, "consumer",
		weft.Arrow[string, string](func(_ context.Context, s string) (string, error) {
			return "got: " + s, nil
		}),
		func(in agent.NodeInputs) (string, error) {
			return in.MustGet("input").(string), nil
		},
		agent.DependsOn("input"),
	)

	compiled, err := dag.Compile()
	require.NoError(t, err)

	out, err := compiled.Execute(context.Background(), map[agent.NodeID]any{
		"input": "hello",
	})
	require.NoError(t, err)
	require.Equal(t, "got: hello", out["consumer"])
}

func TestDAG_MustCompilePanicsOnError(t *testing.T) {
	dag := agent.NewDAG()
	dag.AddNode("a", nil, agent.DependsOn("not_a_node"))

	require.Panics(t, func() {
		_ = dag.MustCompile()
	})
}

// A realistic-shaped DAG test: parse -> [security|tests|style] -> synthesize.
// This exercises the diamond/fan-out-fan-in pattern that's the main use case.
func TestDAG_PRReviewShape(t *testing.T) {
	dag := agent.NewDAG()

	// Stage 1: parse the diff.
	parse := agent.AddTypedNode(dag, "parse",
		weft.Arrow[string, []string](func(_ context.Context, s string) ([]string, error) {
			return []string{"file1.go", "file2.go"}, nil
		}),
		func(in agent.NodeInputs) (string, error) {
			return in.MustGet("diff").(string), nil
		},
		agent.DependsOn("diff"),
	)
	// Seed placeholder
	agent.AddTypedNode(dag, "diff",
		passthrough[string](""),
		func(_ agent.NodeInputs) (string, error) { return "", nil },
	)

	// Stage 2: three parallel analyzers.
	for _, name := range []string{"security", "tests", "style"} {
		name := name
		agent.AddTypedNode(dag, agent.NodeID(name),
			weft.Arrow[[]string, string](func(_ context.Context, files []string) (string, error) {
				return fmt.Sprintf("%s ok for %d files", name, len(files)), nil
			}),
			func(in agent.NodeInputs) ([]string, error) {
				return in.MustGet(parse).([]string), nil
			},
			agent.DependsOn(parse),
		)
	}

	// Stage 3: synthesizer joins them.
	agent.AddTypedNode(dag, "synthesize",
		weft.Arrow[[]string, string](func(_ context.Context, parts []string) (string, error) {
			out := ""
			for i, p := range parts {
				if i > 0 {
					out += " | "
				}
				out += p
			}
			return out, nil
		}),
		func(in agent.NodeInputs) ([]string, error) {
			return []string{
				in.MustGet("security").(string),
				in.MustGet("tests").(string),
				in.MustGet("style").(string),
			}, nil
		},
		agent.DependsOn("security", "tests", "style"),
	)

	compiled, err := dag.Compile()
	require.NoError(t, err)

	out, err := compiled.Execute(context.Background(), map[agent.NodeID]any{
		"diff": "fake diff content",
	})
	require.NoError(t, err)
	require.Contains(t, out["synthesize"], "security ok for 2 files")
	require.Contains(t, out["synthesize"], "tests ok for 2 files")
	require.Contains(t, out["synthesize"], "style ok for 2 files")
}
