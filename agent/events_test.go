package agent_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/vinodhalaharvi/weft/weft"

	"github.com/vinodhalaharvi/sibyl/agent"
)

// --- Event basics ---------------------------------------------------------

func TestEvents_KindsMatchExpected(t *testing.T) {
	// Sanity: each event type returns the expected Kind. Catches typos
	// in the EventKind constants and constructor wiring.
	cases := []struct {
		event agent.Event
		want  agent.EventKind
	}{
		{agent.NewWorkflowStarted("wf1", "ConvergeWorkflow", nil), agent.EventKindWorkflowStarted},
		{agent.NewWorkflowCompleted("wf1", "out", 0), agent.EventKindWorkflowCompleted},
		{agent.NewWorkflowFailed("wf1", errors.New("x"), 0), agent.EventKindWorkflowFailed},
		{agent.NewNodeStarted("wf1", "n1", "label"), agent.EventKindNodeStarted},
		{agent.NewNodeCompleted("wf1", "n1", "label", nil, 0), agent.EventKindNodeCompleted},
		{agent.NewNodeFailed("wf1", "n1", "label", errors.New("x"), 0), agent.EventKindNodeFailed},
		{agent.NewActivityStarted("wf1", "Research", "n1"), agent.EventKindActivityStarted},
		{agent.NewActivityCompleted("wf1", "Research", "n1", nil, 0), agent.EventKindActivityCompleted},
		{agent.NewToolCalled("wf1", "calc", map[string]any{"a": 1}, 1), agent.EventKindToolCalled},
		{agent.NewToolCompleted("wf1", "calc", 1, "result", nil, 0), agent.EventKindToolCompleted},
	}
	for _, c := range cases {
		require.Equal(t, c.want, c.event.Kind(), "kind mismatch for %T", c.event)
		require.NotZero(t, c.event.Timestamp(), "timestamp should be auto-stamped")
		require.NotEmpty(t, c.event.WorkflowID(), "workflow id should be set")
	}
}

// --- MemoryBroker ---------------------------------------------------------

func TestMemoryBroker_PerWorkflowDelivery(t *testing.T) {
	b := agent.NewMemoryBroker()
	defer b.Close()

	chA, cancelA := b.Subscribe("wf-A", 8)
	defer cancelA()
	chB, cancelB := b.Subscribe("wf-B", 8)
	defer cancelB()

	b.Publish(agent.NewNodeStarted("wf-A", "n1", ""))
	b.Publish(agent.NewNodeStarted("wf-B", "n2", ""))
	b.Publish(agent.NewNodeStarted("wf-A", "n3", ""))

	// chA should see wf-A events only (2 of them).
	received := drain(t, chA, 2, 200*time.Millisecond)
	require.Len(t, received, 2)
	require.Equal(t, "n1", received[0].(agent.NodeStarted).NodeID)
	require.Equal(t, "n3", received[1].(agent.NodeStarted).NodeID)

	// chB should see wf-B events only (1).
	received = drain(t, chB, 1, 200*time.Millisecond)
	require.Len(t, received, 1)
	require.Equal(t, "n2", received[0].(agent.NodeStarted).NodeID)
}

func TestMemoryBroker_SubscribeAllSeesEverything(t *testing.T) {
	b := agent.NewMemoryBroker()
	defer b.Close()

	chAll, cancel := b.SubscribeAll(16)
	defer cancel()

	b.Publish(agent.NewNodeStarted("wf-A", "n1", ""))
	b.Publish(agent.NewNodeStarted("wf-B", "n2", ""))

	received := drain(t, chAll, 2, 200*time.Millisecond)
	require.Len(t, received, 2)
}

func TestMemoryBroker_DropsOnSlowSubscriber(t *testing.T) {
	b := agent.NewMemoryBroker()
	defer b.Close()

	// Tiny buffer of 2. Send 10 events without draining. The slow
	// subscriber should not block the publisher.
	_, cancel := b.Subscribe("wf-slow", 2)
	defer cancel()

	for i := 0; i < 10; i++ {
		b.Publish(agent.NewNodeStarted("wf-slow", "n", ""))
	}

	published, dropped := b.Stats()
	require.EqualValues(t, 10, published)
	require.Greater(t, dropped, int64(0), "some events should have been dropped")
}

func TestMemoryBroker_CancelUnsubscribes(t *testing.T) {
	b := agent.NewMemoryBroker()
	defer b.Close()

	ch, cancel := b.Subscribe("wf", 4)
	cancel() // immediate

	// Publishing should not panic on the now-closed channel.
	b.Publish(agent.NewNodeStarted("wf", "n", ""))

	// Channel should be closed.
	select {
	case _, ok := <-ch:
		require.False(t, ok, "channel should be closed after cancel")
	case <-time.After(100 * time.Millisecond):
		t.Fatal("expected channel close")
	}
}

func TestMemoryBroker_ConcurrentPublishSubscribe(t *testing.T) {
	b := agent.NewMemoryBroker()
	defer b.Close()

	const N = 50
	var wg sync.WaitGroup
	var seen atomic.Int64

	// One subscriber that pulls events.
	ch, cancel := b.Subscribe("wf", 256)
	defer cancel()
	go func() {
		for range ch {
			seen.Add(1)
		}
	}()

	// Many concurrent publishers.
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				b.Publish(agent.NewNodeStarted("wf", "n", ""))
			}
		}()
	}
	wg.Wait()
	// Give the subscriber a moment to drain.
	time.Sleep(100 * time.Millisecond)

	require.Greater(t, seen.Load(), int64(0))
	pub, _ := b.Stats()
	require.EqualValues(t, N*10, pub)
}

func TestMemoryBroker_CloseClosesAllSubscribers(t *testing.T) {
	b := agent.NewMemoryBroker()

	chA, _ := b.Subscribe("wf-A", 4)
	chB, _ := b.Subscribe("wf-B", 4)
	chAll, _ := b.SubscribeAll(4)

	b.Close()

	for name, ch := range map[string]<-chan agent.Event{"A": chA, "B": chB, "all": chAll} {
		select {
		case _, ok := <-ch:
			require.False(t, ok, "subscriber %s should be closed", name)
		case <-time.After(100 * time.Millisecond):
			t.Fatalf("subscriber %s not closed within timeout", name)
		}
	}
}

// --- Emitter --------------------------------------------------------------

func TestEmitter_StampsWorkflowIDOnEmptyEvents(t *testing.T) {
	b := agent.NewMemoryBroker()
	defer b.Close()

	ch, cancel := b.Subscribe("wf-1", 4)
	defer cancel()

	emitter := agent.NewEmitter(b, "wf-1")
	// Emit an event with empty WorkflowID; the emitter should stamp it.
	emitter.Emit(agent.NodeStarted{NodeID: "n1"}) // intentionally empty WID

	events := drain(t, ch, 1, 200*time.Millisecond)
	require.Len(t, events, 1)
	require.Equal(t, "wf-1", events[0].WorkflowID(), "emitter should stamp WID")
}

func TestEmitter_NoopWhenNil(t *testing.T) {
	// A nil emitter is fine — Emit becomes a no-op. Important for code
	// paths that don't have a broker wired up (tests, demos).
	var e *agent.Emitter
	require.NotPanics(t, func() {
		e.Emit(agent.NewNodeStarted("wf", "n", ""))
	})
}

func TestEmitter_ContextRoundtrip(t *testing.T) {
	b := agent.NewMemoryBroker()
	defer b.Close()

	emitter := agent.NewEmitter(b, "wf")
	ctx := agent.WithEmitter(context.Background(), emitter)

	got := agent.EmitterFromContext(ctx)
	require.NotNil(t, got)
	require.Equal(t, "wf", got.WorkflowID())
}

func TestEmitter_NoEmitterInContextReturnsNoop(t *testing.T) {
	// EmitterFromContext should always return a usable *Emitter even
	// when none was attached — that's the contract that lets activities
	// call EmitterFromContext(ctx).Emit(...) without nil checks.
	e := agent.EmitterFromContext(context.Background())
	require.NotNil(t, e)
	require.NotPanics(t, func() {
		e.Emit(agent.NewNodeStarted("x", "y", ""))
	})
}

// --- Integration: DAG execution emits node events --------------------------

func TestDAGExecute_EmitsNodeLifecycleEvents(t *testing.T) {
	b := agent.NewMemoryBroker()
	defer b.Close()

	ch, cancel := b.Subscribe("wf-dag", 32)
	defer cancel()

	dag := agent.NewDAG()
	agent.AddTypedNode(dag, "a",
		weft.Arrow[int, int](func(_ context.Context, n int) (int, error) {
			return n + 1, nil
		}),
		func(_ agent.NodeInputs) (int, error) { return 1, nil },
	)
	agent.AddTypedNode(dag, "b",
		weft.Arrow[int, int](func(_ context.Context, n int) (int, error) {
			return n * 2, nil
		}),
		func(in agent.NodeInputs) (int, error) { return in.MustGet("a").(int), nil },
		agent.DependsOn("a"),
	)

	compiled, err := dag.Compile()
	require.NoError(t, err)

	ctx := agent.WithEmitter(context.Background(), agent.NewEmitter(b, "wf-dag"))
	results, err := compiled.Execute(ctx, nil)
	require.NoError(t, err)
	require.Equal(t, 4, results["b"]) // (1+1)*2

	// Expect 4 events: a started, a completed, b started, b completed.
	events := drain(t, ch, 4, 500*time.Millisecond)
	require.Len(t, events, 4)
	require.Equal(t, agent.EventKindNodeStarted, events[0].Kind())
	require.Equal(t, agent.EventKindNodeCompleted, events[1].Kind())
	require.Equal(t, agent.EventKindNodeStarted, events[2].Kind())
	require.Equal(t, agent.EventKindNodeCompleted, events[3].Kind())
}

func TestDAGExecute_EmitsNodeFailedOnError(t *testing.T) {
	b := agent.NewMemoryBroker()
	defer b.Close()

	ch, cancel := b.Subscribe("wf-fail", 16)
	defer cancel()

	dag := agent.NewDAG()
	agent.AddTypedNode(dag, "bad",
		weft.Arrow[any, any](func(_ context.Context, _ any) (any, error) {
			return nil, errors.New("planned failure")
		}),
		func(_ agent.NodeInputs) (any, error) { return nil, nil },
	)
	compiled, err := dag.Compile()
	require.NoError(t, err)

	ctx := agent.WithEmitter(context.Background(), agent.NewEmitter(b, "wf-fail"))
	_, err = compiled.Execute(ctx, nil)
	require.Error(t, err)

	events := drain(t, ch, 2, 500*time.Millisecond)
	require.Len(t, events, 2)
	require.Equal(t, agent.EventKindNodeStarted, events[0].Kind())
	require.Equal(t, agent.EventKindNodeFailed, events[1].Kind())
	failed := events[1].(agent.NodeFailed)
	require.Contains(t, failed.Error, "planned failure")
}

// --- Integration: ToolAgent emits tool events -----------------------------

type counterTool struct {
	calls atomic.Int64
}

func (c *counterTool) Name() string        { return "counter" }
func (c *counterTool) Description() string { return "counts" }
func (c *counterTool) ArgsSchema() agent.ToolArgsSchema {
	return agent.ToolArgsSchema{}
}
func (c *counterTool) Run(_ context.Context, _ map[string]any) (string, error) {
	n := c.calls.Add(1)
	return "called " + string(rune('0'+int32(n))), nil
}

func TestToolAgent_EmitsToolCalledAndCompleted(t *testing.T) {
	b := agent.NewMemoryBroker()
	defer b.Close()

	ch, cancel := b.Subscribe("wf-tool", 16)
	defer cancel()

	registry := agent.NewToolRegistry()
	registry.MustRegister(&counterTool{})

	step := 0
	llm := agent.CompleteFunc(func(_ context.Context, _, _ string) (string, error) {
		step++
		if step == 1 {
			return `{"action": "tool", "tool": "counter", "args": {}}`, nil
		}
		return `{"action": "final", "answer": "done"}`, nil
	})

	arrow := agent.ToolAgentArrow(llm, registry)
	ctx := agent.WithEmitter(context.Background(), agent.NewEmitter(b, "wf-tool"))
	out, err := arrow(ctx, agent.ToolAgentInput{Task: "count", MaxSteps: 5})
	require.NoError(t, err)
	require.True(t, out.Converged)

	events := drain(t, ch, 2, 500*time.Millisecond)
	require.Len(t, events, 2)
	require.Equal(t, agent.EventKindToolCalled, events[0].Kind())
	require.Equal(t, agent.EventKindToolCompleted, events[1].Kind())
	called := events[0].(agent.ToolCalled)
	require.Equal(t, "counter", called.ToolName)
	require.Equal(t, 1, called.Step)
}

// --- StreamFromComplete adapter -------------------------------------------

func TestStreamFromComplete_SingleChunkOutput(t *testing.T) {
	c := agent.CompleteFunc(func(_ context.Context, _, _ string) (string, error) {
		return "hello world", nil
	})
	stream := agent.StreamFromComplete(c)
	require.NotNil(t, stream)

	ch, err := stream(context.Background(), "sys", "user")
	require.NoError(t, err)

	var chunks []agent.TokenChunk
	for chunk := range ch {
		chunks = append(chunks, chunk)
	}
	require.Len(t, chunks, 1)
	require.Equal(t, "hello world", chunks[0].Text)
	require.True(t, chunks[0].Final)
}

func TestStreamFromComplete_NilReturnsNil(t *testing.T) {
	require.Nil(t, agent.StreamFromComplete(nil))
}

// --- CollectStream --------------------------------------------------------

func TestCollectStream_EmitsTokenChunkEvents(t *testing.T) {
	b := agent.NewMemoryBroker()
	defer b.Close()
	ch, cancel := b.Subscribe("wf-stream", 16)
	defer cancel()

	emitter := agent.NewEmitter(b, "wf-stream")

	// Manually construct a stream that emits three chunks.
	streamCh := make(chan agent.TokenChunk, 3)
	streamCh <- agent.TokenChunk{Text: "hello ", Index: 0}
	streamCh <- agent.TokenChunk{Text: "world", Index: 1}
	streamCh <- agent.TokenChunk{Text: "", Index: 2, Final: true}
	close(streamCh)

	text, err := agent.CollectStream(context.Background(), streamCh, emitter, "call-1")
	require.NoError(t, err)
	require.Equal(t, "hello world", text)

	events := drain(t, ch, 2, 200*time.Millisecond)
	require.GreaterOrEqual(t, len(events), 2)
	chunk0 := events[0].(agent.TokenChunkEvent)
	require.Equal(t, "hello ", chunk0.Text)
	require.Equal(t, "call-1", chunk0.CallID)
}

func TestCollectStream_PropagatesError(t *testing.T) {
	streamCh := make(chan agent.TokenChunk, 2)
	streamCh <- agent.TokenChunk{Text: "partial", Index: 0}
	streamCh <- agent.TokenChunk{Index: 1, Final: true, Err: errors.New("upstream broke")}
	close(streamCh)

	text, err := agent.CollectStream(context.Background(), streamCh, nil, "call")
	require.Error(t, err)
	require.Contains(t, err.Error(), "upstream broke")
	require.Equal(t, "partial", text)
}

// --- CompleteWithStreaming ------------------------------------------------

func TestCompleteWithStreaming_PrefersStream(t *testing.T) {
	streamCalled := false
	fallbackCalled := false

	stream := agent.CompleteStreamFunc(func(_ context.Context, _, _ string) (<-chan agent.TokenChunk, error) {
		streamCalled = true
		ch := make(chan agent.TokenChunk, 1)
		ch <- agent.TokenChunk{Text: "streamed", Index: 0, Final: true}
		close(ch)
		return ch, nil
	})
	fallback := agent.CompleteFunc(func(_ context.Context, _, _ string) (string, error) {
		fallbackCalled = true
		return "fallback", nil
	})

	text, err := agent.CompleteWithStreaming(context.Background(), stream, fallback, "sys", "user", "call")
	require.NoError(t, err)
	require.Equal(t, "streamed", text)
	require.True(t, streamCalled)
	require.False(t, fallbackCalled)
}

func TestCompleteWithStreaming_FallsBackWhenNoStream(t *testing.T) {
	fallback := agent.CompleteFunc(func(_ context.Context, _, _ string) (string, error) {
		return "fallback-text", nil
	})

	text, err := agent.CompleteWithStreaming(context.Background(), nil, fallback, "", "", "call")
	require.NoError(t, err)
	require.Equal(t, "fallback-text", text)
}

// --- Helpers --------------------------------------------------------------

// drain pulls up to n events from ch, waiting at most timeout total.
func drain(t *testing.T, ch <-chan agent.Event, n int, timeout time.Duration) []agent.Event {
	t.Helper()
	var out []agent.Event
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for len(out) < n {
		select {
		case ev, ok := <-ch:
			if !ok {
				return out
			}
			out = append(out, ev)
		case <-deadline.C:
			return out
		}
	}
	return out
}
