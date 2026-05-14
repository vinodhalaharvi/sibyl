package agent_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/vinodhalaharvi/sibyl/agent"
)

// TestGlobalBroker_RoundTrip verifies that activities can emit events via
// EmitterForActivity when a global broker is registered, without anyone
// having to call WithEmitter.
func TestGlobalBroker_RoundTrip(t *testing.T) {
	b := agent.NewMemoryBroker()
	defer b.Close()
	agent.SetGlobalBroker(b)
	defer agent.SetGlobalBroker(nil)

	ch, cancel := b.SubscribeAll(8)
	defer cancel()

	// Calling EmitterForActivity outside an activity returns an emitter
	// with no workflow ID but a real broker, so events still publish
	// to SubscribeAll observers.
	e := agent.EmitterForActivity(context.Background())
	e.Emit(agent.NewNodeStarted("", "manual", "manual"))

	// Should be delivered.
	select {
	case ev := <-ch:
		ns, ok := ev.(agent.NodeStarted)
		require.True(t, ok)
		require.Equal(t, "manual", ns.NodeID)
	case <-time.After(200 * time.Millisecond):
		t.Fatal("event not received")
	}
}

func TestGlobalBroker_PerContextEmitterTakesPriority(t *testing.T) {
	// Two brokers: one registered globally, one passed via context.
	// EmitterForActivity should prefer the per-context emitter.
	global := agent.NewMemoryBroker()
	defer global.Close()
	agent.SetGlobalBroker(global)
	defer agent.SetGlobalBroker(nil)

	perCtx := agent.NewMemoryBroker()
	defer perCtx.Close()
	emitter := agent.NewEmitter(perCtx, "wf-ctx")
	ctx := agent.WithEmitter(context.Background(), emitter)

	gCh, gCancel := global.SubscribeAll(4)
	defer gCancel()
	pCh, pCancel := perCtx.SubscribeAll(4)
	defer pCancel()

	e := agent.EmitterForActivity(ctx)
	e.Emit(agent.NewNodeStarted("", "test", "test"))

	// Should be delivered to perCtx, NOT to global.
	select {
	case <-pCh:
		// expected
	case <-time.After(200 * time.Millisecond):
		t.Fatal("per-context broker didn't receive event")
	}

	select {
	case ev := <-gCh:
		t.Fatalf("global broker should not have received event: %v", ev)
	case <-time.After(100 * time.Millisecond):
		// expected — global gets nothing
	}
}

func TestGlobalBroker_NilGlobalReturnsNoop(t *testing.T) {
	// No global broker, no context emitter — should return a no-op
	// Emitter that doesn't panic when used.
	agent.SetGlobalBroker(nil)
	e := agent.EmitterForActivity(context.Background())
	require.NotNil(t, e)
	require.NotPanics(t, func() {
		e.Emit(agent.NewNodeStarted("wf", "n", "l"))
	})
}
