// Package agent — broker.go provides an in-memory pub/sub for events
// keyed by WorkflowID.
//
// Design:
//
//   - Subscribers register against a workflow ID and receive a channel
//     of events scoped to that workflow.
//   - Publishers (typically through an Emitter) call Publish; the
//     broker fans the event out to all matching subscribers.
//   - Subscriber channels are buffered. If a subscriber's buffer fills,
//     events are DROPPED, not blocked. This is deliberate: a slow UI
//     should never slow down the agent.
//
// The default in-process broker is fine for hackathon demos and local
// dev. For multi-process deployments (workers across pods), swap the
// implementation with one backed by Redis Streams, NATS, or similar.
// The Broker interface stays the same.
package agent

import (
	"context"
	"sync"
	"sync/atomic"
)

// Broker is the pub/sub interface for events. Implementations must be
// safe for concurrent use.
type Broker interface {
	// Publish broadcasts an event to all subscribers matching the
	// event's WorkflowID. Never blocks; drops events for slow subscribers.
	Publish(event Event)

	// Subscribe returns a channel of events scoped to workflowID, plus
	// a cancel function that unsubscribes and closes the channel.
	// The buffer parameter controls the channel buffer size (typically
	// 64-256 — large enough for normal bursts, small enough to drop
	// events rather than balloon memory on a hung subscriber).
	Subscribe(workflowID string, buffer int) (<-chan Event, func())

	// SubscribeAll returns a channel of ALL events regardless of
	// workflow ID. Useful for global observers (metrics, logging).
	SubscribeAll(buffer int) (<-chan Event, func())

	// Close stops the broker and unblocks/closes all subscriber channels.
	Close()
}

// MemoryBroker is the in-process default Broker. Concurrent-safe via mutex.
type MemoryBroker struct {
	mu sync.RWMutex

	// per-workflow subscribers
	workflowSubs map[string][]*subscription

	// all-workflow subscribers
	allSubs []*subscription

	// metric counters (atomic, lock-free)
	published atomic.Int64
	dropped   atomic.Int64

	closed atomic.Bool
}

type subscription struct {
	ch     chan Event
	cancel func()
}

// NewMemoryBroker returns a fresh in-process broker.
func NewMemoryBroker() *MemoryBroker {
	return &MemoryBroker{
		workflowSubs: make(map[string][]*subscription),
	}
}

// Publish broadcasts event to all matching subscribers. Never blocks.
func (b *MemoryBroker) Publish(event Event) {
	if b.closed.Load() {
		return
	}
	b.published.Add(1)
	wid := event.WorkflowID()

	b.mu.RLock()
	wfSubs := b.workflowSubs[wid] // may be nil
	allSubs := b.allSubs
	// Snapshot the slices while holding the read lock; release before
	// sending so a slow channel can't block other publishers.
	wfSnapshot := append([]*subscription{}, wfSubs...)
	allSnapshot := append([]*subscription{}, allSubs...)
	b.mu.RUnlock()

	for _, sub := range wfSnapshot {
		select {
		case sub.ch <- event:
		default:
			b.dropped.Add(1) // buffer full; drop and keep going
		}
	}
	for _, sub := range allSnapshot {
		select {
		case sub.ch <- event:
		default:
			b.dropped.Add(1)
		}
	}
}

// Subscribe registers a subscriber for events on workflowID.
//
// The returned cancel function unsubscribes and closes the channel.
// Callers MUST call cancel when done (typically via defer) to avoid
// leaking goroutines/channels.
func (b *MemoryBroker) Subscribe(workflowID string, buffer int) (<-chan Event, func()) {
	if buffer <= 0 {
		buffer = 64
	}
	ch := make(chan Event, buffer)
	sub := &subscription{ch: ch}

	b.mu.Lock()
	b.workflowSubs[workflowID] = append(b.workflowSubs[workflowID], sub)
	b.mu.Unlock()

	var once sync.Once
	cancel := func() {
		once.Do(func() {
			b.mu.Lock()
			subs := b.workflowSubs[workflowID]
			for i, s := range subs {
				if s == sub {
					b.workflowSubs[workflowID] = append(subs[:i], subs[i+1:]...)
					break
				}
			}
			if len(b.workflowSubs[workflowID]) == 0 {
				delete(b.workflowSubs, workflowID)
			}
			b.mu.Unlock()
			close(ch)
		})
	}
	sub.cancel = cancel
	return ch, cancel
}

// SubscribeAll registers a global subscriber for ALL events.
func (b *MemoryBroker) SubscribeAll(buffer int) (<-chan Event, func()) {
	if buffer <= 0 {
		buffer = 256
	}
	ch := make(chan Event, buffer)
	sub := &subscription{ch: ch}

	b.mu.Lock()
	b.allSubs = append(b.allSubs, sub)
	b.mu.Unlock()

	var once sync.Once
	cancel := func() {
		once.Do(func() {
			b.mu.Lock()
			for i, s := range b.allSubs {
				if s == sub {
					b.allSubs = append(b.allSubs[:i], b.allSubs[i+1:]...)
					break
				}
			}
			b.mu.Unlock()
			close(ch)
		})
	}
	sub.cancel = cancel
	return ch, cancel
}

// Close shuts down the broker and closes all subscriber channels.
// Safe to call multiple times.
func (b *MemoryBroker) Close() {
	if !b.closed.CompareAndSwap(false, true) {
		return
	}
	// Snapshot subscribers under the lock, then close channels OUTSIDE
	// the lock so subscriber cancel funcs (which acquire the lock) can't
	// deadlock with us.
	b.mu.Lock()
	var allSubs []*subscription
	for _, subs := range b.workflowSubs {
		allSubs = append(allSubs, subs...)
	}
	allSubs = append(allSubs, b.allSubs...)
	b.workflowSubs = make(map[string][]*subscription)
	b.allSubs = nil
	b.mu.Unlock()

	for _, s := range allSubs {
		// Close the channel directly. Don't call s.cancel() — its inner
		// sync.Once + map mutation would race with our reset above.
		// closing a channel from outside the producer is fine because
		// our Publish path is guarded by the closed flag.
		safeClose(s.ch)
	}
}

// safeClose closes ch unless already closed. Recovering from "close of
// closed channel" panic is the simplest correct pattern when multiple
// shutdown paths may close the same channel.
func safeClose(ch chan Event) {
	defer func() { _ = recover() }()
	close(ch)
}

// Stats reports broker counters. Useful for the worker's shutdown log
// and a future /metrics endpoint.
func (b *MemoryBroker) Stats() (published, dropped int64) {
	return b.published.Load(), b.dropped.Load()
}

// --- Emitter --------------------------------------------------------------

// Emitter is the publish-side facade: it knows the broker and the
// current workflow ID, so callers can `emitter.Emit(NewNodeStarted(...))`
// without repeating either piece of state.
//
// Pass an Emitter through context (via WithEmitter) so activities,
// arrows, and tools can publish events without needing to know about
// the broker directly.
type Emitter struct {
	broker     Broker
	workflowID string
}

// NewEmitter returns an Emitter bound to a workflow and broker.
func NewEmitter(broker Broker, workflowID string) *Emitter {
	return &Emitter{broker: broker, workflowID: workflowID}
}

// Emit publishes an event. If the event's WorkflowID is empty, the
// emitter's workflowID is stamped automatically (typical case). If
// the event already has a WorkflowID, it is respected — useful when
// child workflows want to emit on behalf of a parent (rare).
func (e *Emitter) Emit(event Event) {
	if e == nil || e.broker == nil {
		return
	}
	// Stamp workflow ID if missing. This requires reflection-free
	// access; we do it by checking and re-wrapping the eventBase
	// where applicable. Concrete event types embed eventBase, so
	// they all support WorkflowID() — if it's empty, we use ours.
	if event.WorkflowID() == "" {
		event = withWorkflowID(event, e.workflowID)
	}
	e.broker.Publish(event)
}

// WorkflowID returns the workflow ID this emitter is bound to.
func (e *Emitter) WorkflowID() string {
	if e == nil {
		return ""
	}
	return e.workflowID
}

// withWorkflowID returns the event with WorkflowID set on its eventBase.
// We only need to handle the concrete types we ship — new event types
// added later should be appended here. Failing that, the event flows
// through with an empty WorkflowID, which means it'll still be delivered
// to SubscribeAll subscribers but not to workflow-specific ones.
func withWorkflowID(event Event, wid string) Event {
	switch e := event.(type) {
	case WorkflowStarted:
		e.WID = wid
		return e
	case WorkflowCompleted:
		e.WID = wid
		return e
	case WorkflowFailed:
		e.WID = wid
		return e
	case NodeStarted:
		e.WID = wid
		return e
	case NodeCompleted:
		e.WID = wid
		return e
	case NodeFailed:
		e.WID = wid
		return e
	case ActivityStarted:
		e.WID = wid
		return e
	case ActivityCompleted:
		e.WID = wid
		return e
	case TokenChunkEvent:
		e.WID = wid
		return e
	case ToolCalled:
		e.WID = wid
		return e
	case ToolCompleted:
		e.WID = wid
		return e
	case TokenUsageEvent:
		e.WID = wid
		return e
	}
	return event
}

// --- Context plumbing -----------------------------------------------------

type emitterKey struct{}

// WithEmitter returns a context carrying the given Emitter. Activities,
// arrows, tools, and streaming LLM adapters all retrieve the emitter
// via EmitterFromContext.
//
// Using context for emission is normally a code smell, but for
// cross-cutting observability that touches every layer it's the
// pragmatic choice. The alternative — threading an Emitter through
// every function signature — would be noisy and break weft.Arrow's
// uniform signature.
func WithEmitter(ctx context.Context, emitter *Emitter) context.Context {
	return context.WithValue(ctx, emitterKey{}, emitter)
}

// EmitterFromContext returns the Emitter stored in ctx, or a no-op
// Emitter if none is set. Always returns a usable *Emitter — callers
// can `EmitterFromContext(ctx).Emit(...)` without nil checks.
func EmitterFromContext(ctx context.Context) *Emitter {
	e, ok := ctx.Value(emitterKey{}).(*Emitter)
	if !ok || e == nil {
		return &Emitter{} // no-op
	}
	return e
}
