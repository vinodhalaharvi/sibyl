// Package agent — broker_temporal.go bridges Temporal's activity context
// to the global broker. Kept separate so broker.go stays Temporal-free
// and testable without the SDK in scope.
package agent

import (
	"context"

	"go.temporal.io/sdk/activity"
)

// EmitterForActivity returns the Emitter that should be used inside a
// Temporal activity body. It looks up the workflow ID from Temporal's
// activity.GetInfo(ctx) (if running inside an activity) and pairs it
// with the globally-registered broker.
//
// Fallback chain:
//
//  1. An Emitter explicitly injected via WithEmitter — used (tests do this).
//  2. The global broker + Temporal activity info workflow ID.
//  3. The global broker with no workflow ID (still publishes to SubscribeAll).
//  4. A no-op Emitter (no broker registered at all).
//
// Always returns a usable *Emitter — call sites can write
// `EmitterForActivity(ctx).Emit(...)` without nil checks.
func EmitterForActivity(ctx context.Context) *Emitter {
	// Per-context Emitter takes priority (tests use this).
	if e, ok := ctx.Value(emitterKey{}).(*Emitter); ok && e != nil {
		return e
	}
	b := getGlobalBroker()
	if b == nil {
		return &Emitter{} // no-op
	}
	wid := ""
	// activity.GetInfo panics if we're not inside an activity, so guard
	// with IsActivity.
	if activity.IsActivity(ctx) {
		wid = activity.GetInfo(ctx).WorkflowExecution.ID
	}
	return &Emitter{broker: b, workflowID: wid}
}
