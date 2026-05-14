// Package agent — stream.go defines the streaming LLM seam.
//
// CompleteFunc (in llm.go) returns the full response atomically. That's
// the right primitive for most agent code — atomicity composes well,
// it's easy to test, and it matches what Temporal activities expect.
//
// But for a live UI that shows "Claude is typing...", we also want
// token-level streaming. The streaming seam is a SECOND type, not a
// replacement:
//
//	type CompleteStreamFunc func(ctx, system, user) (<-chan TokenChunk, error)
//
// Backends that support streaming (Anthropic Messages API with
// stream:true, Claude Code CLI with --output-format stream-json)
// implement both. Backends that don't (ScriptedLLM) can be
// auto-wrapped via StreamFromComplete, which "fakes" streaming by
// returning the whole response as a single chunk. That way agent
// code can always speak the streaming protocol without caring whether
// the underlying backend really streams.
//
// IMPORTANT: streaming is NOT used inside Temporal activities directly —
// activities are atomic from Temporal's perspective. Streaming is used
// in the EVENT EMISSION path: the streaming adapter wraps a
// CompleteFunc and pushes TokenChunkEvents to the broker as chunks
// arrive, while still returning the full string to the activity.
// This way Temporal's event history remains clean (one activity = one
// atomic LLM call) while the UI gets live updates.
package agent

import (
	"context"
	"strings"
)

// TokenChunk is one piece of streamed LLM output.
type TokenChunk struct {
	// Text is the chunk's content. May be a single token or many.
	Text string
	// Index is monotonic within a stream (0, 1, 2, ...).
	Index int
	// Final marks the last chunk so consumers can close their state.
	Final bool
	// Err, if set on the final chunk, indicates the stream errored
	// rather than completing normally.
	Err error
}

// CompleteStreamFunc is the streaming counterpart of CompleteFunc.
// Implementations return a channel of chunks that will be closed when
// the stream ends (normally or with an error in the final chunk).
//
// Cancellation: when ctx is cancelled, the returned channel is closed
// promptly with no further chunks. The producer goroutine must respect
// ctx.Done().
type CompleteStreamFunc func(ctx context.Context, system, user string) (<-chan TokenChunk, error)

// StreamingMiddleware composes around CompleteStreamFunc, like
// Middleware composes around CompleteFunc. We don't ship streaming
// middlewares yet (logging/cache/retry are all atomic concepts that
// fit better on CompleteFunc) but the type is here so we can later.
type StreamingMiddleware func(CompleteStreamFunc) CompleteStreamFunc

// StreamFromComplete adapts a non-streaming CompleteFunc into a
// CompleteStreamFunc by emitting the full response as a single chunk
// with Final=true. Useful so agent code can always use the streaming
// protocol regardless of backend.
//
// This is the OPPOSITE of what you usually want — there's no real
// streaming happening. Use only when you need API compatibility.
func StreamFromComplete(c CompleteFunc) CompleteStreamFunc {
	if c == nil {
		return nil
	}
	return func(ctx context.Context, system, user string) (<-chan TokenChunk, error) {
		ch := make(chan TokenChunk, 1)
		go func() {
			defer close(ch)
			text, err := c(ctx, system, user)
			select {
			case ch <- TokenChunk{Text: text, Index: 0, Final: true, Err: err}:
			case <-ctx.Done():
			}
		}()
		return ch, nil
	}
}

// CollectStream consumes a CompleteStreamFunc and returns the
// concatenated text, an Emitter-publishable usage event, and any
// stream error.
//
// Use this inside an activity to turn streaming back into atomic:
// the activity invokes the stream, publishes TokenChunkEvents as
// each chunk arrives, and returns the assembled string. Temporal's
// event history sees one activity call (good); the UI gets live
// chunks (good).
//
// callID identifies this LLM call within the workflow; passed to
// TokenChunkEvent so the UI can disambiguate multiple parallel calls
// (e.g. Researcher and Critic both streaming at once).
func CollectStream(ctx context.Context, stream <-chan TokenChunk, emitter *Emitter, callID string) (string, error) {
	if stream == nil {
		return "", nil
	}
	var b strings.Builder
	for chunk := range stream {
		if chunk.Text != "" {
			b.WriteString(chunk.Text)
			if emitter != nil {
				emitter.Emit(TokenChunkEvent{
					CallID: callID,
					Index:  chunk.Index,
					Text:   chunk.Text,
					Final:  chunk.Final,
				})
			}
		}
		if chunk.Err != nil {
			return b.String(), chunk.Err
		}
	}
	return b.String(), nil
}

// CompleteWithStreaming is the convenience helper for activities that
// want to (a) call the LLM atomically (matching CompleteFunc semantics)
// while (b) emitting TokenChunkEvents for the UI.
//
// If stream is non-nil, the call streams; chunks are emitted to the
// emitter (resolved from ctx) and assembled into the returned string.
// If stream is nil, falls back to the atomic CompleteFunc with no
// per-token events emitted.
//
// callID disambiguates parallel LLM calls in the UI.
func CompleteWithStreaming(
	ctx context.Context,
	stream CompleteStreamFunc,
	fallback CompleteFunc,
	system, user, callID string,
) (string, error) {
	emitter := EmitterFromContext(ctx)
	if stream != nil {
		ch, err := stream(ctx, system, user)
		if err != nil {
			return "", err
		}
		return CollectStream(ctx, ch, emitter, callID)
	}
	if fallback != nil {
		out, err := fallback(ctx, system, user)
		// Even without true streaming, emit a single chunk event so the
		// UI sees something. Marked Final immediately.
		if emitter != nil {
			emitter.Emit(TokenChunkEvent{
				CallID: callID,
				Index:  0,
				Text:   out,
				Final:  true,
			})
		}
		return out, err
	}
	return "", nil
}
