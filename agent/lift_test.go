package agent_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/vinodhalaharvi/weft/weft"

	"github.com/vinodhalaharvi/sibyl/agent"
)

func TestCompleteAsArrow_RoundTrip(t *testing.T) {
	// A CompleteFunc, lifted to an Arrow, called via the Arrow interface,
	// should produce the same result as calling the CompleteFunc directly.
	var seenSys, seenUser string
	c := agent.CompleteFunc(func(_ context.Context, sys, user string) (string, error) {
		seenSys, seenUser = sys, user
		return "echo:" + user, nil
	})

	arr := agent.CompleteAsArrow(c)
	out, err := arr(context.Background(), agent.CompletionRequest{
		SystemPrompt: "you are S",
		UserMessage:  "hello",
	})
	require.NoError(t, err)
	require.Equal(t, "echo:hello", out)
	require.Equal(t, "you are S", seenSys)
	require.Equal(t, "hello", seenUser)
}

func TestArrowAsComplete_RoundTrip(t *testing.T) {
	// Inverse of the above — an Arrow lifted down to a CompleteFunc.
	arr := weft.Arrow[agent.CompletionRequest, string](
		func(_ context.Context, r agent.CompletionRequest) (string, error) {
			return r.SystemPrompt + "/" + r.UserMessage, nil
		},
	)

	c := agent.ArrowAsComplete(arr)
	out, err := c(context.Background(), "S", "U")
	require.NoError(t, err)
	require.Equal(t, "S/U", out)
}

func TestCompleteAsArrow_PropagatesErrors(t *testing.T) {
	boom := errors.New("provider down")
	c := agent.CompleteFunc(func(_ context.Context, _, _ string) (string, error) {
		return "", boom
	})

	_, err := agent.CompleteAsArrow(c)(context.Background(), agent.CompletionRequest{})
	require.ErrorIs(t, err, boom)
}

func TestArrowAsActivity_BindsCleanly(t *testing.T) {
	// ArrowAsActivity should produce a function with the exact shape Temporal
	// expects. A simple call-through test is enough to verify the wiring.
	arr := weft.Arrow[int, string](func(_ context.Context, n int) (string, error) {
		if n < 0 {
			return "", errors.New("negative")
		}
		return "ok", nil
	})

	fn := agent.ArrowAsActivity(arr)

	out, err := fn(context.Background(), 42)
	require.NoError(t, err)
	require.Equal(t, "ok", out)

	_, err = fn(context.Background(), -1)
	require.Error(t, err)
}

// TestCompose_LLMPipeline shows the actual pattern callers would use:
// build a composed arrow that does prompt-building + LLM call + parsing,
// then use it as a unit.
func TestCompose_LLMPipeline(t *testing.T) {
	scripted := &agent.ScriptedLLM{Responses: []string{"42"}}
	llm := agent.CompleteAsArrow(scripted.Complete)

	// Stage 1: build a CompletionRequest from a typed input.
	buildReq := weft.Pure(func(q string) agent.CompletionRequest {
		return agent.CompletionRequest{
			SystemPrompt: "answer with just a number",
			UserMessage:  q,
		}
	})

	// Stage 3: parse the string response into an int.
	parse := weft.Arrow[string, int](func(_ context.Context, s string) (int, error) {
		if s == "42" {
			return 42, nil
		}
		return 0, errors.New("unexpected response")
	})

	pipeline := weft.Pipe3(buildReq, llm, parse)

	got, err := pipeline(context.Background(), "what is the answer?")
	require.NoError(t, err)
	require.Equal(t, 42, got)

	// And the LLM saw exactly one call with the right system prompt.
	calls := scripted.Calls()
	require.Len(t, calls, 1)
	require.Equal(t, "answer with just a number", calls[0].SystemPrompt)
	require.Equal(t, "what is the answer?", calls[0].UserMessage)
}
