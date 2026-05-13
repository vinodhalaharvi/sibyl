package agent_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/vinodhalaharvi/sibyl/agent"
)

func TestAnthropicClient_HappyPath(t *testing.T) {
	var gotBody map[string]any
	var gotHeaders http.Header

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodPost, r.Method)
		require.Equal(t, "/v1/messages", r.URL.Path)
		gotHeaders = r.Header.Clone()

		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.NoError(t, json.Unmarshal(body, &gotBody))

		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"content": [{"type": "text", "text": "Hello back."}],
			"stop_reason": "end_turn",
			"model": "claude-sonnet-4-5"
		}`))
	}))
	t.Cleanup(srv.Close)

	c, err := agent.NewAnthropicClient(agent.AnthropicConfig{
		APIKey:  "test-key",
		BaseURL: srv.URL,
		Model:   "claude-sonnet-4-5",
	})
	require.NoError(t, err)

	got, err := c.Complete(context.Background(), "you are a helper", "say hi")
	require.NoError(t, err)
	require.Equal(t, "Hello back.", got)

	// Headers
	require.Equal(t, "test-key", gotHeaders.Get("x-api-key"))
	require.Equal(t, "2023-06-01", gotHeaders.Get("anthropic-version"))
	require.Equal(t, "application/json", gotHeaders.Get("Content-Type"))

	// Body
	require.Equal(t, "claude-sonnet-4-5", gotBody["model"])
	require.Equal(t, "you are a helper", gotBody["system"])
	msgs, ok := gotBody["messages"].([]any)
	require.True(t, ok)
	require.Len(t, msgs, 1)
	first := msgs[0].(map[string]any)
	require.Equal(t, "user", first["role"])
	require.Equal(t, "say hi", first["content"])
}

func TestAnthropicClient_ErrorResponse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error": {"type": "invalid_request_error", "message": "model not found"}}`))
	}))
	t.Cleanup(srv.Close)

	c, err := agent.NewAnthropicClient(agent.AnthropicConfig{
		APIKey: "k", BaseURL: srv.URL,
	})
	require.NoError(t, err)

	_, err = c.Complete(context.Background(), "", "hi")
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid_request_error")
	require.Contains(t, err.Error(), "model not found")
	require.Contains(t, err.Error(), "400")
}

func TestAnthropicClient_NonJSONErrorBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("upstream gateway timeout"))
	}))
	t.Cleanup(srv.Close)

	c, err := agent.NewAnthropicClient(agent.AnthropicConfig{APIKey: "k", BaseURL: srv.URL})
	require.NoError(t, err)

	_, err = c.Complete(context.Background(), "", "hi")
	require.Error(t, err)
	require.Contains(t, err.Error(), "503")
	require.Contains(t, err.Error(), "upstream gateway timeout")
}

func TestAnthropicClient_EmptyContent(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"content": [], "stop_reason": "max_tokens"}`))
	}))
	t.Cleanup(srv.Close)

	c, err := agent.NewAnthropicClient(agent.AnthropicConfig{APIKey: "k", BaseURL: srv.URL})
	require.NoError(t, err)

	_, err = c.Complete(context.Background(), "", "hi")
	require.Error(t, err)
	require.Contains(t, err.Error(), "empty content")
	require.Contains(t, err.Error(), "max_tokens")
}

func TestAnthropicClient_MissingAPIKey(t *testing.T) {
	t.Setenv("ANTHROPIC_API_KEY", "")
	_, err := agent.NewAnthropicClient(agent.AnthropicConfig{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "APIKey not set")
}

func TestAnthropicClient_ReadsKeyFromEnv(t *testing.T) {
	t.Setenv("ANTHROPIC_API_KEY", "env-key")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "env-key", r.Header.Get("x-api-key"))
		_, _ = w.Write([]byte(`{"content":[{"type":"text","text":"ok"}]}`))
	}))
	t.Cleanup(srv.Close)

	c, err := agent.NewAnthropicClient(agent.AnthropicConfig{BaseURL: srv.URL})
	require.NoError(t, err)
	out, err := c.Complete(context.Background(), "", "hi")
	require.NoError(t, err)
	require.Equal(t, "ok", out)
}

// Verifies the method value composes with CompleteFunc seamlessly.
func TestAnthropicClient_CompleteIsCompleteFunc(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"content":[{"type":"text","text":"hi"}]}`))
	}))
	t.Cleanup(srv.Close)

	c, err := agent.NewAnthropicClient(agent.AnthropicConfig{APIKey: "k", BaseURL: srv.URL})
	require.NoError(t, err)

	var fn agent.CompleteFunc = c.Complete // method value -> CompleteFunc
	out, err := fn(context.Background(), "", "hi")
	require.NoError(t, err)
	require.True(t, strings.Contains(out, "hi"))
}
