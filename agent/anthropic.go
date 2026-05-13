package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

// AnthropicConfig configures an AnthropicClient.
type AnthropicConfig struct {
	// APIKey is the Anthropic API key. If empty, ANTHROPIC_API_KEY is read
	// from the environment.
	APIKey string
	// Model is the Anthropic model identifier, e.g. "claude-sonnet-4-5".
	// Defaults to "claude-sonnet-4-5" if empty.
	Model string
	// MaxTokens is the maximum tokens to generate per call. Defaults to 1024.
	MaxTokens int
	// HTTPClient is the underlying HTTP client. Defaults to a client with a
	// 2-minute timeout.
	HTTPClient *http.Client
	// BaseURL overrides the API endpoint, used in tests. Defaults to
	// "https://api.anthropic.com".
	BaseURL string
	// Version is the anthropic-version header. Defaults to "2023-06-01".
	Version string
}

// AnthropicClient calls the Anthropic Messages API. Its Complete method
// satisfies CompleteFunc via a method value.
type AnthropicClient struct {
	cfg AnthropicConfig
}

// NewAnthropicClient validates the config and returns a client. If APIKey is
// empty and ANTHROPIC_API_KEY is not set, it returns an error.
func NewAnthropicClient(cfg AnthropicConfig) (*AnthropicClient, error) {
	if cfg.APIKey == "" {
		cfg.APIKey = os.Getenv("ANTHROPIC_API_KEY")
	}
	if cfg.APIKey == "" {
		return nil, errors.New("AnthropicClient: APIKey not set (and ANTHROPIC_API_KEY env var is empty)")
	}
	if cfg.Model == "" {
		cfg.Model = "claude-sonnet-4-5"
	}
	if cfg.MaxTokens <= 0 {
		cfg.MaxTokens = 1024
	}
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = &http.Client{Timeout: 2 * time.Minute}
	}
	if cfg.BaseURL == "" {
		cfg.BaseURL = "https://api.anthropic.com"
	}
	if cfg.Version == "" {
		cfg.Version = "2023-06-01"
	}
	return &AnthropicClient{cfg: cfg}, nil
}

// Wire types for the Messages API. We model only what we use.

type anthropicMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type anthropicRequest struct {
	Model     string             `json:"model"`
	MaxTokens int                `json:"max_tokens"`
	System    string             `json:"system,omitempty"`
	Messages  []anthropicMessage `json:"messages"`
}

type anthropicContentBlock struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

type anthropicResponse struct {
	Content    []anthropicContentBlock `json:"content"`
	StopReason string                  `json:"stop_reason"`
	Model      string                  `json:"model"`
}

type anthropicError struct {
	Type    string `json:"type"`
	Message string `json:"message"`
}

type anthropicErrorEnvelope struct {
	Error anthropicError `json:"error"`
}

// Complete sends a single user message to the Anthropic Messages API and
// returns the assistant's text. Satisfies CompleteFunc as a method value.
func (a *AnthropicClient) Complete(ctx context.Context, systemPrompt, userMessage string) (string, error) {
	reqBody := anthropicRequest{
		Model:     a.cfg.Model,
		MaxTokens: a.cfg.MaxTokens,
		System:    systemPrompt,
		Messages: []anthropicMessage{
			{Role: "user", Content: userMessage},
		},
	}

	body, err := json.Marshal(reqBody)
	if err != nil {
		return "", fmt.Errorf("anthropic: marshal request: %w", err)
	}

	url := a.cfg.BaseURL + "/v1/messages"
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("anthropic: build request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("x-api-key", a.cfg.APIKey)
	httpReq.Header.Set("anthropic-version", a.cfg.Version)

	resp, err := a.cfg.HTTPClient.Do(httpReq)
	if err != nil {
		return "", fmt.Errorf("anthropic: http call: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("anthropic: read response: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var env anthropicErrorEnvelope
		if jerr := json.Unmarshal(respBody, &env); jerr == nil && env.Error.Message != "" {
			return "", fmt.Errorf("anthropic: %s (status %d): %s",
				env.Error.Type, resp.StatusCode, env.Error.Message)
		}
		return "", fmt.Errorf("anthropic: status %d: %s", resp.StatusCode, string(respBody))
	}

	var out anthropicResponse
	if err := json.Unmarshal(respBody, &out); err != nil {
		return "", fmt.Errorf("anthropic: parse response: %w; body: %s", err, string(respBody))
	}

	// Concatenate text blocks. In practice there's almost always exactly one.
	var text bytes.Buffer
	for _, b := range out.Content {
		if b.Type == "text" {
			text.WriteString(b.Text)
		}
	}
	if text.Len() == 0 {
		return "", fmt.Errorf("anthropic: empty content in response (stop_reason=%q)", out.StopReason)
	}
	return text.String(), nil
}
