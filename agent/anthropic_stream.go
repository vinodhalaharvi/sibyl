package agent

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// CompleteStream sends a streaming Messages API request and returns a
// channel of TokenChunk deltas as they arrive. Implements CompleteStreamFunc
// as a method value: pass `client.CompleteStream` anywhere a CompleteStreamFunc
// is expected.
//
// Anthropic's SSE format (https://docs.claude.com/en/api/messages-streaming):
//
//	event: content_block_delta
//	data: {"type":"content_block_delta","delta":{"type":"text_delta","text":"Hello"}}
//
//	event: message_stop
//	data: {"type":"message_stop"}
//
// We only care about content_block_delta events with text_delta payloads.
// All other event types (message_start, ping, content_block_start, etc) are
// ignored — we don't yet emit usage events from streamed responses (the
// usage block is in message_delta toward the end if you want to add it).
func (a *AnthropicClient) CompleteStream(ctx context.Context, systemPrompt, userMessage string) (<-chan TokenChunk, error) {
	reqBody := struct {
		Model     string             `json:"model"`
		MaxTokens int                `json:"max_tokens"`
		System    string             `json:"system,omitempty"`
		Messages  []anthropicMessage `json:"messages"`
		Stream    bool               `json:"stream"`
	}{
		Model:     a.cfg.Model,
		MaxTokens: a.cfg.MaxTokens,
		System:    systemPrompt,
		Messages: []anthropicMessage{
			{Role: "user", Content: userMessage},
		},
		Stream: true,
	}

	body, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("anthropic stream: marshal request: %w", err)
	}

	url := a.cfg.BaseURL + "/v1/messages"
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("anthropic stream: build request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "text/event-stream")
	httpReq.Header.Set("x-api-key", a.cfg.APIKey)
	httpReq.Header.Set("anthropic-version", a.cfg.Version)

	resp, err := a.cfg.HTTPClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("anthropic stream: http call: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		// On error status, read the (small) body for diagnostics and close.
		errBody, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		var env anthropicErrorEnvelope
		if jerr := json.Unmarshal(errBody, &env); jerr == nil && env.Error.Message != "" {
			return nil, fmt.Errorf("anthropic stream: %s (status %d): %s",
				env.Error.Type, resp.StatusCode, env.Error.Message)
		}
		return nil, fmt.Errorf("anthropic stream: status %d: %s", resp.StatusCode, string(errBody))
	}

	ch := make(chan TokenChunk, 32)

	go func() {
		defer resp.Body.Close()
		defer close(ch)

		scanner := bufio.NewScanner(resp.Body)
		// Anthropic deltas are small but we allow a generous buffer in case
		// of large message_start chunks that include accumulated content.
		scanner.Buffer(make([]byte, 0, 8*1024), 1024*1024)

		var index int
		for scanner.Scan() {
			line := scanner.Text()

			// SSE lines look like:
			//   data: {"type":"content_block_delta",...}
			//   <blank line>
			// We only care about the data: lines.
			if !strings.HasPrefix(line, "data:") {
				continue
			}
			payload := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
			if payload == "" || payload == "[DONE]" {
				continue
			}

			var ev anthropicStreamEvent
			if err := json.Unmarshal([]byte(payload), &ev); err != nil {
				// Malformed event — skip it rather than failing the whole
				// stream. Anthropic occasionally inserts non-JSON pings.
				continue
			}

			switch ev.Type {
			case "content_block_delta":
				if ev.Delta != nil && ev.Delta.Type == "text_delta" && ev.Delta.Text != "" {
					select {
					case ch <- TokenChunk{Text: ev.Delta.Text, Index: index, Final: false}:
						index++
					case <-ctx.Done():
						return
					}
				}
			case "message_stop":
				// Send a final empty chunk to mark stream end clearly.
				select {
				case ch <- TokenChunk{Index: index, Final: true}:
				case <-ctx.Done():
				}
				return
			case "error":
				// Anthropic can send an error event mid-stream (rate limit etc).
				msg := "unknown stream error"
				if ev.ErrorMessage != "" {
					msg = ev.ErrorMessage
				}
				select {
				case ch <- TokenChunk{Index: index, Final: true, Err: fmt.Errorf("anthropic stream: %s", msg)}:
				case <-ctx.Done():
				}
				return
			default:
				// message_start, content_block_start, content_block_stop,
				// ping, message_delta — currently ignored. Add handlers
				// here later if you want usage/stop_reason events.
			}
		}

		if err := scanner.Err(); err != nil {
			select {
			case ch <- TokenChunk{Index: index, Final: true, Err: fmt.Errorf("anthropic stream: read: %w", err)}:
			case <-ctx.Done():
			}
		}
	}()

	return ch, nil
}

// anthropicStreamEvent models the union of event payloads we care about.
// Fields not in the current event are zero-valued.
type anthropicStreamEvent struct {
	Type  string                     `json:"type"`
	Delta *anthropicStreamEventDelta `json:"delta,omitempty"`
	// For "error" events:
	ErrorMessage string `json:"message,omitempty"`
}

type anthropicStreamEventDelta struct {
	Type string `json:"type"` // "text_delta" is the one we use
	Text string `json:"text"`
}
