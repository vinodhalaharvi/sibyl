package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
)

// ClaudeCodeConfig configures a ClaudeCodeClient.
type ClaudeCodeConfig struct {
	// Binary is the path or name of the Claude Code executable. Defaults to "claude".
	Binary string
	// Model passes through to `claude --model` if non-empty (e.g. "sonnet", "opus").
	Model string
	// Permission mode passes through to --permission-mode. Defaults to "bypassPermissions"
	// since we never need tool access for plain completions; we just want the model's text.
	PermissionMode string
	// MaxTurns passes through to --max-turns. Defaults to 1: we want one
	// response, not an agent loop inside the CLI.
	MaxTurns int
	// ExtraArgs are appended verbatim to the command line. Use for escape-hatch
	// flags we don't otherwise model.
	ExtraArgs []string
}

// ClaudeCodeClient runs the Claude Code CLI (`claude -p`) as a subprocess and
// returns its output. Authentication relies on the CLI's own configuration —
// either an ANTHROPIC_API_KEY env var or the Pro/Max subscription credentials
// stored locally by `claude` itself. No API key is read by this client.
//
// Its Complete method satisfies CompleteFunc as a method value.
type ClaudeCodeClient struct {
	cfg ClaudeCodeConfig
}

// NewClaudeCodeClient applies defaults to the config and returns a client.
// It does not verify that the binary exists on PATH — the failure surfaces on
// the first Complete call.
func NewClaudeCodeClient(cfg ClaudeCodeConfig) *ClaudeCodeClient {
	if cfg.Binary == "" {
		cfg.Binary = "claude"
	}
	if cfg.PermissionMode == "" {
		cfg.PermissionMode = "bypassPermissions"
	}
	if cfg.MaxTurns == 0 {
		cfg.MaxTurns = 1
	}
	return &ClaudeCodeClient{cfg: cfg}
}

// claudeCodePrintResponse mirrors the JSON shape emitted by
// `claude -p --output-format json`. The CLI returns more fields (cost,
// duration, session_id...); we only need .result.
type claudeCodePrintResponse struct {
	Type    string `json:"type"`
	Subtype string `json:"subtype"`
	IsError bool   `json:"is_error"`
	Result  string `json:"result"`
}

// Complete runs `claude -p <prompt> --append-system-prompt <sys> ...` and
// returns the assistant's text from the parsed JSON output.
func (c *ClaudeCodeClient) Complete(ctx context.Context, systemPrompt, userMessage string) (string, error) {
	if userMessage == "" {
		return "", errors.New("claude-code: empty user message")
	}

	args := []string{
		"-p", userMessage,
		"--output-format", "json",
		"--permission-mode", c.cfg.PermissionMode,
		"--max-turns", fmt.Sprintf("%d", c.cfg.MaxTurns),
	}
	if systemPrompt != "" {
		args = append(args, "--append-system-prompt", systemPrompt)
	}
	if c.cfg.Model != "" {
		args = append(args, "--model", c.cfg.Model)
	}
	args = append(args, c.cfg.ExtraArgs...)

	cmd := exec.CommandContext(ctx, c.cfg.Binary, args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("claude-code: %s failed: %w; stderr: %s",
			c.cfg.Binary, err, stderr.String())
	}

	out := stdout.Bytes()
	if len(out) == 0 {
		return "", fmt.Errorf("claude-code: empty output; stderr: %s", stderr.String())
	}

	var parsed claudeCodePrintResponse
	if err := json.Unmarshal(out, &parsed); err != nil {
		return "", fmt.Errorf("claude-code: parse JSON: %w; raw: %s", err, string(out))
	}
	if parsed.IsError {
		return "", fmt.Errorf("claude-code: CLI reported error: %s", parsed.Result)
	}
	if parsed.Result == "" {
		return "", fmt.Errorf("claude-code: empty result field; raw: %s", string(out))
	}
	return parsed.Result, nil
}
