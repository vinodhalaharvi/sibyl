package agent_test

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/vinodhalaharvi/sibyl/agent"
)

// writeFakeClaude writes a shell script to t.TempDir() that mimics enough of
// `claude -p ... --output-format json` for our purposes. It writes a JSON
// payload to stdout. Any extra behavior (echoing args, exiting nonzero) is
// controlled by the `script` argument, which is shell prepended before the
// canned payload.
func writeFakeClaude(t *testing.T, script string) string {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("fake claude is a POSIX shell script")
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "claude")
	body := "#!/bin/sh\n" + script + "\n"
	require.NoError(t, os.WriteFile(path, []byte(body), 0o755))
	return path
}

func TestClaudeCodeClient_HappyPath(t *testing.T) {
	// Fake CLI: print a JSON response with the expected shape.
	bin := writeFakeClaude(t, `cat <<'EOF'
{"type":"result","subtype":"success","is_error":false,"result":"Paris is the capital of France."}
EOF`)

	c := agent.NewClaudeCodeClient(agent.ClaudeCodeConfig{Binary: bin})
	out, err := c.Complete(context.Background(), "be brief", "what is the capital of France?")
	require.NoError(t, err)
	require.Equal(t, "Paris is the capital of France.", out)
}

func TestClaudeCodeClient_PassesArgs(t *testing.T) {
	// Fake CLI: dump our argv to a sidechannel file so the test can assert on it,
	// then return a minimal valid JSON response.
	argDump := filepath.Join(t.TempDir(), "args.txt")
	bin := writeFakeClaude(t, `printf '%s\n' "$@" > `+argDump+`
cat <<'EOF'
{"type":"result","is_error":false,"result":"ok"}
EOF`)

	c := agent.NewClaudeCodeClient(agent.ClaudeCodeConfig{
		Binary:         bin,
		Model:          "sonnet",
		PermissionMode: "default",
		MaxTurns:       2,
		ExtraArgs:      []string{"--verbose"},
	})
	out, err := c.Complete(context.Background(), "SYS PROMPT", "USER MSG")
	require.NoError(t, err)
	require.Equal(t, "ok", out)

	dumped, err := os.ReadFile(argDump)
	require.NoError(t, err)
	args := string(dumped)

	// Spot-check the args we expect.
	require.Contains(t, args, "-p")
	require.Contains(t, args, "USER MSG")
	require.Contains(t, args, "--output-format")
	require.Contains(t, args, "json")
	require.Contains(t, args, "--permission-mode")
	require.Contains(t, args, "default")
	require.Contains(t, args, "--max-turns")
	require.Contains(t, args, "--append-system-prompt")
	require.Contains(t, args, "SYS PROMPT")
	require.Contains(t, args, "--model")
	require.Contains(t, args, "sonnet")
	require.Contains(t, args, "--verbose")
}

func TestClaudeCodeClient_CLIReturnsErrorJSON(t *testing.T) {
	bin := writeFakeClaude(t, `cat <<'EOF'
{"type":"result","is_error":true,"result":"rate limit hit"}
EOF`)

	c := agent.NewClaudeCodeClient(agent.ClaudeCodeConfig{Binary: bin})
	_, err := c.Complete(context.Background(), "", "hi")
	require.Error(t, err)
	require.Contains(t, err.Error(), "rate limit hit")
}

func TestClaudeCodeClient_NonZeroExit(t *testing.T) {
	bin := writeFakeClaude(t, `echo "boom" >&2
exit 7`)

	c := agent.NewClaudeCodeClient(agent.ClaudeCodeConfig{Binary: bin})
	_, err := c.Complete(context.Background(), "", "hi")
	require.Error(t, err)
	require.Contains(t, err.Error(), "boom")
}

func TestClaudeCodeClient_MalformedJSON(t *testing.T) {
	bin := writeFakeClaude(t, `echo "not even json"`)
	c := agent.NewClaudeCodeClient(agent.ClaudeCodeConfig{Binary: bin})
	_, err := c.Complete(context.Background(), "", "hi")
	require.Error(t, err)
	require.Contains(t, err.Error(), "parse JSON")
}

func TestClaudeCodeClient_EmptyUserMessage(t *testing.T) {
	c := agent.NewClaudeCodeClient(agent.ClaudeCodeConfig{Binary: "/nonexistent"})
	_, err := c.Complete(context.Background(), "sys", "")
	require.Error(t, err)
	require.Contains(t, err.Error(), "empty user message")
}

func TestClaudeCodeClient_CompleteIsCompleteFunc(t *testing.T) {
	bin := writeFakeClaude(t, `cat <<'EOF'
{"type":"result","is_error":false,"result":"hi"}
EOF`)
	c := agent.NewClaudeCodeClient(agent.ClaudeCodeConfig{Binary: bin})
	var fn agent.CompleteFunc = c.Complete
	out, err := fn(context.Background(), "", "hello")
	require.NoError(t, err)
	require.Equal(t, "hi", out)
}
