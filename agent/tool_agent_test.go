package agent_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/vinodhalaharvi/sibyl/agent"
)

// --- ToolRegistry -----------------------------------------------------------

// fakeTool is a minimal Tool used in registry tests.
type fakeTool struct {
	name string
	desc string
	run  func(ctx context.Context, args map[string]any) (string, error)
}

func (f *fakeTool) Name() string        { return f.name }
func (f *fakeTool) Description() string { return f.desc }
func (f *fakeTool) ArgsSchema() agent.ToolArgsSchema {
	return agent.ToolArgsSchema{Args: []agent.ToolArg{{Name: "q", Type: "string", Required: true}}}
}
func (f *fakeTool) Run(ctx context.Context, args map[string]any) (string, error) {
	return f.run(ctx, args)
}

func TestToolRegistry_RegisterAndGet(t *testing.T) {
	r := agent.NewToolRegistry()
	tool := &fakeTool{name: "echo", desc: "echoes"}
	require.NoError(t, r.Register(tool))

	got, ok := r.Get("echo")
	require.True(t, ok)
	require.Equal(t, "echo", got.Name())
}

func TestToolRegistry_RejectsDuplicate(t *testing.T) {
	r := agent.NewToolRegistry()
	require.NoError(t, r.Register(&fakeTool{name: "echo"}))
	err := r.Register(&fakeTool{name: "echo"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "already registered")
}

func TestToolRegistry_RejectsNilOrEmpty(t *testing.T) {
	r := agent.NewToolRegistry()
	require.Error(t, r.Register(nil))
	require.Error(t, r.Register(&fakeTool{name: ""}))
}

func TestToolRegistry_NamesSorted(t *testing.T) {
	r := agent.NewToolRegistry()
	r.MustRegister(&fakeTool{name: "charlie"})
	r.MustRegister(&fakeTool{name: "alpha"})
	r.MustRegister(&fakeTool{name: "bravo"})

	require.Equal(t, []string{"alpha", "bravo", "charlie"}, r.Names())
}

// --- CalculatorTool ---------------------------------------------------------

func TestCalculatorTool_BasicArithmetic(t *testing.T) {
	c := &agent.CalculatorTool{}

	cases := []struct {
		expr string
		want string
	}{
		{"2 + 3", "5"},
		{"10 - 4", "6"},
		{"6 * 7", "42"},
		{"20 / 4", "5"},
		{"(2 + 3) * 4", "20"},
		{"2 + 3 * 4", "14"}, // precedence
		{"-5 + 8", "3"},
		{"10 / 4", "2.5"},
		{"3.14 * 2", "6.28"},
		{"1 + 2 + 3 + 4", "10"},
		{"((1 + 2) * 3) - 4", "5"},
	}

	for _, tc := range cases {
		t.Run(tc.expr, func(t *testing.T) {
			out, err := c.Run(context.Background(), map[string]any{"expression": tc.expr})
			require.NoError(t, err)
			require.Equal(t, tc.want, out)
		})
	}
}

func TestCalculatorTool_DivisionByZero(t *testing.T) {
	c := &agent.CalculatorTool{}
	_, err := c.Run(context.Background(), map[string]any{"expression": "1 / 0"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "division by zero")
}

func TestCalculatorTool_MalformedInput(t *testing.T) {
	c := &agent.CalculatorTool{}
	cases := []string{
		"",
		"1 + ",
		"(2 + 3",
		"foo",
		"1 ** 2",
	}
	for _, expr := range cases {
		t.Run(expr, func(t *testing.T) {
			_, err := c.Run(context.Background(), map[string]any{"expression": expr})
			require.Error(t, err)
		})
	}
}

// --- FileReadTool -----------------------------------------------------------

func TestFileReadTool_ReadsFileInAllowedRoot(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "hello.txt")
	require.NoError(t, os.WriteFile(path, []byte("hello world"), 0o644))

	tool := agent.NewFileReadTool(dir)
	out, err := tool.Run(context.Background(), map[string]any{"path": path})
	require.NoError(t, err)
	require.Equal(t, "hello world", out)
}

func TestFileReadTool_RejectsOutsideAllowedRoot(t *testing.T) {
	dir := t.TempDir()
	tool := agent.NewFileReadTool(dir)
	// /etc/hostname is outside any t.TempDir() — should be denied.
	_, err := tool.Run(context.Background(), map[string]any{"path": "/etc/hostname"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "outside allowed roots")
}

func TestFileReadTool_RejectsPathTraversal(t *testing.T) {
	dir := t.TempDir()
	tool := agent.NewFileReadTool(dir)
	// Try to escape via ..
	traversal := filepath.Join(dir, "..", "..", "etc", "passwd")
	_, err := tool.Run(context.Background(), map[string]any{"path": traversal})
	require.Error(t, err)
}

func TestFileReadTool_TruncatesLargeFiles(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "big.txt")
	// 100 KB content
	content := strings.Repeat("a", 100*1024)
	require.NoError(t, os.WriteFile(path, []byte(content), 0o644))

	tool := agent.NewFileReadTool(dir)
	tool.MaxBytes = 64 * 1024
	out, err := tool.Run(context.Background(), map[string]any{"path": path})
	require.NoError(t, err)
	require.Contains(t, out, "[... file truncated;")
	// Body should be 64KB + truncation marker.
	require.Less(t, len(out), 65*1024+200)
}

func TestFileReadTool_EmptyPathErrors(t *testing.T) {
	tool := agent.NewFileReadTool(t.TempDir())
	_, err := tool.Run(context.Background(), map[string]any{"path": ""})
	require.Error(t, err)
}

// --- ToolAgent loop ---------------------------------------------------------

// scriptedTool is a Tool that returns canned responses, useful for
// testing the agent loop without real I/O.
type scriptedTool struct {
	name      string
	desc      string
	responses []string
	idx       int
	calls     []map[string]any
	failOn    int // index at which to error; -1 disables
}

func (s *scriptedTool) Name() string        { return s.name }
func (s *scriptedTool) Description() string { return s.desc }
func (s *scriptedTool) ArgsSchema() agent.ToolArgsSchema {
	return agent.ToolArgsSchema{Args: []agent.ToolArg{{Name: "q", Type: "string", Required: true}}}
}
func (s *scriptedTool) Run(_ context.Context, args map[string]any) (string, error) {
	s.calls = append(s.calls, args)
	if s.failOn >= 0 && s.idx == s.failOn {
		s.idx++
		return "", errors.New("scripted failure")
	}
	if s.idx >= len(s.responses) {
		return "", errors.New("scripted tool exhausted")
	}
	resp := s.responses[s.idx]
	s.idx++
	return resp, nil
}

func TestToolAgent_SingleStepFinal(t *testing.T) {
	// LLM emits "final" immediately. No tool calls.
	llm := agent.CompleteFunc(func(_ context.Context, _, _ string) (string, error) {
		return `{"action": "final", "reasoning": "I already know this", "answer": "42"}`, nil
	})
	registry := agent.NewToolRegistry()

	arrow := agent.ToolAgentArrow(llm, registry)
	out, err := arrow(context.Background(), agent.ToolAgentInput{
		Task:     "What is 6 * 7?",
		MaxSteps: 5,
	})

	require.NoError(t, err)
	require.True(t, out.Converged)
	require.Equal(t, "42", out.Answer)
	require.Len(t, out.Steps, 1)
	require.Equal(t, "42", out.Steps[0].Final)
}

func TestToolAgent_ToolCallThenFinal(t *testing.T) {
	// Step 1: LLM calls the calculator. Step 2: LLM emits final.
	scripted := &scriptedTool{name: "calc", desc: "calc", responses: []string{"42"}, failOn: -1}
	registry := agent.NewToolRegistry()
	registry.MustRegister(scripted)

	callCount := 0
	llm := agent.CompleteFunc(func(_ context.Context, _, _ string) (string, error) {
		callCount++
		if callCount == 1 {
			return `{"action": "tool", "tool": "calc", "args": {"q": "6*7"}}`, nil
		}
		return `{"action": "final", "answer": "The answer is 42."}`, nil
	})

	arrow := agent.ToolAgentArrow(llm, registry)
	out, err := arrow(context.Background(), agent.ToolAgentInput{
		Task:     "What is 6 * 7?",
		MaxSteps: 5,
	})

	require.NoError(t, err)
	require.True(t, out.Converged)
	require.Equal(t, "The answer is 42.", out.Answer)
	require.Len(t, out.Steps, 2)
	require.Equal(t, "calc", out.Steps[0].ToolName)
	require.Equal(t, "42", out.Steps[0].ToolResult)
	require.Equal(t, "The answer is 42.", out.Steps[1].Final)
}

func TestToolAgent_UnknownToolHandled(t *testing.T) {
	// LLM calls a non-existent tool; agent should record the error
	// in the history and let the LLM see and react. Then the LLM
	// course-corrects and finalizes.
	registry := agent.NewToolRegistry()

	callCount := 0
	llm := agent.CompleteFunc(func(_ context.Context, _, _ string) (string, error) {
		callCount++
		if callCount == 1 {
			return `{"action": "tool", "tool": "nonexistent", "args": {}}`, nil
		}
		return `{"action": "final", "answer": "OK, I gave up on the tool."}`, nil
	})

	arrow := agent.ToolAgentArrow(llm, registry)
	out, err := arrow(context.Background(), agent.ToolAgentInput{
		Task:     "Do a thing",
		MaxSteps: 5,
	})

	require.NoError(t, err)
	require.True(t, out.Converged)
	require.Len(t, out.Steps, 2)
	require.True(t, out.Steps[0].ToolError)
	require.Contains(t, out.Steps[0].ToolResult, "not registered")
}

func TestToolAgent_ToolErrorRecorded(t *testing.T) {
	// Tool returns an error; agent records and LLM gives up.
	scripted := &scriptedTool{name: "flaky", desc: "flaky", responses: []string{}, failOn: 0}
	registry := agent.NewToolRegistry()
	registry.MustRegister(scripted)

	callCount := 0
	llm := agent.CompleteFunc(func(_ context.Context, _, _ string) (string, error) {
		callCount++
		if callCount == 1 {
			return `{"action": "tool", "tool": "flaky", "args": {}}`, nil
		}
		return `{"action": "final", "answer": "Tool failed; I can't answer."}`, nil
	})

	arrow := agent.ToolAgentArrow(llm, registry)
	out, err := arrow(context.Background(), agent.ToolAgentInput{
		Task:     "Try the flaky tool",
		MaxSteps: 5,
	})

	require.NoError(t, err)
	require.Len(t, out.Steps, 2)
	require.True(t, out.Steps[0].ToolError)
	require.Contains(t, out.Steps[0].ToolResult, "scripted failure")
}

func TestToolAgent_HitsMaxStepsWithoutConvergence(t *testing.T) {
	// LLM never emits final. Agent should hit MaxSteps and return
	// Converged: false.
	scripted := &scriptedTool{
		name:      "loop",
		desc:      "loop",
		responses: []string{"r1", "r2", "r3", "r4", "r5"},
		failOn:    -1,
	}
	registry := agent.NewToolRegistry()
	registry.MustRegister(scripted)

	llm := agent.CompleteFunc(func(_ context.Context, _, _ string) (string, error) {
		return `{"action": "tool", "tool": "loop", "args": {}}`, nil
	})

	arrow := agent.ToolAgentArrow(llm, registry)
	out, err := arrow(context.Background(), agent.ToolAgentInput{
		Task:     "Loop forever",
		MaxSteps: 3,
	})

	require.NoError(t, err)
	require.False(t, out.Converged)
	require.Len(t, out.Steps, 3)
}

func TestToolAgent_MalformedJSONFailsFast(t *testing.T) {
	llm := agent.CompleteFunc(func(_ context.Context, _, _ string) (string, error) {
		return "this is not json", nil
	})
	registry := agent.NewToolRegistry()

	arrow := agent.ToolAgentArrow(llm, registry)
	_, err := arrow(context.Background(), agent.ToolAgentInput{
		Task:     "Hi",
		MaxSteps: 5,
	})

	require.Error(t, err)
	require.Contains(t, err.Error(), "malformed decision JSON")
}

func TestToolAgent_NilCompleteRejected(t *testing.T) {
	arrow := agent.ToolAgentArrow(nil, agent.NewToolRegistry())
	_, err := arrow(context.Background(), agent.ToolAgentInput{Task: "x", MaxSteps: 5})
	require.Error(t, err)
}

func TestToolAgent_NilRegistryRejected(t *testing.T) {
	llm := agent.CompleteFunc(func(_ context.Context, _, _ string) (string, error) { return "{}", nil })
	arrow := agent.ToolAgentArrow(llm, nil)
	_, err := arrow(context.Background(), agent.ToolAgentInput{Task: "x", MaxSteps: 5})
	require.Error(t, err)
}

// Verify the activity entry point delegates correctly to the loop.
func TestActivities_RunToolAgent_NilTools(t *testing.T) {
	a := &agent.Activities{Complete: func(_ context.Context, _, _ string) (string, error) { return "{}", nil }}
	_, err := a.RunToolAgent(context.Background(), agent.ToolAgentInput{Task: "x", MaxSteps: 5})
	require.Error(t, err)
	require.Contains(t, err.Error(), "Tools is nil")
}

func TestActivities_RunToolAgent_HappyPath(t *testing.T) {
	a := &agent.Activities{
		Complete: func(_ context.Context, _, _ string) (string, error) {
			return `{"action": "final", "answer": "done"}`, nil
		},
		Tools: agent.NewToolRegistry(),
	}
	out, err := a.RunToolAgent(context.Background(), agent.ToolAgentInput{
		Task:     "x",
		MaxSteps: 1,
	})
	require.NoError(t, err)
	require.True(t, out.Converged)
	require.Equal(t, "done", out.Answer)
}
