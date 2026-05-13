package agent_test

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/vinodhalaharvi/sibyl/agent"
)

// We exercise the decomposer via the Activities.Decompose entry point so
// we're testing the same code path the workflow uses, while keeping the
// underlying arrows package-private.

func TestDecompose_NoSplitMarkers(t *testing.T) {
	acts := &agent.Activities{}
	out, err := acts.Decompose(context.Background(), agent.DecomposeInput{
		Question: "What is the capital of France?",
	})
	require.NoError(t, err)
	require.Len(t, out, 1)
	require.Equal(t, "What is the capital of France?", out[0].Text)
	require.Equal(t, 0, out[0].Index)
}

func TestDecompose_AndSplit(t *testing.T) {
	acts := &agent.Activities{}
	out, err := acts.Decompose(context.Background(), agent.DecomposeInput{
		Question: "What is Go and how does it compare to Rust",
	})
	require.NoError(t, err)
	require.Len(t, out, 2)
	require.Equal(t, "What is Go", out[0].Text)
	require.Equal(t, "how does it compare to Rust", out[1].Text)
}

func TestDecompose_MultipleQuestionMarks(t *testing.T) {
	acts := &agent.Activities{}
	out, err := acts.Decompose(context.Background(), agent.DecomposeInput{
		Question: "What is Go? Why was it created? Who designed it?",
	})
	require.NoError(t, err)
	require.Len(t, out, 3)
	require.Equal(t, "What is Go?", out[0].Text)
	require.Equal(t, "Why was it created?", out[1].Text)
	require.Equal(t, "Who designed it?", out[2].Text)
}

func TestDecompose_VersusSplit(t *testing.T) {
	acts := &agent.Activities{}
	out, err := acts.Decompose(context.Background(), agent.DecomposeInput{
		Question: "Postgres vs MySQL for a side project",
	})
	require.NoError(t, err)
	require.Len(t, out, 2)
}

func TestDecompose_SemicolonSplit(t *testing.T) {
	acts := &agent.Activities{}
	out, err := acts.Decompose(context.Background(), agent.DecomposeInput{
		Question: "Explain channels; explain goroutines; explain select",
	})
	require.NoError(t, err)
	require.Len(t, out, 3)
}

func TestDecompose_MaxSubQuestionsCap(t *testing.T) {
	acts := &agent.Activities{}
	out, err := acts.Decompose(context.Background(), agent.DecomposeInput{
		Question:        "a? b? c? d? e? f?",
		MaxSubQuestions: 3,
	})
	require.NoError(t, err)
	require.Len(t, out, 3, "cap should truncate to 3")
	require.Equal(t, "a?", out[0].Text)
	require.Equal(t, "c?", out[2].Text)
}

func TestDecompose_PreservesIndices(t *testing.T) {
	acts := &agent.Activities{}
	out, err := acts.Decompose(context.Background(), agent.DecomposeInput{
		Question: "Is A true? Is B true? Is C true?",
	})
	require.NoError(t, err)
	require.Len(t, out, 3)
	for i, sq := range out {
		require.Equal(t, i, sq.Index, "subquestion at position %d should have Index=%d", i, i)
	}
}

func TestDecompose_EmptyQuestionReturnsEmpty(t *testing.T) {
	acts := &agent.Activities{}
	out, err := acts.Decompose(context.Background(), agent.DecomposeInput{
		Question: "",
	})
	require.NoError(t, err)
	require.Empty(t, out)
}

// --- Synthesizer tests ------------------------------------------------------

func TestSynthesize_HappyPath(t *testing.T) {
	acts := &agent.Activities{}
	out, err := acts.Synthesize(context.Background(), []agent.SubAnswer{
		{SubQuestion: agent.SubQuestion{Index: 0, Text: "What is Go?"}, Answer: "A programming language.", Converged: true},
		{SubQuestion: agent.SubQuestion{Index: 1, Text: "Who made it?"}, Answer: "Google.", Converged: true},
	})
	require.NoError(t, err)
	require.Contains(t, out, "What is Go?")
	require.Contains(t, out, "A programming language.")
	require.Contains(t, out, "Who made it?")
	require.Contains(t, out, "Google.")
}

func TestSynthesize_IncludesFailureFootnote(t *testing.T) {
	acts := &agent.Activities{}
	out, err := acts.Synthesize(context.Background(), []agent.SubAnswer{
		{SubQuestion: agent.SubQuestion{Index: 0, Text: "OK Q"}, Answer: "OK A", Converged: true},
		{SubQuestion: agent.SubQuestion{Index: 1, Text: "Bad Q"}, Error: "child timed out"},
	})
	require.NoError(t, err)
	require.Contains(t, out, "OK A")
	require.Contains(t, out, "could not be answered")
	require.Contains(t, out, "Bad Q")
	require.Contains(t, out, "child timed out")
}

func TestSynthesize_AllFailures(t *testing.T) {
	acts := &agent.Activities{}
	out, err := acts.Synthesize(context.Background(), []agent.SubAnswer{
		{SubQuestion: agent.SubQuestion{Index: 0, Text: "Q1"}, Error: "boom"},
		{SubQuestion: agent.SubQuestion{Index: 1, Text: "Q2"}, Error: "kaboom"},
	})
	require.NoError(t, err)
	// Output should still be non-empty — it's the failure footnote.
	require.NotEmpty(t, out)
	require.True(t, strings.Contains(out, "Q1") && strings.Contains(out, "Q2"))
}

func TestSynthesize_Empty(t *testing.T) {
	acts := &agent.Activities{}
	out, err := acts.Synthesize(context.Background(), nil)
	require.NoError(t, err)
	require.Empty(t, out)
}
