package agent

import (
	"context"
	"fmt"
	"strings"

	"github.com/vinodhalaharvi/weft/weft"
)

// decompose.go contains the pure pipeline pieces used by the supervisor:
//
//   - Decompose: split a Question into a list of SubQuestions.
//   - Synthesize: merge a list of SubAnswers into a single final answer.
//
// Both are weft.Arrow values. They are reachable as Temporal activities
// via supervisor.go's adapters; you can also call them directly from
// tests or compose them into larger pipelines without Temporal.
//
// The decomposer is intentionally a deterministic heuristic — no LLM,
// no cost, no flakiness. This makes the supervisor's tests trivially
// repeatable and keeps the demo working without an API key. Swap in
// an LLM-backed decomposer by replacing decomposeArrow with another
// weft.Arrow[Question, []SubQuestion] of your choosing.

// SubQuestion is one piece of a decomposed question. The Index gives it
// a stable position for ordering child workflow IDs and for the synthesizer.
type SubQuestion struct {
	Index int
	Text  string
}

// SubAnswer is the result of one child workflow.
type SubAnswer struct {
	SubQuestion SubQuestion
	Answer      string
	Converged   bool
	Rounds      int
	Error       string // non-empty if the child workflow failed; Answer is empty in that case
}

// SupervisorInput is the top-level request submitted to a supervisor.
type SupervisorInput struct {
	// Question is the original user question. The supervisor will split this
	// into subquestions and dispatch a convergence child workflow per subquestion.
	Question string
	// MaxRoundsPerChild caps each child convergence loop's iterations.
	// Required (>0). Passed verbatim to each child as Question.MaxRounds.
	MaxRoundsPerChild int
	// MaxSubQuestions caps how many subquestions the decomposer will emit.
	// Defaults to 5 if zero. Useful as an upper bound on parallelism / cost.
	MaxSubQuestions int
}

// SupervisorOutput is what a SupervisorWorkflow returns.
type SupervisorOutput struct {
	// Synthesis is the final synthesized answer text, suitable for display.
	Synthesis string
	// SubAnswers is the per-subquestion detail, in subquestion order.
	// Includes failed children (with Error set, Answer empty).
	SubAnswers []SubAnswer
	// SuccessCount is the number of children that converged (or returned a
	// non-converged-but-non-erroring final answer).
	SuccessCount int
	// FailureCount is the number of children that errored. The supervisor
	// always proceeds to synthesis as long as at least one child succeeded.
	FailureCount int
}

// --- Decomposer arrow -------------------------------------------------------

// decomposeArrow is the canonical heuristic decomposer.
// Exposed for testing and so callers can swap or wrap it.
var decomposeArrow weft.Arrow[Question, []SubQuestion] = func(_ context.Context, q Question) ([]SubQuestion, error) {
	max := q.MaxRounds // overloaded: in this arrow's input, the field is unused for the cap
	_ = max            // kept for parity with the wider Question type

	parts := splitHeuristic(q.Text)
	out := make([]SubQuestion, len(parts))
	for i, p := range parts {
		out[i] = SubQuestion{Index: i, Text: p}
	}
	return out, nil
}

// splitHeuristic is the pure, deterministic split.
//
// Rules, in order:
//  1. If the question contains multiple "?" separated chunks, those are the splits.
//  2. Otherwise, split on common conjunctions: " and ", " vs ", " versus ",
//     " compared to ", "; ".
//  3. Trim whitespace; drop empties; if nothing splits, return the original
//     as a single subquestion.
//
// This is intentionally simple — about 20 lines of string handling.
// Swap in something smarter as your needs grow.
func splitHeuristic(text string) []string {
	text = strings.TrimSpace(text)
	if text == "" {
		return nil
	}

	// Rule 1: multiple question marks
	if strings.Count(text, "?") >= 2 {
		raw := strings.Split(text, "?")
		var out []string
		for _, r := range raw {
			r = strings.TrimSpace(r)
			if r != "" {
				out = append(out, r+"?")
			}
		}
		if len(out) >= 2 {
			return out
		}
	}

	// Rule 2: conjunctions. Iterate in priority order so " versus " beats
	// " vs " and so on. We split on the *first* one that matches to keep
	// the parse predictable.
	splitters := []string{
		"; ",
		" versus ",
		" compared to ",
		" vs. ",
		" vs ",
		" and ",
	}
	lower := strings.ToLower(text)
	for _, sep := range splitters {
		if idx := strings.Index(lower, sep); idx >= 0 {
			parts := splitCaseInsensitive(text, sep)
			var out []string
			for _, p := range parts {
				p = strings.TrimSpace(p)
				if p != "" {
					out = append(out, p)
				}
			}
			if len(out) >= 2 {
				return out
			}
		}
	}

	// No splits found — return the original.
	return []string{text}
}

// splitCaseInsensitive splits s around every occurrence of sep, ignoring case
// in sep. Preserves the original casing of s in the output.
func splitCaseInsensitive(s, sep string) []string {
	if sep == "" {
		return []string{s}
	}
	lowerS := strings.ToLower(s)
	lowerSep := strings.ToLower(sep)

	var out []string
	start := 0
	for {
		idx := strings.Index(lowerS[start:], lowerSep)
		if idx < 0 {
			out = append(out, s[start:])
			return out
		}
		out = append(out, s[start:start+idx])
		start += idx + len(sep)
	}
}

// applyMaxSubQuestions truncates to at most n entries. n <= 0 means no cap.
func applyMaxSubQuestions(qs []SubQuestion, n int) []SubQuestion {
	if n <= 0 || len(qs) <= n {
		return qs
	}
	return qs[:n]
}

// --- Synthesizer arrow ------------------------------------------------------

// synthesizeArrow merges a slice of SubAnswers into a single text blob.
//
// Heuristic strategy: prefix each successful sub-answer with its
// subquestion as a heading, joined by blank lines. Failed children are
// noted at the end so the reader knows what's missing.
//
// Replace this arrow with an LLM-backed one (e.g. "summarize these
// answers into one coherent response") for production use.
var synthesizeArrow weft.Arrow[[]SubAnswer, string] = func(_ context.Context, answers []SubAnswer) (string, error) {
	if len(answers) == 0 {
		return "", nil
	}

	var b strings.Builder
	var failures []SubAnswer
	for _, a := range answers {
		if a.Error != "" {
			failures = append(failures, a)
			continue
		}
		if b.Len() > 0 {
			b.WriteString("\n\n")
		}
		fmt.Fprintf(&b, "## %s\n\n%s", a.SubQuestion.Text, a.Answer)
	}

	if len(failures) > 0 {
		if b.Len() > 0 {
			b.WriteString("\n\n")
		}
		b.WriteString("---\n\nThe following sub-questions could not be answered:")
		for _, f := range failures {
			fmt.Fprintf(&b, "\n- %q (%s)", f.SubQuestion.Text, f.Error)
		}
	}

	return b.String(), nil
}
