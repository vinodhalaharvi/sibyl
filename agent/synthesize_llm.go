package agent

import (
	"context"
	"fmt"
	"strings"

	"github.com/vinodhalaharvi/weft/weft"
)

// LLMSynthesizer returns a weft.Arrow that uses a CompleteFunc to merge
// per-child SubAnswers into one coherent final answer.
//
// Versus the default heuristic synthesizer (which concatenates with
// markdown headings), this produces:
//
//   - cross-references between sub-answers
//   - a single voice and unified narrative
//   - elision of redundancy
//   - graceful handling of partial failures
//
// In exchange you spend one LLM call per supervisor run. Use the
// heuristic synthesizer when you want zero-cost, fully reproducible
// output; use this when you care about reading quality.
//
// The returned arrow has the standard signature, so it slots into
// Activities.Synthesizer the same way the heuristic does.
func LLMSynthesizer(c CompleteFunc) weft.Arrow[[]SubAnswer, string] {
	if c == nil {
		return func(_ context.Context, _ []SubAnswer) (string, error) {
			return "", fmt.Errorf("LLMSynthesizer: CompleteFunc is nil")
		}
	}
	return weft.Pipe3(
		weft.Arrow[[]SubAnswer, CompletionRequest](buildSynthesisRequest),
		CompleteAsArrow(c),
		weft.Pure(trimResponse),
	)
}

func buildSynthesisRequest(_ context.Context, answers []SubAnswer) (CompletionRequest, error) {
	const system = `You are a synthesis editor merging multiple research sub-answers into one coherent final answer.

Guidelines:
- Produce a single, unified answer in clean prose. Do NOT keep sub-headings or use bullet-per-subquestion format.
- Cross-reference between sub-answers when relevant — show how they relate, contradict, or complement each other.
- Drop redundancies. Keep specifics (numbers, names, terms) that any sub-answer mentioned.
- If some sub-answers failed (Error set, Answer empty), briefly note what couldn't be answered at the end, in one sentence.
- Match the granularity of the inputs: don't add new facts the sub-answers didn't already cover.
Return only the synthesized answer, no preamble.`

	var b strings.Builder
	b.WriteString("Sub-answers to merge:\n\n")
	for _, sa := range answers {
		if sa.Error != "" {
			fmt.Fprintf(&b, "### Sub-question (FAILED): %s\nError: %s\n\n", sa.SubQuestion.Text, sa.Error)
			continue
		}
		fmt.Fprintf(&b, "### Sub-question: %s\n%s\n\n", sa.SubQuestion.Text, sa.Answer)
	}

	return CompletionRequest{
		SystemPrompt: system,
		UserMessage:  strings.TrimRight(b.String(), "\n"),
	}, nil
}
