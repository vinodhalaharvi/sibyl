// Command sibyl-dag-demo runs an example static DAG showing the
// fan-out/fan-in pattern from the README.
//
// The DAG has 5 nodes:
//
//	         ┌─> security_audit ─┐
//	parse_diff ─┼─> test_coverage  ─┼─> synthesize_review
//	         └─> style_check    ─┘
//
// All three analyzers run in parallel after parse_diff completes.
// Each is a stub that just returns a fixed string; in real use they
// would be ToolAgent or LLM-backed analyzers.
//
// The DAG runs inside the local process for demonstration. To run it
// as a durable Temporal activity, register a workflow that calls
// CompiledDAG.Execute via an activity wrapper.
package main

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/vinodhalaharvi/weft/weft"

	"github.com/vinodhalaharvi/sibyl/agent"
)

func main() {
	dag := agent.NewDAG()

	// Seed placeholder for the diff input.
	agent.AddTypedNode(dag, "diff",
		weft.Arrow[string, string](func(_ context.Context, s string) (string, error) {
			return s, nil
		}),
		func(_ agent.NodeInputs) (string, error) { return "", nil },
	)

	// Parse stage: turn the diff into a list of changed files.
	agent.AddTypedNode(dag, "parse_diff",
		weft.Arrow[string, []string](func(_ context.Context, _ string) ([]string, error) {
			// Stub: pretend we parsed the diff.
			return []string{"agent/dag.go", "agent/tools.go", "agent/tool_agent.go"}, nil
		}),
		func(in agent.NodeInputs) (string, error) {
			return in.MustGet("diff").(string), nil
		},
		agent.DependsOn("diff"),
	)

	// Three parallel analyzers. In real use, each would be a ToolAgent
	// or a CompleteFunc call. Here they're stubs to keep the demo
	// runnable without an LLM.
	makeAnalyzer := func(name, summary string) {
		agent.AddTypedNode(dag, agent.NodeID(name),
			weft.Arrow[[]string, string](func(_ context.Context, files []string) (string, error) {
				return fmt.Sprintf("%s: examined %d files. %s",
					name, len(files), summary), nil
			}),
			func(in agent.NodeInputs) ([]string, error) {
				return in.MustGet("parse_diff").([]string), nil
			},
			agent.DependsOn("parse_diff"),
		)
	}
	makeAnalyzer("security_audit", "no SQL injection, no unsanitized inputs.")
	makeAnalyzer("test_coverage", "all changes have associated tests; coverage holding at 88%.")
	makeAnalyzer("style_check", "passes gofmt, go vet, and staticcheck.")

	// Synthesize stage: merge the three analyzer outputs into one review.
	agent.AddTypedNode(dag, "synthesize",
		weft.Arrow[[]string, string](func(_ context.Context, parts []string) (string, error) {
			return "## PR Review Summary\n\n" + strings.Join(parts, "\n\n"), nil
		}),
		func(in agent.NodeInputs) ([]string, error) {
			return []string{
				in.MustGet("security_audit").(string),
				in.MustGet("test_coverage").(string),
				in.MustGet("style_check").(string),
			}, nil
		},
		agent.DependsOn("security_audit", "test_coverage", "style_check"),
	)

	compiled, err := dag.Compile()
	if err != nil {
		log.Fatalln("compile:", err)
	}

	fmt.Println("Running DAG: parse_diff -> [security|tests|style] -> synthesize")
	fmt.Println("(analyzers run in parallel)")
	fmt.Println()

	results, err := compiled.Execute(context.Background(), map[agent.NodeID]any{
		"diff": "(pretend diff content here)",
	})
	if err != nil {
		log.Fatalln("execute:", err)
	}

	fmt.Println("=============================================")
	fmt.Println(results["synthesize"])
	fmt.Println("=============================================")
	fmt.Println()
	fmt.Println("Per-node outputs:")
	for _, id := range []agent.NodeID{"parse_diff", "security_audit", "test_coverage", "style_check"} {
		fmt.Printf("  [%s] %v\n", id, results[id])
	}
}
