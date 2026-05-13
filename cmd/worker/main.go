// Command sibyl-worker runs a Temporal worker hosting Sibyl's
// ConvergeWorkflow and its Researcher/Critic activities.
//
// Backend selection via -llm:
//
//	scripted    (default) deterministic canned responses; no network, no keys
//	anthropic   Anthropic Messages API; requires ANTHROPIC_API_KEY
//	claude-code shell out to local `claude -p`; uses your Claude Code login
//
// Synthesis strategy via -synthesize:
//
//	heuristic   (default) concatenate child answers with markdown headings
//	llm         use the same LLM backend to summarize children into one answer
package main

import (
	"flag"
	"log"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"github.com/vinodhalaharvi/sibyl/agent"
	sibylworker "github.com/vinodhalaharvi/sibyl/worker"
)

func main() {
	backend := flag.String("llm", "scripted", "completion backend: scripted | anthropic | claude-code")
	model := flag.String("model", "", "model name (passes through to backend if set)")
	synthesize := flag.String("synthesize", "heuristic", "synthesis strategy: heuristic | llm")
	flag.Parse()

	complete, err := pickBackend(*backend, *model)
	if err != nil {
		log.Fatalln("backend setup failed:", err)
	}
	log.Printf("Sibyl worker using LLM backend: %s", *backend)
	log.Printf("Sibyl worker using synthesis strategy: %s", *synthesize)

	c, err := client.Dial(client.Options{})
	if err != nil {
		log.Fatalln("unable to create Temporal client:", err)
	}
	defer c.Close()

	opts := sibylworker.Options{}
	switch *synthesize {
	case "heuristic", "":
		// Default; leave Options.Synthesizer nil.
	case "llm":
		opts.Synthesizer = agent.LLMSynthesizer(complete)
	default:
		log.Fatalf("unknown -synthesize value: %q (choices: heuristic, llm)", *synthesize)
	}

	w := worker.New(c, agent.TaskQueue, worker.Options{})
	sibylworker.RegisterWithOptions(w, complete, opts)

	log.Println("Sibyl worker started on task queue:", agent.TaskQueue)
	log.Println("Press Ctrl+C to stop.")
	if err := w.Run(worker.InterruptCh()); err != nil {
		log.Fatalln("worker stopped with error:", err)
	}
}

func pickBackend(name, model string) (agent.CompleteFunc, error) {
	switch name {
	case "scripted":
		// Deterministic two-round converge. Same data every question; useful
		// for verifying the Temporal plumbing without any API calls.
		s := &agent.ScriptedLLM{
			Cycle: true,
			Responses: []string{
				"Paris is the capital of France.",
				`{"approved": false, "confidence": 0.4, "feedback": "Answer is correct but too terse. Add a sentence about why Paris is significant."}`,
				"Paris is the capital of France. It has been the political, cultural, and economic center of the country for over a thousand years.",
				`{"approved": true, "confidence": 0.92, "feedback": ""}`,
			},
		}
		return s.Complete, nil

	case "anthropic":
		cfg := agent.AnthropicConfig{Model: model}
		c, err := agent.NewAnthropicClient(cfg)
		if err != nil {
			return nil, err
		}
		return c.Complete, nil

	case "claude-code":
		cfg := agent.ClaudeCodeConfig{Model: model}
		c := agent.NewClaudeCodeClient(cfg)
		return c.Complete, nil

	default:
		return nil, &backendError{name: name}
	}
}

type backendError struct{ name string }

func (e *backendError) Error() string {
	return "unknown backend: " + e.name + " (choices: scripted, anthropic, claude-code)"
}
