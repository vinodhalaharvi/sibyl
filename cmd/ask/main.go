// Command sibyl-ask submits a single question to a running Sibyl worker
// and prints the answer when it converges (or hits MaxRounds).
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"

	"go.temporal.io/sdk/client"

	"github.com/vinodhalaharvi/sibyl/agent"
)

func main() {
	var (
		question  = flag.String("q", "What is the capital of France?", "question for the agents")
		maxRounds = flag.Int("rounds", 3, "maximum researcher/critic rounds")
		wfID      = flag.String("id", "", "workflow ID (default: auto-generated)")
	)
	flag.Parse()

	c, err := client.Dial(client.Options{})
	if err != nil {
		log.Fatalln("unable to create Temporal client:", err)
	}
	defer c.Close()

	opts := client.StartWorkflowOptions{
		ID:        *wfID,
		TaskQueue: agent.TaskQueue,
	}
	if opts.ID == "" {
		opts.ID = fmt.Sprintf("sibyl-%d", os.Getpid())
	}

	we, err := c.ExecuteWorkflow(context.Background(), opts, "ConvergeWorkflow",
		agent.Question{Text: *question, MaxRounds: *maxRounds})
	if err != nil {
		log.Fatalln("unable to start workflow:", err)
	}

	fmt.Printf("Started workflow id=%s run=%s\n", we.GetID(), we.GetRunID())
	fmt.Println("Waiting for result...")

	var ans agent.Answer
	if err := we.Get(context.Background(), &ans); err != nil {
		log.Fatalln("workflow failed:", err)
	}

	fmt.Println()
	fmt.Println("==================== RESULT ====================")
	fmt.Printf("Converged: %v   Rounds: %d\n", ans.Converged, ans.Rounds)
	fmt.Println()
	fmt.Println("Answer:")
	fmt.Println(ans.Text)
	fmt.Println()
	fmt.Println("--- History ---")
	for _, r := range ans.History {
		fmt.Printf("Round %d:\n", r.Number)
		fmt.Printf("  Research: %s\n", r.Research)
		fmt.Printf("  Verdict:  approved=%v confidence=%.2f feedback=%q\n",
			r.Verdict.Approved, r.Verdict.Confidence, r.Verdict.Feedback)
	}
}
