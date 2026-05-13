// Command sibyl-ask-supervisor submits a SupervisorWorkflow that decomposes
// the question, fans out child convergence workflows in parallel, and
// synthesizes a final answer.
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
		question        = flag.String("q", "What is the capital of France and what's the population", "question for the supervisor")
		maxRoundsPerCh  = flag.Int("rounds", 3, "maximum rounds per child convergence workflow")
		maxSubQuestions = flag.Int("max-subs", 5, "maximum number of subquestions to fan out (0 = default)")
		wfID            = flag.String("id", "", "supervisor workflow ID (default: auto-generated)")
	)
	flag.Parse()

	c, err := client.Dial(client.Options{})
	if err != nil {
		log.Fatalln("unable to create Temporal client:", err)
	}
	defer c.Close()

	opts := client.StartWorkflowOptions{
		ID:        *wfID,
		TaskQueue: agent.SupervisorTaskQueue,
	}
	if opts.ID == "" {
		opts.ID = fmt.Sprintf("sibyl-supervisor-%d", os.Getpid())
	}

	we, err := c.ExecuteWorkflow(context.Background(), opts, agent.SupervisorWorkflowName,
		agent.SupervisorInput{
			Question:          *question,
			MaxRoundsPerChild: *maxRoundsPerCh,
			MaxSubQuestions:   *maxSubQuestions,
		})
	if err != nil {
		log.Fatalln("unable to start supervisor workflow:", err)
	}

	fmt.Printf("Started supervisor id=%s run=%s\n", we.GetID(), we.GetRunID())
	fmt.Println("Watch in the UI: http://localhost:8080")
	fmt.Println("Waiting for synthesis...")

	var out agent.SupervisorOutput
	if err := we.Get(context.Background(), &out); err != nil {
		log.Fatalln("supervisor failed:", err)
	}

	fmt.Println()
	fmt.Println("==================== SYNTHESIS ====================")
	fmt.Printf("Successes: %d   Failures: %d\n\n", out.SuccessCount, out.FailureCount)
	fmt.Println(out.Synthesis)

	fmt.Println()
	fmt.Println("==================== PER-CHILD DETAIL ====================")
	for _, sa := range out.SubAnswers {
		fmt.Printf("[%d] %s\n", sa.SubQuestion.Index, sa.SubQuestion.Text)
		if sa.Error != "" {
			fmt.Printf("    ERROR: %s\n", sa.Error)
		} else {
			fmt.Printf("    Converged: %v   Rounds: %d\n", sa.Converged, sa.Rounds)
			fmt.Printf("    Answer: %s\n", sa.Answer)
		}
	}
}
