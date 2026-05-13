// Package agent defines the Sibyl multi-agent convergence framework.
//
// Sibyl orchestrates LLM agents using Temporal workflows. The convergence
// pattern uses two cooperating agents:
//
//   - Researcher: produces a candidate answer to the user's question
//   - Critic:     evaluates the answer and decides if it converges,
//     needs revision, or has failed
//
// The workflow loops until the Critic approves or maxRounds is reached.
// Each LLM call is a Temporal Activity (retryable, durable). The workflow
// itself is deterministic and replay-safe.
package agent

// Question is the input to a Sibyl run.
type Question struct {
	// Text is the question or task posed to the agents.
	Text string
	// MaxRounds caps the researcher/critic iterations. Required (>0).
	MaxRounds int
}

// Answer is the final result of a Sibyl run.
type Answer struct {
	// Text is the final answer text approved by the Critic (or the last
	// candidate if MaxRounds was reached without convergence).
	Text string
	// Rounds is how many researcher/critic iterations occurred.
	Rounds int
	// Converged is true if the Critic approved the answer before MaxRounds.
	Converged bool
	// History records each round's research and critique.
	History []Round
}

// Round captures one iteration of the convergence loop.
type Round struct {
	Number   int
	Research string  // what the Researcher produced
	Verdict  Verdict // what the Critic returned
}

// Verdict is the Critic's structured judgment of a candidate answer.
type Verdict struct {
	// Approved indicates the answer is good enough to return.
	Approved bool
	// Confidence is the Critic's self-rated confidence (0.0-1.0).
	Confidence float64
	// Feedback is critique passed back to the Researcher when not approved.
	// Empty when Approved is true.
	Feedback string
}
