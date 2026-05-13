# Sibyl

A small, principled multi-agent convergence framework built on
[Temporal](https://temporal.io) and Go.

Two cooperating agents — a **Researcher** and a **Critic** — iterate over a
question until the Critic approves the answer or `MaxRounds` is reached. The
loop itself is a Temporal Workflow (durable, replay-safe), and every LLM call
is a Temporal Activity (retried automatically on transient failures).

## Why this exists

The agent loop — *"call the model, run a tool, reason about the result, call
the model again"* — has the same shape as a long-running orchestration: many
steps, each can fail, each is expensive, and the whole thing must survive
crashes, restarts, and timeouts. Temporal is purpose-built for that shape.
Sibyl is a small reference implementation of an agent convergence pattern on
top of it.

## Project layout

```
sibyl/
├── agent/                    package agent — workflow, activities, types
│   ├── types.go              Question, Answer, Verdict, Round
│   ├── llm.go                LLMClient interface + ScriptedLLM (for tests)
│   ├── activities.go         Researcher and Critic activities
│   ├── workflow.go           ConvergeWorkflow — the durable loop
│   ├── workflow_test.go      end-to-end workflow tests
│   ├── activities_test.go    activity unit tests
│   └── helpers_test.go       small test helpers
├── worker/
│   └── worker.go             Register() helper to wire Sibyl onto a Temporal worker
├── cmd/
│   ├── worker/main.go        runnable worker (ships with a ScriptedLLM demo)
│   └── ask/main.go           CLI to submit one question and print the answer
├── go.mod / go.sum
├── Makefile
└── README.md
```

## Quick start

You need Go 1.24+ and the Temporal CLI (for the local dev server).

```bash
# 1. Resolve deps and run tests
go mod tidy
go test -race ./...

# 2. In one terminal, start the Temporal dev server
temporal server start-dev --db-filename temporal.db --ui-port 8080

# 3. In a second terminal, start the Sibyl worker
go run ./cmd/worker

# 4. In a third terminal, ask a question
go run ./cmd/ask -q "What is the capital of France?" -rounds 3

# Open http://localhost:8080 to watch the workflow execute live.
```

The bundled `cmd/worker` uses a **ScriptedLLM** — a deterministic, in-memory
"model" that returns canned responses. This lets you run the whole stack
end-to-end without API keys. To go live, implement the `agent.LLMClient`
interface against your provider and swap it into `cmd/worker/main.go`.

## The LLMClient interface

```go
type LLMClient interface {
    Complete(ctx context.Context, systemPrompt, userMessage string) (string, error)
}
```

A real implementation looks like:

```go
type AnthropicClient struct{ apiKey string }

func (a *AnthropicClient) Complete(ctx context.Context, sys, user string) (string, error) {
    // POST to https://api.anthropic.com/v1/messages, return the assistant's text
    // ...
}
```

Pass it to `worker.Register`:

```go
sibylworker.Register(w, &AnthropicClient{apiKey: os.Getenv("ANTHROPIC_API_KEY")})
```

## How the convergence loop works

```
┌──────────────────────────────────────────────────────────┐
│ ConvergeWorkflow (deterministic Go code)                 │
│                                                          │
│   for round := 1; round <= MaxRounds; round++ {          │
│       candidate := ExecuteActivity(Research, ...)        │
│       verdict   := ExecuteActivity(Critique, candidate)  │
│       if verdict.Approved {                              │
│           return candidate                               │
│       }                                                  │
│       // carry feedback forward to the next round        │
│   }                                                      │
└──────────────────────────────────────────────────────────┘
```

Both `Research` and `Critique` are activities. Their results are recorded in
the workflow's event history, so on a worker crash the workflow resumes
without re-running them.

The Critic returns structured JSON:

```json
{"approved": true, "confidence": 0.92, "feedback": ""}
```

If the model returns malformed JSON, the activity returns a non-retryable
error (`InvalidLLMResponse`) — retrying won't help if it's a prompt/model
problem, and we want to fail fast.

## Testing strategy

Temporal ships an in-process test environment (`testsuite.WorkflowTestSuite`)
that runs workflows and activities without a real server. All Sibyl tests use
it — no Docker, no network, no API keys:

```bash
go test -race -count=1 ./...
```

The `ScriptedLLM` test double lets each test specify the exact sequence of
LLM responses the workflow will see. Tests cover:

- happy path: converges on round 1
- revision path: converges on round 2 after critic feedback
- max-rounds path: terminates with `Converged: false`
- input validation: empty question, zero MaxRounds
- non-retryable errors: malformed critic JSON fails fast (one call, not five)
- the LLM-call parsing logic in each activity

## Production notes

- **Real LLM client.** Implement `LLMClient` against your provider. Keep
  retries inside the provider client minimal; let Temporal's activity retry
  policy handle it.
- **Cost control.** Set `MaxRounds` conservatively. Every round is two LLM
  calls. The Temporal Web UI shows exactly how many calls have happened so far.
- **Long human-in-the-loop.** Add a signal handler (`workflow.GetSignalChannel`)
  to inject human guidance mid-loop. The workflow can block on a signal for
  hours or days without consuming worker resources.
- **Multi-agent fan-out.** For more than two agents, spawn child workflows
  with `workflow.ExecuteChildWorkflow` — each child has its own event history
  and can crash/recover independently.

## Naming

A Sibyl, in Greek myth, was an oracle who deliberated before speaking. That's
what a convergence loop is: a structured deliberation before an answer is
returned. The library name is intentionally lowercase: `sibyl`.

## License

MIT (or your choice — edit before publishing).
