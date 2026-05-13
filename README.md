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
├── agent/                    package agent — workflows, activities, types
│   ├── types.go              Question, Answer, Verdict, Round
│   ├── llm.go                CompleteFunc seam + Middleware + ScriptedLLM
│   ├── lift.go               bridge between weft Arrows and Temporal activities
│   ├── activities.go         Researcher and Critic — composed weft pipelines
│   ├── workflow.go           ConvergeWorkflow — the single-question convergence loop
│   ├── decompose.go          deterministic decompose + synthesize pipelines
│   ├── supervisor.go         SupervisorWorkflow — fan-out coordinator
│   ├── anthropic.go          Anthropic API client (CompleteFunc)
│   ├── claudecode.go         Local Claude Code CLI client (CompleteFunc)
│   └── *_test.go             unit tests (60 tests, in-process Temporal)
├── worker/
│   └── worker.go             Register() helper to wire Sibyl onto a Temporal worker
├── cmd/
│   ├── worker/main.go        runnable worker (-llm scripted | anthropic | claude-code)
│   ├── ask/main.go           submit a single ConvergeWorkflow
│   └── ask-supervisor/main.go  submit a SupervisorWorkflow (multi-agent fan-out)
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

The bundled `cmd/worker` uses a **ScriptedLLM** by default — a deterministic,
in-memory "model" that returns canned responses. This lets you run the whole
stack end-to-end without API keys. To use a real LLM, pass `-llm`:

```bash
# Use the Anthropic API (requires ANTHROPIC_API_KEY)
go run ./cmd/worker -llm anthropic

# Use your local Claude Code CLI (uses your Pro/Max subscription auth)
go run ./cmd/worker -llm claude-code

# Default: scripted, no network, no auth
go run ./cmd/worker -llm scripted
```

## The CompleteFunc seam

The LLM boundary is a **function type**, not an interface:

```go
type CompleteFunc func(ctx context.Context, systemPrompt, userMessage string) (string, error)
```

A function type is the right tool for a single-method seam in Go: any
compatible method becomes a `CompleteFunc` via a method value, test doubles
can be plain closures, and middleware composes as ordinary function wrapping.

Three backends ship in the box:

| Type | Use it for | How |
|---|---|---|
| `ScriptedLLM` | unit tests / offline demos | canned responses, records calls |
| `AnthropicClient` | production / billed API | direct HTTP to `api.anthropic.com` |
| `ClaudeCodeClient` | running on your machine | shells out to `claude -p` |

Each exposes a `Complete` method that satisfies `CompleteFunc`:

```go
c, _ := agent.NewAnthropicClient(agent.AnthropicConfig{})
sibylworker.Register(w, c.Complete)   // method value -> CompleteFunc
```

### Middleware

Because `CompleteFunc` is a function type, wrapping it is trivial:

```go
func WithLogging(log *slog.Logger) agent.Middleware {
    return func(next agent.CompleteFunc) agent.CompleteFunc {
        return func(ctx context.Context, sys, user string) (string, error) {
            start := time.Now()
            out, err := next(ctx, sys, user)
            log.Info("llm call", "took", time.Since(start), "err", err)
            return out, err
        }
    }
}

complete := agent.Chain(rawClient.Complete, WithLogging(logger), WithRateLimit(...))
sibylworker.Register(w, complete)
```

## Composing arrows with weft

Sibyl uses [weft](https://github.com/vinodhalaharvi/weft) as its compositional
layer. Every step inside an activity — prompt building, LLM call, response
parsing — is a `weft.Arrow[A, B]`. The activity body is just `Pipe3` over
three of them:

```go
// agent/activities.go
researcher := weft.Pipe3(
    buildResearchRequest,        // weft.Arrow[ResearchInput, CompletionRequest]
    agent.CompleteAsArrow(c),    // weft.Arrow[CompletionRequest, string]
    weft.Pure(trimResponse),     // weft.Arrow[string, string]
)
```

Why this matters: as we move toward multi-agent systems, the unit of
composition is no longer the activity — it's the Arrow. You can add a
caching layer, swap parsers, or fan out to multiple LLMs in parallel
(`weft.Par`) without rewriting the activity surface. The activity wrapper
just dispatches to whichever arrow you've composed.

The `agent` package exposes two adapters (in `lift.go`):

| Adapter              | Direction              | Use it when                                  |
|----------------------|------------------------|----------------------------------------------|
| `CompleteAsArrow`    | `CompleteFunc` → Arrow | Lifting an LLM client into a weft pipeline   |
| `ArrowAsActivity`    | Arrow → activity func  | Registering a composed arrow with Temporal   |

The convergence loop itself remains a Temporal workflow (must be deterministic
for replay), but the work *inside* each round is now expressible in the
broader weft algebra. This is the seam you'd build a multi-agent supervisor
on top of.

## Multi-agent supervision

`SupervisorWorkflow` decomposes a question into subquestions, spawns a child
`ConvergeWorkflow` per subquestion in parallel, waits for all of them, and
synthesizes a final answer.

```
┌──────────────────────────────────────────────────────────────┐
│ SupervisorWorkflow                                           │
│                                                              │
│  1. Decompose activity        question -> []SubQuestion      │
│  2. for each SubQuestion:                                    │
│       ExecuteChildWorkflow(ConvergeWorkflow, ...)            │
│  3. Wait for all children (swallow individual failures)      │
│  4. Synthesize activity       []SubAnswer -> final string    │
└──────────────────────────────────────────────────────────────┘
         │                  │                  │
         ▼                  ▼                  ▼
   ConvergeWorkflow   ConvergeWorkflow   ConvergeWorkflow
   (subquestion 1)    (subquestion 2)    (subquestion N)
```

Run it:

```bash
make ask-supervisor Q="What is Go and how does it compare to Rust"
```

Or directly:

```bash
go run ./cmd/ask-supervisor -q "Postgres vs SQLite vs MySQL for a side project" -rounds 3
```

Each child workflow appears in the Web UI as a separate execution with a
deterministic ID (`<supervisor-id>-sub-<index>`), so you can drill into any
child's event history independently.

**Failure handling.** Individual child failures are recorded in the output
(`SubAnswer.Error`) but don't fail the supervisor. The supervisor only fails
if every child failed, or if decomposition or synthesis itself failed.

**Decomposer.** Sibyl ships a deterministic heuristic decomposer that splits
on `?`, `and`, `vs`, `versus`, `compared to`, `;`. It's pure code, no LLM call,
no flakiness. Swap it for an LLM-backed decomposer by replacing
`decomposeArrow` in `agent/decompose.go` — it's a single `weft.Arrow`.

**Synthesizer.** Same story: the default synthesizer concatenates child
answers with markdown headings. Replace `synthesizeArrow` for LLM-backed
summarization.

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
