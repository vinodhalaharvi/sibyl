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
//
// Caching via -cache:
//
//	memory   (default) in-process TTL cache; lost on worker restart
//	sqlite   persistent SQLite cache at -cache-path; survives restarts
//	none     no caching
//
// Observability:
//
//	-metrics-addr      ":9090" exposes /metrics for Prometheus
//	-otel-endpoint     "localhost:4318" enables OTLP/HTTP trace export
//	-otel-service-name "sibyl-worker" overrides the OTel service.name
//	-otel-insecure     use HTTP instead of HTTPS to the OTel collector
//
// Middleware: every CompleteFunc is wrapped with Logging + Retry + TokenAccounting
// + Metrics + (optionally) Cache. Token totals are logged on shutdown.
package main

import (
	"context"
	"flag"
	"log"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"github.com/vinodhalaharvi/sibyl/agent"
	sibylworker "github.com/vinodhalaharvi/sibyl/worker"
)

func main() {
	backend := flag.String("llm", "scripted", "completion backend: scripted | anthropic | claude-code")
	model := flag.String("model", "", "model name (passes through to backend if set)")
	synthesize := flag.String("synthesize", "heuristic", "synthesis strategy: heuristic | llm")
	cacheKind := flag.String("cache", "memory", "cache: memory | sqlite | none")
	cachePath := flag.String("cache-path", "./sibyl-cache.db", "sqlite cache path (only used when -cache=sqlite)")
	cacheTTL := flag.Duration("cache-ttl", time.Hour, "cache entry TTL")
	metricsAddr := flag.String("metrics-addr", "", "if set, serve /metrics on this address (e.g. ':9090')")
	otelEndpoint := flag.String("otel-endpoint", "", "OTLP/HTTP endpoint, e.g. 'localhost:4318' (empty disables tracing)")
	otelService := flag.String("otel-service-name", "sibyl-worker", "OTel service.name resource attribute")
	otelInsecure := flag.Bool("otel-insecure", true, "use HTTP (not HTTPS) to the OTel collector")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

	// OTel tracing setup. If -otel-endpoint is empty, this is a no-op
	// and tracing call sites cost ~zero.
	tracingShutdown, err := agent.SetupTracing(context.Background(), agent.TracingConfig{
		ServiceName: *otelService,
		Endpoint:    *otelEndpoint,
		Insecure:    *otelInsecure,
	})
	if err != nil {
		log.Fatalln("otel setup failed:", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = tracingShutdown(ctx)
	}()
	if *otelEndpoint != "" {
		log.Printf("Sibyl worker exporting OTel traces to: %s (service=%s)", *otelEndpoint, *otelService)
	}

	// Prometheus metrics: always constructed (cheap; no-op if no /metrics
	// endpoint is served). The middleware uses it regardless so token
	// counts and call rates are always recorded — the /metrics endpoint
	// just makes them queryable.
	metrics := agent.NewMetrics()
	if *metricsAddr != "" {
		go serveMetrics(*metricsAddr, metrics)
		log.Printf("Sibyl worker serving Prometheus metrics on %s/metrics", *metricsAddr)
	}

	rawComplete, err := pickBackend(*backend, *model)
	if err != nil {
		log.Fatalln("backend setup failed:", err)
	}
	log.Printf("Sibyl worker using LLM backend: %s", *backend)
	log.Printf("Sibyl worker using synthesis strategy: %s", *synthesize)
	log.Printf("Sibyl worker using cache: %s", *cacheKind)

	// Build the middleware chain. Order matters:
	//   cache -> metrics -> retry -> tokens -> logging -> raw
	// Reading inside-out at call time: cache hit returns early; misses
	// flow through retry (absorb transient errors), then through token
	// accounting and metrics (recorded once per attempt that reached the
	// real backend), then through logging, then to the raw LLM client.
	tokenSink := &agent.AtomicTokenSink{}
	mws := []agent.Middleware{
		agent.WithLogging(logger),
		agent.WithTokenAccounting(tokenSink),
		agent.WithRetry(3, 200*time.Millisecond),
		agent.WithMetrics(metrics, *backend),
	}
	cache, err := buildCache(*cacheKind, *cachePath, *cacheTTL)
	if err != nil {
		log.Fatalln("cache setup failed:", err)
	}
	if cache != nil {
		// Prepend cache as the outermost middleware so hits short-circuit
		// everything else (no retry, no logging of the call, no token cost).
		mws = append([]agent.Middleware{agent.WithCache(cache)}, mws...)
		if closer, ok := cache.(interface{ Close() error }); ok {
			defer func() {
				if err := closer.Close(); err != nil {
					log.Printf("cache close: %v", err)
				}
			}()
		}
	}
	complete := agent.Chain(rawComplete, mws...)

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

	// Streaming: enable for backends that support it (anthropic only today).
	// nil for others means activities fall back to atomic Complete and emit
	// a single TokenChunkEvent rather than per-token.
	if streamFn, err := pickStream(*backend, *model); err != nil {
		log.Fatalln("stream setup failed:", err)
	} else if streamFn != nil {
		opts.Stream = streamFn
		log.Printf("Sibyl worker streaming enabled for %s backend", *backend)
	}

	w := worker.New(c, agent.TaskQueue, worker.Options{})
	sibylworker.RegisterWithOptions(w, complete, opts)

	log.Println("Sibyl worker started on task queue:", agent.TaskQueue)
	log.Println("Press Ctrl+C to stop.")
	runErr := w.Run(worker.InterruptCh())

	// Report token totals on shutdown so demo runs end with a cost summary.
	in, out, calls := tokenSink.Snapshot()
	log.Printf("Token usage during this run: calls=%d input=~%d output=~%d total=~%d",
		calls, in, out, in+out)

	if runErr != nil {
		log.Fatalln("worker stopped with error:", runErr)
	}
}

// serveMetrics starts a tiny HTTP server exposing /metrics. Runs in a
// goroutine; errors are logged but don't stop the worker — losing
// metrics scrape capability shouldn't kill the agent.
func serveMetrics(addr string, m *agent.Metrics) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(m.Registry(), promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Printf("metrics server error: %v", err)
	}
}

// buildCache returns the Cache implementation selected by kind. A return
// of (nil, nil) means "no caching middleware at all" (kind == "none").
func buildCache(kind, path string, ttl time.Duration) (agent.Cache, error) {
	switch kind {
	case "none":
		return nil, nil
	case "memory", "":
		return agent.NewMemoryCache(ttl), nil
	case "sqlite":
		return agent.NewSQLiteCache(path, ttl)
	default:
		return nil, &cacheError{kind: kind}
	}
}

type cacheError struct{ kind string }

func (e *cacheError) Error() string {
	return "unknown -cache value: " + e.kind + " (choices: memory, sqlite, none)"
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

// pickStream returns the streaming completion function for the named
// backend, or nil if the backend doesn't support streaming.
//
// Only the Anthropic Messages API supports real SSE streaming today.
// claude-code emits structured JSON but not in a stream-friendly format;
// scripted backends have no concept of streaming. For those, returning
// nil signals the activity to use the atomic Complete and emit a single
// TokenChunkEvent with the full response.
func pickStream(name, model string) (agent.CompleteStreamFunc, error) {
	switch name {
	case "anthropic":
		cfg := agent.AnthropicConfig{Model: model}
		c, err := agent.NewAnthropicClient(cfg)
		if err != nil {
			return nil, err
		}
		return c.CompleteStream, nil
	default:
		return nil, nil
	}
}

type backendError struct{ name string }

func (e *backendError) Error() string {
	return "unknown backend: " + e.name + " (choices: scripted, anthropic, claude-code)"
}
