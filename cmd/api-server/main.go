// Command sibyl-api-server is a self-contained HTTP+SSE server that
// runs a Sibyl worker in-process and exposes the PR-review DAG to a
// web UI.
//
// Endpoints:
//
//	GET  /                 — serves the embedded React UI
//	POST /run              — starts a PRReviewWorkflow, returns {workflow_id}
//	GET  /events?workflow_id=X — SSE stream of all events for that workflow
//	GET  /metrics          — Prometheus metrics
//	GET  /healthz          — liveness check
//	GET  /version          — build info (Go version, VCS revision, build time)
//
// Why co-located? The broker is in-process pub/sub. For the broker's
// events to reach the HTTP handler, the worker that emits them must
// share the same process. Separating worker and API would require a
// cross-process bus (Redis, NATS) which we've decided not to add yet.
//
// The plain `cmd/worker` binary still works for terminal/CLI demos.
// This one adds an HTTP face on the same architecture.
package main

import (
	"context"
	"embed"
	"encoding/json"
	"flag"
	"fmt"
	"io/fs"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"runtime/debug"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"github.com/vinodhalaharvi/sibyl/agent"
	sibylworker "github.com/vinodhalaharvi/sibyl/worker"
)

//go:embed web/index.html
var webFS embed.FS

func main() {
	addr := flag.String("addr", ":8090", "HTTP listen address")
	backend := flag.String("llm", "claude-code", "completion backend: scripted | anthropic | claude-code")
	model := flag.String("model", "", "model name (passes through to backend if set)")
	cacheKind := flag.String("cache", "memory", "cache: memory | sqlite | none")
	cachePath := flag.String("cache-path", "./sibyl-api-cache.db", "sqlite cache path")
	cacheTTL := flag.Duration("cache-ttl", time.Hour, "cache entry TTL")
	metricsAddr := flag.String("metrics-addr", "", "if set, serve /metrics on this address (e.g. ':9091')")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

	// Build the broker and register it globally so all activities running
	// in this process emit events to it.
	broker := agent.NewMemoryBroker()
	defer broker.Close()
	agent.SetGlobalBroker(broker)

	// Metrics: always constructed, scraped if -metrics-addr is set.
	metrics := agent.NewMetrics()
	if *metricsAddr != "" {
		go serveMetrics(*metricsAddr, metrics)
		log.Printf("Metrics on %s/metrics", *metricsAddr)
	}

	// LLM backend wiring.
	rawComplete, err := pickBackend(*backend, *model)
	if err != nil {
		log.Fatalln("backend setup failed:", err)
	}
	streamFn, err := pickStream(*backend, *model)
	if err != nil {
		log.Fatalln("stream setup failed:", err)
	}
	log.Printf("Backend: %s (streaming=%v)", *backend, streamFn != nil)

	// Middleware chain matches cmd/worker. The middleware is on the
	// CompleteFunc only; CompleteStreamFunc is plumbed raw — streaming
	// middleware would be a separate stack and we don't have one yet.
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
		mws = append([]agent.Middleware{agent.WithCache(cache)}, mws...)
		if closer, ok := cache.(interface{ Close() error }); ok {
			defer func() { _ = closer.Close() }()
		}
	}
	complete := agent.Chain(rawComplete, mws...)

	// Temporal client + worker, in-process so the broker is shared.
	tc, err := client.Dial(client.Options{})
	if err != nil {
		log.Fatalln("Temporal client dial failed:", err)
	}
	defer tc.Close()

	w := worker.New(tc, agent.TaskQueue, worker.Options{})
	sibylworker.RegisterWithOptions(w, complete, sibylworker.Options{
		Stream: streamFn,
	})

	if err := w.Start(); err != nil {
		log.Fatalln("worker start failed:", err)
	}
	defer w.Stop()
	log.Println("Worker started on task queue:", agent.TaskQueue)

	// HTTP server.
	srv := &server{
		tc:     tc,
		broker: broker,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleIndex)
	mux.HandleFunc("/run", srv.handleRun)
	mux.HandleFunc("/events", srv.handleEvents)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/version", srv.handleVersion)

	httpSrv := &http.Server{
		Addr:              *addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	// Graceful shutdown.
	idleClosed := make(chan struct{})
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		log.Println("Shutting down...")
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = httpSrv.Shutdown(shutCtx)
		close(idleClosed)
	}()

	log.Printf("Listening on http://localhost%s", *addr)
	if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalln("HTTP serve error:", err)
	}
	<-idleClosed

	// Final cost summary like the plain worker.
	in, out, calls := tokenSink.Snapshot()
	log.Printf("Token usage: calls=%d input=~%d output=~%d total=~%d", calls, in, out, in+out)
}

type server struct {
	tc     client.Client
	broker *agent.MemoryBroker
}

// handleIndex serves the embedded React UI from web/index.html.
func (s *server) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	sub, err := fs.Sub(webFS, "web")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	data, err := fs.ReadFile(sub, "index.html")
	if err != nil {
		http.Error(w, "ui missing", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	_, _ = w.Write(data)
}

// versionResponse is what GET /version returns.
type versionResponse struct {
	GoVersion string `json:"go_version"`
	Revision  string `json:"revision,omitempty"`
	Modified  bool   `json:"modified,omitempty"`
	BuildTime string `json:"build_time,omitempty"`
}

// handleVersion returns build info embedded by the Go toolchain. Useful
// for confirming which commit a running server was built from without
// having to wire a separate -ldflags pipeline.
func (s *server) handleVersion(w http.ResponseWriter, _ *http.Request) {
	resp := versionResponse{}
	if info, ok := debug.ReadBuildInfo(); ok {
		resp.GoVersion = info.GoVersion
		for _, s := range info.Settings {
			switch s.Key {
			case "vcs.revision":
				resp.Revision = s.Value
			case "vcs.modified":
				resp.Modified = s.Value == "true"
			case "vcs.time":
				resp.BuildTime = s.Value
			}
		}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

// runRequest is the POST /run payload.
type runRequest struct {
	Diff string `json:"diff"`
}

// runResponse is what POST /run returns.
type runResponse struct {
	WorkflowID string `json:"workflow_id"`
}

// handleRun starts a PRReviewWorkflow asynchronously. Returns the
// workflow ID immediately; the client subscribes to /events to watch.
func (s *server) handleRun(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	var req runRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad json: "+err.Error(), http.StatusBadRequest)
		return
	}
	if req.Diff == "" {
		req.Diff = "Refactor PR #4218: switches our cache from in-memory to SQLite."
	}

	wfID := "sibyl-pr-review-" + uuid.NewString()[:8]
	_, err := s.tc.ExecuteWorkflow(r.Context(),
		client.StartWorkflowOptions{
			ID:        wfID,
			TaskQueue: agent.TaskQueue,
		},
		agent.ReviewWorkflowName,
		agent.PRReviewInput{
			Diff:             req.Diff,
			StreamingBackend: true, // activities respect this; falls back gracefully
		},
	)
	if err != nil {
		http.Error(w, "start workflow: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Emit a synthetic workflow.started event ourselves — Temporal's
	// own event history doesn't expose start as an emitted Sibyl event,
	// but the UI wants to know "the workflow exists now."
	s.broker.Publish(agent.NewWorkflowStarted(wfID, agent.ReviewWorkflowName, req))

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(runResponse{WorkflowID: wfID})
}

// handleEvents implements SSE for /events?workflow_id=X.
//
// Subscribes to the broker for events on workflow_id, forwards them as
// SSE messages. Closes when the client disconnects or when a
// terminal event (workflow.completed / workflow.failed) is observed.
func (s *server) handleEvents(w http.ResponseWriter, r *http.Request) {
	workflowID := r.URL.Query().Get("workflow_id")
	if workflowID == "" {
		http.Error(w, "workflow_id required", http.StatusBadRequest)
		return
	}

	// SSE headers.
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no") // disable nginx buffering

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	// Generous buffer — 256 events should outpace any UI; if the buffer
	// fills, the broker drops events (a slow consumer doesn't slow the
	// agent).
	ch, cancel := s.broker.Subscribe(workflowID, 256)
	defer cancel()

	// Send a comment line immediately so the browser knows we're live.
	_, _ = fmt.Fprintf(w, ": connected to %s\n\n", workflowID)
	flusher.Flush()

	// Heartbeat every 15s in case there's a long gap between events.
	heartbeat := time.NewTicker(15 * time.Second)
	defer heartbeat.Stop()

	// Watch for workflow termination so we can close the SSE cleanly.
	var done atomic.Bool
	watchDone := func(ev agent.Event) {
		switch ev.(type) {
		case agent.WorkflowCompleted, agent.WorkflowFailed:
			done.Store(true)
		}
	}

	for {
		select {
		case <-r.Context().Done():
			return

		case <-heartbeat.C:
			_, _ = fmt.Fprintf(w, ": heartbeat\n\n")
			flusher.Flush()

		case ev, ok := <-ch:
			if !ok {
				return
			}
			payload, err := json.Marshal(ev)
			if err != nil {
				continue // skip malformed events
			}
			_, _ = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", ev.Kind(), payload)
			flusher.Flush()
			watchDone(ev)
			if done.Load() {
				// Give the UI a brief moment to receive the terminal event,
				// then close so it knows to stop waiting.
				time.Sleep(50 * time.Millisecond)
				return
			}
		}
	}
}

// --- Backend setup helpers (duplicated from cmd/worker for self-containment) ---

func pickBackend(name, model string) (agent.CompleteFunc, error) {
	switch name {
	case "scripted":
		s := &agent.ScriptedLLM{Cycle: true, Responses: []string{
			"Static demo response from scripted backend.",
		}}
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
		return agent.NewClaudeCodeClient(cfg).Complete, nil
	default:
		return nil, fmt.Errorf("unknown backend: %s", name)
	}
}

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

func buildCache(kind, path string, ttl time.Duration) (agent.Cache, error) {
	switch kind {
	case "none":
		return nil, nil
	case "memory", "":
		return agent.NewMemoryCache(ttl), nil
	case "sqlite":
		return agent.NewSQLiteCache(path, ttl)
	default:
		return nil, fmt.Errorf("unknown cache: %s", kind)
	}
}

func serveMetrics(addr string, m *agent.Metrics) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(m.Registry(), promhttp.HandlerOpts{}))
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Printf("metrics server error: %v", err)
	}
}
