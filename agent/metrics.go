// Package agent — metrics.go provides Prometheus metric definitions.
//
// Why Prometheus alongside OTel?
//
//	OTel tracing: "what happened in THIS specific request?" Useful for
//	debugging a single user's broken workflow.
//
//	Prometheus metrics: "what's happening in AGGREGATE?" Useful for
//	alerting ("LLM error rate > 1%"), dashboards ("cache hit rate over
//	time"), and capacity planning ("p99 LLM latency").
//
//	The two are complementary, not redundant. Mature systems run both.
//
// Resist the urge to over-instrument. The metric set below is intentionally
// small (~10 families). Add more only when you have a concrete dashboard
// or alert that needs them. Every metric is a perpetual maintenance cost.
//
// Cardinality discipline:
//
//	Label values become Prometheus time-series. High-cardinality labels
//	(user_id, request_id, full prompt content) DESTROY Prometheus.
//	The labels here are intentionally bounded: backend names are a
//	closed set, tool names are configured at startup, statuses are
//	"success"|"error", etc.
//
// Registration model:
//
//	A *Metrics value holds all registered collectors. The worker creates
//	one at startup and threads it through (typically by stashing in
//	Activities or attaching to a CompleteFunc middleware). A no-op
//	*Metrics (nil) is safe; all observation calls become no-ops.
package agent

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

// Metrics is the set of Prometheus collectors Sibyl emits. Construct one
// per worker process via NewMetrics; pass nil anywhere a Metrics-receiver
// is optional.
type Metrics struct {
	registry *prometheus.Registry

	llmCalls     *prometheus.CounterVec
	llmDuration  *prometheus.HistogramVec
	llmTokens    *prometheus.CounterVec
	toolCalls    *prometheus.CounterVec
	toolDuration *prometheus.HistogramVec
	cacheOps     *prometheus.CounterVec
	workflowRuns *prometheus.CounterVec
	workflowDur  *prometheus.HistogramVec
	brokerEvents *prometheus.CounterVec
	brokerSubs   prometheus.Gauge
}

// NewMetrics creates a fresh Metrics with all collectors registered on
// a NEW Prometheus registry. The registry is exposed via Registry() so
// the HTTP handler can serve it.
//
// We DELIBERATELY don't use the global prometheus.DefaultRegisterer.
// A private registry per Metrics instance makes the package testable
// (no global state pollution) and lets multiple Sibyl instances coexist
// in one process if anyone ever wants that.
func NewMetrics() *Metrics {
	reg := prometheus.NewRegistry()

	m := &Metrics{
		registry: reg,

		llmCalls: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "sibyl_llm_calls_total",
				Help: "Total LLM completion calls, labelled by backend and status.",
			},
			[]string{"backend", "status"},
		),
		llmDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "sibyl_llm_duration_seconds",
				Help:    "Duration of LLM completion calls.",
				Buckets: prometheus.ExponentialBuckets(0.1, 2, 10), // 0.1s..51.2s
			},
			[]string{"backend", "status"},
		),
		llmTokens: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "sibyl_llm_tokens_total",
				Help: "Approximate token usage; direction is 'input' or 'output'.",
			},
			[]string{"direction"},
		),
		toolCalls: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "sibyl_tool_invocations_total",
				Help: "Total tool invocations, labelled by tool name and status.",
			},
			[]string{"tool_name", "status"},
		),
		toolDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "sibyl_tool_duration_seconds",
				Help:    "Duration of tool invocations.",
				Buckets: prometheus.ExponentialBuckets(0.01, 2, 12), // 10ms..40s
			},
			[]string{"tool_name", "status"},
		),
		cacheOps: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "sibyl_cache_operations_total",
				Help: "Cache operations by result: 'hit', 'miss', 'set'.",
			},
			[]string{"result"},
		),
		workflowRuns: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "sibyl_workflow_executions_total",
				Help: "Workflow executions, labelled by workflow type and final status.",
			},
			[]string{"workflow_type", "status"},
		),
		workflowDur: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "sibyl_workflow_duration_seconds",
				Help:    "End-to-end workflow duration.",
				Buckets: prometheus.ExponentialBuckets(0.5, 2, 12), // 0.5s..2048s
			},
			[]string{"workflow_type", "status"},
		),
		brokerEvents: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "sibyl_broker_events_total",
				Help: "Events flowing through the in-memory broker; result is 'published' or 'dropped'.",
			},
			[]string{"result"},
		),
		brokerSubs: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "sibyl_broker_subscribers",
				Help: "Current number of active broker subscribers.",
			},
		),
	}

	// Register all collectors. Order doesn't matter; each MustRegister
	// will panic on duplicate registration which can't happen on a
	// fresh private registry.
	reg.MustRegister(
		m.llmCalls,
		m.llmDuration,
		m.llmTokens,
		m.toolCalls,
		m.toolDuration,
		m.cacheOps,
		m.workflowRuns,
		m.workflowDur,
		m.brokerEvents,
		m.brokerSubs,
	)

	// Include the standard Go runtime + process collectors so dashboards
	// can show GC pause times, goroutine counts, RSS, etc.
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	return m
}

// Registry returns the underlying prometheus.Registry, so an HTTP server
// can plumb it into a /metrics handler:
//
//	mux.Handle("/metrics", promhttp.HandlerFor(m.Registry(), promhttp.HandlerOpts{}))
func (m *Metrics) Registry() *prometheus.Registry {
	if m == nil {
		return nil
	}
	return m.registry
}

// --- Observation helpers --------------------------------------------------
//
// These are the call sites that instrument the rest of the codebase.
// Each is nil-safe (no panic if m is nil), which means callers can
// `metrics.ObserveLLMCall(...)` without nil checks even when metrics are
// disabled.

// ObserveLLMCall records one LLM call's outcome. status is "success" or "error".
func (m *Metrics) ObserveLLMCall(backend, status string, duration time.Duration) {
	if m == nil {
		return
	}
	m.llmCalls.WithLabelValues(backend, status).Inc()
	m.llmDuration.WithLabelValues(backend, status).Observe(duration.Seconds())
}

// ObserveLLMTokens records token counts from a single call.
func (m *Metrics) ObserveLLMTokens(input, output int) {
	if m == nil {
		return
	}
	if input > 0 {
		m.llmTokens.WithLabelValues("input").Add(float64(input))
	}
	if output > 0 {
		m.llmTokens.WithLabelValues("output").Add(float64(output))
	}
}

// ObserveToolCall records a tool invocation. status is "success" or "error".
func (m *Metrics) ObserveToolCall(toolName, status string, duration time.Duration) {
	if m == nil {
		return
	}
	m.toolCalls.WithLabelValues(toolName, status).Inc()
	m.toolDuration.WithLabelValues(toolName, status).Observe(duration.Seconds())
}

// ObserveCacheOperation records a cache event. result is "hit"|"miss"|"set".
func (m *Metrics) ObserveCacheOperation(result string) {
	if m == nil {
		return
	}
	m.cacheOps.WithLabelValues(result).Inc()
}

// ObserveWorkflow records a workflow's completion. status is "success" or "error".
func (m *Metrics) ObserveWorkflow(workflowType, status string, duration time.Duration) {
	if m == nil {
		return
	}
	m.workflowRuns.WithLabelValues(workflowType, status).Inc()
	m.workflowDur.WithLabelValues(workflowType, status).Observe(duration.Seconds())
}

// ObserveBrokerEvent records an event flowing through the broker.
// result is "published" (successfully delivered to at least one buffer)
// or "dropped" (buffer full somewhere).
func (m *Metrics) ObserveBrokerEvent(result string) {
	if m == nil {
		return
	}
	m.brokerEvents.WithLabelValues(result).Inc()
}

// SetBrokerSubscribers sets the current number of subscribers.
func (m *Metrics) SetBrokerSubscribers(n int) {
	if m == nil {
		return
	}
	m.brokerSubs.Set(float64(n))
}

// --- Metrics middleware ---------------------------------------------------

// WithMetrics wraps a CompleteFunc to emit Prometheus observations. Pairs
// naturally with WithLogging, WithRetry, WithCache:
//
//	complete := Chain(realLLM,
//	    WithCache(cache),
//	    WithMetrics(metrics, "anthropic"),
//	    WithRetry(3, time.Second),
//	    WithLogging(logger),
//	)
//
// Records call count, duration, status, and approximate token usage.
// Token counts use the same approxTokens heuristic as WithTokenAccounting
// (chars/4) — for exact counts, pair with a tokenizer-backed middleware.
//
// backend names the underlying provider for label values ("anthropic",
// "claude-code", "scripted") so dashboards can be sliced by provider.
func WithMetrics(metrics *Metrics, backend string) Middleware {
	return func(next CompleteFunc) CompleteFunc {
		return func(ctx context.Context, system, user string) (string, error) {
			start := time.Now()
			out, err := next(ctx, system, user)
			dur := time.Since(start)

			status := "success"
			if err != nil {
				status = "error"
			}
			metrics.ObserveLLMCall(backend, status, dur)
			metrics.ObserveLLMTokens(
				approxTokens(system)+approxTokens(user),
				approxTokens(out),
			)
			return out, err
		}
	}
}
