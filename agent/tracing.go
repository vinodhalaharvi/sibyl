// Package agent — tracing.go provides OpenTelemetry tracing instrumentation.
//
// Why OTel alongside the in-memory event broker?
//
//	The event broker (broker.go) is for LIVE UI streaming: HTTP/SSE
//	handlers subscribe and forward to browsers. It dies with the worker.
//
//	OTel tracing is for POST-HOC OPERATIONAL ANALYSIS: spans go to an
//	external collector (Jaeger, Tempo, Honeycomb, Datadog) where they
//	persist and can be correlated across services. A trace from a user
//	request can show:
//
//	  Temporal workflow started
//	    └─ ConvergeWorkflow
//	        ├─ Research activity
//	        │   └─ Anthropic Messages API call
//	        ├─ Critique activity
//	        │   └─ Anthropic Messages API call
//	        └─ Research activity (retry)
//	            └─ Anthropic Messages API call
//
//	The two systems serve different audiences (user/UI vs operator/SRE)
//	and don't fight. Both are wired into the same call sites.
//
// Initialization:
//
//	The worker calls SetupTracing once at startup. If no exporter is
//	configured (env vars missing), tracing is a no-op — spans go to a
//	disabled provider that costs ~zero. Apps that want tracing set
//	OTEL_EXPORTER_OTLP_ENDPOINT (and friends) per OTel convention.
//
// Span naming convention:
//
//	sibyl.workflow.<name>     — top-level workflow span
//	sibyl.activity.<name>     — Temporal activity span
//	sibyl.dag.node.<id>       — DAG node execution
//	sibyl.tool.<name>         — tool dispatch
//	sibyl.llm.complete        — LLM call (atomic)
//	sibyl.llm.stream          — LLM call (streaming)
package agent

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

// TracerName is the instrumentation library name reported in spans.
// Pick one identifier; don't fragment by package.
const TracerName = "github.com/vinodhalaharvi/sibyl"

// TracingConfig configures the OTel SDK at worker startup.
type TracingConfig struct {
	// ServiceName populates the standard "service.name" resource attribute.
	// Defaults to "sibyl-worker". Override per deployment.
	ServiceName string

	// ServiceVersion populates "service.version". Useful for correlating
	// regressions with deployments.
	ServiceVersion string

	// Endpoint is the OTLP HTTP endpoint, e.g. "http://localhost:4318".
	// If empty, tracing is disabled (no exporter, no overhead).
	Endpoint string

	// Insecure controls whether to use HTTP (true) or HTTPS (false).
	// Local collectors usually want true. Cloud vendors want false.
	Insecure bool

	// Headers are added to OTLP requests — typically for vendor auth
	// (e.g. "x-honeycomb-team: <key>", "DD-API-KEY: <key>").
	Headers map[string]string
}

// SetupTracing initializes the global OTel tracer provider. Returns a
// shutdown function the caller must defer to flush queued spans.
//
// If cfg.Endpoint is empty, returns a no-op shutdown and disables
// tracing globally. Existing tracing calls become free.
func SetupTracing(ctx context.Context, cfg TracingConfig) (func(context.Context) error, error) {
	if cfg.Endpoint == "" {
		// No endpoint → no exporter. Global tracer becomes a no-op.
		// Tracing call sites continue to work but emit no spans.
		return func(context.Context) error { return nil }, nil
	}
	if cfg.ServiceName == "" {
		cfg.ServiceName = "sibyl-worker"
	}

	exporterOpts := []otlptracehttp.Option{
		otlptracehttp.WithEndpoint(cfg.Endpoint),
	}
	if cfg.Insecure {
		exporterOpts = append(exporterOpts, otlptracehttp.WithInsecure())
	}
	if len(cfg.Headers) > 0 {
		exporterOpts = append(exporterOpts, otlptracehttp.WithHeaders(cfg.Headers))
	}

	exporter, err := otlptrace.New(ctx, otlptracehttp.NewClient(exporterOpts...))
	if err != nil {
		return nil, fmt.Errorf("otel: create OTLP exporter: %w", err)
	}

	// Resource attributes go on every span. Keep the set small and
	// stable — these are indexed and queried by name in most backends.
	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName(cfg.ServiceName),
			semconv.ServiceVersion(cfg.ServiceVersion),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("otel: create resource: %w", err)
	}

	// Use a batch span processor — it buffers spans and flushes
	// periodically. The default settings are fine for our throughput.
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)

	// Register the standard W3C propagators so trace context can flow
	// across service boundaries (HTTP headers, etc).
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	return tp.Shutdown, nil
}

// Tracer returns Sibyl's tracer. Cheap to call; the underlying
// TracerProvider caches by name.
func Tracer() trace.Tracer {
	return otel.Tracer(TracerName)
}

// --- Span helpers ---------------------------------------------------------
//
// These helpers reduce boilerplate at instrumentation sites. They follow
// a consistent pattern:
//
//	ctx, span := agent.StartNodeSpan(ctx, nodeID)
//	defer span.End()
//	... work ...
//	if err != nil {
//	    agent.RecordError(span, err)
//	}
//
// The defer-end pattern guarantees spans close even on panic.

// StartWorkflowSpan opens a span for a workflow execution. The name
// includes the workflow type so flame graphs are readable.
//
// Use this in the workflow code (which runs in the worker goroutine,
// so OTel context propagation works normally).
func StartWorkflowSpan(ctx context.Context, workflowType, workflowID string) (context.Context, trace.Span) {
	return Tracer().Start(ctx, "sibyl.workflow."+workflowType,
		trace.WithAttributes(
			attribute.String("workflow.type", workflowType),
			attribute.String("workflow.id", workflowID),
		),
	)
}

// StartActivitySpan opens a span for a Temporal activity. The activity
// name matches the registered activity name (e.g. "Research").
//
// Note: Temporal activities receive a NEW context built by the SDK,
// so spans here are children of whatever was in the activity options'
// HeaderWriter (when propagation is wired) or root spans (when not).
// We don't wire propagation in this patch — that's a follow-up.
func StartActivitySpan(ctx context.Context, activityName string) (context.Context, trace.Span) {
	return Tracer().Start(ctx, "sibyl.activity."+activityName,
		trace.WithAttributes(
			attribute.String("activity.name", activityName),
		),
	)
}

// StartNodeSpan opens a span for a DAG node execution.
func StartNodeSpan(ctx context.Context, nodeID string) (context.Context, trace.Span) {
	return Tracer().Start(ctx, "sibyl.dag.node."+nodeID,
		trace.WithAttributes(
			attribute.String("node.id", nodeID),
		),
	)
}

// StartToolSpan opens a span for a tool dispatch.
func StartToolSpan(ctx context.Context, toolName string, step int) (context.Context, trace.Span) {
	return Tracer().Start(ctx, "sibyl.tool."+toolName,
		trace.WithAttributes(
			attribute.String("tool.name", toolName),
			attribute.Int("tool.step", step),
		),
	)
}

// StartLLMSpan opens a span for an LLM completion call. The backend
// is the backend selector ("anthropic", "claude-code", "scripted")
// so traces can be sliced by provider.
func StartLLMSpan(ctx context.Context, backend, callKind string) (context.Context, trace.Span) {
	return Tracer().Start(ctx, "sibyl.llm."+callKind,
		trace.WithAttributes(
			attribute.String("llm.backend", backend),
			attribute.String("llm.call_kind", callKind),
		),
	)
}

// RecordError marks a span as failed and attaches the error message.
// If err is nil, the span stays in OK status.
func RecordError(span trace.Span, err error) {
	if err == nil {
		return
	}
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
}

// SetTokens attaches token usage attributes to the current span.
// These show up in trace backends as filterable fields.
func SetTokens(span trace.Span, input, output int) {
	span.SetAttributes(
		attribute.Int("llm.tokens.input", input),
		attribute.Int("llm.tokens.output", output),
		attribute.Int("llm.tokens.total", input+output),
	)
}
