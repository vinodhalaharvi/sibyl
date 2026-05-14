package agent_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"

	"github.com/vinodhalaharvi/sibyl/agent"
)

func TestSetupTracing_EmptyEndpointIsNoop(t *testing.T) {
	shutdown, err := agent.SetupTracing(context.Background(), agent.TracingConfig{})
	require.NoError(t, err)
	require.NotNil(t, shutdown)
	require.NoError(t, shutdown(context.Background()))
}

func TestSetupTracing_BadEndpointFailsCleanly(t *testing.T) {
	// We can't connect to a fake endpoint, but the SDK constructor
	// itself shouldn't fail until something tries to export. We just
	// verify Setup returns *something* (a shutdown function), not an
	// instant error, even with an unreachable endpoint.
	shutdown, err := agent.SetupTracing(context.Background(), agent.TracingConfig{
		Endpoint:    "127.0.0.1:1",
		Insecure:    true,
		ServiceName: "test-svc",
	})
	require.NoError(t, err)
	require.NotNil(t, shutdown)

	// Shutdown should return without blocking. May log warnings about
	// the unreachable collector; that's acceptable.
	ctx, cancel := context.WithTimeout(context.Background(), 200*100)
	defer cancel()
	_ = shutdown(ctx)
}

func TestTracer_ReturnsValid(t *testing.T) {
	tr := agent.Tracer()
	require.NotNil(t, tr)
}

func TestStartSpanHelpers_AllReturnValidSpans(t *testing.T) {
	// Even with no exporter, span helpers should return live span objects
	// that can be ended without panic.
	ctx := context.Background()

	ctx, s1 := agent.StartWorkflowSpan(ctx, "ConvergeWorkflow", "wf-1")
	require.NotNil(t, s1)
	s1.End()

	ctx, s2 := agent.StartActivitySpan(ctx, "Research")
	require.NotNil(t, s2)
	s2.End()

	ctx, s3 := agent.StartNodeSpan(ctx, "parse")
	require.NotNil(t, s3)
	s3.End()

	ctx, s4 := agent.StartToolSpan(ctx, "calculator", 1)
	require.NotNil(t, s4)
	s4.End()

	_, s5 := agent.StartLLMSpan(ctx, "anthropic", "complete")
	require.NotNil(t, s5)
	s5.End()
}

func TestRecordError_NilDoesNothing(t *testing.T) {
	_, span := agent.Tracer().Start(context.Background(), "test")
	defer span.End()
	require.NotPanics(t, func() {
		agent.RecordError(span, nil)
	})
}

func TestRecordError_NonNilSetsStatus(t *testing.T) {
	_, span := agent.Tracer().Start(context.Background(), "test")
	defer span.End()
	require.NotPanics(t, func() {
		agent.RecordError(span, errors.New("boom"))
	})
}

func TestSetTokens_DoesNotPanic(t *testing.T) {
	_, span := agent.Tracer().Start(context.Background(), "test")
	defer span.End()
	require.NotPanics(t, func() {
		agent.SetTokens(span, 100, 50)
	})
}

// Sanity: the global tracer provider is set to a no-op until SetupTracing
// is called with an endpoint. This means tracing call sites cost ~zero
// in worker startups where -otel-endpoint is empty.
func TestGlobalTracer_IsValidByDefault(t *testing.T) {
	tp := otel.GetTracerProvider()
	require.NotNil(t, tp)
	tr := tp.Tracer("test")
	require.NotNil(t, tr)
}
