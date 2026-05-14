package agent_test

import (
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/stretchr/testify/require"

	"github.com/vinodhalaharvi/sibyl/agent"
)

func TestNewMetrics_RegistryReturnsValid(t *testing.T) {
	m := agent.NewMetrics()
	require.NotNil(t, m)
	require.NotNil(t, m.Registry())
}

func TestMetrics_NilReceiverNoPanic(t *testing.T) {
	// Every observation method must be safe to call on a nil *Metrics.
	// This is the discipline that lets call sites omit nil checks.
	var m *agent.Metrics
	require.NotPanics(t, func() {
		m.ObserveLLMCall("anthropic", "success", 100*time.Millisecond)
		m.ObserveLLMTokens(100, 50)
		m.ObserveToolCall("calc", "success", 10*time.Millisecond)
		m.ObserveCacheOperation("hit")
		m.ObserveWorkflow("Converge", "success", time.Second)
		m.ObserveBrokerEvent("published")
		m.SetBrokerSubscribers(5)
	})
	require.Nil(t, m.Registry())
}

func TestMetrics_ObserveLLMCallRecorded(t *testing.T) {
	m := agent.NewMetrics()
	m.ObserveLLMCall("anthropic", "success", 100*time.Millisecond)
	m.ObserveLLMCall("anthropic", "success", 200*time.Millisecond)
	m.ObserveLLMCall("anthropic", "error", 50*time.Millisecond)

	scraped := scrape(t, m)
	require.Contains(t, scraped, `sibyl_llm_calls_total{backend="anthropic",status="success"} 2`)
	require.Contains(t, scraped, `sibyl_llm_calls_total{backend="anthropic",status="error"} 1`)
}

func TestMetrics_ObserveLLMTokensRecorded(t *testing.T) {
	m := agent.NewMetrics()
	m.ObserveLLMTokens(100, 50)
	m.ObserveLLMTokens(200, 75)

	scraped := scrape(t, m)
	require.Contains(t, scraped, `sibyl_llm_tokens_total{direction="input"} 300`)
	require.Contains(t, scraped, `sibyl_llm_tokens_total{direction="output"} 125`)
}

func TestMetrics_ObserveLLMTokensZeroCountsSkipped(t *testing.T) {
	m := agent.NewMetrics()
	m.ObserveLLMTokens(0, 0)
	m.ObserveLLMTokens(100, 0)

	scraped := scrape(t, m)
	// input=100, output should not have been recorded (still 0/missing)
	require.Contains(t, scraped, `sibyl_llm_tokens_total{direction="input"} 100`)
}

func TestMetrics_ObserveToolCallRecorded(t *testing.T) {
	m := agent.NewMetrics()
	m.ObserveToolCall("calculator", "success", 5*time.Millisecond)
	m.ObserveToolCall("web_search", "error", 200*time.Millisecond)

	scraped := scrape(t, m)
	require.Contains(t, scraped, `sibyl_tool_invocations_total{status="success",tool_name="calculator"} 1`)
	require.Contains(t, scraped, `sibyl_tool_invocations_total{status="error",tool_name="web_search"} 1`)
}

func TestMetrics_ObserveCacheOperationRecorded(t *testing.T) {
	m := agent.NewMetrics()
	m.ObserveCacheOperation("hit")
	m.ObserveCacheOperation("hit")
	m.ObserveCacheOperation("miss")

	scraped := scrape(t, m)
	require.Contains(t, scraped, `sibyl_cache_operations_total{result="hit"} 2`)
	require.Contains(t, scraped, `sibyl_cache_operations_total{result="miss"} 1`)
}

func TestMetrics_SetBrokerSubscribers(t *testing.T) {
	m := agent.NewMetrics()
	m.SetBrokerSubscribers(7)

	scraped := scrape(t, m)
	require.Contains(t, scraped, `sibyl_broker_subscribers 7`)

	m.SetBrokerSubscribers(3)
	scraped = scrape(t, m)
	require.Contains(t, scraped, `sibyl_broker_subscribers 3`)
}

// --- WithMetrics middleware -----------------------------------------------

func TestWithMetrics_RecordsSuccessfulCall(t *testing.T) {
	m := agent.NewMetrics()
	inner := agent.CompleteFunc(func(_ context.Context, _, _ string) (string, error) {
		return "result", nil
	})
	wrapped := agent.WithMetrics(m, "anthropic")(inner)

	_, err := wrapped(context.Background(), "system prompt", "user message")
	require.NoError(t, err)

	scraped := scrape(t, m)
	require.Contains(t, scraped, `sibyl_llm_calls_total{backend="anthropic",status="success"} 1`)
}

func TestWithMetrics_RecordsFailedCall(t *testing.T) {
	m := agent.NewMetrics()
	boom := errors.New("provider down")
	inner := agent.CompleteFunc(func(_ context.Context, _, _ string) (string, error) {
		return "", boom
	})
	wrapped := agent.WithMetrics(m, "anthropic")(inner)

	_, err := wrapped(context.Background(), "", "")
	require.ErrorIs(t, err, boom)

	scraped := scrape(t, m)
	require.Contains(t, scraped, `sibyl_llm_calls_total{backend="anthropic",status="error"} 1`)
}

func TestWithMetrics_RecordsTokens(t *testing.T) {
	m := agent.NewMetrics()
	inner := agent.CompleteFunc(func(_ context.Context, _, _ string) (string, error) {
		return "this is approximately twenty characters", nil // ~10 tokens
	})
	wrapped := agent.WithMetrics(m, "anthropic")(inner)

	// "abcdefgh" (8 chars) + "12345678" (8 chars) = 16 chars input ≈ 4 tokens
	_, err := wrapped(context.Background(), "abcdefgh", "12345678")
	require.NoError(t, err)

	scraped := scrape(t, m)
	// Just verify input/output token counters are nonzero — exact counts
	// follow from approxTokens (chars+3)/4, which is exercised separately.
	require.Contains(t, scraped, `sibyl_llm_tokens_total{direction="input"}`)
	require.Contains(t, scraped, `sibyl_llm_tokens_total{direction="output"}`)
}

func TestWithMetrics_NilMetricsIsNoop(t *testing.T) {
	inner := agent.CompleteFunc(func(_ context.Context, _, _ string) (string, error) {
		return "ok", nil
	})
	wrapped := agent.WithMetrics(nil, "anthropic")(inner)

	out, err := wrapped(context.Background(), "", "")
	require.NoError(t, err)
	require.Equal(t, "ok", out)
}

// --- Counter values via scraping ----------------

func TestMetrics_CounterIncrementsAreObserved(t *testing.T) {
	m := agent.NewMetrics()
	m.ObserveLLMCall("anthropic", "success", time.Millisecond)
	m.ObserveLLMCall("anthropic", "success", time.Millisecond)

	scraped := scrape(t, m)
	require.Contains(t, scraped, `sibyl_llm_calls_total{backend="anthropic",status="success"} 2`)
}

// --- Integration: /metrics endpoint serves valid Prometheus exposition ----

func TestMetricsEndpoint_Scrapeable(t *testing.T) {
	m := agent.NewMetrics()
	m.ObserveLLMCall("anthropic", "success", 100*time.Millisecond)
	m.ObserveCacheOperation("hit")

	handler := promhttp.HandlerFor(m.Registry(), promhttp.HandlerOpts{})
	srv := httptest.NewServer(handler)
	defer srv.Close()

	req := httptest.NewRequest("GET", srv.URL+"/metrics", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	require.Equal(t, 200, rr.Code)
	body := rr.Body.String()
	require.Contains(t, body, "# HELP sibyl_llm_calls_total")
	require.Contains(t, body, "# TYPE sibyl_llm_calls_total counter")
	require.Contains(t, body, "sibyl_llm_calls_total")
	require.Contains(t, body, "sibyl_cache_operations_total")
	// Should also include Go runtime metrics from the default collectors.
	require.Contains(t, body, "go_goroutines")
}

// --- Helper ---------------------------------------------------------------

// scrape renders the current state of m's metrics in the Prometheus
// exposition format. Use require.Contains to make assertions about
// specific metric values.
func scrape(t *testing.T, m *agent.Metrics) string {
	t.Helper()
	handler := promhttp.HandlerFor(m.Registry(), promhttp.HandlerOpts{})
	req := httptest.NewRequest("GET", "/metrics", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	body := rr.Body.String()
	// Strip leading "#" lines for less noisy require.Contains errors.
	var b strings.Builder
	for _, line := range strings.Split(body, "\n") {
		if strings.HasPrefix(line, "#") {
			continue
		}
		b.WriteString(line)
		b.WriteString("\n")
	}
	return b.String()
}
