// Package agent — review_workflow.go implements the PR-review DAG as a
// Temporal workflow. This is the workflow the web UI invokes via
// cmd/api-server.
//
// Shape:
//
//	         ┌─> security_audit ─┐
//	parse ───┼─> test_coverage  ─┼─> synthesize
//	         └─> style_check    ─┘
//
// Each node is a Temporal activity. The workflow code runs a tiny
// topological dispatcher: it executes nodes in layers, with each layer's
// nodes running in parallel via workflow.Future.
//
// Why a workflow per DAG (and not just DAG.Execute inside one activity)?
//
//	Durability granularity. If you kill the worker mid-DAG, only the
//	in-flight activities restart on the next worker; completed ones are
//	read from the event history. With one big activity, the whole DAG
//	restarts.
//
//	Visibility. Each node shows up in the Temporal Web UI as its own
//	activity, with its own duration, input, output, and retry count.
//	You can see exactly where a DAG run spent its time.
//
// The LLM seam: the activity functions accept a CompleteFunc via the
// Activities struct (the existing pattern). Streaming is automatic when
// the underlying CompleteFunc is wrapped via the stream adapter; the
// activity emits TokenChunkEvents to the broker as chunks arrive.
package agent

import (
	"context"
	"fmt"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// ReviewWorkflowName is the registered name for the PR-review DAG workflow.
const ReviewWorkflowName = "PRReviewWorkflow"

// Activity name constants for the review DAG. These match the node IDs
// the UI expects, so a single string can drive both the workflow
// dispatch and the UI visualization.
const (
	ParseDiffActivityName     = "ParseDiff"
	SecurityAuditActivityName = "SecurityAudit"
	TestCoverageActivityName  = "TestCoverage"
	StyleCheckActivityName    = "StyleCheck"
	SynthesizeReviewActivity  = "SynthesizeReview"
)

// PRReviewInput is the workflow input.
type PRReviewInput struct {
	// Diff is the unified diff to review. For the demo it can be a
	// short text; in production this would be a real `git diff` output
	// possibly truncated to fit the model's context window.
	Diff string
	// StreamingBackend, if true, signals the activities to use the
	// streaming completion path so TokenChunkEvents flow to the broker.
	StreamingBackend bool
}

// PRReviewOutput is the workflow result.
type PRReviewOutput struct {
	Parse      string
	Security   string
	Tests      string
	Style      string
	Synthesis  string
	DurationMs int64
}

// PRReviewWorkflow executes the PR-review DAG as Temporal activities.
//
// Layer 0:  ParseDiff
// Layer 1:  SecurityAudit, TestCoverage, StyleCheck (parallel)
// Layer 2:  SynthesizeReview
//
// All four LLM-backed activities use Activities.Complete. The parse step
// is a stub today (just acknowledges the diff exists); in production it
// would call the LLM with a "list the changed files" prompt.
func PRReviewWorkflow(ctx workflow.Context, in PRReviewInput) (PRReviewOutput, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("PRReviewWorkflow starting")
	start := workflow.Now(ctx)

	if in.Diff == "" {
		return PRReviewOutput{}, temporal.NewNonRetryableApplicationError(
			"Diff must not be empty", "InvalidInput", nil)
	}

	// Activity options. StartToCloseTimeout is the per-attempt budget;
	// for an LLM call we allow 3 minutes to accommodate slow backends
	// (claude-code can take 30-60s on cold cache; anthropic streaming
	// can take longer for long responses).
	//
	// We deliberately do NOT set HeartbeatTimeout. Heartbeats are for
	// activities with meaningful progress to report (e.g. "30% through
	// a 1GB file"). An LLM call has no progress between "started" and
	// "got response", so a heartbeat would just be a liveness ping —
	// and StartToCloseTimeout already provides that. Setting both with
	// a heartbeat shorter than StartToClose would kill activities that
	// are simply slow but progressing fine, which is exactly the bug
	// we hit on slow claude-code responses.
	actCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 3 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        time.Second,
			BackoffCoefficient:     2.0,
			MaximumAttempts:        3,
			NonRetryableErrorTypes: []string{"InvalidInput", "InvalidLLMResponse", "ConfigurationError"},
		},
	})

	// Layer 0: parse.
	var parseOut string
	if err := workflow.ExecuteActivity(actCtx, ParseDiffActivityName, in).
		Get(ctx, &parseOut); err != nil {
		return PRReviewOutput{}, fmt.Errorf("parse_diff failed: %w", err)
	}

	// Layer 1: three analyzers in parallel.
	// We spawn all three, then wait on all. If any single one fails, the
	// workflow fails; for "swallow failures" semantics, you'd collect
	// errors instead.
	secFuture := workflow.ExecuteActivity(actCtx, SecurityAuditActivityName, AnalyzerInput{
		NodeID:    "security_audit",
		ParseText: parseOut,
		Streaming: in.StreamingBackend,
	})
	testFuture := workflow.ExecuteActivity(actCtx, TestCoverageActivityName, AnalyzerInput{
		NodeID:    "test_coverage",
		ParseText: parseOut,
		Streaming: in.StreamingBackend,
	})
	styleFuture := workflow.ExecuteActivity(actCtx, StyleCheckActivityName, AnalyzerInput{
		NodeID:    "style_check",
		ParseText: parseOut,
		Streaming: in.StreamingBackend,
	})

	var secOut, testOut, styleOut string
	if err := secFuture.Get(ctx, &secOut); err != nil {
		return PRReviewOutput{}, fmt.Errorf("security_audit failed: %w", err)
	}
	if err := testFuture.Get(ctx, &testOut); err != nil {
		return PRReviewOutput{}, fmt.Errorf("test_coverage failed: %w", err)
	}
	if err := styleFuture.Get(ctx, &styleOut); err != nil {
		return PRReviewOutput{}, fmt.Errorf("style_check failed: %w", err)
	}

	// Layer 2: synthesize.
	var synth string
	if err := workflow.ExecuteActivity(actCtx, SynthesizeReviewActivity, SynthesizeInput{
		NodeID:    "synthesize",
		Security:  secOut,
		Tests:     testOut,
		Style:     styleOut,
		Streaming: in.StreamingBackend,
	}).Get(ctx, &synth); err != nil {
		return PRReviewOutput{}, fmt.Errorf("synthesize failed: %w", err)
	}

	return PRReviewOutput{
		Parse:      parseOut,
		Security:   secOut,
		Tests:      testOut,
		Style:      styleOut,
		Synthesis:  synth,
		DurationMs: workflow.Now(ctx).Sub(start).Milliseconds(),
	}, nil
}

// AnalyzerInput is the input to all three analyzer activities (security,
// tests, style). They share an input shape because they do the same
// kind of work — call the LLM with a node-specific prompt over the
// parse output.
type AnalyzerInput struct {
	NodeID    string // "security_audit" | "test_coverage" | "style_check"
	ParseText string // output of ParseDiff
	Streaming bool   // use streaming LLM if true
}

// SynthesizeInput is the input to the final synthesize activity.
type SynthesizeInput struct {
	NodeID    string
	Security  string
	Tests     string
	Style     string
	Streaming bool
}

// --- Activity implementations ---------------------------------------------

// ParseDiff is a stub today. It records that the diff was received and
// returns a brief summary. In production this would call the LLM with
// a "extract changed files and key changes" prompt.
//
// Defined on Activities so it has access to a.Complete and emits events
// through the standard ctx-carried Emitter.
func (a *Activities) ParseDiff(ctx context.Context, in PRReviewInput) (string, error) {
	emitter := EmitterForActivity(ctx)
	start := time.Now()
	emitter.Emit(NewActivityStarted("", ParseDiffActivityName, "parse_diff"))

	// Stub work: pretend we parsed. 200ms feels intentional and gives
	// the UI time to show the node-running state.
	time.Sleep(200 * time.Millisecond)
	out := fmt.Sprintf("Parsed diff (%d chars). Files appear to touch: agent/, cmd/, worker/.", len(in.Diff))

	emitter.Emit(NewActivityCompleted("", ParseDiffActivityName, "parse_diff", nil, time.Since(start)))
	return out, nil
}

// SecurityAudit is the security analyzer activity.
func (a *Activities) SecurityAudit(ctx context.Context, in AnalyzerInput) (string, error) {
	return runAnalyzer(ctx, a, in, SecurityAuditActivityName,
		"You are a security-focused code reviewer.",
		"Audit this change for security risks: SQL injection, unsanitized user input, hard-coded secrets, path traversal, insecure deserialization, missing authorization checks. Be concise (3-4 sentences). The change summary follows:\n\n"+in.ParseText)
}

// TestCoverage is the test-coverage analyzer activity.
func (a *Activities) TestCoverage(ctx context.Context, in AnalyzerInput) (string, error) {
	return runAnalyzer(ctx, a, in, TestCoverageActivityName,
		"You are a code-quality reviewer focused on testing.",
		"Assess whether this change has adequate test coverage. Look for: new functions without tests, edge cases not exercised, integration tests missing. Be concise (3-4 sentences). The change summary follows:\n\n"+in.ParseText)
}

// StyleCheck is the style/lint analyzer activity.
func (a *Activities) StyleCheck(ctx context.Context, in AnalyzerInput) (string, error) {
	return runAnalyzer(ctx, a, in, StyleCheckActivityName,
		"You are a code-style reviewer focused on Go idioms.",
		"Review this change for style and idiom issues: naming, error handling, package conventions, godoc completeness, gofmt compliance. Be concise (3-4 sentences). The change summary follows:\n\n"+in.ParseText)
}

// SynthesizeReview is the final-merge activity. Takes all three analyzer
// outputs and produces a unified review.
func (a *Activities) SynthesizeReview(ctx context.Context, in SynthesizeInput) (string, error) {
	if a.Complete == nil {
		return "", temporal.NewNonRetryableApplicationError(
			"Activities.Complete is nil", "ConfigurationError", nil)
	}
	emitter := EmitterForActivity(ctx)
	start := time.Now()
	emitter.Emit(NewActivityStarted("", SynthesizeReviewActivity, in.NodeID))

	ctx, span := StartActivitySpan(ctx, SynthesizeReviewActivity)
	defer span.End()

	system := "You are a senior code reviewer combining three specialist reviews into one final verdict."
	user := fmt.Sprintf(
		"Synthesize the following three reviews into one concise final verdict (5-7 sentences). End with **APPROVE** or **REQUEST CHANGES**.\n\n"+
			"## Security review\n%s\n\n## Test coverage review\n%s\n\n## Style review\n%s",
		in.Security, in.Tests, in.Style,
	)

	out, err := callLLMWithStreaming(ctx, a, system, user, in.NodeID, in.Streaming)
	dur := time.Since(start)
	emitter.Emit(NewActivityCompleted("", SynthesizeReviewActivity, in.NodeID, err, dur))
	if err != nil {
		RecordError(span, err)
		return "", err
	}
	return out, nil
}

// runAnalyzer is the shared body of the three analyzer activities. It
// emits the activity-started event, calls the LLM (streaming or not),
// records the duration, and returns the assembled response.
func runAnalyzer(ctx context.Context, a *Activities, in AnalyzerInput, activityName, system, user string) (string, error) {
	if a.Complete == nil {
		return "", temporal.NewNonRetryableApplicationError(
			"Activities.Complete is nil", "ConfigurationError", nil)
	}
	emitter := EmitterForActivity(ctx)
	start := time.Now()
	emitter.Emit(NewActivityStarted("", activityName, in.NodeID))

	ctx, span := StartActivitySpan(ctx, activityName)
	defer span.End()

	out, err := callLLMWithStreaming(ctx, a, system, user, in.NodeID, in.Streaming)
	dur := time.Since(start)
	emitter.Emit(NewActivityCompleted("", activityName, in.NodeID, err, dur))
	if err != nil {
		RecordError(span, err)
		return "", err
	}
	return out, nil
}

// callLLMWithStreaming routes the LLM call through the streaming seam
// if a streaming backend is wired up, otherwise falls back to the
// atomic CompleteFunc. Either way the activity returns the assembled
// string for Temporal's event history.
//
// callID is stamped on TokenChunkEvents so the UI can attribute chunks
// to a specific node — we use the NodeID for this since one activity
// = one node = one LLM call in this workflow.
func callLLMWithStreaming(ctx context.Context, a *Activities, system, user, callID string, streaming bool) (string, error) {
	if streaming && a.Stream != nil {
		return CompleteWithStreaming(ctx, a.Stream, a.Complete, system, user, callID)
	}
	// Non-streaming path: still emit a single TokenChunkEvent so the UI
	// shows the response appearing (rather than nothing during the call,
	// followed by a sudden NodeCompleted with the whole text).
	return CompleteWithStreaming(ctx, nil, a.Complete, system, user, callID)
}
