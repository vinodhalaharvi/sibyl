package agents_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/vinodhalaharvi/weft/weft"

	"github.com/vinodhalaharvi/sibyl/agents"
	"github.com/vinodhalaharvi/sibyl/channels"
	channelstest "github.com/vinodhalaharvi/sibyl/channels/test"
)

// === Helpers ===

// Finding is a typical domain output that carries severity.
type Finding struct {
	ID         string
	Title      string
	Body       string
	Severity   channels.Severity
	Confidence float64
}

// Implement the SeverityCarrier interface so the SeverityAtLeast helper
// can be used with this type.
func (f Finding) GetSeverity() channels.Severity { return f.Severity }
func (f Finding) GetConfidence() float64         { return f.Confidence }

// renderFinding turns a Finding into a Message — what a policy's RenderFn
// would typically do.
func renderFinding(_ context.Context, f Finding) channels.Message {
	return channels.Message{
		Subject:  channels.Subject{Kind: "finding", ID: f.ID},
		Title:    f.Title,
		Body:     f.Body,
		Severity: f.Severity,
		Actions: []channels.Action{
			{ID: "accept", Label: "Confirm"},
			{ID: "reject", Label: "False positive"},
			{ID: "snooze", Label: "Defer"},
		},
	}
}

// testDispatcher builds a Dispatcher backed by one in-memory channel.
func testDispatcher(t *testing.T) (*channels.Dispatcher, *channelstest.Channel) {
	t.Helper()
	mem := channelstest.New("inmem")
	d, err := channels.New(
		channels.WithChannel(mem.Channel()),
		channels.WithRouter(channels.FixedRouter("inmem", "default")),
	)
	if err != nil {
		t.Fatal(err)
	}
	return d, mem
}

// === Register ===

func TestRegisterRequiresID(t *testing.T) {
	_, err := agents.Register(agents.Spec[int, int]{
		Run: weft.Id[int](),
	})
	if err == nil {
		t.Error("expected error for missing ID")
	}
}

func TestRegisterRequiresRun(t *testing.T) {
	_, err := agents.Register(agents.Spec[int, int]{
		ID: "OktaScanner",
	})
	if err == nil {
		t.Error("expected error for missing Run")
	}
}

func TestRegisterStoresMetadata(t *testing.T) {
	a, err := agents.Register(agents.Spec[int, int]{
		ID:      "OktaScanner",
		Vendors: []string{"okta"},
		Run:     weft.Id[int](),
	})
	if err != nil {
		t.Fatal(err)
	}
	if a.ID != "OktaScanner" {
		t.Errorf("ID = %q, want OktaScanner", a.ID)
	}
	if len(a.Vendors) != 1 || a.Vendors[0] != "okta" {
		t.Errorf("Vendors = %v, want [okta]", a.Vendors)
	}
}

func TestRegisterRunsThroughBehaviors(t *testing.T) {
	// Two no-op behaviors that mark the request to prove they ran in order.
	calls := []string{}
	behavior := func(name string) agents.Behavior[int, int] {
		return func(inner weft.Arrow[int, int]) weft.Arrow[int, int] {
			return func(ctx context.Context, i int) (int, error) {
				calls = append(calls, name+":before")
				out, err := inner(ctx, i)
				calls = append(calls, name+":after")
				return out, err
			}
		}
	}
	a, err := agents.Register(agents.Spec[int, int]{
		ID:  "Composite",
		Run: weft.Id[int](),
		Behaviors: []agents.Behavior[int, int]{
			behavior("inner"),
			behavior("outer"),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = a.Run(context.Background(), 42)
	if err != nil {
		t.Fatal(err)
	}
	// Order: outer.before -> inner.before -> id -> inner.after -> outer.after
	want := []string{"outer:before", "inner:before", "inner:after", "outer:after"}
	if !equal(calls, want) {
		t.Errorf("call order = %v, want %v", calls, want)
	}
}

func TestRegisterRejectsNilBehavior(t *testing.T) {
	_, err := agents.Register(agents.Spec[int, int]{
		ID:        "X",
		Run:       weft.Id[int](),
		Behaviors: []agents.Behavior[int, int]{nil},
	})
	if err == nil {
		t.Error("expected error for nil behavior")
	}
}

func TestMustRegisterPanicsOnError(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected MustRegister to panic")
		}
	}()
	agents.MustRegister(agents.Spec[int, int]{}) // missing ID + Run
}

// === Agent context ===

func TestAgentContextRoundTrip(t *testing.T) {
	ac := agents.AgentContext{
		AgentID:    "X",
		WorkflowID: "wf-1",
		RunID:      "run-1",
		InvokedBy:  "okta:00u1",
	}
	ctx := agents.WithAgentContext(context.Background(), ac)
	got := agents.AgentContextFrom(ctx)
	if got.AgentID != ac.AgentID || got.WorkflowID != ac.WorkflowID ||
		got.RunID != ac.RunID || got.InvokedBy != ac.InvokedBy {
		t.Errorf("round-trip mismatch: got %+v want %+v", got, ac)
	}
}

func TestAgentContextZeroValueIfMissing(t *testing.T) {
	got := agents.AgentContextFrom(context.Background())
	if got.AgentID != "" || got.WorkflowID != "" || got.RunID != "" || got.InvokedBy != "" {
		t.Errorf("expected zero value, got %+v", got)
	}
}

// === HITL behavior ===

func TestWithHumanVerdict_AcceptReturnsOutput(t *testing.T) {
	d, mem := testDispatcher(t)
	mem.EnqueueVerdict(channels.Verdict{Choice: "accept"})

	scanner := agents.MustRegister(agents.Spec[string, Finding]{
		ID:  "Scanner",
		Run: weft.Pure(func(_ string) Finding { return Finding{ID: "F1", Title: "Stale", Severity: channels.SeverityCritical} }),
		Behaviors: []agents.Behavior[string, Finding]{
			agents.WithHumanVerdict[string, Finding](d, agents.AlwaysAskPolicy[Finding]{
				RenderFn: renderFinding,
				Timeout:  50 * time.Millisecond,
			}),
		},
	})

	got, err := scanner.Run(context.Background(), "/scan")
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != "F1" {
		t.Errorf("expected output to pass through on accept, got %+v", got)
	}
	if len(mem.Posts()) != 1 {
		t.Errorf("expected 1 post, got %d", len(mem.Posts()))
	}
}

func TestWithHumanVerdict_RejectReturnsZeroByDefault(t *testing.T) {
	d, mem := testDispatcher(t)
	mem.EnqueueVerdict(channels.Verdict{Choice: "reject"})

	scanner := agents.MustRegister(agents.Spec[string, Finding]{
		ID:  "Scanner",
		Run: weft.Pure(func(_ string) Finding { return Finding{ID: "F1", Title: "Stale", Severity: channels.SeverityCritical} }),
		Behaviors: []agents.Behavior[string, Finding]{
			agents.WithHumanVerdict[string, Finding](d, agents.AlwaysAskPolicy[Finding]{
				RenderFn: renderFinding,
				Timeout:  50 * time.Millisecond,
			}),
		},
	})
	got, err := scanner.Run(context.Background(), "/scan")
	if err != nil {
		t.Fatal(err)
	}
	if (got != Finding{}) {
		t.Errorf("expected zero value on reject, got %+v", got)
	}
}

func TestWithHumanVerdict_SnoozeReturnsErrSnoozed(t *testing.T) {
	d, mem := testDispatcher(t)
	mem.EnqueueVerdict(channels.Verdict{Choice: "snooze"})

	scanner := agents.MustRegister(agents.Spec[string, Finding]{
		ID:  "Scanner",
		Run: weft.Pure(func(_ string) Finding { return Finding{ID: "F1", Severity: channels.SeverityCritical} }),
		Behaviors: []agents.Behavior[string, Finding]{
			agents.WithHumanVerdict[string, Finding](d, agents.AlwaysAskPolicy[Finding]{
				RenderFn: renderFinding,
				Timeout:  50 * time.Millisecond,
			}),
		},
	})

	_, err := scanner.Run(context.Background(), "/scan")
	if !errors.Is(err, agents.ErrSnoozed) {
		t.Errorf("expected ErrSnoozed, got %v", err)
	}
}

func TestWithHumanVerdict_TimeoutFailsOpenByDefault(t *testing.T) {
	d, _ := testDispatcher(t) // no verdict queued

	scanner := agents.MustRegister(agents.Spec[string, Finding]{
		ID:  "Scanner",
		Run: weft.Pure(func(_ string) Finding { return Finding{ID: "F1", Severity: channels.SeverityCritical} }),
		Behaviors: []agents.Behavior[string, Finding]{
			agents.WithHumanVerdict[string, Finding](d, agents.AlwaysAskPolicy[Finding]{
				RenderFn: renderFinding,
				Timeout:  20 * time.Millisecond,
			}),
		},
	})

	got, err := scanner.Run(context.Background(), "/scan")
	if err != nil {
		t.Fatalf("expected fail-open (no error) on timeout, got %v", err)
	}
	if got.ID != "F1" {
		t.Errorf("expected output preserved on timeout, got %+v", got)
	}
}

func TestWithHumanVerdict_PredicateSkipsLowSeverity(t *testing.T) {
	d, mem := testDispatcher(t)
	// Don't queue any verdict — if HITL did engage, this would time out
	// fail-open. But we expect HITL to be skipped, so no post should fire.

	policy := agents.WithPredicate[Finding](
		agents.AlwaysAskPolicy[Finding]{RenderFn: renderFinding, Timeout: 20 * time.Millisecond},
		agents.SeverityAtLeast[Finding](channels.SeverityHigh),
	)

	scanner := agents.MustRegister(agents.Spec[string, Finding]{
		ID:  "Scanner",
		Run: weft.Pure(func(_ string) Finding { return Finding{ID: "F-low", Severity: channels.SeverityLow} }),
		Behaviors: []agents.Behavior[string, Finding]{
			agents.WithHumanVerdict[string, Finding](d, policy),
		},
	})

	got, err := scanner.Run(context.Background(), "/scan")
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != "F-low" {
		t.Errorf("expected pass-through, got %+v", got)
	}
	if len(mem.Posts()) != 0 {
		t.Errorf("expected NO posts (predicate should skip), got %d", len(mem.Posts()))
	}
}

func TestWithHumanVerdict_PredicateEngagesForCritical(t *testing.T) {
	d, mem := testDispatcher(t)
	mem.EnqueueVerdict(channels.Verdict{Choice: "accept"})

	policy := agents.WithPredicate[Finding](
		agents.AlwaysAskPolicy[Finding]{RenderFn: renderFinding, Timeout: 50 * time.Millisecond},
		agents.SeverityAtLeast[Finding](channels.SeverityHigh),
	)

	scanner := agents.MustRegister(agents.Spec[string, Finding]{
		ID:  "Scanner",
		Run: weft.Pure(func(_ string) Finding { return Finding{ID: "F-crit", Severity: channels.SeverityCritical} }),
		Behaviors: []agents.Behavior[string, Finding]{
			agents.WithHumanVerdict[string, Finding](d, policy),
		},
	})

	got, err := scanner.Run(context.Background(), "/scan")
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != "F-crit" {
		t.Errorf("expected output after accept, got %+v", got)
	}
	if len(mem.Posts()) != 1 {
		t.Errorf("expected 1 post (predicate should engage), got %d", len(mem.Posts()))
	}
}

func TestWithHumanVerdict_NeverAskPolicySkipsEntirely(t *testing.T) {
	d, mem := testDispatcher(t)

	scanner := agents.MustRegister(agents.Spec[string, Finding]{
		ID:  "Scanner",
		Run: weft.Pure(func(_ string) Finding { return Finding{ID: "F1", Severity: channels.SeverityCritical} }),
		Behaviors: []agents.Behavior[string, Finding]{
			agents.WithHumanVerdict[string, Finding](d, agents.NeverAskPolicy[Finding]{}),
		},
	})

	got, err := scanner.Run(context.Background(), "/scan")
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != "F1" {
		t.Errorf("expected pass-through, got %+v", got)
	}
	if len(mem.Posts()) != 0 {
		t.Errorf("expected 0 posts with NeverAskPolicy, got %d", len(mem.Posts()))
	}
}

func TestWithHumanVerdict_InnerErrorShortCircuits(t *testing.T) {
	d, mem := testDispatcher(t)

	scanner := agents.MustRegister(agents.Spec[string, Finding]{
		ID: "Scanner",
		Run: func(_ context.Context, _ string) (Finding, error) {
			return Finding{}, errors.New("scanner exploded")
		},
		Behaviors: []agents.Behavior[string, Finding]{
			agents.WithHumanVerdict[string, Finding](d, agents.AlwaysAskPolicy[Finding]{
				RenderFn: renderFinding,
				Timeout:  50 * time.Millisecond,
			}),
		},
	})

	_, err := scanner.Run(context.Background(), "/scan")
	if err == nil || !strings.Contains(err.Error(), "scanner exploded") {
		t.Errorf("expected scanner error to propagate, got %v", err)
	}
	if len(mem.Posts()) != 0 {
		t.Errorf("expected no posts when inner errored, got %d", len(mem.Posts()))
	}
}

func TestWithHumanVerdict_WorkflowContextAttachesRef(t *testing.T) {
	d, mem := testDispatcher(t)
	mem.EnqueueVerdict(channels.Verdict{Choice: "accept"})

	scanner := agents.MustRegister(agents.Spec[string, Finding]{
		ID:  "Scanner",
		Run: weft.Pure(func(_ string) Finding { return Finding{ID: "F1", Title: "x", Severity: channels.SeverityCritical} }),
		Behaviors: []agents.Behavior[string, Finding]{
			agents.WithHumanVerdict[string, Finding](d, agents.AlwaysAskPolicy[Finding]{
				RenderFn: renderFinding,
				Timeout:  50 * time.Millisecond,
			}),
		},
	})

	ctx := agents.WithAgentContext(context.Background(), agents.AgentContext{
		AgentID:    "Scanner",
		WorkflowID: "wf-42",
		RunID:      "run-7",
	})
	_, err := scanner.Run(ctx, "/scan")
	if err != nil {
		t.Fatal(err)
	}
	posts := mem.Posts()
	if len(posts) != 1 {
		t.Fatalf("expected 1 post, got %d", len(posts))
	}
	if posts[0].Message.WorkflowRef.WorkflowID != "wf-42" {
		t.Errorf("WorkflowRef.WorkflowID = %q, want wf-42", posts[0].Message.WorkflowRef.WorkflowID)
	}
	if got := posts[0].Message.Metadata["agent_id"]; got != "Scanner" {
		t.Errorf("Metadata.agent_id = %q, want Scanner", got)
	}
}

// === Predicates ===

func TestSeverityAtLeast(t *testing.T) {
	pred := agents.SeverityAtLeast[Finding](channels.SeverityHigh)
	cases := []struct {
		sev  channels.Severity
		want bool
	}{
		{channels.SeverityCritical, true},
		{channels.SeverityHigh, true},
		{channels.SeverityMedium, false},
		{channels.SeverityLow, false},
		{channels.SeverityInfo, false},
		{channels.Severity(""), false},
	}
	for _, c := range cases {
		got := pred(context.Background(), Finding{Severity: c.sev})
		if got != c.want {
			t.Errorf("SeverityAtLeast(HIGH)(%v) = %v, want %v", c.sev, got, c.want)
		}
	}
}

func TestSeverityIn(t *testing.T) {
	pred := agents.SeverityIn[Finding](channels.SeverityCritical, channels.SeverityLow)
	cases := []struct {
		sev  channels.Severity
		want bool
	}{
		{channels.SeverityCritical, true},
		{channels.SeverityHigh, false}, // not in set
		{channels.SeverityLow, true},
		{channels.SeverityInfo, false},
	}
	for _, c := range cases {
		got := pred(context.Background(), Finding{Severity: c.sev})
		if got != c.want {
			t.Errorf("SeverityIn(CRIT,LOW)(%v) = %v, want %v", c.sev, got, c.want)
		}
	}
}

func TestConfidenceBelow(t *testing.T) {
	pred := agents.ConfidenceBelow[Finding](0.7)
	if !pred(context.Background(), Finding{Confidence: 0.5}) {
		t.Error("0.5 < 0.7 should be true")
	}
	if pred(context.Background(), Finding{Confidence: 0.9}) {
		t.Error("0.9 < 0.7 should be false")
	}
}

func TestAnyOfPredicate(t *testing.T) {
	pred := agents.AnyOfPredicate[Finding](
		agents.SeverityAtLeast[Finding](channels.SeverityCritical),
		agents.ConfidenceBelow[Finding](0.5),
	)
	// Low severity but low confidence → engage.
	if !pred(context.Background(), Finding{Severity: channels.SeverityLow, Confidence: 0.3}) {
		t.Error("low confidence should trigger AnyOf")
	}
	// High severity but high confidence → engage.
	if !pred(context.Background(), Finding{Severity: channels.SeverityCritical, Confidence: 0.9}) {
		t.Error("critical severity should trigger AnyOf")
	}
	// Neither → skip.
	if pred(context.Background(), Finding{Severity: channels.SeverityMedium, Confidence: 0.9}) {
		t.Error("neither condition should be false")
	}
}

func TestAllOfPredicate(t *testing.T) {
	pred := agents.AllOfPredicate[Finding](
		agents.SeverityAtLeast[Finding](channels.SeverityHigh),
		agents.ConfidenceBelow[Finding](0.7),
	)
	// High AND low-confidence → true.
	if !pred(context.Background(), Finding{Severity: channels.SeverityCritical, Confidence: 0.3}) {
		t.Error("both conditions met should be true")
	}
	// High but high-confidence → false.
	if pred(context.Background(), Finding{Severity: channels.SeverityCritical, Confidence: 0.9}) {
		t.Error("confidence too high should be false")
	}
	// Low but low-confidence → false.
	if pred(context.Background(), Finding{Severity: channels.SeverityLow, Confidence: 0.3}) {
		t.Error("severity too low should be false")
	}
}

// === Misc ===

func equal[T comparable](a, b []T) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
