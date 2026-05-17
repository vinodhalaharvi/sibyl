package slack

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/vinodhalaharvi/sibyl/channels"
)

// fakeClient implements Client for tests. Posts and reactions are scriptable.
type fakeClient struct {
	mu sync.Mutex

	posts []struct {
		channel string
		opts    PostOptions
	}

	// reactionsByTS maps ts → reactions returned by GetReactions.
	// Tests script the expected reactions before triggering Await.
	reactionsByTS map[string][]Reaction

	users map[string]UserInfo

	postErr error
}

func (f *fakeClient) Post(ctx context.Context, channel string, opts PostOptions) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.postErr != nil {
		return "", f.postErr
	}
	idx := len(f.posts)
	f.posts = append(f.posts, struct {
		channel string
		opts    PostOptions
	}{channel, opts})
	return tsFor(idx), nil
}

func (f *fakeClient) GetReactions(ctx context.Context, channel, ts string) ([]Reaction, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	rs, ok := f.reactionsByTS[ts]
	if !ok {
		return nil, nil // no reactions yet
	}
	return rs, nil
}

func (f *fakeClient) GetUserInfo(ctx context.Context, userID string) (UserInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if u, ok := f.users[userID]; ok {
		return u, nil
	}
	return UserInfo{}, errors.New("user not found")
}

func tsFor(i int) string {
	return "1700000000.00000" + itoa(i)
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	const digits = "0123456789"
	var buf [10]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = digits[n%10]
		n /= 10
	}
	return string(buf[i:])
}

// === Tests ===

func TestNewValidatesInputs(t *testing.T) {
	if _, err := New("", &fakeClient{}, Config{}); err == nil {
		t.Error("expected error for empty name")
	}
	if _, err := New("slack", nil, Config{}); err == nil {
		t.Error("expected error for nil client")
	}
}

func TestNotifyPostsAndReturnsReceipt(t *testing.T) {
	fc := &fakeClient{}
	ch, err := New("slack", fc, Config{})
	if err != nil {
		t.Fatal(err)
	}
	r, err := ch.Notify(context.Background(), "C123", channels.Message{
		Title:    "Test finding",
		Body:     "Body content",
		Severity: channels.SeverityHigh,
	})
	if err != nil {
		t.Fatal(err)
	}
	if r.Channel != "slack" || r.Target != "C123" || r.Native == "" {
		t.Errorf("unexpected receipt: %+v", r)
	}
	if len(fc.posts) != 1 || fc.posts[0].channel != "C123" {
		t.Errorf("expected one post to C123, got %+v", fc.posts)
	}
}

func TestNotifyPostErrorPropagates(t *testing.T) {
	fc := &fakeClient{postErr: errors.New("rate limited")}
	ch, _ := New("slack", fc, Config{})
	_, err := ch.Notify(context.Background(), "C123", channels.Message{Title: "x"})
	if err == nil || !strings.Contains(err.Error(), "rate limited") {
		t.Errorf("expected rate-limited error, got %v", err)
	}
}

func TestAwaitFindsVerdictFromReaction(t *testing.T) {
	fc := &fakeClient{
		reactionsByTS: map[string][]Reaction{
			tsFor(0): {{Name: "white_check_mark", Users: []string{"U999"}, Count: 1}},
		},
		users: map[string]UserInfo{
			"U999": {ID: "U999", DisplayName: "vinod", Email: "vinod@acme.com"},
		},
	}
	ch, _ := New("slack", fc, Config{PollInterval: 5 * time.Millisecond})

	r, err := ch.Notify(context.Background(), "C123", channels.Message{Title: "x"})
	if err != nil {
		t.Fatal(err)
	}
	v, err := ch.Await(context.Background(),
		[]channels.Receipt{r},
		channels.AwaitOpts{Timeout: 100 * time.Millisecond},
	)
	if err != nil {
		t.Fatalf("Await error: %v", err)
	}
	if v.Choice != "accept" {
		t.Errorf("Choice = %q, want accept", v.Choice)
	}
	if v.Actor.Canonical != "slack:U999" {
		t.Errorf("Actor.Canonical = %q, want slack:U999", v.Actor.Canonical)
	}
	if v.Actor.DisplayName != "vinod" || v.Actor.Email != "vinod@acme.com" {
		t.Errorf("Actor enrichment failed: %+v", v.Actor)
	}
	if v.Actor.Source != "slack-lookup" {
		t.Errorf("Actor.Source = %q, want slack-lookup", v.Actor.Source)
	}
}

func TestAwaitFallsBackToPassthroughOnUserLookupFailure(t *testing.T) {
	fc := &fakeClient{
		reactionsByTS: map[string][]Reaction{
			tsFor(0): {{Name: "white_check_mark", Users: []string{"U999"}, Count: 1}},
		},
		// no users map → GetUserInfo returns error
	}
	ch, _ := New("slack", fc, Config{PollInterval: 5 * time.Millisecond})
	r, _ := ch.Notify(context.Background(), "C123", channels.Message{Title: "x"})
	v, err := ch.Await(context.Background(),
		[]channels.Receipt{r},
		channels.AwaitOpts{Timeout: 50 * time.Millisecond},
	)
	if err != nil {
		t.Fatalf("Await error: %v", err)
	}
	if v.Actor.Canonical != "slack:U999" {
		t.Errorf("Actor.Canonical = %q, want slack:U999", v.Actor.Canonical)
	}
	if v.Actor.Source != "slack-passthrough" {
		t.Errorf("Actor.Source = %q, want slack-passthrough", v.Actor.Source)
	}
}

func TestAwaitRespectsRestrictedOptions(t *testing.T) {
	fc := &fakeClient{
		reactionsByTS: map[string][]Reaction{
			tsFor(0): {
				// Only reject is present; if restricted to ["accept"], should time out.
				{Name: "x", Users: []string{"U999"}, Count: 1},
			},
		},
	}
	ch, _ := New("slack", fc, Config{PollInterval: 5 * time.Millisecond})
	r, _ := ch.Notify(context.Background(), "C123", channels.Message{Title: "x"})
	_, err := ch.Await(context.Background(),
		[]channels.Receipt{r},
		channels.AwaitOpts{
			Timeout: 30 * time.Millisecond,
			Options: []string{"accept"}, // only accept counts
		},
	)
	if err != channels.ErrTimeout {
		t.Errorf("expected ErrTimeout, got %v", err)
	}
}

func TestAwaitTimesOutCleanly(t *testing.T) {
	fc := &fakeClient{reactionsByTS: map[string][]Reaction{}}
	ch, _ := New("slack", fc, Config{PollInterval: 5 * time.Millisecond})
	r, _ := ch.Notify(context.Background(), "C123", channels.Message{Title: "x"})
	_, err := ch.Await(context.Background(),
		[]channels.Receipt{r},
		channels.AwaitOpts{Timeout: 20 * time.Millisecond},
	)
	if err != channels.ErrTimeout {
		t.Errorf("expected ErrTimeout, got %v", err)
	}
}

func TestAwaitContextCancelPropagates(t *testing.T) {
	fc := &fakeClient{reactionsByTS: map[string][]Reaction{}}
	ch, _ := New("slack", fc, Config{PollInterval: 5 * time.Millisecond})
	r, _ := ch.Notify(context.Background(), "C123", channels.Message{Title: "x"})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := ch.Await(ctx, []channels.Receipt{r}, channels.AwaitOpts{Timeout: time.Second})
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", err)
	}
}

// === Render tests ===

func TestDefaultRenderHeader(t *testing.T) {
	opts := DefaultRender(channels.Message{Title: "Stale OAuth", Severity: channels.SeverityCritical})
	if opts.Blocks[0].Kind != BlockHeader {
		t.Fatalf("first block should be header, got %v", opts.Blocks[0].Kind)
	}
	if !strings.Contains(opts.Blocks[0].Text, "CRITICAL") {
		t.Errorf("expected CRITICAL prefix, got %q", opts.Blocks[0].Text)
	}
}

func TestDefaultRenderEvidence(t *testing.T) {
	opts := DefaultRender(channels.Message{
		Title: "x",
		Evidence: []channels.Evidence{
			{Kind: "log", Description: "auth failed"},
			{Kind: "metric", Description: "p99 > 5s"},
		},
	})
	found := false
	for _, b := range opts.Blocks {
		if b.Kind == BlockContext {
			for _, e := range b.Elements {
				if strings.Contains(e, "auth failed") {
					found = true
				}
			}
		}
	}
	if !found {
		t.Error("expected evidence to render as context elements")
	}
}

func TestDefaultRenderActionsAsReactionHint(t *testing.T) {
	opts := DefaultRender(channels.Message{
		Title: "x",
		Actions: []channels.Action{
			{ID: "accept", Label: "Confirm"},
			{ID: "reject", Label: "Dismiss"},
		},
	})
	found := false
	for _, b := range opts.Blocks {
		if b.Kind == BlockContext {
			for _, e := range b.Elements {
				if strings.HasPrefix(e, "React:") && strings.Contains(e, ":white_check_mark:") {
					found = true
				}
			}
		}
	}
	if !found {
		t.Error("expected reaction-hint context line")
	}
	// Confirm we did NOT emit interactive Action buttons.
	for _, b := range opts.Blocks {
		if b.Kind == BlockActions {
			for _, btn := range b.Buttons {
				if btn.ActionID != "" {
					t.Errorf("expected no interactive (ActionID) buttons, got %+v", btn)
				}
			}
		}
	}
}

func TestDefaultRenderLinksAsURLButtons(t *testing.T) {
	opts := DefaultRender(channels.Message{
		Title: "x",
		Links: []channels.Link{{Label: "Open trace", URL: "https://x.example/y"}},
	})
	found := false
	for _, b := range opts.Blocks {
		if b.Kind == BlockActions && len(b.Buttons) == 1 && b.Buttons[0].URL != "" {
			found = true
		}
	}
	if !found {
		t.Error("expected one URL button for the link")
	}
}

func TestDefaultRenderWorkflowRef(t *testing.T) {
	opts := DefaultRender(channels.Message{
		Title:       "x",
		WorkflowRef: channels.WorkflowRef{WorkflowID: "wf-42", TraceURL: "https://temporal.test/wf-42"},
	})
	found := false
	for _, b := range opts.Blocks {
		if b.Kind == BlockContext {
			for _, e := range b.Elements {
				if strings.Contains(e, "wf-42") && strings.Contains(e, "Open in Temporal") {
					found = true
				}
			}
		}
	}
	if !found {
		t.Error("expected workflow ref + Temporal link in context block")
	}
}

func TestActionReactionHintUnknownActionsLackEmoji(t *testing.T) {
	got := actionReactionHint([]channels.Action{
		{ID: "escalate", Label: "Escalate"},
	})
	if !strings.Contains(got, "Escalate") {
		t.Errorf("expected label to render, got %q", got)
	}
	if strings.Contains(got, ":white_check_mark:") {
		t.Errorf("unknown action should not get standard emoji, got %q", got)
	}
}

func TestActionReactionHintEmpty(t *testing.T) {
	if got := actionReactionHint(nil); got != "" {
		t.Errorf("nil actions should return empty, got %q", got)
	}
}
