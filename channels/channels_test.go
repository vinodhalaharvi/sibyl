package channels

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// fakeChannel builds a Channel suitable for testing. Each closure can be
// overridden; nil fields are honored.
type fakeChannel struct {
	name     string
	notify   func(ctx context.Context, target string, msg Message) (Receipt, error)
	await    func(ctx context.Context, receipts []Receipt, opts AwaitOpts) (Verdict, error)
	notifies []struct {
		target string
		msg    Message
	}
}

func (f *fakeChannel) ch() Channel {
	return Channel{
		Name: f.name,
		Notify: func(ctx context.Context, target string, msg Message) (Receipt, error) {
			f.notifies = append(f.notifies, struct {
				target string
				msg    Message
			}{target, msg})
			if f.notify != nil {
				return f.notify(ctx, target, msg)
			}
			return Receipt{Channel: f.name, Target: target, Native: "T-default"}, nil
		},
		Await: f.await,
	}
}

func TestNewRequiresAtLeastOneChannel(t *testing.T) {
	_, err := New(WithRouter(FixedRouter("nope", "nope")))
	if err == nil {
		t.Fatal("expected error when no channels registered")
	}
}

func TestNewRequiresRouter(t *testing.T) {
	c := (&fakeChannel{name: "slack"}).ch()
	_, err := New(WithChannel(c))
	if err == nil {
		t.Fatal("expected error when no router set")
	}
}

func TestWithChannelRejectsEmptyName(t *testing.T) {
	c := Channel{Name: ""}
	_, err := New(WithChannel(c), WithRouter(FixedRouter("x", "y")))
	if err == nil || !strings.Contains(err.Error(), "Channel.Name is required") {
		t.Fatalf("expected name-required error, got %v", err)
	}
}

func TestWithChannelRejectsDuplicates(t *testing.T) {
	a := (&fakeChannel{name: "slack"}).ch()
	b := (&fakeChannel{name: "slack"}).ch()
	_, err := New(WithChannel(a), WithChannel(b), WithRouter(FixedRouter("slack", "x")))
	if err == nil || !strings.Contains(err.Error(), "duplicate channel name") {
		t.Fatalf("expected duplicate-name error, got %v", err)
	}
}

func TestNotifyRoutesToChannel(t *testing.T) {
	fc := &fakeChannel{name: "slack"}
	d, err := New(WithChannel(fc.ch()), WithRouter(FixedRouter("slack", "C123")))
	if err != nil {
		t.Fatal(err)
	}
	result, err := d.Notify(context.Background(), Message{Title: "hi", Severity: SeverityCritical})
	if err != nil {
		t.Fatalf("Notify error: %v", err)
	}
	if len(result.Receipts) != 1 || result.Receipts[0].Channel != "slack" {
		t.Errorf("expected one slack receipt, got %+v", result.Receipts)
	}
	if len(fc.notifies) != 1 || fc.notifies[0].target != "C123" {
		t.Errorf("expected target C123, got %+v", fc.notifies)
	}
}

func TestNotifyReturnsNoTargetWhenRouterEmpty(t *testing.T) {
	emptyRouter := func(_ context.Context, _ []string, _ Message) ([]Target, error) {
		return nil, nil
	}
	fc := &fakeChannel{name: "slack"}
	d, err := New(WithChannel(fc.ch()), WithRouter(emptyRouter))
	if err != nil {
		t.Fatal(err)
	}
	_, err = d.Notify(context.Background(), Message{Title: "hi"})
	if !errors.Is(err, ErrNoTarget) {
		t.Errorf("expected ErrNoTarget, got %v", err)
	}
}

func TestNotifyCollectsPerChannelErrors(t *testing.T) {
	good := (&fakeChannel{
		name: "slack",
		notify: func(_ context.Context, _ string, _ Message) (Receipt, error) {
			return Receipt{Channel: "slack", Native: "T-OK"}, nil
		},
	}).ch()
	bad := Channel{
		Name: "email",
		Notify: func(_ context.Context, _ string, _ Message) (Receipt, error) {
			return Receipt{}, errors.New("SMTP relay unreachable")
		},
	}
	router := MultiRouter(
		FixedRouter("slack", "C123"),
		FixedRouter("email", "sec@acme.com"),
	)
	d, err := New(WithChannel(good), WithChannel(bad), WithRouter(router))
	if err != nil {
		t.Fatal(err)
	}
	result, err := d.Notify(context.Background(), Message{Title: "hi"})
	if err != nil {
		t.Fatalf("Notify error: %v", err)
	}
	if len(result.Receipts) != 1 || result.Receipts[0].Channel != "slack" {
		t.Errorf("expected one slack receipt, got %+v", result.Receipts)
	}
	if len(result.Errors) != 1 || !strings.Contains(result.Errors[0], "SMTP relay") {
		t.Errorf("expected one SMTP error, got %+v", result.Errors)
	}
}

func TestNotifySkipsChannelsWithNilNotify(t *testing.T) {
	notifyOnly := Channel{Name: "telegrams", Notify: nil} // weird but legal
	router := FixedRouter("telegrams", "x")
	d, err := New(WithChannel(notifyOnly), WithRouter(router))
	if err != nil {
		t.Fatal(err)
	}
	result, err := d.Notify(context.Background(), Message{Title: "hi"})
	if err != nil {
		t.Fatalf("Notify error: %v", err)
	}
	if len(result.Receipts) != 0 {
		t.Errorf("expected no receipts, got %+v", result.Receipts)
	}
	if len(result.Errors) != 1 || !strings.Contains(result.Errors[0], "not supported") {
		t.Errorf("expected ErrNotSupported in errors, got %+v", result.Errors)
	}
}

func TestSeverityRouter(t *testing.T) {
	r := SeverityRouter("slack", map[Severity]string{
		SeverityCritical: "C-pager",
		SeverityHigh:     "C-general",
	})

	got, err := r(context.Background(), []string{"slack"}, Message{Severity: SeverityCritical})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Target != "C-pager" {
		t.Errorf("CRITICAL should route to C-pager, got %+v", got)
	}

	got, err = r(context.Background(), []string{"slack"}, Message{Severity: SeverityLow})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Errorf("LOW should be dropped, got %+v", got)
	}
}

func TestMultiRouter(t *testing.T) {
	r := MultiRouter(
		FixedRouter("slack", "C-1"),
		FixedRouter("email", "x@y.com"),
	)
	got, err := r(context.Background(), nil, Message{})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 targets, got %d", len(got))
	}
	if got[0].Channel != "slack" || got[1].Channel != "email" {
		t.Errorf("expected ordered slack,email, got %+v", got)
	}
}

func TestAwaitRequiresReceipts(t *testing.T) {
	fc := &fakeChannel{name: "slack"}
	d, err := New(WithChannel(fc.ch()), WithRouter(FixedRouter("slack", "x")))
	if err != nil {
		t.Fatal(err)
	}
	_, err = d.Await(context.Background(), nil, AwaitOpts{})
	if err == nil {
		t.Fatal("expected error when no receipts provided")
	}
}

func TestAwaitSingleChannel(t *testing.T) {
	fc := &fakeChannel{
		name: "slack",
		await: func(ctx context.Context, receipts []Receipt, opts AwaitOpts) (Verdict, error) {
			return Verdict{
				Choice:  "accept",
				Actor:   Identity{Canonical: "slack:U999", Source: "test"},
				Channel: "slack",
				At:      time.Date(2026, 5, 16, 0, 0, 0, 0, time.UTC),
			}, nil
		},
	}
	d, err := New(WithChannel(fc.ch()), WithRouter(FixedRouter("slack", "x")))
	if err != nil {
		t.Fatal(err)
	}
	v, err := d.Await(context.Background(),
		[]Receipt{{Channel: "slack", Native: "T1"}},
		AwaitOpts{Timeout: 5 * time.Second},
	)
	if err != nil {
		t.Fatalf("Await error: %v", err)
	}
	if v.Choice != "accept" || v.Actor.Canonical != "slack:U999" {
		t.Errorf("unexpected verdict: %+v", v)
	}
}

func TestAwaitTimeoutPropagates(t *testing.T) {
	fc := &fakeChannel{
		name: "slack",
		await: func(_ context.Context, _ []Receipt, _ AwaitOpts) (Verdict, error) {
			return Verdict{}, ErrTimeout
		},
	}
	d, err := New(WithChannel(fc.ch()), WithRouter(FixedRouter("slack", "x")))
	if err != nil {
		t.Fatal(err)
	}
	_, err = d.Await(context.Background(),
		[]Receipt{{Channel: "slack", Native: "T1"}},
		AwaitOpts{Timeout: time.Millisecond},
	)
	if !errors.Is(err, ErrTimeout) {
		t.Errorf("expected ErrTimeout, got %v", err)
	}
}

func TestPassthroughResolver(t *testing.T) {
	id, err := PassthroughResolver(context.Background(), "slack", "U123", map[string]string{
		"display_name": "Vinod",
		"email":        "vinod@acme.com",
	})
	if err != nil {
		t.Fatal(err)
	}
	if id.Canonical != "slack:U123" {
		t.Errorf("canonical = %q, want slack:U123", id.Canonical)
	}
	if id.DisplayName != "Vinod" {
		t.Errorf("display = %q, want Vinod", id.DisplayName)
	}
	if id.Email != "vinod@acme.com" {
		t.Errorf("email = %q, want vinod@acme.com", id.Email)
	}
	if id.Source != "passthrough" {
		t.Errorf("source = %q, want passthrough", id.Source)
	}
}
