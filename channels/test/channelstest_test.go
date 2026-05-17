package channelstest_test

import (
	"context"
	"testing"
	"time"

	"github.com/vinodhalaharvi/sibyl/channels"
	channelstest "github.com/vinodhalaharvi/sibyl/channels/test"
)

func TestInMemoryNotifyAndAwait(t *testing.T) {
	mem := channelstest.New("test")
	d, err := channels.New(
		channels.WithChannel(mem.Channel()),
		channels.WithRouter(channels.FixedRouter("test", "default")),
	)
	if err != nil {
		t.Fatal(err)
	}

	// Post.
	result, err := d.Notify(context.Background(), channels.Message{
		Title:    "Hello",
		Severity: channels.SeverityHigh,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Receipts) != 1 {
		t.Fatalf("expected 1 receipt, got %d", len(result.Receipts))
	}
	if got := mem.Posts(); len(got) != 1 || got[0].Message.Title != "Hello" {
		t.Errorf("expected one recorded post titled Hello, got %+v", got)
	}

	// Enqueue a verdict and await.
	mem.EnqueueVerdict(channels.Verdict{
		Choice: "accept",
		Actor:  channels.Identity{Canonical: "test:U1"},
	})
	v, err := d.Await(context.Background(), result.Receipts, channels.AwaitOpts{Timeout: 100 * time.Millisecond})
	if err != nil {
		t.Fatalf("Await: %v", err)
	}
	if v.Choice != "accept" {
		t.Errorf("Choice = %q, want accept", v.Choice)
	}
}

func TestInMemoryTimeoutWithNoVerdict(t *testing.T) {
	mem := channelstest.New("test")
	d, err := channels.New(
		channels.WithChannel(mem.Channel()),
		channels.WithRouter(channels.FixedRouter("test", "default")),
	)
	if err != nil {
		t.Fatal(err)
	}
	r, _ := d.Notify(context.Background(), channels.Message{})
	_, err = d.Await(context.Background(), r.Receipts, channels.AwaitOpts{Timeout: 20 * time.Millisecond})
	if err != channels.ErrTimeout {
		t.Errorf("expected ErrTimeout, got %v", err)
	}
}

func TestInMemoryResetClearsState(t *testing.T) {
	mem := channelstest.New("test")
	d, _ := channels.New(
		channels.WithChannel(mem.Channel()),
		channels.WithRouter(channels.FixedRouter("test", "default")),
	)
	_, _ = d.Notify(context.Background(), channels.Message{Title: "first"})
	mem.EnqueueVerdict(channels.Verdict{Choice: "accept"})
	mem.Reset()
	if len(mem.Posts()) != 0 {
		t.Error("expected posts to be cleared")
	}
	// Verdict should be gone too — an await now should time out.
	r, _ := d.Notify(context.Background(), channels.Message{Title: "second"})
	_, err := d.Await(context.Background(), r.Receipts, channels.AwaitOpts{Timeout: 10 * time.Millisecond})
	if err != channels.ErrTimeout {
		t.Errorf("expected ErrTimeout after Reset, got %v", err)
	}
}
