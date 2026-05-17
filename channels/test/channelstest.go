// Package channelstest provides a deterministic in-memory Channel
// implementation for use in tests of code that depends on the channels
// package.
//
// Typical use:
//
//	mem := channelstest.New("inmem")
//	dispatcher, _ := channels.New(
//	    channels.WithChannel(mem.Channel()),
//	    channels.WithRouter(channels.FixedRouter("inmem", "default")),
//	)
//	// ... run the code under test ...
//	mem.EnqueueVerdict(channels.Verdict{Choice: "accept", Channel: "inmem"})
//	// ... AwaitVerdict will see this verdict
//
// All operations are concurrency-safe.
package channelstest

import (
	"context"
	"sync"
	"time"

	"github.com/vinodhalaharvi/sibyl/channels"
)

// Channel is an in-memory recorder + scriptable verdict source.
// Construct one with New; access the channels.Channel via Channel().
type Channel struct {
	name string

	mu       sync.Mutex
	posts    []Post
	verdicts []channels.Verdict
}

// Post records one Notify call.
type Post struct {
	Target  string
	Message channels.Message
	Receipt channels.Receipt
}

// New creates an in-memory test channel with the given name.
func New(name string) *Channel {
	return &Channel{name: name}
}

// Channel returns the channels.Channel value to register with a Dispatcher.
func (c *Channel) Channel() channels.Channel {
	return channels.Channel{
		Name: c.name,
		Notify: func(ctx context.Context, target string, msg channels.Message) (channels.Receipt, error) {
			c.mu.Lock()
			defer c.mu.Unlock()
			receipt := channels.Receipt{
				Channel: c.name,
				Target:  target,
				Native:  generateNative(len(c.posts)),
			}
			c.posts = append(c.posts, Post{Target: target, Message: msg, Receipt: receipt})
			return receipt, nil
		},
		Await: func(ctx context.Context, receipts []channels.Receipt, opts channels.AwaitOpts) (channels.Verdict, error) {
			deadline := time.Now().Add(opts.Timeout)
			if opts.Timeout == 0 {
				deadline = time.Now().Add(time.Second) // very short for tests
			}
			ticker := time.NewTicker(2 * time.Millisecond)
			defer ticker.Stop()
			for {
				c.mu.Lock()
				if len(c.verdicts) > 0 {
					v := c.verdicts[0]
					c.verdicts = c.verdicts[1:]
					c.mu.Unlock()
					return v, nil
				}
				c.mu.Unlock()
				if time.Now().After(deadline) {
					return channels.Verdict{}, channels.ErrTimeout
				}
				select {
				case <-ctx.Done():
					return channels.Verdict{}, ctx.Err()
				case <-ticker.C:
				}
			}
		},
	}
}

// Posts returns a copy of all recorded posts so far.
func (c *Channel) Posts() []Post {
	c.mu.Lock()
	defer c.mu.Unlock()
	cp := make([]Post, len(c.posts))
	copy(cp, c.posts)
	return cp
}

// EnqueueVerdict queues a verdict to be returned by the next Await call.
// Verdicts are consumed FIFO.
func (c *Channel) EnqueueVerdict(v channels.Verdict) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if v.Channel == "" {
		v.Channel = c.name
	}
	if v.At.IsZero() {
		v.At = time.Now()
	}
	c.verdicts = append(c.verdicts, v)
}

// Reset clears both recorded posts and queued verdicts.
func (c *Channel) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.posts = nil
	c.verdicts = nil
}

// generateNative produces a deterministic native ID for posts. Format
// "T-<sequence>" — readable, sortable, stable across test runs.
func generateNative(seq int) string {
	return "T-" + itoa(seq)
}

// itoa is a tiny inline conversion to avoid importing strconv for one call.
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
