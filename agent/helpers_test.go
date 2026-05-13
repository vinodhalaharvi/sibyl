package agent_test

import (
	"context"

	"go.temporal.io/sdk/activity"
)

// testCtx returns a context.Background() — kept as a tiny helper so the test
// file reads cleanly and we can swap it out if we ever need a richer ctx.
func testCtx() context.Context {
	return context.Background()
}

func registerOpts(name string) activity.RegisterOptions {
	return activity.RegisterOptions{Name: name}
}
