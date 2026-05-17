// Package worker provides a one-line Temporal worker registration
// helper for OAuth workflows and activities.
//
// Usage (matches the pattern of github.com/vinodhalaharvi/sibyl/worker):
//
//	reg := oauth.NewRegistry()
//	reg.MustRegister(okta.MustNewProvider(okta.Config{...}))
//
//	stores := map[string]oauth.TokenStore{"default": sqliteStore}
//	mapper := oauth.NewMemoryStateMapper()
//
//	oauthworker.Register(w, oauthworker.Config{
//	    Providers: reg,
//	    Stores:    stores,
//	    Mapper:    mapper,
//	})
//
// That replaces ~12 lines of manual workflow + activity registration.
package worker

import (
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/vinodhalaharvi/sibyl/auth/oauth"
)

// Config bundles the dependencies the OAuth worker needs.
type Config struct {
	// Providers is the registry of OAuth providers (Okta, etc.).
	// Required; the workflow's activities look up providers by name.
	Providers *oauth.Registry

	// Stores maps store keys (e.g. "default") to TokenStore values.
	// Required; the workflow's StoreTokens activity uses this to
	// persist tokens after a successful exchange.
	Stores map[string]oauth.TokenStore

	// Mapper is the state-to-workflow-id mapper. Required for the
	// HTTP callback handler to find the right workflow to signal.
	// Strictly speaking the workflow itself doesn't use the mapper
	// (StartFlow uses it synchronously), but we accept it here so
	// the worker and callback handler share a single instance.
	Mapper oauth.StateMapper

	// ClientConfigs is optional per-provider HTTP config (endpoints,
	// client IDs) for advanced use cases. Usually nil — providers
	// carry their own configs.
	ClientConfigs map[string]oauth.ClientConfig
}

// Register adds OAuth workflows and activities to a Temporal worker.
//
// After this call, the worker is ready to:
//   - run OAuthFlowWorkflow as a registered workflow
//   - run the three OAuth activities (Exchange, Whoami, StoreTokens)
//
// The caller still needs to:
//   - start the worker (w.Start() or w.Run())
//   - hook up the HTTP callback handler in their HTTP server
//   - call StartFlow from their application code to kick off flows
func Register(w worker.Worker, cfg Config) {
	if cfg.Providers == nil {
		panic("oauth/worker: Register: Providers is required")
	}
	if cfg.Stores == nil {
		panic("oauth/worker: Register: Stores is required")
	}
	if cfg.Mapper == nil {
		panic("oauth/worker: Register: Mapper is required")
	}

	acts := oauth.NewActivities(cfg.Providers, cfg.Stores, cfg.Mapper, cfg.ClientConfigs)

	w.RegisterWorkflowWithOptions(oauth.OAuthFlowWorkflow, workflow.RegisterOptions{
		Name: oauth.OAuthFlowWorkflowName,
	})
	w.RegisterActivityWithOptions(acts.Exchange, activity.RegisterOptions{
		Name: oauth.ExchangeActivityName,
	})
	w.RegisterActivityWithOptions(acts.Whoami, activity.RegisterOptions{
		Name: oauth.WhoamiActivityName,
	})
	w.RegisterActivityWithOptions(acts.StoreTokens, activity.RegisterOptions{
		Name: oauth.StoreTokensActivityName,
	})
}
