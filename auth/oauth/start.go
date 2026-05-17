// Package oauth — start.go provides StartFlow, the synchronous entry
// point for kicking off an OAuth flow.
//
// Why this is here:
//
//	The OAuth dance has a synchronous prep step (generate PKCE values,
//	build the authorize URL) and a long-running durable step (wait
//	for the callback, exchange, whoami, persist). The durable part
//	lives in OAuthFlowWorkflow; the synchronous part lives here.
//
//	Splitting these means HTTP handlers can call StartFlow and get
//	the authorize URL immediately, without polling a workflow query
//	to retrieve it. The user gets redirected the moment they hit
//	/oauth/start; the workflow sits waiting for the callback in
//	the background.
package oauth

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"

	"go.temporal.io/sdk/client"
)

// StartFlowInput is the input to StartFlow.
type StartFlowInput struct {
	// Provider is the registered provider name.
	Provider string

	// Scopes is the OAuth scope list to request.
	Scopes []string

	// RedirectURI is the absolute callback URL — provider redirects
	// here after the user clicks Allow.
	RedirectURI string

	// StoreKey selects which TokenStore the workflow uses to persist
	// the resulting tokens. Most deployments use "default".
	StoreKey string

	// TaskQueue is the Temporal task queue the workflow runs on. Must
	// match a queue that has the OAuthFlowWorkflow + activities registered.
	TaskQueue string

	// WorkflowIDPrefix is prepended to the generated workflow ID for
	// readability in Temporal Web UI. Empty defaults to "oauth-flow".
	WorkflowIDPrefix string
}

// StartFlowResult is what StartFlow returns. AuthURL is ready to use
// immediately — HTTP handlers can return an http.Redirect to it. The
// WorkflowID can be used later to query status or await the result.
type StartFlowResult struct {
	// AuthURL is the provider's authorize endpoint with all params
	// filled in. Redirect the user here.
	AuthURL string

	// WorkflowID is the Temporal workflow ID. Use this to await the
	// flow completion or query its status.
	WorkflowID string

	// State is the CSRF token; included for callers that want to do
	// their own bookkeeping outside the StateMapper.
	State string
}

// StartFlow does the synchronous prep for an OAuth flow and kicks off
// OAuthFlowWorkflow. Returns the authorize URL and workflow ID
// immediately; the workflow runs in the background until the callback
// fires or the timeout elapses.
//
// Steps performed synchronously here:
//
//  1. Generate fresh State and PKCE values.
//  2. Look up the named provider in the registry and call its
//     Authorize arrow to build the authorize URL.
//  3. Record the (state → workflow-id) mapping in the state mapper.
//  4. Start the workflow with State and CodeVerifier in its input.
//
// If any step fails, no workflow is started and the error is returned.
// On success, the returned WorkflowID identifies a running workflow
// that's already waiting for the callback signal.
//
// Typical HTTP handler usage:
//
//	result, err := oauth.StartFlow(r.Context(), tc, providers, mapper, oauth.StartFlowInput{
//	    Provider: "okta", Scopes: []string{"openid", "email"},
//	    RedirectURI: "http://localhost:8090/oauth/callback",
//	    StoreKey: "default", TaskQueue: "my-tq",
//	})
//	if err != nil { http.Error(w, ...) ; return }
//	http.Redirect(w, r, result.AuthURL, http.StatusFound)
func StartFlow(
	ctx context.Context,
	tc client.Client,
	providers *Registry,
	mapper StateMapper,
	in StartFlowInput,
) (StartFlowResult, error) {
	// 1. Validate input.
	if in.Provider == "" || in.RedirectURI == "" || in.StoreKey == "" || in.TaskQueue == "" {
		return StartFlowResult{}, fmt.Errorf("oauth: StartFlow: Provider, RedirectURI, StoreKey, TaskQueue are required")
	}
	if providers == nil {
		return StartFlowResult{}, fmt.Errorf("oauth: StartFlow: provider registry is nil")
	}
	if mapper == nil {
		return StartFlowResult{}, fmt.Errorf("oauth: StartFlow: state mapper is nil")
	}

	// 2. Look up the provider.
	provider, err := providers.Get(in.Provider)
	if err != nil {
		return StartFlowResult{}, fmt.Errorf("oauth: StartFlow: %w", err)
	}

	// 3. Build the auth request (generates State + PKCE).
	req, err := NewAuthRequest(in.Provider, in.RedirectURI, in.Scopes)
	if err != nil {
		return StartFlowResult{}, fmt.Errorf("oauth: StartFlow: %w", err)
	}

	// 4. Get the redirect URL from the provider's Authorize arrow.
	redirect, err := provider.Authorize(ctx, req)
	if err != nil {
		return StartFlowResult{}, fmt.Errorf("oauth: StartFlow: provider.Authorize: %w", err)
	}

	// 5. Generate a workflow ID. We need this NOW (before ExecuteWorkflow)
	//    because the state mapping must point to it, and the state
	//    mapping must exist before the user could possibly arrive at
	//    the callback URL.
	wfIDPrefix := in.WorkflowIDPrefix
	if wfIDPrefix == "" {
		wfIDPrefix = "oauth-flow"
	}
	wfID := wfIDPrefix + "-" + in.Provider + "-" + randomShortID()

	// 6. Record the state-to-workflow-id mapping. The HTTP callback
	//    handler will look up this state to know which workflow to
	//    signal.
	if err := mapper.Put(ctx, redirect.State, wfID); err != nil {
		return StartFlowResult{}, fmt.Errorf("oauth: StartFlow: state mapper Put: %w", err)
	}

	// 7. Start the workflow. It'll immediately sit at the
	//    "awaiting callback" wait.
	_, err = tc.ExecuteWorkflow(ctx,
		client.StartWorkflowOptions{
			ID:        wfID,
			TaskQueue: in.TaskQueue,
		},
		OAuthFlowWorkflowName,
		OAuthFlowInput{
			Provider:     in.Provider,
			State:        redirect.State,
			CodeVerifier: redirect.CodeVerifier,
			RedirectURI:  in.RedirectURI,
			StoreKey:     in.StoreKey,
			// CallbackTimeout uses the workflow's default (30 min).
		},
	)
	if err != nil {
		// Workflow didn't start. Clean up the state mapping we just made.
		_ = mapper.Delete(ctx, redirect.State)
		return StartFlowResult{}, fmt.Errorf("oauth: StartFlow: ExecuteWorkflow: %w", err)
	}

	return StartFlowResult{
		AuthURL:    redirect.URL,
		WorkflowID: wfID,
		State:      redirect.State,
	}, nil
}

// randomShortID returns a short URL-safe random identifier for use in
// workflow IDs. ~10 chars of entropy, suitable for uniqueness in
// human-readable workflow IDs without overwhelming them.
func randomShortID() string {
	buf := make([]byte, 8)
	if _, err := rand.Read(buf); err != nil {
		// Shouldn't happen; if it does, return a fixed string so the
		// caller gets a recognizable workflow ID rather than a panic.
		return "rng-fail"
	}
	return base64.RawURLEncoding.EncodeToString(buf)
}
