// Package oauth — workflow.go implements OAuthFlowWorkflow, the durable
// Temporal workflow that orchestrates an end-to-end OAuth dance.
//
// Why a workflow and not just a sequence of HTTP calls?
//
//	The Authorize step is not synchronous. After we hand the user the
//	provider's authorize URL, we have to wait for them to click "Allow"
//	and for the provider to redirect to our callback URL. That wait
//	could be 5 seconds or 30 minutes; it must survive worker restarts
//	(the user shouldn't have to start over if the server reboots
//	during their authorization).
//
//	Temporal workflows are exactly the right tool: GetSignalChannel
//	blocks durably across worker restarts. The workflow goes to sleep
//	waiting for a "oauth.callback" signal; the HTTP handler that
//	handles the redirect sends that signal; the workflow wakes up
//	with the AuthCode and proceeds.
//
// Workflow signals (callers send these):
//
//	"oauth.callback"  payload: AuthCode { Code, State }
//
// Workflow queries (callers send these):
//
//	"oauth.status"    response: OAuthFlowStatus
//
// Result: Session on success, error on failure.
package oauth

import (
	"fmt"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// Workflow / activity / signal / query names.
const (
	OAuthFlowWorkflowName = "OAuthFlowWorkflow"

	BuildAuthorizeURLActivityName = "BuildAuthorizeURL"
	ExchangeActivityName          = "OAuthExchange"
	WhoamiActivityName            = "OAuthWhoami"
	StoreTokensActivityName       = "OAuthStoreTokens"
	StoreStateMappingActivityName = "OAuthStoreStateMapping"

	CallbackSignal = "oauth.callback"
	StatusQuery    = "oauth.status"
)

// OAuthFlowInput is the workflow input. The store and provider are
// resolved at the worker level from the WorkflowOptions registry —
// workflows can only take serializable arguments.
type OAuthFlowInput struct {
	// Provider is the registered provider name ("okta", etc.).
	Provider string

	// Scopes is the OAuth scope list requested.
	Scopes []string

	// RedirectURI is the callback URL the provider will redirect to.
	RedirectURI string

	// StoreKey selects which TokenStore the worker should use.
	// Most deployments only have one store; pass "default".
	StoreKey string

	// CallbackTimeout caps how long we'll wait for the user to
	// complete the authorize step. Zero means use the default
	// (30 minutes). Anything longer than your Temporal namespace's
	// workflow execution timeout will be capped silently by Temporal.
	CallbackTimeout time.Duration
}

// OAuthFlowStatus is the query response describing where in the flow
// we currently are. UIs poll/query this to render progress.
type OAuthFlowStatus struct {
	Stage     string    `json:"stage"` // "preparing" | "awaiting_callback" | "exchanging" | "completed" | "failed"
	AuthURL   string    `json:"auth_url,omitempty"`
	StartedAt time.Time `json:"started_at"`
	Error     string    `json:"error,omitempty"`
}

// OAuthFlowWorkflow runs an OAuth flow durably from authorize to session.
func OAuthFlowWorkflow(ctx workflow.Context, in OAuthFlowInput) (Session, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("OAuthFlowWorkflow starting", "provider", in.Provider, "scopes", in.Scopes)

	status := &OAuthFlowStatus{
		Stage:     "preparing",
		StartedAt: workflow.Now(ctx),
	}
	if err := workflow.SetQueryHandler(ctx, StatusQuery, func() (OAuthFlowStatus, error) {
		return *status, nil
	}); err != nil {
		return Session{}, fmt.Errorf("oauth: set query handler: %w", err)
	}

	if in.Provider == "" || in.RedirectURI == "" || in.StoreKey == "" {
		status.Stage = "failed"
		status.Error = "missing required input"
		return Session{}, temporal.NewNonRetryableApplicationError(
			"OAuthFlowInput.Provider, RedirectURI, StoreKey are required",
			"InvalidInput", nil)
	}

	callbackTimeout := in.CallbackTimeout
	if callbackTimeout <= 0 {
		callbackTimeout = 30 * time.Minute
	}

	// Activity options: short timeouts for the synchronous steps.
	actCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        time.Second,
			BackoffCoefficient:     2.0,
			MaximumAttempts:        3,
			NonRetryableErrorTypes: []string{"InvalidInput", "ConfigurationError"},
		},
	})

	// Step 1: build the authorize URL. The activity also generates
	// fresh State and CodeVerifier values; we pull those back so we
	// can verify the callback's state matches and use the verifier
	// during exchange.
	var redirect AuthorizeRedirect
	if err := workflow.ExecuteActivity(actCtx, BuildAuthorizeURLActivityName, BuildAuthorizeURLInput{
		Provider:    in.Provider,
		Scopes:      in.Scopes,
		RedirectURI: in.RedirectURI,
	}).Get(ctx, &redirect); err != nil {
		status.Stage = "failed"
		status.Error = fmt.Sprintf("build authorize url: %v", err)
		return Session{}, fmt.Errorf("oauth: build authorize url: %w", err)
	}

	// Step 2: record the state-to-workflow mapping so the HTTP callback
	// handler can route the callback signal here.
	if err := workflow.ExecuteActivity(actCtx, StoreStateMappingActivityName, StoreStateMappingInput{
		State:      redirect.State,
		WorkflowID: workflow.GetInfo(ctx).WorkflowExecution.ID,
	}).Get(ctx, nil); err != nil {
		status.Stage = "failed"
		status.Error = fmt.Sprintf("store state mapping: %v", err)
		return Session{}, fmt.Errorf("oauth: store state mapping: %w", err)
	}

	status.Stage = "awaiting_callback"
	status.AuthURL = redirect.URL

	// Step 3: wait for the callback signal — possibly for a long time.
	// Selector blocks durably; if the worker dies, on restart the
	// workflow is rehydrated and waits again.
	signalCh := workflow.GetSignalChannel(ctx, CallbackSignal)
	var code AuthCode
	receivedSignal := false

	selector := workflow.NewSelector(ctx)
	selector.AddReceive(signalCh, func(c workflow.ReceiveChannel, more bool) {
		c.Receive(ctx, &code)
		receivedSignal = true
	})
	// Use a timer for the timeout so we don't wait forever in case
	// the user never completes the dance.
	timerCtx, cancelTimer := workflow.WithCancel(ctx)
	defer cancelTimer()
	timerFuture := workflow.NewTimer(timerCtx, callbackTimeout)
	selector.AddFuture(timerFuture, func(workflow.Future) {})

	selector.Select(ctx)
	if !receivedSignal {
		status.Stage = "failed"
		status.Error = "callback timeout"
		return Session{}, temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("oauth: callback timeout after %v", callbackTimeout),
			"CallbackTimeout", nil)
	}

	// Step 4: verify state matches what we generated.
	if code.State != redirect.State {
		status.Stage = "failed"
		status.Error = "state mismatch"
		return Session{}, temporal.NewNonRetryableApplicationError(
			"oauth: callback state does not match request state",
			"CSRFViolation", nil)
	}

	status.Stage = "exchanging"

	// Step 5: exchange the code for tokens.
	exchangeCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        time.Second,
			BackoffCoefficient:     2.0,
			MaximumAttempts:        3,
			NonRetryableErrorTypes: []string{"InvalidInput", "ConfigurationError", "CSRFViolation"},
		},
	})

	var tokens TokenPair
	if err := workflow.ExecuteActivity(exchangeCtx, ExchangeActivityName, ExchangeActivityInput{
		Provider:     in.Provider,
		Code:         code,
		CodeVerifier: redirect.CodeVerifier,
		RedirectURI:  in.RedirectURI,
	}).Get(ctx, &tokens); err != nil {
		status.Stage = "failed"
		status.Error = fmt.Sprintf("exchange: %v", err)
		return Session{}, fmt.Errorf("oauth: exchange: %w", err)
	}

	// Step 6: identify the user.
	var identity Identity
	if err := workflow.ExecuteActivity(exchangeCtx, WhoamiActivityName, WhoamiActivityInput{
		Provider: in.Provider,
		Tokens:   tokens,
	}).Get(ctx, &identity); err != nil {
		status.Stage = "failed"
		status.Error = fmt.Sprintf("whoami: %v", err)
		return Session{}, fmt.Errorf("oauth: whoami: %w", err)
	}

	// Step 7: persist the tokens.
	if err := workflow.ExecuteActivity(exchangeCtx, StoreTokensActivityName, StoreTokensActivityInput{
		StoreKey: in.StoreKey,
		Identity: identity,
		Provider: in.Provider,
		Tokens:   tokens,
	}).Get(ctx, nil); err != nil {
		status.Stage = "failed"
		status.Error = fmt.Sprintf("store tokens: %v", err)
		return Session{}, fmt.Errorf("oauth: store tokens: %w", err)
	}

	status.Stage = "completed"

	return Session{
		Identity:  identity,
		Token:     tokens,
		IssuedAt:  workflow.Now(ctx),
		ExpiresAt: tokens.ExpiresAt,
	}, nil
}

// Activity input types.

type BuildAuthorizeURLInput struct {
	Provider    string
	Scopes      []string
	RedirectURI string
}

type StoreStateMappingInput struct {
	State      string
	WorkflowID string
}

type ExchangeActivityInput struct {
	Provider     string
	Code         AuthCode
	CodeVerifier string
	RedirectURI  string
}

type WhoamiActivityInput struct {
	Provider string
	Tokens   TokenPair
}

type StoreTokensActivityInput struct {
	StoreKey string
	Identity Identity
	Provider string
	Tokens   TokenPair
}
