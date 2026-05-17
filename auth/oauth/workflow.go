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

	ExchangeActivityName    = "OAuthExchange"
	WhoamiActivityName      = "OAuthWhoami"
	StoreTokensActivityName = "OAuthStoreTokens"

	CallbackSignal = "oauth.callback"
	StatusQuery    = "oauth.status"
)

// OAuthFlowInput is the workflow input. The caller (typically via
// StartFlow) prepares the State and CodeVerifier values synchronously
// before kicking off the workflow.
//
// Note on sensitivity: CodeVerifier is embedded in workflow input,
// which lands in Temporal's event history. It's a short-lived secret
// (single-use during Exchange) — fine for the framework's default
// threat model. For high-security deployments, use a Temporal data
// converter to encrypt workflow payloads at rest.
type OAuthFlowInput struct {
	// Provider is the registered provider name ("okta", etc.).
	Provider string

	// State is the CSRF token. The workflow verifies the callback's
	// state matches this value before proceeding to Exchange.
	State string

	// CodeVerifier is the PKCE verifier matching the challenge sent
	// during authorize. Used during Exchange.
	CodeVerifier string

	// RedirectURI is the callback URL — provider re-validates this
	// during Exchange, so it must match what was sent to authorize.
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

// OAuthFlowWorkflow runs the durable portion of an OAuth flow: waiting
// for the callback signal, exchanging the code for tokens, identifying
// the user, and persisting the tokens.
//
// The synchronous prep (generating State/CodeVerifier, building the
// authorize URL, recording the state→workflow-id mapping) happens
// before this workflow starts — see StartFlow for the convenience
// entry point that does all of that and kicks off this workflow.
func OAuthFlowWorkflow(ctx workflow.Context, in OAuthFlowInput) (Session, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("OAuthFlowWorkflow starting", "provider", in.Provider)

	status := &OAuthFlowStatus{
		Stage:     "awaiting_callback",
		StartedAt: workflow.Now(ctx),
	}
	if err := workflow.SetQueryHandler(ctx, StatusQuery, func() (OAuthFlowStatus, error) {
		return *status, nil
	}); err != nil {
		return Session{}, fmt.Errorf("oauth: set query handler: %w", err)
	}

	if in.Provider == "" || in.RedirectURI == "" || in.StoreKey == "" || in.State == "" || in.CodeVerifier == "" {
		status.Stage = "failed"
		status.Error = "missing required input"
		return Session{}, temporal.NewNonRetryableApplicationError(
			"OAuthFlowInput requires Provider, RedirectURI, StoreKey, State, CodeVerifier",
			"InvalidInput", nil)
	}

	callbackTimeout := in.CallbackTimeout
	if callbackTimeout <= 0 {
		callbackTimeout = 30 * time.Minute
	}

	// Wait for the callback signal — possibly for a long time. The
	// signal channel blocks durably; if the worker dies, on restart
	// the workflow is rehydrated and waits again with no progress lost.
	signalCh := workflow.GetSignalChannel(ctx, CallbackSignal)
	var code AuthCode
	receivedSignal := false

	selector := workflow.NewSelector(ctx)
	selector.AddReceive(signalCh, func(c workflow.ReceiveChannel, more bool) {
		c.Receive(ctx, &code)
		receivedSignal = true
	})
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

	// Verify state matches what we generated. State mismatch is a CSRF
	// signal — non-retryable, no second chances.
	if code.State != in.State {
		status.Stage = "failed"
		status.Error = "state mismatch"
		return Session{}, temporal.NewNonRetryableApplicationError(
			"oauth: callback state does not match request state",
			"CSRFViolation", nil)
	}

	status.Stage = "exchanging"

	// Activity options for the remaining HTTP-bound steps.
	exchangeCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        time.Second,
			BackoffCoefficient:     2.0,
			MaximumAttempts:        3,
			NonRetryableErrorTypes: []string{"InvalidInput", "ConfigurationError", "CSRFViolation"},
		},
	})

	// Exchange the code for tokens.
	var tokens TokenPair
	if err := workflow.ExecuteActivity(exchangeCtx, ExchangeActivityName, ExchangeActivityInput{
		Provider:     in.Provider,
		Code:         code,
		CodeVerifier: in.CodeVerifier,
		RedirectURI:  in.RedirectURI,
	}).Get(ctx, &tokens); err != nil {
		status.Stage = "failed"
		status.Error = fmt.Sprintf("exchange: %v", err)
		return Session{}, fmt.Errorf("oauth: exchange: %w", err)
	}

	// Identify the user.
	var identity Identity
	if err := workflow.ExecuteActivity(exchangeCtx, WhoamiActivityName, WhoamiActivityInput{
		Provider: in.Provider,
		Tokens:   tokens,
	}).Get(ctx, &identity); err != nil {
		status.Stage = "failed"
		status.Error = fmt.Sprintf("whoami: %v", err)
		return Session{}, fmt.Errorf("oauth: whoami: %w", err)
	}

	// Persist the tokens.
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
