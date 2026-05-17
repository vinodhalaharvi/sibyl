package oauth_test

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"

	"github.com/vinodhalaharvi/sibyl/auth/oauth"
)

// registerOAuthActivities registers the three activities the workflow
// now depends on. (BuildAuthorizeURL and StoreStateMapping are gone —
// they ran outside the workflow in earlier patches; they now run
// synchronously in StartFlow.)
func registerOAuthActivities(env *testsuite.TestWorkflowEnvironment, acts *oauth.Activities) {
	env.RegisterActivityWithOptions(acts.Exchange, activity.RegisterOptions{Name: oauth.ExchangeActivityName})
	env.RegisterActivityWithOptions(acts.Whoami, activity.RegisterOptions{Name: oauth.WhoamiActivityName})
	env.RegisterActivityWithOptions(acts.StoreTokens, activity.RegisterOptions{Name: oauth.StoreTokensActivityName})
}

func TestOAuthFlowWorkflow_HappyPath(t *testing.T) {
	reg := oauth.NewRegistry()
	require.NoError(t, reg.Register(makeFakeProvider("test")))

	store := oauth.NewMemoryTokenStore(oauth.NoOpCipher{})
	defer store.Close()
	mapper := oauth.NewMemoryStateMapper()
	acts := oauth.NewActivities(reg, map[string]oauth.TokenStore{"default": store}, mapper, nil)

	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestWorkflowEnvironment()
	registerOAuthActivities(env, acts)

	// The workflow now expects State and CodeVerifier in its input —
	// the caller (normally StartFlow) generates them synchronously
	// before kicking off the workflow. Here we just supply known values.
	knownState := "test-state-xyz"
	knownVerifier := "test-verifier-abc"

	// Schedule the callback signal shortly after the workflow starts.
	// With the new shape, the workflow goes immediately to
	// "awaiting_callback" — no preceding activities to wait on.
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(oauth.CallbackSignal, oauth.AuthCode{
			Code:  "test-auth-code",
			State: knownState,
		})
	}, 50*time.Millisecond)

	env.ExecuteWorkflow(oauth.OAuthFlowWorkflow, oauth.OAuthFlowInput{
		Provider:        "test",
		State:           knownState,
		CodeVerifier:    knownVerifier,
		RedirectURI:     "http://localhost:8090/cb",
		StoreKey:        "default",
		CallbackTimeout: 30 * time.Second,
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var session oauth.Session
	require.NoError(t, env.GetWorkflowResult(&session))
	require.Equal(t, "test:test-user", session.Identity.Canonical)
	require.Equal(t, "fake-test-auth-code", session.Token.AccessToken)
}

func TestOAuthFlowWorkflow_CallbackTimeout(t *testing.T) {
	reg := oauth.NewRegistry()
	require.NoError(t, reg.Register(makeFakeProvider("test")))

	store := oauth.NewMemoryTokenStore(oauth.NoOpCipher{})
	defer store.Close()
	mapper := oauth.NewMemoryStateMapper()
	acts := oauth.NewActivities(reg, map[string]oauth.TokenStore{"default": store}, mapper, nil)

	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestWorkflowEnvironment()
	registerOAuthActivities(env, acts)

	// No callback signal sent — should time out.
	env.ExecuteWorkflow(oauth.OAuthFlowWorkflow, oauth.OAuthFlowInput{
		Provider:        "test",
		State:           "s",
		CodeVerifier:    "v",
		RedirectURI:     "http://localhost/cb",
		StoreKey:        "default",
		CallbackTimeout: 500 * time.Millisecond,
	})

	require.True(t, env.IsWorkflowCompleted())
	require.Error(t, env.GetWorkflowError())
	var appErr *temporal.ApplicationError
	require.True(t, errors.As(env.GetWorkflowError(), &appErr))
	require.Equal(t, "CallbackTimeout", appErr.Type())
}

func TestOAuthFlowWorkflow_StateMismatchRejected(t *testing.T) {
	reg := oauth.NewRegistry()
	require.NoError(t, reg.Register(makeFakeProvider("test")))

	store := oauth.NewMemoryTokenStore(oauth.NoOpCipher{})
	defer store.Close()
	mapper := oauth.NewMemoryStateMapper()
	acts := oauth.NewActivities(reg, map[string]oauth.TokenStore{"default": store}, mapper, nil)

	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestWorkflowEnvironment()
	registerOAuthActivities(env, acts)

	env.RegisterDelayedCallback(func() {
		// Signal with a state that doesn't match the workflow's expected state.
		env.SignalWorkflow(oauth.CallbackSignal, oauth.AuthCode{
			Code: "any-code", State: "wrong-state",
		})
	}, 50*time.Millisecond)

	env.ExecuteWorkflow(oauth.OAuthFlowWorkflow, oauth.OAuthFlowInput{
		Provider:        "test",
		State:           "expected-state",
		CodeVerifier:    "v",
		RedirectURI:     "http://localhost/cb",
		StoreKey:        "default",
		CallbackTimeout: 5 * time.Second,
	})

	require.True(t, env.IsWorkflowCompleted())
	require.Error(t, env.GetWorkflowError())
	require.Contains(t, env.GetWorkflowError().Error(), "CSRFViolation")
}

func TestOAuthFlowWorkflow_MissingRequiredInputFails(t *testing.T) {
	reg := oauth.NewRegistry()
	store := oauth.NewMemoryTokenStore(oauth.NoOpCipher{})
	defer store.Close()
	acts := oauth.NewActivities(reg, map[string]oauth.TokenStore{"default": store}, oauth.NewMemoryStateMapper(), nil)

	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestWorkflowEnvironment()
	registerOAuthActivities(env, acts)

	// Missing State and CodeVerifier — should fail validation.
	env.ExecuteWorkflow(oauth.OAuthFlowWorkflow, oauth.OAuthFlowInput{
		Provider:    "test",
		RedirectURI: "http://localhost/cb",
		StoreKey:    "default",
		// State and CodeVerifier intentionally empty
	})

	require.True(t, env.IsWorkflowCompleted())
	require.Error(t, env.GetWorkflowError())
	require.Contains(t, env.GetWorkflowError().Error(), "InvalidInput")
}
