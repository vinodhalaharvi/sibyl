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

func registerOAuthActivities(env *testsuite.TestWorkflowEnvironment, acts *oauth.Activities) {
	env.RegisterActivityWithOptions(acts.BuildAuthorizeURL, activity.RegisterOptions{Name: oauth.BuildAuthorizeURLActivityName})
	env.RegisterActivityWithOptions(acts.Exchange, activity.RegisterOptions{Name: oauth.ExchangeActivityName})
	env.RegisterActivityWithOptions(acts.Whoami, activity.RegisterOptions{Name: oauth.WhoamiActivityName})
	env.RegisterActivityWithOptions(acts.StoreTokens, activity.RegisterOptions{Name: oauth.StoreTokensActivityName})
	env.RegisterActivityWithOptions(acts.StoreStateMapping, activity.RegisterOptions{Name: oauth.StoreStateMappingActivityName})
}

func TestOAuthFlowWorkflow_HappyPath(t *testing.T) {
	// Set up provider, store, and activities.
	reg := oauth.NewRegistry()
	require.NoError(t, reg.Register(makeFakeProvider("test")))

	store := oauth.NewMemoryTokenStore(oauth.NoOpCipher{})
	defer store.Close()
	mapper := oauth.NewMemoryStateMapper()

	acts := oauth.NewActivities(reg, map[string]oauth.TokenStore{"default": store}, mapper, nil)

	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestWorkflowEnvironment()
	registerOAuthActivities(env, acts)

	// Schedule the callback signal to fire shortly after the workflow
	// starts (after BuildAuthorizeURL completes and the state mapping
	// has been recorded). The delayed callback polls the mapper until
	// a state appears, then signals with that state.
	env.RegisterDelayedCallback(func() {
		// Look up the most recent state by snapshotting the mapper.
		// In a real flow the HTTP handler would do this lookup; here
		// we just grab the one and only entry we expect to see.
		snapshot := mapper.SnapshotForTests()
		require.Len(t, snapshot, 1, "expected one state mapping by now")
		var state string
		for s := range snapshot {
			state = s
		}
		env.SignalWorkflow(oauth.CallbackSignal, oauth.AuthCode{
			Code:  "test-auth-code",
			State: state,
		})
	}, 100*time.Millisecond)

	env.ExecuteWorkflow(oauth.OAuthFlowWorkflow, oauth.OAuthFlowInput{
		Provider:        "test",
		Scopes:          []string{"openid", "email"},
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

	// Don't send any callback signal. Workflow should time out.
	env.ExecuteWorkflow(oauth.OAuthFlowWorkflow, oauth.OAuthFlowInput{
		Provider:        "test",
		Scopes:          []string{"openid"},
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

	// Send a callback with a wrong state — workflow should reject it.
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(oauth.CallbackSignal, oauth.AuthCode{
			Code: "any-code", State: "wrong-state",
		})
	}, 100*time.Millisecond)

	env.ExecuteWorkflow(oauth.OAuthFlowWorkflow, oauth.OAuthFlowInput{
		Provider:        "test",
		Scopes:          []string{"openid"},
		RedirectURI:     "http://localhost/cb",
		StoreKey:        "default",
		CallbackTimeout: 5 * time.Second,
	})

	require.True(t, env.IsWorkflowCompleted())
	require.Error(t, env.GetWorkflowError())
	require.Contains(t, env.GetWorkflowError().Error(), "CSRFViolation")
}

func TestOAuthFlowWorkflow_MissingProviderInputFails(t *testing.T) {
	reg := oauth.NewRegistry()
	store := oauth.NewMemoryTokenStore(oauth.NoOpCipher{})
	defer store.Close()
	acts := oauth.NewActivities(reg, map[string]oauth.TokenStore{"default": store}, oauth.NewMemoryStateMapper(), nil)

	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestWorkflowEnvironment()
	registerOAuthActivities(env, acts)

	env.ExecuteWorkflow(oauth.OAuthFlowWorkflow, oauth.OAuthFlowInput{})

	require.True(t, env.IsWorkflowCompleted())
	require.Error(t, env.GetWorkflowError())
	require.Contains(t, env.GetWorkflowError().Error(), "InvalidInput")
}

// --- end of file ---
