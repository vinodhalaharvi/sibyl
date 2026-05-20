package agents_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vinodhalaharvi/weft/weft"

	"github.com/vinodhalaharvi/sibyl/agents"
	"github.com/vinodhalaharvi/sibyl/auth/oauth"
)

// === Test fixtures ===

// reqWithToken is a typical agent input: a domain payload plus a slot
// for the OAuth bearer token. StructFieldInjector targets the Token field.
type reqWithToken struct {
	Op    string
	Token oauth.TokenPair
}

// resp is the agent's return value.
type resp struct {
	OpEcho       string
	SeenToken    string
	SeenExpires  time.Time
	SeenIdentity string
}

// newStore builds an in-memory store with NoOpCipher and optionally seeds
// it with one token for (canonical, provider).
func newStore(t *testing.T, canonical, provider string, tokens *oauth.TokenPair) oauth.TokenStore {
	t.Helper()
	s := oauth.NewMemoryTokenStore(oauth.NoOpCipher{})
	if tokens != nil {
		err := s.Put(context.Background(), oauth.Identity{Canonical: canonical, Provider: provider}, provider, *tokens)
		if err != nil {
			t.Fatalf("seed token: %v", err)
		}
	}
	return s
}

// freshToken returns a TokenPair that's valid for an hour.
func freshToken(access, refresh string) oauth.TokenPair {
	return oauth.TokenPair{
		AccessToken:  access,
		RefreshToken: refresh,
		TokenType:    "Bearer",
		ExpiresAt:    time.Now().Add(time.Hour),
	}
}

// counterRefresh returns a RefreshArrow that bumps an int each call and
// produces a new TokenPair whose AccessToken is "refreshed-<n>".
func counterRefresh(counter *int32) oauth.RefreshArrow {
	return func(_ context.Context, _ oauth.TokenPair) (oauth.TokenPair, error) {
		n := atomic.AddInt32(counter, 1)
		return oauth.TokenPair{
			AccessToken:  fmt.Sprintf("refreshed-%d", n),
			RefreshToken: "rt",
			TokenType:    "Bearer",
			ExpiresAt:    time.Now().Add(time.Hour),
		}, nil
	}
}

// invokedByCtx returns a context with InvokedBy populated.
func invokedByCtx(canonical string) context.Context {
	return agents.WithAgentContext(context.Background(), agents.AgentContext{
		AgentID:   "TestAgent",
		InvokedBy: canonical,
	})
}

// stdInject builds an Inject arrow for reqWithToken that writes into the
// Token field via StructFieldInjector.
func stdInject() weft.Arrow[agents.InjectInput[reqWithToken], reqWithToken] {
	return agents.StructFieldInjector(func(r *reqWithToken, tok oauth.TokenPair) {
		r.Token = tok
	})
}

// echoInner returns a Run arrow that echoes the access token + op into resp.
func echoInner() weft.Arrow[reqWithToken, resp] {
	return func(_ context.Context, r reqWithToken) (resp, error) {
		return resp{
			OpEcho:      r.Op,
			SeenToken:   r.Token.AccessToken,
			SeenExpires: r.Token.ExpiresAt,
		}, nil
	}
}

// === Tests ===

// Happy path: token in store, gets injected, inner sees it, no refresh.
func TestWithOAuth_HappyPath(t *testing.T) {
	tok := freshToken("at-1", "rt-1")
	store := newStore(t, "user:alice", "github", &tok)

	var refreshCount int32
	refresh := counterRefresh(&refreshCount)

	beh := agents.WithOAuth[reqWithToken, resp](store, refresh, agents.OAuthPolicy[reqWithToken]{
		Provider:        "github",
		ResolveIdentity: agents.InvokedByIdentity[reqWithToken]("github"),
		Inject:          stdInject(),
	})

	agent, err := agents.Register(agents.Spec[reqWithToken, resp]{
		ID:        "GithubReader",
		Vendors:   []string{"github"},
		Run:       echoInner(),
		Behaviors: []agents.Behavior[reqWithToken, resp]{beh},
	})
	if err != nil {
		t.Fatal(err)
	}

	got, err := agent.Run(invokedByCtx("user:alice"), reqWithToken{Op: "list-repos"})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got.OpEcho != "list-repos" {
		t.Errorf("OpEcho = %q, want list-repos", got.OpEcho)
	}
	if got.SeenToken != "at-1" {
		t.Errorf("SeenToken = %q, want at-1 (token not injected from store)", got.SeenToken)
	}
	if atomic.LoadInt32(&refreshCount) != 0 {
		t.Errorf("refresh was called %d times; expected 0 on happy path", refreshCount)
	}
}

// Auth error from inner triggers refresh + retry exactly once. The retry
// must see the newly refreshed token, not the original.
func TestWithOAuth_AuthErrorTriggersRefreshAndRetry(t *testing.T) {
	tok := freshToken("at-stale", "rt-stale")
	store := newStore(t, "user:bob", "okta", &tok)

	var refreshCount int32
	refresh := counterRefresh(&refreshCount)

	// Inner returns AuthenticationError on first call (stale token),
	// success on second (refreshed token).
	var innerCalls int32
	var lastTokenSeen string
	inner := func(_ context.Context, r reqWithToken) (resp, error) {
		atomic.AddInt32(&innerCalls, 1)
		lastTokenSeen = r.Token.AccessToken
		if r.Token.AccessToken == "at-stale" {
			return resp{}, oauth.AuthenticationError{Status: 401}
		}
		return resp{OpEcho: r.Op, SeenToken: r.Token.AccessToken}, nil
	}

	beh := agents.WithOAuth[reqWithToken, resp](store, refresh, agents.OAuthPolicy[reqWithToken]{
		Provider:        "okta",
		ResolveIdentity: agents.InvokedByIdentity[reqWithToken]("okta"),
		Inject:          stdInject(),
	})

	wrapped := beh(inner)
	got, err := wrapped(invokedByCtx("user:bob"), reqWithToken{Op: "whoami"})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if atomic.LoadInt32(&innerCalls) != 2 {
		t.Errorf("inner called %d times, want 2 (initial + retry)", innerCalls)
	}
	if atomic.LoadInt32(&refreshCount) != 1 {
		t.Errorf("refresh called %d times, want 1", refreshCount)
	}
	if got.SeenToken != "refreshed-1" {
		t.Errorf("retry used token %q, want refreshed-1", got.SeenToken)
	}
	if lastTokenSeen != "refreshed-1" {
		t.Errorf("last inner saw %q, want refreshed-1", lastTokenSeen)
	}

	// Store should now hold the refreshed pair (oauth.WithRefresh persists).
	stored, err := store.Get(context.Background(),
		oauth.Identity{Canonical: "user:bob", Provider: "okta"}, "okta")
	if err != nil {
		t.Fatalf("post-refresh Get: %v", err)
	}
	if stored.AccessToken != "refreshed-1" {
		t.Errorf("stored token = %q after refresh, want refreshed-1", stored.AccessToken)
	}
}

// Missing token is a HARD error per the pilot's design call. It surfaces
// as a typed MissingCredentialError carrying the identity and provider,
// and Unwraps to oauth.ErrTokenNotFound for low-level matching.
func TestWithOAuth_MissingCredentialIsHardError(t *testing.T) {
	store := newStore(t, "", "", nil) // empty store

	var refreshCount int32
	refresh := counterRefresh(&refreshCount)

	beh := agents.WithOAuth[reqWithToken, resp](store, refresh, agents.OAuthPolicy[reqWithToken]{
		Provider:        "github",
		ResolveIdentity: agents.InvokedByIdentity[reqWithToken]("github"),
		Inject:          stdInject(),
	})

	var innerCalls int32
	inner := func(_ context.Context, _ reqWithToken) (resp, error) {
		atomic.AddInt32(&innerCalls, 1)
		return resp{}, nil
	}

	wrapped := beh(inner)
	_, err := wrapped(invokedByCtx("user:carol"), reqWithToken{Op: "x"})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	var missing agents.MissingCredentialError
	if !errors.As(err, &missing) {
		t.Fatalf("err = %v, want MissingCredentialError", err)
	}
	if missing.Identity.Canonical != "user:carol" {
		t.Errorf("MissingCredentialError.Identity.Canonical = %q, want user:carol", missing.Identity.Canonical)
	}
	if missing.Provider != "github" {
		t.Errorf("MissingCredentialError.Provider = %q, want github", missing.Provider)
	}
	if !errors.Is(err, oauth.ErrTokenNotFound) {
		t.Error("MissingCredentialError should unwrap to oauth.ErrTokenNotFound")
	}
	if atomic.LoadInt32(&innerCalls) != 0 {
		t.Errorf("inner called %d times on missing credential, want 0", innerCalls)
	}
	if atomic.LoadInt32(&refreshCount) != 0 {
		t.Errorf("refresh called %d times on missing credential, want 0", refreshCount)
	}
}

// Missing AgentContext.InvokedBy surfaces MissingIdentityError before
// any store/refresh activity. The default resolver enforces "you must
// say who you're acting as."
func TestWithOAuth_MissingInvokedByIsHardError(t *testing.T) {
	tok := freshToken("at", "rt")
	store := newStore(t, "user:never-used", "github", &tok)

	var refreshCount int32
	refresh := counterRefresh(&refreshCount)

	beh := agents.WithOAuth[reqWithToken, resp](store, refresh, agents.OAuthPolicy[reqWithToken]{
		Provider:        "github",
		ResolveIdentity: agents.InvokedByIdentity[reqWithToken]("github"),
		Inject:          stdInject(),
	})

	var innerCalls int32
	inner := func(_ context.Context, _ reqWithToken) (resp, error) {
		atomic.AddInt32(&innerCalls, 1)
		return resp{}, nil
	}

	wrapped := beh(inner)
	// No AgentContext attached -> InvokedBy is empty -> resolver fails.
	_, err := wrapped(context.Background(), reqWithToken{Op: "x"})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	var miss agents.MissingIdentityError
	if !errors.As(err, &miss) {
		t.Fatalf("err = %v, want MissingIdentityError", err)
	}
	if miss.Provider != "github" {
		t.Errorf("MissingIdentityError.Provider = %q, want github", miss.Provider)
	}
	if atomic.LoadInt32(&innerCalls) != 0 {
		t.Errorf("inner called %d times when identity missing, want 0", innerCalls)
	}
	if atomic.LoadInt32(&refreshCount) != 0 {
		t.Errorf("refresh called %d times when identity missing, want 0", refreshCount)
	}
}

// FixedIdentity overrides the default and lets a service-account agent
// run without an InvokedBy in context.
func TestWithOAuth_FixedIdentityOverridesInvokedBy(t *testing.T) {
	serviceID := oauth.Identity{Canonical: "service:slack-bot", Provider: "slack"}
	tok := freshToken("bot-at", "bot-rt")
	store := oauth.NewMemoryTokenStore(oauth.NoOpCipher{})
	if err := store.Put(context.Background(), serviceID, "slack", tok); err != nil {
		t.Fatal(err)
	}

	var refreshCount int32
	refresh := counterRefresh(&refreshCount)

	beh := agents.WithOAuth[reqWithToken, resp](store, refresh, agents.OAuthPolicy[reqWithToken]{
		Provider:        "slack",
		ResolveIdentity: agents.FixedIdentity[reqWithToken](serviceID),
		Inject: agents.StructFieldInjector(func(r *reqWithToken, tok oauth.TokenPair) {
			r.Token = tok
		}),
	})

	wrapped := beh(echoInner())
	// No AgentContext: would fail with InvokedByIdentity, but FixedIdentity
	// doesn't look at it.
	got, err := wrapped(context.Background(), reqWithToken{Op: "post-message"})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got.SeenToken != "bot-at" {
		t.Errorf("SeenToken = %q, want bot-at", got.SeenToken)
	}
}

// StructFieldInjector does not mutate the original input — only the
// copy passed to inner. Behavior callers should be able to log the
// original input separately after Inject ran.
func TestStructFieldInjector_DoesNotMutateOriginal(t *testing.T) {
	tok := freshToken("at", "rt")
	store := newStore(t, "user:dave", "github", &tok)

	var refreshCount int32
	refresh := counterRefresh(&refreshCount)

	beh := agents.WithOAuth[reqWithToken, resp](store, refresh, agents.OAuthPolicy[reqWithToken]{
		Provider:        "github",
		ResolveIdentity: agents.InvokedByIdentity[reqWithToken]("github"),
		Inject:          stdInject(),
	})

	wrapped := beh(echoInner())
	original := reqWithToken{Op: "fetch"}
	got, err := wrapped(invokedByCtx("user:dave"), original)
	if err != nil {
		t.Fatal(err)
	}
	if original.Token.AccessToken != "" {
		t.Errorf("original mutated: Token.AccessToken = %q, want empty", original.Token.AccessToken)
	}
	if got.SeenToken != "at" {
		t.Errorf("SeenToken = %q, want at", got.SeenToken)
	}
}

// A non-auth error inside inner is NOT retried — only AuthenticationError
// is. This matches oauth.WithRefresh's documented contract.
func TestWithOAuth_NonAuthErrorIsNotRetried(t *testing.T) {
	tok := freshToken("at", "rt")
	store := newStore(t, "user:eve", "github", &tok)

	var refreshCount int32
	refresh := counterRefresh(&refreshCount)

	var innerCalls int32
	sentinel := errors.New("downstream failure unrelated to auth")
	inner := func(_ context.Context, _ reqWithToken) (resp, error) {
		atomic.AddInt32(&innerCalls, 1)
		return resp{}, sentinel
	}

	beh := agents.WithOAuth[reqWithToken, resp](store, refresh, agents.OAuthPolicy[reqWithToken]{
		Provider:        "github",
		ResolveIdentity: agents.InvokedByIdentity[reqWithToken]("github"),
		Inject:          stdInject(),
	})

	wrapped := beh(inner)
	_, err := wrapped(invokedByCtx("user:eve"), reqWithToken{Op: "x"})
	if !errors.Is(err, sentinel) {
		t.Errorf("err = %v, want wrapped sentinel", err)
	}
	if atomic.LoadInt32(&innerCalls) != 1 {
		t.Errorf("inner called %d times on non-auth error, want 1", innerCalls)
	}
	if atomic.LoadInt32(&refreshCount) != 0 {
		t.Errorf("refresh called %d times on non-auth error, want 0", refreshCount)
	}
}

// Behaviors are opt-in: an agent that lists Vendors but does NOT include
// WithOAuth in its Behaviors runs without any auth machinery. This is the
// "Vendors is declarative, WithOAuth is the wiring" invariant.
func TestWithOAuth_NotAppliedMeansNoAuthMachinery(t *testing.T) {
	// No WithOAuth in Behaviors; Vendors declared anyway.
	a, err := agents.Register(agents.Spec[reqWithToken, resp]{
		ID:      "DeclaresVendorButNoBehavior",
		Vendors: []string{"github"},
		Run:     echoInner(),
	})
	if err != nil {
		t.Fatal(err)
	}
	got, err := a.Run(context.Background(), reqWithToken{Op: "list"})
	if err != nil {
		t.Fatalf("Run without auth behavior: %v", err)
	}
	if got.SeenToken != "" {
		t.Errorf("SeenToken = %q without WithOAuth applied, want empty", got.SeenToken)
	}
}

// === Construction validation ===

func TestWithOAuth_PanicsOnNilStore(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic on nil store")
		}
	}()
	_ = agents.WithOAuth[reqWithToken, resp](
		nil,
		counterRefresh(new(int32)),
		agents.OAuthPolicy[reqWithToken]{
			Provider:        "x",
			ResolveIdentity: agents.InvokedByIdentity[reqWithToken]("x"),
			Inject:          stdInject(),
		},
	)
}

func TestWithOAuth_PanicsOnEmptyProvider(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic on empty Provider")
		}
	}()
	_ = agents.WithOAuth[reqWithToken, resp](
		oauth.NewMemoryTokenStore(oauth.NoOpCipher{}),
		counterRefresh(new(int32)),
		agents.OAuthPolicy[reqWithToken]{
			Provider:        "",
			ResolveIdentity: agents.InvokedByIdentity[reqWithToken]("x"),
			Inject:          stdInject(),
		},
	)
}

func TestInvokedByIdentity_PanicsOnEmptyProvider(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic on empty provider")
		}
	}()
	_ = agents.InvokedByIdentity[int]("")
}

func TestFixedIdentity_PanicsOnEmptyCanonical(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic on empty Canonical")
		}
	}()
	_ = agents.FixedIdentity[int](oauth.Identity{})
}

func TestStructFieldInjector_PanicsOnNilMutate(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic on nil mutate")
		}
	}()
	_ = agents.StructFieldInjector[int](nil)
}
