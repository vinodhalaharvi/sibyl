package oauth_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/vinodhalaharvi/sibyl/auth/oauth"
)

// --- AuthRequest generation -----------------------------------------------

func TestNewAuthRequest_GeneratesValidPKCE(t *testing.T) {
	req, err := oauth.NewAuthRequest("okta", "http://localhost:8090/cb", []string{"openid", "email"})
	require.NoError(t, err)
	require.Equal(t, "okta", req.Provider)
	require.Equal(t, []string{"openid", "email"}, req.Scopes)
	require.Equal(t, "http://localhost:8090/cb", req.RedirectURI)
	// PKCE invariants:
	require.NotEmpty(t, req.State)
	require.NotEmpty(t, req.CodeVerifier)
	require.NotEmpty(t, req.CodeChallenge)
	require.NotEqual(t, req.CodeVerifier, req.CodeChallenge, "challenge must differ from verifier")
	// Length checks per RFC 7636: 43-128 chars for verifier.
	require.GreaterOrEqual(t, len(req.CodeVerifier), 43)
	require.LessOrEqual(t, len(req.CodeVerifier), 128)
	// State should have enough entropy to never collide (43+ chars).
	require.GreaterOrEqual(t, len(req.State), 32)
}

func TestNewAuthRequest_EachCallGeneratesFreshValues(t *testing.T) {
	// Two consecutive calls must produce different State + CodeVerifier
	// to prevent replay or correlation attacks.
	r1, err := oauth.NewAuthRequest("okta", "u", nil)
	require.NoError(t, err)
	r2, err := oauth.NewAuthRequest("okta", "u", nil)
	require.NoError(t, err)
	require.NotEqual(t, r1.State, r2.State)
	require.NotEqual(t, r1.CodeVerifier, r2.CodeVerifier)
}

// --- TokenPair behavior --------------------------------------------------

func TestTokenPair_IsExpired(t *testing.T) {
	past := oauth.TokenPair{ExpiresAt: time.Now().Add(-time.Hour)}
	future := oauth.TokenPair{ExpiresAt: time.Now().Add(time.Hour)}
	zero := oauth.TokenPair{}

	require.True(t, past.IsExpired())
	require.False(t, future.IsExpired())
	require.False(t, zero.IsExpired(), "zero expiry treated as never-expires")
}

func TestTokenPair_IsExpiringWithin(t *testing.T) {
	future := oauth.TokenPair{ExpiresAt: time.Now().Add(30 * time.Second)}
	require.True(t, future.IsExpiringWithin(time.Minute))
	require.False(t, future.IsExpiringWithin(10*time.Second))
}

// --- AES-GCM cipher round-trip --------------------------------------------

func TestAESGCMCipher_RoundTrip(t *testing.T) {
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i)
	}
	cipher, err := oauth.NewAESGCMCipher(key)
	require.NoError(t, err)

	plaintext := []byte("super secret token material with special chars: 🔐 +\"=&")
	ciphertext, err := cipher.Encrypt(plaintext)
	require.NoError(t, err)
	require.NotEqual(t, plaintext, ciphertext)

	decrypted, err := cipher.Decrypt(ciphertext)
	require.NoError(t, err)
	require.Equal(t, plaintext, decrypted)
}

func TestAESGCMCipher_DifferentNoncesEachEncryption(t *testing.T) {
	// Encrypting the same plaintext twice must produce different
	// ciphertexts (because the nonce is fresh each call). This is
	// a core security property of GCM.
	key := make([]byte, 32)
	cipher, _ := oauth.NewAESGCMCipher(key)
	plaintext := []byte("same plaintext both times")
	c1, _ := cipher.Encrypt(plaintext)
	c2, _ := cipher.Encrypt(plaintext)
	require.NotEqual(t, c1, c2)
}

func TestAESGCMCipher_RejectsBadKeyLength(t *testing.T) {
	_, err := oauth.NewAESGCMCipher(make([]byte, 16)) // AES-128 key — wrong
	require.Error(t, err)
	require.Contains(t, err.Error(), "32-byte key")
}

func TestAESGCMCipher_TamperingFailsAuthCheck(t *testing.T) {
	key := make([]byte, 32)
	cipher, _ := oauth.NewAESGCMCipher(key)
	ciphertext, _ := cipher.Encrypt([]byte("data"))
	// Flip a byte in the ciphertext.
	ciphertext[len(ciphertext)-1] ^= 0xff
	_, err := cipher.Decrypt(ciphertext)
	require.Error(t, err, "tampering must fail GCM auth check")
}

func TestNoOpCipher_PassesThrough(t *testing.T) {
	c := oauth.NoOpCipher{}
	in := []byte("plain")
	out, err := c.Encrypt(in)
	require.NoError(t, err)
	require.Equal(t, in, out)
	dec, err := c.Decrypt(out)
	require.NoError(t, err)
	require.Equal(t, in, dec)
}

// --- MemoryTokenStore -----------------------------------------------------

func sampleIdentity() oauth.Identity {
	return oauth.Identity{
		Canonical:   "okta:00u123",
		Provider:    "okta",
		ProviderID:  "00u123",
		Email:       "user@example.com",
		DisplayName: "Test User",
	}
}

func sampleTokens() oauth.TokenPair {
	return oauth.TokenPair{
		AccessToken:  "access-token-xyz",
		RefreshToken: "refresh-token-xyz",
		TokenType:    "Bearer",
		ExpiresAt:    time.Now().Add(time.Hour),
		Scope:        "openid email",
	}
}

func TestMemoryTokenStore_BasicCRUD(t *testing.T) {
	store := oauth.NewMemoryTokenStore(oauth.NoOpCipher{})
	defer store.Close()

	id := sampleIdentity()
	tokens := sampleTokens()

	// Initially empty.
	_, err := store.Get(context.Background(), id, "okta")
	require.ErrorIs(t, err, oauth.ErrTokenNotFound)

	// Put + Get round-trip.
	require.NoError(t, store.Put(context.Background(), id, "okta", tokens))
	got, err := store.Get(context.Background(), id, "okta")
	require.NoError(t, err)
	require.Equal(t, tokens.AccessToken, got.AccessToken)
	require.Equal(t, tokens.RefreshToken, got.RefreshToken)

	// List.
	refs, err := store.List(context.Background())
	require.NoError(t, err)
	require.Len(t, refs, 1)
	require.Equal(t, "okta:00u123", refs[0].Identity.Canonical)
	require.Equal(t, "okta", refs[0].Provider)

	// Delete.
	require.NoError(t, store.Delete(context.Background(), id, "okta"))
	_, err = store.Get(context.Background(), id, "okta")
	require.ErrorIs(t, err, oauth.ErrTokenNotFound)
}

func TestMemoryTokenStore_EncryptsWithRealCipher(t *testing.T) {
	// Use a real cipher; verify that round-tripping through Put/Get
	// returns the original plaintext.
	key := make([]byte, 32)
	c, _ := oauth.NewAESGCMCipher(key)
	store := oauth.NewMemoryTokenStore(c)
	defer store.Close()

	id := sampleIdentity()
	tokens := sampleTokens()
	require.NoError(t, store.Put(context.Background(), id, "okta", tokens))
	got, err := store.Get(context.Background(), id, "okta")
	require.NoError(t, err)
	require.Equal(t, tokens.AccessToken, got.AccessToken)
}

func TestMemoryTokenStore_NilCipherPanics(t *testing.T) {
	require.Panics(t, func() {
		oauth.NewMemoryTokenStore(nil)
	})
}

// --- SQLiteTokenStore -----------------------------------------------------

func TestSQLiteTokenStore_BasicCRUD(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "tokens.db")
	store, err := oauth.NewSQLiteTokenStore(path, oauth.NoOpCipher{})
	require.NoError(t, err)
	defer store.Close()

	id := sampleIdentity()
	tokens := sampleTokens()

	_, err = store.Get(context.Background(), id, "okta")
	require.ErrorIs(t, err, oauth.ErrTokenNotFound)

	require.NoError(t, store.Put(context.Background(), id, "okta", tokens))
	got, err := store.Get(context.Background(), id, "okta")
	require.NoError(t, err)
	require.Equal(t, tokens.AccessToken, got.AccessToken)
}

func TestSQLiteTokenStore_PersistsAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "tokens.db")

	id := sampleIdentity()
	tokens := sampleTokens()

	// First open: write a token.
	store1, err := oauth.NewSQLiteTokenStore(path, oauth.NoOpCipher{})
	require.NoError(t, err)
	require.NoError(t, store1.Put(context.Background(), id, "okta", tokens))
	require.NoError(t, store1.Close())

	// Second open on same path: token survives.
	store2, err := oauth.NewSQLiteTokenStore(path, oauth.NoOpCipher{})
	require.NoError(t, err)
	defer store2.Close()
	got, err := store2.Get(context.Background(), id, "okta")
	require.NoError(t, err)
	require.Equal(t, tokens.AccessToken, got.AccessToken)
}

func TestSQLiteTokenStore_PutOverwrites(t *testing.T) {
	dir := t.TempDir()
	store, err := oauth.NewSQLiteTokenStore(filepath.Join(dir, "t.db"), oauth.NoOpCipher{})
	require.NoError(t, err)
	defer store.Close()

	id := sampleIdentity()
	t1 := sampleTokens()
	t1.AccessToken = "first"
	require.NoError(t, store.Put(context.Background(), id, "okta", t1))

	t2 := sampleTokens()
	t2.AccessToken = "second"
	require.NoError(t, store.Put(context.Background(), id, "okta", t2))

	got, _ := store.Get(context.Background(), id, "okta")
	require.Equal(t, "second", got.AccessToken)
}

func TestSQLiteTokenStore_ListReturnsAllRefs(t *testing.T) {
	dir := t.TempDir()
	store, err := oauth.NewSQLiteTokenStore(filepath.Join(dir, "t.db"), oauth.NoOpCipher{})
	require.NoError(t, err)
	defer store.Close()

	for i := 0; i < 3; i++ {
		id := oauth.Identity{
			Canonical:  "okta:user" + string(rune('A'+i)),
			Provider:   "okta",
			ProviderID: "user" + string(rune('A'+i)),
		}
		require.NoError(t, store.Put(context.Background(), id, "okta", sampleTokens()))
	}
	refs, err := store.List(context.Background())
	require.NoError(t, err)
	require.Len(t, refs, 3)
}

func TestSQLiteTokenStore_EncryptsAtRest(t *testing.T) {
	dir := t.TempDir()
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i)
	}
	cipher, _ := oauth.NewAESGCMCipher(key)

	store, err := oauth.NewSQLiteTokenStore(filepath.Join(dir, "encrypted.db"), cipher)
	require.NoError(t, err)
	defer store.Close()

	id := sampleIdentity()
	tokens := sampleTokens()
	tokens.AccessToken = "this-is-the-secret-token"
	require.NoError(t, store.Put(context.Background(), id, "okta", tokens))

	// Round-trip via Get returns plaintext.
	got, err := store.Get(context.Background(), id, "okta")
	require.NoError(t, err)
	require.Equal(t, "this-is-the-secret-token", got.AccessToken)

	// Read raw bytes from the DB file to verify the plaintext isn't there.
	// SQLite stores BLOBs verbatim, so a string search finds the literal
	// token if it was written unencrypted.
	require.NoError(t, store.Close())
	dbBytes := readWholeFile(t, filepath.Join(dir, "encrypted.db"))
	require.False(t, strings.Contains(string(dbBytes), "this-is-the-secret-token"),
		"plaintext token should not appear in encrypted DB file")
}

// --- MemoryStateMapper ----------------------------------------------------

func TestMemoryStateMapper_BasicLookup(t *testing.T) {
	m := oauth.NewMemoryStateMapper()
	require.NoError(t, m.Put(context.Background(), "state-abc", "workflow-1"))
	id, ok := m.Get(context.Background(), "state-abc")
	require.True(t, ok)
	require.Equal(t, "workflow-1", id)

	// Unknown state.
	_, ok = m.Get(context.Background(), "missing")
	require.False(t, ok)

	// Delete.
	require.NoError(t, m.Delete(context.Background(), "state-abc"))
	_, ok = m.Get(context.Background(), "state-abc")
	require.False(t, ok)
}

func TestMemoryStateMapper_RejectsEmptyState(t *testing.T) {
	m := oauth.NewMemoryStateMapper()
	require.Error(t, m.Put(context.Background(), "", "wf"))
}

// --- ParseCallback --------------------------------------------------------

func TestParseCallback_HappyPath(t *testing.T) {
	code, err := oauth.ParseCallback("code=abc123&state=xyz")
	require.NoError(t, err)
	require.Equal(t, "abc123", code.Code)
	require.Equal(t, "xyz", code.State)
}

func TestParseCallback_MissingCode(t *testing.T) {
	_, err := oauth.ParseCallback("state=xyz")
	require.Error(t, err)
	require.Contains(t, err.Error(), "missing code or state")
}

func TestParseCallback_ErrorResponse(t *testing.T) {
	// If the provider redirects with ?error=..., we surface that
	// as an error rather than returning a zero AuthCode.
	_, err := oauth.ParseCallback("error=access_denied&error_description=user+said+no")
	require.Error(t, err)
	require.Contains(t, err.Error(), "access_denied")
}

// --- Registry -------------------------------------------------------------

func TestRegistry_RegisterAndGet(t *testing.T) {
	r := oauth.NewRegistry()
	p := makeFakeProvider("test")
	require.NoError(t, r.Register(p))

	got, err := r.Get("test")
	require.NoError(t, err)
	require.Equal(t, "test", got.Name)
}

func TestRegistry_RejectsDuplicate(t *testing.T) {
	r := oauth.NewRegistry()
	require.NoError(t, r.Register(makeFakeProvider("test")))
	require.Error(t, r.Register(makeFakeProvider("test")))
}

func TestRegistry_RejectsEmptyName(t *testing.T) {
	r := oauth.NewRegistry()
	p := makeFakeProvider("")
	require.Error(t, r.Register(p))
}

func TestRegistry_RejectsIncompleteProvider(t *testing.T) {
	r := oauth.NewRegistry()
	p := makeFakeProvider("incomplete")
	p.Whoami = nil
	require.Error(t, r.Register(p))
}

// --- helpers -------------------------------------------------------------

func makeFakeProvider(name string) oauth.Provider {
	return oauth.Provider{
		Name: name,
		Authorize: func(_ context.Context, in oauth.AuthRequest) (oauth.AuthorizeRedirect, error) {
			return oauth.AuthorizeRedirect{
				URL: "https://fake/authorize?state=" + in.State, State: in.State, CodeVerifier: in.CodeVerifier,
			}, nil
		},
		Exchange: func(_ context.Context, in oauth.ExchangeRequest) (oauth.TokenPair, error) {
			return oauth.TokenPair{AccessToken: "fake-" + in.Code.Code, ExpiresAt: time.Now().Add(time.Hour)}, nil
		},
		Refresh: func(_ context.Context, tp oauth.TokenPair) (oauth.TokenPair, error) {
			return oauth.TokenPair{AccessToken: "refreshed-" + tp.AccessToken, ExpiresAt: time.Now().Add(time.Hour)}, nil
		},
		Whoami: func(_ context.Context, _ oauth.TokenPair) (oauth.Identity, error) {
			return oauth.Identity{Canonical: name + ":test-user", Provider: name, ProviderID: "test-user"}, nil
		},
	}
}

func readWholeFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	return data
}
