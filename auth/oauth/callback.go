// Package oauth — callback.go provides the HTTP callback handler that
// terminates an OAuth flow.
//
// The provider redirects the user back to a URL like:
//
//	GET /oauth/callback?code=abc...&state=xyz...
//
// This handler:
//
//  1. Parses the query (handles the ?error= variant too)
//  2. Looks up the workflow ID associated with the state
//  3. Signals that workflow with the auth code
//  4. Deletes the state mapping (one-shot)
//  5. Renders a "you can close this tab" page
//
// Step 2 is what the StateMapper provides. Without it we'd have no way
// to route the callback to the right running workflow.
package oauth

import (
	"fmt"
	"html"
	"io"
	"net/http"

	"go.temporal.io/sdk/client"
)

// CallbackHandlerOptions configures NewCallbackHandler.
type CallbackHandlerOptions struct {
	// SuccessHTML is the response body served when the callback is
	// processed successfully. If empty, a default minimal page is
	// rendered. The template will receive no parameters; supply a
	// fully-rendered HTML string.
	SuccessHTML string

	// ErrorWriter, if set, is used to write error responses. Defaults
	// to writing the error message as plain text with the appropriate
	// status code. Useful for replacing with a custom error page.
	ErrorWriter func(w http.ResponseWriter, statusCode int, err error)
}

// NewCallbackHandler returns an http.Handler that processes OAuth
// redirect callbacks. The handler:
//
//  1. Parses code+state from the request query string.
//  2. Looks up the workflow ID from the state mapper.
//  3. Signals the workflow with the auth code.
//  4. Deletes the state mapping.
//  5. Renders the success HTML (default or custom).
//
// Errors at any stage write an appropriate HTTP status with a brief
// error message (overridable via opts.ErrorWriter).
//
// Typical use:
//
//	http.Handle("/oauth/callback", oauth.NewCallbackHandler(tc, mapper, oauth.CallbackHandlerOptions{}))
func NewCallbackHandler(tc client.Client, mapper StateMapper, opts CallbackHandlerOptions) http.Handler {
	if tc == nil {
		panic("oauth: NewCallbackHandler: Temporal client is nil")
	}
	if mapper == nil {
		panic("oauth: NewCallbackHandler: state mapper is nil")
	}
	successHTML := opts.SuccessHTML
	if successHTML == "" {
		successHTML = defaultSuccessHTML
	}
	writeErr := opts.ErrorWriter
	if writeErr == nil {
		writeErr = defaultErrorWriter
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// 1. Parse the callback. ParseCallback handles ?error= too.
		code, err := ParseCallback(r.URL.RawQuery)
		if err != nil {
			writeErr(w, http.StatusBadRequest, err)
			return
		}

		// 2. Look up which workflow this state belongs to.
		wfID, ok := mapper.Get(r.Context(), code.State)
		if !ok {
			writeErr(w, http.StatusBadRequest, fmt.Errorf(
				"unknown state — possible CSRF, stale callback, or workflow already completed"))
			return
		}

		// 3. Signal the workflow with the auth code. The empty RunID
		//    means "the current/latest run for this workflow ID."
		if err := tc.SignalWorkflow(r.Context(), wfID, "", CallbackSignal, code); err != nil {
			writeErr(w, http.StatusInternalServerError,
				fmt.Errorf("signal workflow %q: %w", wfID, err))
			return
		}

		// 4. Clean up the state mapping. Best-effort; logging only if
		//    it fails because the signal already succeeded.
		_ = mapper.Delete(r.Context(), code.State)

		// 5. Render success.
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = io.WriteString(w, successHTML)
	})
}

// defaultSuccessHTML is the minimal success page shown when the
// callback is processed. Customize via CallbackHandlerOptions.SuccessHTML.
const defaultSuccessHTML = `<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>Authorization complete</title>
<style>
  body { font-family: system-ui, sans-serif; padding: 3rem; max-width: 600px; margin: 0 auto; color: #333; }
  .ok { color: #0a6; font-size: 1.2em; margin-bottom: 1em; }
  .hint { color: #888; font-size: 0.9em; }
</style>
</head>
<body>
<p class="ok">✓ Authorization complete.</p>
<p class="hint">You can close this tab and return to the application.</p>
</body>
</html>
`

// defaultErrorWriter writes a plain-text error response with the given
// HTTP status code.
func defaultErrorWriter(w http.ResponseWriter, status int, err error) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(status)
	_, _ = fmt.Fprintf(w, "OAuth callback error: %s\n", html.EscapeString(err.Error()))
}
