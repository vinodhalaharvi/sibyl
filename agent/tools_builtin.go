// Package agent — tools_builtin.go provides three ready-to-use tools:
// web_search, calculator, file_read.
//
// Each is a concrete implementation of the Tool interface. Register
// them on a ToolRegistry to make them available to a ToolAgent.
//
// Implementation choices:
//
//   - web_search uses DuckDuckGo's no-API-key Instant Answer endpoint.
//     For most production use you'd want a real search API (Brave,
//     Tavily, Serper, etc) — this is enough to demonstrate the shape.
//
//   - calculator parses simple arithmetic expressions: +, -, *, /,
//     parens. Pure Go, no eval, safe for untrusted LLM-emitted input.
//
//   - file_read reads files under a configurable allow-list of
//     directories. Path traversal attempts are rejected. The LLM
//     cannot read files outside the allowed roots.
package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"unicode"
)

// --- WebSearchTool ----------------------------------------------------------

// WebSearchTool queries DuckDuckGo's Instant Answer API. It's a tiny,
// free, no-key search source — enough to demonstrate tool integration.
// Swap for a real search API in production.
type WebSearchTool struct {
	HTTPClient *http.Client // defaults to a 10s-timeout client
	UserAgent  string       // optional; default identifies as sibyl
	MaxResults int          // default 3
}

// NewWebSearchTool returns a WebSearchTool with sensible defaults.
func NewWebSearchTool() *WebSearchTool {
	return &WebSearchTool{
		HTTPClient: &http.Client{Timeout: 10 * time.Second},
		UserAgent:  "sibyl-agent/0.1",
		MaxResults: 3,
	}
}

func (w *WebSearchTool) Name() string { return "web_search" }
func (w *WebSearchTool) Description() string {
	return "Search the web for current information. Returns short summaries of the top hits. Use for factual lookups about people, places, events, or recent news."
}
func (w *WebSearchTool) ArgsSchema() ToolArgsSchema {
	return ToolArgsSchema{
		Args: []ToolArg{
			{Name: "query", Type: "string", Description: "search terms", Required: true},
		},
	}
}

func (w *WebSearchTool) Run(ctx context.Context, args map[string]any) (string, error) {
	q, _ := args["query"].(string)
	q = strings.TrimSpace(q)
	if q == "" {
		return "", errors.New("web_search: query is required")
	}

	endpoint := "https://api.duckduckgo.com/?" + url.Values{
		"q":             {q},
		"format":        {"json"},
		"no_html":       {"1"},
		"skip_disambig": {"1"},
	}.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return "", fmt.Errorf("web_search: build request: %w", err)
	}
	if w.UserAgent != "" {
		req.Header.Set("User-Agent", w.UserAgent)
	}

	resp, err := w.HTTPClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("web_search: http: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("web_search: read body: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("web_search: status %d", resp.StatusCode)
	}

	// DDG returns a rich object. We pluck the highest-signal fields.
	var ddg struct {
		AbstractText   string `json:"AbstractText"`
		AbstractURL    string `json:"AbstractURL"`
		AbstractSource string `json:"AbstractSource"`
		Heading        string `json:"Heading"`
		Answer         string `json:"Answer"`
		AnswerType     string `json:"AnswerType"`
		RelatedTopics  []struct {
			Text string `json:"Text"`
			URL  string `json:"FirstURL"`
		} `json:"RelatedTopics"`
	}
	if err := json.Unmarshal(body, &ddg); err != nil {
		return "", fmt.Errorf("web_search: parse json: %w", err)
	}

	var out strings.Builder
	if ddg.Answer != "" {
		fmt.Fprintf(&out, "Direct answer (%s): %s\n", ddg.AnswerType, ddg.Answer)
	}
	if ddg.AbstractText != "" {
		fmt.Fprintf(&out, "%s — %s (%s)\n",
			ddg.Heading, ddg.AbstractText, ddg.AbstractSource)
		if ddg.AbstractURL != "" {
			fmt.Fprintf(&out, "Source: %s\n", ddg.AbstractURL)
		}
	}
	if len(ddg.RelatedTopics) > 0 {
		max := w.MaxResults
		if max <= 0 {
			max = 3
		}
		out.WriteString("\nRelated:\n")
		for i, r := range ddg.RelatedTopics {
			if i >= max {
				break
			}
			if r.Text == "" {
				continue
			}
			fmt.Fprintf(&out, "- %s\n", r.Text)
			if r.URL != "" {
				fmt.Fprintf(&out, "  %s\n", r.URL)
			}
		}
	}

	result := strings.TrimSpace(out.String())
	if result == "" {
		return fmt.Sprintf("No results found for %q.", q), nil
	}
	return result, nil
}

// --- CalculatorTool ---------------------------------------------------------

// CalculatorTool evaluates simple arithmetic expressions. Supports
// +, -, *, /, parens, and unary minus. Pure Go parser — does NOT eval
// arbitrary input. Safe for LLM-emitted expressions.
type CalculatorTool struct{}

func (c *CalculatorTool) Name() string { return "calculator" }
func (c *CalculatorTool) Description() string {
	return "Evaluate a basic arithmetic expression. Supports +, -, *, /, parentheses, and unary minus. Numbers can be integers or decimals. Example: \"(2 + 3) * 4.5\"."
}
func (c *CalculatorTool) ArgsSchema() ToolArgsSchema {
	return ToolArgsSchema{
		Args: []ToolArg{
			{Name: "expression", Type: "string", Description: "arithmetic expression to evaluate", Required: true},
		},
	}
}

func (c *CalculatorTool) Run(_ context.Context, args map[string]any) (string, error) {
	expr, _ := args["expression"].(string)
	if strings.TrimSpace(expr) == "" {
		return "", errors.New("calculator: expression is required")
	}
	p := &arithParser{src: expr}
	result, err := p.parseExpression()
	if err != nil {
		return "", fmt.Errorf("calculator: %w", err)
	}
	p.skipSpaces()
	if p.pos != len(p.src) {
		return "", fmt.Errorf("calculator: unexpected input at position %d: %q",
			p.pos, p.src[p.pos:])
	}
	return strconv.FormatFloat(result, 'g', -1, 64), nil
}

// arithParser is a recursive-descent parser for the grammar:
//
//	expr   = term (("+"|"-") term)*
//	term   = factor (("*"|"/") factor)*
//	factor = "-" factor | "(" expr ")" | NUMBER
type arithParser struct {
	src string
	pos int
}

func (p *arithParser) skipSpaces() {
	for p.pos < len(p.src) && unicode.IsSpace(rune(p.src[p.pos])) {
		p.pos++
	}
}

func (p *arithParser) parseExpression() (float64, error) {
	left, err := p.parseTerm()
	if err != nil {
		return 0, err
	}
	for {
		p.skipSpaces()
		if p.pos >= len(p.src) {
			return left, nil
		}
		op := p.src[p.pos]
		if op != '+' && op != '-' {
			return left, nil
		}
		p.pos++
		right, err := p.parseTerm()
		if err != nil {
			return 0, err
		}
		if op == '+' {
			left += right
		} else {
			left -= right
		}
	}
}

func (p *arithParser) parseTerm() (float64, error) {
	left, err := p.parseFactor()
	if err != nil {
		return 0, err
	}
	for {
		p.skipSpaces()
		if p.pos >= len(p.src) {
			return left, nil
		}
		op := p.src[p.pos]
		if op != '*' && op != '/' {
			return left, nil
		}
		p.pos++
		right, err := p.parseFactor()
		if err != nil {
			return 0, err
		}
		if op == '*' {
			left *= right
		} else {
			if right == 0 {
				return 0, errors.New("division by zero")
			}
			left /= right
		}
	}
}

func (p *arithParser) parseFactor() (float64, error) {
	p.skipSpaces()
	if p.pos >= len(p.src) {
		return 0, errors.New("unexpected end of expression")
	}
	if p.src[p.pos] == '-' {
		p.pos++
		v, err := p.parseFactor()
		if err != nil {
			return 0, err
		}
		return -v, nil
	}
	if p.src[p.pos] == '+' {
		p.pos++
		return p.parseFactor()
	}
	if p.src[p.pos] == '(' {
		p.pos++
		v, err := p.parseExpression()
		if err != nil {
			return 0, err
		}
		p.skipSpaces()
		if p.pos >= len(p.src) || p.src[p.pos] != ')' {
			return 0, errors.New("missing closing paren")
		}
		p.pos++
		return v, nil
	}
	// number
	start := p.pos
	for p.pos < len(p.src) {
		ch := p.src[p.pos]
		if (ch >= '0' && ch <= '9') || ch == '.' {
			p.pos++
			continue
		}
		break
	}
	if start == p.pos {
		return 0, fmt.Errorf("expected number at position %d, got %q", p.pos, string(p.src[p.pos]))
	}
	v, err := strconv.ParseFloat(p.src[start:p.pos], 64)
	if err != nil {
		return 0, fmt.Errorf("parse number: %w", err)
	}
	return v, nil
}

// --- FileReadTool -----------------------------------------------------------

// FileReadTool reads files under an allow-list of root directories.
// Path-traversal attacks (..) are rejected by canonicalizing the
// requested path and verifying it stays under one of AllowedRoots.
type FileReadTool struct {
	// AllowedRoots is the list of directories under which file reads
	// are permitted. Any request for a path outside these roots is
	// refused. Each entry is canonicalized at construction time.
	AllowedRoots []string

	// MaxBytes caps the number of bytes returned per call. Default 64KB.
	// Larger files are truncated with a trailing notice.
	MaxBytes int
}

// NewFileReadTool returns a FileReadTool restricted to the given roots.
// Roots are canonicalized via filepath.Abs + filepath.EvalSymlinks; if
// either fails for a root, it is still kept verbatim (the safer error
// surfaces at Run time).
func NewFileReadTool(roots ...string) *FileReadTool {
	canonical := make([]string, 0, len(roots))
	for _, r := range roots {
		abs, err := filepath.Abs(r)
		if err != nil {
			abs = r
		}
		if resolved, err := filepath.EvalSymlinks(abs); err == nil {
			abs = resolved
		}
		canonical = append(canonical, abs)
	}
	return &FileReadTool{
		AllowedRoots: canonical,
		MaxBytes:     64 * 1024,
	}
}

func (f *FileReadTool) Name() string { return "file_read" }
func (f *FileReadTool) Description() string {
	return "Read a UTF-8 text file from one of the allowed directories. Returns the file contents, truncated if larger than 64KB."
}
func (f *FileReadTool) ArgsSchema() ToolArgsSchema {
	return ToolArgsSchema{
		Args: []ToolArg{
			{Name: "path", Type: "string", Description: "absolute or relative file path to read", Required: true},
		},
	}
}

func (f *FileReadTool) Run(_ context.Context, args map[string]any) (string, error) {
	requested, _ := args["path"].(string)
	if requested == "" {
		return "", errors.New("file_read: path is required")
	}

	// Canonicalize the requested path so ".." is resolved before checking.
	abs, err := filepath.Abs(requested)
	if err != nil {
		return "", fmt.Errorf("file_read: abs path: %w", err)
	}
	// Resolve symlinks so an attacker can't hide behind a symlinked
	// allowed-root entry pointing at /etc. EvalSymlinks fails for
	// non-existent paths; if it errors, fall back to the abs path so
	// the read-attempt's stat error surfaces normally.
	if resolved, err := filepath.EvalSymlinks(abs); err == nil {
		abs = resolved
	}

	// Verify the path is under an allowed root.
	allowed := false
	for _, root := range f.AllowedRoots {
		if strings.HasPrefix(abs, root+string(filepath.Separator)) || abs == root {
			allowed = true
			break
		}
	}
	if !allowed {
		return "", fmt.Errorf("file_read: path %q is outside allowed roots", requested)
	}

	file, err := os.Open(abs)
	if err != nil {
		return "", fmt.Errorf("file_read: open: %w", err)
	}
	defer file.Close()

	limit := f.MaxBytes
	if limit <= 0 {
		limit = 64 * 1024
	}
	// Read up to limit+1 so we can detect truncation.
	buf := make([]byte, limit+1)
	n, err := io.ReadFull(file, buf)
	if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) && !errors.Is(err, io.EOF) {
		return "", fmt.Errorf("file_read: read: %w", err)
	}
	truncated := false
	if n > limit {
		n = limit
		truncated = true
	}
	out := string(buf[:n])
	if truncated {
		out += fmt.Sprintf("\n\n[... file truncated; %d byte limit reached]", limit)
	}
	return out, nil
}
