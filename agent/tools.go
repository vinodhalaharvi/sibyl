// Package agent — tools.go defines the Tool interface and a Registry
// for managing available tools at activity time.
//
// Tools are the "act" side of a ReAct (Reason + Act) agent loop:
//
//  1. The LLM is told what tools exist (name + description + schema).
//  2. It emits a JSON decision: "use tool X with args Y" or "I'm done".
//  3. The agent dispatches the tool, captures the result, appends it
//     to the conversation history, and loops.
//
// The Tool interface is deliberately small: a name, a description, an
// argument schema (for the LLM's benefit), and a Run method. Implement
// it once per tool; register it on a Registry that the ToolAgent consults.
package agent

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
)

// Tool is a single capability the agent can invoke. Implementations
// MUST be safe for concurrent use — a single Tool may be invoked from
// multiple agent loops at the same time.
type Tool interface {
	// Name is the identifier the LLM uses to refer to this tool.
	// Conventionally lowercase, snake_case: "web_search", "calculator".
	Name() string

	// Description is shown to the LLM as part of the system prompt.
	// Keep it short and concrete; the LLM uses this to decide WHEN to
	// reach for the tool. Examples and constraints belong here.
	Description() string

	// ArgsSchema returns a JSON-Schema-ish description of accepted
	// arguments. The agent serializes this into the system prompt;
	// the LLM is expected to emit matching args. Keep the schema flat
	// and obvious — deeply nested schemas tend to confuse models.
	ArgsSchema() ToolArgsSchema

	// Run executes the tool with the given arguments. Returns a string
	// suitable for inclusion in the agent's conversation history.
	// Errors should be human-readable; the LLM may see them and decide
	// to retry or abandon.
	Run(ctx context.Context, args map[string]any) (string, error)
}

// ToolArgsSchema describes a tool's accepted arguments. Each entry is
// one named argument. The "Type" string is informational — we don't
// validate strict types in the framework, since LLMs sometimes coerce
// (sending "5" instead of 5 for an integer). Tools should be lenient
// in what they accept.
type ToolArgsSchema struct {
	Args []ToolArg
}

// ToolArg describes a single tool argument.
type ToolArg struct {
	Name        string
	Type        string // "string" | "number" | "boolean" | etc — informational
	Description string
	Required    bool
}

// ToolRegistry holds the tools available to a ToolAgent. Safe for
// concurrent reads after registration is complete; registration itself
// is typically done at worker startup before any agent loops run.
type ToolRegistry struct {
	mu    sync.RWMutex
	tools map[string]Tool
}

// NewToolRegistry returns an empty Registry.
func NewToolRegistry() *ToolRegistry {
	return &ToolRegistry{tools: make(map[string]Tool)}
}

// Register adds a tool. Returns an error if a tool with the same name
// is already registered (we treat that as a configuration bug).
func (r *ToolRegistry) Register(t Tool) error {
	if t == nil {
		return errors.New("ToolRegistry: nil tool")
	}
	name := t.Name()
	if name == "" {
		return errors.New("ToolRegistry: tool with empty name")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.tools[name]; exists {
		return fmt.Errorf("ToolRegistry: tool %q already registered", name)
	}
	r.tools[name] = t
	return nil
}

// MustRegister is Register that panics on error. Useful at init time.
func (r *ToolRegistry) MustRegister(t Tool) {
	if err := r.Register(t); err != nil {
		panic(err)
	}
}

// Get returns the tool named name and whether it was found.
func (r *ToolRegistry) Get(name string) (Tool, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	t, ok := r.tools[name]
	return t, ok
}

// Names returns the registered tool names in sorted order. Useful for
// rendering the system prompt deterministically.
func (r *ToolRegistry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, 0, len(r.tools))
	for name := range r.tools {
		names = append(names, name)
	}
	// Sort for deterministic prompt rendering.
	sortStrings(names)
	return names
}

// describeForPrompt renders all registered tools into a section of the
// system prompt the LLM sees. The format is a minimal Markdown-ish
// listing that fits in any model's context cheaply.
func (r *ToolRegistry) describeForPrompt() string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.tools) == 0 {
		return "No tools available."
	}

	var b strings.Builder
	b.WriteString("Available tools:\n\n")

	names := make([]string, 0, len(r.tools))
	for name := range r.tools {
		names = append(names, name)
	}
	sortStrings(names)

	for _, name := range names {
		t := r.tools[name]
		fmt.Fprintf(&b, "- **%s**: %s\n", name, t.Description())
		schema := t.ArgsSchema()
		if len(schema.Args) == 0 {
			b.WriteString("  Arguments: none.\n")
			continue
		}
		b.WriteString("  Arguments:\n")
		for _, a := range schema.Args {
			req := ""
			if a.Required {
				req = " (required)"
			}
			fmt.Fprintf(&b, "    - %s [%s]%s — %s\n", a.Name, a.Type, req, a.Description)
		}
	}
	return b.String()
}

// sortStrings is a tiny in-place lexical sort. We avoid the stdlib sort
// import here to keep the file's import surface minimal — strings are
// already fully imported.
func sortStrings(s []string) {
	// insertion sort; fine for small N (tool counts are tiny in practice)
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j-1] > s[j]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}
