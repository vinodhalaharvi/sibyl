// Package agent — tool_agent.go implements a ReAct-style agent loop.
//
// The agent receives a Task and access to a ToolRegistry. On each
// iteration:
//
//  1. Build a prompt: original task + history of (tool_call,
//     tool_result) pairs so far.
//  2. Ask the LLM what to do next.
//  3. Parse the response as JSON; expect either "final" (return the
//     answer) or "tool" (call a tool, append the result to history,
//     loop).
//  4. Cap iterations to MaxSteps to prevent runaway.
//
// The agent is implemented as a weft.Arrow so it composes naturally
// with DAGs, supervisors, or other arrows. The activity entry point
// (RunToolAgent) wraps it in the Temporal idiom.
package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"go.temporal.io/sdk/temporal"
)

// ToolAgentInput is the input to a tool-agent run.
type ToolAgentInput struct {
	// Task is the user's request in natural language.
	Task string
	// MaxSteps caps the number of LLM/tool iterations. Required (>0).
	// A reasonable default is 10; a low cap is a safety net against
	// runaway agents that keep calling tools.
	MaxSteps int
}

// ToolAgentOutput is what the agent returns.
type ToolAgentOutput struct {
	// Answer is the agent's final response. Empty if the agent failed
	// to converge before MaxSteps.
	Answer string
	// Steps records each step taken — the tool calls, results, and
	// the final answer. Useful for debugging, audit, and showing
	// the agent's reasoning in the Web UI.
	Steps []ToolAgentStep
	// Converged is true iff the agent emitted a "final" action before
	// hitting MaxSteps.
	Converged bool
}

// ToolAgentStep is one iteration of the agent loop.
type ToolAgentStep struct {
	// Reasoning is the LLM's chain-of-thought for this step (if any).
	Reasoning string
	// ToolName is the tool the LLM decided to call this step. Empty
	// if this step's Action was "final".
	ToolName string
	// ToolArgs is the arguments passed to the tool.
	ToolArgs map[string]any
	// ToolResult is the tool's output, or its error message stringified.
	ToolResult string
	// ToolError is true iff the tool returned an error (still recorded
	// in the conversation so the LLM can see and react).
	ToolError bool
	// Final is the final answer if this was the last step. Empty otherwise.
	Final string
}

// toolDecision is what we expect the LLM to emit, as JSON.
type toolDecision struct {
	Action    string         `json:"action"` // "tool" | "final"
	Reasoning string         `json:"reasoning,omitempty"`
	Tool      string         `json:"tool,omitempty"`
	Args      map[string]any `json:"args,omitempty"`
	Answer    string         `json:"answer,omitempty"`
}

// RunToolAgent is the Temporal activity entry point for the tool agent.
//
// Like Research/Critique, this lives on the Activities struct so the
// worker can register it with a shared CompleteFunc and tool registry.
// The ToolRegistry is supplied at construction time via Activities.Tools.
func (a *Activities) RunToolAgent(ctx context.Context, in ToolAgentInput) (ToolAgentOutput, error) {
	if a.Complete == nil {
		return ToolAgentOutput{}, temporal.NewNonRetryableApplicationError(
			"Activities.Complete is nil", "ConfigurationError", nil)
	}
	if a.Tools == nil {
		return ToolAgentOutput{}, temporal.NewNonRetryableApplicationError(
			"Activities.Tools is nil", "ConfigurationError", nil)
	}
	if in.Task == "" {
		return ToolAgentOutput{}, temporal.NewNonRetryableApplicationError(
			"Task must not be empty", "InvalidInput", nil)
	}
	if in.MaxSteps <= 0 {
		return ToolAgentOutput{}, temporal.NewNonRetryableApplicationError(
			"MaxSteps must be > 0", "InvalidInput", nil)
	}
	return runToolAgentLoop(ctx, a.Complete, a.Tools, in)
}

// runToolAgentLoop is the core loop, separated from the activity wrapper
// so it can be exercised directly in tests without the Temporal runtime.
func runToolAgentLoop(
	ctx context.Context,
	complete CompleteFunc,
	tools *ToolRegistry,
	in ToolAgentInput,
) (ToolAgentOutput, error) {
	out := ToolAgentOutput{}
	history := []ToolAgentStep{}

	systemPrompt := buildToolAgentSystemPrompt(tools)

	for step := 0; step < in.MaxSteps; step++ {
		userMsg := buildToolAgentUserMessage(in.Task, history)

		raw, err := complete(ctx, systemPrompt, userMsg)
		if err != nil {
			return out, fmt.Errorf("tool agent step %d: LLM call: %w", step+1, err)
		}

		var decision toolDecision
		if err := json.Unmarshal([]byte(trimResponse(raw)), &decision); err != nil {
			return out, temporal.NewNonRetryableApplicationError(
				fmt.Sprintf("tool agent step %d: malformed decision JSON: %v; raw: %q",
					step+1, err, raw),
				"InvalidLLMResponse", nil)
		}

		switch decision.Action {
		case "final":
			finalStep := ToolAgentStep{
				Reasoning: decision.Reasoning,
				Final:     decision.Answer,
			}
			history = append(history, finalStep)
			out.Answer = decision.Answer
			out.Steps = history
			out.Converged = true
			return out, nil

		case "tool":
			toolName := decision.Tool
			if toolName == "" {
				return out, temporal.NewNonRetryableApplicationError(
					fmt.Sprintf("tool agent step %d: action=tool but no tool name given", step+1),
					"InvalidLLMResponse", nil)
			}

			emitter := EmitterFromContext(ctx)
			emitter.Emit(NewToolCalled("", toolName, decision.Args, step+1))
			toolStart := time.Now()

			// OTel span around the tool dispatch — visible in traces with
			// duration and (on error) the failure message. We end the
			// span explicitly per iteration, not via defer, so it doesn't
			// accumulate spans across loop iterations.
			toolCtx, span := StartToolSpan(ctx, toolName, step+1)

			tool, ok := tools.Get(toolName)
			if !ok {
				msg := fmt.Sprintf("Error: tool %q is not registered. Available tools: %s.",
					toolName, strings.Join(tools.Names(), ", "))
				RecordError(span, errors.New(msg))
				span.End()
				emitter.Emit(NewToolCompleted("", toolName, step+1, "", errors.New(msg), time.Since(toolStart)))
				history = append(history, ToolAgentStep{
					Reasoning:  decision.Reasoning,
					ToolName:   toolName,
					ToolArgs:   decision.Args,
					ToolResult: msg,
					ToolError:  true,
				})
				continue
			}
			result, runErr := tool.Run(toolCtx, decision.Args)
			if runErr != nil {
				RecordError(span, runErr)
			}
			span.End()
			emitter.Emit(NewToolCompleted("", toolName, step+1, result, runErr, time.Since(toolStart)))

			step := ToolAgentStep{
				Reasoning: decision.Reasoning,
				ToolName:  toolName,
				ToolArgs:  decision.Args,
			}
			if runErr != nil {
				step.ToolError = true
				step.ToolResult = "Error: " + runErr.Error()
			} else {
				step.ToolResult = result
			}
			history = append(history, step)

		default:
			return out, temporal.NewNonRetryableApplicationError(
				fmt.Sprintf("tool agent step %d: unknown action %q", step+1, decision.Action),
				"InvalidLLMResponse", nil)
		}
	}

	// Hit MaxSteps without convergence. Return best-effort partial state.
	out.Steps = history
	out.Converged = false
	if len(history) > 0 {
		out.Answer = "(agent did not converge within MaxSteps; partial history available in Steps)"
	}
	return out, nil
}

func buildToolAgentSystemPrompt(tools *ToolRegistry) string {
	const preamble = `You are an agent that solves tasks by calling tools when useful and emitting a final answer when ready.

You communicate via a strict JSON protocol. On every turn, respond with a single JSON object and NOTHING ELSE:

To call a tool:
{"action": "tool", "reasoning": "<why this tool now>", "tool": "<tool_name>", "args": {<key>: <value>, ...}}

To give the final answer:
{"action": "final", "reasoning": "<brief>", "answer": "<your final answer>"}

Rules:
- Always emit valid JSON. Do not wrap in code fences or include any text outside the JSON object.
- "tool" must be the exact name of a registered tool. "args" must match its argument schema.
- Prefer calling a tool when the task requires current information, computation, or files.
- Stop calling tools and emit "final" as soon as you have enough information.
- If a tool call fails, you'll see the error in the next turn. Decide whether to retry, try a different tool, or give up and answer with what you have.

`
	return preamble + "\n" + tools.describeForPrompt()
}

func buildToolAgentUserMessage(task string, history []ToolAgentStep) string {
	var b strings.Builder
	fmt.Fprintf(&b, "Task: %s\n", task)
	if len(history) > 0 {
		b.WriteString("\nHistory so far:\n")
		for i, s := range history {
			fmt.Fprintf(&b, "\n--- Step %d ---\n", i+1)
			if s.Reasoning != "" {
				fmt.Fprintf(&b, "Reasoning: %s\n", s.Reasoning)
			}
			if s.ToolName != "" {
				argsJSON, _ := json.Marshal(s.ToolArgs)
				fmt.Fprintf(&b, "Called tool: %s with args: %s\n", s.ToolName, string(argsJSON))
				if s.ToolError {
					fmt.Fprintf(&b, "Result (ERROR): %s\n", s.ToolResult)
				} else {
					fmt.Fprintf(&b, "Result: %s\n", s.ToolResult)
				}
			}
			if s.Final != "" {
				fmt.Fprintf(&b, "Final answer: %s\n", s.Final)
			}
		}
		b.WriteString("\nDecide the next action.")
	}
	return b.String()
}

// ToolAgentArrow returns a weft.Arrow that wraps the tool-agent loop.
// Use it to embed a tool-using agent inside a DAG node or any other
// weft pipeline. The arrow has signature Arrow[ToolAgentInput, ToolAgentOutput].
//
// This is the composability story: the same loop runs as a Temporal
// activity (via Activities.RunToolAgent) OR as a node in a DAG, with
// identical semantics.
func ToolAgentArrow(complete CompleteFunc, tools *ToolRegistry) func(context.Context, ToolAgentInput) (ToolAgentOutput, error) {
	return func(ctx context.Context, in ToolAgentInput) (ToolAgentOutput, error) {
		if complete == nil {
			return ToolAgentOutput{}, errors.New("ToolAgentArrow: CompleteFunc is nil")
		}
		if tools == nil {
			return ToolAgentOutput{}, errors.New("ToolAgentArrow: ToolRegistry is nil")
		}
		return runToolAgentLoop(ctx, complete, tools, in)
	}
}
