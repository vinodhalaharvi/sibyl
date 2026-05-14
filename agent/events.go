// Package agent — events.go defines the typed event hierarchy emitted
// by workflows, activities, and streaming LLM adapters.
//
// Why typed events?
//
//	Events are the wire format between the durable backend (workflows,
//	activities, tool calls, LLM streams) and any UI that wants to show
//	what's happening — most importantly a web UI consuming SSE.
//
//	Free-form map[string]any events are flexible but fragile: the UI
//	silently breaks when fields change, and there's no schema to
//	reason about. Typed events trade flexibility for discoverability
//	and compile-time safety on the producer side.
//
// Event flow:
//
//  1. A workflow/activity/streaming-adapter calls Emitter.Emit(event).
//  2. The Emitter forwards to the Broker registered for this WorkflowID.
//  3. Subscribers (typically SSE HTTP handlers) receive the event from
//     the broker and serialize it to JSON for the client.
//
// All events implement the Event interface. New event types should add
// a Kind constant and a struct that embeds eventBase or sets WorkflowID,
// Timestamp themselves.
package agent

import (
	"time"
)

// EventKind identifies an event's type for serialization and routing.
// Subscribers can filter by kind without knowing the concrete struct.
type EventKind string

const (
	// Workflow lifecycle
	EventKindWorkflowStarted   EventKind = "workflow.started"
	EventKindWorkflowCompleted EventKind = "workflow.completed"
	EventKindWorkflowFailed    EventKind = "workflow.failed"

	// DAG / supervisor node lifecycle
	EventKindNodeStarted   EventKind = "node.started"
	EventKindNodeCompleted EventKind = "node.completed"
	EventKindNodeFailed    EventKind = "node.failed"

	// Activity lifecycle (lower level than nodes; one node may span
	// multiple activities — Research + Critique inside a Converge)
	EventKindActivityStarted   EventKind = "activity.started"
	EventKindActivityCompleted EventKind = "activity.completed"

	// LLM streaming
	EventKindTokenChunk EventKind = "llm.token"

	// Tool agent
	EventKindToolCalled    EventKind = "tool.called"
	EventKindToolCompleted EventKind = "tool.completed"

	// Token accounting (per-call summary)
	EventKindTokenUsage EventKind = "llm.usage"
)

// Event is the interface all emitted events implement. The Kind/WorkflowID/
// Timestamp methods are used for routing and ordering; the concrete struct
// is JSON-serialized as the payload.
type Event interface {
	Kind() EventKind
	WorkflowID() string
	Timestamp() time.Time
}

// eventBase carries fields common to every event. Embed it so concrete
// event types only need to add their own fields.
type eventBase struct {
	WID string    `json:"workflow_id"`
	TS  time.Time `json:"timestamp"`
}

func (e eventBase) WorkflowID() string { return e.WID }
func (e eventBase) Timestamp() time.Time {
	if e.TS.IsZero() {
		return time.Now()
	}
	return e.TS
}

// --- Workflow lifecycle events ---------------------------------------------

// WorkflowStarted is emitted when a workflow begins executing.
type WorkflowStarted struct {
	eventBase
	WorkflowType string `json:"workflow_type"` // e.g. "ConvergeWorkflow", "SupervisorWorkflow"
	Input        any    `json:"input,omitempty"`
}

func (e WorkflowStarted) Kind() EventKind { return EventKindWorkflowStarted }

// NewWorkflowStarted is a convenience constructor that stamps WID and TS.
func NewWorkflowStarted(workflowID, workflowType string, input any) WorkflowStarted {
	return WorkflowStarted{
		eventBase:    eventBase{WID: workflowID, TS: time.Now()},
		WorkflowType: workflowType,
		Input:        input,
	}
}

// WorkflowCompleted is emitted when a workflow finishes successfully.
type WorkflowCompleted struct {
	eventBase
	Output     any   `json:"output,omitempty"`
	DurationMs int64 `json:"duration_ms"`
}

func (e WorkflowCompleted) Kind() EventKind { return EventKindWorkflowCompleted }

func NewWorkflowCompleted(workflowID string, output any, duration time.Duration) WorkflowCompleted {
	return WorkflowCompleted{
		eventBase:  eventBase{WID: workflowID, TS: time.Now()},
		Output:     output,
		DurationMs: duration.Milliseconds(),
	}
}

// WorkflowFailed is emitted when a workflow terminates with an error.
type WorkflowFailed struct {
	eventBase
	Error      string `json:"error"`
	DurationMs int64  `json:"duration_ms"`
}

func (e WorkflowFailed) Kind() EventKind { return EventKindWorkflowFailed }

func NewWorkflowFailed(workflowID string, err error, duration time.Duration) WorkflowFailed {
	msg := ""
	if err != nil {
		msg = err.Error()
	}
	return WorkflowFailed{
		eventBase:  eventBase{WID: workflowID, TS: time.Now()},
		Error:      msg,
		DurationMs: duration.Milliseconds(),
	}
}

// --- Node lifecycle events (DAG, supervisor children) ----------------------

// NodeStarted is emitted when a DAG node or supervisor child begins.
type NodeStarted struct {
	eventBase
	NodeID string `json:"node_id"`
	Label  string `json:"label,omitempty"` // human-readable label, e.g. "security_audit"
}

func (e NodeStarted) Kind() EventKind { return EventKindNodeStarted }

func NewNodeStarted(workflowID, nodeID, label string) NodeStarted {
	return NodeStarted{
		eventBase: eventBase{WID: workflowID, TS: time.Now()},
		NodeID:    nodeID,
		Label:     label,
	}
}

// NodeCompleted is emitted when a DAG node or supervisor child finishes
// successfully. Output is the node's typed output, serialized as JSON.
type NodeCompleted struct {
	eventBase
	NodeID     string `json:"node_id"`
	Label      string `json:"label,omitempty"`
	Output     any    `json:"output,omitempty"`
	DurationMs int64  `json:"duration_ms"`
}

func (e NodeCompleted) Kind() EventKind { return EventKindNodeCompleted }

func NewNodeCompleted(workflowID, nodeID, label string, output any, duration time.Duration) NodeCompleted {
	return NodeCompleted{
		eventBase:  eventBase{WID: workflowID, TS: time.Now()},
		NodeID:     nodeID,
		Label:      label,
		Output:     output,
		DurationMs: duration.Milliseconds(),
	}
}

// NodeFailed is emitted when a DAG node or supervisor child errors.
type NodeFailed struct {
	eventBase
	NodeID     string `json:"node_id"`
	Label      string `json:"label,omitempty"`
	Error      string `json:"error"`
	DurationMs int64  `json:"duration_ms"`
}

func (e NodeFailed) Kind() EventKind { return EventKindNodeFailed }

func NewNodeFailed(workflowID, nodeID, label string, err error, duration time.Duration) NodeFailed {
	msg := ""
	if err != nil {
		msg = err.Error()
	}
	return NodeFailed{
		eventBase:  eventBase{WID: workflowID, TS: time.Now()},
		NodeID:     nodeID,
		Label:      label,
		Error:      msg,
		DurationMs: duration.Milliseconds(),
	}
}

// --- Activity lifecycle events ---------------------------------------------

// ActivityStarted is emitted when an activity begins. Activities are
// lower-level than nodes — a single "Researcher" node spans one Research
// activity. Useful for debugging at finer granularity.
type ActivityStarted struct {
	eventBase
	ActivityName string `json:"activity_name"`
	NodeID       string `json:"node_id,omitempty"` // optional parent node
}

func (e ActivityStarted) Kind() EventKind { return EventKindActivityStarted }

func NewActivityStarted(workflowID, activityName, nodeID string) ActivityStarted {
	return ActivityStarted{
		eventBase:    eventBase{WID: workflowID, TS: time.Now()},
		ActivityName: activityName,
		NodeID:       nodeID,
	}
}

// ActivityCompleted is emitted when an activity finishes (success OR error;
// check the Error field).
type ActivityCompleted struct {
	eventBase
	ActivityName string `json:"activity_name"`
	NodeID       string `json:"node_id,omitempty"`
	Error        string `json:"error,omitempty"`
	DurationMs   int64  `json:"duration_ms"`
}

func (e ActivityCompleted) Kind() EventKind { return EventKindActivityCompleted }

func NewActivityCompleted(workflowID, activityName, nodeID string, err error, duration time.Duration) ActivityCompleted {
	msg := ""
	if err != nil {
		msg = err.Error()
	}
	return ActivityCompleted{
		eventBase:    eventBase{WID: workflowID, TS: time.Now()},
		ActivityName: activityName,
		NodeID:       nodeID,
		Error:        msg,
		DurationMs:   duration.Milliseconds(),
	}
}

// --- LLM streaming events --------------------------------------------------

// TokenChunkEvent carries one chunk of streamed LLM output. The chunk
// is whatever the upstream API delivers — could be a single token, a
// few tokens, or a whole sentence depending on backend buffering.
//
// Index is monotonic per (workflow_id, call_id) — the UI uses this to
// detect drops or reorder. CallID identifies a single LLM call within a
// workflow (multiple calls per workflow, e.g. Researcher + Critic).
//
// Final=true marks the last chunk of a call so the UI can close the
// streaming indicator without waiting for a separate completion event.
type TokenChunkEvent struct {
	eventBase
	CallID string `json:"call_id"`
	Index  int    `json:"index"`
	Text   string `json:"text"`
	Final  bool   `json:"final"`
}

func (e TokenChunkEvent) Kind() EventKind { return EventKindTokenChunk }

// --- Tool agent events -----------------------------------------------------

// ToolCalled is emitted when a tool-using agent dispatches a tool.
type ToolCalled struct {
	eventBase
	ToolName string         `json:"tool_name"`
	Args     map[string]any `json:"args,omitempty"`
	Step     int            `json:"step"`
}

func (e ToolCalled) Kind() EventKind { return EventKindToolCalled }

func NewToolCalled(workflowID, toolName string, args map[string]any, step int) ToolCalled {
	return ToolCalled{
		eventBase: eventBase{WID: workflowID, TS: time.Now()},
		ToolName:  toolName,
		Args:      args,
		Step:      step,
	}
}

// ToolCompleted is emitted after a tool returns (success or error).
type ToolCompleted struct {
	eventBase
	ToolName   string `json:"tool_name"`
	Step       int    `json:"step"`
	Result     string `json:"result,omitempty"`
	Error      string `json:"error,omitempty"`
	DurationMs int64  `json:"duration_ms"`
}

func (e ToolCompleted) Kind() EventKind { return EventKindToolCompleted }

func NewToolCompleted(workflowID, toolName string, step int, result string, err error, duration time.Duration) ToolCompleted {
	msg := ""
	if err != nil {
		msg = err.Error()
	}
	return ToolCompleted{
		eventBase:  eventBase{WID: workflowID, TS: time.Now()},
		ToolName:   toolName,
		Step:       step,
		Result:     result,
		Error:      msg,
		DurationMs: duration.Milliseconds(),
	}
}

// --- Token usage event (per-LLM-call summary) ------------------------------

// TokenUsageEvent reports approximate token usage for one completed
// LLM call. Emitted in addition to (not instead of) TokenChunkEvents
// during streaming, or as the sole "usage" event for non-streaming calls.
type TokenUsageEvent struct {
	eventBase
	CallID       string `json:"call_id,omitempty"`
	InputTokens  int    `json:"input_tokens"`
	OutputTokens int    `json:"output_tokens"`
}

func (e TokenUsageEvent) Kind() EventKind { return EventKindTokenUsage }
