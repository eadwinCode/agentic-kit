package core

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

// MarshalPayload encodes an event payload. A nil payload becomes JSON null.
func MarshalPayload(payload any) json.RawMessage {
	if raw, ok := payload.(json.RawMessage); ok {
		if len(raw) == 0 {
			return json.RawMessage("null")
		}
		return raw
	}
	b, err := json.Marshal(payload)
	if err != nil {
		return json.RawMessage(`{"error":"unencodable payload"}`)
	}
	return b
}

// RecordEventTypes are the platform's types that go in the thread record:
// what must outlive a run (see ports.EventStore). Every other platform type
// is live only: a notice on the bus, and an event on the run stream when
// one is open. The TS runtime has the same set.
var RecordEventTypes = map[string]bool{
	"INPUT_REQUIRED": true, "INPUT_EXPIRED": true, "HITL_RESPONSE": true,
	"RUN_REFUSED": true, "TOKEN_BUDGET_EXHAUSTED": true, "COST_BUDGET_EXHAUSTED": true,
	"CONTEXT_COMPACTED": true, "MESSAGES_DROPPED": true,
	"RUN_STARTED": true, "RUN_ENDED": true,
}

// runOf is the run an entry belongs to: the one its payload names, as
// runId or as resume.runId.
func runOf(payload json.RawMessage) string {
	var p struct {
		RunID  string `json:"runId"`
		Resume *struct {
			RunID string `json:"runId"`
		} `json:"resume"`
	}
	_ = json.Unmarshal(payload, &p)
	if p.RunID != "" {
		return p.RunID
	}
	if p.Resume != nil {
		return p.Resume.RunID
	}
	return ""
}

// Publish publishes a platform event (§2.2). A record type is stored first
// (the store mints its seq) and then sent; any other type is sent as a
// notice. Either way it reaches the run stream this process has open on the
// thread.
func Publish(ctx context.Context, deps ports.RuntimePorts, threadID, typ string, payload any) (ports.AgentEvent, error) {
	return PublishFor(ctx, deps, threadID, "", typ, payload)
}

// PublishFor is Publish for an entry that belongs to runID; empty takes the
// run the payload names.
func PublishFor(ctx context.Context, deps ports.RuntimePorts, threadID, runID, typ string, payload any) (ports.AgentEvent, error) {
	if !RecordEventTypes[typ] {
		return publishNotice(ctx, deps, threadID, typ, payload)
	}
	return record(ctx, deps, threadID, runID, typ, MarshalPayload(payload))
}

// record stores an entry in the thread record, then sends it.
func record(ctx context.Context, deps ports.RuntimePorts, threadID, runID, typ string, payload json.RawMessage) (ports.AgentEvent, error) {
	if runID == "" {
		runID = runOf(payload)
	}
	event, err := deps.Storage.Events.Append(ctx, threadID, ports.NewThreadEvent{Type: typ, Payload: payload, RunID: runID})
	if err != nil {
		return event, err
	}
	err = deps.Bus.Publish(ctx, threadID, event)
	// A platform entry the stream shows too (a park). An app's durable event
	// is delivered as this entry alone, so a tab never sees it twice.
	if ReservedEventTypes[typ] {
		toSegment(ctx, deps, event)
	}
	return event, err
}

// streamContent is what a run stream carries in place of the bus while a
// segment is open: a tab reads these from the stream, so the bus sending
// them too would show them twice.
var streamContent = map[string]bool{
	"CHUNK": true, "SUBAGENT_CHUNK": true, "TEXT_RESULT": true, "STEP_COMMITTED": true,
	"SUBAGENT_STARTED": true, "SUBAGENT_COMPLETED": true, "SUBAGENT_FAILED": true,
}

// toSegment hands an event to the run stream this process has open on the
// thread, if any; the segment keeps what belongs in a stream (see
// SegmentStream).
func toSegment(ctx context.Context, deps ports.RuntimePorts, event ports.AgentEvent) {
	if seg := ActiveSegment(deps, event.ThreadID); seg != nil {
		seg.Forward(ctx, event.Type, event.Payload, ReservedEventTypes[event.Type])
	}
}

// PublishNotice publishes a bus-only notice (never persisted), e.g. HITL
// death notices (§2.5).
func PublishNotice(ctx context.Context, deps ports.RuntimePorts, threadID, typ string, payload any) error {
	_, err := publishNotice(ctx, deps, threadID, typ, payload)
	return err
}

func publishNotice(ctx context.Context, deps ports.RuntimePorts, threadID, typ string, payload any) (ports.AgentEvent, error) {
	event := ports.AgentEvent{
		ThreadID: threadID, Seq: 0, Type: typ,
		Payload: MarshalPayload(payload), CreatedAt: time.Now(),
	}
	// While this process has a segment open on the thread, stream content
	// and an app's own events go to the run stream alone; everything else
	// goes on the bus, and to the stream when it has a shape for it.
	seg := ActiveSegment(deps, threadID)
	custom := !ReservedEventTypes[typ]
	if seg != nil && (custom || streamContent[typ]) {
		seg.Forward(ctx, typ, event.Payload, !custom)
		return event, nil
	}
	err := deps.Bus.Publish(ctx, threadID, event)
	if seg != nil {
		seg.Forward(ctx, typ, event.Payload, true)
	}
	return event, err
}

// ReservedEventTypes are the event types the platform itself emits. An app
// cannot publish these: a client's reducer trusts them to mean what the
// engine meant.
var ReservedEventTypes = map[string]bool{
	"CHUNK": true, "STATE_CHANGE": true, "STEP_COMMITTED": true, "STEP_FINISHED": true,
	"INPUT_REQUIRED": true, "INPUT_EXPIRED": true, "HITL_RESPONSE": true,
	"MESSAGE_APPENDED": true, "MESSAGES_DROPPED": true, "CONTEXT_COMPACTED": true,
	"SUBAGENT_STARTED": true, "SUBAGENT_CHUNK": true, "SUBAGENT_COMPLETED": true, "SUBAGENT_FAILED": true,
	"TEXT_RESULT": true, "THREAD_DELETED": true, "HEARTBEAT": true,
	"RUN_REFUSED": true, "TOKEN_BUDGET_EXHAUSTED": true, "COST_BUDGET_EXHAUSTED": true,
	"RUN_STARTED": true, "RUN_ENDED": true, "RECORD_CHANGED": true, "SNAPSHOT": true,
}

// PublishOptions tunes PublishEvent.
type PublishOptions struct {
	// Durable also keeps the event in the thread record, so a tab that
	// opens the thread next week still sees it. Otherwise (the default) it
	// is live only: it goes out on the bus and, during a run, on the run's
	// stream, which a tab that reconnects within the grace window replays.
	Durable bool
}

// PublishEvent publishes an event of your own on a thread, through the same
// pipeline the platform's events take (§2.2): the live bus, the run stream
// during a run (as CUSTOM), and the thread record when it is durable. A
// client sees it in its event stream exactly like a built-in one. Platform
// event types are refused.
func PublishEvent(ctx context.Context, deps ports.RuntimePorts, threadID, typ string, payload any, opts PublishOptions) (ports.AgentEvent, error) {
	if typ == "" {
		return ports.AgentEvent{}, errors.New("PublishEvent: an event type is required")
	}
	if ReservedEventTypes[typ] {
		return ports.AgentEvent{}, fmt.Errorf("PublishEvent: %s is a platform event type; pick your own", typ)
	}
	if opts.Durable {
		return record(ctx, deps, threadID, "", typ, MarshalPayload(payload))
	}
	return publishNotice(ctx, deps, threadID, typ, payload)
}

// EventPublisher is what a tool calls to publish, already bound to the
// thread the tool is acting on.
type EventPublisher func(ctx context.Context, typ string, payload any, opts PublishOptions) (ports.AgentEvent, error)

// ThreadPublisher binds PublishEvent to one thread.
func ThreadPublisher(deps ports.RuntimePorts, threadID string) EventPublisher {
	return func(ctx context.Context, typ string, payload any, opts PublishOptions) (ports.AgentEvent, error) {
		// The publish is durable state, not part of the model call: it must
		// land even while a stopped generation is being torn down.
		return PublishEvent(context.WithoutCancel(ctx), deps, threadID, typ, payload, opts)
	}
}

// WithPublishEvent gives every tool an EventPublisher through its context,
// bound to the thread it runs on: main agent, nested run, or a segment
// resumed after an approval alike. Read it with ToolContextFrom.
func WithPublishEvent(deps ports.RuntimePorts, threadID string, tools []ports.Tool) []ports.Tool {
	publisher := ThreadPublisher(deps, threadID)
	out := make([]ports.Tool, 0, len(tools))
	for _, t := range tools {
		if t.Execute == nil {
			out = append(out, t)
			continue
		}
		inner := t.Execute
		wrapped := t
		wrapped.Execute = func(ctx context.Context, input json.RawMessage) (string, error) {
			return inner(ContextWithPublisher(ctx, publisher), input)
		}
		out = append(out, wrapped)
	}
	return out
}

// StateChange is one run state change (§3.4): see Transition.
type StateChange struct {
	From []ports.ExecutionState
	To   ports.ExecutionState
	// RunID is the run the caller acts for; the change lands only while the
	// thread still belongs to it. Empty means any run.
	RunID string
	// NewRunID makes the thread belong to a new run (run admission).
	NewRunID string
	// Model is the thread's model, for the admin view; looked up when empty.
	Model string
}

// Transition is the one way a run moves its thread's state (§3.4). It is a
// compare-and-set on the durable row, on the state AND the run that owns the
// thread, and only the caller that wins it writes the hot cache and the
// admin view. So a stop is never overwritten by a finish, and a stopped or
// replaced run can never move the thread again: its change simply loses.
// Returns whether this caller made the change. Publishing the STATE_CHANGE
// is left to the caller, whose payload differs per change.
func Transition(ctx context.Context, deps ports.RuntimePorts, threadID string, c StateChange) (bool, error) {
	won, err := deps.Storage.Threads.Transition(ctx, threadID, ports.ThreadTransition{
		From: c.From, To: c.To, RunID: c.RunID, NewRunID: c.NewRunID,
	})
	if err != nil || !won {
		return false, err
	}
	if _, err := deps.Kv.Set(ctx, StateKey(threadID), string(c.To), ports.SetOptions{Expiry: ThreadKeyTTL}); err != nil {
		return true, err
	}
	upsertAdminThread(ctx, deps, threadID, c.To, c.Model)
	return true, nil
}

// SetThreadState moves a thread to a new state on BOTH the caller's storage
// and the platform's own operational view (§2.9).
//
// One choke point on purpose: the admin thread table is what lets a
// dashboard answer "what is running right now" without reading the caller's
// database, and it is only true if every transition passes through here.
// Model is looked up when empty, so callers that already hold the thread can
// skip a read.
func SetThreadState(ctx context.Context, deps ports.RuntimePorts, threadID string, state ports.ExecutionState, model string) error {
	if err := deps.Storage.Threads.SetState(ctx, threadID, state); err != nil {
		return err
	}
	upsertAdminThread(ctx, deps, threadID, state, model)
	return nil
}

func upsertAdminThread(ctx context.Context, deps ports.RuntimePorts, threadID string, state ports.ExecutionState, model string) {
	resolved := model
	if resolved == "" {
		if t, err := deps.Storage.Threads.Get(ctx, threadID); err == nil && t != nil {
			resolved = t.Model
		}
	}
	if resolved == "" {
		resolved = "unknown"
	}
	// Observability must never be able to fail a transition that succeeded.
	_ = deps.Admin.Threads().Upsert(ctx, ports.NewAdminThread{ID: threadID, State: state, Model: resolved})
}
