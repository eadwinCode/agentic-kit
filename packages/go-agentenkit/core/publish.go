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

// Publish persists to the replayable event log, then fans out live to all
// subscribers (§2.2). Seq comes from Kv.Incr, monotonic per thread (§3.4).
func Publish(ctx context.Context, deps ports.RuntimePorts, threadID, typ string, payload any) (ports.AgentEvent, error) {
	seq, err := deps.Kv.Incr(ctx, SeqKey(threadID))
	if err != nil {
		return ports.AgentEvent{}, err
	}
	event := ports.AgentEvent{
		ThreadID: threadID, Seq: seq, Type: typ,
		Payload: MarshalPayload(payload), CreatedAt: time.Now(),
	}
	if err := deps.Storage.Events.Append(ctx, threadID, event); err != nil {
		return event, err
	}
	return event, deps.Bus.Publish(ctx, threadID, event)
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
	return event, deps.Bus.Publish(ctx, threadID, event)
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
	"RUN_REFUSED": true, "TOKEN_BUDGET_EXHAUSTED": true,
}

// PublishOptions tunes PublishEvent.
type PublishOptions struct {
	// Notice sends the event over the bus only (seq 0), never to the log: a
	// progress tick, a typing indicator, anything nobody needs to see twice.
	// The default writes it to the thread's log, so a reconnecting client
	// replays it.
	Notice bool
}

// PublishEvent publishes an event of your own on a thread, through the same
// pipeline the platform's events take: the durable log and the live bus
// (§2.2). A client sees it in its event stream exactly like a built-in one.
// Platform event types are refused.
func PublishEvent(ctx context.Context, deps ports.RuntimePorts, threadID, typ string, payload any, opts PublishOptions) (ports.AgentEvent, error) {
	if typ == "" {
		return ports.AgentEvent{}, errors.New("PublishEvent: an event type is required")
	}
	if ReservedEventTypes[typ] {
		return ports.AgentEvent{}, fmt.Errorf("PublishEvent: %s is a platform event type; pick your own", typ)
	}
	if opts.Notice {
		return publishNotice(ctx, deps, threadID, typ, payload)
	}
	return Publish(ctx, deps, threadID, typ, payload)
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
	return nil
}
