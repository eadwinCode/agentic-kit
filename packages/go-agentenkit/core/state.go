package core

import (
	"context"
	"encoding/json"

	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

// The AgentRunState / StorageContext / BoundStorage types live in the ports
// package (Go needs them in a leaf package). This file keeps the behavior
// half of core/state.ts: giving every tool the run's state.

type runStateKey struct{}
type runIDKey struct{}
type publisherKey struct{}

// ContextWithRunID names the run a context serves. Execute stamps it on the
// model-call context, so a model wrapper (a cost recorder, a rate limiter)
// can tell which run it is working for.
func ContextWithRunID(ctx context.Context, runID string) context.Context {
	return context.WithValue(ctx, runIDKey{}, runID)
}

// RunIDFromContext reads the run a context serves: the dispatched run for
// the main agent, the nested run's own id inside a subagent. Empty outside
// a run.
func RunIDFromContext(ctx context.Context) string {
	id, _ := ctx.Value(runIDKey{}).(string)
	return id
}

type approvalKey struct{}
type toolCallIDKey struct{}

// Approval is what the human sent back with an approval (§2.5): answers to
// the questions the tool asked, a corrected value, a reason.
type Approval struct {
	Payload json.RawMessage
}

// ContextWithApproval marks a tool call as the resumption of an approved park.
func ContextWithApproval(ctx context.Context, a Approval) context.Context {
	return context.WithValue(ctx, approvalKey{}, a)
}

// ApprovalFromContext reads the approval a resumed tool was given, or nil on
// a first, live call.
func ApprovalFromContext(ctx context.Context) *Approval {
	if a, ok := ctx.Value(approvalKey{}).(Approval); ok {
		return &a
	}
	return nil
}

// ContextWithToolCallID names the call a resumed tool is answering; goai
// stamps its own id on live calls.
func ContextWithToolCallID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, toolCallIDKey{}, id)
}

func toolCallIDFromContext(ctx context.Context) string {
	id, _ := ctx.Value(toolCallIDKey{}).(string)
	return id
}

// ContextWithPublisher attaches a thread-bound EventPublisher to a context.
func ContextWithPublisher(ctx context.Context, p EventPublisher) context.Context {
	return context.WithValue(ctx, publisherKey{}, p)
}

// PublisherFromContext reads the EventPublisher a tool was given, or nil
// outside a run.
func PublisherFromContext(ctx context.Context) EventPublisher {
	p, _ := ctx.Value(publisherKey{}).(EventPublisher)
	return p
}

// ContextWithRunState attaches the run's state to a context.
func ContextWithRunState(ctx context.Context, state ports.AgentRunState) context.Context {
	if state == nil {
		state = ports.AgentRunState{}
	}
	return context.WithValue(ctx, runStateKey{}, state)
}

// RunStateFromContext reads the run's state inside a tool's Execute (§2.10).
// Present for every tool of every run, including a nested one and a segment
// resumed after an approval. Never nil.
func RunStateFromContext(ctx context.Context) ports.AgentRunState {
	if s, ok := ctx.Value(runStateKey{}).(ports.AgentRunState); ok && s != nil {
		return s
	}
	return ports.AgentRunState{}
}

// WithRunState gives every tool the run's state through its context (§2.10).
// goai calls Execute(ctx, input); this puts the state on that ctx so a tool
// reads it exactly where it already reads its tool-call id. Applied to the
// whole toolset, not just the marked ones: a tool that does not need approval
// still needs to know which tenant it is acting for.
func WithRunState(tools []ports.Tool, state ports.AgentRunState) []ports.Tool {
	out := make([]ports.Tool, 0, len(tools))
	for _, t := range tools {
		if t.Execute == nil {
			out = append(out, t)
			continue
		}
		inner := t.Execute
		wrapped := t
		wrapped.Execute = func(ctx context.Context, input json.RawMessage) (string, error) {
			return inner(ContextWithRunState(ctx, state), input)
		}
		out = append(out, wrapped)
	}
	return out
}
