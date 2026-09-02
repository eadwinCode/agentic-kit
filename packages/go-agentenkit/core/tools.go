package core

import (
	"context"
	"errors"

	"github.com/zendev-sh/goai"

	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

// ToolContext is what a tool's handler receives beside goai's own context:
// the run's state (§2.10) and a way to publish events on the thread.
type ToolContext struct {
	// State is whatever the caller attached to this run. Never nil.
	State ports.AgentRunState
	// ToolCallID identifies this call.
	ToolCallID string
	// PublishEvent publishes an event of your own on the thread the tool is
	// acting on: a progress label, a preview URL, anything a client should
	// react to. Durable by default, so a reconnecting client replays it;
	// PublishOptions{Notice: true} sends a bus-only notice. See the
	// "Custom events" guide.
	PublishEvent EventPublisher
	// Approval is present only when this call is the resumption of an
	// approved park (§2.5): whatever the human sent back with the approval.
	// Nil on a first, live call.
	Approval *Approval
}

var errNoPublisher = errors.New("agentenkit: PublishEvent is only available inside a run")

// ToolContextFrom rebuilds the ToolContext from a tool's context, for tools
// built with goai.NewTool by hand rather than AgentTool.
func ToolContextFrom(ctx context.Context) ToolContext {
	tc := ToolContext{
		State:        RunStateFromContext(ctx),
		ToolCallID:   goai.ToolCallIDFromContext(ctx),
		PublishEvent: PublisherFromContext(ctx),
		Approval:     ApprovalFromContext(ctx),
	}
	if tc.ToolCallID == "" {
		tc.ToolCallID = toolCallIDFromContext(ctx)
	}
	if tc.PublishEvent == nil {
		tc.PublishEvent = func(context.Context, string, any, PublishOptions) (ports.AgentEvent, error) {
			return ports.AgentEvent{}, errNoPublisher
		}
	}
	return tc
}

// AgentTool is goai.NewTool with the platform's ToolContext handed to the
// handler: the run state (§2.10) and PublishEvent.
//
//	lookupInvoice := agentenkit.AgentTool("lookupInvoice", "Find one invoice",
//		func(ctx context.Context, in struct{ InvoiceID string `json:"invoiceId"` }, tc agentenkit.ToolContext) (string, error) {
//			return db.FindInvoice(ctx, in.InvoiceID, tc.State["orgId"])
//		})
//
// The result is an ordinary platform Tool: it composes with
// MarkRequiresConfirmation and can be passed anywhere a Tool can.
func AgentTool[In any](name, description string, execute func(ctx context.Context, input In, tc ToolContext) (string, error)) ports.Tool {
	return ports.WrapTool(goai.NewTool(name, description, func(ctx context.Context, input In) (string, error) {
		return execute(ctx, input, ToolContextFrom(ctx))
	}))
}
