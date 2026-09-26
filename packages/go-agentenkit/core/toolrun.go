package core

import (
	"context"
	"encoding/json"

	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

// ToolRun is what a tool call knows about the run it is part of, beyond its
// state: the run's own ports (its storage is bound to the run's state, so a
// tenant's usage rows land in the tenant's store), the thread, the run, and
// the ledger its spend is booked on so tool costs count against the run's
// caps.
type ToolRun struct {
	Deps     ports.RuntimePorts
	ThreadID string
	// RunID is the dispatched run: a nested run's calls are billed to its
	// parent.
	RunID string
	// AgentID is empty for the main agent, the nested run's id otherwise.
	AgentID   string
	AgentName string
	Ledger    *RunLedger
}

type toolRunKey struct{}

// ContextWithToolRun gives a tool call its ToolRun.
func ContextWithToolRun(ctx context.Context, run ToolRun) context.Context {
	return context.WithValue(ctx, toolRunKey{}, run)
}

// ToolRunFromContext is the ToolRun a call was given, if it runs inside an
// agentenkit run.
func ToolRunFromContext(ctx context.Context) (ToolRun, bool) {
	run, ok := ctx.Value(toolRunKey{}).(ToolRun)
	return run, ok
}

// WithToolRun gives every tool its ToolRun through its context, the way
// WithRunState gives it the state.
func WithToolRun(tools []ports.Tool, run ToolRun) []ports.Tool {
	out := make([]ports.Tool, 0, len(tools))
	for _, t := range tools {
		if t.Execute == nil {
			out = append(out, t)
			continue
		}
		inner := t.Execute
		wrapped := t
		wrapped.Execute = func(ctx context.Context, input json.RawMessage) (string, error) {
			return inner(ContextWithToolRun(ctx, run), input)
		}
		out = append(out, wrapped)
	}
	return out
}

// RecordToolUsage books one row of a tool's spend: on the run's ledger when
// there is one, so it counts against the run's caps, else stored on its own.
func RecordToolUsage(ctx context.Context, run ToolRun, u ports.NewUsage) ports.NewUsage {
	u.RunID = run.RunID
	u.AgentID = run.AgentID
	u.AgentName = run.AgentName
	if run.Ledger != nil {
		return run.Ledger.Record(ctx, run.Deps, run.ThreadID, u)
	}
	return RecordCall(ctx, run.Deps, run.ThreadID, u)
}

// ToolUseRow is a usage row for one use of a paid tool service: no tokens,
// the tool and the adapter named where a price table looks.
func ToolUseRow(tool, adapter string, uses int) ports.NewUsage {
	return ports.NewUsage{
		Kind:             ports.KindTool,
		Model:            "tool:" + tool,
		ModelID:          adapter,
		Outcome:          ports.UsageFinished,
		ProviderMetadata: map[string]any{"tool": tool, "adapter": adapter, "uses": uses},
	}
}
