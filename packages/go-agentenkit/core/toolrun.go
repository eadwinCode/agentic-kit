package core

import (
	"context"
	"encoding/json"
	"sync"

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
	// State is the run's state (§2.10), for adapters that pick per tenant.
	State ports.AgentRunState
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
		name := t.Name
		wrapped := t
		wrapped.Execute = func(ctx context.Context, input json.RawMessage) (string, error) {
			return callWithSandboxHook(ContextWithToolRun(ctx, run), run, name, func(ctx context.Context) (string, error) {
				return inner(ctx, input)
			})
		}
		out = append(out, wrapped)
	}
	return out
}

// sandboxUse is where a tool call notes the sandbox it used, for
// AfterSandboxCall.
type sandboxUse struct {
	mu      sync.Mutex
	sandbox ports.Sandbox
}

type sandboxUseKey struct{}

// noteSandboxUse records that the call in ctx used s. The last sandbox
// wins: a call that found its sandbox gone and got a fresh one saves to the
// fresh one.
func noteSandboxUse(ctx context.Context, s ports.Sandbox) {
	if use, ok := ctx.Value(sandboxUseKey{}).(*sandboxUse); ok {
		use.mu.Lock()
		use.sandbox = s
		use.mu.Unlock()
	}
}

// callWithSandboxHook runs one tool call and, when it used the sandbox and
// the app set AfterSandboxCall, calls the hook once after it. The hook's
// error becomes the call's, unless the call failed on its own.
func callWithSandboxHook(ctx context.Context, run ToolRun, name string, call func(context.Context) (string, error)) (string, error) {
	hook := run.Deps.Tools.AfterSandboxCall
	if hook == nil {
		return call(ctx)
	}
	use := &sandboxUse{}
	output, err := call(context.WithValue(ctx, sandboxUseKey{}, use))
	use.mu.Lock()
	s := use.sandbox
	use.mu.Unlock()
	if s == nil {
		return output, err
	}
	// A park is not a failure, and it is not undone by the hook either: the
	// call waits, whatever the hook said.
	_, parked := IsParked(output)
	if hookErr := hook(ctx, ports.SandboxCall{
		ThreadID: run.ThreadID, RunID: run.RunID, AgentID: run.AgentID,
		ToolName: name, ToolCallID: toolCallIDOr(ctx), Sandbox: s, Err: err,
	}); hookErr != nil && err == nil && !parked {
		return "", hookErr
	}
	return output, err
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
//
// Seconds, when given, is the sandbox time the call took, for a per-second
// price.
func ToolUseRow(tool, adapter string, uses int, seconds ...float64) ports.NewUsage {
	meta := map[string]any{"tool": tool, "adapter": adapter, "uses": uses}
	if len(seconds) > 0 {
		meta["seconds"] = seconds[0]
	}
	return ports.NewUsage{
		Kind:             ports.KindTool,
		Model:            ports.ToolUsePrefix + tool,
		ModelID:          adapter,
		Outcome:          ports.UsageFinished,
		ProviderMetadata: meta,
	}
}
