package core

import (
	"context"
	"fmt"

	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

// Stop is the whole stop mechanism (§2.1): one button, one behavior. The
// engine's poller sees CANCELLED on the hot cache and cancels the run; the
// durable state is the recovery truth (§3.4).
func Stop(ctx context.Context, deps ports.RuntimePorts, threadID string) (ports.StopResult, error) {
	thread, err := deps.Storage.Threads.Get(ctx, threadID)
	if err != nil {
		return ports.StopResult{}, err
	}
	if thread == nil || (thread.State != ports.StateRunning && thread.State != ports.StateWaitingForInput) {
		state := "unknown"
		if thread != nil {
			state = string(thread.State)
		}
		return ports.StopResult{Accepted: false, Error: fmt.Sprintf("Cannot stop thread in state %s", state)}, nil
	}
	if _, err := deps.Kv.Set(ctx, StateKey(threadID), string(ports.StateCancelled), ports.SetOptions{}); err != nil {
		return ports.StopResult{}, err
	}
	if err := SetThreadState(ctx, deps, threadID, ports.StateCancelled, thread.Model); err != nil {
		return ports.StopResult{}, err
	}
	if _, err := Publish(ctx, deps, threadID, "STATE_CHANGE", map[string]any{"state": ports.StateCancelled}); err != nil {
		return ports.StopResult{}, err
	}
	if thread.State == ports.StateWaitingForInput {
		if err := closeOpenParks(ctx, deps, threadID); err != nil {
			return ports.StopResult{}, err
		}
	}
	return ports.StopResult{Accepted: true}, nil
}

// closeOpenParks answers every open approval with a cancellation, so the
// history the next run sends is well-formed (§2.5). A park persists the
// assistant's tool call and defers its result to the resume; a stop means
// that resume never comes, and a dangling call is a prompt no strict
// provider accepts. Nested parks close their whole chain: the child's call
// and every spawnSubagent call waiting on it (§2.7), whose run records end
// CANCELLED too.
func closeOpenParks(ctx context.Context, deps ports.RuntimePorts, threadID string) error {
	open, err := LoadOpenHitls(ctx, deps, threadID)
	if err != nil {
		return err
	}
	cancelled := ports.StateCancelled
	result := map[string]any{"cancelled": true, "reason": "stopped"}
	for _, p := range open {
		if _, err := deps.Storage.Messages.Append(ctx, threadID, ports.NewMessage{
			Role: ports.RoleTool, AgentID: p.AgentID, Content: ToolResultContent(p.ToolCallID, p.ToolName, result),
		}); err != nil {
			return err
		}
		if p.Nested != nil {
			_ = deps.Admin.Runs().Patch(ctx, p.Nested.AgentID, ports.RunPatch{State: &cancelled, StopReason: ports.Ptr("cancelled")})
		}
		for _, f := range p.Frames {
			if _, err := deps.Storage.Messages.Append(ctx, threadID, ports.NewMessage{
				Role: ports.RoleTool, AgentID: f.AgentID, Content: ToolResultContent(f.ToolCallID, "spawnSubagent", result),
			}); err != nil {
				return err
			}
			if f.Nested != nil {
				_ = deps.Admin.Runs().Patch(ctx, f.Nested.AgentID, ports.RunPatch{State: &cancelled, StopReason: ports.Ptr("cancelled")})
			}
		}
	}
	return nil
}
