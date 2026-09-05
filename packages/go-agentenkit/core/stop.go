package core

import (
	"context"
	"fmt"
	"time"

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
	wasParked := thread.State == ports.StateWaitingForInput
	runID, err := CurrentRunID(ctx, deps, threadID)
	if err != nil {
		return ports.StopResult{}, err
	}
	endedAt := time.Now()
	if _, err := deps.Kv.Set(ctx, StateKey(threadID), string(ports.StateCancelled), ports.SetOptions{}); err != nil {
		return ports.StopResult{}, err
	}
	if err := SetThreadState(ctx, deps, threadID, ports.StateCancelled, thread.Model); err != nil {
		return ports.StopResult{}, err
	}
	if runID != "" {
		recordStoppedRun(ctx, deps, runID, endedAt)
	}
	if _, err := Publish(ctx, deps, threadID, "STATE_CHANGE", map[string]any{
		"state": ports.StateCancelled, "stopReason": "cancelled", "runId": runID, "endedAt": endedAt,
	}); err != nil {
		return ports.StopResult{}, err
	}
	if wasParked {
		if err := closeOpenParks(ctx, deps, threadID); err != nil {
			return ports.StopResult{}, err
		}
	}
	return ports.StopResult{Accepted: true}, nil
}

// A queued or parked run may never execute again. Close its record at stop
// without overwriting usage a running worker may still be accruing.
func recordStoppedRun(ctx context.Context, deps ports.RuntimePorts, runID string, endedAt time.Time) {
	prior, err := deps.Admin.Runs().Get(ctx, runID)
	if err != nil || prior == nil || prior.State == ports.StateCancelled {
		return
	}
	_ = deps.Admin.Runs().Patch(ctx, runID, ports.RunPatch{
		State: ports.Ptr(ports.StateCancelled), StopReason: ports.Ptr("cancelled"),
		EndedAt: &endedAt, DurationMs: ports.Ptr(endedAt.Sub(prior.StartedAt).Milliseconds()),
	})
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
	result := map[string]any{"cancelled": true, "reason": "stopped"}
	for _, p := range open {
		if _, err := deps.Storage.Messages.Append(ctx, threadID, ports.NewMessage{
			Role: ports.RoleTool, AgentID: p.AgentID, Content: ToolResultContent(p.ToolCallID, p.ToolName, result),
		}); err != nil {
			return err
		}
		if p.Nested != nil {
			recordStoppedRun(ctx, deps, p.Nested.AgentID, time.Now())
		}
		for _, f := range p.Frames {
			if _, err := deps.Storage.Messages.Append(ctx, threadID, ports.NewMessage{
				Role: ports.RoleTool, AgentID: f.AgentID, Content: ToolResultContent(f.ToolCallID, "spawnSubagent", result),
			}); err != nil {
				return err
			}
			if f.Nested != nil {
				recordStoppedRun(ctx, deps, f.Nested.AgentID, time.Now())
			}
		}
	}
	return nil
}
