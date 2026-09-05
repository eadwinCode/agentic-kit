package core

import (
	"context"
	"fmt"
	"time"

	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

// StopOptions tunes StopRun.
type StopOptions struct {
	// Agent resolves the registered agent a run belongs to, by the name on
	// its record. A stop that ends a run no worker holds (queued, or parked
	// on an approval) runs that agent's OnSettle itself (§5.6); with no
	// resolver the run is left unsettled.
	Agent func(name string) *RegisteredAgent
}

// Stop is the whole stop mechanism (§2.1): one button, one behavior. The
// engine's poller sees CANCELLED on the hot cache and cancels the run; the
// durable state is the recovery truth (§3.4).
//
// It knows no agents, so a run it ends without a worker is not settled; a
// handle's Stop, or StopRun with a resolver, is the one to call for that.
func Stop(ctx context.Context, deps ports.RuntimePorts, threadID string) (ports.StopResult, error) {
	return StopRun(ctx, deps, threadID, StopOptions{})
}

// StopRun is Stop with the agent resolver a settle needs.
func StopRun(ctx context.Context, deps ports.RuntimePorts, threadID string, opts StopOptions) (ports.StopResult, error) {
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
		if opts.Agent != nil {
			settleAfterStop(ctx, deps, opts.Agent, threadID, runID)
		}
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

// settleAfterStop settles a stopped run when no worker is there to (§5.6).
// A queued run never reached a worker; a parked run's worker is long gone.
// Their steps were priced as they happened, so the bill is real, and the
// spec's OnSettle is where it gets charged.
//
// It takes the run lock and settles under it, exactly as a worker would. A
// held lock means a worker owns the run right now, and that worker settles
// it: from its own cancel path, or when it finds the thread cancelled at
// its segment start. Either way the run record's settle mark makes the
// second arrival a no-op.
func settleAfterStop(ctx context.Context, deps ports.RuntimePorts, lookup func(string) *RegisteredAgent, threadID, runID string) {
	rec, err := deps.Admin.Runs().Get(ctx, runID)
	if err != nil || rec == nil || rec.SettledAt != nil {
		return
	}
	agent := lookup(rec.Agent)
	if agent == nil {
		return
	}
	locked, err := deps.Kv.Set(ctx, RunLockKey(threadID), runID, ports.SetOptions{
		OnlyIfNotExists: true, Expiry: deps.Config.RunLockLease,
	})
	if err != nil || !locked {
		return // a worker holds the run; the settle is its to run
	}
	defer func() { _ = deps.Kv.Del(context.WithoutCancel(ctx), RunLockKey(threadID)) }()
	settleStoppedRun(ctx, deps, agent, threadID, runID)
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
