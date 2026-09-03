package core

import (
	"context"
	"time"

	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

// ReclaimGraceAfter is the HITL TTL plus a small grace, so an in-flight
// answer always lands first and reclamation only ever sees true orphans.
func ReclaimGraceAfter(deps ports.RuntimePorts) time.Duration {
	return deps.Config.HITLTTL + deps.Config.ReclaimGrace
}

// ReclaimIfOrphaned is §2.5 orphan reclamation, the FALLBACK path.
//
// A park schedules its own expiry on the queue (see ParkForApproval), so the
// deadline holds whether or not anyone is watching. This covers what a timer
// cannot: threads parked before that existed, and queue adapters that drop a
// delay. Callers are first-touch checks in Run and Respond.
//
// It does not heal inline. It re-dispatches the run and lets the engine
// resolve the park, so there is exactly ONE definition of what an expired
// approval becomes, and a thread holding several open approvals (§2.7) is
// resolved as a set. Re-dispatching the same run twice is safe: the run lock
// and the engine's readiness check make the duplicate a no-op (§2.8).
//
// Returns true iff a re-dispatch was enqueued.
func ReclaimIfOrphaned(ctx context.Context, deps ports.RuntimePorts, threadID string) (bool, error) {
	thread, err := deps.Storage.Threads.Get(ctx, threadID)
	if err != nil {
		return false, err
	}
	if thread == nil || thread.State != ports.StateWaitingForInput {
		return false, nil
	}
	open, err := LoadOpenHitls(ctx, deps, threadID)
	if err != nil {
		return false, err
	}
	if len(open) == 0 {
		return false, nil
	}
	// Every open request must be past its window. One that is still
	// answerable would make the resumed segment a no-op anyway (§2.7).
	for _, p := range open {
		if time.Now().Before(p.Deadline(deps.Config).Add(deps.Config.ReclaimGrace)) {
			return false, nil
		}
	}
	// Rebuild the original dispatch from the ticket persisted in the event
	// payload; a legacy park without one falls back to the default handle.
	// Resuming a parked run REUSES its id (§2.1).
	runID, err := CurrentRunID(ctx, deps, threadID)
	if err != nil {
		return false, err
	}
	job := ports.RunJob{ThreadID: threadID, RunID: runID, Model: thread.Model}
	if r := open[len(open)-1].Resume; r != nil {
		job.Model = r.Model
		job.Agent = r.Agent
		job.TokenBudget = r.TokenBudget
		job.ProviderOptions = r.ProviderOptions
		job.State = r.State
		job.MaxSteps = r.MaxSteps
	}
	if err := deps.Queue.Enqueue(ctx, job, nil); err != nil {
		return false, err
	}
	return true, nil
}
