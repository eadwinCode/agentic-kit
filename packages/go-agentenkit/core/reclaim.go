package core

import (
	"context"
	"errors"
	"time"

	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

// ReclaimGraceAfter is the HITL TTL plus a small grace, so an in-flight
// answer always lands first and reclamation only ever sees true orphans.
func ReclaimGraceAfter(deps ports.RuntimePorts) time.Duration {
	return deps.Config.HITLTTL + deps.Config.ReclaimGrace
}

// reclaimJobKey names a thread's reclaim row on the queue, so two callers
// noticing the same orphan at once enqueue one job.
func reclaimJobKey(runID string) string { return "reclaim:" + runID }

// ReclaimIfOrphaned is §2.5 orphan reclamation, the FALLBACK path.
//
// A park schedules its own expiry on the queue (see ParkForApproval), so the
// deadline holds whether or not anyone is watching. This covers what a timer
// cannot: threads parked before that existed, and queue adapters that drop a
// delay. Callers are first-touch checks in Run and Respond, and the
// stuck-run sweep.
//
// It also covers a run the queue lost: a thread QUEUED or RUNNING whose run
// lock nobody holds and whose job the queue no longer has. A worker that
// died leaves exactly that behind once its lock lapses, and so does a job
// dropped as dead. The run is re-dispatched and resumes from its last
// persisted step, unless its record already ended, in which case the thread
// is moved to match the record.
//
// It does not heal inline. It re-dispatches the run and lets the engine
// resolve the park, so there is exactly ONE definition of what an expired
// approval becomes, and a thread holding several open approvals (§2.7) is
// resolved as a set. Re-dispatching the same run twice is safe: the run lock
// and the engine's readiness check make the duplicate a no-op (§2.8).
//
// Returns true iff a re-dispatch was enqueued or the thread was healed.
func ReclaimIfOrphaned(ctx context.Context, deps ports.RuntimePorts, threadID string) (bool, error) {
	thread, err := deps.Storage.Threads.Get(ctx, threadID)
	if err != nil {
		return false, err
	}
	if thread == nil {
		return false, nil
	}
	switch thread.State {
	case ports.StateWaitingForInput:
		return reclaimParked(ctx, deps, thread)
	case ports.StateQueued, ports.StateRunning:
		return reclaimLost(ctx, deps, thread)
	default:
		return false, nil
	}
}

// reclaimParked re-dispatches a parked thread whose approvals all expired.
func reclaimParked(ctx context.Context, deps ports.RuntimePorts, thread *ports.ThreadDTO) (bool, error) {
	threadID := thread.ID
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
	job := ports.RunJob{ThreadID: threadID, RunID: runID, Model: thread.Model, Kind: ports.JobReclaim, EnqueuedAt: time.Now().UnixMilli()}
	if r := open[len(open)-1].Resume; r != nil {
		job.Model = r.Model
		job.Agent = r.Agent
		job.DispatchedAt = r.DispatchedAt
		job.TokenBudget = r.TokenBudget
		job.CostBudgetMicros = r.CostBudgetMicros
		job.ProviderOptions = r.ProviderOptions
		job.State = r.State
		job.MaxSteps = r.MaxSteps
	}
	return enqueueReclaim(ctx, deps, job)
}

// reclaimLost re-dispatches a QUEUED or RUNNING thread that nothing is
// working on: no run lock, no job on the queue, and a run record that has
// sat untouched for longer than a lock lease. With a queue that cannot look
// a job up, the run must also have been quiet for longer than any of its
// jobs could wait.
func reclaimLost(ctx context.Context, deps ports.RuntimePorts, thread *ports.ThreadDTO) (bool, error) {
	threadID := thread.ID
	runID, err := CurrentRunID(ctx, deps, threadID)
	if err != nil || runID == "" {
		return false, err
	}
	if _, held, err := deps.Kv.Get(ctx, RunLockKey(threadID)); err != nil {
		return false, err
	} else if held {
		return false, nil // a live worker: the lock is renewed while one runs (§3.4)
	}
	// A queue that cannot look a job up (QStash) cannot say whether it still
	// holds this one. Then only time can: see the wait below.
	queueCannotSay := false
	job, err := deps.Queue.Find(ctx, runID)
	switch {
	case errors.Is(err, ports.ErrUnsupported):
		queueCannotSay = true
	case err != nil:
		return false, err
	case job != nil:
		return false, nil // still queued, or leased: the queue has it
	}
	rec, err := deps.Admin.Runs().Get(ctx, runID)
	if err != nil {
		return false, err
	}
	if rec == nil {
		return false, nil
	}
	if rec.EndedAt != nil {
		// The run ended but the thread never heard: a Finalize that failed
		// half way. The thread is moved to match the record.
		state := rec.State
		if !isTerminal(state) {
			state = ports.StateFailed
		}
		Logger(deps).Warn("thread stuck past its run's end; moved to the run's state", "thread", threadID, "run", runID, "state", state)
		if won, err := Transition(ctx, deps, threadID, StateChange{
			From: ActiveStates, To: state, RunID: runID, Model: thread.Model,
		}); err != nil || !won {
			return false, err
		}
		_, err := Publish(ctx, deps, threadID, "STATE_CHANGE", map[string]any{
			"state": state, "stopReason": rec.StopReason, "runId": runID, "endedAt": *rec.EndedAt,
		})
		CloseLostSegment(ctx, deps, threadID, runID, "the run ended but its worker never closed its stream")
		return true, err
	}
	// A run just accepted has no lock and no row for a moment between its
	// state write and its enqueue. A lock lease of silence tells that apart
	// from a run nobody will ever pick up.
	last := rec.StartedAt
	if rec.EnqueuedAt != nil && rec.EnqueuedAt.After(last) {
		last = *rec.EnqueuedAt
	}
	wait := deps.Config.RunLockLease
	if queueCannotSay {
		// Without the queue's word, the run is lost only once it has been
		// quiet for longer than any job of it could fairly wait: a retry sent
		// back with a delay, or a queue with a wait limit. The thread's own
		// last change counts too, since a retry moves it back to QUEUED.
		// Should the job turn up after all, it finds the run taken and does
		// nothing.
		if thread.UpdatedAt.After(last) {
			last = thread.UpdatedAt
		}
		wait = max(wait, deps.Config.MaxQueueWait, longestRetryDelay(deps.Config))
	}
	if time.Since(last) < wait {
		return false, nil
	}
	Logger(deps).Warn("run lost by the queue; re-dispatched", "thread", threadID, "run", runID, "state", thread.State)
	// A worker that took it and died may have left its segment's stream open.
	CloseLostSegment(ctx, deps, threadID, runID, "the worker was lost; the run was dispatched again")
	job2 := ports.RunJob{
		ThreadID: threadID, RunID: runID, Model: rec.Model, Agent: rec.Agent,
		Kind: ports.JobReclaim, EnqueuedAt: time.Now().UnixMilli(),
		State: rec.RunState, ProviderOptions: rec.ProviderOptions,
		CostBudgetMicros: rec.CostBudgetMicros, MaxSteps: rec.MaxSteps,
	}
	if rec.EnqueuedAt != nil {
		job2.DispatchedAt = rec.EnqueuedAt.UnixMilli()
	}
	if rec.TokenBudget != nil {
		job2.TokenBudget = *rec.TokenBudget
	}
	return enqueueReclaim(ctx, deps, job2)
}

func enqueueReclaim(ctx context.Context, deps ports.RuntimePorts, job ports.RunJob) (bool, error) {
	err := EnqueueJob(ctx, deps, job, &ports.EnqueueOptions{Key: reclaimJobKey(job.RunID), Priority: ports.PriorityLow})
	if errors.Is(err, ports.ErrDuplicateJob) {
		return false, nil // someone else got there first
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// longestRetryDelay is the longest a failed run's retry is held back (§2.8):
// the base doubled per attempt, capped, plus the most jitter a delay can get.
func longestRetryDelay(c ports.AgentConfig) time.Duration {
	base, limit := c.RunRetryBackoff, c.RunRetryBackoffMax
	if base <= 0 {
		return 0
	}
	d := base
	for i := 1; i < c.RunMaxAttempts; i++ {
		d *= 2
		if limit > 0 && d >= limit {
			break
		}
	}
	if limit > 0 && d > limit {
		d = limit
	}
	return d + d/4
}
