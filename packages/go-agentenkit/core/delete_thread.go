package core

import (
	"context"

	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

// DeleteThread is the §3.2 deletion behavior: one call removes the thread
// and everything that follows it (messages, events, usage rows) plus the
// thread's hot kv keys.
//
// Guards:
//   - QUEUED and RUNNING are refused: a job is on its way or a live worker
//     is mid-segment and would keep writing behind the delete. Stop first,
//     then delete.
//   - WAITING_FOR_INPUT deletes cleanly: a park holds NO process (§2.5), and
//     a late resume dispatch is a no-op against the missing thread. Its open
//     run record is closed here, so it never counts as in flight again.
func DeleteThread(ctx context.Context, deps ports.RuntimePorts, threadID string) (ports.DeleteThreadResult, error) {
	thread, err := deps.Storage.Threads.Get(ctx, threadID)
	if err != nil {
		return ports.DeleteThreadResult{}, err
	}
	if thread == nil {
		return ports.DeleteThreadResult{Accepted: false, Error: "Thread not found"}, nil
	}
	if thread.State == ports.StateRunning || thread.State == ports.StateQueued {
		return ports.DeleteThreadResult{Accepted: false, Error: "Thread has an active run — stop it before deleting"}, nil
	}
	// The platform's own records for the thread close first (§2.9): a run
	// still open here can never be worked on again once the thread is gone.
	runs, _ := deps.Admin.Runs().ListByThread(ctx, threadID)
	for _, run := range runs {
		if run.EndedAt == nil {
			closeRunRecord(ctx, deps, run.ID, FinalizeInput{State: ports.StateCancelled, StopReason: "deleted", RunID: run.ID})
		}
	}
	// Its run streams go now rather than at their grace window's end. The
	// record says which it had, so it is read before the cascade takes it.
	segments, _ := deps.Storage.Events.List(ctx, threadID, ports.ThreadEventFilter{Types: []string{"RUN_STARTED"}})
	if err := deps.Storage.Threads.Delete(ctx, threadID); err != nil {
		return ports.DeleteThreadResult{}, err
	}
	if deps.Streams != nil {
		for _, e := range segments {
			var p struct {
				StreamID string `json:"streamId"`
			}
			if e.PayloadInto(&p) == nil && p.StreamID != "" {
				_ = deps.Streams.Delete(ctx, p.StreamID)
			}
		}
	}
	// The platform's own history of the thread goes with it: a deleted thread
	// must not live on in operational views. Best effort, like every admin
	// write; the caller's data is already gone.
	if err := deps.Admin.Threads().Delete(ctx, threadID); err != nil {
		Logger(deps).Error("admin history of a deleted thread not removed", "thread", threadID, "err", err)
	}
	// Live UIs subscribed to the thread learn it ceased to exist: bus-only
	// notice, the event log is gone with it.
	_ = PublishNotice(ctx, deps, threadID, "THREAD_DELETED", map[string]any{"threadId": threadID})

	// Hot cache cleanup: a deleted thread must not resurrect from kv. The
	// retry counters are per run, so every run the thread had is swept.
	keys := []string{
		// SeqKey is the counter older releases kept.
		StateKey(threadID), RunLockKey(threadID), SeqKey(threadID),
		AttemptsKey(threadID), RunIDKey(threadID), RedriveKey(threadID),
	}
	for _, run := range runs {
		keys = append(keys, AttemptsKey(run.ID), RedriveKey(run.ID), SegmentKey(run.ID))
	}
	for _, key := range keys {
		if err := deps.Kv.Del(ctx, key); err != nil {
			return ports.DeleteThreadResult{}, err
		}
	}
	return ports.DeleteThreadResult{Accepted: true}, nil
}
