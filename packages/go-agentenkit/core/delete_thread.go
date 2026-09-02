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
//   - RUNNING is refused: a live worker is mid-segment and would keep writing
//     behind the delete. Stop first, then delete.
//   - WAITING_FOR_INPUT deletes cleanly: a park holds NO process (§2.5), and
//     a late resume dispatch is a no-op against the missing thread.
func DeleteThread(ctx context.Context, deps ports.RuntimePorts, threadID string) (ports.DeleteThreadResult, error) {
	thread, err := deps.Storage.Threads.Get(ctx, threadID)
	if err != nil {
		return ports.DeleteThreadResult{}, err
	}
	if thread == nil {
		return ports.DeleteThreadResult{Accepted: false, Error: "Thread not found"}, nil
	}
	if thread.State == ports.StateRunning {
		return ports.DeleteThreadResult{Accepted: false, Error: "Thread has an active run — stop it before deleting"}, nil
	}
	if err := deps.Storage.Threads.Delete(ctx, threadID); err != nil {
		return ports.DeleteThreadResult{}, err
	}
	// Live UIs subscribed to the thread learn it ceased to exist: bus-only
	// notice, the event log is gone with it.
	_ = PublishNotice(ctx, deps, threadID, "THREAD_DELETED", map[string]any{"threadId": threadID})

	// Hot cache cleanup: a deleted thread must not resurrect from kv
	for _, key := range []string{
		StateKey(threadID), RunLockKey(threadID), SeqKey(threadID),
		AttemptsKey(threadID), RunIDKey(threadID), RedriveKey(threadID),
	} {
		if err := deps.Kv.Del(ctx, key); err != nil {
			return ports.DeleteThreadResult{}, err
		}
	}
	return ports.DeleteThreadResult{Accepted: true}, nil
}
