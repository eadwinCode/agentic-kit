import type { RuntimePorts } from '../ports/runtime.js';
import type { DeleteThreadResult } from '../ports/runtime.js';
import { publishNotice } from './publish.js';
import { redriveKey, runIdKey } from './keys.js';

/** The §3.2 deletion behavior: one call removes the thread and everything
 *  that follows it — messages, events, usage rows, subagent runs — plus the
 *  thread's hot kv keys (state cache, run lock, event seq, retry counter).
 *
 *  Guards:
 *  - RUNNING is refused: a live worker is mid-segment and would keep writing
 *    behind the delete. `stop()` first, then delete.
 *  - WAITING_FOR_INPUT deletes cleanly: a park holds NO process (§2.5) — the
 *    run segment already ended, and a late resume dispatch is a no-op against
 *    the missing thread (the engine's terminal-state guard, §2.8). */
export async function deleteThread(
  deps: RuntimePorts,
  threadId: string,
): Promise<DeleteThreadResult> {
  const thread = await deps.storage.threads.get(threadId);
  if (!thread) return { accepted: false, error: 'Thread not found' };
  if (thread.state === 'RUNNING') {
    return { accepted: false, error: 'Thread has an active run — stop it before deleting' };
  }

  // Cascade: messages, events, usage, runs follow the thread (§3.2)
  await deps.storage.threads.delete(threadId);

  // Live UIs subscribed to the thread channel learn it ceased to exist —
  // bus-only notice (seq 0, never persisted: the event log is gone with it)
  await publishNotice(deps, threadId, 'THREAD_DELETED', { threadId });

  // Hot cache cleanup — a deleted thread must not resurrect from kv
  await deps.kv.del(`agent:state:${threadId}`);
  await deps.kv.del(`agent:lock:${threadId}`);
  await deps.kv.del(`agent:seq:${threadId}`);
  await deps.kv.del(`agent:attempts:${threadId}`);
  await deps.kv.del(runIdKey(threadId));
  await deps.kv.del(redriveKey(threadId));

  return { accepted: true };
}
