import type { RuntimePorts } from '../ports/runtime.js';
import { HITL_TTL_MS } from './hitl.js';
import { publish } from './publish.js';

// Small grace so a live waiter always times out first — reclamation only ever
// sees true orphans. Concurrent callers are safe: threads.claimState is a
// compare-and-set (§3.4) — exactly one caller wins, everyone else skips.
export function reclaimGraceAfterMs(deps: RuntimePorts): number {
  return deps.config.hitlTtlMs + deps.config.reclaimGraceMs;
}

/** §2.5 orphan reclamation: heals a thread whose parked invocation died before
 *  the TTL fired (deploy, crash, infra kill). Invoked by listeners — SSE
 *  distributor (death notices + heartbeat) and first-touch checks in
 *  run/stop/respond — never by a scheduler.
 *  Returns true iff THIS caller performed the reclamation. */
export async function reclaimIfOrphaned(deps: RuntimePorts, threadId: string): Promise<boolean> {
  const stale = await deps.storage.events.latest(threadId, 'INPUT_REQUIRED');
  const age = stale ? Date.now() - new Date(stale.createdAt).getTime() : 0;
  if (!stale || age < reclaimGraceAfterMs(deps)) return false;

  // Atomic claim — exactly one caller wins
  const claimed = await deps.storage.threads.claimState(
    threadId,
    'WAITING_FOR_INPUT',
    'RUNNING',
  );
  if (!claimed) return false;

  const { toolCallId } = stale.payload as { toolCallId: string };

  // The same tool result the engine would have produced on timeout (§2.5)
  await deps.storage.messages.append(threadId, {
    role: 'tool',
    content: { toolCallId, result: { responded: false, cancelled: true, reason: 'timeout' } },
  });
  await publish(deps, threadId, 'INPUT_EXPIRED', { toolCallId });
  await publish(deps, threadId, 'STATE_CHANGE', { state: 'RUNNING' });

  // Keep the hot cache in sync with the durable claim (§3.4 invariant 5)
  await deps.kv.set(`agent:state:${threadId}`, 'RUNNING');

  const thread = await deps.storage.threads.get(threadId);
  if (thread) {
    await deps.queue.enqueue({ threadId, model: thread.model }); // re-enter via the queue (§2.8)
  }
  return true;
}
