import type { RuntimePorts } from '../ports/runtime.js';
import type { ResumeInfo } from './types.js';
import { claimRun } from './keys.js';
import { publish } from './publish.js';

// Small grace so an in-flight /respond delivery always lands first —
// reclamation only ever sees true orphans. Concurrent callers are safe:
// threads.claimState is a compare-and-set (§3.4) — exactly one caller wins,
// everyone else skips.
export function reclaimGraceAfterMs(deps: RuntimePorts): number {
  return deps.config.hitlTtlMs + deps.config.reclaimGraceMs;
}

/** §2.5 orphan reclamation: heals a thread whose parked HITL request expired
 *  with no answer (the TTL turns it into the timeout denial and the run
 *  continues). Invoked by listeners — SSE distributor (death notices +
 *  heartbeat) and first-touch checks in run/stop/respond — never by a
 *  scheduler.
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

  const payload = stale.payload as {
    toolCallId: string;
    toolName?: string;
    resume?: ResumeInfo;
  };

  // The same tool result the resumed segment would have produced on TTL
  // expiry — the model reads "user had no response, action cancelled" (§2.5)
  await deps.storage.messages.append(threadId, {
    role: 'tool',
    content: [
      {
        type: 'tool-result',
        toolCallId: payload.toolCallId,
        toolName: payload.toolName ?? 'unknown',
        result: { responded: false, cancelled: true, reason: 'timeout' },
      },
    ],
  });
  await publish(deps, threadId, 'INPUT_EXPIRED', { toolCallId: payload.toolCallId });
  await publish(deps, threadId, 'STATE_CHANGE', { state: 'RUNNING' });

  // Keep the hot cache in sync with the durable claim (§3.4 invariant 5)
  await deps.kv.set(`agent:state:${threadId}`, 'RUNNING');

  const thread = await deps.storage.threads.get(threadId);
  // Re-enter via the queue (§2.8) — the ticket persisted in the event payload
  // rebuilds the original dispatch; a legacy park falls back to the default.
  if (thread) {
    const runId = await claimRun(deps, threadId);
    await deps.queue.enqueue({
      threadId,
      runId,
      model: payload.resume?.model ?? thread.model,
      ...(payload.resume
        ? {
            agent: payload.resume.agent,
            ...(payload.resume.tokenBudget !== undefined
              ? { tokenBudget: payload.resume.tokenBudget }
              : {}),
            ...(payload.resume.providerOptions
              ? { providerOptions: payload.resume.providerOptions }
              : {}),
          }
        : {}),
    });
  }
  return true;
}
