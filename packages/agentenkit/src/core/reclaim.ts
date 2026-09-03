import type { RuntimePorts } from '../ports/runtime.js';
import type { ResumeInfo } from './types.js';
import { currentRunId } from './keys.js';
import { hitlDeadline, loadOpenHitls } from './hitl.js';

// Small grace so an in-flight /respond delivery always lands first —
// reclamation only ever sees true orphans.
export function reclaimGraceAfterMs(deps: RuntimePorts): number {
  return deps.config.hitlTtlMs + deps.config.reclaimGraceMs;
}

/** §2.5 orphan reclamation, the FALLBACK path.
 *
 *  A park schedules its own expiry on the queue (see parkForApproval), so the
 *  deadline holds whether or not anyone is watching. This covers what a timer
 *  cannot: threads parked before that existed, and queue adapters that drop a
 *  delay. Callers are first-touch checks in run() and respond(), plus any
 *  listener that wants the denial to land live in front of a user.
 *
 *  It no longer heals inline. It re-dispatches the run and lets the engine
 *  resolve the park, so there is exactly ONE definition of what an expired
 *  approval becomes — and so a thread holding several open approvals (§2.7)
 *  is resolved as a set rather than one request at a time. Re-dispatching the
 *  same run twice is safe: the run lock and the engine's readiness check make
 *  the duplicate a no-op (§2.8).
 *
 *  Returns true iff a re-dispatch was enqueued. */
export async function reclaimIfOrphaned(deps: RuntimePorts, threadId: string): Promise<boolean> {
  const thread = await deps.storage.threads.get(threadId);
  if (thread?.state !== 'WAITING_FOR_INPUT') return false;

  const open = await loadOpenHitls(deps, threadId);
  if (open.length === 0) return false;

  // Every open request must be past its window. One that is still answerable
  // would make the resumed segment a no-op anyway (§2.7).
  const now = Date.now();
  if (!open.every((p) => now >= hitlDeadline(p, deps.config) + deps.config.reclaimGraceMs)) {
    return false;
  }

  // Rebuild the original dispatch from the ticket persisted in the event
  // payload; a legacy park without one falls back to the default handle.
  const requested = await deps.storage.events.listByType(threadId, 'INPUT_REQUIRED');
  const resume = (requested.at(-1)?.payload as { resume?: ResumeInfo } | null)?.resume;

  await deps.queue.enqueue({
    threadId,
    // Resuming a parked run REUSES its id — it is the same run continuing,
    // and the park's own expiry job must stay a duplicate of this one (§2.1).
    runId: await currentRunId(deps, threadId),
    model: resume?.model ?? thread.model,
    ...(resume
      ? {
          agent: resume.agent,
          ...(resume.tokenBudget !== undefined ? { tokenBudget: resume.tokenBudget } : {}),
          ...(resume.providerOptions ? { providerOptions: resume.providerOptions } : {}),
        }
      : {}),
  });
  return true;
}
