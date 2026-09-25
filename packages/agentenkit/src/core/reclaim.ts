import type { RuntimePorts } from '../ports/runtime.js';
import type { ExecutionState, ProviderOptions, ResumeInfo, ThreadDTO } from './types.js';
import { currentRunId } from './keys.js';
import { hitlDeadline, loadOpenHitls } from './hitl.js';
import { enqueueJob } from './lease.js';
import { DuplicateJobError, PRIORITY_LOW, UnsupportedError } from '../ports/queue.js';
import { runLockKey } from './lease.js';
import { ACTIVE_STATES, publish, transition } from './publish.js';
import { isTerminal } from './settle.js';

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
 *  It also covers a run the queue lost: a thread QUEUED or RUNNING whose run
 *  lock nobody holds and whose job the queue no longer has. A worker that died
 *  leaves exactly that behind once its lock lapses, and so does a job dropped
 *  as dead. The run is re-dispatched and resumes from its last persisted
 *  step, unless its record already ended, in which case the thread is moved
 *  to match the record.
 *
 *  It no longer heals inline. It re-dispatches the run and lets the engine
 *  resolve the park, so there is exactly ONE definition of what an expired
 *  approval becomes — and so a thread holding several open approvals (§2.7)
 *  is resolved as a set rather than one request at a time. Re-dispatching the
 *  same run twice is safe: the run lock and the engine's readiness check make
 *  the duplicate a no-op (§2.8).
 *
 *  Returns true iff a re-dispatch was enqueued or the thread was healed. */
export async function reclaimIfOrphaned(deps: RuntimePorts, threadId: string): Promise<boolean> {
  const thread = await deps.storage.threads.get(threadId);
  if (!thread) return false;
  if (thread.state === 'QUEUED' || thread.state === 'RUNNING') return reclaimLost(deps, thread);
  if (thread.state !== 'WAITING_FOR_INPUT') return false;

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

  // Resuming a parked run REUSES its id — it is the same run continuing, and
  // the park's own expiry job must stay a duplicate of this one (§2.1). Keyed
  // on the run, so two callers that heal the thread at once queue one job.
  const runId = await currentRunId(deps, threadId);
  try {
    await enqueueJob(
      deps,
      {
        kind: 'reclaim',
        threadId,
        runId,
        model: resume?.model ?? thread.model,
        ...(resume
          ? {
              agent: resume.agent,
              ...(resume.tokenBudget !== undefined ? { tokenBudget: resume.tokenBudget } : {}),
              ...(resume.providerOptions ? { providerOptions: resume.providerOptions } : {}),
              ...(resume.state ? { state: resume.state } : {}),
              ...(resume.costBudgetMicros !== undefined ? { costBudgetMicros: resume.costBudgetMicros } : {}),
              ...(resume.maxSteps ? { maxSteps: resume.maxSteps } : {}),
              ...(resume.dispatchedAt ? { dispatchedAt: resume.dispatchedAt } : {}),
            }
          : {}),
      },
      { key: `reclaim:${runId}`, priority: PRIORITY_LOW },
    );
  } catch (err) {
    if (err instanceof DuplicateJobError) return false; // someone else got there first
    throw err;
  }
  return true;
}

/** Re-dispatch a QUEUED or RUNNING thread that nothing is working on: no run
 *  lock, no job on the queue, and a run record that has sat untouched for
 *  longer than a lock lease. */
async function reclaimLost(deps: RuntimePorts, thread: ThreadDTO): Promise<boolean> {
  const threadId = thread.id;
  const runId = await currentRunId(deps, threadId);
  if (!runId) return false;
  // A live worker: the lock is renewed while one runs (§3.4).
  if ((await deps.kv.get(runLockKey(threadId))) !== null) return false;
  let job;
  try {
    job = await deps.queue.find(runId);
  } catch (err) {
    if (err instanceof UnsupportedError) return false; // this queue cannot say; nothing to do safely
    throw err;
  }
  if (job) return false; // still queued, or leased: the queue has it
  const rec = await deps.admin.runs.get(runId);
  if (!rec) return false;
  const log = (deps.log ?? console) as { warn?: (m: string, ...r: unknown[]) => void };
  if (rec.endedAt) {
    // The run ended but the thread never heard: a finalize that failed half
    // way. The thread is moved to match the record.
    const state: ExecutionState = isTerminal(rec.state) ? rec.state : 'FAILED';
    log.warn?.("thread stuck past its run's end; moved to the run's state", { threadId, runId, state });
    if (!(await transition(deps, threadId, { from: ACTIVE_STATES, to: state, runId, model: thread.model }))) {
      return false;
    }
    await publish(deps, threadId, 'STATE_CHANGE', {
      state, stopReason: rec.stopReason, runId, endedAt: rec.endedAt,
    });
    return true;
  }
  // A run just accepted has no lock and no job for a moment between its state
  // write and its enqueue. A lock lease of silence tells that apart from a run
  // nobody will ever pick up.
  let last = new Date(rec.startedAt).getTime();
  if (rec.enqueuedAt && new Date(rec.enqueuedAt).getTime() > last) last = new Date(rec.enqueuedAt).getTime();
  if (Date.now() - last < deps.config.runLockLeaseSeconds * 1000) return false;
  log.warn?.('run lost by the queue; re-dispatched', { threadId, runId, state: thread.state });
  try {
    await enqueueJob(
      deps,
      {
        kind: 'reclaim',
        threadId, runId, model: rec.model, agent: rec.agent,
        enqueuedAt: Date.now(),
        ...(rec.enqueuedAt ? { dispatchedAt: new Date(rec.enqueuedAt).getTime() } : {}),
        ...(rec.runState ? { state: rec.runState } : {}),
        ...(rec.providerOptions ? { providerOptions: rec.providerOptions as ProviderOptions } : {}),
        ...(rec.costBudgetMicros ? { costBudgetMicros: rec.costBudgetMicros } : {}),
        ...(rec.maxSteps ? { maxSteps: rec.maxSteps } : {}),
        ...(rec.tokenBudget ? { tokenBudget: rec.tokenBudget } : {}),
      },
      { key: `reclaim:${runId}`, priority: PRIORITY_LOW },
    );
  } catch (err) {
    if (err instanceof DuplicateJobError) return false; // someone else got there first
    throw err;
  }
  return true;
}
