import { randomUUID } from 'node:crypto';
import type { RuntimePorts } from '../ports/runtime.js';

/** The thread's CURRENT run id (§2.1).
 *
 *  Stop and start-a-new-run both write `agent:state:{threadId}`, so the state
 *  key alone can never tell a worker its run is over: a user who stops and
 *  then sends another message overwrites CANCELLED with RUNNING before the
 *  worker's poll ever reads it, and the old run keeps going.
 *
 *  This key only moves forward. A worker whose id no longer matches knows it
 *  has been replaced, whatever the state key says. */
export const runIdKey = (threadId: string) => `agent:run:${threadId}`;

/** Counts §2.8 failure retries of ONE run. Keyed by the run, so a new run on
 *  the thread starts with a full budget by construction and nothing an older
 *  run banked can reach it. A legacy job without a run id keys it by the
 *  thread instead; see `counterScope`. */
export const attemptsKey = (scope: string) => `agent:attempts:${scope}`;

/** Re-dispatch counter for a job that keeps finding the run lock held (§2.8).
 *  Separate from the attempts key — a blocked job has not failed, it simply
 *  has not started yet. Keyed by the run, like `attemptsKey`. */
export const redriveKey = (scope: string) => `agent:redrive:${scope}`;

/** What the retry counters are keyed by: the run id, or the thread id for a
 *  legacy dispatch that has none. */
export const counterScope = (threadId: string, runId?: string) => runId || threadId;

/** How long a retry counter lives when nothing clears it: long past any retry
 *  backoff, short enough that a counter a crash left behind does not sit in
 *  the kv for ever. */
export const COUNTER_TTL_SECONDS = 6 * 60 * 60;

/** How long a thread's hot keys (state, current run, event seq) live after
 *  they were last written. The durable row is the truth; these only save a
 *  read, so a thread idle past this simply reads storage again, and its seq
 *  counter carries on from the stored events (see nextSeq). Without an
 *  expiry, every thread ever run keeps three keys in the kv for ever. */
export const THREAD_KEY_TTL_SECONDS = 30 * 24 * 60 * 60;

/** The run that owns the thread right now, or null on a thread that predates
 *  run ids. Resuming a parked run (§2.5) REUSES this — a resume is the same
 *  run continuing, not a new one, so it must never bump the id. */
export async function currentRunId(
  deps: RuntimePorts,
  threadId: string,
): Promise<string | undefined> {
  return (await deps.kv.get(runIdKey(threadId))) ?? undefined;
}

/** Claim the thread for a brand new run and return its id (§2.1).
 *
 *  Always called BEFORE the state key is written: bumping this id is what
 *  retires an older worker, and it must not depend on the state key that the
 *  new run is about to overwrite. */
export async function claimRun(deps: RuntimePorts, threadId: string): Promise<string> {
  const runId = randomUUID();
  await deps.kv.set(runIdKey(threadId), runId, { exSeconds: THREAD_KEY_TTL_SECONDS });
  return runId;
}
