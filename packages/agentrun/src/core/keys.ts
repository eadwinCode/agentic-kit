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

/** Re-dispatch counter for a job that keeps finding the run lock held by an
 *  OLDER run (§2.8). Separate from `agent:attempts:` — a blocked job has not
 *  failed, it simply has not started yet. */
export const redriveKey = (threadId: string) => `agent:redrive:${threadId}`;

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
  await deps.kv.set(runIdKey(threadId), runId);
  return runId;
}
