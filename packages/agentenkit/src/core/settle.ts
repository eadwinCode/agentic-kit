import { randomUUID } from 'node:crypto';
import type { RunFinishInfo, RuntimePorts } from '../ports/runtime.js';
import type { ExecutionState, UsageTotals } from './types.js';
import type { RegisteredAgent } from './agent.js';
import { emptyTotals } from './usage.js';
import { Lease } from './lease.js';

/** How long a settle claim holds (§5.6). A claim older than this belongs to a
 *  settler that died, and the next settle takes the run over. */
export const SETTLE_CLAIM_TTL_MS = 10 * 60 * 1000;

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

/** Whether a state is one a run cannot leave. */
export const isTerminal = (state?: ExecutionState | null): boolean =>
  state === 'CANCELLED' || state === 'COMPLETED' || state === 'FAILED';

/** Sum every model call a run made, nested runs included (§4). A storage
 *  hiccup must not turn a finished run into a failed one, so the read never
 *  throws; but a hook that bills from these totals must be able to tell
 *  "spent nothing" from "could not read", so the error comes back beside the
 *  zero totals rather than swallowed. */
export async function runBill(
  deps: RuntimePorts,
  threadId: string,
  runId?: string,
): Promise<{ usage: UsageTotals; error?: unknown }> {
  if (!runId) return { usage: emptyTotals() };
  try {
    return { usage: await deps.storage.usage.total(threadId, { runId }) };
  } catch (error) {
    (deps.log ?? console).error('run bill not read', { run: runId, err: error });
    return { usage: emptyTotals(), error };
  }
}

/** Run the spec's `onSettle` for a run, once (§5.6). Every path that ends a
 *  run goes through here: the worker that finished or aborted it, the worker
 *  that failed it, a stop that ended it while no worker held it, and the
 *  late-settle sweep.
 *
 *  Once is kept by a claim on the run record, made in one conditional write
 *  before the hook runs: only the settler that wins it calls the hook, so two
 *  that arrive together cannot both bill. The hook's success marks the run
 *  settled; its failure drops the claim, so a later stop, delivery or sweep
 *  runs it again. A claim that is never ended (the settler died mid-hook) is
 *  taken over after SETTLE_CLAIM_TTL_MS. That is also why the hook must be
 *  idempotent by runId: a hook slower than the claim, or a mark that could
 *  not be written, can see the same run twice.
 *
 *  `reached` is whether this call ran the settle at all; `error` is the
 *  hook's. A run with no record (a foreign dispatch) settles unclaimed, as it
 *  always did. A store that cannot take the claim leaves the run unsettled
 *  for the sweep rather than risk a second bill. */
export async function settleRun(
  deps: RuntimePorts,
  agent: RegisteredAgent | null,
  info: RunFinishInfo,
): Promise<{ reached: boolean; error?: unknown }> {
  const log = deps.log ?? console;
  const hook = async (): Promise<unknown> => {
    const onSettle = agent?.args.onSettle;
    if (typeof onSettle !== 'function') return undefined;
    try {
      await onSettle(info);
      return undefined;
    } catch (err) {
      return err ?? new Error('onSettle threw');
    }
  };
  const runId = info.runId;
  if (!runId) return { reached: true, error: await hook() };

  const token = randomUUID();
  let claimed = false;
  let failure: unknown;
  for (let attempt = 0; attempt < 3; attempt++) {
    if (attempt > 0) await sleep(attempt * 200);
    try {
      claimed = await deps.admin.runs.claimSettle(runId, token, new Date(Date.now() - SETTLE_CLAIM_TTL_MS));
      failure = undefined;
      break;
    } catch (err) {
      failure = err;
    }
  }
  if (failure !== undefined) {
    log.error('settle not claimed; the run stays unsettled for the late-settle sweep', { run: runId, err: failure });
    return { reached: false };
  }
  if (!claimed) {
    const rec = await deps.admin.runs.get(runId).catch(() => undefined);
    // No record to claim: settled unclaimed.
    if (rec === null) return { reached: true, error: await hook() };
    return { reached: false }; // settled already, or being settled by someone else
  }

  const error = await hook();
  let markError: unknown;
  for (let attempt = 0; attempt < 3; attempt++) {
    if (attempt > 0) await sleep(attempt * 200);
    try {
      await deps.admin.runs.endSettle(runId, token, error === undefined);
      markError = undefined;
      break;
    } catch (err) {
      markError = err;
    }
  }
  if (error !== undefined) {
    // The claim is dropped, so a later stop, delivery or sweep can run the
    // hook again.
    log.error('settle hook failed; the run stays unsettled for a retry', { run: runId, err: error });
  } else if (markError !== undefined) {
    log.error('settle mark not written; the claim lapses and the late-settle sweep may settle this run again', {
      run: runId, err: markError,
    });
  }
  return { reached: true, error };
}

/** Fire a spec's `onFinish`. The run has already ended: a callback that throws
 *  is the caller's bug to see in the log, not a reason to change the end. */
export async function callOnFinish(deps: RuntimePorts, agent: RegisteredAgent | null, info: RunFinishInfo) {
  const onFinish = agent?.args.onFinish;
  if (typeof onFinish !== 'function') return;
  try {
    await onFinish(info);
  } catch (err) {
    (deps.log ?? console).error('onFinish threw', { runId: info.runId, err: String(err) });
  }
}

/** Settle a run whose record has ended but whose settle never ran (§2.1,
 *  §5.6): a stop that ended it while it was queued or parked, a worker that
 *  died between finishing and settling, or a settle hook that failed. The
 *  steps it did make were priced as they happened, so its bill is real and
 *  has to reach the hook. Nothing happens when the run already settled, or
 *  when its record is still open.
 *
 *  The caller holds the run lock: a stop takes it for exactly this, and so do
 *  a worker that finds the thread already ended and the sweep. */
export async function settleEndedRun(
  deps: RuntimePorts,
  agent: RegisteredAgent | null,
  threadId: string,
  runId?: string,
): Promise<boolean> {
  if (!agent || !runId) return false;
  const rec = await deps.admin.runs.get(runId).catch(() => null);
  if (!rec || rec.settledAt || !isTerminal(rec.state)) return false;
  const bill = await runBill(deps, threadId, runId);
  const info: RunFinishInfo = {
    threadId, runId, state: rec.state, stopReason: rec.stopReason || 'cancelled',
    cancelled: rec.state === 'CANCELLED',
    ...(rec.error ? { error: rec.error } : {}),
    tokensUsed: rec.totalTokens,
    attribution: {
      inputTokens: rec.inputTokens, cachedInputTokens: rec.cachedInputTokens,
      outputTokens: rec.outputTokens, totalTokens: rec.totalTokens,
    },
    steps: rec.steps,
    usage: bill.usage,
    ...(bill.error !== undefined ? { usageError: bill.error } : {}),
  };
  // The hook's error is not acted on: the run has already ended either way,
  // and the claim is dropped for a retry.
  const { reached } = await settleRun(deps, agent, info);
  if (reached) await callOnFinish(deps, agent, info);
  return reached;
}

/** Settle an ended run whose settle never ran (§5.6), under the run lock.
 *  False when it was settled meanwhile, or a worker holds the run (that
 *  worker settles it). */
export async function settleLate(
  deps: RuntimePorts,
  agent: RegisteredAgent | null,
  threadId: string,
  runId?: string,
): Promise<boolean> {
  if (!runId) return false;
  const lease = await Lease.acquire(deps, threadId, runId, undefined);
  if (!lease) return false;
  // Renewed while the hook runs: a slow settle must not let a queued job take
  // the lock and settle the same run beside it.
  lease.keep();
  try {
    return await settleEndedRun(deps, agent, threadId, runId);
  } finally {
    await lease.release();
  }
}
