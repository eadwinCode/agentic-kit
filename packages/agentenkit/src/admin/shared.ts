import type { ExecutionState } from '../core/types.js';
import type { RunTotals } from '../ports/admin.js';

/** Zero totals over no runs. */
export const emptyRunTotals = (): RunTotals => ({
  runs: 0, byState: {}, byStopReason: {}, steps: 0,
  inputTokens: 0, cachedInputTokens: 0, outputTokens: 0, totalTokens: 0,
});

/** Fold one group of `runs` runs (one state and stop reason, with their
 *  summed counters) into totals. Every admin store groups the same way. */
export function addRunTotals(
  out: RunTotals,
  state: ExecutionState,
  stopReason: string | null | undefined,
  runs: number,
  sums: { steps: number; inputTokens: number; cachedInputTokens: number; outputTokens: number; totalTokens: number },
): void {
  out.runs += runs;
  out.byState[state] = (out.byState[state] ?? 0) + runs;
  if (stopReason) out.byStopReason[stopReason] = (out.byStopReason[stopReason] ?? 0) + runs;
  out.steps += Number(sums.steps);
  out.inputTokens += Number(sums.inputTokens);
  out.cachedInputTokens += Number(sums.cachedInputTokens);
  out.outputTokens += Number(sums.outputTokens);
  out.totalTokens += Number(sums.totalTokens);
}

/** The run columns a patch may write (§2.9). Anything else in a patch is
 *  ignored rather than turned into SQL. */
export const RUN_COLUMNS = new Set([
  'state', 'stopReason', 'error', 'startedAt', 'endedAt', 'enqueuedAt', 'durationMs', 'queuedMs',
  'settledAt', 'steps', 'inputTokens', 'cachedInputTokens', 'outputTokens', 'totalTokens',
  'attempts', 'result', 'prompt', 'tokenBudget', 'runState', 'providerOptions',
]);
/** The run columns that hold JSON. */
export const JSON_COLUMNS = new Set(['result', 'runState', 'providerOptions']);
