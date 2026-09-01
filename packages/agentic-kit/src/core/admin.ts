import type { RuntimePorts } from '../ports/runtime.js';
import type { RunFilter, StepRecord } from '../ports/admin.js';
import type {
  AgentEvent,
  ExecutionState,
  RunRecord,
  UsageTotals,
} from './types.js';

/** One completed loop iteration, read back off the event log (§2.9). */
export interface Percentiles {
  p50: number;
  p95: number;
  max: number;
}

export interface RunStats {
  total: number;
  byState: Partial<Record<ExecutionState, number>>;
  byStopReason: Record<string, number>;
  tokens: UsageTotals;
  /** Wall time from enqueue to finish, over runs that ended. A parked run
   *  legitimately includes however long the human took (§2.5). */
  duration: Percentiles | null;
  /** Time spent waiting for a worker — the backlog signal (§2.8). */
  queued: Percentiles | null;
  failed: number;
}

export interface AdminOverview {
  runs: RunStats;
  /** Threads by state, from the platform's own view — never a read of the
   *  caller's database (§2.9). */
  threads: Partial<Record<ExecutionState, number>>;
  /** Every run ever, by state, unbounded by the stats window. */
  runsByState: Partial<Record<ExecutionState, number>>;
  /** Runs still in flight, newest first. */
  active: RunRecord[];
}

/** Everything about one run, assembled for a timeline view. */
export interface RunDetail {
  run: RunRecord;
  steps: StepRecord[];
  /** Nested runs spawned beneath it (§2.7) — the same RunRecord shape. */
  subagents: RunRecord[];
  /** The run's events with CHUNKs stripped — the readable spine, not the
   *  token-by-token firehose. */
  events: AgentEvent[];
}

const EMPTY: UsageTotals = {
  inputTokens: 0, cachedInputTokens: 0, outputTokens: 0, totalTokens: 0,
};

function percentiles(values: number[]): Percentiles | null {
  if (values.length === 0) return null;
  const s = [...values].sort((a, b) => a - b);
  const at = (q: number) => s[Math.min(s.length - 1, Math.floor(q * s.length))]!;
  return { p50: at(0.5), p95: at(0.95), max: s[s.length - 1]! };
}

export function summarise(runs: RunRecord[]): RunStats {
  const byState: Partial<Record<ExecutionState, number>> = {};
  const byStopReason: Record<string, number> = {};
  const tokens = { ...EMPTY };
  const durations: number[] = [];
  const queued: number[] = [];
  let failed = 0;

  for (const r of runs) {
    byState[r.state] = (byState[r.state] ?? 0) + 1;
    if (r.stopReason) byStopReason[r.stopReason] = (byStopReason[r.stopReason] ?? 0) + 1;
    tokens.inputTokens += r.inputTokens;
    tokens.cachedInputTokens += r.cachedInputTokens;
    tokens.outputTokens += r.outputTokens;
    tokens.totalTokens += r.totalTokens;
    if (typeof r.durationMs === 'number') durations.push(r.durationMs);
    if (typeof r.queuedMs === 'number') queued.push(r.queuedMs);
    if (r.state === 'FAILED') failed += 1;
  }

  return {
    total: runs.length,
    byState,
    byStopReason,
    tokens,
    duration: percentiles(durations),
    queued: percentiles(queued),
    failed,
  };
}

/** Default window for a dashboard read: enough to be useful, bounded enough
 *  that "show me everything" can never become a table scan. */
const DEFAULT_LIMIT = 200;

export async function listRuns(
  deps: RuntimePorts,
  filter: RunFilter = {},
): Promise<RunRecord[]> {
  return deps.admin.runs.list({ ...filter, limit: filter.limit ?? DEFAULT_LIMIT });
}

export async function runStats(
  deps: RuntimePorts,
  range: { since?: Date; until?: Date; limit?: number } = {},
): Promise<RunStats> {
  // Percentiles are computed here rather than pushed into the adapter, so an
  // adapter only ever writes filters it can express in one indexed query.
  return summarise(await listRuns(deps, { ...range, limit: range.limit ?? 1_000 }));
}

export async function overview(
  deps: RuntimePorts,
  range: { since?: Date } = {},
): Promise<AdminOverview> {
  const [threads, runsByState, recent, active] = await Promise.all([
    deps.admin.threads.countByState(),
    deps.admin.runs.countByState(),
    listRuns(deps, { since: range.since, limit: 1_000 }),
    deps.admin.runs.list({ state: ['RUNNING', 'WAITING_FOR_INPUT'], limit: 50 }),
  ]);
  return { runs: summarise(recent), threads, runsByState, active };
}

/** A run's steps, in order (§2.9). Rows in the platform's own store rather
 *  than a scan of the caller's event log. */
export async function listSteps(deps: RuntimePorts, runId: string): Promise<StepRecord[]> {
  return deps.admin.steps.listByRun(runId);
}

export async function getRun(deps: RuntimePorts, runId: string): Promise<RunDetail | null> {
  const run = await deps.admin.runs.get(runId);
  if (!run) return null;

  const [steps, siblings, events] = await Promise.all([
    listSteps(deps, runId),
    deps.admin.runs.listByThread(run.threadId),
    deps.storage.events.listSince(run.threadId, -1),
  ]);

  const from = new Date(run.startedAt).getTime();
  const to = run.endedAt ? new Date(run.endedAt).getTime() : Infinity;
  return {
    run,
    steps,
    // Its children: same table, distinguished by depth (§2.7).
    subagents: siblings.filter((c: RunRecord) => c.parentRunId === runId),
    // CHUNK is the token firehose; a timeline wants the spine.
    events: events.filter((e: AgentEvent) => {
      if (e.type === 'CHUNK' || e.type === 'SUBAGENT_CHUNK') return false;
      const at = new Date(e.createdAt).getTime();
      return at >= from && at <= to;
    }),
  };
}
