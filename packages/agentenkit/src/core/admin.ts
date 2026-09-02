import type { RuntimePorts } from '../ports/runtime.js';
import type { RunFilter, StepRecord, ThreadStart } from '../ports/admin.js';
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

/** A thread with its runs rolled up (§2.9) — what a listing needs to rank and
 *  compare threads without opening each one. */
export interface ThreadSummary {
  id: string;
  state: ExecutionState;
  model: string;
  firstSeenAt: Date;
  updatedAt: Date;
  /** Runs on this thread, nested ones included. */
  runs: number;
  steps: number;
  tokens: UsageTotals;
  /** Summed run durations. Not wall time: nested runs overlap their parent. */
  durationMs: number;
  /** What started it — the first dispatched run's prompt. */
  prompt?: string | null;
  /** The parameters that started it, recorded on first sight (§2.9); falls
   *  back to the earliest dispatched run in the window. */
  startedWith?: ThreadStart | null;
}

/** A thread opened up: its runs, and every step across them in order. */
export interface ThreadDetail {
  thread: ThreadSummary;
  runs: RunRecord[];
  steps: StepRecord[];
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

const addTokens = (into: UsageTotals, from: UsageTotals) => {
  into.inputTokens += from.inputTokens;
  into.cachedInputTokens += from.cachedInputTokens;
  into.outputTokens += from.outputTokens;
  into.totalTokens += from.totalTokens;
};

function rollUp(thread: {
  id: string; state: ExecutionState; model: string; firstSeenAt: Date; updatedAt: Date;
  startedWith?: ThreadStart | null;
}, runs: RunRecord[]): ThreadSummary {
  const tokens = { ...EMPTY };
  let steps = 0;
  let durationMs = 0;
  for (const r of runs) {
    addTokens(tokens, r);
    steps += r.steps;
    durationMs += r.durationMs ?? 0;
  }
  // The dispatched run is the one a person started; a nested run's prompt is
  // a brief the model wrote.
  const root = runs.filter((r) => r.depth === 0).sort(
    (a, b) => a.startedAt.getTime() - b.startedAt.getTime(),
  )[0];
  const startedWith: ThreadStart | null =
    thread.startedWith ??
    (root
      ? {
          runId: root.id, agent: root.agent, model: root.model, at: root.startedAt,
          prompt: root.prompt ?? null, tokenBudget: root.tokenBudget ?? null,
          state: root.runState ?? null, providerOptions: root.providerOptions ?? null,
        }
      : null);
  return {
    ...thread, runs: runs.length, steps, tokens, durationMs,
    prompt: startedWith?.prompt ?? null, startedWith,
  };
}

/** Threads with their runs rolled up, newest activity first (§2.9). One pass
 *  over the window's runs rather than a query per thread. */
export async function listThreads(
  deps: RuntimePorts,
  filter: { state?: ExecutionState[]; since?: Date; limit?: number } = {},
): Promise<ThreadSummary[]> {
  const [threads, runs] = await Promise.all([
    deps.admin.threads.list({ ...filter, limit: filter.limit ?? DEFAULT_LIMIT }),
    deps.admin.runs.list({ since: filter.since, limit: 5_000 }),
  ]);
  const byThread = new Map<string, RunRecord[]>();
  for (const r of runs) {
    const list = byThread.get(r.threadId) ?? [];
    list.push(r);
    byThread.set(r.threadId, list);
  }
  return threads.map((t) => rollUp(t, byThread.get(t.id) ?? []));
}

/** One thread opened up: its runs and every step across them (§2.9). */
export async function getThread(
  deps: RuntimePorts,
  threadId: string,
): Promise<ThreadDetail | null> {
  const [runs, steps] = await Promise.all([
    deps.admin.runs.listByThread(threadId),
    deps.admin.steps.listByThread(threadId),
  ]);
  const rows = await deps.admin.threads.list({ limit: 5_000 });
  const thread = rows.find((t) => t.id === threadId);
  if (!thread && runs.length === 0) return null;

  const base = thread ?? {
    id: threadId,
    state: (runs[0]?.state ?? 'IDLE') as ExecutionState,
    model: runs[0]?.model ?? 'unknown',
    firstSeenAt: runs.at(-1)?.startedAt ?? new Date(),
    updatedAt: runs[0]?.startedAt ?? new Date(),
  };
  return { thread: rollUp(base, runs), runs, steps };
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
