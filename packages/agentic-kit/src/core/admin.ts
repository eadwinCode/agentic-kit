import type { RuntimePorts } from '../ports/runtime.js';
import type { RunFilter } from '../ports/storage.js';
import type {
  AgentEvent,
  ExecutionState,
  RunRecord,
  UsageTotals,
} from './types.js';

/** One completed loop iteration, read back off the event log (§2.9). */
export interface StepMarker {
  runId?: string;
  /** Which stream ran it — null is the main agent (§2.7). */
  agentId: string | null;
  index: number;
  durationMs: number;
  finishReason: string;
  tokens: UsageTotals;
  tools: string[];
  at: Date;
}

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
  threads: Partial<Record<ExecutionState, number>>;
  runs: RunStats;
  /** Runs still in flight, newest first. */
  active: RunRecord[];
}

/** Everything about one run, assembled for a timeline view. */
export interface RunDetail {
  run: RunRecord;
  steps: StepMarker[];
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

/** Thrown when the configured Storage does not provide the optional admin
 *  queries (§2.9). Deliberately explicit: silently returning nothing would
 *  look like "no traffic" rather than "not wired up". */
export class AdminUnavailableError extends Error {
  constructor() {
    super(
      'This Storage adapter does not implement the optional `admin` queries. ' +
        'Cross-thread views need them; per-thread reads work without.',
    );
    this.name = 'AdminUnavailableError';
  }
}

const requireAdmin = (deps: RuntimePorts) => {
  const admin = deps.storage.admin;
  if (!admin) throw new AdminUnavailableError();
  return admin;
};

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
  return requireAdmin(deps).listRuns({ ...filter, limit: filter.limit ?? DEFAULT_LIMIT });
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
  const admin = requireAdmin(deps);
  const [threads, recent, active] = await Promise.all([
    admin.countThreadsByState(),
    listRuns(deps, { since: range.since, limit: 1_000 }),
    admin.listRuns({ state: ['RUNNING', 'WAITING_FOR_INPUT'], limit: 50 }),
  ]);
  return { threads, runs: summarise(recent), active };
}

/** Step markers for a thread, newest run last. Reads the event log rather than
 *  a step table — the log is already written and indexed by (thread, type). */
export async function listSteps(
  deps: RuntimePorts,
  threadId: string,
  runId?: string,
): Promise<StepMarker[]> {
  const events = await deps.storage.events.listByType(threadId, 'STEP_FINISHED');
  return events
    .map((e) => {
      const p = (e.payload ?? {}) as Partial<StepMarker> & { tokens?: UsageTotals };
      return {
        runId: p.runId,
        agentId: p.agentId ?? null,
        index: p.index ?? 0,
        durationMs: p.durationMs ?? 0,
        finishReason: p.finishReason ?? '',
        tokens: p.tokens ?? { ...EMPTY },
        tools: p.tools ?? [],
        at: new Date(e.createdAt),
      };
    })
    .filter((s) => !runId || s.runId === runId);
}

export async function getRun(deps: RuntimePorts, runId: string): Promise<RunDetail | null> {
  const run = await deps.storage.runs.get(runId);
  if (!run) return null;

  const [steps, subagents, events] = await Promise.all([
    listSteps(deps, run.threadId, runId),
    deps.storage.runs.listByThread(run.threadId),
    deps.storage.events.listSince(run.threadId, -1),
  ]);

  const from = new Date(run.startedAt).getTime();
  const to = run.endedAt ? new Date(run.endedAt).getTime() : Infinity;
  return {
    run,
    steps,
    // Its children: same table, distinguished by depth (§2.7).
    subagents: subagents.filter((c) => c.parentRunId === runId),
    // CHUNK is the token firehose; a timeline wants the spine.
    events: events.filter((e: AgentEvent) => {
      if (e.type === 'CHUNK' || e.type === 'SUBAGENT_CHUNK') return false;
      const at = new Date(e.createdAt).getTime();
      return at >= from && at <= to;
    }),
  };
}
