import type { ExecutionState, NewRunRecord, RunPatch, RunRecord } from '../core/types.js';
import type {
  AdminStore, AdminThread, AdminThreadFilter, NewAdminThread,
  NewStepRecord, RunFilter, StepRecord,
} from '../ports/admin.js';

/** Operational history held in memory — tests, and the default when nothing
 *  durable is configured. Loses everything on restart, which is exactly what
 *  the SQLite and Postgres stores exist to fix. */
export class MemoryAdminStore implements AdminStore {
  readonly runRows = new Map<string, RunRecord>();
  readonly stepRows: StepRecord[] = [];

  readonly threadRows = new Map<string, AdminThread>();
  /** Who holds each run's settle claim. */
  private readonly settleTokens = new Map<string, string>();

  threads = {
    upsert: async (t: NewAdminThread) => {
      const now = new Date();
      const prior = this.threadRows.get(t.id);
      this.threadRows.set(t.id, {
        ...t,
        // First sight only: a later upsert never replaces what started it.
        startedWith: prior?.startedWith ?? t.startedWith ?? null,
        firstSeenAt: prior?.firstSeenAt ?? now,
        updatedAt: now,
      });
    },
    countByState: async () => {
      const out: Partial<Record<ExecutionState, number>> = {};
      for (const t of this.threadRows.values()) out[t.state] = (out[t.state] ?? 0) + 1;
      return out;
    },
    list: async (f: AdminThreadFilter) => {
      let rows = [...this.threadRows.values()];
      if (f.state?.length) rows = rows.filter((t) => f.state!.includes(t.state));
      if (f.since) rows = rows.filter((t) => t.updatedAt >= f.since!);
      return rows
        .sort((a, b) => b.updatedAt.getTime() - a.updatedAt.getTime())
        .slice(0, f.limit ?? 100);
    },
  };

  runs = {
    start: async (r: NewRunRecord) => {
      const rec: RunRecord = {
        parentRunId: null, depth: 0, prompt: null, tokenBudget: null, runState: null, providerOptions: null,
        enqueuedAt: null, ...r,
        state: r.state ?? 'RUNNING', startedAt: new Date(), steps: 0, attempts: 0,
        inputTokens: 0, cachedInputTokens: 0, outputTokens: 0, totalTokens: 0,
      };
      this.runRows.set(rec.id, rec);
      return rec;
    },
    patch: async (runId: string, patch: RunPatch) => {
      const cur = this.runRows.get(runId);
      if (cur) this.runRows.set(runId, { ...cur, ...patch });
    },
    get: async (runId: string) => this.runRows.get(runId) ?? null,
    listByThread: async (threadId: string) =>
      [...this.runRows.values()]
        .filter((r) => r.threadId === threadId)
        .sort((a, b) => b.startedAt.getTime() - a.startedAt.getTime()),
    list: async (f: RunFilter) => {
      let rows = [...this.runRows.values()];
      if (f.state?.length) rows = rows.filter((r) => f.state!.includes(r.state));
      if (f.agent) rows = rows.filter((r) => r.agent === f.agent);
      if (f.threadId) rows = rows.filter((r) => r.threadId === f.threadId);
      if (f.since) rows = rows.filter((r) => r.startedAt >= f.since!);
      if (f.until) rows = rows.filter((r) => r.startedAt <= f.until!);
      if (f.unsettled) rows = rows.filter((r) => r.endedAt && !r.settledAt);
      if (f.depth !== undefined) rows = rows.filter((r) => r.depth === f.depth);
      const c = f.before;
      if (c) {
        rows = rows.filter((r) =>
          r.startedAt.getTime() < c.startedAt.getTime() ||
          (r.startedAt.getTime() === c.startedAt.getTime() && r.id < c.id));
      }
      return rows
        .sort((a, b) => b.startedAt.getTime() - a.startedAt.getTime() || (a.id < b.id ? 1 : a.id > b.id ? -1 : 0))
        .slice(0, f.limit ?? 100);
    },
    claimSettle: async (runId: string, token: string, staleBefore: Date) => {
      const cur = this.runRows.get(runId);
      if (!cur || cur.settledAt || (cur.settlingAt && cur.settlingAt >= staleBefore)) return false;
      this.runRows.set(runId, { ...cur, settlingAt: new Date() });
      this.settleTokens.set(runId, token);
      return true;
    },
    endSettle: async (runId: string, token: string, settled: boolean) => {
      const cur = this.runRows.get(runId);
      if (!cur || this.settleTokens.get(runId) !== token) return;
      this.settleTokens.delete(runId);
      this.runRows.set(runId, { ...cur, settlingAt: null, ...(settled ? { settledAt: new Date() } : {}) });
    },
    countByState: async () => {
      const out: Partial<Record<ExecutionState, number>> = {};
      for (const r of this.runRows.values()) out[r.state] = (out[r.state] ?? 0) + 1;
      return out;
    },
  };

  steps = {
    record: async (s: NewStepRecord) => {
      this.stepRows.push({ ...s, at: s.at ?? new Date() });
    },
    listByRun: async (runId: string) =>
      this.stepRows.filter((s) => s.runId === runId).sort((a, b) => a.index - b.index),
    listByThread: async (threadId: string) =>
      this.stepRows
        .filter((s) => s.threadId === threadId)
        .sort((a, b) => a.at.getTime() - b.at.getTime()),
  };
}
