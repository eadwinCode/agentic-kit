import type { ExecutionState, NewRunRecord, RunPatch, RunRecord } from '../core/types.js';
import type {
  AdminStore, AdminThread, AdminThreadFilter, NewAdminThread,
  NewStepRecord, RunDeltas, RunFilter, RunTotals, StepRecord,
} from '../ports/admin.js';
import { addRunTotals, emptyRunTotals } from './shared.js';

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
    get: async (threadId: string) => this.threadRows.get(threadId) ?? null,
    delete: async (threadId: string) => {
      this.threadRows.delete(threadId);
      for (const [id, r] of this.runRows) {
        if (r.threadId === threadId) {
          this.runRows.delete(id);
          this.settleTokens.delete(id);
        }
      }
      for (let i = this.stepRows.length - 1; i >= 0; i--) {
        if (this.stepRows[i]!.threadId === threadId) this.stepRows.splice(i, 1);
      }
    },
  };

  /** The RunFilter, limit aside. */
  private matches(r: RunRecord, f: RunFilter): boolean {
    if (f.state?.length && !f.state.includes(r.state)) return false;
    if (f.agent && r.agent !== f.agent) return false;
    if (f.threadId && r.threadId !== f.threadId) return false;
    if (f.threadIds?.length && !f.threadIds.includes(r.threadId)) return false;
    if (f.since && r.startedAt < f.since) return false;
    if (f.until && r.startedAt > f.until) return false;
    if (f.unsettled && (!r.endedAt || r.settledAt)) return false;
    if (f.depth !== undefined && r.depth !== f.depth) return false;
    const c = f.before;
    if (c && !(r.startedAt.getTime() < c.startedAt.getTime() ||
      (r.startedAt.getTime() === c.startedAt.getTime() && r.id < c.id))) return false;
    return true;
  }

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
      const rows = [...this.runRows.values()].filter((r) => this.matches(r, f));
      return rows
        .sort((a, b) => b.startedAt.getTime() - a.startedAt.getTime() || (a.id < b.id ? 1 : a.id > b.id ? -1 : 0))
        .slice(0, f.limit ?? 100);
    },
    increment: async (runId: string, d: RunDeltas) => {
      const cur = this.runRows.get(runId);
      if (!cur) return;
      this.runRows.set(runId, {
        ...cur,
        steps: cur.steps + d.steps,
        inputTokens: cur.inputTokens + d.inputTokens,
        cachedInputTokens: cur.cachedInputTokens + d.cachedInputTokens,
        outputTokens: cur.outputTokens + d.outputTokens,
        totalTokens: cur.totalTokens + d.totalTokens,
      });
    },
    totals: async (f: RunFilter): Promise<RunTotals> => {
      const out = emptyRunTotals();
      for (const r of this.runRows.values()) {
        if (!this.matches(r, { ...f, before: undefined })) continue;
        addRunTotals(out, r.state, r.stopReason, 1, r);
      }
      return out;
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
