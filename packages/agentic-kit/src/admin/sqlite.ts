import type { ExecutionState, NewRunRecord, RunPatch, RunRecord } from '../core/types.js';
import type {
  AdminStore, AdminThread, AdminThreadFilter, NewAdminThread,
  NewStepRecord, RunFilter, StepRecord,
} from '../ports/admin.js';
import type { SqliteLike } from '../adapters/sqlite.js';

const date = (n: number | null | undefined) => (n == null ? null : new Date(n));
const json = (v: unknown) => (v === undefined ? null : JSON.stringify(v));
const parse = (v: unknown) => (typeof v === 'string' ? JSON.parse(v) : (v ?? null));

const SCHEMA = `
CREATE TABLE IF NOT EXISTS agentic_runs (
  id TEXT PRIMARY KEY, threadId TEXT NOT NULL, parentRunId TEXT,
  depth INTEGER NOT NULL DEFAULT 0, agent TEXT NOT NULL, model TEXT NOT NULL,
  state TEXT NOT NULL DEFAULT 'RUNNING', stopReason TEXT, error TEXT,
  startedAt INTEGER NOT NULL, endedAt INTEGER, durationMs INTEGER, queuedMs INTEGER,
  attempts INTEGER NOT NULL DEFAULT 0, steps INTEGER NOT NULL DEFAULT 0,
  inputTokens INTEGER NOT NULL DEFAULT 0, cachedInputTokens INTEGER NOT NULL DEFAULT 0,
  outputTokens INTEGER NOT NULL DEFAULT 0, totalTokens INTEGER NOT NULL DEFAULT 0,
  result TEXT
);
CREATE INDEX IF NOT EXISTS agentic_runs_thread ON agentic_runs(threadId, startedAt);
CREATE INDEX IF NOT EXISTS agentic_runs_state ON agentic_runs(state, startedAt);
CREATE INDEX IF NOT EXISTS agentic_runs_parent ON agentic_runs(parentRunId);
CREATE TABLE IF NOT EXISTS agentic_threads (
  id TEXT PRIMARY KEY, state TEXT NOT NULL, model TEXT NOT NULL,
  firstSeenAt INTEGER NOT NULL, updatedAt INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS agentic_threads_state ON agentic_threads(state, updatedAt);
CREATE TABLE IF NOT EXISTS agentic_steps (
  runId TEXT NOT NULL, agentId TEXT, "index" INTEGER NOT NULL,
  durationMs INTEGER NOT NULL, finishReason TEXT NOT NULL,
  inputTokens INTEGER NOT NULL DEFAULT 0, cachedInputTokens INTEGER NOT NULL DEFAULT 0,
  outputTokens INTEGER NOT NULL DEFAULT 0, totalTokens INTEGER NOT NULL DEFAULT 0,
  tools TEXT, at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS agentic_steps_run ON agentic_steps(runId, "index");
`;

/** Operational history in SQLite — what `dev: true` uses (§2.9).
 *
 *  Tables are prefixed `agentic_` because this may well share a database with
 *  the caller's own schema; nothing here should collide with a table they own. */
export class SqliteAdminStore implements AdminStore {
  constructor(private readonly db: SqliteLike) {
    for (const stmt of SCHEMA.split(';')) {
      const sql = stmt.trim();
      if (sql) this.db.prepare(sql).run();
    }
  }

  private all(sql: string, ...p: unknown[]): any[] {
    return this.db.prepare(sql).all(...p) as any[];
  }
  private one(sql: string, ...p: unknown[]): any | null {
    return (this.db.prepare(sql).all(...p) as any[])[0] ?? null;
  }
  private write(sql: string, ...p: unknown[]): void {
    this.db.prepare(sql).run(...p);
  }

  private toRun = (r: any): RunRecord => ({
    id: r.id, threadId: r.threadId, parentRunId: r.parentRunId ?? null, depth: r.depth,
    agent: r.agent, model: r.model, state: r.state as ExecutionState,
    stopReason: r.stopReason ?? null, error: r.error ?? null,
    startedAt: new Date(r.startedAt), endedAt: date(r.endedAt),
    durationMs: r.durationMs ?? null, queuedMs: r.queuedMs ?? null,
    attempts: r.attempts, steps: r.steps,
    inputTokens: r.inputTokens, cachedInputTokens: r.cachedInputTokens,
    outputTokens: r.outputTokens, totalTokens: r.totalTokens,
    result: parse(r.result),
  });

  threads = {
    upsert: async (t: NewAdminThread) => {
      const now = Date.now();
      // firstSeenAt survives an update; everything else is overwritten.
      this.write(
        `INSERT INTO agentic_threads (id,state,model,firstSeenAt,updatedAt) VALUES (?,?,?,?,?)
         ON CONFLICT(id) DO UPDATE SET state = excluded.state,
           model = excluded.model, updatedAt = excluded.updatedAt`,
        t.id, t.state, t.model, now, now,
      );
    },
    countByState: async () =>
      Object.fromEntries(
        this.all('SELECT state, COUNT(*) AS n FROM agentic_threads GROUP BY state')
          .map((r) => [r.state, r.n]),
      ) as Partial<Record<ExecutionState, number>>,
    list: async (f: AdminThreadFilter): Promise<AdminThread[]> => {
      const where: string[] = [];
      const vals: unknown[] = [];
      if (f.state?.length) {
        where.push(`state IN (${f.state.map(() => '?').join(',')})`);
        vals.push(...f.state);
      }
      if (f.since) { where.push('updatedAt >= ?'); vals.push(f.since.getTime()); }
      return this.all(
        `SELECT * FROM agentic_threads${where.length ? ` WHERE ${where.join(' AND ')}` : ''}` +
          ' ORDER BY updatedAt DESC LIMIT ?',
        ...vals, f.limit ?? 100,
      ).map((r) => ({
        id: r.id, state: r.state as ExecutionState, model: r.model,
        firstSeenAt: new Date(r.firstSeenAt), updatedAt: new Date(r.updatedAt),
      }));
    },
  };

  runs = {
    start: async (run: NewRunRecord) => {
      const startedAt = Date.now();
      this.write(
        'INSERT INTO agentic_runs (id,threadId,parentRunId,depth,agent,model,state,startedAt) VALUES (?,?,?,?,?,?,?,?)',
        run.id, run.threadId, run.parentRunId ?? null, run.depth ?? 0,
        run.agent, run.model, 'RUNNING', startedAt,
      );
      return this.toRun({
        ...run, parentRunId: run.parentRunId ?? null, depth: run.depth ?? 0,
        state: 'RUNNING', startedAt, attempts: 0, steps: 0,
        inputTokens: 0, cachedInputTokens: 0, outputTokens: 0, totalTokens: 0,
      });
    },
    patch: async (runId: string, patch: RunPatch) => {
      const cols: string[] = [];
      const vals: unknown[] = [];
      for (const [k, v] of Object.entries(patch)) {
        if (v === undefined) continue;
        cols.push(`${k} = ?`);
        vals.push(v instanceof Date ? v.getTime() : k === 'result' ? json(v) : (v as unknown));
      }
      if (cols.length === 0) return;
      this.write(`UPDATE agentic_runs SET ${cols.join(', ')} WHERE id = ?`, ...vals, runId);
    },
    get: async (runId: string) => {
      const r = this.one('SELECT * FROM agentic_runs WHERE id = ?', runId);
      return r ? this.toRun(r) : null;
    },
    listByThread: async (threadId: string) =>
      this.all('SELECT * FROM agentic_runs WHERE threadId = ? ORDER BY startedAt DESC', threadId)
        .map(this.toRun),
    list: async (f: RunFilter) => {
      const where: string[] = [];
      const vals: unknown[] = [];
      if (f.state?.length) {
        where.push(`state IN (${f.state.map(() => '?').join(',')})`);
        vals.push(...f.state);
      }
      if (f.agent) { where.push('agent = ?'); vals.push(f.agent); }
      if (f.threadId) { where.push('threadId = ?'); vals.push(f.threadId); }
      if (f.since) { where.push('startedAt >= ?'); vals.push(f.since.getTime()); }
      if (f.until) { where.push('startedAt <= ?'); vals.push(f.until.getTime()); }
      return this.all(
        `SELECT * FROM agentic_runs${where.length ? ` WHERE ${where.join(' AND ')}` : ''}` +
          ' ORDER BY startedAt DESC LIMIT ?',
        ...vals, f.limit ?? 100,
      ).map(this.toRun);
    },
    countByState: async () =>
      Object.fromEntries(
        this.all('SELECT state, COUNT(*) AS n FROM agentic_runs GROUP BY state')
          .map((r) => [r.state, r.n]),
      ) as Partial<Record<ExecutionState, number>>,
  };

  steps = {
    record: async (s: NewStepRecord) => {
      this.write(
        'INSERT INTO agentic_steps (runId,agentId,"index",durationMs,finishReason,inputTokens,cachedInputTokens,outputTokens,totalTokens,tools,at) VALUES (?,?,?,?,?,?,?,?,?,?,?)',
        s.runId, s.agentId, s.index, s.durationMs, s.finishReason,
        s.inputTokens, s.cachedInputTokens, s.outputTokens, s.totalTokens,
        json(s.tools), (s.at ?? new Date()).getTime(),
      );
    },
    listByRun: async (runId: string): Promise<StepRecord[]> =>
      this.all('SELECT * FROM agentic_steps WHERE runId = ? ORDER BY "index"', runId)
        .map((r) => ({
          runId: r.runId, agentId: r.agentId ?? null, index: r.index,
          durationMs: r.durationMs, finishReason: r.finishReason,
          inputTokens: r.inputTokens, cachedInputTokens: r.cachedInputTokens,
          outputTokens: r.outputTokens, totalTokens: r.totalTokens,
          tools: parse(r.tools) ?? [], at: new Date(r.at),
        })),
  };
}
