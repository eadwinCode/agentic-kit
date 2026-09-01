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
  result TEXT, prompt TEXT, tokenBudget INTEGER, runState TEXT
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
  runId TEXT NOT NULL, threadId TEXT, agentId TEXT, "index" INTEGER NOT NULL,
  durationMs INTEGER NOT NULL, finishReason TEXT NOT NULL,
  inputTokens INTEGER NOT NULL DEFAULT 0, cachedInputTokens INTEGER NOT NULL DEFAULT 0,
  outputTokens INTEGER NOT NULL DEFAULT 0, totalTokens INTEGER NOT NULL DEFAULT 0,
  tools TEXT, text TEXT, toolCalls TEXT, at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS agentic_steps_run ON agentic_steps(runId, "index");
CREATE INDEX IF NOT EXISTS agentic_steps_thread ON agentic_steps(threadId, at);
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
    // CREATE TABLE IF NOT EXISTS never adds a column to a database that already
    // exists, so newer fields are added separately. SQLite has no
    // ADD COLUMN IF NOT EXISTS, hence the check.
    const have = new Set(
      (this.db.prepare('PRAGMA table_info(agentic_steps)').all() as any[]).map((c) => c.name),
    );
    for (const col of ['text', 'toolCalls', 'threadId']) {
      if (!have.has(col)) this.db.prepare(`ALTER TABLE agentic_steps ADD COLUMN ${col} TEXT`).run();
    }
    const runCols = new Set(
      (this.db.prepare('PRAGMA table_info(agentic_runs)').all() as any[]).map((c) => c.name),
    );
    for (const [col, type] of [['prompt', 'TEXT'], ['tokenBudget', 'INTEGER'], ['runState', 'TEXT']]) {
      if (!runCols.has(col)) {
        this.db.prepare(`ALTER TABLE agentic_runs ADD COLUMN ${col} ${type}`).run();
      }
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
    prompt: r.prompt ?? null, tokenBudget: r.tokenBudget ?? null,
    runState: parse(r.runState),
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
        'INSERT INTO agentic_runs (id,threadId,parentRunId,depth,agent,model,state,startedAt,prompt,tokenBudget,runState) VALUES (?,?,?,?,?,?,?,?,?,?,?)',
        run.id, run.threadId, run.parentRunId ?? null, run.depth ?? 0,
        run.agent, run.model, 'RUNNING', startedAt,
        run.prompt ?? null, run.tokenBudget ?? null, json(run.runState),
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
        'INSERT INTO agentic_steps (runId,threadId,agentId,"index",durationMs,finishReason,inputTokens,cachedInputTokens,outputTokens,totalTokens,tools,text,toolCalls,at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
        s.runId, s.threadId, s.agentId, s.index, s.durationMs, s.finishReason,
        s.inputTokens, s.cachedInputTokens, s.outputTokens, s.totalTokens,
        json(s.tools), s.text ?? null, json(s.toolCalls),
        (s.at ?? new Date()).getTime(),
      );
    },
    listByRun: async (runId: string): Promise<StepRecord[]> =>
      this.all('SELECT * FROM agentic_steps WHERE runId = ? ORDER BY "index"', runId)
        .map(this.toStep),
    listByThread: async (threadId: string): Promise<StepRecord[]> =>
      this.all('SELECT * FROM agentic_steps WHERE threadId = ? ORDER BY at', threadId)
        .map(this.toStep),
  };

  private toStep = (r: any): StepRecord => ({
          runId: r.runId, threadId: r.threadId ?? '', agentId: r.agentId ?? null, index: r.index,
          durationMs: r.durationMs, finishReason: r.finishReason,
          inputTokens: r.inputTokens, cachedInputTokens: r.cachedInputTokens,
          outputTokens: r.outputTokens, totalTokens: r.totalTokens,
          tools: parse(r.tools) ?? [], text: r.text ?? null,
          toolCalls: parse(r.toolCalls) ?? [], at: new Date(r.at),
  });
}
