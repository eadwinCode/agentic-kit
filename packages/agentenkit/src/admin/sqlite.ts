import type { ExecutionState, NewRunRecord, RunPatch, RunRecord } from '../core/types.js';
import type {
  AdminStore, AdminThread, AdminThreadFilter, NewAdminThread,
  NewStepRecord, RunFilter, StepRecord, ThreadStart,
} from '../ports/admin.js';
import type { SqliteLike } from '../adapters/sqlite.js';
import { gatedAdminStore, runMigrations, type MigrationDriver } from './migrations/runner.js';
import { dialect, migrations } from './migrations/sqlite/index.js';

const date = (n: number | null | undefined) => (n == null ? null : new Date(n));
/** The stored ThreadStart, with its `at` back as a Date. */
const parseStart = (v: unknown): ThreadStart | null => {
  const raw = typeof v === 'string' ? JSON.parse(v) : (v ?? null);
  return raw ? { ...raw, at: new Date(raw.at) } : null;
};
const json = (v: unknown) => (v === undefined ? null : JSON.stringify(v));
const parse = (v: unknown) => (typeof v === 'string' ? JSON.parse(v) : (v ?? null));

/** Operational history in SQLite — what `dev: true` uses (§2.9).
 *
 *  Tables are prefixed `agentic_` because this may well share a database with
 *  the caller's own schema; nothing here should collide with a table they own.
 *
 *  The schema is applied by the migrator, not here. Use `SqliteAdminStore.open`
 *  unless you are driving migrations yourself: it returns a store that waits
 *  for the schema before its first call. */
export class SqliteAdminStore implements AdminStore {
  // Private: a store built without its migration would answer every query
  // with "no such table". `open` is the only way in.
  private constructor(private readonly db: SqliteLike) {}

  /** The store `setupAgentCore` uses: schema migration running behind it, and
   *  every call gated on it (§2.9).
   *
   *  Opening the database stays synchronous — a file that cannot be opened is
   *  a configuration problem worth failing on at startup. Only the schema
   *  moves to the background, so a service starts at the same speed whether or
   *  not it has migrating to do. */
  static open(db: SqliteLike, log?: { error(message: string, ...rest: unknown[]): void }): AdminStore {
    const store = new SqliteAdminStore(db);
    const ready = runMigrations(store.driver(), dialect, migrations).catch((err) => {
      // Loud, because everything downstream of this is silent: admin writes
      // are best effort, so a failed migration shows up as a dashboard with
      // nothing in it rather than as an error.
      (log ?? console).error('admin migrations failed', err);
      throw err;
    });
    return gatedAdminStore(store, ready);
  }

  /** The two calls the migrator needs, over this store's handle. */
  private driver(): MigrationDriver {
    return {
      exec: async (sql, params = []) => {
        this.db.prepare(sql).run(...(params as unknown[]));
      },
      rows: async (sql, params = []) =>
        this.db.prepare(sql).all(...(params as unknown[])) as Array<Record<string, unknown>>,
    };
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
    runState: parse(r.runState), providerOptions: parse(r.providerOptions),
  });

  threads = {
    upsert: async (t: NewAdminThread) => {
      const now = Date.now();
      // firstSeenAt and startedWith survive an update; the rest is overwritten.
      this.write(
        `INSERT INTO agentic_threads (id,state,model,firstSeenAt,updatedAt,startedWith) VALUES (?,?,?,?,?,?)
         ON CONFLICT(id) DO UPDATE SET state = excluded.state,
           model = excluded.model, updatedAt = excluded.updatedAt,
           startedWith = COALESCE(agentic_threads.startedWith, excluded.startedWith)`,
        // SQL NULL when absent, never the string "null": COALESCE must see
        // the column as empty for the first sight to land.
        t.id, t.state, t.model, now, now, t.startedWith ? json(t.startedWith) : null,
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
        startedWith: parseStart(r.startedWith),
      }));
    },
  };

  runs = {
    start: async (run: NewRunRecord) => {
      const startedAt = Date.now();
      this.write(
        'INSERT INTO agentic_runs (id,threadId,parentRunId,depth,agent,model,state,startedAt,prompt,tokenBudget,runState,providerOptions) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)',
        run.id, run.threadId, run.parentRunId ?? null, run.depth ?? 0,
        run.agent, run.model, 'RUNNING', startedAt,
        run.prompt ?? null, run.tokenBudget ?? null, json(run.runState), json(run.providerOptions),
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
