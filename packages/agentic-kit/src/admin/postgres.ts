import type { ExecutionState, NewRunRecord, RunPatch, RunRecord } from '../core/types.js';
import type {
  AdminStore, AdminThread, AdminThreadFilter, NewAdminThread,
  NewStepRecord, RunFilter, StepRecord,
} from '../ports/admin.js';

/** Minimal structural type over a Postgres client. Both `pg`'s `Pool` and its
 *  `Client` satisfy it, so the package never imports the driver (§3.4). */
export interface PgLike {
  query(text: string, params?: unknown[]): Promise<{ rows: any[] }>;
}

/** Open a `pg` Pool from a connection string. Dynamically imported so the
 *  driver only loads for callers who actually use Postgres, and marked
 *  webpackIgnore so a bundler leaves it as a runtime import. */
export async function openPostgres(url: string): Promise<PgLike> {
  const specifier = 'pg';
  try {
    const pg: any = await import(/* webpackIgnore: true */ /* @vite-ignore */ specifier);
    const Pool = pg.Pool ?? pg.default?.Pool;
    return new Pool({ connectionString: url });
  } catch (err) {
    throw new Error(
      'Could not load `pg`. Install it (`bun add pg`) to use the Postgres ' +
        `admin store. (${err instanceof Error ? err.message : String(err)})`,
    );
  }
}

// Tables are prefixed `agentic_` because this frequently shares a database
// with the caller's own schema — AGENTIC_KIT_ADMIN_DATABASE_URL may well point
// at the database they already have. Nothing here should collide with theirs.
const SCHEMA = [
  `CREATE TABLE IF NOT EXISTS agentic_threads (
     id TEXT PRIMARY KEY, state TEXT NOT NULL, model TEXT NOT NULL,
     "firstSeenAt" TIMESTAMPTZ NOT NULL DEFAULT now(),
     "updatedAt" TIMESTAMPTZ NOT NULL DEFAULT now()
   )`,
  `CREATE INDEX IF NOT EXISTS agentic_threads_state ON agentic_threads(state, "updatedAt")`,
  `CREATE TABLE IF NOT EXISTS agentic_runs (
     id TEXT PRIMARY KEY, "threadId" TEXT NOT NULL, "parentRunId" TEXT,
     depth INT NOT NULL DEFAULT 0, agent TEXT NOT NULL, model TEXT NOT NULL,
     state TEXT NOT NULL DEFAULT 'RUNNING', "stopReason" TEXT, error TEXT,
     "startedAt" TIMESTAMPTZ NOT NULL DEFAULT now(), "endedAt" TIMESTAMPTZ,
     "durationMs" INT, "queuedMs" INT,
     attempts INT NOT NULL DEFAULT 0, steps INT NOT NULL DEFAULT 0,
     "inputTokens" INT NOT NULL DEFAULT 0, "cachedInputTokens" INT NOT NULL DEFAULT 0,
     "outputTokens" INT NOT NULL DEFAULT 0, "totalTokens" INT NOT NULL DEFAULT 0,
     result JSONB
   )`,
  `CREATE INDEX IF NOT EXISTS agentic_runs_thread ON agentic_runs("threadId", "startedAt" DESC)`,
  `CREATE INDEX IF NOT EXISTS agentic_runs_state ON agentic_runs(state, "startedAt" DESC)`,
  `CREATE INDEX IF NOT EXISTS agentic_runs_parent ON agentic_runs("parentRunId")`,
  `CREATE TABLE IF NOT EXISTS agentic_steps (
     "runId" TEXT NOT NULL, "agentId" TEXT, "index" INT NOT NULL,
     "durationMs" INT NOT NULL, "finishReason" TEXT NOT NULL,
     "inputTokens" INT NOT NULL DEFAULT 0, "cachedInputTokens" INT NOT NULL DEFAULT 0,
     "outputTokens" INT NOT NULL DEFAULT 0, "totalTokens" INT NOT NULL DEFAULT 0,
     tools JSONB, at TIMESTAMPTZ NOT NULL DEFAULT now()
   )`,
  `CREATE INDEX IF NOT EXISTS agentic_steps_run ON agentic_steps("runId", "index")`,
];

const toRun = (r: any): RunRecord => ({
  id: r.id, threadId: r.threadId, parentRunId: r.parentRunId ?? null, depth: r.depth,
  agent: r.agent, model: r.model, state: r.state as ExecutionState,
  stopReason: r.stopReason ?? null, error: r.error ?? null,
  startedAt: new Date(r.startedAt), endedAt: r.endedAt ? new Date(r.endedAt) : null,
  durationMs: r.durationMs ?? null, queuedMs: r.queuedMs ?? null,
  attempts: r.attempts, steps: r.steps,
  inputTokens: r.inputTokens, cachedInputTokens: r.cachedInputTokens,
  outputTokens: r.outputTokens, totalTokens: r.totalTokens,
  result: r.result ?? null,
});

/** Operational history in Postgres — the production store (§2.9), reached
 *  through AGENTIC_KIT_ADMIN_DATABASE_URL. Point it at its own database or the
 *  one you already have; the prefix keeps them apart either way. */
export class PostgresAdminStore implements AdminStore {
  private constructor(private readonly db: PgLike) {}

  /** Creates the tables if they are missing, then returns the store. A factory
   *  rather than a constructor because that first step is asynchronous — and
   *  `setupAgentCore` awaits its store precisely so this can fail at startup. */
  static async connect(db: PgLike): Promise<PostgresAdminStore> {
    for (const stmt of SCHEMA) await db.query(stmt);
    return new PostgresAdminStore(db);
  }

  threads = {
    upsert: async (t: NewAdminThread) => {
      await this.db.query(
        `INSERT INTO agentic_threads (id, state, model) VALUES ($1, $2, $3)
         ON CONFLICT (id) DO UPDATE
           SET state = EXCLUDED.state, model = EXCLUDED.model, "updatedAt" = now()`,
        [t.id, t.state, t.model],
      );
    },
    countByState: async () => {
      const { rows } = await this.db.query(
        'SELECT state, COUNT(*)::int AS n FROM agentic_threads GROUP BY state',
      );
      return Object.fromEntries(rows.map((r) => [r.state, r.n])) as Partial<
        Record<ExecutionState, number>
      >;
    },
    list: async (f: AdminThreadFilter): Promise<AdminThread[]> => {
      const where: string[] = [];
      const vals: unknown[] = [];
      if (f.state?.length) {
        where.push(`state = ANY($${vals.push(f.state)})`);
      }
      if (f.since) where.push(`"updatedAt" >= $${vals.push(f.since)}`);
      const { rows } = await this.db.query(
        `SELECT * FROM agentic_threads${where.length ? ` WHERE ${where.join(' AND ')}` : ''}
         ORDER BY "updatedAt" DESC LIMIT $${vals.push(f.limit ?? 100)}`,
        vals,
      );
      return rows.map((r) => ({
        id: r.id, state: r.state as ExecutionState, model: r.model,
        firstSeenAt: new Date(r.firstSeenAt), updatedAt: new Date(r.updatedAt),
      }));
    },
  };

  runs = {
    start: async (run: NewRunRecord) => {
      const { rows } = await this.db.query(
        `INSERT INTO agentic_runs (id, "threadId", "parentRunId", depth, agent, model, state)
         VALUES ($1, $2, $3, $4, $5, $6, 'RUNNING') RETURNING *`,
        [run.id, run.threadId, run.parentRunId ?? null, run.depth ?? 0, run.agent, run.model],
      );
      return toRun(rows[0]);
    },
    patch: async (runId: string, patch: RunPatch) => {
      const sets: string[] = [];
      const vals: unknown[] = [];
      for (const [k, v] of Object.entries(patch)) {
        if (v === undefined) continue;
        // JSON columns need the value serialised; everything else pg handles.
        sets.push(`"${k}" = $${vals.push(k === 'result' ? JSON.stringify(v) : v)}`);
      }
      if (sets.length === 0) return;
      await this.db.query(
        `UPDATE agentic_runs SET ${sets.join(', ')} WHERE id = $${vals.push(runId)}`,
        vals,
      );
    },
    get: async (runId: string) => {
      const { rows } = await this.db.query('SELECT * FROM agentic_runs WHERE id = $1', [runId]);
      return rows[0] ? toRun(rows[0]) : null;
    },
    listByThread: async (threadId: string) => {
      const { rows } = await this.db.query(
        'SELECT * FROM agentic_runs WHERE "threadId" = $1 ORDER BY "startedAt" DESC',
        [threadId],
      );
      return rows.map(toRun);
    },
    list: async (f: RunFilter) => {
      const where: string[] = [];
      const vals: unknown[] = [];
      if (f.state?.length) where.push(`state = ANY($${vals.push(f.state)})`);
      if (f.agent) where.push(`agent = $${vals.push(f.agent)}`);
      if (f.threadId) where.push(`"threadId" = $${vals.push(f.threadId)}`);
      if (f.since) where.push(`"startedAt" >= $${vals.push(f.since)}`);
      if (f.until) where.push(`"startedAt" <= $${vals.push(f.until)}`);
      const { rows } = await this.db.query(
        `SELECT * FROM agentic_runs${where.length ? ` WHERE ${where.join(' AND ')}` : ''}
         ORDER BY "startedAt" DESC LIMIT $${vals.push(f.limit ?? 100)}`,
        vals,
      );
      return rows.map(toRun);
    },
    countByState: async () => {
      const { rows } = await this.db.query(
        'SELECT state, COUNT(*)::int AS n FROM agentic_runs GROUP BY state',
      );
      return Object.fromEntries(rows.map((r) => [r.state, r.n])) as Partial<
        Record<ExecutionState, number>
      >;
    },
  };

  steps = {
    record: async (s: NewStepRecord) => {
      await this.db.query(
        `INSERT INTO agentic_steps
           ("runId", "agentId", "index", "durationMs", "finishReason",
            "inputTokens", "cachedInputTokens", "outputTokens", "totalTokens", tools, at)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`,
        [
          s.runId, s.agentId, s.index, s.durationMs, s.finishReason,
          s.inputTokens, s.cachedInputTokens, s.outputTokens, s.totalTokens,
          JSON.stringify(s.tools), s.at ?? new Date(),
        ],
      );
    },
    listByRun: async (runId: string): Promise<StepRecord[]> => {
      const { rows } = await this.db.query(
        'SELECT * FROM agentic_steps WHERE "runId" = $1 ORDER BY "index"',
        [runId],
      );
      return rows.map((r) => ({
        runId: r.runId, agentId: r.agentId ?? null, index: r.index,
        durationMs: r.durationMs, finishReason: r.finishReason,
        inputTokens: r.inputTokens, cachedInputTokens: r.cachedInputTokens,
        outputTokens: r.outputTokens, totalTokens: r.totalTokens,
        tools: r.tools ?? [], at: new Date(r.at),
      }));
    },
  };
}
