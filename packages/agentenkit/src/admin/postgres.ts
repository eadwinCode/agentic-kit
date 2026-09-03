import type { ExecutionState, NewRunRecord, RunPatch, RunRecord } from '../core/types.js';
import type {
  AdminStore, AdminThread, AdminThreadFilter, NewAdminThread,
  NewStepRecord, RunFilter, StepRecord,
} from '../ports/admin.js';
import { gatedAdminStore, runMigrations, type MigrationDriver } from './migrations/runner.js';
import { dialect, migrations } from './migrations/postgres/index.js';

/** Minimal structural type over a Postgres client. Both `pg`'s `Pool` and its
 *  `Client` satisfy it, so the package never imports the driver (§3.4). */
export interface PgLike {
  query(text: string, params?: unknown[]): Promise<{ rows: any[] }>;
  /** Checks out ONE connection. `pg`'s Pool has it; a bare Client does not,
   *  and does not need it — it already is one connection.
   *
   *  The migrator needs this: a pool hands each query whichever connection is
   *  free, so BEGIN on one and DDL on another is not a transaction at all. */
  connect?(): Promise<PgClientLike>;
}

/** One checked-out connection, which must be handed back. */
export interface PgClientLike {
  query(text: string, params?: unknown[]): Promise<{ rows: any[] }>;
  release(): void;
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

/** Run the migrations on ONE connection, checked out for the length of the
 *  transaction and handed back after. */
async function migrate(db: PgLike): Promise<void> {
  const client = db.connect ? await db.connect() : null;
  const handle = client ?? db;
  const driver: MigrationDriver = {
    exec: async (sql, params = []) => {
      await handle.query(sql, params as unknown[]);
    },
    rows: async (sql, params = []) =>
      (await handle.query(sql, params as unknown[])).rows as Array<Record<string, unknown>>,
  };
  try {
    await runMigrations(driver, dialect, migrations);
  } finally {
    client?.release();
  }
}

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
  prompt: r.prompt ?? null, tokenBudget: r.tokenBudget ?? null,
  runState: r.runState ?? null, providerOptions: r.providerOptions ?? null,
});

/** Operational history in Postgres — the production store (§2.9), reached
 *  through AGENTIC_KIT_ADMIN_DATABASE_URL. Point it at its own database or the
 *  one you already have; the prefix keeps them apart either way. */
export class PostgresAdminStore implements AdminStore {
  private constructor(private readonly db: PgLike) {}

  /** The store with its schema migration already running behind it (§2.9).
   *
   *  Connecting stays synchronous — a URL that cannot be reached is a
   *  configuration problem worth failing on at startup. Only the schema moves
   *  to the background, so a service starts at the same speed whether or not
   *  it has migrating to do, and the returned store waits for it before its
   *  first call.
   *
   *  Several workers starting at once is expected and safe: each migration
   *  runs under a transaction-scoped advisory lock, so they queue rather than
   *  race. */
  static connect(
    db: PgLike,
    log?: { error(message: string, ...rest: unknown[]): void },
  ): AdminStore {
    const store = new PostgresAdminStore(db);
    const ready = migrate(db).catch((err) => {
      // Loud, because everything downstream of this is silent: admin writes
      // are best effort, so a failed migration shows up as a dashboard with
      // nothing in it rather than as an error.
      (log ?? console).error('admin migrations failed', err);
      throw err;
    });
    return gatedAdminStore(store, ready);
  }

  threads = {
    upsert: async (t: NewAdminThread) => {
      await this.db.query(
        `INSERT INTO agentic_threads (id, state, model, "startedWith") VALUES ($1, $2, $3, $4)
         ON CONFLICT (id) DO UPDATE
           SET state = EXCLUDED.state, model = EXCLUDED.model, "updatedAt" = now(),
               "startedWith" = COALESCE(agentic_threads."startedWith", EXCLUDED."startedWith")`,
        [t.id, t.state, t.model, t.startedWith ? JSON.stringify(t.startedWith) : null],
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
        startedWith: r.startedWith ? { ...r.startedWith, at: new Date(r.startedWith.at) } : null,
      }));
    },
  };

  runs = {
    start: async (run: NewRunRecord) => {
      const { rows } = await this.db.query(
        `INSERT INTO agentic_runs
           (id, "threadId", "parentRunId", depth, agent, model, state,
            prompt, "tokenBudget", "runState", "providerOptions")
         VALUES ($1, $2, $3, $4, $5, $6, 'RUNNING', $7, $8, $9, $10) RETURNING *`,
        [
          run.id, run.threadId, run.parentRunId ?? null, run.depth ?? 0,
          run.agent, run.model, run.prompt ?? null, run.tokenBudget ?? null,
          run.runState ? JSON.stringify(run.runState) : null,
          run.providerOptions ? JSON.stringify(run.providerOptions) : null,
        ],
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
           ("runId", "threadId", "agentId", "index", "durationMs", "finishReason",
            "inputTokens", "cachedInputTokens", "outputTokens", "totalTokens",
            tools, text, "toolCalls", at)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)`,
        [
          s.runId, s.threadId, s.agentId, s.index, s.durationMs, s.finishReason,
          s.inputTokens, s.cachedInputTokens, s.outputTokens, s.totalTokens,
          JSON.stringify(s.tools), s.text ?? null,
          s.toolCalls ? JSON.stringify(s.toolCalls) : null,
          s.at ?? new Date(),
        ],
      );
    },
    listByRun: async (runId: string): Promise<StepRecord[]> => {
      const { rows } = await this.db.query(
        'SELECT * FROM agentic_steps WHERE "runId" = $1 ORDER BY "index"',
        [runId],
      );
      return rows.map(toStep);
    },
    listByThread: async (threadId: string): Promise<StepRecord[]> => {
      const { rows } = await this.db.query(
        'SELECT * FROM agentic_steps WHERE "threadId" = $1 ORDER BY at',
        [threadId],
      );
      return rows.map(toStep);
    },
  };
}

const toStep = (r: any): StepRecord => ({
        runId: r.runId, threadId: r.threadId ?? '', agentId: r.agentId ?? null, index: r.index,
        durationMs: r.durationMs, finishReason: r.finishReason,
        inputTokens: r.inputTokens, cachedInputTokens: r.cachedInputTokens,
        outputTokens: r.outputTokens, totalTokens: r.totalTokens,
        tools: r.tools ?? [], text: r.text ?? null,
        toolCalls: r.toolCalls ?? [], at: new Date(r.at),
});
