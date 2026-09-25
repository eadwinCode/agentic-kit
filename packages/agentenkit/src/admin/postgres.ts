import type { ExecutionState, NewRunRecord, RunPatch, RunRecord } from '../core/types.js';
import type {
  AdminStore, AdminThread, AdminThreadFilter, NewAdminThread,
  NewStepRecord, RunDeltas, RunFilter, RunTotals, StepRecord,
} from '../ports/admin.js';
import { addRunTotals, emptyRunTotals, JSON_COLUMNS, RUN_COLUMNS } from './shared.js';
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

/** The RunFilter as a WHERE clause, limit aside. */
function runWhere(f: RunFilter): { where: string; vals: unknown[] } {
  const where: string[] = [];
  const vals: unknown[] = [];
  if (f.state?.length) where.push(`state = ANY($${vals.push(f.state)})`);
  if (f.agent) where.push(`agent = $${vals.push(f.agent)}`);
  if (f.threadId) where.push(`"threadId" = $${vals.push(f.threadId)}`);
  if (f.threadIds?.length) where.push(`"threadId" = ANY($${vals.push(f.threadIds)})`);
  if (f.since) where.push(`"startedAt" >= $${vals.push(f.since)}`);
  if (f.until) where.push(`"startedAt" <= $${vals.push(f.until)}`);
  if (f.unsettled) where.push('"endedAt" IS NOT NULL AND "settledAt" IS NULL');
  if (f.depth !== undefined) where.push(`depth = $${vals.push(f.depth)}`);
  if (f.before) {
    where.push(`("startedAt", id) < ($${vals.push(f.before.startedAt)}, $${vals.push(f.before.id)})`);
  }
  return { where: where.length ? ` WHERE ${where.join(' AND ')}` : '', vals };
}

/** BIGINT columns come back from `pg` as strings (§2.9). */
const num = (v: unknown): number | null => (v === null || v === undefined ? null : Number(v));

const toThread = (r: any): AdminThread => ({
  id: r.id, state: r.state as ExecutionState, model: r.model,
  firstSeenAt: new Date(r.firstSeenAt), updatedAt: new Date(r.updatedAt),
  startedWith: r.startedWith ? { ...r.startedWith, at: new Date(r.startedWith.at) } : null,
});

const toRun = (r: any): RunRecord => ({
  id: r.id, threadId: r.threadId, parentRunId: r.parentRunId ?? null, depth: r.depth,
  agent: r.agent, model: r.model, state: r.state as ExecutionState,
  stopReason: r.stopReason ?? null, error: r.error ?? null,
  startedAt: new Date(r.startedAt), endedAt: r.endedAt ? new Date(r.endedAt) : null,
  enqueuedAt: r.enqueuedAt ? new Date(r.enqueuedAt) : null,
  durationMs: num(r.durationMs), queuedMs: num(r.queuedMs),
  settledAt: r.settledAt ? new Date(r.settledAt) : null,
  settlingAt: r.settlingAt ? new Date(r.settlingAt) : null,
  attempts: r.attempts, steps: r.steps,
  inputTokens: Number(r.inputTokens), cachedInputTokens: Number(r.cachedInputTokens),
  outputTokens: Number(r.outputTokens), totalTokens: Number(r.totalTokens),
  result: r.result ?? null,
  prompt: r.prompt ?? null, tokenBudget: r.tokenBudget ?? null,
  runState: r.runState ?? null, providerOptions: r.providerOptions ?? null,
  costBudgetMicros: num(r.costBudgetMicros), maxSteps: num(r.maxSteps),
});

/** Operational history in Postgres — the production store (§2.9), reached
 *  through AGENTIC_KIT_ADMIN_DATABASE_URL. Point it at its own database or the
 *  one you already have; the prefix keeps them apart either way. */
export class PostgresAdminStore implements AdminStore {
  private constructor(private readonly db: PgLike) {}

  /** The store with its schema migration already running behind it (§2.9).
   *
   *  Nothing here touches the network: a `pg` Pool connects lazily, so a
   *  URL that cannot be reached shows up as a failed migration, logged, and
   *  retried on a later admin call (see `gatedAdminStore`). The schema runs
   *  in the background, so a service starts at the same speed whether or not
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
    // A pg Pool emits 'error' when an idle connection drops (a failover, a
    // server restart). An EventEmitter with no listener for it throws, which
    // ends the process. The pool replaces the connection by itself, so a
    // log line is all the error needs. A caller's own listener is left alone.
    const emitter = db as unknown as {
      on?(event: 'error', fn: (err: unknown) => void): unknown;
      listenerCount?(event: 'error'): number;
    };
    if (typeof emitter.on === 'function' && (emitter.listenerCount?.('error') ?? 0) === 0) {
      emitter.on('error', (err) => (log ?? console).error('admin postgres connection error', err));
    }
    return gatedAdminStore(store, () =>
      migrate(db).catch((err) => {
        // Loud, because everything downstream of this is silent: admin writes
        // are best effort, so a failed migration shows up as a dashboard with
        // nothing in it rather than as an error.
        (log ?? console).error('admin migrations failed', err);
        throw err;
      }),
    );
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
      return rows.map(toThread);
    },
    get: async (threadId: string): Promise<AdminThread | null> => {
      const { rows } = await this.db.query('SELECT * FROM agentic_threads WHERE id = $1', [threadId]);
      return rows[0] ? toThread(rows[0]) : null;
    },
    delete: async (threadId: string) => {
      await this.transaction(async (q) => {
        await q('DELETE FROM agentic_steps WHERE "threadId" = $1', [threadId]);
        await q('DELETE FROM agentic_runs WHERE "threadId" = $1', [threadId]);
        await q('DELETE FROM agentic_threads WHERE id = $1', [threadId]);
      });
    },
  };

  /** Run `fn` in one transaction, on one connection: a pool hands each query
   *  whichever connection is free, which is not a transaction at all. */
  private async transaction(fn: (q: (sql: string, params?: unknown[]) => Promise<unknown>) => Promise<void>) {
    const conn = this.db.connect ? await this.db.connect() : null;
    const q = (sql: string, params?: unknown[]) => (conn ?? this.db).query(sql, params);
    try {
      await q('BEGIN');
      try {
        await fn(q);
        await q('COMMIT');
      } catch (err) {
        await q('ROLLBACK').catch(() => undefined);
        throw err;
      }
    } finally {
      conn?.release();
    }
  }

  runs = {
    start: async (run: NewRunRecord) => {
      const { rows } = await this.db.query(
        `INSERT INTO agentic_runs
           (id, "threadId", "parentRunId", depth, agent, model, state,
            prompt, "tokenBudget", "runState", "providerOptions", "enqueuedAt", "costBudgetMicros", "maxSteps")
         VALUES ($1, $2, $3, $4, $5, $6, $11, $7, $8, $9, $10, $12, $13, $14) RETURNING *`,
        [
          run.id, run.threadId, run.parentRunId ?? null, run.depth ?? 0,
          run.agent, run.model, run.prompt ?? null, run.tokenBudget ?? null,
          run.runState ? JSON.stringify(run.runState) : null,
          run.providerOptions ? JSON.stringify(run.providerOptions) : null,
          run.state ?? 'RUNNING', run.enqueuedAt ?? null,
          run.costBudgetMicros ?? null, run.maxSteps ?? null,
        ],
      );
      return toRun(rows[0]);
    },
    patch: async (runId: string, patch: RunPatch) => {
      const sets: string[] = [];
      const vals: unknown[] = [];
      for (const [k, v] of Object.entries(patch)) {
        // Only the run's own columns, never a key SQL was built from blindly.
        // JSON columns need the value serialised; everything else pg handles.
        if (v === undefined || !RUN_COLUMNS.has(k)) continue;
        sets.push(`"${k}" = $${vals.push(JSON_COLUMNS.has(k) ? JSON.stringify(v) : v)}`);
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
      const { where, vals } = runWhere(f);
      // Ordered on the id after the start time, so a page's last run is a
      // cursor that splits the listing exactly (RunFilter.before).
      const { rows } = await this.db.query(
        `SELECT * FROM agentic_runs${where}
         ORDER BY "startedAt" DESC, id DESC LIMIT $${vals.push(f.limit ?? 100)}`,
        vals,
      );
      return rows.map(toRun);
    },
    increment: async (runId: string, d: RunDeltas) => {
      await this.db.query(
        `UPDATE agentic_runs SET steps = steps + $1, "inputTokens" = "inputTokens" + $2,
           "cachedInputTokens" = "cachedInputTokens" + $3, "outputTokens" = "outputTokens" + $4,
           "totalTokens" = "totalTokens" + $5 WHERE id = $6`,
        [d.steps, d.inputTokens, d.cachedInputTokens, d.outputTokens, d.totalTokens, runId],
      );
    },
    totals: async (f: RunFilter): Promise<RunTotals> => {
      const { where, vals } = runWhere({ ...f, before: undefined });
      const { rows } = await this.db.query(
        `SELECT state, "stopReason", COUNT(*) AS n, COALESCE(SUM(steps),0) AS steps,
           COALESCE(SUM("inputTokens"),0) AS "inputTokens", COALESCE(SUM("cachedInputTokens"),0) AS "cachedInputTokens",
           COALESCE(SUM("outputTokens"),0) AS "outputTokens", COALESCE(SUM("totalTokens"),0) AS "totalTokens"
         FROM agentic_runs${where} GROUP BY state, "stopReason"`,
        vals,
      );
      const out = emptyRunTotals();
      for (const r of rows) addRunTotals(out, r.state, r.stopReason, Number(r.n), r);
      return out;
    },
    claimSettle: async (runId: string, token: string, staleBefore: Date) => {
      const { rows } = await this.db.query(
        `UPDATE agentic_runs SET "settlingAt" = now(), "settleToken" = $1
         WHERE id = $2 AND "settledAt" IS NULL AND ("settlingAt" IS NULL OR "settlingAt" < $3)
         RETURNING id`,
        [token, runId, staleBefore],
      );
      return rows.length === 1;
    },
    endSettle: async (runId: string, token: string, settled: boolean) => {
      await this.db.query(
        settled
          ? `UPDATE agentic_runs SET "settledAt" = now(), "settlingAt" = NULL, "settleToken" = NULL
             WHERE id = $1 AND "settleToken" = $2`
          : `UPDATE agentic_runs SET "settlingAt" = NULL, "settleToken" = NULL
             WHERE id = $1 AND "settleToken" = $2`,
        [runId, token],
      );
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
        durationMs: Number(r.durationMs), finishReason: r.finishReason,
        inputTokens: Number(r.inputTokens), cachedInputTokens: Number(r.cachedInputTokens),
        outputTokens: Number(r.outputTokens), totalTokens: Number(r.totalTokens),
        tools: r.tools ?? [], text: r.text ?? null,
        toolCalls: r.toolCalls ?? [], at: new Date(r.at),
});
