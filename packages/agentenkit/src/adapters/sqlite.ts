import { randomUUID } from 'node:crypto';
import type {
  AgentEvent, ExecutionState, MessageDTO, NewMessage, NewThreadEvent, NewUsage, ThreadDTO, ThreadEventFilter,
  UsageFilter, UsageTotals, ThreadTransition,
} from '../core/types.js';
import { UsageMerger } from '../core/usage.js';
import type { Storage } from '../ports/storage.js';
import { StreamClosedError, StreamGoneError, type RunStreams, type StreamMeta, type StreamSnapshot } from '../ports/streams.js';
import { isStreamEnd, type StreamEnd, type StreamEvent, type StreamItem } from '../core/stream-events.js';
import { sleep } from './stream-scripts.js';

/** Minimal structural type over a synchronous SQLite handle — `bun:sqlite`'s
 *  `Database` satisfies it. Kept structural for the same reason every other
 *  adapter is: the package never imports a driver itself (§3.4), so a caller
 *  can hand in their own handle (an in-memory one for tests, a tuned one for
 *  production). */
export interface SqliteLike {
  prepare(sql: string): SqliteStatementLike;
  exec?(sql: string): unknown;
  run?(sql: string): unknown;
}

export interface SqliteStatementLike {
  all(...params: unknown[]): unknown[];
  get?(...params: unknown[]): unknown;
  run(...params: unknown[]): unknown;
}

/** Open a SQLite handle with whichever driver the process has: `bun:sqlite`
 *  first, then `node:sqlite`. Preferring one and requiring it are different
 *  things — a Next.js server runs on Node even inside a Bun workspace, which
 *  is exactly where a bun-only store fails. Pass ':memory:' for a database
 *  that lives only as long as the process.
 *
 *  Imported dynamically so neither driver enters the module graph until asked
 *  for: importing this file is safe anywhere, only calling this needs one. */
export async function openSqlite(filename = 'agentic-kit.sqlite'): Promise<SqliteLike> {
  const tried: string[] = [];
  for (const specifier of ['bun:sqlite', 'node:sqlite']) {
    let Ctor: (new (filename: string) => SqliteLike) | undefined;
    try {
      // webpackIgnore keeps this a real runtime import. Bundlers otherwise
      // turn a variable specifier into a context module and fail to resolve
      // `node:sqlite` even on a Node that ships it — which is what happens
      // inside a Next.js server build.
      const mod: any = await import(/* webpackIgnore: true */ /* @vite-ignore */ specifier);
      Ctor = mod.Database ?? mod.DatabaseSync;
    } catch (err) {
      tried.push(`${specifier}: ${err instanceof Error ? err.message : String(err)}`);
      continue;
    }
    // Outside the try: a driver that loaded but cannot open or set up the
    // file (locked, unwritable) must say so, not report a missing driver.
    if (Ctor) return tuneSqlite(new Ctor(filename));
  }
  throw new Error(
    'No SQLite driver available. Run under Bun, or Node 22+ for node:sqlite, ' +
      `or construct the store with your own handle. Tried — ${tried.join('; ')}`,
  );
}


/** Set a file up for more than one process (§3.4): WAL lets readers run beside
 *  the one writer, and busy_timeout makes a writer wait up to five seconds for
 *  the lock rather than fail at once with SQLITE_BUSY. `openSqlite` calls it;
 *  call it yourself on a handle you opened some other way. An in-memory
 *  database keeps its own journal mode, which is fine.
 *
 *  busy_timeout goes FIRST: switching a new file to WAL takes a lock too.
 *  And SQLite does not always wait on the busy timeout for that switch: with
 *  several processes opening one fresh file at once (the workers of a
 *  `next build`) it can give up at once with "database is locked". So the
 *  switch is tried again for up to five seconds, like any other write. The
 *  Go runtime's Tune does the same. */
export function tuneSqlite<T extends SqliteLike>(db: T): T {
  db.prepare('PRAGMA busy_timeout=5000').all(); // answers with a row
  for (let tries = 0; ; tries++) {
    try {
      db.prepare('PRAGMA journal_mode=WAL').all();
      return db;
    } catch (err) {
      const locked = /locked|busy/i.test(err instanceof Error ? err.message : String(err));
      if (!locked || tries >= 100) throw err;
      sleepSync(50);
    }
  }
}

/** Blocks for `ms`. Only for a setup step that must stay synchronous. */
function sleepSync(ms: number): void {
  Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ms);
}

// SQLite has no date or JSON type: times are epoch milliseconds so they sort
// and compare as integers, and structured columns are TEXT holding JSON.
const json = (v: unknown) => (v === undefined ? null : JSON.stringify(v));
const parse = (v: unknown) => (typeof v === 'string' ? JSON.parse(v) : v ?? null);

const SCHEMA = `
CREATE TABLE IF NOT EXISTS threads (
  id TEXT PRIMARY KEY, title TEXT NOT NULL DEFAULT 'New Thread',
  state TEXT NOT NULL DEFAULT 'IDLE', model TEXT NOT NULL DEFAULT 'gpt-4o',
  createdAt INTEGER NOT NULL, updatedAt INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS messages (
  id TEXT PRIMARY KEY, threadId TEXT NOT NULL, agentId TEXT,
  role TEXT NOT NULL, content TEXT NOT NULL, createdAt INTEGER NOT NULL,
  seq INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS messages_thread ON messages(threadId, seq);
CREATE TABLE IF NOT EXISTS events (
  id TEXT PRIMARY KEY, threadId TEXT NOT NULL, seq INTEGER NOT NULL,
  type TEXT NOT NULL, payload TEXT, createdAt INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS events_thread_seq ON events(threadId, seq);
CREATE INDEX IF NOT EXISTS events_thread_type ON events(threadId, type, seq);
-- One row per MODEL CALL, not per run segment. cachedInputTokens holds cache
-- READS, so the column that was already there still means what it always
-- meant, and cache writes get their own column beside it. A NULL costMicros
-- is an unpriced call, which is not the same as one that cost nothing.
-- (Statements here are split on the semicolon, so never write one in a
-- comment.)
CREATE TABLE IF NOT EXISTS usage (
  id TEXT PRIMARY KEY, threadId TEXT NOT NULL, runId TEXT, agentId TEXT, agentName TEXT,
  kind TEXT NOT NULL DEFAULT 'step', step INTEGER NOT NULL DEFAULT 0,
  model TEXT, modelId TEXT,
  inputTokens INTEGER NOT NULL, cachedInputTokens INTEGER NOT NULL,
  cacheWriteInputTokens INTEGER NOT NULL DEFAULT 0,
  outputTokens INTEGER NOT NULL, reasoningTokens INTEGER NOT NULL DEFAULT 0,
  totalTokens INTEGER NOT NULL,
  outcome TEXT NOT NULL DEFAULT 'finished', estimated INTEGER NOT NULL DEFAULT 0,
  providerMetadata TEXT,
  costMicros INTEGER, costCurrency TEXT, costSource TEXT,
  createdAt INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS usage_thread ON usage(threadId);
CREATE INDEX IF NOT EXISTS usage_run ON usage(runId, createdAt);
CREATE TABLE IF NOT EXISTS runs (
  id TEXT PRIMARY KEY, threadId TEXT NOT NULL, parentRunId TEXT,
  depth INTEGER NOT NULL DEFAULT 0, agent TEXT NOT NULL, model TEXT NOT NULL,
  state TEXT NOT NULL DEFAULT 'RUNNING', stopReason TEXT, error TEXT,
  startedAt INTEGER NOT NULL, endedAt INTEGER, durationMs INTEGER, queuedMs INTEGER,
  attempts INTEGER NOT NULL DEFAULT 0, steps INTEGER NOT NULL DEFAULT 0,
  inputTokens INTEGER NOT NULL DEFAULT 0, cachedInputTokens INTEGER NOT NULL DEFAULT 0,
  outputTokens INTEGER NOT NULL DEFAULT 0, totalTokens INTEGER NOT NULL DEFAULT 0,
  result TEXT
);
CREATE INDEX IF NOT EXISTS runs_thread ON runs(threadId, startedAt);
CREATE INDEX IF NOT EXISTS runs_state ON runs(state, startedAt);
CREATE INDEX IF NOT EXISTS runs_parent ON runs(parentRunId);
`;

/** A complete Storage over SQLite (§3.2) — every table the platform needs,
 *  created on construction. This is what `dev: true` wires up: a working
 *  agent platform with no infrastructure to stand up first. */
export class SqliteStorage implements Storage {
  constructor(private readonly db: SqliteLike) {
    for (const stmt of SCHEMA.split(';')) {
      const sql = stmt.trim();
      if (sql) this.db.prepare(sql).run();
    }
    // CREATE TABLE IF NOT EXISTS leaves an existing table as it was, so a
    // usage table from before cost moved onto it (§4) gets its new columns
    // here. Idempotent: a column that exists is skipped.
    this.addMissing('usage', {
      runId: 'TEXT',
      agentName: 'TEXT',
      kind: "TEXT NOT NULL DEFAULT 'step'",
      step: 'INTEGER NOT NULL DEFAULT 0',
      model: 'TEXT',
      modelId: 'TEXT',
      cacheWriteInputTokens: 'INTEGER NOT NULL DEFAULT 0',
      reasoningTokens: 'INTEGER NOT NULL DEFAULT 0',
      outcome: "TEXT NOT NULL DEFAULT 'finished'",
      estimated: 'INTEGER NOT NULL DEFAULT 0',
      providerMetadata: 'TEXT',
      costMicros: 'INTEGER',
      costCurrency: 'TEXT',
      costSource: 'TEXT',
    });
    this.db.prepare('CREATE INDEX IF NOT EXISTS usage_run ON usage(runId, createdAt)').run();
    // The thread's current run, for ThreadTransition's compare-and-set.
    this.addMissing('threads', { runId: 'TEXT' });
    // The run a record entry belongs to.
    this.addMissing('events', { runId: 'TEXT' });
    // One event per seq on a thread: a counter that restarted must fail its
    // write, never land a second event under a seq clients already have. A log
    // from before this check may already hold duplicates; the index is then
    // left off and said so, rather than refusing to start.
    try {
      this.db.prepare('CREATE UNIQUE INDEX IF NOT EXISTS events_thread_seq_unique ON events(threadId, seq)').run();
    } catch (err) {
      console.warn('event seq uniqueness not enforced: the log already holds duplicate seqs', err);
    }
    // The same for messages: two appends that raced to one seq must not both
    // land, or a turn's order would depend on which row the database returns
    // first.
    try {
      this.db.prepare('CREATE UNIQUE INDEX IF NOT EXISTS messages_thread_seq_unique ON messages(threadId, seq)').run();
    } catch (err) {
      console.warn('message seq uniqueness not enforced: the table already holds duplicate seqs', err);
    }
  }

  /** Run `fn` in one write transaction. IMMEDIATE takes the write lock up
   *  front, so another process waits (busy_timeout) rather than failing half
   *  way through. */
  private tx<T>(fn: () => T): T {
    this.write('BEGIN IMMEDIATE');
    try {
      const out = fn();
      this.write('COMMIT');
      return out;
    } catch (err) {
      try { this.write('ROLLBACK'); } catch { /* already gone */ }
      throw err;
    }
  }

  private changes(res: unknown): number {
    return Number((res as { changes?: number | bigint } | undefined)?.changes ?? 0);
  }

  private addMissing(table: string, cols: Record<string, string>) {
    const have = new Set(
      (this.db.prepare(`PRAGMA table_info(${table})`).all() as Array<{ name: string }>).map((c) => c.name),
    );
    for (const [col, type] of Object.entries(cols)) {
      if (!have.has(col)) this.db.prepare(`ALTER TABLE ${table} ADD COLUMN ${col} ${type}`).run();
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

  private toThread = (r: any): ThreadDTO => ({
    id: r.id, state: r.state as ExecutionState, model: r.model,
    createdAt: new Date(r.createdAt), updatedAt: new Date(r.updatedAt),
  });
  private toMessage = (r: any): MessageDTO => ({
    id: r.id, threadId: r.threadId, agentId: r.agentId ?? null,
    role: r.role, content: parse(r.content), createdAt: new Date(r.createdAt),
  });
  private toEvent = (r: any): AgentEvent => ({
    threadId: r.threadId, seq: r.seq, type: r.type,
    payload: parse(r.payload), createdAt: new Date(r.createdAt), ...(r.runId ? { runId: r.runId } : {}),
  }) as AgentEvent;
  threads = {
    get: async (threadId: string) => {
      const r = this.one('SELECT * FROM threads WHERE id = ?', threadId);
      return r ? this.toThread(r) : null;
    },
    create: async (init?: { model?: string }) => {
      const now = Date.now();
      const id = randomUUID();
      this.write(
        'INSERT INTO threads (id,title,state,model,createdAt,updatedAt) VALUES (?,?,?,?,?,?)',
        id, 'New Thread', 'IDLE', init?.model ?? 'gpt-4o', now, now,
      );
      return this.toThread({
        id, state: 'IDLE', model: init?.model ?? 'gpt-4o', createdAt: now, updatedAt: now,
      });
    },
    list: async () =>
      this.all('SELECT * FROM threads ORDER BY updatedAt DESC').map(this.toThread),
    setState: async (threadId: string, state: ExecutionState) => {
      this.write('UPDATE threads SET state = ?, updatedAt = ? WHERE id = ?',
        state, Date.now(), threadId);
    },
    delete: async (threadId: string) => {
      // One transaction: a thread is gone with everything it owned, or not at
      // all. No FK cascade here: the schema is created by this adapter and the
      // cascade is spelled out, so a caller can read exactly what is removed.
      this.tx(() => {
        const res = this.db.prepare('DELETE FROM threads WHERE id = ?').run(threadId);
        if (this.changes(res) === 0) throw new Error(`Unknown thread ${threadId}`);
        for (const t of ['messages', 'events', 'usage', 'runs']) {
          this.write(`DELETE FROM ${t} WHERE threadId = ?`, threadId);
        }
      });
    },
    transition: async (threadId: string, tr: ThreadTransition) => {
      if (tr.from.length === 0) return false;
      // One conditional UPDATE, so exactly one caller can win (§3.4): the
      // driver's change count says whether it was this one. A thread with no
      // run recorded yet (from before the column) matches any run.
      const res = this.db
        .prepare(
          `UPDATE threads SET state = ?, updatedAt = ?, runId = COALESCE(NULLIF(?, ''), runId)
           WHERE id = ? AND (? = '' OR runId IS NULL OR runId = ?)
             AND state IN (${tr.from.map(() => '?').join(', ')})`,
        )
        .run(tr.to, Date.now(), tr.newRunId ?? '', threadId, tr.runId ?? '', tr.runId ?? '', ...tr.from);
      return Number((res as { changes?: number | bigint } | undefined)?.changes ?? 0) > 0;
    },
    claimState: async (threadId: string, from: ExecutionState, to: ExecutionState) => {
      // The §3.4 compare-and-set: one conditional UPDATE, so exactly one
      // caller can win, and the driver's change count says whether it was
      // this one. A read before or after would race another process.
      const res = this.db
        .prepare('UPDATE threads SET state = ?, updatedAt = ? WHERE id = ? AND state = ?')
        .run(to, Date.now(), threadId, from);
      return this.changes(res) > 0;
    },
  };

  messages = {
    append: async (threadId: string, m: NewMessage) => {
      const now = Date.now();
      const id = randomUUID();
      // An explicit seq keeps insertion order stable: several messages land
      // inside the same millisecond, so createdAt alone cannot order them.
      // Taken inside the INSERT, one statement, so another process cannot
      // read the same MAX in between; the unique index backs that up.
      this.write(
        `INSERT INTO messages (id,threadId,agentId,role,content,createdAt,seq)
         VALUES (?,?,?,?,?,?,(SELECT COALESCE(MAX(seq),0)+1 FROM messages WHERE threadId = ?))`,
        id, threadId, m.agentId ?? null, m.role, json(m.content), now, threadId,
      );
      return this.toMessage({
        id, threadId, agentId: m.agentId ?? null, role: m.role,
        content: json(m.content), createdAt: now,
      });
    },
    list: async (threadId: string, opts?: { agentId?: string | null }) => {
      if (opts && 'agentId' in opts) {
        return this.all(
          opts.agentId === null
            ? 'SELECT * FROM messages WHERE threadId = ? AND agentId IS NULL ORDER BY seq'
            : 'SELECT * FROM messages WHERE threadId = ? AND agentId = ? ORDER BY seq',
          ...(opts.agentId === null ? [threadId] : [threadId, opts.agentId]),
        ).map(this.toMessage);
      }
      return this.all('SELECT * FROM messages WHERE threadId = ? ORDER BY seq', threadId)
        .map(this.toMessage);
    },
    deleteFrom: async (threadId: string, messageId: string) => {
      const target = this.one('SELECT seq FROM messages WHERE id = ? AND threadId = ?',
        messageId, threadId);
      if (!target) return 0;
      const doomed = this.all('SELECT id FROM messages WHERE threadId = ? AND seq >= ?',
        threadId, target.seq);
      this.write('DELETE FROM messages WHERE threadId = ? AND seq >= ?', threadId, target.seq);
      return doomed.length;
    },
  };

  events = {
    // The seq is minted in the insert itself: SQLite runs one writer at a
    // time, so no two appends on a thread read the same MAX.
    append: async (threadId: string, e: NewThreadEvent): Promise<AgentEvent> => {
      const createdAt = e.createdAt ? new Date(e.createdAt).getTime() : Date.now();
      const row = this.all(
        `INSERT INTO events (id,threadId,seq,type,payload,createdAt,runId)
         SELECT ?,?,COALESCE(MAX(seq),0)+1,?,?,?,? FROM events WHERE threadId = ?
         RETURNING seq`,
        randomUUID(), threadId, e.type, json(e.payload), createdAt, e.runId ?? null, threadId,
      )[0] as { seq: number };
      return {
        threadId, seq: Number(row.seq), type: e.type, payload: e.payload,
        createdAt: new Date(createdAt), ...(e.runId ? { runId: e.runId } : {}),
      };
    },
    list: async (threadId: string, f: ThreadEventFilter = {}) => {
      const where = ['threadId = ?'];
      const args: unknown[] = [threadId];
      if (f.types) {
        where.push(`type IN (${f.types.map(() => '?').join(',') || 'NULL'})`);
        args.push(...f.types);
      }
      if (f.runId !== undefined) { where.push('runId = ?'); args.push(f.runId); }
      if (f.after !== undefined) { where.push('seq > ?'); args.push(f.after); }
      const limit = f.limit ? ` LIMIT ${Math.floor(f.limit)}` : '';
      return this.all(`SELECT * FROM events WHERE ${where.join(' AND ')} ORDER BY seq${limit}`, ...args)
        .map(this.toEvent);
    },
    listSince: async (threadId: string, sinceSeq: number) =>
      this.all('SELECT * FROM events WHERE threadId = ? AND seq > ? ORDER BY seq',
        threadId, sinceSeq).map(this.toEvent),
    latest: async (threadId: string, type: string) => {
      const r = this.one(
        'SELECT * FROM events WHERE threadId = ? AND type = ? ORDER BY seq DESC LIMIT 1',
        threadId, type);
      return r ? this.toEvent(r) : null;
    },
    listByType: async (threadId: string, type: string) =>
      this.all('SELECT * FROM events WHERE threadId = ? AND type = ? ORDER BY seq',
        threadId, type).map(this.toEvent),
    prune: async (types: string[], opts: { limit: number; dryRun?: boolean }) => {
      const counts: Record<string, number> = {};
      const marks = types.map(() => '?').join(',') || 'NULL';
      if (opts.dryRun) {
        for (const r of this.all(`SELECT type, COUNT(*) AS n FROM events WHERE type IN (${marks}) GROUP BY type`, ...types) as Array<{ type: string; n: number }>) {
          counts[r.type] = Number(r.n);
        }
        return counts;
      }
      const rows = this.all(`SELECT id, type FROM events WHERE type IN (${marks}) LIMIT ?`, ...types, opts.limit) as Array<{ id: string; type: string }>;
      if (rows.length === 0) return counts;
      this.write(`DELETE FROM events WHERE id IN (${rows.map(() => '?').join(',')})`, ...rows.map((r) => r.id));
      for (const r of rows) counts[r.type] = (counts[r.type] ?? 0) + 1;
      return counts;
    },
  };

  usage = {
    record: async (threadId: string, u: NewUsage) => {
      this.write(
        `INSERT INTO usage (id,threadId,runId,agentId,agentName,kind,step,model,modelId,
           inputTokens,cachedInputTokens,cacheWriteInputTokens,outputTokens,reasoningTokens,
           totalTokens,outcome,estimated,providerMetadata,costMicros,costCurrency,costSource,createdAt)
         VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
        randomUUID(), threadId, u.runId ?? null, u.agentId ?? null, u.agentName ?? null,
        u.kind, u.step, u.model ?? null, u.modelId ?? null,
        u.inputTokens, u.cacheReadInputTokens, u.cacheWriteInputTokens,
        u.outputTokens, u.reasoningTokens, u.totalTokens,
        u.outcome, u.estimated ? 1 : 0,
        u.providerMetadata ? JSON.stringify(u.providerMetadata) : null,
        u.cost?.micros ?? null, u.cost?.currency ?? null, u.cost?.source ?? null,
        Date.now(),
      );
    },
    // One grouped read rather than every row: a long thread holds a usage row
    // per model call (§4), and the bill only ever wants them by agent and
    // model. Summing the groups gives the totals, so the two always agree.
    total: async (threadId: string, filter: UsageFilter = {}): Promise<UsageTotals> => {
      // Grouped by currency as well, so a group never sums two units; the
      // merge puts one agent's spend on one model back on a single line.
      const rows = this.all(
        `SELECT COALESCE(agentId,'') AS agentId, COALESCE(agentName,'') AS agentName,
                COALESCE(model,'') AS model, COALESCE(modelId,'') AS modelId,
                COALESCE(SUM(inputTokens),0) AS i, COALESCE(SUM(cachedInputTokens),0) AS cr,
                COALESCE(SUM(cacheWriteInputTokens),0) AS cw, COALESCE(SUM(outputTokens),0) AS o,
                COALESCE(SUM(reasoningTokens),0) AS rt, COALESCE(SUM(totalTokens),0) AS t,
                COUNT(*) AS calls, COALESCE(SUM(estimated),0) AS est,
                COALESCE(SUM(costMicros),0) AS cost, COALESCE(costCurrency,'') AS currency,
                COALESCE(SUM(CASE WHEN costMicros IS NULL THEN 1 ELSE 0 END),0) AS unpriced
         FROM usage WHERE threadId = ?${filter.runId ? ' AND runId = ?' : ''}
         GROUP BY agentId, agentName, model, modelId, costCurrency
         ORDER BY MIN(createdAt)`,
        ...(filter.runId ? [threadId, filter.runId] : [threadId]),
      );
      const merge = new UsageMerger();
      for (const r of rows) {
        merge.add({
          line: {
            agentId: r.agentId || null, agentName: r.agentName || null,
            model: r.model || null, modelId: r.modelId || null,
            inputTokens: r.i, cacheReadInputTokens: r.cr, cacheWriteInputTokens: r.cw,
            outputTokens: r.o, reasoningTokens: r.rt,
            calls: r.calls, estimated: r.est, costMicros: r.cost,
          },
          currency: r.currency,
          totalTokens: r.t,
          unpriced: r.unpriced,
        });
      }
      return merge.totals();
    },
  };



}

/** The current time in epoch milliseconds, the form expiresAt is kept in. */
const NOW_MS = `CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)`;

const STREAMS_SCHEMA = [
  `CREATE TABLE IF NOT EXISTS run_streams (
     id TEXT PRIMARY KEY, "threadId" TEXT NOT NULL, "runId" TEXT NOT NULL,
     closed INTEGER NOT NULL, "endEvent" TEXT, "expiresAt" INTEGER NOT NULL)`,
  `CREATE INDEX IF NOT EXISTS run_streams_expires ON run_streams("expiresAt")`,
  `CREATE INDEX IF NOT EXISTS run_streams_thread ON run_streams("threadId")`,
  `CREATE TABLE IF NOT EXISTS run_stream_events (
     pos INTEGER PRIMARY KEY AUTOINCREMENT, "streamId" TEXT NOT NULL, event TEXT NOT NULL)`,
  `CREATE INDEX IF NOT EXISTS run_stream_events_stream ON run_stream_events("streamId", pos)`,
];

/** RunStreams over SQLite: the run_streams and run_stream_events tables, the
 *  same ones the Go adapter uses. The offset is the event row's key, which
 *  AUTOINCREMENT never reuses. Readers in this process wake on its own
 *  appends; one in another process sees them on its next poll. Expired
 *  streams are deleted as new ones open, 500 a pass, at most once a minute. */
export class SqliteRunStreams implements RunStreams {
  private readonly waiters = new Map<string, Set<() => void>>();
  private swept = 0;

  constructor(
    private readonly db: SqliteLike,
    private readonly opts: { pollMs?: number } = {},
  ) {
    for (const sql of STREAMS_SCHEMA) this.db.prepare(sql).run();
  }

  private tx<T>(work: () => T): T {
    this.db.prepare('BEGIN IMMEDIATE').run();
    try {
      const out = work();
      this.db.prepare('COMMIT').run();
      return out;
    } catch (err) {
      this.db.prepare('ROLLBACK').run();
      throw err;
    }
  }

  private wake(streamId: string) {
    const set = this.waiters.get(streamId);
    this.waiters.delete(streamId);
    for (const wake of set ?? []) wake();
  }

  /** Registers a wake-up before the reader reads, so an append that lands
   *  between the read and the wait still wakes it. */
  private arm(streamId: string) {
    let wake!: () => void;
    const woken = new Promise<void>((resolve) => { wake = resolve; });
    let set = this.waiters.get(streamId);
    if (!set) { set = new Set(); this.waiters.set(streamId, set); }
    set.add(wake);
    const disarm = () => {
      const s = this.waiters.get(streamId);
      s?.delete(wake);
      if (s && s.size === 0) this.waiters.delete(streamId);
    };
    return { woken, disarm };
  }

  private sweep() {
    if (Date.now() - this.swept < 60_000) return;
    this.swept = Date.now();
    const ids = this.db
      .prepare(`SELECT id FROM run_streams WHERE "expiresAt" <= ${NOW_MS} LIMIT 500`)
      .all() as Array<{ id: string }>;
    for (const { id } of ids) this.remove(id);
  }

  private remove(streamId: string) {
    this.tx(() => {
      this.db.prepare('DELETE FROM run_stream_events WHERE "streamId" = ?').run(streamId);
      this.db.prepare('DELETE FROM run_streams WHERE id = ?').run(streamId);
    });
  }

  async open(streamId: string, meta: StreamMeta, ttlMs: number) {
    this.sweep();
    // A row past its expiry is gone: replace it rather than reopen it.
    this.db.prepare(`DELETE FROM run_streams WHERE id = ? AND "expiresAt" <= ${NOW_MS}`).run(streamId);
    this.db
      .prepare(`INSERT INTO run_streams (id, "threadId", "runId", closed, "expiresAt") VALUES (?, ?, ?, 0, ?)
                ON CONFLICT (id) DO NOTHING`)
      .run(streamId, meta.threadId, meta.runId, Date.now() + ttlMs);
  }

  /** Inside a transaction: whether the stream is live and closed. */
  private state(streamId: string) {
    const row = this.db
      .prepare(`SELECT closed, "expiresAt" > ${NOW_MS} AS live FROM run_streams WHERE id = ?`)
      .all(streamId)[0] as { closed: number; live: number } | undefined;
    return { live: !!row?.live, closed: !!row?.closed };
  }

  private insert(streamId: string, events: StreamEvent[]): string[] {
    const stmt = this.db.prepare('INSERT INTO run_stream_events ("streamId", event) VALUES (?, ?) RETURNING pos');
    return events.map((e) => String((stmt.all(streamId, JSON.stringify(e))[0] as { pos: number }).pos));
  }

  async append(streamId: string, events: StreamEvent[]) {
    if (events.length === 0) return [];
    const offsets = this.tx(() => {
      const { live, closed } = this.state(streamId);
      if (!live) throw new StreamGoneError(streamId);
      if (closed) throw new StreamClosedError(streamId);
      return this.insert(streamId, events);
    });
    this.wake(streamId);
    return offsets;
  }

  async close(streamId: string, end: StreamEnd, graceMs: number) {
    this.tx(() => {
      const { live, closed } = this.state(streamId);
      if (!live) throw new StreamGoneError(streamId);
      if (closed) return;
      this.insert(streamId, [end]);
      this.db
        .prepare('UPDATE run_streams SET closed = 1, "endEvent" = ?, "expiresAt" = ? WHERE id = ?')
        .run(JSON.stringify(end), Date.now() + graceMs, streamId);
    });
    this.wake(streamId);
  }

  async delete(streamId: string) {
    this.remove(streamId);
    this.wake(streamId);
  }

  private page(streamId: string, after: string | null) {
    const row = this.db
      .prepare(`SELECT "threadId", "runId", closed, "endEvent", "expiresAt" > ${NOW_MS} AS live
                FROM run_streams WHERE id = ?`)
      .all(streamId)[0] as
      | { threadId: string; runId: string; closed: number; endEvent: string | null; live: number }
      | undefined;
    if (!row || !row.live) return null;
    const rows = this.db
      .prepare('SELECT pos, event FROM run_stream_events WHERE "streamId" = ? AND pos > ? ORDER BY pos')
      .all(streamId, after ? Number(after) : 0) as Array<{ pos: number; event: string }>;
    return {
      meta: { threadId: row.threadId, runId: row.runId },
      closed: !!row.closed,
      end: row.closed && row.endEvent ? (JSON.parse(row.endEvent) as StreamEnd) : null,
      items: rows.map((r) => ({ ...JSON.parse(r.event), offset: String(r.pos) }) as StreamItem),
    };
  }

  async *read(streamId: string, after: string | null, signal?: AbortSignal): AsyncIterable<StreamItem> {
    let cursor = after;
    for (;;) {
      if (signal?.aborted) return;
      const { woken, disarm } = this.arm(streamId);
      const page = this.page(streamId, cursor);
      if (!page) {
        disarm();
        throw new StreamGoneError(streamId);
      }
      if (page.items.length > 0) disarm();
      for (const item of page.items) {
        yield item;
        cursor = item.offset;
        if (isStreamEnd(item)) return;
      }
      if (page.items.length > 0) continue;
      // Read from past the end item: nothing more will ever come.
      if (page.closed) {
        disarm();
        return;
      }
      await Promise.race([woken, sleep(this.opts.pollMs ?? 250, signal)]);
      disarm();
    }
  }

  async snapshot(streamId: string, after?: string | null): Promise<StreamSnapshot | null> {
    const page = this.page(streamId, after ?? null);
    return page && { meta: page.meta, items: page.items, end: page.end };
  }
}
