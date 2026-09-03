import { randomUUID } from 'node:crypto';
import type {
  AgentEvent, ExecutionState, MessageDTO, NewMessage, NewUsage, ThreadDTO, UsageFilter, UsageTotals,
} from '../core/types.js';
import { emptyTotals } from '../core/usage.js';
import type { Storage } from '../ports/storage.js';

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
    try {
      // webpackIgnore keeps this a real runtime import. Bundlers otherwise
      // turn a variable specifier into a context module and fail to resolve
      // `node:sqlite` even on a Node that ships it — which is what happens
      // inside a Next.js server build.
      const mod: any = await import(/* webpackIgnore: true */ /* @vite-ignore */ specifier);
      const Ctor = mod.Database ?? mod.DatabaseSync;
      if (Ctor) return new Ctor(filename) as SqliteLike;
    } catch (err) {
      tried.push(`${specifier}: ${err instanceof Error ? err.message : String(err)}`);
    }
  }
  throw new Error(
    'No SQLite driver available. Run under Bun, or Node 22+ for node:sqlite, ' +
      `or construct the store with your own handle. Tried — ${tried.join('; ')}`,
  );
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
    payload: parse(r.payload), createdAt: new Date(r.createdAt),
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
      // No FK cascade here: the schema is created by this adapter and the
      // cascade is spelled out, so a caller can read exactly what is removed.
      for (const t of ['messages', 'events', 'usage', 'runs']) {
        this.write(`DELETE FROM ${t} WHERE threadId = ?`, threadId);
      }
      this.write('DELETE FROM threads WHERE id = ?', threadId);
    },
    claimState: async (threadId: string, from: ExecutionState, to: ExecutionState) => {
      // The §3.4 compare-and-set: one conditional UPDATE, so exactly one
      // caller can win. SQLite serialises writers, which is enough.
      const before = this.one('SELECT state FROM threads WHERE id = ?', threadId);
      if (!before || before.state !== from) return false;
      this.write('UPDATE threads SET state = ?, updatedAt = ? WHERE id = ? AND state = ?',
        to, Date.now(), threadId, from);
      const after = this.one('SELECT state FROM threads WHERE id = ?', threadId);
      return after?.state === to;
    },
  };

  messages = {
    append: async (threadId: string, m: NewMessage) => {
      const now = Date.now();
      const id = randomUUID();
      // An explicit seq keeps insertion order stable: several messages land
      // inside the same millisecond, so createdAt alone cannot order them.
      const seq = (this.one(
        'SELECT COALESCE(MAX(seq),0) AS n FROM messages WHERE threadId = ?', threadId,
      )?.n ?? 0) + 1;
      this.write(
        'INSERT INTO messages (id,threadId,agentId,role,content,createdAt,seq) VALUES (?,?,?,?,?,?,?)',
        id, threadId, m.agentId ?? null, m.role, json(m.content), now, seq,
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
    append: async (threadId: string, e: AgentEvent) => {
      this.write('INSERT INTO events (id,threadId,seq,type,payload,createdAt) VALUES (?,?,?,?,?,?)',
        randomUUID(), threadId, e.seq, e.type, json(e.payload), new Date(e.createdAt).getTime());
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
      const rows = this.all(
        `SELECT COALESCE(agentId,'') AS agentId, COALESCE(agentName,'') AS agentName,
                COALESCE(model,'') AS model, COALESCE(modelId,'') AS modelId,
                COALESCE(SUM(inputTokens),0) AS i, COALESCE(SUM(cachedInputTokens),0) AS cr,
                COALESCE(SUM(cacheWriteInputTokens),0) AS cw, COALESCE(SUM(outputTokens),0) AS o,
                COALESCE(SUM(reasoningTokens),0) AS rt, COALESCE(SUM(totalTokens),0) AS t,
                COUNT(*) AS calls, COALESCE(SUM(estimated),0) AS est,
                COALESCE(SUM(costMicros),0) AS cost, MAX(costCurrency) AS currency,
                COALESCE(SUM(CASE WHEN costMicros IS NULL THEN 1 ELSE 0 END),0) AS unpriced
         FROM usage WHERE threadId = ?${filter.runId ? ' AND runId = ?' : ''}
         GROUP BY agentId, agentName, model, modelId
         ORDER BY MIN(createdAt)`,
        ...(filter.runId ? [threadId, filter.runId] : [threadId]),
      );
      const out = emptyTotals();
      for (const r of rows) {
        out.inputTokens += r.i;
        out.cachedInputTokens += r.cr;
        out.outputTokens += r.o;
        out.totalTokens += r.t;
        out.costMicros += r.cost;
        out.unpriced += r.unpriced;
        out.currency ??= r.currency ?? undefined;
        out.lines.push({
          agentId: r.agentId || null, agentName: r.agentName || null,
          model: r.model || null, modelId: r.modelId || null,
          inputTokens: r.i, cacheReadInputTokens: r.cr, cacheWriteInputTokens: r.cw,
          outputTokens: r.o, reasoningTokens: r.rt,
          calls: r.calls, estimated: r.est, costMicros: r.cost,
        });
      }
      return out;
    },
  };



}
