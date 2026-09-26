import { afterAll, describe, expect, it } from 'bun:test';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { simulateReadableStream, tool } from 'ai';
import type { LanguageModelV1StreamPart } from '@ai-sdk/provider';
import { MockLanguageModelV1 } from 'ai/test';
import { z } from 'zod';
import { setupAgentCore } from '../src/runtime.js';
import { MemoryAdminStore } from '../src/admin/memory.js';
import { SqliteAdminStore } from '../src/admin/sqlite.js';
import { PostgresAdminStore, type PgLike } from '../src/admin/postgres.js';
import { MemoryBus, MemoryKv, MemoryQueue, MemoryStorage, PUBLISHED_KEPT } from '../src/adapters/memory.js';
import { InlineQueue } from '../src/adapters/inline.js';
import { openSqlite, SqliteStorage } from '../src/adapters/sqlite.js';
import { PrismaStorage } from '../src/adapters/prisma.js';
import { markRequiresConfirmation } from '../src/core/engine.js';
import { runStats } from '../src/core/admin.js';
import { resolveConfig } from '../src/core/types.js';
import { DuplicateJobError, type Queue } from '../src/ports/queue.js';
import type { AdminStore } from '../src/ports/admin.js';
import type { RuntimePorts } from '../src/ports/runtime.js';

// Workstream I: every adapter keeps its port's promise under load and across
// processes. The cases both runtimes have run in the Go package too
// (adapters_hardening_test.go), under the same names.

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

describe('sqlite (§3.4)', () => {
  it('open turns on WAL and a busy timeout', async () => {
    const db = await openSqlite(join(mkdtempSync(join(tmpdir(), 'wal-')), 'wal.sqlite'));
    expect((db.prepare('PRAGMA journal_mode').all()[0] as any).journal_mode).toBe('wal');
    expect(Object.values(db.prepare('PRAGMA busy_timeout').all()[0] as any)[0]).toBe(5000);
  });

  it('many opening one fresh file at once all succeed', async () => {
    // The workers of a `next build` do this: each opens the same new file.
    const file = join(mkdtempSync(join(tmpdir(), 'race-')), 'race.sqlite');
    const script = `
      import { openSqlite } from ${JSON.stringify(new URL('../src/adapters/sqlite.ts', import.meta.url).pathname)};
      try { await openSqlite(${JSON.stringify(file)}); console.log('ok'); }
      catch (e) { console.log('FAIL ' + e.message); }`;
    const runs = Array.from({ length: 8 }, () =>
      Bun.spawn(['bun', '-e', script], { stdout: 'pipe', stderr: 'pipe' }));
    const out = await Promise.all(runs.map((p) => new Response(p.stdout).text()));
    expect(out.map((o) => o.trim())).toEqual(Array(8).fill('ok'));
  });

  it('storage: a missing thread cannot be deleted', async () => {
    const s = new SqliteStorage(await openSqlite(':memory:'));
    await expect(s.threads.delete('nope')).rejects.toThrow('Unknown thread');
    const th = await s.threads.create();
    await s.messages.append(th.id, { role: 'user', content: 'hi' });
    await s.threads.delete(th.id);
    expect(await s.messages.list(th.id)).toHaveLength(0); // its messages went with it
  });

  it('storage: a message seq cannot repeat', async () => {
    const db = await openSqlite(':memory:');
    const s = new SqliteStorage(db);
    const th = await s.threads.create();
    await s.messages.append(th.id, { role: 'user', content: 'one' });
    expect(() =>
      db.prepare(`INSERT INTO messages (id,threadId,role,content,createdAt,seq) VALUES ('dup',?,'user','"two"',0,1)`)
        .run(th.id),
    ).toThrow();
  });

  it('storage: only one claim wins', async () => {
    const s = new SqliteStorage(await openSqlite(':memory:'));
    const th = await s.threads.create();
    expect(await s.threads.claimState(th.id, 'IDLE', 'RUNNING')).toBe(true);
    expect(await s.threads.claimState(th.id, 'IDLE', 'RUNNING')).toBe(false); // the state moved on
  });
});

const pools: Array<{ end(): Promise<void> }> = [];
afterAll(async () => {
  await Promise.all(pools.map((p) => p.end().catch(() => undefined)));
});

/** The three admin stores, fresh; Postgres when TEST_ADMIN_PG is set. */
const adminStores: Record<string, (() => Promise<AdminStore>) | null> = {
  memory: async () => new MemoryAdminStore(),
  sqlite: async () => SqliteAdminStore.open(await openSqlite(':memory:')),
  postgres: process.env.TEST_ADMIN_PG
    ? async () => {
        const { Pool } = await import('pg');
        const pool = new Pool({ connectionString: process.env.TEST_ADMIN_PG });
        pools.push(pool);
        await pool.query('DROP TABLE IF EXISTS agentic_steps, agentic_runs, agentic_threads, agentic_migrations');
        return PostgresAdminStore.connect(pool as PgLike);
      }
    : null,
};

describe('admin store (§2.9)', () => {
  for (const [name, open] of Object.entries(adminStores)) {
    const run = open ? it : it.skip;

    run(`increment adds in one write (${name})`, async () => {
      const runs = (await open!()).runs;
      await runs.start({ id: 'inc', threadId: 't-inc', agent: 'chat', model: 'm' });
      await Promise.all(
        Array.from({ length: 20 }, () =>
          runs.increment('inc', { steps: 1, inputTokens: 0, cachedInputTokens: 0, outputTokens: 0, totalTokens: 10 })),
      );
      const rec = await runs.get('inc');
      expect(rec!.steps).toBe(20); // every step counted
      expect(rec!.totalTokens).toBe(200);
    });

    run(`stats count every run past the sample (${name})`, async () => {
      const store = await open!();
      for (let i = 0; i < 5; i++) {
        await store.runs.start({ id: `stat-${i}`, threadId: 't-stat', agent: 'chat', model: 'm' });
        await store.runs.increment(`stat-${i}`, {
          steps: 0, inputTokens: 0, cachedInputTokens: 0, outputTokens: 0, totalTokens: 10,
        });
      }
      const stats = await runStats({ admin: store } as RuntimePorts, { limit: 2 });
      expect(stats.total).toBe(5); // every run counted
      expect(stats.tokens.totalTokens).toBe(50);
      expect(stats.sampled).toBe(true); // the percentiles say they are a sample
    });
  }

  it('a patch writes only run columns', async () => {
    const store = SqliteAdminStore.open(await openSqlite(':memory:'));
    await store.runs.start({ id: 'p1', threadId: 't', agent: 'chat', model: 'm' });
    // An object in a JSON column is encoded; a key that is no column is left
    // out rather than turned into SQL.
    await store.runs.patch('p1', { result: { ok: true }, ...({ 'x = 1; --': 2 } as object) });
    expect((await store.runs.get('p1'))!.result).toEqual({ ok: true });
  });
});

function scriptedModel() {
  return new MockLanguageModelV1({
    provider: 'mock',
    modelId: 'mock',
    doStream: async () => {
      const chunks: LanguageModelV1StreamPart[] = [
        { type: 'tool-call', toolCallType: 'function', toolCallId: 'c1', toolName: 'wipe', args: '{}' },
        { type: 'finish', finishReason: 'tool-calls', usage: { promptTokens: 10, completionTokens: 5 } },
      ];
      return { stream: simulateReadableStream({ chunks }), rawCall: { rawPrompt: null, rawSettings: {} } };
    },
  });
}

async function parkedRuntime() {
  const storage = new MemoryStorage();
  const queue = new MemoryQueue();
  const admin = new MemoryAdminStore();
  const model = scriptedModel();
  const runtime = await setupAgentCore({
    storage, queue, admin, bus: new MemoryBus(), kv: new MemoryKv(),
    resolveModel: () => ({ instance: () => model, contextWindow: 128_000 }),
    config: resolveConfig({ hitlTtlMs: 60 * 60_000 }),
  });
  const wipe = markRequiresConfirmation(tool({ parameters: z.object({}), execute: async () => 'wiped' }));
  const chat = runtime.createStreamTextAgent({ name: 'chat', tools: { wipe } });
  const ran = await chat.run({ prompt: 'delete' });
  await runtime.worker.handleJob(queue.items.shift()!);
  queue.delays.shift();
  return { runtime, queue, admin, ran };
}

describe('queues (§2.8)', () => {
  const queues: Record<string, () => Queue> = { memory: () => new MemoryQueue(), inline: () => new InlineQueue() };
  for (const [name, make] of Object.entries(queues)) {
    it(`a keyed job is queued once (${name})`, async () => {
      const q = make();
      const job = { threadId: 't1', runId: 'r1', model: 'm', kind: 'expiry' as const };
      await q.enqueue(job, { key: 'hitl-expiry:c1', delaySeconds: 3600 });
      await expect(q.enqueue(job, { key: 'hitl-expiry:c1', delaySeconds: 3600 })).rejects.toBeInstanceOf(DuplicateJobError);
      expect((await q.find('r1'))?.threadId).toBe('t1'); // the run's job is found
      expect((await q.stats()).delayed).toBe(1);
      await q.cancel('hitl-expiry:c1');
      expect(await q.find('r1')).toBeNull(); // cancel withdrew it
      if (q instanceof InlineQueue) q.clear();
    });
  }

  it('inline queue: a job due before bind is held, not dropped', async () => {
    const q = new InlineQueue();
    await q.enqueue({ threadId: 'early', model: 'm' });
    await sleep(20); // due, and nobody bound yet
    const got: string[] = [];
    q.bind(async (job) => { got.push(job.threadId); });
    await sleep(10);
    expect(got).toEqual(['early']);
  });

  it('inline queue: a long delay does not fire at once', async () => {
    const q = new InlineQueue();
    let ran = false;
    q.bind(async () => { ran = true; });
    await q.enqueue({ threadId: 'later', model: 'm' }, { delaySeconds: 30 * 24 * 3600 });
    await sleep(30);
    expect(ran).toBe(false); // a 30-day delay waits
    expect((await q.stats()).delayed).toBe(1);
    q.clear();
  });

  it("hitl: an answer withdraws the park's expiry row", async () => {
    const { runtime, queue, ran } = await parkedRuntime();
    expect(queue.items.map((j) => queue.keyOf(j))).toContain('hitl-expiry:c1');
    await runtime.hitl.respond({ threadId: ran.threadId, toolCallId: 'c1', approved: true });
    const keys = queue.items.map((j) => queue.keyOf(j));
    expect(keys).not.toContain('hitl-expiry:c1');
    expect(keys).toContain('hitl-resume:c1');
  });

  it('delete: a deleted thread leaves nothing in the admin store', async () => {
    const { runtime, admin, ran } = await parkedRuntime();
    expect((await runtime.deleteThread(ran.threadId)).accepted).toBe(true);
    expect(await admin.runs.get(ran.runId!)).toBeNull(); // the run record is gone with the thread
    expect(await admin.threads.get(ran.threadId)).toBeNull();
    expect(await admin.steps.listByThread(ran.threadId)).toHaveLength(0);
  });
});

describe('memory adapters', () => {
  it('memory bus keeps only the latest events', async () => {
    const bus = new MemoryBus();
    for (let i = 0; i < PUBLISHED_KEPT + 5; i++) {
      await bus.publish('t', { threadId: 't', seq: i + 1, type: 'X', payload: null, createdAt: new Date() });
    }
    expect(bus.published).toHaveLength(PUBLISHED_KEPT); // bounded
    expect(bus.published[0]!.seq).toBe(6); // the oldest are the ones dropped
  });
});

describe('prisma storage (§3.2)', () => {
  it('orders messages by seq and keeps seq out of what it returns', async () => {
    const calls: any[] = [];
    const rows = [
      { id: 'a', threadId: 't', role: 'user', content: 'one', seq: 1n },
      { id: 'b', threadId: 't', role: 'assistant', content: 'two', seq: 2n },
    ];
    const prisma: any = {
      message: {
        create: async ({ data }: any) => ({ id: 'c', ...data, seq: 3n }),
        findMany: async (a: any) => { calls.push(['findMany', a]); return rows; },
        findFirst: async (a: any) => { calls.push(['findFirst', a]); return { seq: 2n }; },
        deleteMany: async (a: any) => { calls.push(['deleteMany', a]); return { count: 1 }; },
      },
    };
    const s = new PrismaStorage(prisma);
    const listed = await s.messages.list('t');
    expect(calls[0][1].orderBy).toEqual({ seq: 'asc' }); // the order they were written
    expect(JSON.stringify(listed)).not.toContain('seq'); // a BigInt JSON cannot carry
    expect(JSON.stringify(await s.messages.append('t', { role: 'user', content: 'x' }))).not.toContain('seq');
    expect(await s.messages.deleteFrom('t', 'b')).toBe(1);
    expect(calls.at(-1)).toEqual(['deleteMany', { where: { threadId: 't', seq: { gte: 2n } } }]);
  });
});
