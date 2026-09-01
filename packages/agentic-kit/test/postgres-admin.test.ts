import { describe, expect, it, beforeAll, afterAll } from 'bun:test';
import { PostgresAdminStore, type PgLike } from '../src/admin/postgres.js';

// A real database, because a store that only satisfies a mock proves nothing
// about the SQL.
//
// Unreachable is a FAILURE, not a skip. A suite that quietly passes with zero
// assertions is worse than no suite: it reads as green while covering nothing.
// Set SKIP_PG_TESTS=1 to opt out deliberately.
const URL = process.env.TEST_ADMIN_PG ?? 'postgresql://postgres:password@localhost:5433/agentic_admin_test';

let pool: any;
let store: PostgresAdminStore;
let reachable = true;
let reason = '';

beforeAll(async () => {
  try {
    const { Pool } = await import('pg');
    pool = new Pool({ connectionString: URL, connectionTimeoutMillis: 2_000 });
    await pool.query('SELECT 1');
    // A clean slate each run, so counts are assertions and not accumulations.
    await pool.query('DROP TABLE IF EXISTS agentic_steps, agentic_runs, agentic_threads');
    store = await PostgresAdminStore.connect(pool as PgLike);
  } catch (err) {
    reachable = false;
    reason = err instanceof Error ? err.message : String(err);
  }
});

/** Every test calls this first, so an unreachable database fails loudly. */
function requireDb() {
  if (reachable) return true;
  if (process.env.SKIP_PG_TESTS) return false;
  throw new Error(
    `Postgres is required for these tests but was unreachable at ${URL} — ${reason}. ` +
      'Start it, point TEST_ADMIN_PG elsewhere, or set SKIP_PG_TESTS=1 to opt out.',
  );
}

afterAll(async () => {
  if (pool) {
    await pool.query('DROP TABLE IF EXISTS agentic_steps, agentic_runs, agentic_threads')
      .catch(() => undefined);
    await pool.end().catch(() => undefined);
  }
});

describe('PostgresAdminStore (§2.9)', () => {
  it('creates its schema and is safe to connect twice', async () => {
    if (!requireDb()) return;
    // CREATE TABLE IF NOT EXISTS everywhere: a second process starting up must
    // not race the first into an error.
    await expect(PostgresAdminStore.connect(pool as PgLike)).resolves.toBeDefined();
  });

  it('round-trips a run through its lifecycle', async () => {
    if (!requireDb()) return;
    const run = await store.runs.start({
      id: 'pg-r1', threadId: 'pg-t1', agent: 'chat', model: 'gpt-4o',
    });
    expect(run).toMatchObject({ id: 'pg-r1', depth: 0, parentRunId: null, state: 'RUNNING' });
    expect(run.startedAt).toBeInstanceOf(Date);

    const endedAt = new Date();
    await store.runs.patch('pg-r1', {
      state: 'COMPLETED', stopReason: 'completed', endedAt,
      durationMs: 42, steps: 3, totalTokens: 90, result: { text: 'done' },
    });

    const got = (await store.runs.get('pg-r1'))!;
    expect(got).toMatchObject({ state: 'COMPLETED', durationMs: 42, steps: 3, totalTokens: 90 });
    expect(got.endedAt).toBeInstanceOf(Date);
    // JSONB comes back parsed, not as a string.
    expect(got.result).toEqual({ text: 'done' });
  });

  it('keeps a nested run in the same table, by depth and parent', async () => {
    if (!requireDb()) return;
    await store.runs.start({
      id: 'pg-r2', threadId: 'pg-t1', agent: 'kid', model: 'gpt-4o',
      depth: 1, parentRunId: 'pg-r1',
    });
    const rows = await store.runs.listByThread('pg-t1');
    expect(rows.map((r) => r.depth).sort()).toEqual([0, 1]);
    expect(rows.find((r) => r.depth === 1)!.parentRunId).toBe('pg-r1');
  });

  it('filters runs by state, agent and time', async () => {
    if (!requireDb()) return;
    expect((await store.runs.list({ state: ['COMPLETED'] })).map((r) => r.id)).toEqual(['pg-r1']);
    expect((await store.runs.list({ agent: 'kid' })).map((r) => r.id)).toEqual(['pg-r2']);
    expect(await store.runs.list({ since: new Date(Date.now() + 60_000) })).toEqual([]);
    expect(await store.runs.countByState()).toMatchObject({ COMPLETED: 1, RUNNING: 1 });
    // The limit is honoured, so "show me everything" can never be a table scan.
    expect(await store.runs.list({ limit: 1 })).toHaveLength(1);
  });

  it('upserts a thread rather than duplicating it', async () => {
    if (!requireDb()) return;
    await store.threads.upsert({ id: 'pg-t1', state: 'RUNNING', model: 'gpt-4o' });
    const first = (await store.threads.list({}))[0]!;
    await store.threads.upsert({ id: 'pg-t1', state: 'COMPLETED', model: 'gpt-4o' });

    const rows = await store.threads.list({});
    expect(rows).toHaveLength(1);
    expect(rows[0]!.state).toBe('COMPLETED');
    // firstSeenAt survives the update; updatedAt moves.
    expect(rows[0]!.firstSeenAt.getTime()).toBe(first.firstSeenAt.getTime());
    expect(await store.threads.countByState()).toEqual({ COMPLETED: 1 });
  });

  it('stores steps in order, with their tool list', async () => {
    if (!requireDb()) return;
    for (const index of [2, 1]) {
      await store.steps.record({
        runId: 'pg-r1', agentId: null, index, durationMs: index * 10,
        finishReason: 'stop', inputTokens: 10, cachedInputTokens: 0,
        outputTokens: 5, totalTokens: 15, tools: ['sendEmail'],
      });
    }
    const steps = await store.steps.listByRun('pg-r1');
    expect(steps.map((s) => s.index)).toEqual([1, 2]);   // ordered, not insertion order
    expect(steps[0]!.tools).toEqual(['sendEmail']);      // JSONB round-trip
    expect(steps[0]!.at).toBeInstanceOf(Date);
  });
});

describe('AGENTIC_KIT_ADMIN_DATABASE_URL (§2.9)', () => {
  it('selects Postgres for the default store, and a real run lands in it', async () => {
    if (!requireDb()) return;
    const { setupAgentCore } = await import('../src/runtime.js');
    const { MemoryBus, MemoryKv, MemoryQueue, MemoryStorage } = await import(
      '../src/adapters/memory.js'
    );
    const { MockLanguageModelV1 } = await import('ai/test');
    const { simulateReadableStream } = await import('ai');

    const before = process.env.AGENTIC_KIT_ADMIN_DATABASE_URL;
    process.env.AGENTIC_KIT_ADMIN_DATABASE_URL = URL;
    try {
      const queue = new MemoryQueue();
      // No `admin` passed: the env var alone has to route it to Postgres.
      const runtime = await setupAgentCore({
        storage: new MemoryStorage(), bus: new MemoryBus(), queue, kv: new MemoryKv(),
        resolveModel: () => ({
          instance: () =>
            new MockLanguageModelV1({
              provider: 'mock', modelId: 'mock',
              doStream: async () => ({
                stream: simulateReadableStream({
                  chunks: [
                    { type: 'text-delta', textDelta: 'ok' },
                    {
                      type: 'finish', finishReason: 'stop',
                      usage: { promptTokens: 10, completionTokens: 5 },
                    },
                  ] as any,
                }),
                rawCall: { rawPrompt: null, rawSettings: {} },
              }),
            }) as any,
          contextWindow: 128_000,
        }),
      });

      const chat = runtime.createStreamTextAgent({ name: 'chat', model: 'gpt-4o' });
      const ran = await chat.run({ prompt: 'hi' });
      await runtime.worker.handleJob(queue.items[0]!);

      // Read it back through the store, and again straight from the table.
      const detail = (await runtime.admin.getRun(ran.runId!))!;
      expect(detail.run).toMatchObject({ state: 'COMPLETED', steps: 1, totalTokens: 15 });
      expect(detail.steps).toHaveLength(1);

      const { rows } = await pool.query('SELECT * FROM agentic_runs WHERE id = $1', [ran.runId]);
      expect(rows[0].state).toBe('COMPLETED');
      expect(rows[0].totalTokens).toBe(15);

      // The thread index is the platform's own, written on every transition.
      const threads = await pool.query('SELECT * FROM agentic_threads WHERE id = $1', [ran.threadId]);
      expect(threads.rows[0].state).toBe('COMPLETED');
    } finally {
      if (before === undefined) delete process.env.AGENTIC_KIT_ADMIN_DATABASE_URL;
      else process.env.AGENTIC_KIT_ADMIN_DATABASE_URL = before;
    }
  }, 15_000);
});
