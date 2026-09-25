import { describe, expect, it } from 'bun:test';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { MemoryRunStreams } from '../src/adapters/memory.js';
import { openSqlite, SqliteRunStreams } from '../src/adapters/sqlite.js';
import { parseStreamId, streamIdOf } from '../src/core/stream-events.js';
import { runStreamsSuite } from './run-streams-suite.js';

runStreamsSuite('memory', async () => new MemoryRunStreams());

// SQLite on a file, so a second handle is another process's view.
const sqliteFile = join(mkdtempSync(join(tmpdir(), 'streams-')), 'streams.sqlite');
const openSqliteStreams = async () => new SqliteRunStreams(await openSqlite(sqliteFile), { pollMs: 50 });
runStreamsSuite('sqlite', openSqliteStreams, { other: openSqliteStreams, settleMs: 100 });

describe('stream ids', () => {
  it('a stream id names its run and segment', () => {
    expect(streamIdOf('run_1', 2)).toBe('run_1:2');
    expect(parseStreamId('run_1:2')).toEqual({ runId: 'run_1', segment: 2 });
    expect(parseStreamId('run:with:colons:3')).toEqual({ runId: 'run:with:colons', segment: 3 });
    expect(parseStreamId('run_1')).toBeNull();
    expect(parseStreamId('run_1:0')).toBeNull();
  });
});

const redisAddr = process.env.TEST_REDIS_ADDR;
if (redisAddr) {
  const { createClient } = await import('redis');
  const { RedisRunStreams } = await import('../src/adapters/redis.js');
  const connect = async () => {
    const client = createClient({ url: `redis://${redisAddr}` });
    await client.connect();
    return client;
  };
  const shared = await connect();
  runStreamsSuite('redis', async () => new RedisRunStreams(shared as any, { pollMs: 1_000 }), {
    // Another process: its own client, so no in-process wake-up reaches the
    // reader. The tail still sees the XADD, as it would across processes.
    other: async () => new RedisRunStreams((await connect()) as any),
  });
}

if (redisAddr) {
  const { createClient } = await import('redis');
  const { UpstashRunStreams } = await import('../src/adapters/upstash.js');
  const client = createClient({ url: `redis://${redisAddr}` });
  await client.connect();
  // Upstash's client parses every string reply that is JSON. Doing the same
  // here proves the adapter copes with it.
  const parseDeep = (v: unknown): unknown => {
    if (Array.isArray(v)) return v.map(parseDeep);
    if (typeof v === 'string') {
      try { return JSON.parse(v); } catch { return v; }
    }
    return v;
  };
  const upstashLike: any = {
    eval: async (script: string, keys: string[], args: string[]) =>
      parseDeep(await client.eval(script, { keys, arguments: args })),
  };
  runStreamsSuite('upstash', async () => new UpstashRunStreams(upstashLike, { pollMs: 50 }), {
    other: async () => new UpstashRunStreams(upstashLike, { pollMs: 50 }),
    settleMs: 100,
  });
}

// Prisma against a real database: TEST_PRISMA_DB is a Postgres URL with the
// example app's migrations applied, and the example's generated client is
// the one used.
const prismaDb = process.env.TEST_PRISMA_DB;
if (prismaDb) {
  const { createRequire } = await import('node:module');
  const require = createRequire(join(import.meta.dir, '../../../examples/nextjs-app/package.json'));
  const { PrismaClient } = require('@prisma/client');
  const { PrismaRunStreams } = await import('../src/adapters/prisma.js');
  const prisma = new PrismaClient({ datasources: { db: { url: prismaDb } } });
  const other = new PrismaClient({ datasources: { db: { url: prismaDb } } });
  runStreamsSuite('prisma', async () => new PrismaRunStreams(prisma, { pollMs: 50 }), {
    other: async () => new PrismaRunStreams(other, { pollMs: 50 }),
    settleMs: 100,
  });
}
