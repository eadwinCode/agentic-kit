import { describe, expect, it } from 'bun:test';
import { join } from 'node:path';
import { setupAgentCore } from '../src/runtime.js';
import { MemoryAdminStore } from '../src/admin/memory.js';
import { MemoryBus, MemoryKv, MemoryQueue, MemoryRunStreams, MemoryStorage } from '../src/adapters/memory.js';
import { openSqlite, SqliteStorage } from '../src/adapters/sqlite.js';
import { resolveConfig } from '../src/core/types.js';
import type { Storage } from '../src/ports/storage.js';

// pruneEvents: the stream-only rows older releases left in the event table
// go, in batches; an app's own types and the record stay. The Go package
// runs the same cases under the same names (prune_test.go).

const stores: Array<[string, () => Promise<Storage>]> = [
  ['memory', async () => new MemoryStorage()],
  ['sqlite', async () => new SqliteStorage(await openSqlite(':memory:'))],
];
if (process.env.TEST_PRISMA_DB) {
  stores.push(['prisma', async () => {
    const { createRequire } = await import('node:module');
    const require = createRequire(join(import.meta.dir, '../../../examples/nextjs-app/package.json'));
    const { PrismaClient } = require('@prisma/client');
    const prisma = new PrismaClient({ datasources: { db: { url: process.env.TEST_PRISMA_DB } } });
    // Start from an empty table, so the counts are this test's.
    await prisma.agentEvent.deleteMany({});
    const { PrismaStorage } = await import('../src/adapters/prisma.js');
    return new PrismaStorage(prisma);
  }]);
}

describe('prune events', () => {
  for (const [name, open] of stores) {
    it(`old stream-only rows go in batches; the record and an app's types stay (${name})`, async () => {
      const storage = await open();
      const runtime = await setupAgentCore({
        storage, admin: new MemoryAdminStore(), bus: new MemoryBus(), queue: new MemoryQueue(), kv: new MemoryKv(),
        streams: new MemoryRunStreams(),
        resolveModel: () => ({ instance: () => ({}) as any, contextWindow: 128_000 }),
        config: resolveConfig(),
      });
      const ctx = { state: {} };
      const threads = [await storage.threads.create({}, ctx), await storage.threads.create({}, ctx)];
      // What an older release wrote: chunks and state changes beside the
      // record and an app's own event.
      for (const t of threads) {
        for (const type of ['CHUNK', 'CHUNK', 'STATE_CHANGE', 'INPUT_REQUIRED', 'INVOICE_CREATED', 'STEP_COMMITTED']) {
          await storage.events.append(t.id, { type, payload: {} }, ctx);
        }
      }

      const dry = await runtime.pruneEvents({ dryRun: true });
      expect(dry.byType).toEqual({ CHUNK: 4, STATE_CHANGE: 2, STEP_COMMITTED: 2 });
      expect(dry.total).toBe(8);
      expect(await storage.events.listSince(threads[0]!.id, -1, ctx)).toHaveLength(6); // nothing went

      const done = await runtime.pruneEvents({ batchSize: 3 });
      expect(done.total).toBe(8);
      expect(done.batches).toBeGreaterThan(2); // a batch at a time
      for (const t of threads) {
        const left = (await storage.events.listSince(t.id, -1, ctx)).map((e) => e.type);
        expect(left).toEqual(['INPUT_REQUIRED', 'INVOICE_CREATED']);
      }
      expect((await runtime.pruneEvents()).total).toBe(0); // safe to run again
    });
  }
});
