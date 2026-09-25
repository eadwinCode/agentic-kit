import { afterEach, beforeEach, describe, expect, it } from 'bun:test';
import { gatedAdminStore } from '../src/admin/migrations/runner.js';
import { PostgresAdminStore, type PgLike } from '../src/admin/postgres.js';
import { InlineQueue } from '../src/adapters/inline.js';
import { RedisBus, type RedisClientLike } from '../src/adapters/redis.js';
import type { AdminStore } from '../src/ports/admin.js';
import type { RunJob } from '../src/core/types.js';

// Workstream A: an adapter error, a failed migration or a job that arrives
// early must never end the process or vanish without a trace.

const tick = () => new Promise((r) => setTimeout(r, 5));

/** Collects unhandled rejections for the length of a test. */
let unhandled: unknown[] = [];
const onUnhandled = (err: unknown) => unhandled.push(err);
const events = process as unknown as {
  on(event: 'unhandledRejection', fn: (err: unknown) => void): void;
  off(event: 'unhandledRejection', fn: (err: unknown) => void): void;
};
beforeEach(() => {
  unhandled = [];
  events.on('unhandledRejection', onUnhandled);
});
afterEach(() => {
  events.off('unhandledRejection', onUnhandled);
});

/** An AdminStore whose every call answers `ok`. */
const fakeStore = (): AdminStore =>
  ({
    threads: { countByState: async () => 'ok' },
    runs: { get: async () => 'ok' },
    steps: {},
  }) as unknown as AdminStore;

describe('gatedAdminStore', () => {
  it('a failed migration is not an unhandled rejection', async () => {
    gatedAdminStore(fakeStore(), async () => {
      throw new Error('database down');
    });
    await tick();
    expect(unhandled).toEqual([]);
  });

  it('fails fast after a failure, then runs the migration again once the wait is over', async () => {
    let clock = 0;
    let runs = 0;
    let fail = true;
    const store = gatedAdminStore(
      fakeStore(),
      async () => {
        runs++;
        if (fail) throw new Error('database down');
      },
      { retryMs: 1_000, now: () => clock },
    );
    await expect(store.runs.get('r1')).rejects.toThrow('database down');
    expect(runs).toBe(1);

    clock = 500; // inside the wait: no new attempt
    await expect(store.runs.get('r1')).rejects.toThrow('database down');
    expect(runs).toBe(1);

    fail = false;
    clock = 1_000; // the database is back
    expect(await store.runs.get('r1')).toBe('ok' as never);
    expect(runs).toBe(2);
    expect(await store.threads.countByState()).toBe('ok' as never);
    expect(runs).toBe(2);
  });

  it('waits for a migration still running', async () => {
    let finish!: () => void;
    const store = gatedAdminStore(fakeStore(), () => new Promise<void>((r) => (finish = r)));
    let answered = false;
    const call = store.runs.get('r1').then(() => (answered = true));
    await tick();
    expect(answered).toBe(false);
    finish();
    await call;
    expect(answered).toBe(true);
  });
});

describe('PostgresAdminStore.connect', () => {
  const pool = () => {
    const listeners: Array<(err: unknown) => void> = [];
    const db = {
      query: async () => {
        throw new Error('connection refused');
      },
      on: (_: 'error', fn: (err: unknown) => void) => listeners.push(fn),
      listenerCount: () => listeners.length,
    };
    return { db: db as unknown as PgLike, listeners };
  };
  const quiet = { error: () => undefined };

  it("listens for the pool's 'error', so a dropped idle connection does not end the process", async () => {
    const { db, listeners } = pool();
    PostgresAdminStore.connect(db, quiet);
    expect(listeners.length).toBe(1);
    expect(() => listeners[0](new Error('terminated'))).not.toThrow();
    await tick();
    expect(unhandled).toEqual([]); // the failed migration is handled too
  });

  it("leaves a caller's own listener alone", () => {
    const { db, listeners } = pool();
    (db as any).on('error', () => undefined);
    PostgresAdminStore.connect(db, quiet);
    expect(listeners.length).toBe(1);
  });
});

describe('InlineQueue', () => {
  const job = (threadId: string) => ({ threadId, model: 'm' }) as RunJob;

  it('holds a job that comes due before bind, and delivers it once bound', async () => {
    const queue = new InlineQueue();
    await queue.enqueue(job('early'));
    await tick();
    const seen: string[] = [];
    queue.bind(async (j) => void seen.push(j.threadId));
    await tick();
    expect(seen).toEqual(['early']);
  });

  it('logs a failed job instead of swallowing it', async () => {
    const queue = new InlineQueue();
    const logged: unknown[] = [];
    const original = console.error;
    console.error = (...args: unknown[]) => void logged.push(args);
    try {
      queue.bind(async () => {
        throw new Error('worker broke');
      });
      await queue.enqueue(job('t1'));
      await tick();
      await tick();
    } finally {
      console.error = original;
    }
    expect(logged.length).toBe(1);
    expect(unhandled).toEqual([]);
  });
});

describe('RedisBus', () => {
  const client = (subscribeFails = false) => {
    const sub = {
      listeners: {} as Record<string, (err: unknown) => void>,
      quits: 0,
      on(event: string, fn: (err: unknown) => void) {
        this.listeners[event] = fn;
      },
      connect: async () => undefined,
      subscribe: async () => {
        if (subscribeFails) throw new Error('subscribe refused');
      },
      unsubscribe: async () => undefined,
      quit: async () => {
        sub.quits++;
      },
    };
    const redis = { duplicate: () => sub } as unknown as RedisClientLike;
    return { redis, sub };
  };

  it("listens for the subscriber's 'error', so a failover does not end the process", async () => {
    const { redis, sub } = client();
    const original = console.error;
    console.error = () => undefined;
    try {
      const stop = await new RedisBus(redis, 60_000).subscribe('t1', () => undefined);
      expect(typeof sub.listeners.error).toBe('function');
      expect(() => sub.listeners.error(new Error('ECONNRESET'))).not.toThrow();
      await stop();
    } finally {
      console.error = original;
    }
  });

  it('closes its connection when the subscribe fails', async () => {
    const { redis, sub } = client(true);
    await expect(new RedisBus(redis, 60_000).subscribe('t1', () => undefined)).rejects.toThrow(
      'subscribe refused',
    );
    expect(sub.quits).toBe(1);
  });
});
