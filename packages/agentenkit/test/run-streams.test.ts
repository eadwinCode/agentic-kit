import { describe, expect, it } from 'bun:test';
import { MemoryRunStreams } from '../src/adapters/memory.js';
import { parseStreamId, streamIdOf } from '../src/core/stream-events.js';
import { runStreamsSuite } from './run-streams-suite.js';

runStreamsSuite('memory', async () => new MemoryRunStreams());

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
