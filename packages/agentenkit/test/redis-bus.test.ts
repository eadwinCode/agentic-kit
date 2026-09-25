import { describe, expect, it } from 'bun:test';
import { createClient } from 'redis';
import { RedisBus } from '../src/adapters/redis.js';
import type { AgentEvent } from '../src/core/types.js';

// Workstream G: the Redis bus shares one subscriber connection per process.
// The same cases run in the Go package (redis_bus_test.go). Needs
// TEST_REDIS_ADDR (host:port).
const addr = process.env.TEST_REDIS_ADDR;
const suite = addr ? describe : describe.skip;

async function connect() {
  const client = createClient({ url: `redis://${addr}` });
  await client.connect();
  return client;
}

async function subscriberConnections(client: any): Promise<number> {
  const list: string = await client.sendCommand(['CLIENT', 'LIST']);
  return list.split('\n').filter((l) => / sub=[1-9]/.test(l)).length;
}

const event = (threadId: string): AgentEvent =>
  ({ threadId, seq: 1, type: 'X', payload: null, createdAt: new Date() }) as AgentEvent;

suite('RedisBus', () => {
  it('many subscribers share one connection', async () => {
    const client = await connect();
    const before = await subscriberConnections(client);
    const bus = new RedisBus(client as any, 3_600_000);
    const got = new Map<string, number>();
    const stops: Array<() => Promise<void>> = [];
    for (let i = 0; i < 20; i++) {
      const thread = `rb-ts-${'abcde'[i % 5]}`;
      stops.push(await bus.subscribe(thread, (e) => got.set(e.threadId, (got.get(e.threadId) ?? 0) + 1)));
    }
    expect((await subscriberConnections(client)) - before).toBe(1);
    for (const t of 'abcde') await bus.publish(`rb-ts-${t}`, event(`rb-ts-${t}`));
    for (let i = 0; i < 100 && [...'abcde'].some((t) => got.get(`rb-ts-${t}`) !== 4); i++) {
      await new Promise((r) => setTimeout(r, 20));
    }
    for (const t of 'abcde') expect(got.get(`rb-ts-${t}`)).toBe(4);
    for (const stop of stops) await stop();
    const numsub: any = await client.sendCommand(['PUBSUB', 'NUMSUB', 'thread:rb-ts-a:events']);
    expect(Number(numsub[1])).toBe(0); // released with its last subscriber
    await client.quit();
  });

  it('turns createdAt back into a Date', async () => {
    const client = await connect();
    const bus = new RedisBus(client as any, 3_600_000);
    let seen: AgentEvent | null = null;
    const stop = await bus.subscribe('rb-ts-date', (e) => (seen = e));
    await bus.publish('rb-ts-date', event('rb-ts-date'));
    for (let i = 0; i < 100 && !seen; i++) await new Promise((r) => setTimeout(r, 20));
    expect(seen!.createdAt).toBeInstanceOf(Date);
    await stop();
    await client.quit();
  });
});
