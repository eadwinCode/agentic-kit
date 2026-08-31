import type { AgentEvent } from '../core/types.js';
import type { Kv } from '../ports/kv.js';
import type { EventBus } from '../ports/bus.js';
import { THREAD_CHANNEL } from './upstash.js';

/** Minimal structural type of a node-redis (v4) client — the real client
 *  satisfies it without importing the SDK here. Works against any Redis:
 *  local Docker, self-hosted, or managed. */
export interface RedisClientLike {
  connect(): Promise<void>;
  get(key: string): Promise<string | null>;
  set(key: string, value: string, opts?: { EX?: number }): Promise<unknown>;
  del(...keys: string[]): Promise<unknown>;
  incr(key: string): Promise<number>;
  publish(channel: string, value: string): Promise<unknown>;
  duplicate(): RedisSubscriberLike;
}

/** A dedicated connection for subscriptions (node-redis `client.duplicate()`). */
export interface RedisSubscriberLike {
  connect(): Promise<void>;
  subscribe(channel: string, handler: (message: string) => void): Promise<void>;
  unsubscribe(channel: string): Promise<unknown>;
  quit(): Promise<unknown>;
  on(event: string, handler: (...args: string[]) => void): unknown;
}

/** Reference Kv adapter over plain Redis (node-redis). */
export class RedisKv implements Kv {
  constructor(private readonly redis: RedisClientLike) {}

  get(key: string) { return this.redis.get(key); }
  async set(key: string, value: string, opts?: { exSeconds?: number }) {
    await this.redis.set(key, value, opts?.exSeconds ? { EX: opts.exSeconds } : undefined);
  }
  del(key: string) { return this.redis.del(key).then(() => undefined); }
  incr(key: string) { return this.redis.incr(key); }
}

/** Reference EventBus adapter over plain Redis Pub/Sub (node-redis).
 *
 *  While subscribed, emits a bus-only `HEARTBEAT` notice (seq 0, never
 *  persisted) every heartbeatMs — the §2.5 watchdog pattern: pub/sub is
 *  at-most-once, so the SSE distributor treats heartbeats as a trigger to
 *  re-check for orphaned HITL waits. */
export class RedisBus implements EventBus {
  constructor(
    private readonly client: RedisClientLike,
    private readonly heartbeatMs = 60_000,
  ) {}

  async publish(threadId: string, event: AgentEvent) {
    await this.client.publish(THREAD_CHANNEL(threadId), JSON.stringify(event));
  }

  async subscribe(threadId: string, handler: (event: AgentEvent) => void) {
    const sub = this.client.duplicate();
    await sub.connect();
    await sub.subscribe(THREAD_CHANNEL(threadId), (message) => {
      try {
        handler(JSON.parse(message) as AgentEvent);
      } catch {
        // malformed frame — never kill the subscription
      }
    });

    const heartbeat = setInterval(() => {
      handler({
        threadId, seq: 0, type: 'HEARTBEAT', payload: null, createdAt: new Date(),
      } as AgentEvent);
    }, this.heartbeatMs);

    return async () => {
      clearInterval(heartbeat);
      await sub.unsubscribe(THREAD_CHANNEL(threadId)).catch(() => undefined);
      await sub.quit().catch(() => undefined);
    };
  }
}
