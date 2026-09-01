import type { AgentEvent } from '../core/types.js';
import type { Kv } from '../ports/kv.js';
import type { EventBus } from '../ports/bus.js';

/** Minimal structural type of the @upstash/redis client we use — the real
 *  client satisfies it without importing the SDK here. */
export interface UpstashRedisLike {
  get(key: string): Promise<string | null>;
  set(key: string, value: string, opts?: { ex?: number; nx?: boolean }): Promise<unknown>;
  del(...keys: string[]): Promise<unknown>;
  incr(key: string): Promise<number>;
  publish(channel: string, value: string): Promise<unknown>;
}

/** Reference Kv adapter over Upstash Redis. */
export class UpstashKv implements Kv {
  constructor(private readonly redis: UpstashRedisLike) {}

  get(key: string) { return this.redis.get(key); }
  async set(
    key: string,
    value: string,
    opts?: { exSeconds?: number; onlyIfNotExists?: boolean },
  ): Promise<boolean> {
    const res = await this.redis.set(
      key,
      value,
      opts?.onlyIfNotExists
        ? { nx: true, ex: opts.exSeconds }
        : opts?.exSeconds
          ? { ex: opts.exSeconds }
          : undefined,
    );
    // SET NX returns null when the key exists
    return opts?.onlyIfNotExists ? res === 'OK' : true;
  }
  del(key: string) { return this.redis.del(key).then(() => undefined); }
  incr(key: string) { return this.redis.incr(key); }
}

/** Upstash Pub/Sub over REST requires a WebSocket-based subscriber; the Redis
 *  REST client's `subscribe` differs per SDK version. Wire it in explicitly:
 *
 *    new UpstashBus(publisherRedis, {
 *      subscribe: (threadId, handler) => {
 *        // e.g. `redis.subscribe(channel, cb)` from a WS-enabled client
 *      },
 *    })
 */
export interface UpstashSubscriberLike {
  subscribe(threadId: string, handler: (raw: string) => void): Promise<() => void>;
}

export const THREAD_CHANNEL = (threadId: string) => `thread:${threadId}:events`;

export class UpstashBus implements EventBus {
  constructor(
    private readonly redis: UpstashRedisLike,
    private readonly subscriber?: UpstashSubscriberLike,
  ) {}

  async publish(threadId: string, event: AgentEvent) {
    await this.redis.publish(THREAD_CHANNEL(threadId), JSON.stringify(event));
  }

  async subscribe(threadId: string, handler: (event: AgentEvent) => void) {
    if (!this.subscriber) {
      throw new Error(
        'UpstashBus requires a subscriber (WebSocket-based) for live tailing — ' +
          'see the UpstashSubscriberLike docblock. Replay-only usage works without one.',
      );
    }
    return this.subscriber.subscribe(threadId, (raw) => {
      try {
        handler(JSON.parse(raw) as AgentEvent);
      } catch {
        // malformed frame — never kill the subscription
      }
    });
  }
}
