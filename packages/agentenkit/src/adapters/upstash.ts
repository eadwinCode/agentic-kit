import type { AgentEvent } from '../core/types.js';
import type { Kv } from '../ports/kv.js';
import type { EventBus } from '../ports/bus.js';
import { ScriptedRunStreams, sleep } from './stream-scripts.js';

/** The compare-and-act scripts behind `setIfValue` / `delIfValue`, the same
 *  ones the Go adapters run: one round trip, atomic on the server. */
export const SET_IF_VALUE_SCRIPT = `if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
if tonumber(ARGV[3]) > 0 then redis.call('SET', KEYS[1], ARGV[2], 'PX', ARGV[3]) else redis.call('SET', KEYS[1], ARGV[2]) end
return 1`;
export const DEL_IF_VALUE_SCRIPT = `if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
return redis.call('DEL', KEYS[1])`;
export const INCR_WITH_EXPIRY_SCRIPT = `local n = redis.call('INCR', KEYS[1])
if n == 1 and tonumber(ARGV[1]) > 0 then redis.call('PEXPIRE', KEYS[1], ARGV[1]) end
return n`;

/** Minimal structural type of the @upstash/redis client we use — the real
 *  client satisfies it without importing the SDK here. */
export interface UpstashRedisLike {
  get(key: string): Promise<string | null>;
  set(key: string, value: string, opts?: { ex?: number; nx?: boolean }): Promise<unknown>;
  del(...keys: string[]): Promise<unknown>;
  incr(key: string): Promise<number>;
  publish(channel: string, value: string): Promise<unknown>;
  eval(script: string, keys: string[], args: unknown[]): Promise<unknown>;
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
  async setIfValue(key: string, expected: string, value: string, opts?: { exSeconds?: number }) {
    const ms = Math.round((opts?.exSeconds ?? 0) * 1000);
    return Number(await this.redis.eval(SET_IF_VALUE_SCRIPT, [key], [expected, value, ms])) === 1;
  }
  async delIfValue(key: string, expected: string) {
    return Number(await this.redis.eval(DEL_IF_VALUE_SCRIPT, [key], [expected])) === 1;
  }
  async incrWithExpiry(key: string, exSeconds: number) {
    return Number(await this.redis.eval(INCR_WITH_EXPIRY_SCRIPT, [key], [Math.round(exSeconds * 1000)]));
  }
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

/** RunStreams over Upstash Redis Streams: the same keys and scripts as
 *  RedisRunStreams (see stream-scripts.ts). Upstash's REST API has no
 *  blocking read, so a live reader polls every `pollMs`. */
export class UpstashRunStreams extends ScriptedRunStreams {
  constructor(redis: UpstashRedisLike, opts: { pollMs?: number } = {}) {
    super(
      (script, keys, args) => redis.eval(script, keys, args),
      (_key, _after, maxMs, signal) => sleep(maxMs, signal),
      opts.pollMs ?? 250,
    );
  }
}
