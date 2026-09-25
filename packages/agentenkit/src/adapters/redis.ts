import type { AgentEvent } from '../core/types.js';
import type { Kv } from '../ports/kv.js';
import type { EventBus } from '../ports/bus.js';
import { DEL_IF_VALUE_SCRIPT, INCR_WITH_EXPIRY_SCRIPT, SET_IF_VALUE_SCRIPT, THREAD_CHANNEL } from './upstash.js';

/** Minimal structural type of a node-redis (v4) client — the real client
 *  satisfies it without importing the SDK here. Works against any Redis:
 *  local Docker, self-hosted, or managed. */
export interface RedisClientLike {
  connect(): Promise<unknown>;
  // node-redis exposes these commands as overloaded generic signatures. Keep
  // the structural boundary permissive and normalize results in the adapter.
  get: any;
  set: any;
  del: any;
  incr: any;
  publish: any;
  /** node-redis v4: `eval(script, { keys, arguments })`. */
  eval: any;
  duplicate(): any;
}

/** A dedicated connection for subscriptions (node-redis `client.duplicate()`). */
export interface RedisSubscriberLike {
  connect(): Promise<unknown>;
  subscribe: any;
  unsubscribe: any;
  quit: any;
  on: any;
}

/** Reference Kv adapter over plain Redis (node-redis). */
export class RedisKv implements Kv {
  constructor(private readonly redis: RedisClientLike) {}

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
        ? { NX: true, EX: opts.exSeconds }
        : opts?.exSeconds
          ? { EX: opts.exSeconds }
          : undefined,
    );
    // SET NX returns 'OK' on success, null when the key exists
    return opts?.onlyIfNotExists ? res === 'OK' : true;
  }
  del(key: string) { return this.redis.del(key).then(() => undefined); }
  incr(key: string) { return this.redis.incr(key); }
  async setIfValue(key: string, expected: string, value: string, opts?: { exSeconds?: number }) {
    const ms = Math.round((opts?.exSeconds ?? 0) * 1000);
    const n = await this.redis.eval(SET_IF_VALUE_SCRIPT, {
      keys: [key],
      arguments: [expected, value, String(ms)],
    });
    return Number(n) === 1;
  }
  async delIfValue(key: string, expected: string) {
    const n = await this.redis.eval(DEL_IF_VALUE_SCRIPT, { keys: [key], arguments: [expected] });
    return Number(n) === 1;
  }
  async incrWithExpiry(key: string, exSeconds: number) {
    const n = await this.redis.eval(INCR_WITH_EXPIRY_SCRIPT, {
      keys: [key],
      arguments: [String(Math.round(exSeconds * 1000))],
    });
    return Number(n);
  }
}

/** Reference EventBus adapter over plain Redis Pub/Sub (node-redis).
 *
 *  One subscriber connection per process, shared by every subscription: a
 *  channel is subscribed when its first handler arrives and unsubscribed when
 *  its last one leaves. A connection per viewer would run a busy deployment
 *  into Redis's `maxclients`.
 *
 *  While subscribed, a bus-only `HEARTBEAT` notice (seq 0, never persisted)
 *  reaches every subscription each `heartbeatMs`, from one timer per process —
 *  the §2.5 watchdog pattern: pub/sub is at-most-once, so the SSE distributor
 *  treats heartbeats as a trigger to re-check for orphaned HITL waits, and a
 *  follower fills any gap from the log. */
export class RedisBus implements EventBus {
  private sub: Promise<RedisSubscriberLike> | null = null;
  private readonly handlers = new Map<string, Set<(event: AgentEvent) => void>>();
  private heartbeat?: ReturnType<typeof setInterval>;

  constructor(
    private readonly client: RedisClientLike,
    private readonly heartbeatMs = 60_000,
  ) {}

  async publish(threadId: string, event: AgentEvent) {
    await this.client.publish(THREAD_CHANNEL(threadId), JSON.stringify(event));
  }

  /** The shared subscriber connection, opened on first use. */
  private connection(): Promise<RedisSubscriberLike> {
    if (!this.sub) {
      const sub = this.client.duplicate() as RedisSubscriberLike;
      // node-redis emits 'error' on a dropped connection and reconnects by
      // itself. With no listener, the emit throws and ends the process, so one
      // Redis failover would take down every worker and SSE server.
      sub.on('error', (err: unknown) => console.error('redis subscriber error', err));
      this.sub = Promise.resolve(sub.connect()).then(
        () => sub,
        (err) => {
          this.sub = null; // a later subscribe tries again
          void Promise.resolve(sub.quit()).catch(() => undefined);
          throw err;
        },
      );
    }
    return this.sub;
  }

  private deliver(handlers: Iterable<(event: AgentEvent) => void>, event: AgentEvent) {
    for (const handler of handlers) {
      try {
        handler(event);
      } catch {
        // one throwing handler must not stop the others
      }
    }
  }

  async subscribe(threadId: string, handler: (event: AgentEvent) => void) {
    const channel = THREAD_CHANNEL(threadId);
    const sub = await this.connection();
    let set = this.handlers.get(channel);
    if (!set) {
      set = new Set();
      this.handlers.set(channel, set);
      const mine = set;
      try {
        await sub.subscribe(channel, (message: string) => {
          let event: AgentEvent;
          try {
            event = JSON.parse(message) as AgentEvent;
            event.createdAt = new Date(event.createdAt);
          } catch {
            return; // malformed frame — never kill the subscription
          }
          this.deliver([...mine], event);
        });
      } catch (err) {
        this.handlers.delete(channel);
        throw err;
      }
    }
    set.add(handler);
    this.startHeartbeat();

    let done = false;
    return async () => {
      if (done) return;
      done = true;
      const current = this.handlers.get(channel);
      current?.delete(handler);
      if (current && current.size === 0) {
        this.handlers.delete(channel);
        await sub.unsubscribe(channel).catch(() => undefined);
      }
      if (this.handlers.size === 0) this.stopHeartbeat();
    };
  }

  private startHeartbeat() {
    if (this.heartbeat) return;
    this.heartbeat = setInterval(() => {
      for (const [channel, set] of this.handlers) {
        const threadId = channel.slice('thread:'.length, -':events'.length);
        this.deliver([...set], {
          threadId, seq: 0, type: 'HEARTBEAT', payload: null, createdAt: new Date(),
        } as AgentEvent);
      }
    }, this.heartbeatMs);
    (this.heartbeat as unknown as { unref?: () => void }).unref?.();
  }

  private stopHeartbeat() {
    if (this.heartbeat) clearInterval(this.heartbeat);
    this.heartbeat = undefined;
  }
}
