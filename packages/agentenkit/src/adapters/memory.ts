import type { AgentEvent, ExecutionState, MessageDTO, NewMessage, NewUsage, RunJob, ThreadDTO, ThreadTransition, UsageFilter, UsageTotals } from '../core/types.js';
import { sumUsage } from '../core/usage.js';
import type { Storage } from '../ports/storage.js';
import type { EventBus } from '../ports/bus.js';
import { DuplicateJobError, QueueFullError, type EnqueueOptions, type Queue, type QueueStats, type QueuedJob } from '../ports/queue.js';
import type { Kv } from '../ports/kv.js';

const id = () => Math.random().toString(36).slice(2, 12);
interface MemEntry { value: string; expiresAt?: number }

/** In-memory Kv — tests and local prototyping. Expiry is enforced lazily on
 *  read; incr is synchronous internally, so concurrent callers can never
 *  collide on the same counter (§3.4). */
export class MemoryKv implements Kv {
  private m = new Map<string, MemEntry>();
  private lastSweep = Date.now();

  /** Expired keys nobody reads again are dropped as writes come in, at most
   *  once a minute: no timer to leak, and the map does not grow for ever. */
  private sweep() {
    const now = Date.now();
    if (now - this.lastSweep < 60_000) return;
    this.lastSweep = now;
    for (const [k, e] of this.m) if (e.expiresAt && e.expiresAt < now) this.m.delete(k);
  }
  async get(key: string) {
    const e = this.m.get(key);
    if (!e) return null;
    if (e.expiresAt && e.expiresAt < Date.now()) { this.m.delete(key); return null; }
    return e.value;
  }
  async set(key: string, value: string, opts?: { exSeconds?: number; onlyIfNotExists?: boolean }) {
    this.sweep();
    if (opts?.onlyIfNotExists) {
      const existing = this.m.get(key);
      if (existing && !(existing.expiresAt && existing.expiresAt < Date.now())) return false;
    }
    this.m.set(key, {
      value,
      expiresAt: opts?.exSeconds ? Date.now() + opts.exSeconds * 1000 : undefined,
    });
    return true;
  }
  async del(key: string) { this.m.delete(key); }
  async setIfValue(key: string, expected: string, value: string, opts?: { exSeconds?: number }) {
    // No awaits between the read and the write — atomic within the event loop
    if ((await this.get(key)) !== expected) return false;
    this.m.set(key, {
      value,
      expiresAt: opts?.exSeconds ? Date.now() + opts.exSeconds * 1000 : undefined,
    });
    return true;
  }
  async delIfValue(key: string, expected: string) {
    if ((await this.get(key)) !== expected) return false;
    this.m.delete(key);
    return true;
  }
  async incrWithExpiry(key: string, exSeconds: number) {
    // No awaits between read and write — atomic within the event loop
    const live = await this.get(key);
    const n = Number(live ?? 0) + 1;
    const e = this.m.get(key);
    this.m.set(key, {
      value: String(n),
      expiresAt: live === null ? (exSeconds > 0 ? Date.now() + exSeconds * 1000 : undefined) : e?.expiresAt,
    });
    return n;
  }
  async incr(key: string) {
    this.sweep();
    // No awaits between read and write — atomic within the event loop
    const e = this.m.get(key);
    const n = Number(e?.value ?? 0) + 1;
    this.m.set(key, { value: String(n), expiresAt: e?.expiresAt });
    return n;
  }
}

/** How many of the latest published events a MemoryBus keeps in `published`:
 *  enough for any test, bounded for a dev server that runs for days. */
export const PUBLISHED_KEPT = 10_000;

/** Synchronous in-memory bus. Publishes are delivered to subscribers in order. */
export class MemoryBus implements EventBus {
  private subs = new Map<string, Set<(e: AgentEvent) => void>>();
  /** The latest PUBLISHED_KEPT events published, in order. */
  readonly published: AgentEvent[] = [];

  async publish(threadId: string, event: AgentEvent) {
    this.published.push(event);
    if (this.published.length > PUBLISHED_KEPT) {
      this.published.splice(0, this.published.length - PUBLISHED_KEPT);
    }
    for (const h of this.subs.get(threadId) ?? []) h(event);
  }
  async subscribe(threadId: string, handler: (e: AgentEvent) => void) {
    let set = this.subs.get(threadId);
    if (!set) { set = new Set(); this.subs.set(threadId, set); }
    set.add(handler);
    return () => {
      set!.delete(handler);
      // A thread nobody watches holds nothing.
      if (set!.size === 0 && this.subs.get(threadId) === set) this.subs.delete(threadId);
    };
  }
  /** Live subscriptions on a thread — lets a test prove a stream cleaned up
   *  after itself rather than leaking one per reconnect. */
  subscribers(threadId: string): number {
    return this.subs.get(threadId)?.size ?? 0;
  }
}

/** In-memory queue with a drain() helper: tests process jobs exactly like the
 *  worker would (`await queue.drain(job => runtime.engine.executeWithPolicy(job))`). */
export class MemoryQueue implements Queue {
  readonly items: RunJob[] = [];
  /** Delivery delay requested per enqueue, index-aligned with `items`. */
  readonly delays: Array<number | undefined> = [];
  /** Each job's key and priority, kept on the job itself so a test that
   *  shifts `items` by hand leaves nothing out of step. */
  private readonly meta = new WeakMap<RunJob, { key?: string; priority: number; delaySeconds?: number }>();
  /** Refuses a fresh dispatch past this many waiting jobs; 0 is unbounded. */
  maxDepth = 0;

  async enqueue(job: RunJob, opts?: EnqueueOptions) {
    if (opts?.key && this.items.some((j) => this.meta.get(j)?.key === opts.key)) {
      throw new DuplicateJobError(opts.key);
    }
    // The cap counts everything ready to run, but refuses only new work: a
    // retry, a resume or an expiry belongs to a run already under way.
    if (this.maxDepth > 0 && !job.kind) {
      const ready = this.items.filter((j) => !this.meta.get(j)?.delaySeconds).length;
      if (ready >= this.maxDepth) throw new QueueFullError();
    }
    const item = { ...job };
    this.meta.set(item, { key: opts?.key, priority: opts?.priority ?? 0, delaySeconds: opts?.delaySeconds });
    this.items.push(item);
    this.delays.push(opts?.delaySeconds);
  }

  /** The key a queued job was enqueued under. */
  keyOf(job: RunJob): string | undefined {
    return this.meta.get(job)?.key;
  }

  /** The priority a queued job was enqueued with. */
  priorityOf(job: RunJob): number {
    return this.meta.get(job)?.priority ?? 0;
  }

  async cancel(key: string) {
    for (let i = this.items.length - 1; i >= 0; i--) {
      if (key && this.meta.get(this.items[i]!)?.key === key) {
        this.items.splice(i, 1);
        this.delays.splice(i, 1);
      }
    }
  }

  async find(runId: string): Promise<QueuedJob | null> {
    const i = this.items.findIndex((j) => runId && j.runId === runId);
    if (i === -1) return null;
    const job = this.items[i]!;
    return {
      id: String(i), runId, threadId: job.threadId, kind: job.kind, attempts: 0,
      runAt: new Date(Date.now() + (this.meta.get(job)?.delaySeconds ?? 0) * 1000), position: i,
    };
  }

  async stats(): Promise<QueueStats> {
    const delayed = this.items.filter((j) => (this.meta.get(j)?.delaySeconds ?? 0) > 0).length;
    return {
      ready: this.items.length - delayed, delayed, inFlight: 0, dead: 0,
      oldestReadyMs: 0, paused: false, claimErrors: 0,
    };
  }

  async drain(handler: (job: RunJob) => Promise<void>): Promise<number> {
    let n = 0;
    while (this.items.length) {
      const job = this.items.shift()!;
      this.delays.shift();
      await handler(job);
      n++;
    }
    return n;
  }
}

/** Full in-memory Storage — tests, demos, and a template for custom adapters. */
export class MemoryStorage implements Storage {
  threads = {
    store: new Map<string, ThreadDTO>(),
    async get(t: string) { return this.store.get(t) ?? null; },
    async create(init?: { model?: string }) {
      const now = new Date();
      const t: ThreadDTO = { id: id(), state: 'IDLE', model: init?.model ?? 'gpt-4o', createdAt: now, updatedAt: now };
      this.store.set(t.id, t);
      return t;
    },
    async list() {
      return [...this.store.values()].sort((a, b) => b.updatedAt.getTime() - a.updatedAt.getTime());
    },
    async setState(t: string, state: ExecutionState) {
      const thread = this.store.get(t);
      if (!thread) throw new Error(`Unknown thread ${t}`);
      thread.state = state; thread.updatedAt = new Date();
    },
    // Arrow-bound to the MemoryStorage instance: the cascade reaches the
    // sibling sections (messages / events / usage) — §3.2 contract. Run
    // records are not here; they are the platform's own (§2.9).
    delete: async (t: string) => {
      if (!this.threads.store.has(t)) throw new Error(`Unknown thread ${t}`);
      this.threads.store.delete(t);
      this.threads.runs.delete(t);
      this.messages.store.delete(t);
      this.events.store.delete(t);
      this.usage.recorded = this.usage.recorded.filter((u) => u.threadId !== t);
    },
    async claimState(t: string, from: ExecutionState, to: ExecutionState) {
      const thread = this.store.get(t);
      if (!thread || thread.state !== from) return false;
      thread.state = to; thread.updatedAt = new Date();
      return true;
    },
    /** Each thread's current run (ThreadTransition). */
    runs: new Map<string, string>(),
    async transition(t: string, tr: ThreadTransition) {
      // No awaits between the read and the write — atomic within the event loop
      const thread = this.store.get(t);
      if (!thread || !tr.from.includes(thread.state)) return false;
      const current = this.runs.get(t);
      if (tr.runId && current && current !== tr.runId) return false;
      thread.state = tr.to; thread.updatedAt = new Date();
      if (tr.newRunId) this.runs.set(t, tr.newRunId);
      return true;
    },
  };

  messages = {
    store: new Map<string, MessageDTO[]>(),
    async append(t: string, m: NewMessage) {
      const dto: MessageDTO = {
        id: id(), threadId: t, agentId: m.agentId ?? null,
        role: m.role, content: m.content, createdAt: new Date(),
      };
      let list = this.store.get(t);
      if (!list) { list = []; this.store.set(t, list); }
      list.push(dto);
      return dto;
    },
    async list(t: string, opts?: { agentId?: string | null }) {
      const rows = this.store.get(t) ?? [];
      if (!opts || !('agentId' in opts)) return [...rows];
      return rows.filter((m) => (m.agentId ?? null) === (opts.agentId ?? null));
    },
    async deleteFrom(t: string, messageId: string) {
      const rows = this.store.get(t) ?? [];
      const from = rows.findIndex((m) => m.id === messageId);
      if (from === -1) return 0;
      return rows.splice(from).length;
    },
  };

  events = {
    store: new Map<string, AgentEvent[]>(),
    async append(t: string, e: AgentEvent) {
      let list = this.store.get(t);
      if (!list) { list = []; this.store.set(t, list); }
      list.push(e);
    },
    async listSince(t: string, sinceSeq: number) {
      return (this.store.get(t) ?? []).filter((e) => e.seq > sinceSeq)
        .sort((a, b) => a.seq - b.seq);
    },
    async latest(t: string, type: string) {
      const list = this.store.get(t) ?? [];
      for (let i = list.length - 1; i >= 0; i--) if (list[i].type === type) return list[i];
      return null;
    },
    async listByType(t: string, type: string) {
      return (this.store.get(t) ?? []).filter((e) => e.type === type)
        .sort((a, b) => a.seq - b.seq);
    },
  };

  usage = {
    recorded: [] as (NewUsage & { threadId: string })[],
    async record(t: string, u: NewUsage) { this.recorded.push({ threadId: t, ...u }); },
    async total(t: string, filter: UsageFilter = {}): Promise<UsageTotals> {
      return sumUsage(
        this.recorded.filter(
          (u) => u.threadId === t && (!filter.runId || u.runId === filter.runId),
        ),
      );
    },
  };



}
