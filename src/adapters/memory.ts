import type { AgentEvent, ExecutionState, MessageDTO, NewMessage, NewRun, NewUsage, RunDTO, RunJob, ThreadDTO, UsageTotals } from '../core/types.js';
import type { Storage } from '../ports/storage.js';
import type { EventBus } from '../ports/bus.js';
import type { EnqueueOptions, Queue } from '../ports/queue.js';
import type { Kv } from '../ports/kv.js';

const id = () => Math.random().toString(36).slice(2, 12);
interface MemEntry { value: string; expiresAt?: number }

/** In-memory Kv — tests and local prototyping. Expiry is enforced lazily on
 *  read; incr is synchronous internally, so concurrent callers can never
 *  collide on the same counter (§3.4). */
export class MemoryKv implements Kv {
  private m = new Map<string, MemEntry>();
  async get(key: string) {
    const e = this.m.get(key);
    if (!e) return null;
    if (e.expiresAt && e.expiresAt < Date.now()) { this.m.delete(key); return null; }
    return e.value;
  }
  async set(key: string, value: string, opts?: { exSeconds?: number; onlyIfNotExists?: boolean }) {
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
  async incr(key: string) {
    // No awaits between read and write — atomic within the event loop
    const e = this.m.get(key);
    const n = Number(e?.value ?? 0) + 1;
    this.m.set(key, { value: String(n), expiresAt: e?.expiresAt });
    return n;
  }
}

/** Synchronous in-memory bus. Publishes are delivered to subscribers in order. */
export class MemoryBus implements EventBus {
  private subs = new Map<string, Set<(e: AgentEvent) => void>>();
  readonly published: AgentEvent[] = [];

  async publish(threadId: string, event: AgentEvent) {
    this.published.push(event);
    for (const h of this.subs.get(threadId) ?? []) h(event);
  }
  async subscribe(threadId: string, handler: (e: AgentEvent) => void) {
    let set = this.subs.get(threadId);
    if (!set) { set = new Set(); this.subs.set(threadId, set); }
    set.add(handler);
    return () => { set!.delete(handler); };
  }
}

/** In-memory queue with a drain() helper: tests process jobs exactly like the
 *  worker would (`await queue.drain(job => runtime.engine.executeWithPolicy(job))`). */
export class MemoryQueue implements Queue {
  readonly items: RunJob[] = [];
  /** Delivery delay requested per enqueue, index-aligned with `items`. */
  readonly delays: Array<number | undefined> = [];
  async enqueue(job: RunJob, opts?: EnqueueOptions) {
    this.items.push(job);
    this.delays.push(opts?.delaySeconds);
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
    // sibling sections (messages / events / usage / runs) — §3.2 contract
    delete: async (t: string) => {
      if (!this.threads.store.has(t)) throw new Error(`Unknown thread ${t}`);
      this.threads.store.delete(t);
      this.messages.store.delete(t);
      this.events.store.delete(t);
      this.usage.recorded = this.usage.recorded.filter((u) => u.threadId !== t);
      for (const [runId, run] of this.runs.store) if (run.threadId === t) this.runs.store.delete(runId);
    },
    async claimState(t: string, from: ExecutionState, to: ExecutionState) {
      const thread = this.store.get(t);
      if (!thread || thread.state !== from) return false;
      thread.state = to; thread.updatedAt = new Date();
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
    async list(t: string) { return [...(this.store.get(t) ?? [])]; },
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
  };

  usage = {
    recorded: [] as (NewUsage & { threadId: string })[],
    async record(t: string, u: NewUsage) { this.recorded.push({ threadId: t, ...u }); },
    async total(t: string): Promise<UsageTotals> {
      return this.recorded
        .filter((u) => u.threadId === t)
        .reduce<UsageTotals>(
          (sum, u) => ({
            inputTokens: sum.inputTokens + u.inputTokens,
            cachedInputTokens: sum.cachedInputTokens + u.cachedInputTokens,
            outputTokens: sum.outputTokens + u.outputTokens,
            totalTokens: sum.totalTokens + u.totalTokens,
          }),
          { inputTokens: 0, cachedInputTokens: 0, outputTokens: 0, totalTokens: 0 },
        );
    },
  };

  runs = {
    store: new Map<string, RunDTO>(),
    async create(t: string, r: NewRun) {
      const now = new Date();
      const dto: RunDTO = { id: id(), threadId: t, ...r, createdAt: now, updatedAt: now };
      this.store.set(dto.id, dto);
      return dto;
    },
    async update(runId: string, patch: Partial<RunDTO>) {
      const run = this.store.get(runId);
      if (!run) throw new Error(`Unknown run ${runId}`);
      Object.assign(run, patch, { updatedAt: new Date() });
    },
  };
}
