import type { AgentEvent, ExecutionState, MessageDTO, NewMessage, NewRunRecord, NewUsage, RunJob, RunPatch, RunRecord, ThreadDTO, UsageTotals } from '../core/types.js';
import type { RunFilter, Storage } from '../ports/storage.js';
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
    store: new Map<string, RunRecord>(),
    async start(r: NewRunRecord) {
      const rec: RunRecord = {
        parentRunId: null, depth: 0, ...r,
        state: 'RUNNING', startedAt: new Date(), steps: 0, attempts: 0,
        inputTokens: 0, cachedInputTokens: 0, outputTokens: 0, totalTokens: 0,
      };
      this.store.set(rec.id, rec);
      return rec;
    },
    async patch(runId: string, patch: RunPatch) {
      const cur = this.store.get(runId);
      if (cur) this.store.set(runId, { ...cur, ...patch });
    },
    async get(runId: string) { return this.store.get(runId) ?? null; },
    async listByThread(t: string) {
      return [...this.store.values()]
        .filter((r) => r.threadId === t)
        .sort((a, b) => b.startedAt.getTime() - a.startedAt.getTime());
    },
  };

  admin = {
    parent: this as MemoryStorage,
    async listRuns(f: RunFilter) {
      let rows = [...this.parent.runs.store.values()];
      if (f.state?.length) rows = rows.filter((r) => f.state!.includes(r.state));
      if (f.agent) rows = rows.filter((r) => r.agent === f.agent);
      if (f.since) rows = rows.filter((r) => r.startedAt >= f.since!);
      if (f.until) rows = rows.filter((r) => r.startedAt <= f.until!);
      rows.sort((a, b) => b.startedAt.getTime() - a.startedAt.getTime());
      return rows.slice(0, f.limit ?? 100);
    },
    async countThreadsByState() {
      const out: Record<string, number> = {};
      for (const t of this.parent.threads.store.values()) {
        out[t.state] = (out[t.state] ?? 0) + 1;
      }
      return out as any;
    },
  };

}
