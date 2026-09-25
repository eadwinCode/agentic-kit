import type { AgentEvent, ExecutionState, MessageDTO, NewMessage, NewThreadEvent, NewUsage, ThreadDTO, ThreadEventFilter, ThreadTransition, UsageFilter, UsageTotals } from '../core/types.js';
import { UsageMerger } from '../core/usage.js';
import type { Storage } from '../ports/storage.js';
import { StreamClosedError, StreamGoneError, type RunStreams, type StreamMeta, type StreamSnapshot } from '../ports/streams.js';
import { isStreamEnd, type StreamEnd, type StreamEvent, type StreamItem } from '../core/stream-events.js';
import { sleep } from './stream-scripts.js';

/** Minimal structural type of the Prisma client surface we use. The real
 *  `PrismaClient` satisfies it — no SDK import needed in the package. */
export interface PrismaLike {
  thread: {
    findUnique(a: { where: { id: string } }): Promise<(Omit<ThreadDTO, 'createdAt' | 'updatedAt'> & { createdAt: Date; updatedAt: Date }) | null>;
    findMany(a: { orderBy: { updatedAt: 'desc' } }): Promise<ThreadDTO[]>;
    create(a: { data: { model?: string } }): Promise<ThreadDTO>;
    update(a: { where: { id: string }; data: { state: ExecutionState } }): Promise<unknown>;
    updateMany(a: {
      where: {
        id: string;
        state: ExecutionState | { in: ExecutionState[] };
        OR?: Array<{ runId: string | null }>;
      };
      data: { state: ExecutionState; runId?: string };
    }): Promise<{ count: number }>;
    delete(a: { where: { id: string } }): Promise<unknown>;
    groupBy(a: { by: ['state']; _count: { _all: true } }): Promise<
      Array<{ state: ExecutionState; _count: { _all: number } }>
    >;
  };
  message: {
    create(a: { data: { threadId: string; agentId?: string | null; role: string; content: any } }): Promise<any>;
    findMany(a: {
      where: { threadId: string; agentId?: string | null };
      orderBy: { seq: 'asc' };
    }): Promise<any[]>;
    findFirst(a: { where: { id: string; threadId: string }; select: { seq: true } }): Promise<{ seq: bigint | number } | null>;
    deleteMany(a: { where: { threadId: string; seq: { gte: bigint | number } } }): Promise<{ count: number }>;
  };
  agentEvent: {
    create(a: { data: { threadId: string; seq: number; type: string; payload: any; runId?: string | null; createdAt?: Date } }): Promise<AgentEvent>;
    findMany(a: {
      // One signature for every read: by seq, by type, by run. Several
      // overloads would be a shape the real PrismaClient cannot satisfy.
      where: { threadId: string; seq?: { gt: number }; type?: string | { in: string[] }; runId?: string };
      orderBy: { seq: 'asc' };
      take?: number;
    }): Promise<AgentEvent[]>;
    findFirst(a: { where: { threadId: string; type?: string }; orderBy: { seq: 'desc' } }): Promise<AgentEvent | null>;
  };
  tokenUsage: {
    create(a: { data: TokenUsageRow }): Promise<unknown>;
    /** One grouped read rather than every row: a long thread holds a usage row
     *  per model call (§4), and the bill only ever wants them by agent and
     *  model.
     *
     *  Loosely typed on purpose. Prisma generates `groupBy` with a chain of
     *  conditional generics that no hand-written signature can mirror, so a
     *  precise one here would make the real PrismaClient fail to satisfy this
     *  interface. The RESULT is typed, which is the half the adapter reads. */
    groupBy(a: any): Promise<UsageGroupRow[]>;
  };
}

/** The row this adapter writes. `cachedInputTokens` holds cache READS, keeping
 *  the column that was already there meaning what it always meant; cache
 *  writes are their own column beside it. A null `costMicros` is an unpriced
 *  call, which is not the same as one that cost nothing. */
export interface TokenUsageRow {
  threadId: string;
  runId: string | null;
  agentId: string | null;
  agentName: string | null;
  kind: string;
  step: number;
  model: string | null;
  modelId: string | null;
  inputTokens: number;
  cachedInputTokens: number;
  cacheWriteInputTokens: number;
  outputTokens: number;
  reasoningTokens: number;
  totalTokens: number;
  outcome: string;
  estimated: boolean;
  /** Typed loosely on purpose: Prisma's JSON input is a recursive generated
   *  union (`InputJsonValue`) that no hand-written interface can mirror, so a
   *  precise type here makes the real PrismaClient fail to satisfy this. */
  providerMetadata: any;
  /** Millionths of `costCurrency`; null is an unpriced call. Declare the
   *  column as BigInt: a thread's summed spend outgrows a 4-byte Int. */
  costMicros: number | null;
  costCurrency: string | null;
  costSource: string | null;
}

/** One group of the read above. */
export interface UsageGroupRow {
  agentId: string | null;
  agentName: string | null;
  model: string | null;
  modelId: string | null;
  /** Grouped on too, so a group never sums two units. Null for the unpriced
   *  calls. */
  costCurrency: string | null;
  _count: number;
  /** BigInt columns come back as `bigint`, so every figure is coerced before
   *  it is summed — mixing `bigint` and `number` arithmetic throws. */
  _sum: Record<string, number | bigint | null>;
}

/** A message row without its `seq`: a BigInt, which JSON cannot carry, and
 *  an ordering detail no caller needs. */
function toMessage(row: any): MessageDTO {
  const { seq: _seq, ...rest } = row ?? {};
  return rest as MessageDTO;
}

/** Reference Storage adapter over PostgreSQL/Prisma (schema in the README / spec §2.4). */
export class PrismaStorage implements Storage {
  constructor(private readonly prisma: PrismaLike) {}

  threads = {
    get: (threadId: string) => this.prisma.thread.findUnique({ where: { id: threadId } }),
    create: (init?: { model?: string }) =>
      this.prisma.thread.create({ data: { model: init?.model } }),
    list: () =>
      this.prisma.thread.findMany({ orderBy: { updatedAt: 'desc' } }),
    setState: async (threadId: string, state: ExecutionState) => {
      await this.prisma.thread.update({ where: { id: threadId }, data: { state } });
    },
    claimState: async (threadId: string, from: ExecutionState, to: ExecutionState) => {
      // Single conditional UPDATE — the atomicity contract (§3.4)
      const res = await this.prisma.thread.updateMany({
        where: { id: threadId, state: from },
        data: { state: to },
      });
      return res.count > 0;
    },
    transition: async (threadId: string, tr: ThreadTransition) => {
      if (tr.from.length === 0) return false;
      // Single conditional UPDATE — the atomicity contract (§3.4). A thread
      // with no run recorded yet (from before the column) matches any run.
      const res = await this.prisma.thread.updateMany({
        where: {
          id: threadId,
          state: { in: tr.from },
          ...(tr.runId ? { OR: [{ runId: tr.runId }, { runId: null }] } : {}),
        },
        data: { state: tr.to, ...(tr.newRunId ? { runId: tr.newRunId } : {}) },
      });
      return res.count > 0;
    },
    delete: (threadId: string) =>
      // One delete — the reference schema cascades to Message, AgentEvent,
      // TokenUsage and SubagentRun via `onDelete: Cascade` (spec §2.4)
      this.prisma.thread.delete({ where: { id: threadId } }).then(() => undefined),
  };

  messages = {
    append: async (threadId: string, m: NewMessage) =>
      toMessage(await this.prisma.message.create({
        data: { threadId, agentId: m.agentId ?? null, role: m.role, content: m.content },
      })),
    list: (threadId: string, opts?: { agentId?: string | null }) =>
      this.prisma.message.findMany({
        // `agentId: null` is a real filter (IS NULL), not an absent one — the
        // scope is only dropped when the caller omits it entirely.
        where: {
          threadId,
          ...(opts && 'agentId' in opts ? { agentId: opts.agentId } : {}),
        },
        // By seq, the order they were written: a tool call and its result
        // can share a millisecond, and createdAt would let them swap.
        orderBy: { seq: 'asc' },
      }).then((rows) => rows.map(toMessage)),
    deleteFrom: async (threadId: string, messageId: string) => {
      // One delete over the same order `list` uses: the message and every
      // one written after it.
      const target = await this.prisma.message.findFirst({
        where: { id: messageId, threadId },
        select: { seq: true },
      });
      if (!target) return 0;
      const { count } = await this.prisma.message.deleteMany({
        where: { threadId, seq: { gte: target.seq } },
      });
      return count;
    },
  };

  events = {
    // The seq is the thread's next: read the top one and insert above it.
    // Two appends that read the same top collide on the (threadId, seq)
    // unique index, and the loser tries again.
    append: async (threadId: string, event: NewThreadEvent): Promise<AgentEvent> => {
      for (let attempt = 0; ; attempt++) {
        const top = await this.prisma.agentEvent.findFirst({ where: { threadId }, orderBy: { seq: 'desc' } });
        try {
          const row = await this.prisma.agentEvent.create({
            data: {
              threadId, seq: (top?.seq ?? 0) + 1, type: event.type, payload: event.payload ?? null,
              runId: event.runId ?? null,
              ...(event.createdAt ? { createdAt: event.createdAt } : {}),
            },
          });
          return withRun(row);
        } catch (err: any) {
          if (err?.code !== 'P2002' || attempt >= 9) throw err;
        }
      }
    },
    list: async (threadId: string, f: ThreadEventFilter = {}) =>
      (await this.prisma.agentEvent.findMany({
        where: {
          threadId,
          ...(f.types ? { type: { in: f.types } } : {}),
          ...(f.runId !== undefined ? { runId: f.runId } : {}),
          ...(f.after !== undefined ? { seq: { gt: f.after } } : {}),
        },
        orderBy: { seq: 'asc' },
        ...(f.limit ? { take: f.limit } : {}),
      })).map(withRun),
    listSince: async (threadId: string, sinceSeq: number) =>
      (await this.prisma.agentEvent.findMany({
        where: { threadId, seq: { gt: sinceSeq } },
        orderBy: { seq: 'asc' },
      })).map(withRun),
    latest: async (threadId: string, type: string) => {
      const row = await this.prisma.agentEvent.findFirst({ where: { threadId, type }, orderBy: { seq: 'desc' } });
      return row && withRun(row);
    },
    listByType: async (threadId: string, type: string) =>
      (await this.prisma.agentEvent.findMany({ where: { threadId, type }, orderBy: { seq: 'asc' } })).map(withRun),
    prune: async (types: string[], opts: { limit: number; dryRun?: boolean }) => {
      // Loosely typed: these reads span every thread, which the narrowed
      // signatures above (one thread at a time) do not cover.
      const events = this.prisma.agentEvent as any;
      const counts: Record<string, number> = {};
      if (opts.dryRun) {
        for (const g of await events.groupBy({ by: ['type'], where: { type: { in: types } }, _count: { _all: true } })) {
          counts[g.type] = g._count._all;
        }
        return counts;
      }
      const rows: Array<{ id: string; type: string }> = await events.findMany({
        where: { type: { in: types } }, select: { id: true, type: true }, take: opts.limit,
      });
      if (rows.length === 0) return counts;
      await events.deleteMany({ where: { id: { in: rows.map((r) => r.id) } } });
      for (const r of rows) counts[r.type] = (counts[r.type] ?? 0) + 1;
      return counts;
    },
  };

  usage = {
    total: async (threadId: string, filter: UsageFilter = {}): Promise<UsageTotals> => {
      const where = { threadId, ...(filter.runId ? { runId: filter.runId } : {}) };
      // Grouped by currency as well, so a group never sums two units. A
      // group with no currency is the unpriced calls.
      const by = ['agentId', 'agentName', 'model', 'modelId', 'costCurrency'];
      const keyOf = (g: UsageGroupRow) => [g.agentId, g.agentName, g.model, g.modelId, g.costCurrency].join('\u0000');
      const [groups, estimatedByGroup] = await Promise.all([
        this.prisma.tokenUsage.groupBy({
          by,
          where,
          _sum: {
            inputTokens: true, cachedInputTokens: true, cacheWriteInputTokens: true,
            outputTokens: true, reasoningTokens: true, totalTokens: true, costMicros: true,
          },
          _count: true,
          _min: { createdAt: true },
        }),
        // Which calls were guesses: a conditional count a grouped sum cannot
        // carry, read alongside rather than hardcoded.
        this.prisma.tokenUsage.groupBy({ by, where: { ...where, estimated: true }, _count: true }),
      ]);
      const estimatedOf = new Map(estimatedByGroup.map((g) => [keyOf(g), g._count]));
      // First-seen order, as every other adapter gives it.
      const at = (g: UsageGroupRow) => new Date((g as any)._min?.createdAt ?? 0).getTime();
      const merge = new UsageMerger();
      for (const g of [...groups].sort((a, b) => at(a) - at(b))) {
        const n = (k: string) => Number(g._sum[k] ?? 0);
        merge.add({
          line: {
            agentId: g.agentId, agentName: g.agentName, model: g.model, modelId: g.modelId,
            inputTokens: n('inputTokens'),
            cacheReadInputTokens: n('cachedInputTokens'),
            cacheWriteInputTokens: n('cacheWriteInputTokens'),
            outputTokens: n('outputTokens'),
            reasoningTokens: n('reasoningTokens'),
            calls: g._count,
            estimated: estimatedOf.get(keyOf(g)) ?? 0,
            costMicros: n('costMicros'),
          },
          currency: g.costCurrency,
          totalTokens: n('totalTokens'),
          unpriced: g.costCurrency ? 0 : g._count,
        });
      }
      return merge.totals();
    },
    record: async (threadId: string, usage: NewUsage) => {
      await this.prisma.tokenUsage.create({
        data: {
          threadId,
          runId: usage.runId ?? null,
          agentId: usage.agentId ?? null,
          agentName: usage.agentName ?? null,
          kind: usage.kind,
          step: usage.step,
          model: usage.model ?? null,
          modelId: usage.modelId ?? null,
          inputTokens: usage.inputTokens,
          cachedInputTokens: usage.cacheReadInputTokens,
          cacheWriteInputTokens: usage.cacheWriteInputTokens,
          outputTokens: usage.outputTokens,
          reasoningTokens: usage.reasoningTokens,
          totalTokens: usage.totalTokens,
          outcome: usage.outcome,
          estimated: usage.estimated ?? false,
          providerMetadata: usage.providerMetadata ?? null,
          costMicros: usage.cost?.micros ?? null,
          costCurrency: usage.cost?.currency ?? null,
          costSource: usage.cost?.source ?? null,
        },
      });
    },
  };



}

/** A record row as an event: `runId` only when the entry has one, as the
 *  other stores and the Go runtime send it. */
function withRun(row: AgentEvent): AgentEvent {
  const { runId, ...rest } = row as AgentEvent & { id?: string };
  delete (rest as { id?: string }).id;
  return runId ? { ...rest, runId } : rest;
}

/** The Prisma surface PrismaRunStreams uses: the RunStream and
 *  RunStreamEvent models (see the example schema). Loosely typed where
 *  Prisma's generated signatures cannot be mirrored by hand; the results
 *  the adapter reads are typed. */
export interface PrismaStreamsLike {
  runStream: {
    findUnique(a: { where: { id: string } }): Promise<RunStreamRow | null>;
    create(a: any): Promise<unknown>;
    updateMany(a: any): Promise<{ count: number }>;
    deleteMany(a: any): Promise<{ count: number }>;
    findMany(a: any): Promise<Array<{ id: string }>>;
  };
  runStreamEvent: {
    create(a: { data: { streamId: string; event: string } }): Promise<{ pos: bigint | number }>;
    findMany(a: any): Promise<Array<{ pos: bigint | number; event: string }>>;
    deleteMany(a: any): Promise<{ count: number }>;
  };
  $transaction<T>(work: (tx: any) => Promise<T>): Promise<T>;
}

export interface RunStreamRow {
  id: string;
  threadId: string;
  runId: string;
  closed: boolean;
  endEvent: string | null;
  expiresAt: Date;
}

/** RunStreams over Prisma: one RunStream row per stream and one
 *  RunStreamEvent row per event, whose autoincrement key is the offset.
 *
 *  An append first runs a no-op UPDATE on the stream row where it is still
 *  open. That takes the row's lock, so a close waits for the append to
 *  commit, or the append finds the stream closed: an event can never land
 *  after the end item. Readers in this process wake on its own appends; one
 *  in another process sees them on its next poll. */
export class PrismaRunStreams implements RunStreams {
  private readonly waiters = new Map<string, Set<() => void>>();
  private swept = 0;

  constructor(
    private readonly prisma: PrismaStreamsLike,
    private readonly opts: { pollMs?: number } = {},
  ) {}

  private wake(streamId: string) {
    const set = this.waiters.get(streamId);
    this.waiters.delete(streamId);
    for (const wake of set ?? []) wake();
  }

  private arm(streamId: string) {
    let wake!: () => void;
    const woken = new Promise<void>((resolve) => { wake = resolve; });
    let set = this.waiters.get(streamId);
    if (!set) { set = new Set(); this.waiters.set(streamId, set); }
    set.add(wake);
    const disarm = () => {
      const s = this.waiters.get(streamId);
      s?.delete(wake);
      if (s && s.size === 0) this.waiters.delete(streamId);
    };
    return { woken, disarm };
  }

  private async sweep() {
    if (Date.now() - this.swept < 60_000) return;
    this.swept = Date.now();
    const gone = await this.prisma.runStream.findMany({
      where: { expiresAt: { lte: new Date() } }, select: { id: true }, take: 500,
    });
    for (const { id } of gone) await this.remove(id);
  }

  private remove(streamId: string) {
    return this.prisma.$transaction(async (tx) => {
      await tx.runStreamEvent.deleteMany({ where: { streamId } });
      await tx.runStream.deleteMany({ where: { id: streamId } });
    });
  }

  async open(streamId: string, meta: StreamMeta, ttlMs: number) {
    await this.sweep();
    // A row past its expiry is gone: replace it rather than reopen it.
    await this.prisma.runStream.deleteMany({ where: { id: streamId, expiresAt: { lte: new Date() } } });
    try {
      await this.prisma.runStream.create({
        data: { id: streamId, threadId: meta.threadId, runId: meta.runId, closed: false, expiresAt: new Date(Date.now() + ttlMs) },
      });
    } catch (err: any) {
      if (err?.code !== 'P2002') throw err; // already open
    }
  }

  /** Locks the stream row inside tx, or says why it cannot. */
  private async lock(tx: any, streamId: string): Promise<'open' | 'closed' | 'gone'> {
    const { count } = await tx.runStream.updateMany({
      where: { id: streamId, closed: false, expiresAt: { gt: new Date() } },
      data: { closed: false },
    });
    if (count === 1) return 'open';
    const row = await tx.runStream.findUnique({ where: { id: streamId } });
    return row && row.expiresAt > new Date() ? 'closed' : 'gone';
  }

  async append(streamId: string, events: StreamEvent[]) {
    if (events.length === 0) return [];
    const offsets = await this.prisma.$transaction(async (tx) => {
      const state = await this.lock(tx, streamId);
      if (state === 'gone') throw new StreamGoneError(streamId);
      if (state === 'closed') throw new StreamClosedError(streamId);
      const out: string[] = [];
      for (const e of events) {
        const row = await tx.runStreamEvent.create({ data: { streamId, event: JSON.stringify(e) } });
        out.push(String(row.pos));
      }
      return out;
    });
    this.wake(streamId);
    return offsets;
  }

  async close(streamId: string, end: StreamEnd, graceMs: number) {
    await this.prisma.$transaction(async (tx) => {
      const state = await this.lock(tx, streamId);
      if (state === 'gone') throw new StreamGoneError(streamId);
      if (state === 'closed') return;
      await tx.runStreamEvent.create({ data: { streamId, event: JSON.stringify(end) } });
      await tx.runStream.updateMany({
        where: { id: streamId },
        data: { closed: true, endEvent: JSON.stringify(end), expiresAt: new Date(Date.now() + graceMs) },
      });
    });
    this.wake(streamId);
  }

  async delete(streamId: string) {
    await this.remove(streamId);
    this.wake(streamId);
  }

  private async page(streamId: string, after: string | null) {
    const row = await this.prisma.runStream.findUnique({ where: { id: streamId } });
    if (!row || row.expiresAt <= new Date()) return null;
    const rows = await this.prisma.runStreamEvent.findMany({
      where: { streamId, pos: { gt: BigInt(after ?? 0) } },
      orderBy: { pos: 'asc' },
    });
    return {
      meta: { threadId: row.threadId, runId: row.runId },
      closed: row.closed,
      end: row.closed && row.endEvent ? (JSON.parse(row.endEvent) as StreamEnd) : null,
      items: rows.map((r) => ({ ...JSON.parse(r.event), offset: String(r.pos) }) as StreamItem),
    };
  }

  async *read(streamId: string, after: string | null, signal?: AbortSignal): AsyncIterable<StreamItem> {
    let cursor = after;
    for (;;) {
      if (signal?.aborted) return;
      const { woken, disarm } = this.arm(streamId);
      const page = await this.page(streamId, cursor);
      if (!page) {
        disarm();
        throw new StreamGoneError(streamId);
      }
      if (page.items.length > 0) disarm();
      for (const item of page.items) {
        yield item;
        cursor = item.offset;
        if (isStreamEnd(item)) return;
      }
      if (page.items.length > 0) continue;
      // Read from past the end item: nothing more will ever come.
      if (page.closed) {
        disarm();
        return;
      }
      await Promise.race([woken, sleep(this.opts.pollMs ?? 250, signal)]);
      disarm();
    }
  }

  async snapshot(streamId: string, after?: string | null): Promise<StreamSnapshot | null> {
    const page = await this.page(streamId, after ?? null);
    return page && { meta: page.meta, items: page.items, end: page.end };
  }
}
