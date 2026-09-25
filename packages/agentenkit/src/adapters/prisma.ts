import type { AgentEvent, ExecutionState, NewMessage, NewUsage, ThreadDTO, ThreadTransition, UsageFilter, UsageTotals } from '../core/types.js';
import { UsageMerger } from '../core/usage.js';
import type { Storage } from '../ports/storage.js';

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
      orderBy: { createdAt: 'asc' };
    }): Promise<any[]>;
    deleteMany(a: { where: { id: { in: string[] } } }): Promise<{ count: number }>;
  };
  agentEvent: {
    create(a: { data: { threadId: string; seq: number; type: string; payload: any } }): Promise<unknown>;
    findMany(a: {
      // One signature covering both reads — listSince (by seq) and listByType.
      // Two overloads here would be a shape the real PrismaClient cannot satisfy.
      where: { threadId: string; seq?: { gt: number }; type?: string };
      orderBy: { seq: 'asc' };
    }): Promise<AgentEvent[]>;
    findFirst(a: { where: { threadId: string; type: string }; orderBy: { seq: 'desc' } }): Promise<AgentEvent | null>;
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
    append: (threadId: string, m: NewMessage) =>
      this.prisma.message.create({
        data: { threadId, agentId: m.agentId ?? null, role: m.role, content: m.content },
      }),
    list: (threadId: string, opts?: { agentId?: string | null }) =>
      this.prisma.message.findMany({
        // `agentId: null` is a real filter (IS NULL), not an absent one — the
        // scope is only dropped when the caller omits it entirely.
        where: {
          threadId,
          ...(opts && 'agentId' in opts ? { agentId: opts.agentId } : {}),
        },
        orderBy: { createdAt: 'asc' },
      }),
    deleteFrom: async (threadId: string, messageId: string) => {
      // Delete by id over the same ordering `list` uses, rather than a
      // `createdAt >=` range: a run appends several messages inside one
      // millisecond, and a range would take neighbours with it.
      const rows = await this.prisma.message.findMany({
        where: { threadId },
        orderBy: { createdAt: 'asc' },
      });
      const from = rows.findIndex((m: { id: string }) => m.id === messageId);
      if (from === -1) return 0;
      const { count } = await this.prisma.message.deleteMany({
        where: { id: { in: rows.slice(from).map((m: { id: string }) => m.id) } },
      });
      return count;
    },
  };

  events = {
    append: async (threadId: string, event: AgentEvent) => {
      await this.prisma.agentEvent.create({
        data: { threadId, seq: event.seq, type: event.type, payload: event.payload },
      });
    },
    listSince: (threadId: string, sinceSeq: number) =>
      this.prisma.agentEvent.findMany({
        where: { threadId, seq: { gt: sinceSeq } },
        orderBy: { seq: 'asc' },
      }),
    latest: (threadId: string, type: string) =>
      this.prisma.agentEvent.findFirst({
        where: { threadId, type },
        orderBy: { seq: 'desc' },
      }),
    listByType: (threadId: string, type: string) =>
      this.prisma.agentEvent.findMany({ where: { threadId, type }, orderBy: { seq: 'asc' } }),
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
