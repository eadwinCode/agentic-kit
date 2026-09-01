import type { AgentEvent, ExecutionState, MessageDTO, NewMessage, NewRunRecord, NewUsage, RunPatch, RunRecord, ThreadDTO, UsageTotals } from '../core/types.js';
import type { RunFilter, Storage } from '../ports/storage.js';

/** Minimal structural type of the Prisma client surface we use. The real
 *  `PrismaClient` satisfies it — no SDK import needed in the package. */
export interface PrismaLike {
  thread: {
    findUnique(a: { where: { id: string } }): Promise<(Omit<ThreadDTO, 'createdAt' | 'updatedAt'> & { createdAt: Date; updatedAt: Date }) | null>;
    findMany(a: { orderBy: { updatedAt: 'desc' } }): Promise<ThreadDTO[]>;
    create(a: { data: { model?: string } }): Promise<ThreadDTO>;
    update(a: { where: { id: string }; data: { state: ExecutionState } }): Promise<unknown>;
    updateMany(a: { where: { id: string; state: ExecutionState }; data: { state: ExecutionState } }): Promise<{ count: number }>;
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
    create(a: { data: NewUsage & { threadId: string } }): Promise<unknown>;
    aggregate(a: {
      where: { threadId: string };
      _sum: {
        inputTokens: true;
        cachedInputTokens: true;
        outputTokens: true;
        totalTokens: true;
      };
    }): Promise<{ _sum: Partial<Record<keyof UsageTotals, number | null>> }>;
  };
  agentRun: {
    create(a: { data: Record<string, unknown> }): Promise<unknown>;
    update(a: { where: { id: string }; data: Record<string, unknown> }): Promise<unknown>;
    findUnique(a: { where: { id: string } }): Promise<RunRecord | null>;
    findMany(a: {
      where: Record<string, unknown>;
      orderBy: { startedAt: 'desc' };
      take?: number;
    }): Promise<RunRecord[]>;
  };
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
    total: async (threadId: string): Promise<UsageTotals> => {
      // One aggregate rather than reading every row: a long thread can hold a
      // usage row per run segment (§4).
      const { _sum } = await this.prisma.tokenUsage.aggregate({
        where: { threadId },
        _sum: {
          inputTokens: true,
          cachedInputTokens: true,
          outputTokens: true,
          totalTokens: true,
        },
      });
      return {
        inputTokens: _sum.inputTokens ?? 0,
        cachedInputTokens: _sum.cachedInputTokens ?? 0,
        outputTokens: _sum.outputTokens ?? 0,
        totalTokens: _sum.totalTokens ?? 0,
      };
    },
    record: async (threadId: string, usage: NewUsage) => {
      await this.prisma.tokenUsage.create({
        data: {
          threadId,
          agentId: usage.agentId ?? null,
          inputTokens: usage.inputTokens,
          cachedInputTokens: usage.cachedInputTokens,
          outputTokens: usage.outputTokens,
          totalTokens: usage.totalTokens,
        },
      });
    },
  };

  runs = {
    start: (run: NewRunRecord) =>
      this.prisma.agentRun.create({
        data: { depth: 0, parentRunId: null, ...run, state: 'RUNNING' },
      }) as Promise<RunRecord>,
    patch: async (runId: string, patch: RunPatch) => {
      await this.prisma.agentRun.update({ where: { id: runId }, data: patch as any });
    },
    get: (runId: string) => this.prisma.agentRun.findUnique({ where: { id: runId } }),
    listByThread: (threadId: string) =>
      this.prisma.agentRun.findMany({ where: { threadId }, orderBy: { startedAt: 'desc' } }),
  };

  admin = {
    listRuns: (f: RunFilter) =>
      this.prisma.agentRun.findMany({
        where: {
          ...(f.state?.length ? { state: { in: f.state } } : {}),
          ...(f.agent ? { agent: f.agent } : {}),
          ...(f.since || f.until
            ? { startedAt: { ...(f.since ? { gte: f.since } : {}), ...(f.until ? { lte: f.until } : {}) } }
            : {}),
        },
        orderBy: { startedAt: 'desc' },
        take: f.limit ?? 100,
      }),
    countThreadsByState: async () => {
      const rows = await this.prisma.thread.groupBy({ by: ['state'], _count: { _all: true } });
      return Object.fromEntries(rows.map((r) => [r.state, r._count._all])) as Partial<
        Record<ExecutionState, number>
      >;
    },
  };
}
