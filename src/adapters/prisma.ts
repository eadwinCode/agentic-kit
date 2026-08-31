import type { AgentEvent, ExecutionState, MessageDTO, NewMessage, NewRun, NewUsage, RunDTO, ThreadDTO } from '../core/types.js';
import type { Storage } from '../ports/storage.js';

/** Minimal structural type of the Prisma client surface we use. The real
 *  `PrismaClient` satisfies it — no SDK import needed in the package. */
export interface PrismaLike {
  thread: {
    findUnique(a: { where: { id: string } }): Promise<(Omit<ThreadDTO, 'createdAt' | 'updatedAt'> & { createdAt: Date; updatedAt: Date }) | null>;
    create(a: { data: { model?: string } }): Promise<ThreadDTO>;
    update(a: { where: { id: string }; data: { state: ExecutionState } }): Promise<unknown>;
    updateMany(a: { where: { id: string; state: ExecutionState }; data: { state: ExecutionState } }): Promise<{ count: number }>;
  };
  message: {
    create(a: { data: { threadId: string; agentId?: string | null; role: string; content: any } }): Promise<any>;
    findMany(a: { where: { threadId: string }; orderBy: { createdAt: 'asc' } }): Promise<any[]>;
  };
  agentEvent: {
    create(a: { data: { threadId: string; seq: number; type: string; payload: any } }): Promise<unknown>;
    findMany(a: { where: { threadId: string; seq?: { gt: number } }; orderBy: { seq: 'asc' } }): Promise<AgentEvent[]>;
    findFirst(a: { where: { threadId: string; type: string }; orderBy: { seq: 'desc' } }): Promise<AgentEvent | null>;
  };
  tokenUsage: {
    create(a: { data: NewUsage & { threadId: string } }): Promise<unknown>;
  };
  subagentRun: {
    create(a: { data: { threadId: string } & NewRun }): Promise<RunDTO>;
    update(a: { where: { id: string }; data: Record<string, unknown> }): Promise<RunDTO>;
  };
}

/** Reference Storage adapter over PostgreSQL/Prisma (schema in the README / spec §2.4). */
export class PrismaStorage implements Storage {
  constructor(private readonly prisma: PrismaLike) {}

  threads = {
    get: (threadId: string) => this.prisma.thread.findUnique({ where: { id: threadId } }),
    create: (init?: { model?: string }) =>
      this.prisma.thread.create({ data: { model: init?.model } }),
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
  };

  messages = {
    append: (threadId: string, m: NewMessage) =>
      this.prisma.message.create({
        data: { threadId, agentId: m.agentId ?? null, role: m.role, content: m.content },
      }),
    list: (threadId: string) =>
      this.prisma.message.findMany({ where: { threadId }, orderBy: { createdAt: 'asc' } }),
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
  };

  usage = {
    record: async (threadId: string, usage: NewUsage) => {
      await this.prisma.tokenUsage.create({
        data: {
          threadId,
          agentId: usage.agentId ?? null,
          totalTokens: usage.totalTokens,
        },
      });
    },
  };

  runs = {
    create: (threadId: string, run: NewRun) =>
      this.prisma.subagentRun.create({ data: { threadId, ...run } }),
    update: async (runId: string, patch: Partial<RunDTO>) => {
      await this.prisma.subagentRun.update({ where: { id: runId }, data: patch });
    },
  };
}
