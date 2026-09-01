import type {
  AgentEvent,
  ExecutionState,
  MessageDTO,
  NewMessage,
  NewRun,
  NewUsage,
  UsageTotals,
  RunDTO,
  ThreadDTO,
} from '../core/types.js';

/** Persistence port (§3.2). DTOs cross the boundary; ORM/entity types never
 *  leak into core. Implement this once for MongoDB, MySQL, DynamoDB — or use
 *  the shipped PrismaStorage reference adapter. */
export interface Storage {
  threads: {
    get(threadId: string): Promise<ThreadDTO | null>;
    create(init?: { model?: string }): Promise<ThreadDTO>;
    /** Most recent first — thread pickers / sidebars. */
    list(): Promise<ThreadDTO[]>;
    setState(threadId: string, state: ExecutionState): Promise<void>;
    /** Delete the thread AND everything that follows it: messages, events,
     *  usage rows, and subagent runs. Throws if the thread does not exist.
     *  The reference Prisma schema cascades via `onDelete: Cascade`. */
    delete(threadId: string): Promise<void>;
    /** Compare-and-set: returns true iff THIS caller performed the transition.
     *  Backs HITL reclamation + double-dispatch protection (§2.5, §2.8).
     *  Must be atomic — a single conditional UPDATE or equivalent. */
    claimState(threadId: string, from: ExecutionState, to: ExecutionState): Promise<boolean>;
  };
  messages: {
    append(threadId: string, message: NewMessage): Promise<MessageDTO>;
    list(threadId: string): Promise<MessageDTO[]>;
  };
  events: {
    append(threadId: string, event: AgentEvent): Promise<void>;
    /** All events after the cursor, ascending by seq — SSE replay (§2.2) */
    listSince(threadId: string, sinceSeq: number): Promise<AgentEvent[]>;
    /** Most recent event of a type — the HITL pending check (§2.5) */
    latest(threadId: string, type: string): Promise<AgentEvent | null>;
  };
  usage: {
    record(threadId: string, usage: NewUsage): Promise<void>;
    /** Every recorded segment for the thread, summed (§4). A thread with no
     *  usage rows yet returns zeroes, never null. */
    total(threadId: string): Promise<UsageTotals>;
  };
  runs: {
    create(threadId: string, run: NewRun): Promise<RunDTO>;
    update(runId: string, patch: Partial<RunDTO>): Promise<void>;
  };
}
