import type {
  AgentEvent,
  ExecutionState,
  MessageDTO,
  NewMessage,
  NewRun,
  NewUsage,
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
    setState(threadId: string, state: ExecutionState): Promise<void>;
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
  };
  runs: {
    create(threadId: string, run: NewRun): Promise<RunDTO>;
    update(runId: string, patch: Partial<RunDTO>): Promise<void>;
  };
}
