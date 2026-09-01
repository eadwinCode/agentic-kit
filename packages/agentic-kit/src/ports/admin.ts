import type { ExecutionState, NewRunRecord, RunPatch, RunRecord } from '../core/types.js';

/** Operational history — run records and step timings (§2.9).
 *
 *  NOT a port a caller implements. agentic-kit owns this data and stores it
 *  itself: SQLite in development, Postgres in production via
 *  AGENTIC_KIT_ADMIN_DATABASE_URL. Callers only ever read it back, through
 *  `runtime.admin`, and build whatever view they want on top.
 *
 *  It carries no StorageContext. A run's state belongs to the caller's own
 *  data (§2.10); operational history is the platform's, and scoping it by
 *  tenant is the reader's business, not the writer's. */
export interface AdminStore {
  /** The platform's OWN view of threads (§2.9). Deliberately a copy of the few
   *  fields an operational view needs — not the caller's thread table, which
   *  it never reads. That is what lets a dashboard answer "what is running
   *  right now" without touching their database at all. */
  threads: {
    /** Record a thread at its current state. Called on every transition. */
    upsert(thread: NewAdminThread): Promise<void>;
    countByState(): Promise<Partial<Record<ExecutionState, number>>>;
    list(filter: AdminThreadFilter): Promise<AdminThread[]>;
  };
  runs: {
    start(run: NewRunRecord): Promise<RunRecord>;
    patch(runId: string, patch: RunPatch): Promise<void>;
    get(runId: string): Promise<RunRecord | null>;
    /** Every run on a thread, newest first — nested runs included. */
    listByThread(threadId: string): Promise<RunRecord[]>;
    list(filter: RunFilter): Promise<RunRecord[]>;
    /** Counts by state across every run. The "what is happening right now"
     *  aggregate, pushed down so it never drags rows into memory. */
    countByState(): Promise<Partial<Record<ExecutionState, number>>>;
  };
  steps: {
    record(step: NewStepRecord): Promise<void>;
    /** A run's steps in order. */
    listByRun(runId: string): Promise<StepRecord[]>;
    /** Every step on a thread, oldest first — the shape a timeline wants. */
    listByThread(threadId: string): Promise<StepRecord[]>;
  };
  /** Release any handles. Development stores hold an open file. */
  close?(): Promise<void>;
}

/** One completed loop iteration (§2.9). */
export interface StepRecord {
  runId: string;
  /** Denormalised so a thread's whole timeline — across every run on it, main
   *  and nested — is one query rather than one per run. */
  threadId: string;
  /** Which stream ran it — null is the main agent (§2.7). */
  agentId: string | null;
  index: number;
  durationMs: number;
  finishReason: string;
  inputTokens: number;
  cachedInputTokens: number;
  outputTokens: number;
  totalTokens: number;
  /** Tools the step executed, by name — the summary line. */
  tools: string[];
  /** What the step said, capped. Timing tells you where a run spent itself;
   *  this tells you what it actually did. */
  text?: string | null;
  /** The tools it ran, with arguments and results, each capped.
   *
   *  Note what this means: tool arguments and results are operational data now,
   *  sitting in the platform's store. If yours carry anything you would not
   *  want there, set `recordPayloads: false`. */
  toolCalls?: Array<{ toolName: string; args: unknown; result: unknown }>;
  at: Date;
}

export type NewStepRecord = Omit<StepRecord, 'at'> & { at?: Date };

export interface AdminThread {
  id: string;
  state: ExecutionState;
  model: string;
  firstSeenAt: Date;
  updatedAt: Date;
}

export type NewAdminThread = Omit<AdminThread, 'firstSeenAt' | 'updatedAt'>;

export interface AdminThreadFilter {
  state?: ExecutionState[];
  since?: Date;
  limit?: number;
}

export interface RunFilter {
  /** Any of these states; omitted means all. */
  state?: ExecutionState[];
  agent?: string;
  threadId?: string;
  since?: Date;
  until?: Date;
  /** Newest first. Implementations cap this — core passes a bounded value. */
  limit?: number;
}
