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
    /** One thread, or null when it was never seen. */
    get(threadId: string): Promise<AdminThread | null>;
    /** Remove a thread with its runs and steps: what a deleted thread leaves
     *  behind in operational history (§3.2). An unknown thread is not an
     *  error. */
    delete(threadId: string): Promise<void>;
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
    /** Claim the right to run a run's settle hook (§5.6), in one conditional
     *  write: it wins only while the run is not settled and no other claim on
     *  it is newer than `staleBefore`. A claim older than that belongs to a
     *  settler that died, and is taken over. False when the run is unknown,
     *  settled, or claimed by someone else. */
    claimSettle(runId: string, token: string, staleBefore: Date): Promise<boolean>;
    /** Add to a run's counters in one write (`SET steps = steps + n`), so two
     *  segments that close together both count. An unknown run is not an
     *  error. */
    increment(runId: string, deltas: RunDeltas): Promise<void>;
    /** Count and sum every run the filter matches, grouped in the store:
     *  exact however many runs there are. `limit` and `before` are ignored. */
    totals(filter: RunFilter): Promise<RunTotals>;
    /** End a claim made with `token`. `settled` marks the run settled for
     *  good; otherwise the claim is dropped so a later settle can run the hook
     *  again. A claim that was taken over is left alone. */
    endSettle(runId: string, token: string, settled: boolean): Promise<void>;
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

/** What started a thread (§2.9): the first dispatched run's parameters,
 *  recorded once and never overwritten, so a listing can say who asked for
 *  what without opening the thread. `prompt`, `state` and `providerOptions`
 *  are present only when `recordPayloads` is on. */
export interface ThreadStart {
  runId: string;
  agent: string;
  model: string;
  at: Date;
  prompt?: string | null;
  tokenBudget?: number | null;
  state?: Record<string, unknown> | null;
  providerOptions?: Record<string, unknown> | null;
}

export interface AdminThread {
  id: string;
  state: ExecutionState;
  model: string;
  firstSeenAt: Date;
  updatedAt: Date;
  /** The parameters that started it; null for a thread seen before this was recorded. */
  startedWith?: ThreadStart | null;
}

/** `startedWith` is written on first sight only: a later upsert never
 *  replaces what started the thread. */
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
  /** Only runs on these threads. Omitted or empty means every thread. */
  threadIds?: string[];
  since?: Date;
  until?: Date;
  /** Only runs that have ended and whose settle has not run (§5.6): what the
   *  late-settle sweep looks for. */
  unsettled?: boolean;
  /** Only runs at this depth: 0 is a dispatched run. Omitted is every depth. */
  depth?: number;
  /** Pages through a listing: only runs that sort after this one, newest
   *  first by start time and then by id. Pass the last run of the previous
   *  page. */
  before?: RunCursor;
  /** Newest first. Implementations cap this — core passes a bounded value. */
  limit?: number;
}

/** Amounts to add to a run's counters (see `runs.increment`). */
export interface RunDeltas {
  steps: number;
  inputTokens: number;
  cachedInputTokens: number;
  outputTokens: number;
  totalTokens: number;
}

/** What `runs.totals` counts over a set of runs. Tokens are summed from the
 *  run records: tokens only, no money. */
export interface RunTotals {
  runs: number;
  byState: Partial<Record<ExecutionState, number>>;
  byStopReason: Record<string, number>;
  steps: number;
  inputTokens: number;
  cachedInputTokens: number;
  outputTokens: number;
  totalTokens: number;
}

/** A place in a run listing (see `RunFilter.before`). */
export interface RunCursor {
  startedAt: Date;
  id: string;
}
