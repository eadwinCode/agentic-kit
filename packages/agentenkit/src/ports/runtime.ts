import type { ToolSet } from 'ai';
import type {
  AgentEvent,
  AgentConfig,
  AgentKind,
  ContextUsage,
  ExecutionState,
  MessageDTO,
  ProviderOptions,
  ResolvedModel,
  RunJob,
  RunRecord,
  Cost,
  NewUsage,
  SubagentsConfig,
  ThreadDTO,
  UsageTotals,
} from '../core/types.js';
import type { TokenAttribution } from '../core/usage.js';
import type { Storage } from './storage.js';
import type { AdminStore, RunFilter, StepRecord } from './admin.js';
import type { AgentRunState, BoundStorage } from '../core/state.js';
import type { FollowOptions, SseOptions, SseStream } from '../core/follow.js';
import type { PublishEventOptions } from '../core/publish.js';

export type { AdminStore, NewStepRecord, RunFilter, StepRecord } from './admin.js';
export type { AgentRunState, BoundStorage, StorageContext } from '../core/state.js';
import type { EventBus } from './bus.js';
import type { Queue } from './queue.js';
import type { Kv } from './kv.js';
import type { ExecuteInput, ExecuteOutcome } from '../core/engine.js';
import type {
  AdminOverview,
  RunDetail,
  RunStats,
  ThreadDetail,
  ThreadSummary,
} from '../core/admin.js';
export type {
  AdminOverview,
  RunDetail,
  RunStats,
} from '../core/admin.js';

export type { ExecuteInput, ExecuteOutcome } from '../core/engine.js';

// Re-exported for consumers: these flow through RuntimeOptions/RunJob/AgentCore.
export type { AgentKind, ProviderOptions, ResolvedModel, RunJob, SubagentsConfig } from '../core/types.js';

/** The ports bundle — everything in core/ receives this and nothing else.
 *
 *  `storage` is the caller's implementation with THIS run's context already
 *  bound (§2.10), so core keeps calling `messages.append(threadId, msg)` while
 *  the implementation still receives the state. `admin` is the platform's own
 *  operational store (§2.9) — never the caller's. */
export interface RuntimePorts {
  storage: BoundStorage;
  admin: AdminStore;
  bus: EventBus;
  queue: Queue;
  kv: Kv;
  /** User-provided model resolution (§3.3): models can live in any shape on
   *  the consumer side — the platform only ever sees `ResolvedModel`. */
  resolveModel(modelName: string): ResolvedModel;
  /** Puts a price on every model call before its usage row is stored (§4).
   *  Omitted, every row is stored unpriced. */
  pricer?: Pricer;
  /** Where the platform reports what it could not do without failing the
   *  run, such as a usage row it failed to store. Defaults to `console`. */
  log?: Logger;
  config: AgentConfig;
}

/** The little the platform needs from a logger. `console` satisfies it. */
export interface Logger {
  error(message: string, ...rest: unknown[]): void;
}

/** Turns one model call into money (§4). The runtime calls it after every
 *  call, before the usage row is stored, so cost is part of the row rather
 *  than something a reader has to work out later.
 *
 *  It runs on the run's own path: keep it fast and side-effect free. A price
 *  list lookup is the intended shape; a network call is not. */
export interface Pricer {
  /** Returns null when it cannot price this call. The row is then stored
   *  unpriced, and in a chain the next pricer gets its turn. A throw is
   *  logged and treated the same way: an unpriceable call must never fail a
   *  run. */
  price(usage: NewUsage): Cost | null | Promise<Cost | null>;
}

export interface RuntimeOptions {
  storage: Storage;
  /** Where operational history goes (§2.9). Omitted, it is chosen from the
   *  environment: Postgres when AGENTIC_KIT_ADMIN_DATABASE_URL is set, SQLite
   *  on disk otherwise. Pass one to decide for yourself. */
  admin?: AdminStore;
  bus: EventBus;
  queue: Queue;
  kv: Kv;
  /** Models can come in any shape — config files, a database, provider SDKs.
   *  The platform only ever sees the resolved `ResolvedModel`. */
  resolveModel(modelName: string): ResolvedModel;
  /** Prices every model call (§4). Omitted records tokens only, and every
   *  `UsageTotals` comes back with `unpriced` above zero. See the `pricing`
   *  module for the three that ship: a price table, a provider receipt
   *  reader, and a chain of both. */
  pricer?: Pricer;
  /** Where the platform reports what it could not do without failing the
   *  run. Defaults to `console`. */
  log?: Logger;
  config?: Partial<AgentConfig>;
}

export interface RunInput {
  /** Omit to create a fresh thread first (threads.create, §3.2) */
  threadId?: string;
  prompt: string;
  /** Carried through this whole run (§2.10): every storage call, every tool,
   *  every nested run. Persisted on the dispatch, so a worker picking the job
   *  up later — or resuming it after an approval — sees the same thing. The
   *  platform never reads it. */
  state?: AgentRunState;
  /** Edit + resend (§5.1): replace this user message with `prompt` and drop
   *  every message after it, then answer again from that point. Must name a
   *  message in this thread whose role is 'user'. Omit for a normal turn. */
  editMessageId?: string;
  /** Registry key or provider instance reference — resolved via
   *  `AgentCore.resolveModel`. Overrides the spec default. */
  model?: string;
  /** Max cumulative tokens (input + output) for this run — overrides
   *  spec.tokenBudget / config (§2.1 safety cap). Flows to the worker
   *  via RunJob.tokenBudget. */
  tokenBudget?: number;
  /** Max spend for this run (§4), in millionths of the pricer's currency:
   *  250_000 stops the run after roughly $0.25. Overrides
   *  spec.costBudgetMicros / config. Needs a pricer; without one nothing is
   *  ever priced and the cap can never be reached. */
  costBudgetMicros?: number;
  /** Additional provider-specific options, passed through to the provider
   *  from the AI SDK (§3.1). Merged over the spec default: the execute
   *  input wins per provider namespace. */
  providerOptions?: ProviderOptions;
  /** Name the run yourself (§2.1): your own records can be keyed by it
   *  before dispatch, and the worker sees the same id. Reusing one is
   *  refused, never silently re-run. */
  runId?: string;
  /** Cap this run's round trips below the config's `maxSteps`; a larger
   *  value is clamped to it. */
  maxSteps?: number;
  /** Images the user sent with the prompt. They are stored as image parts on
   *  the user message and reach the model natively. */
  attachments?: Attachment[];
  /** The caller's tenant, written on the dispatch ticket so a queue that
   *  spreads its claims across partitions can keep one tenant's backlog from
   *  starving the others. Opaque to the platform. */
  partitionKey?: string;
  /** The sending client's own name for the user turn. It comes back on the
   *  turn's MESSAGE_APPENDED, so the client that sent it can swap its
   *  optimistic copy for the real one by id, never by matching text. Opaque
   *  to the platform, and not stored. */
  clientMessageId?: string;
}

/** An image on a user turn: a URL the provider can fetch, or a data: URL. */
export interface Attachment {
  url: string;
  mediaType?: string;
}

/** Why a run was refused, for a host that answers differently to each:
 *  `active_run` (the thread already has one), `queue_full` (try again
 *  shortly), `billing` (the billing check said no). */
export type RefusedReason = 'active_run' | 'queue_full' | 'billing';

export interface RunResult {
  accepted: boolean;
  threadId: string;
  /** For the refusals a host acts on. */
  reason?: RefusedReason;
  /** This run's id (§2.1) — the same one carried by the enqueued job. An
   *  in-process worker must pass it back, or its dispatch has no identity. */
  runId?: string;
  state?: ExecutionState;
  error?: string;
}

export interface StopResult {
  accepted: boolean;
  error?: string;
}

export interface DeleteThreadResult {
  accepted: boolean;
  error?: string;
}

export interface RespondInput {
  threadId: string;
  toolCallId: string;
  approved: boolean;
  payload?: unknown;
  /** The run state (§2.10) for the storage calls answering makes. The RESUMED
   *  run rebuilds its own state from the park's ticket — this is only for the
   *  reads and writes `respond` itself performs. */
  state?: AgentRunState;
}

export interface RespondResult {
  delivered: boolean;
  error?: string;
}

/** What a thread has spent, and how full its context is (§2.6, §4). */
export interface ThreadUsage {
  /** Every run segment's tokens, summed. */
  tokens: UsageTotals;
  /** What the next run's prompt would carry before compaction. */
  context: ContextUsage;
  /** The model the budget was measured against. */
  model: string;
}

/** What the platform hands a spec's `onSettle` and `onFinish`. */
export interface RunFinishInfo {
  threadId: string;
  runId?: string;
  state: ExecutionState;
  stopReason: string;
  /** A user stop (§2.1). */
  cancelled: boolean;
  /** Why the run failed, when it did. */
  error?: string;
  tokensUsed: number;
  /** The tokens THIS segment spent. A run that parked and resumed finishes
   *  once, so this is the last segment, not the whole run — and it is tokens
   *  only; the money is in `usage`. */
  attribution: TokenAttribution;
  steps: number;
  /** The whole run's tokens AND money: every segment and every nested run,
   *  read back with `usage.total(threadId, { runId })`. Its `lines` are the
   *  bill, one per agent and model, so a settle hook charges in one pass
   *  without keeping its own tally (§4). Zeroed when the storage read failed;
   *  `unpriced` above zero means some calls went unpriced and `costMicros` is
   *  a floor. */
  usage: UsageTotals;
  /** Set when the platform could not read the run's rows back. `usage` is
   *  then zeroed, and a hook that bills from it should refuse to settle
   *  rather than charge nothing: throw from `onSettle` and the run fails
   *  instead of going free. */
  usageError?: unknown;
}

/** A spec's settle hook (§5.6): where a run is charged. It runs once per run,
 *  whatever way the run ends — completed, stopped (while running, queued or
 *  parked), failed, or found later by the late-settle sweep — before the
 *  terminal state is written when a worker ends the run. A throw fails a run
 *  that was going to complete, and leaves the run unsettled so a later
 *  settle runs it again.
 *
 *  Make it idempotent by `runId`. Once is kept by a claim on the run record,
 *  but a hook slower than the claim (10 minutes), or a mark that could not be
 *  written, can see the same run twice. */
export type SettleFn = (info: RunFinishInfo) => void | Promise<void>;

/** Builds the persona per step with the run's state (§3.1). It wins over the
 *  static `system` when set. A throw fails the step, like a model error. */
export type SystemFn = (threadId: string, state: AgentRunState) => string | Promise<string>;

/** Edits the prompt for one step, just before it is sent (§3.1): `messages` is
 *  the history the platform assembled (compacted, repaired, cache-stamped) and
 *  what comes back is what the model sees. It is the place for context that
 *  must NOT be saved — a screenshot the model should look at once, an editor
 *  snapshot — because anything added here is gone on the next step unless it
 *  is added again. */
export type PrepareStepFn = (
  threadId: string,
  state: AgentRunState,
  messages: Array<any>,
) => Array<any> | Promise<Array<any>>;

/** Durable state used to hydrate a client before it starts live event replay. */
export interface ThreadSnapshot {
  thread: ThreadDTO;
  messages: MessageDTO[];
  /** Nested runs on this thread (§2.7) — name, depth and final state, so a
   *  reconnecting client rebuilds its subagent panel without depending on
   *  events that only replay while a run is unfinished. */
  runs: RunRecord[];
  /** Cursor for starting live replay without duplicating snapshot state. */
  lastEventSeq: number;
  /** Only the unfinished run's events, used to restore transient activity. */
  activeEvents: AgentEvent[];
}

/** Everything streamText accepts except the platform-owned keys (§3.1).
 *  `onChunk` / `onFinish` ARE allowed — the platform chains them (§4). */
export type StreamTextAgentSpec = {
  /** Unique handle key — the queue dispatch key (§5). */
  name: string;
  /** Registry key for this agent's model — resolved via
   *  `AgentCore.resolveModel` (instance + contextWindow). */
  model?: string;
  /** Opt-in subagent delegation (§2.7). */
  subagents?: boolean | SubagentsConfig;
  /** Default per-run token budget (input + output). Per-run
   *  `input.tokenBudget` wins; `undefined` = unbounded apart from `maxSteps`. */
  tokenBudget?: number;
  /** Default per-run money cap (§4), in millionths of the pricer's currency.
   *  Per-run `input.costBudgetMicros` wins; `undefined` = unbounded. Needs a
   *  pricer: an unpriced call spends no money and so can never exhaust it. */
  costBudgetMicros?: number;
  /** Additional provider-specific options, passed through to the provider
   *  from the AI SDK. Per-provider namespace; the execute input wins. */
  providerOptions?: ProviderOptions;
} & Omit<Parameters<typeof import('ai').streamText>[0],
    'model' | 'messages' | 'prompt' | 'system' | 'abortSignal'
    | 'maxSteps' | 'onStepFinish' | 'onError' | 'onFinish' | 'onChunk'> & {
  /** `system` is allowed here (static persona); per-run system is not. */
  system?: string;
  /** The persona built per step with the run's state; wins over `system`. */
  systemFn?: SystemFn;
  /** Edits the prompt per step, for context that must not be saved. */
  prepareStep?: PrepareStepFn;
  tools?: ToolSet;
  onChunk?: (para: any) => void | Promise<void>;   // chained after platform persistence
  /** Charges the run (§5.6). See SettleFn. */
  onSettle?: SettleFn;
  /** Fires once, after the platform finalized the run, with what the run did
   *  and what it spent (§4). */
  onFinish?: (info: RunFinishInfo) => void | Promise<void>;
};

export type GenerateTextAgentSpec = {
  /** Unique handle key — the queue dispatch key (§5). */
  name: string;
  model?: string;
  subagents?: boolean | SubagentsConfig;
  tokenBudget?: number;
  costBudgetMicros?: number;
  providerOptions?: ProviderOptions;
} & Omit<Parameters<typeof import('ai').generateText>[0],
    'model' | 'messages' | 'prompt' | 'abortSignal' | 'onFinish' | 'onStepFinish'> & {
  tools?: ToolSet;
  /** The persona built per step with the run's state; wins over `system`. */
  systemFn?: SystemFn;
  /** Edits the prompt per step, for context that must not be saved. */
  prepareStep?: PrepareStepFn;
  /** Charges the run (§5.6). See SettleFn. */
  onSettle?: SettleFn;
  /** Fires once, after the platform finalized the run (§4). */
  onFinish?: (info: RunFinishInfo) => void | Promise<void>;
};

/** What a stuck-run sweep did. */
export interface ReclaimReport {
  /** How many run records the sweep looked at. */
  checked: number;
  /** How many runs went back on the queue or were moved to their end state. */
  redispatched: number;
  /** How many ended runs had their settle run late. */
  settled: number;
  /** How many runs the sweep could not act on. */
  errors: number;
}

/** An executor bound to a generation flavor and to the user's generation
 *  arguments (§3). Returned by the `create*Agent` factories. */
export interface AgentHandle {
  readonly name: string;
  readonly kind: AgentKind;

  /** Worker-side only (§5.6). Throws on failure — see executeWithPolicy.
   *  Returns 'lock-conflict' when another worker owns the thread's run lock
   *  (nothing was executed), 'stale' when a newer run has replaced this
   *  one (§2.1, §2.8), and 'lock-lost' when this worker could not keep the
   *  lock and stopped early (§3.4). */
  execute(input: ExecuteInput): Promise<ExecuteOutcome>;

  /** execute + §2.8 failure policy: redrive < maxAttempts, else finalize FAILED */
  executeWithPolicy(
    input: ExecuteInput,
    policy?: { maxAttempts?: number },
  ): Promise<void>;

  /** Persist user message → state RUNNING → enqueue a job dispatched back to
   *  THIS handle (the job carries `agent: this.name` and `tokenBudget`). */
  run(input: RunInput): Promise<RunResult>;

  /** Platform stop (§2.1) — works regardless of which agent's run is active. */
  stop(threadId: string, state?: AgentRunState): Promise<StopResult>;
}

export interface AgentCore {
  /** Resolve a registry key to the stable identity and provider instance used
   *  by execution, compaction, usage attribution, and persisted run metadata. */
  resolveModel(modelName: string): ResolvedModel;

  /** Most recent first — thread pickers / sidebars. Takes the run state
   *  (§2.10) so a tenant-scoped Storage can filter; a read has no dispatch
   *  ticket to carry it on. */
  listThreads(state?: AgentRunState): Promise<ThreadDTO[]>;

  /** One call for UIs / history routes: thread + messages + recent events. */
  getThreadSnapshot(threadId: string, state?: AgentRunState): Promise<ThreadSnapshot | null>;

  /** Tokens spent so far and the §2.6 context load. Null when the thread is
   *  gone. Kept out of the snapshot so hydration stays one cheap read. */
  getThreadUsage(threadId: string, state?: AgentRunState): Promise<ThreadUsage | null>;

  /** Delete a thread and everything that follows it — messages, events,
   *  usage rows, subagent runs, and the thread's hot kv keys (§3.2).
   *  Refused while a run is active; stop() first. */
  deleteThread(threadId: string, state?: AgentRunState): Promise<DeleteThreadResult>;

  /** The backstop for a run that nothing is working on (§2.5, §2.8): a QUEUED
   *  or RUNNING record older than `olderThanMs` whose lock nobody holds and
   *  whose job is gone is re-dispatched, and an ended record older than that
   *  whose settle never ran is settled. Call it from a periodic job. It pages
   *  through every such run. A record with no recorded state is read with an
   *  empty state. */
  reclaimStuckRuns(olderThanMs: number): Promise<ReclaimReport>;

  hitl: {
    respond(input: RespondInput): Promise<RespondResult>;
    reclaimIfOrphaned(threadId: string, state?: AgentRunState): Promise<boolean>;
  };

  events: {
    since(threadId: string, sinceSeq: number, state?: AgentRunState): Promise<AgentEvent[]>;
    subscribe(threadId: string, handler: (event: AgentEvent) => void): Promise<() => void>;
    /** Replay then live, as one sequence, with the cursor discipline already
     *  applied (§2.2): subscribe before replaying, never emit at or below the
     *  cursor, forward a seq-0 notice without moving it.
     *
     *  Framework-neutral — an async iterable is something Express, Nest, Hono,
     *  Next or a plain worker can each consume in their own way. Pass a signal,
     *  or the subscription outlives the client. */
    follow(
      threadId: string,
      options?: FollowOptions & { state?: AgentRunState },
    ): AsyncGenerator<AgentEvent>;
    /** `follow`, encoded as Server-Sent Events. Returns the stream and the
     *  headers rather than a Response, because half the ecosystem has none. */
    sse(threadId: string, options?: SseOptions & { state?: AgentRunState }): SseStream;
    /** Publish an event of your own on a thread, from anywhere on the server
     *  — a webhook, a cron job, a route. Tools get the same thing bound to
     *  their thread as `publishEvent` on their options. Durable by default;
     *  `{ durable: false }` is a bus-only notice. Platform event types are
     *  refused. */
    publishEvent(
      threadId: string,
      type: string,
      payload: unknown,
      options?: PublishEventOptions & { state?: AgentRunState },
    ): Promise<AgentEvent>;
  };

  /** Agent factories — see §4. Each call registers a handle under `spec.name`. */
  createStreamTextAgent(spec: StreamTextAgentSpec): AgentHandle;
  createGenerateTextAgent(spec: GenerateTextAgentSpec): AgentHandle;

  /** Worker-side resolution of a registered handle from the queue job. */
  getAgent(name: string): AgentHandle | null;

  /** Operational reads (§2.9). The platform records what runs did; building a
   *  view over it is the caller's business. Everything here comes from the
   *  platform's OWN store — it never reads the caller's database. */
  admin: {
    overview(range?: { since?: Date }): Promise<AdminOverview>;
    listRuns(filter?: RunFilter): Promise<RunRecord[]>;
    stats(range?: { since?: Date; until?: Date }): Promise<RunStats>;
    getRun(runId: string): Promise<RunDetail | null>;
    listRunsByThread(threadId: string): Promise<RunRecord[]>;
    listSteps(runId: string): Promise<StepRecord[]>;
    /** Threads with their runs rolled up — the top level of an operational
     *  view, since a thread is what a person recognises (§2.9). */
    listThreads(filter?: {
      state?: ExecutionState[];
      since?: Date;
      limit?: number;
    }): Promise<ThreadSummary[]>;
    getThread(threadId: string): Promise<ThreadDetail | null>;
  };

  /** The queue dispatch side of the platform (§2.8): resolves the handle,
   *  applies the failure policy, and is idempotent under at-least-once
   *  delivery (the per-thread run lock, §3.4). The HTTP layer only verifies
   *  signatures, parses JSON, and calls this. */
  worker: {
    /** Run one job. Throws `UnknownAgentError` for a job naming an agent this
     *  process does not have, so a queue that retries on failure keeps it.
     *  Abort `signal` on shutdown: the segment stops at once and the job goes
     *  back on the queue without spending an attempt. */
    handleJob(job: RunJob, options?: { signal?: AbortSignal }): Promise<{ accepted: boolean; reason?: string }>;
    /** What a queue calls when it gives up on a job (§2.8): the run behind it
     *  is failed with the reason and settled, so its thread does not read
     *  QUEUED or RUNNING for ever. A job whose run has already moved on is
     *  left alone. */
    handleDeadJob(job: RunJob, attempts: number, cause: unknown): Promise<void>;
  };

  /** The ports bundle, scoped to a run's state when one is given (§2.10): for
   *  a host that reads storage, publishes or checks the kv the way the
   *  platform does. */
  ports(state?: AgentRunState): RuntimePorts;
}
