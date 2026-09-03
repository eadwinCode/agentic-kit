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
}

export interface RunResult {
  accepted: boolean;
  threadId: string;
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

/** What the platform hands a spec's `onFinish` once the run is finalized. */
export interface RunFinishInfo {
  threadId: string;
  runId?: string;
  state: ExecutionState;
  stopReason: string;
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
}

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
  tools?: ToolSet;
  onChunk?: (para: any) => void | Promise<void>;   // chained after platform persistence
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
  /** Fires once, after the platform finalized the run (§4). */
  onFinish?: (info: RunFinishInfo) => void | Promise<void>;
};

/** An executor bound to a generation flavor and to the user's generation
 *  arguments (§3). Returned by the `create*Agent` factories. */
export interface AgentHandle {
  readonly name: string;
  readonly kind: AgentKind;

  /** Worker-side only (§5.6). Throws on failure — see executeWithPolicy.
   *  Returns 'lock-conflict' when another worker owns the thread's run lock
   *  (nothing was executed) and 'stale' when a newer run has replaced this
   *  one (§2.1, §2.8). */
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
    handleJob(job: RunJob): Promise<{ accepted: boolean; reason?: string }>;
  };
}
