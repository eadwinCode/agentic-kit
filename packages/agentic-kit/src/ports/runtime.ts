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
  SubagentsConfig,
  ThreadDTO,
  UsageTotals,
} from '../core/types.js';
import type { Storage } from './storage.js';
import type { AdminStore, RunFilter, StepRecord } from './admin.js';
import type { AgentRunState, BoundStorage } from '../core/state.js';

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
  config: AgentConfig;
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
  /** Additional provider-specific options, passed through to the provider
   *  from the AI SDK. Per-provider namespace; the execute input wins. */
  providerOptions?: ProviderOptions;
} & Omit<Parameters<typeof import('ai').streamText>[0],
    'model' | 'messages' | 'prompt' | 'system' | 'abortSignal'
    | 'maxSteps' | 'onStepFinish' | 'onError'> & {
  /** `system` is allowed here (static persona); per-run system is not. */
  system?: string;
  tools?: ToolSet;
  onChunk?: (para: any) => void | Promise<void>;   // chained after platform persistence
  onFinish?: (finishParams: any) => void | Promise<void>; // chained after platform finalize
};

export type GenerateTextAgentSpec = {
  /** Unique handle key — the queue dispatch key (§5). */
  name: string;
  model?: string;
  subagents?: boolean | SubagentsConfig;
  tokenBudget?: number;
  providerOptions?: ProviderOptions;
} & Omit<Parameters<typeof import('ai').generateText>[0],
    'model' | 'messages' | 'prompt' | 'abortSignal' | 'onFinish' | 'onStepFinish'> & {
  tools?: ToolSet;
  onFinish?: (finishParams: any) => void | Promise<void>; // chained after platform finalize
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

  /** Most recent first — thread pickers / sidebars. */
  listThreads(): Promise<ThreadDTO[]>;

  /** One call for UIs / history routes: thread + messages + recent events. */
  getThreadSnapshot(threadId: string): Promise<ThreadSnapshot | null>;

  /** Tokens spent so far and the §2.6 context load. Null when the thread is
   *  gone. Kept out of the snapshot so hydration stays one cheap read. */
  getThreadUsage(threadId: string): Promise<ThreadUsage | null>;

  /** Delete a thread and everything that follows it — messages, events,
   *  usage rows, subagent runs, and the thread's hot kv keys (§3.2).
   *  Refused while a run is active; stop() first. */
  deleteThread(threadId: string): Promise<DeleteThreadResult>;

  hitl: {
    respond(input: RespondInput): Promise<RespondResult>;
    reclaimIfOrphaned(threadId: string): Promise<boolean>;
  };

  events: {
    since(threadId: string, sinceSeq: number): Promise<AgentEvent[]>;
    subscribe(threadId: string, handler: (event: AgentEvent) => void): Promise<() => void>;
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
  };

  /** The queue dispatch side of the platform (§2.8): resolves the handle,
   *  applies the failure policy, and is idempotent under at-least-once
   *  delivery (the per-thread run lock, §3.4). The HTTP layer only verifies
   *  signatures, parses JSON, and calls this. */
  worker: {
    handleJob(job: RunJob): Promise<{ accepted: boolean; reason?: string }>;
  };
}
