import type { ToolSet } from 'ai';
import type {
  AgentEvent,
  AgentConfig,
  AgentKind,
  ExecutionState,
  MessageDTO,
  ProviderOptions,
  ResolvedModel,
  RunJob,
  SubagentsConfig,
  ThreadDTO,
} from '../core/types.js';
import type { Storage } from './storage.js';
import type { EventBus } from './bus.js';
import type { Queue } from './queue.js';
import type { Kv } from './kv.js';

// Re-exported for consumers: these flow through RuntimeOptions/RunJob/AgentCore.
export type { AgentKind, ProviderOptions, ResolvedModel, RunJob, SubagentsConfig } from '../core/types.js';

/** The ports bundle — everything in core/ receives this and nothing else. */
export interface RuntimePorts {
  storage: Storage;
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
  state?: ExecutionState;
  error?: string;
}

export interface StopResult {
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

/** Durable state used to hydrate a client before it starts live event replay. */
export interface ThreadSnapshot {
  thread: ThreadDTO;
  messages: MessageDTO[];
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
   *  (at-least-once duplicate delivery, §2.8) and nothing was executed. */
  execute(input: {
    threadId: string;
    model: string;
    tokenBudget?: number;
    providerOptions?: ProviderOptions;
  }): Promise<'executed' | 'lock-conflict'>;

  /** execute + §2.8 failure policy: redrive < maxAttempts, else finalize FAILED */
  executeWithPolicy(
    input: { threadId: string; model: string; tokenBudget?: number; providerOptions?: ProviderOptions },
    policy?: { maxAttempts?: number },
  ): Promise<void>;

  /** Persist user message → state RUNNING → enqueue a job dispatched back to
   *  THIS handle (the job carries `agent: this.name` and `tokenBudget`). */
  run(input: RunInput): Promise<RunResult>;

  /** Platform stop (§2.1) — works regardless of which agent's run is active. */
  stop(threadId: string): Promise<StopResult>;
}

export interface AgentCore {
  /** Resolve a registry key to the stable identity and provider instance used
   *  by execution, compaction, usage attribution, and persisted run metadata. */
  resolveModel(modelName: string): ResolvedModel;

  /** Most recent first — thread pickers / sidebars. */
  listThreads(): Promise<ThreadDTO[]>;

  /** One call for UIs / history routes: thread + messages + recent events. */
  getThreadSnapshot(threadId: string): Promise<ThreadSnapshot | null>;

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

  /** The queue dispatch side of the platform (§2.8): resolves the handle,
   *  applies the failure policy, and is idempotent under at-least-once
   *  delivery (the per-thread run lock, §3.4). The HTTP layer only verifies
   *  signatures, parses JSON, and calls this. */
  worker: {
    handleJob(job: RunJob): Promise<{ accepted: boolean; reason?: string }>;
  };
}
