import type { LanguageModel, ToolSet } from 'ai';

/** Lifecycle of a thread. Durable truth lives in Storage.threads; the kv copy
 *  (`agent:state:{threadId}`) is a hot cache the engine polls (§2.1, §3.4). */
export type ExecutionState =
  | 'IDLE'
  | 'RUNNING'
  | 'WAITING_FOR_INPUT'
  | 'CANCELLED'
  | 'COMPLETED'
  | 'FAILED';

export type MessageRole = 'user' | 'assistant' | 'system' | 'tool';

/** Generation flavor of a handle / spawned subagent (§4). */
export type AgentKind = 'stream-text' | 'generate-text';

export interface ThreadDTO {
  id: string;
  state: ExecutionState;
  model: string;
  createdAt: Date;
  updatedAt: Date;
}

export interface MessageDTO {
  id: string;
  threadId: string;
  /** Producing agent; null = main agent (§2.7) */
  agentId: string | null;
  role: MessageRole;
  /** AI SDK message state, plain text, or a CONTEXT_SUMMARY envelope (§2.6) */
  content: unknown;
  createdAt: Date;
}

export interface NewMessage {
  agentId?: string | null;
  role: MessageRole;
  content: unknown;
}

/** Append-only event log entry: the replay source for SSE (re)connects and the
 *  durable record of INPUT_REQUIRED (HITL) requests. `seq` is assigned by the
 *  engine via Kv.incr before append (§3.4). */
export interface AgentEvent {
  threadId: string;
  seq: number;
  type: string;
  payload: unknown;
  createdAt: Date;
}

export interface RunDTO {
  id: string;
  threadId: string;
  name: string;
  model: string;
  depth: number;
  state: ExecutionState;
  result?: unknown;
  createdAt: Date;
  updatedAt: Date;
}

export interface NewRun {
  name: string;
  model: string;
  depth: number;
  state: ExecutionState;
}

/** Token attribution only (§4): total tokens used. USD/credit pricing is a
 *  downstream concern computed over the recorded counters. */
/** Token attribution (§4): the platform records the counters; USD/credit
 *  pricing is a downstream concern computed over them. */
export interface NewUsage {
  agentId?: string | null;
  /** Fresh (uncached) prompt tokens. */
  inputTokens: number;
  /** Prompt tokens served from the provider's prompt cache (§2.6 caching). */
  cachedInputTokens: number;
  outputTokens: number;
  /** input + cached + output. */
  totalTokens: number;
}

/** Additional provider-specific options, passed through to the provider from
 *  the AI SDK. Namespaced per provider — the platform never inspects them. */
export interface ProviderOptions {
  [provider: string]: any;
}

/** Shallow per-provider merge: the execute input wins over the spec default. */
export function mergeProviderOptions(
  base?: ProviderOptions,
  override?: ProviderOptions,
): ProviderOptions | undefined {
  if (!base) return override;
  if (!override) return base;
  return { ...base, ...override };
}

/** Dispatch ticket for the queue (§2.8). At-least-once — consumers must be
 *  idempotent. `agent` resolves via `AgentCore.getAgent`; when missing, the
 *  default handle executes. */
export interface RunJob {
  threadId: string;
  model: string;
  agent?: string;
  tokenBudget?: number;
  providerOptions?: ProviderOptions;
}

/** A model identity after resolution: the real provider instance (created
 *  lazily per run) and the declared context window, which feeds the §2.6
 *  compaction budget math. */
export interface ResolvedModel {
  instance: () => LanguageModel;
  contextWindow?: number;
}

/** Opt-in subagent delegation config (§2.7): the platform constructs the
 *  run-scoped spawnSubagent tool — users cannot build it themselves. */
export interface SubagentsConfig {
  /** Generation flavor for spawned subagents — picks the nested loop (§4).
   *  Default: 'stream-text'. A 'generate-text' child publishes only
   *  SUBAGENT_STARTED / SUBAGENT_COMPLETED / SUBAGENT_FAILED. */
  kind?: AgentKind;
  /** Registry key used when a delegation call omits `model`. */
  model?: string;
  /** Extra tools merged into every spawned subagent's toolset
   *  (HITL-wrapped identically to the parent's tools). */
  tools?: ToolSet;
}

export interface AgentConfig {
  /** How long a parked HITL wait may last before timing out (§2.5) */
  hitlTtlMs: number;
  /** Grace beyond the HITL TTL before orphan reclamation may claim a thread (§2.5) */
  reclaimGraceMs: number;
  /** AI-SDK step ceiling per run (§2.1 safety cap) */
  maxSteps: number;
  /** Queue redrive attempts before a run finalizes FAILED (§2.8) */
  runMaxAttempts: number;
  /** Default per-run token budget (input + output) when neither the execute
   *  input nor the agent spec declares one. `undefined` = unbounded apart
   *  from `maxSteps` (§2.1 safety cap). */
  tokenBudget?: number;
  /** Subagent nesting cap (§2.7) */
  subagentMaxDepth: number;
  /** Concurrent subagents per run (§2.7) */
  subagentMaxConcurrent: number;
  /** AI-SDK step ceiling per subagent run (§2.7) — tighter than `maxSteps` */
  subagentMaxSteps: number;
  /** Characters of a subagent result handed back to the parent (§2.7) */
  subagentResultCapChars: number;
  /** Universal context ceiling (§2.6) */
  contextCeilingTokens: number;
  /** Tokens reserved for the completion when compacting (§2.6) */
  contextOutputReserveTokens: number;
  /** Compact when history estimate exceeds this share of budget (§2.6) */
  compactionTrigger: number;
  /** Share of budget kept verbatim as the recent tail (§2.6) */
  contextTailShare: number;
  /** Prompt caching (§2.6): when true, the engine stamps Anthropic-style
   *  ephemeral cache breakpoints on the prompt prefix so providers that
   *  support marking cache it. OpenAI models cache automatically (≥1024
   *  tokens) and ignore the flag. Default: true. */
  promptCaching: boolean;
  /** Per-model native windows below the ceiling (§2.6) — merged over defaults.
   *  A `contextWindow` declared via `resolveModel` wins over this table. */
  nativeWindows?: Record<string, number>;
  /** Lease (seconds) for the per-thread run lock — must exceed the longest
   *  possible run, including parked HITL waits (§2.8, §3.4). */
  runLockLeaseSeconds: number;
  /** Billing pre-execution check (§4). Return `{ ok: false, error }` to reject a run. */
  billingPreCheck?: (threadId: string) => Promise<{ ok: boolean; error?: string }>;
}

export const DEFAULT_CONFIG: AgentConfig = {
  hitlTtlMs: 15 * 60_000,
  reclaimGraceMs: 60_000,
  maxSteps: 25,
  runMaxAttempts: 3,
  subagentMaxDepth: 2,
  subagentMaxConcurrent: 3,
  subagentMaxSteps: 10,
  subagentResultCapChars: 8_000,
  contextCeilingTokens: 265_000,
  contextOutputReserveTokens: 16_000,
  compactionTrigger: 0.8,
  contextTailShare: 0.25,
  promptCaching: true,
  runLockLeaseSeconds: 30 * 60,
};

export function resolveConfig(partial?: Partial<AgentConfig>): AgentConfig {
  const config = { ...DEFAULT_CONFIG, ...partial };
  if (config.subagentMaxSteps < 1 || config.subagentMaxSteps > config.maxSteps) {
    // A subagent must never get a looser step ceiling than its parent run (§2.7)
    throw new Error(
      `Invalid config: subagentMaxSteps (${config.subagentMaxSteps}) must be between 1 and maxSteps (${config.maxSteps})`,
    );
  }
  if (!Number.isInteger(config.runLockLeaseSeconds) || config.runLockLeaseSeconds < 1) {
    // The lease is the only thing that heals a crashed worker's lock — a
    // zero/fractional value would deadlock the thread forever (§3.4)
    throw new Error(
      `Invalid config: runLockLeaseSeconds (${config.runLockLeaseSeconds}) must be an integer of at least 1`,
    );
  }
  return config;
}
