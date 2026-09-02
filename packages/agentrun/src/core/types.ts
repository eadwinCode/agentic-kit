import type { LanguageModel, ToolSet } from 'ai';
import type { AgentRunState } from './state.js';

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

/** The durable record of ONE agent run (§2.9): when it started, how it ended,
 *  what it cost. Keyed by the run id the platform already mints to fence stale
 *  workers (§2.1), so a run is observable by the same identity that makes it
 *  correct. A thread accumulates many of these over its life — `Thread.state`
 *  only ever describes the latest one. */
export interface RunRecord {
  /** The run id (§2.1) for a dispatched run; the nested run's id otherwise —
   *  which is also the `agentId` its messages and events are tagged with. */
  id: string;
  threadId: string;
  /** The run that spawned this one; null for a dispatched run (§2.7). */
  parentRunId?: string | null;
  /** 0 = the main agent, 1+ = nested. */
  depth: number;
  /** Registered handle for a dispatched run; the delegation's name for a
   *  nested one. */
  agent: string;
  model: string;
  state: ExecutionState;
  /** 'completed' | 'cancelled' | 'token_budget' | 'max_steps', set at finalize. */
  stopReason?: string | null;
  /** Why it failed, when it did. Previously dropped on the floor (§2.8). */
  error?: string | null;
  /** When the run was accepted and enqueued. */
  startedAt: Date;
  endedAt?: Date | null;
  /** endedAt - startedAt, denormalised so a listing never recomputes it. A
   *  parked run legitimately spans however long the human took (§2.5). */
  durationMs?: number | null;
  /** Milliseconds between enqueue and a worker starting work — the number that
   *  says whether workers are keeping up (§2.8). */
  queuedMs?: number | null;
  /** Loop iterations completed, summed across every segment of the run. */
  steps: number;
  inputTokens: number;
  cachedInputTokens: number;
  outputTokens: number;
  totalTokens: number;
  /** §2.8 redrive attempts consumed. Only a dispatched run has these. */
  attempts: number;
  /** What the run was dispatched with (§2.9) — the prompt that started it, the
   *  budget it was given, and the state it carries. Present only when
   *  `recordPayloads` is on. A nested run's is its brief. */
  prompt?: string | null;
  tokenBudget?: number | null;
  runState?: Record<string, unknown> | null;
  /** A nested run's capped result, handed back to its parent (§2.7). */
  result?: unknown;
}

export interface NewRunRecord {
  id: string;
  threadId: string;
  agent: string;
  model: string;
  prompt?: string | null;
  tokenBudget?: number | null;
  runState?: Record<string, unknown> | null;
  /** Defaults to 0 — a dispatched run. */
  depth?: number;
  parentRunId?: string | null;
}

export type RunPatch = Partial<Omit<RunRecord, 'id' | 'threadId'>>;

/** Token attribution only (§4): total tokens used. USD/credit pricing is a
 *  downstream concern computed over the recorded counters. */
/** Token attribution (§4): the platform records the counters; USD/credit
 *  pricing is a downstream concern computed over them. */
/** Cumulative token attribution across every run on a thread (§4). */
export interface UsageTotals {
  /** Fresh (uncached) prompt tokens. */
  inputTokens: number;
  /** Prompt tokens served from the provider's prompt cache (§2.6). */
  cachedInputTokens: number;
  outputTokens: number;
  /** input + cached + output. */
  totalTokens: number;
}

/** How full the next run's prompt would be (§2.6). Token counts are the same
 *  rough estimate the engine itself compacts on, so the number a user sees is
 *  the number the platform acts on. */
export interface ContextUsage {
  /** Estimated tokens the stored history occupies right now. */
  usedTokens: number;
  /** What history may fill: the model's window minus the output reserve. */
  budgetTokens: number;
  /** Compaction runs above this — `budgetTokens × compactionTrigger`. */
  compactAtTokens: number;
  /** Messages currently in the thread, summaries included. */
  messages: number;
}

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
  /** Identifies THIS dispatch (§2.1). A thread has one live run at a time and
   *  `agent:run:{threadId}` holds its id; a job whose id no longer matches has
   *  been replaced by a newer run and must not execute. Omitted on legacy
   *  dispatches, which keep the old no-identity behavior. */
  runId?: string;
  /** Epoch ms at enqueue. The worker subtracts it on pickup to record how long
   *  the job waited — the number that says whether workers keep up (§2.9). */
  enqueuedAt?: number;
  /** The run's state (§2.10), so a worker rehydrates exactly what the caller
   *  attached — hours later, in another process, after an approval. */
  state?: AgentRunState;
  tokenBudget?: number;
  providerOptions?: ProviderOptions;
}

/** Identifies a nested run well enough to re-enter its loop (§2.7). Persisted
 *  in the INPUT_REQUIRED payload so a resume never has to read it back. */
export interface NestedDescriptor {
  agentId: string;
  name: string;
  model: string;
  depth: number;
}

/** Everything needed to resume a parked HITL run segment (§2.5). Persisted
 *  inside the INPUT_REQUIRED event payload so any late reader — /respond or
 *  TTL reclamation — can rebuild the dispatch ticket without extra state. */
export interface ResumeInfo {
  agent: string;
  model: string;
  /** The run these segments belong to (§2.9). */
  runId?: string;
  tokenBudget?: number;
  providerOptions?: ProviderOptions;
  /** The run's state (§2.10). A park can outlive the process that made it, so
   *  the state has to travel on the ticket — rebuilt from here, not from a
   *  closure that is long gone. Without it every storage call after an
   *  approval loses whatever the caller attached, tenant scope included. */
  state?: AgentRunState;
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
  /** How long a parked HITL request stays answerable (§2.5) — expiry turns
   *  it into the timeout denial ("user had no response") and the run continues. */
  hitlTtlMs: number;
  /** Grace beyond the HITL TTL before orphan reclamation may claim a thread (§2.5) */
  reclaimGraceMs: number;
  /** AI-SDK round-trip ceiling per run (§2.1 safety cap) — the platform loop
   *  runs one executeStep (maxSteps: 1) per iteration up to this count. */
  maxSteps: number;
  /** Queue redrive attempts before a run finalizes FAILED (§2.8) */
  runMaxAttempts: number;
  /** How often a running worker re-reads the stop signal (§2.1). Also the
   *  window in which it notices that a newer run has replaced it. */
  stopPollMs: number;
  /** Delay (seconds) before re-dispatching a job that found the run lock still
   *  held by an OLDER run (§2.8). That job has not executed at all — dropping
   *  it would strand the user's message — so it comes back once the previous
   *  run has let go. */
  runRedriveDelaySeconds: number;
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
  /** Record payloads into the operational store (§2.9): the prompt and state a
   *  run was dispatched with, and the text and tool arguments/results each step
   *  produced. Off means timings and counts only.
   *
   *  On by default because a dashboard that cannot show what happened is worth
   *  little — but this is the flag to turn off when prompts or tool payloads
   *  carry anything that should not sit in an operational database. */
  recordPayloads: boolean;
  /** Characters kept per recorded value, so one large prompt or tool result
   *  cannot bloat the store. */
  payloadCapChars: number;
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
   *  possible run segment; parked HITL waits hold NO lock (§2.8, §3.4). */
  runLockLeaseSeconds: number;
  /** Billing pre-execution check (§4). Return `{ ok: false, error }` to reject a run. */
  billingPreCheck?: (threadId: string) => Promise<{ ok: boolean; error?: string }>;
  /** Provider-specific options applied to EVERY run (§3.1) — a reasoning
   *  budget, a service tier, a safety identifier. The lowest of three levels:
   *  an agent spec overrides this, and a run input overrides both, per
   *  provider namespace. */
  providerOptions?: ProviderOptions;
}

export const DEFAULT_CONFIG: AgentConfig = {
  hitlTtlMs: 15 * 60_000,
  reclaimGraceMs: 60_000,
  maxSteps: 25,
  runMaxAttempts: 3,
  stopPollMs: 500,
  runRedriveDelaySeconds: 2,
  subagentMaxDepth: 2,
  subagentMaxConcurrent: 3,
  subagentMaxSteps: 10,
  subagentResultCapChars: 8_000,
  recordPayloads: true,
  payloadCapChars: 2_000,
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
  if (!Number.isInteger(config.stopPollMs) || config.stopPollMs < 1) {
    // The poll is the only thing that delivers a stop to a running worker (§2.1)
    throw new Error(
      `Invalid config: stopPollMs (${config.stopPollMs}) must be an integer of at least 1`,
    );
  }
  if (!Number.isInteger(config.runRedriveDelaySeconds) || config.runRedriveDelaySeconds < 0) {
    throw new Error(
      `Invalid config: runRedriveDelaySeconds (${config.runRedriveDelaySeconds}) must be a non-negative integer`,
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
