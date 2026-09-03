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
  /** The provider options the run was dispatched with (§3.1), merged across
   *  config, spec and input. Present only when `recordPayloads` is on. */
  providerOptions?: Record<string, unknown> | null;
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
  providerOptions?: Record<string, unknown> | null;
  /** Defaults to 0 — a dispatched run. */
  depth?: number;
  parentRunId?: string | null;
}

export type RunPatch = Partial<Omit<RunRecord, 'id' | 'threadId'>>;

/** Cumulative token AND money attribution across every run on a thread (§4).
 *  Tokens are what the provider reported; the money is what the Pricer put on
 *  each row as it was stored. */
export interface UsageTotals {
  /** Fresh (uncached) prompt tokens. */
  inputTokens: number;
  /** Prompt tokens served from the provider's prompt cache (§2.6). */
  cachedInputTokens: number;
  outputTokens: number;
  /** input + cached + output. */
  totalTokens: number;
  /** Summed cost in millionths of one `currency` unit: 1_000_000 is one
   *  dollar when the currency is USD. */
  costMicros: number;
  /** The unit `costMicros` is in, absent when nothing was priced. One
   *  deployment should price in ONE currency: these are summed, not
   *  converted. */
  currency?: string;
  /** How many calls had no cost, because no pricer answered for them. Above
   *  zero, `costMicros` is a floor and not the whole bill. */
  unpriced: number;
  /** The same spend grouped by agent and model: one line per pair, which is
   *  the shape a bill wants. Summing the lines gives the totals above. */
  lines: UsageLine[];
}

/** One agent's spend on one model, summed over its calls (§4). This is the
 *  bill line a credit system charges for. */
export interface UsageLine {
  /** null for the main run, the nested run's id otherwise. */
  agentId?: string | null;
  agentName?: string | null;
  /** The registry key; `modelId` is the wire id it resolved to. */
  model?: string | null;
  modelId?: string | null;
  inputTokens: number;
  cacheReadInputTokens: number;
  cacheWriteInputTokens: number;
  outputTokens: number;
  reasoningTokens: number;
  /** How many model calls this line covers. */
  calls: number;
  /** How many of those had estimated tokens, because they were cut off
   *  before the provider reported real ones. */
  estimated: number;
  costMicros: number;
}

/** Narrows a usage read (§4). */
export interface UsageFilter {
  /** Limit the read to one dispatched run, nested runs included. Omitted
   *  reads the whole thread. */
  runId?: string;
}

/** How the model call that produced a usage row ended. */
export type UsageOutcome =
  /** Ran to its finish; the provider reported the counters itself. */
  | 'finished'
  /** A user stop cut the call mid-stream, so no finish arrived and the
   *  tokens are estimated. */
  | 'aborted'
  /** The provider failed mid-call. What it had already streamed was still
   *  billed, so the row is kept. */
  | 'error';

/** Which part of the platform made the call. */
export type UsageKind =
  /** A step of an agent loop, main run or nested (§2.1, §2.7). */
  | 'step'
  /** The summary the platform writes to keep the prompt inside the model's
   *  window (§2.6). Nobody asked for it, so it is worth being able to see
   *  what it costs on its own. */
  | 'compaction';

/** The money one model call cost. */
export interface Cost {
  /** Millionths of one `currency` unit: 1_000_000 is one dollar when the
   *  currency is USD. Integers, because money summed as a float drifts. */
  micros: number;
  /** ISO-4217 code — 'USD' for the shipped table pricer. */
  currency: string;
  /** Where the number came from: 'receipt' when the provider computed it,
   *  'table' from a price list, 'estimate' for anything a pricer worked out
   *  itself. It rides along so a bill can be audited. */
  source: 'receipt' | 'table' | 'estimate';
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

/** ONE model call, recorded by the engine after every call it makes: a step
 *  of the main run, a step of a nested run, a compaction pass, streamed or
 *  not, finished or cut short.
 *
 *  It carries everything a Pricer needs to put a price on the call, so pricing
 *  never has to reach back into the run to find out what happened. */
export interface NewUsage {
  /** The DISPATCHED run this call belongs to (§2.9). A nested run's calls
   *  carry their parent's run id, so one run's bill is one query. */
  runId?: string | null;
  /** Whose stream made the call (§2.7): null is the main run, otherwise the
   *  nested run's id. */
  agentId?: string | null;
  /** The registered handle for the main run, the delegation's name for a
   *  nested one. What a bill line should say. */
  agentName?: string | null;
  kind: UsageKind;
  /** 1-based iteration inside its loop; 0 for a compaction. */
  step: number;
  /** The registry key the call was made with, e.g. 'claude-sonnet-4@high' —
   *  what a price list is usually keyed by. */
  model?: string | null;
  /** The wire id the provider reported back, which can differ from the key
   *  that asked for it (an alias, a dated snapshot). */
  modelId?: string | null;
  /** Fresh (uncached) prompt tokens. */
  inputTokens: number;
  /** Prompt tokens served from the provider's prompt cache (§2.6 caching). */
  cacheReadInputTokens: number;
  /** Prompt tokens WRITTEN into that cache — a separate line on the
   *  provider's bill, and not part of `totalTokens`. */
  cacheWriteInputTokens: number;
  outputTokens: number;
  /** Thinking tokens. Most providers already count these inside
   *  `outputTokens`, so price them at zero unless yours bills them
   *  separately (see the pricing module). */
  reasoningTokens: number;
  /** input + cache reads + output: the counter the token budget is measured
   *  against. Cache writes and reasoning are deliberately outside it — cache
   *  writes are their own bill line, and reasoning tokens are usually already
   *  inside the output count. */
  totalTokens: number;
  outcome: UsageOutcome;
  /** True when no finish arrived and the counters are the platform's own
   *  estimate over what it does know: the prompt it sent and the text that did
   *  stream, measured the same way compaction measures context fill. */
  estimated?: boolean;
  /** Whatever the provider attached to the finish: a gateway receipt, a
   *  generation id, anything a Pricer wants to read. Two reserved keys are
   *  added by the platform: `responseId` and `responseHeaders`, because a
   *  gateway bills through a header. */
  providerMetadata?: Record<string, unknown> | null;
  /** Filled by the Pricer before the row is stored. Absent means the call
   *  went unpriced, and any total over it is a floor. */
  cost?: Cost | null;
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
  /** The money cap for this run (§4), carried so the worker enforces the same
   *  cap the caller asked for. */
  costBudgetMicros?: number;
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
  costBudgetMicros?: number;
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
  /** The wire id this key resolves to, e.g. 'gpt-4o-2024-11-20' for the key
   *  'gpt-4o'. It goes onto every usage row, so a price list keyed by wire
   *  ids can match one (§4). Omitted means the key IS the id. */
  modelId?: string;
}

/** The model id to record for a registry key: what `resolveModel` declared,
 *  or the key itself when it declared nothing. */
export const wireId = (model: ResolvedModel, key: string): string =>
  model.modelId && model.modelId.length > 0 ? model.modelId : key;

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
  /** Default per-run money cap (§4), in millionths of the pricer's currency:
   *  250_000 is roughly a quarter of a dollar when the pricer works in USD.
   *  `undefined` = unbounded. Checked between steps, exactly like
   *  `tokenBudget`, so the step that crossed the line is always kept in full.
   *  Needs a pricer: an unpriced call spends no money and so can never
   *  exhaust it. */
  costBudgetMicros?: number;
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
  /** Registry key of the cheap model that writes the context summary (§2.6).
   *  It is resolved through your own `resolveModel`, so it must be a key that
   *  resolver knows — a registry with no 'gpt-4o-mini' in it has to name its
   *  own. The call is billed like any other, under `kind: 'compaction'` (§4).
   *  Default: 'gpt-4o-mini'. */
  compactionModel: string;
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
  /** Billing pre-execution check (§4). Return `{ ok: false, error }` to reject
   *  a run. The check can publish on the thread (a credit warning, a reset
   *  date) so every client sees why; the platform also publishes RUN_REFUSED
   *  with the error. */
  billingPreCheck?: (check: BillingCheck) => Promise<{ ok: boolean; error?: string }>;
  /** Provider-specific options applied to EVERY run (§3.1) — a reasoning
   *  budget, a service tier, a safety identifier. The lowest of three levels:
   *  an agent spec overrides this, and a run input overrides both, per
   *  provider namespace. */
  providerOptions?: ProviderOptions;
}

/** What `billingPreCheck` receives: the thread about to run, the run's state
 *  (§2.10), and a way to publish on the thread before the refusal reaches the
 *  caller. `publishEvent(type, payload, { durable })` — durable by default. */
export interface BillingCheck {
  threadId: string;
  state: AgentRunState;
  publishEvent: (
    type: string,
    payload: unknown,
    options?: { durable?: boolean },
  ) => Promise<AgentEvent>;
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
  compactionModel: 'gpt-4o-mini',
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
  if (!config.compactionModel) {
    // Compaction has no fallback: without a model to summarize with, a thread
    // that outgrows its window cannot run at all (§2.6).
    throw new Error('Invalid config: compactionModel must be a non-empty registry key');
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
