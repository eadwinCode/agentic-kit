import { generateText, streamText } from 'ai';
import type { LanguageModel } from 'ai';
import type { RuntimePorts } from '../ports/runtime.js';
import type { AgentKind, ProviderOptions } from './types.js';
import type { RegisteredAgent } from './agent.js';
import { systemCacheMessage } from './cache.js';
import { attributeTokens, type TokenAttribution } from './usage.js';
import { drainOrThrow } from './stream.js';
import { publishNotice } from './publish.js';
import { HITL_PARKED } from './hitl.js';

/** True for the sentinel a parked `requiresConfirmation` tool returns (§2.5).
 *  It is never a real tool result and is never persisted. */
export const isParked = (result: unknown): boolean =>
  typeof result === 'object' &&
  result !== null &&
  (result as Record<string, unknown>)[HITL_PARKED] !== undefined;

/** One platform-owned step (§2.1, §5.6): a single SDK round-trip with
 *  maxSteps: 1. The SDK executes the step's tool calls and reports a
 *  structured result; whether to continue is the loop's decision, never
 *  the SDK's. */
export interface StepResult {
  text: string;
  finishReason: string;
  usage: Record<string, number> | undefined;
  /** Assistant + tool messages this step produced — appended to the
   *  conversation (in memory AND storage) before the next step. */
  responseMessages: Array<{ role: string; content: unknown }>;
  /** The step's executed tool calls and their results. */
  toolResults: Array<{ toolCallId: string; toolName: string; result: unknown }>;
  /** Provider metadata for this step — the only place a cache hit is reported
   *  (§2.6). Without carrying it, cachedInputTokens can never be anything but
   *  zero. */
  providerMetadata?: Record<string, Record<string, unknown> | undefined>;
}

export async function executeStep(
  agent: RegisteredAgent,
  call: {
    kind: AgentKind;
    model: LanguageModel;
    messages: Array<any>;
    tools: Record<string, any>;
    providerOptions?: ProviderOptions;
    abortSignal: AbortSignal;
    onChunk?: (chunk: unknown) => Promise<void>;
    /** Overrides the user's spec args — a nested run brings its own persona
     *  and must not inherit the parent's (§2.7). */
    system?: string;
    /** Move the system prompt into `messages` as a stamped message so it can
     *  carry a cache breakpoint (§2.6). As the SDK's `system:` string it
     *  reaches the provider with no metadata channel and never caches. */
    cacheSystemPrompt?: boolean;
  },
): Promise<StepResult> {
  // Ownership rule (§3.1): user args spread FIRST, platform keys LAST. The
  // user's stream callbacks are stripped here and re-chained by the platform
  // (onChunk below, onFinish at finalize) so the SDK can never own them.
  const {
    onChunk: _userOnChunk,
    onFinish: _userOnFinish,
    onStepFinish: userOnStepFinish,
    system: specSystem,
    ...userArgs
  } = agent.args as Record<string, any>;

  // A nested run brings its own persona and must not inherit the parent's.
  const system = call.system ?? specSystem;
  const hoistSystem =
    call.cacheSystemPrompt === true && typeof system === 'string' && system.length > 0;

  const shared = {
    ...userArgs,
    model: call.model,
    // Hoisted, the system prompt leads the messages and carries the
    // breakpoint; a fresh array each step leaves the loop's own array alone.
    messages: hoistSystem ? [systemCacheMessage(system), ...call.messages] : call.messages,
    tools: call.tools,
    abortSignal: call.abortSignal,
    maxSteps: 1, // the loop owns continuation
    ...(hoistSystem || system === undefined ? {} : { system }),
    // Provider-specific options (§3.1): forwarded under both the v5-native
    // key and the v4 alias.
    ...(call.providerOptions
      ? {
          providerOptions: call.providerOptions,
          experimental_providerMetadata: call.providerOptions as any,
        }
      : {}),
    ...(call.onChunk
      ? { onChunk: async ({ chunk }: any) => { await call.onChunk!(chunk); } }
      : {}),
    ...(userOnStepFinish
      ? { onStepFinish: (step: any) => userOnStepFinish?.(step) } // user callback still fires
      : {}),
  };

  if (call.kind === 'stream-text') {
    const result = streamText(shared as any);
    // streamText is lazy: drain the full stream so onChunk fires per part, and
    // let a provider failure throw here rather than hanging on promises that
    // never settle (see drainOrThrow).
    await drainOrThrow(result.fullStream);

    const [text, usage, finishReason, response, steps, meta] = await Promise.all([
      result.text,
      result.usage,
      result.finishReason,
      result.response,
      result.steps,
      // v4 exposes it under the experimental name; v5 drops the prefix.
      (result as any).providerMetadata ?? (result as any).experimental_providerMetadata,
    ]);
    return {
      text,
      finishReason,
      usage: usage as any,
      responseMessages: (response?.messages ?? []) as any,
      toolResults: (steps?.at(-1)?.toolResults ?? []) as any,
      providerMetadata: meta as any,
    };
  }

  const result = await generateText(shared as any);
  return {
    text: result.text,
    finishReason: result.finishReason,
    usage: result.usage as any,
    responseMessages: (result.response?.messages ?? []) as any,
    toolResults: (result.steps?.at(-1)?.toolResults ?? []) as any,
    providerMetadata:
      ((result as any).providerMetadata ??
        (result as any).experimental_providerMetadata) as any,
  };
}

/** Keep a recorded value small: one oversized tool result should not be able
 *  to bloat the operational store (§2.9). Structured values are truncated by
 *  their serialised form so the shape stays readable. */
const cap = (text: string | undefined, limit: number): string | null =>
  !text ? null : text.length > limit ? `${text.slice(0, limit)}…` : text;

function capValue(value: unknown, limit: number): unknown {
  if (value === undefined) return null;
  if (typeof value === 'string') return cap(value, limit);
  const json = JSON.stringify(value) ?? 'null';
  return json.length <= limit ? value : `${json.slice(0, limit)}…`;
}

/** Tokens a run has spent, main agent and nested runs together (§2.7). Shared
 *  by reference so a child's spend counts against the run's safety cap the
 *  moment it happens — a budget that ignores delegated work is not a budget. */
export interface RunLedger {
  tokensUsed: number;
}

export interface LoopInput {
  /** Whose stream this loop persists to (§2.7). `null` is the main agent. */
  agentId: string | null;
  /** The run these steps belong to (§2.9) — a thread has many runs, so a step
   *  marker without it cannot be attributed to one. */
  runId?: string;
  kind: AgentKind;
  model: LanguageModel;
  /** Seeded context: compacted history for the main agent, the brief plus its
   *  own persisted turns for a nested run. Mutated as steps are appended. */
  messages: Array<any>;
  tools: Record<string, any>;
  maxSteps: number;
  abortSignal: AbortSignal;
  providerOptions?: ProviderOptions;
  /** Cumulative cap for the whole run, checked against the shared ledger. */
  tokenBudget?: number;
  onChunk?: (chunk: unknown) => Promise<void>;
  /** Persona for a nested run; omitted, the agent's own spec `system` stands. */
  system?: string;
  /** Carry the system prompt as a stamped message rather than the SDK's
   *  `system:` string, so it can hold a cache breakpoint (§2.6). */
  cacheSystemPrompt?: boolean;
}

export interface LoopOutcome {
  text: string;
  finishReason: string;
  /** What THIS loop spent — the caller records it against its own agentId. */
  attribution: TokenAttribution;
  tokensUsed: number;
  /** A requiresConfirmation tool parked: the segment ends here (§2.5). */
  parked: boolean;
  /** The parked call, when `parked`. */
  parkedToolCallId?: string;
  /** The abort signal fired mid-loop — a user stop (§2.1). */
  aborted: boolean;
  /** Iterations this loop completed (§2.9). */
  steps: number;
}

/** The platform-owned loop (§2.1, §5.6), run by the main agent and by every
 *  nested run alike (§2.7).
 *
 *  One single-round-trip step per iteration. After EVERY step the produced
 *  messages are persisted under this loop's `agentId`, so a worker that dies
 *  mid-run resumes from the last step — and so a parked nested run can be
 *  re-entered later instead of restarted. Every continuation decision — tool
 *  results ready, budget spent, step ceiling, HITL park, user stop — is made
 *  here between steps, never inside the SDK. */
export async function runLoop(
  deps: RuntimePorts,
  agent: RegisteredAgent,
  threadId: string,
  input: LoopInput,
  ledger: RunLedger,
): Promise<LoopOutcome> {
  const attribution: TokenAttribution = {
    inputTokens: 0,
    cachedInputTokens: 0,
    outputTokens: 0,
    totalTokens: 0,
  };
  let tokensUsed = 0;
  let lastText = '';
  let lastFinishReason = '';
  let parked = false;
  let parkedToolCallId: string | undefined;
  let stepsLeft = input.maxSteps;
  let stepsRun = 0;

  while (stepsLeft > 0 && !input.abortSignal.aborted) {
    stepsLeft--;
    const stepStartedAt = Date.now();
    let step: StepResult;
    try {
      step = await executeStep(agent, {
        kind: input.kind,
        model: input.model,
        messages: input.messages,
        tools: input.tools,
        providerOptions: input.providerOptions,
        abortSignal: input.abortSignal,
        onChunk: input.kind === 'stream-text' ? input.onChunk : undefined,
        system: input.system,
        cacheSystemPrompt: input.cacheSystemPrompt,
      });
    } catch (err) {
      if (input.abortSignal.aborted) break; // user stop mid-step
      throw err; // real failure → §2.8 redrive policy
    }

    // Per-step durability (§5.6): append this step's turns BEFORE the next
    // step. A parked HITL tool result (the sentinel) is NOT a real result —
    // it is skipped here; the resumed segment appends the user's verdict.
    const persisted = step.responseMessages.filter((m) => {
      if (m.role !== 'tool') return true;
      const parts = Array.isArray(m.content) ? m.content : [];
      return !parts.some((p: any) => isParked(p?.result));
    });
    for (const m of persisted) {
      await deps.storage.messages.append(threadId, {
        role: m.role as any,
        content: m.content,
        agentId: input.agentId,
      });
    }
    input.messages.push(...step.responseMessages);

    // Token attribution (§4), accumulated across the segment's steps — and
    // into the run-wide ledger the safety cap is checked against (§2.7).
    const a = attributeTokens(step.usage as any, step.providerMetadata);
    attribution.inputTokens += a.inputTokens;
    attribution.cachedInputTokens += a.cachedInputTokens;
    attribution.outputTokens += a.outputTokens;
    attribution.totalTokens += a.totalTokens;
    tokensUsed += a.totalTokens;
    ledger.tokensUsed += a.totalTokens;
    lastText = step.text ?? '';
    lastFinishReason = step.finishReason;
    stepsRun += 1;

    // §2.9: one row per step in the platform's OWN store, plus a bus-only
    // notice so live dashboards see it. The notice is not persisted to the
    // caller's event log — operational history is not their data to carry.
    const marker = {
      runId: input.runId ?? '',
      threadId,
      agentId: input.agentId,
      index: stepsRun,
      durationMs: Date.now() - stepStartedAt,
      finishReason: step.finishReason,
      inputTokens: a.inputTokens,
      cachedInputTokens: a.cachedInputTokens,
      outputTokens: a.outputTokens,
      totalTokens: a.totalTokens,
      tools: (step.toolResults ?? [])
        .map((r: any) => r?.toolName)
        .filter(Boolean) as string[],
      ...(deps.config.recordPayloads
        ? {
            text: cap(step.text, deps.config.payloadCapChars),
            toolCalls: (step.toolResults ?? []).map((r: any) => ({
              toolName: r?.toolName,
              args: capValue(r?.args, deps.config.payloadCapChars),
              result: capValue(r?.result, deps.config.payloadCapChars),
            })),
          }
        : {}),
    };
    if (input.runId) await deps.admin.steps.record(marker);
    await publishNotice(deps, threadId, 'STEP_FINISHED', marker);

    // §2.5 park: a requiresConfirmation tool returned the sentinel — the
    // segment ends here on WAITING_FOR_INPUT (set by parkForApproval).
    const parkedResult = (step.toolResults ?? []).find((r: any) => isParked(r?.result));
    if (parkedResult) {
      parked = true;
      parkedToolCallId = (parkedResult.result as any)[HITL_PARKED];
      break;
    }

    // Budget check BETWEEN steps (§2.1) — the step that crossed the line is
    // kept in full; the next one never starts.
    if (input.tokenBudget && ledger.tokensUsed >= input.tokenBudget) break;

    // 'tool-calls' → the SDK executed the step's tools; the loop feeds the
    // results back. Anything else ('stop', 'length', …) ends the run.
    if (step.finishReason !== 'tool-calls') break;
  }

  return {
    text: lastText,
    finishReason: lastFinishReason,
    attribution,
    tokensUsed,
    parked,
    parkedToolCallId,
    aborted: input.abortSignal.aborted,
    steps: stepsRun,
  };
}
