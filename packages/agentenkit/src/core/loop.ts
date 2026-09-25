import { generateText, streamText } from 'ai';
import type { LanguageModel } from 'ai';
import type { RuntimePorts } from '../ports/runtime.js';
import type { AgentKind, ProviderOptions } from './types.js';
import type { RegisteredAgent } from './agent.js';
import { systemCacheMessage } from './cache.js';
import {
  fillTokens,
  providerMeta,
  type RunLedger,
  type TokenAttribution,
} from './usage.js';
import { estimateTokens } from './context.js';
import type { NewUsage } from './types.js';
import { ChunkBatcher, chunkPayload, drainOrThrow } from './stream.js';
import { publish, publishNotice } from './publish.js';
import { HITL_PARKED } from './hitl.js';
import { RunLockLostError } from './lease.js';

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
  /** The response id and headers, where the SDK reported them. A gateway
   *  bills through a header, so a receipt pricer reads it from there (§4). */
  response?: { id?: string; headers?: Record<string, string> };
  /** What actually reached the client before the call ended. Set even when
   *  the call failed or was stopped mid-stream, so the tokens of a cut-off
   *  call can still be estimated and billed. */
  streamedText?: string;
  /** False when no finish ever arrived: the stream was cut off by a stop or a
   *  provider failure, and the counters are not the provider's own. */
  finished: boolean;
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
    /** Filled as the stream runs, so a call cut off half way still knows how
     *  much it produced and can be billed for it (§4). */
    partial?: { text: string };
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
    onChunk: async ({ chunk }: any) => {
      // Accumulated first, so the text is already banked if the call is
      // stopped before its finish arrives (§4).
      if (call.partial && chunk?.type === 'text-delta') {
        call.partial.text += chunk.textDelta ?? chunk.text ?? '';
      }
      await call.onChunk?.(chunk);
    },
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
      response: { id: (response as any)?.id, headers: (response as any)?.headers },
      streamedText: call.partial?.text || text,
      // The SDK makes up a finish when the provider's stream closes without
      // one, with reason 'unknown'. A step like that did not finish: the
      // provider cut it short without saying so.
      finished: finishReason !== 'unknown',
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
    response: { id: (result.response as any)?.id, headers: (result.response as any)?.headers },
    streamedText: result.text,
    finished: true,
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

export { RunLedger, seedRunLedger } from './usage.js';

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
  /** True once the run lock is gone (§3.4): another worker may own the
   *  thread, so this loop must not write another step to it. */
  fenced?: () => boolean;
  /** Writes the parks raised during a step, once that step is saved (see
   *  ParkBox). Only the main loop sets it: a nested run's parks wait for the
   *  main agent's step. */
  commitParks?: () => Promise<void>;
  providerOptions?: ProviderOptions;
  /** Cumulative cap for the whole run, checked against the shared ledger. */
  tokenBudget?: number;
  /** Money cap for the whole run (§4), checked between the same steps. */
  costBudgetMicros?: number;
  /** The DISPATCHED run every call here is billed to (§4). A nested loop's
   *  `runId` is its own; this stays the parent's, so one run's whole bill,
   *  delegated work included, is a single query. */
  billingRunId?: string;
  /** The registry key the model was resolved from — what a price list is
   *  usually keyed by. */
  modelKey?: string;
  /** The wire id that key resolved to (`wireId(resolved, key)`), for a price
   *  list keyed by wire ids instead. */
  modelId?: string;
  /** The name that goes on the bill line: the registered handle for the main
   *  run, the delegation's name for a nested one. */
  agentName?: string;
  /** Every raw chunk, as the SDK streams it: the host's own callback. */
  onChunk?: (chunk: unknown) => Promise<void>;
  /** Publishes a chunk as an event. Deltas reach it merged (see ChunkBatcher),
   *  and everything a step streamed is out before the step is committed. */
  publishChunk?: (chunk: unknown) => Promise<void>;
  /** Calls whose tool failed (see withHitl), for their tool-result chunk. */
  toolErrors?: Map<string, string>;
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
  /** The last model call ended without a finish and without a user stop,
   *  and no error said why. The run must not be taken as finished. */
  interrupted: boolean;
  /** Iterations this loop completed (§2.9). */
  steps: number;
  /** The run hit its money cap and stopped between steps (§4). */
  costExhausted: boolean;
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
  let costExhausted = false;
  let interrupted = false;
  // The input count of the last finished call. A call cut off before its
  // finish never reports one, and its prompt was the same size as the
  // previous step's plus a little, so this is the honest floor to bill.
  let lastInput = 0;

  while (stepsLeft > 0 && !input.abortSignal.aborted) {
    stepsLeft--;
    const stepStartedAt = Date.now();
    const partial = { text: '' };
    let step: StepResult;
    const batcher = input.publishChunk ? new ChunkBatcher(input.publishChunk) : null;
    try {
      step = await executeStep(agent, {
        kind: input.kind,
        model: input.model,
        messages: input.messages,
        tools: input.tools,
        providerOptions: input.providerOptions,
        abortSignal: input.abortSignal,
        onChunk:
          input.kind === 'stream-text'
            ? async (chunk: unknown) => {
                const published = chunkPayload(chunk, input.toolErrors);
                if (published !== null) await batcher?.push(published);
                await input.onChunk?.(chunk);
              }
            : undefined,
        system: input.system,
        cacheSystemPrompt: input.cacheSystemPrompt,
        partial,
      });
      await batcher?.flush(); // the step's text is out before anything below
    } catch (err) {
      await batcher?.flush();
      // The call ended without a finish: a user stop, or the provider failing
      // part way. Either way the provider billed for what it had already
      // produced, so the call is recorded rather than dropped — with
      // estimated counters where it never reported real ones (§4).
      // Nothing finished on this run yet, so estimate the prompt that was
      // certainly sent rather than billing the call as free.
      const cut = unfinishedUsage(
        input,
        stepsRun + 1,
        partial.text,
        input.abortSignal.aborted ? 'aborted' : 'error',
        lastInput || estimateTokens(input.messages),
      );
      if (cut.totalTokens > 0) {
        const priced = await ledger.record(deps, threadId, cut);
        addAttribution(attribution, priced);
        tokensUsed += priced.totalTokens;
      }
      if (input.abortSignal.aborted) break; // user stop mid-step
      throw err; // real failure → §2.8 redrive policy
    }
    if (!step.finished) {
      // The stream ended with no finish and no error. Billed like any call cut
      // short, then handed back unfinished: the caller retries it rather than
      // taking the partial step as the run's end.
      const cut = unfinishedUsage(
        input,
        stepsRun + 1,
        partial.text,
        input.abortSignal.aborted ? 'aborted' : 'error',
        lastInput || estimateTokens(input.messages),
      );
      if (cut.totalTokens > 0) {
        const priced = await ledger.record(deps, threadId, cut);
        addAttribution(attribution, priced);
        tokensUsed += priced.totalTokens;
      }
      if (!input.abortSignal.aborted) interrupted = true;
      break;
    }

    // One priced usage row per model call (§4), recorded below once the step
    // is committed.
    const stepUsage: NewUsage = {
      runId: input.billingRunId,
      agentId: input.agentId,
      agentName: input.agentName,
      kind: 'step',
      step: stepsRun + 1,
      model: input.modelKey,
      modelId: input.modelId,
      outcome: 'finished',
      providerMetadata: providerMeta(step.providerMetadata, step.response),
      ...fillTokens(step.usage as any, step.providerMetadata),
    };

    // The step is done, and its messages are the first thing it writes. A
    // worker that lost the lock meanwhile must not write them: the next
    // holder may already be writing its own. The call itself did happen and
    // the provider billed it, so its usage is still recorded.
    if (input.fenced?.()) {
      await ledger.record(deps, threadId, stepUsage);
      throw new RunLockLostError();
    }

    // Per-step durability (§5.6): append this step's turns BEFORE the next
    // step. A parked HITL tool result (the sentinel) is NOT a real result —
    // it is skipped here; the resumed segment appends the user's verdict. Only
    // the parked parts go: another tool in the same step that already ran
    // keeps its result, or the model would be told it never did.
    const persisted = step.responseMessages.flatMap((m) => {
      if (m.role !== 'tool') return [m];
      const parts = Array.isArray(m.content) ? m.content : [];
      const kept = parts.filter((p: any) => !isParked(p?.result));
      if (kept.length === 0) return [];
      return [kept.length === parts.length ? m : { ...m, content: kept }];
    });
    for (const m of persisted) {
      await deps.storage.messages.append(threadId, {
        role: m.role as any,
        content: m.content,
        agentId: input.agentId,
      });
    }
    input.messages.push(...step.responseMessages);

    // One priced usage row per model call (§4), booked on the run-wide ledger
    // the caps are checked against (§2.7). Written as soon as the step's
    // messages are, before anything else: a crash after this point resumes
    // from the saved step and never runs the call again, so its row must
    // already be there.
    const priced = await ledger.record(deps, threadId, stepUsage);

    // A replay boundary (§2.2). Everything this step produced is now durable
    // history, so a reconnecting client must NOT also replay its chunks — it
    // would render the same text twice, once from the message and once from
    // the stream that produced it. Persisted (not a bus notice) because the
    // snapshot needs its seq to know where durable ends and live begins.
    await publish(deps, threadId, 'STEP_COMMITTED', {
      index: stepsRun,
      agentId: input.agentId,
    });
    // The step is durable now, tool calls included, so the parks it raised
    // can be written: WAITING_FOR_INPUT and the approval requests.
    await input.commitParks?.();

    lastInput = priced.inputTokens;
    addAttribution(attribution, priced);
    tokensUsed += priced.totalTokens;
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
      inputTokens: priced.inputTokens,
      cachedInputTokens: priced.cacheReadInputTokens,
      outputTokens: priced.outputTokens,
      totalTokens: priced.totalTokens,
      tools: (step.toolResults ?? [])
        .map((r: any) => r?.toolName)
        .filter(Boolean) as string[],
      at: new Date(),
      ...(deps.config.recordPayloads
        ? {
            // Left out when empty, as the Go runtime does.
            ...(step.text ? { text: cap(step.text, deps.config.payloadCapChars) } : {}),
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
    // kept in full; the next one never starts. Published before the break, so
    // a client learns why the run ended the moment it does.
    if (input.tokenBudget && ledger.tokensUsed >= input.tokenBudget) {
      await publish(deps, threadId, 'TOKEN_BUDGET_EXHAUSTED', {
        agentId: input.agentId,
        tokensUsed: ledger.tokensUsed,
        tokenBudget: input.tokenBudget,
      });
      break;
    }

    // The money cap (§4), checked in the same place and the same way, against
    // the same shared ledger: a nested run's calls count against it, and a
    // run that parked or retried starts from what it already spent (see
    // seedRunLedger).
    //
    // It only ever sees priced calls: with no pricer configured nothing is
    // ever spent and the cap never fires.
    if (input.costBudgetMicros && ledger.costMicros >= input.costBudgetMicros) {
      await publish(deps, threadId, 'COST_BUDGET_EXHAUSTED', {
        agentId: input.agentId,
        costMicros: ledger.costMicros,
        costBudgetMicros: input.costBudgetMicros,
        ...(ledger.currency ? { currency: ledger.currency } : {}),
      });
      costExhausted = true;
      break;
    }

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
    interrupted,
    steps: stepsRun,
    costExhausted,
  };
}

/** Sum one recorded call into a segment's running attribution (§4). */
function addAttribution(into: TokenAttribution, u: NewUsage): void {
  into.inputTokens += u.inputTokens;
  into.cachedInputTokens += u.cacheReadInputTokens;
  into.outputTokens += u.outputTokens;
  into.totalTokens += u.totalTokens;
}

/** The row for a call that never reported a finish: a user stop, or the
 *  provider failing mid-stream (§4).
 *
 *  The provider still billed for it, so the counters are filled in from what
 *  IS known: the previous call's input count for the prompt that was certainly
 *  sent, and the same estimator compaction measures context fill with, over
 *  the text that actually streamed — so context fill and the cost of a cut-off
 *  step follow one rule. `estimated` marks the row, so a bill built from these
 *  rows can say which lines are guesses. */
function unfinishedUsage(
  input: LoopInput,
  step: number,
  streamedText: string,
  outcome: 'aborted' | 'error',
  lastInput: number,
): NewUsage {
  const outputTokens = streamedText ? estimateTokens(streamedText) : 0;
  return {
    runId: input.billingRunId,
    agentId: input.agentId,
    agentName: input.agentName,
    kind: 'step',
    step,
    model: input.modelKey,
    modelId: input.modelId,
    inputTokens: lastInput,
    cacheReadInputTokens: 0,
    cacheWriteInputTokens: 0,
    outputTokens,
    reasoningTokens: 0,
    totalTokens: lastInput + outputTokens,
    outcome,
    estimated: true,
  };
}
