import { generateText, streamText } from 'ai';
import type { LanguageModel } from 'ai';
import { randomUUID } from 'node:crypto';
import type { RuntimePorts } from '../ports/runtime.js';
import type { AgentKind, ExecutionState, ProviderOptions, ResumeInfo } from './types.js';
import { compactContext } from './context.js';
import { attributeTokens, countTokens, type TokenAttribution } from './usage.js';
import { markPromptCaching } from './cache.js';
import { mergeProviderOptions } from './types.js';
import type { RegisteredAgent } from './agent.js';
import {
  HITL_PARKED,
  hitlKey,
  loadPendingHitl,
  parkForApproval,
  type PendingHitl,
} from './hitl.js';
import { publish } from './publish.js';
import { spawnSubagentTool } from './subagent.js';
import { redriveKey, runIdKey } from './keys.js';

export { countTokens } from './usage.js';

const runLockKey = (threadId: string) => `agent:lock:${threadId}`;

/** The safety cap (§2.1) must be either absent (unbounded apart from
 *  maxSteps) or a positive number — `0`/negative/NaN would silently disable
 *  the cap and let a run spend without bound. */
export function validateTokenBudget(value: number | undefined, label: string): void {
  if (value !== undefined && (!Number.isFinite(value) || value <= 0)) {
    throw new Error(`${label} must be a positive finite number`);
  }
}

/** Tools the engine treats as destructive: parked behind parkForApproval
 *  (§2.5) instead of executing directly. Purely a marker — see withHitl. */
export function markRequiresConfirmation<T extends object>(t: T): T {
  return Object.assign(t, { requiresConfirmation: true });
}

const isParked = (result: unknown): boolean =>
  typeof result === 'object' &&
  result !== null &&
  (result as Record<string, unknown>)[HITL_PARKED] !== undefined;

/** Wrap every marked tool so a call parks (§2.5) instead of executing: the
 *  request is persisted as INPUT_REQUIRED and the wrapper returns the park
 *  sentinel — nothing blocks. The real tool runs in the RESUMED segment on
 *  approval (see resumePendingHitl). */
function withHitl(
  deps: RuntimePorts,
  threadId: string,
  tools: Record<string, any>,
  resume: ResumeInfo,
) {
  const out: Record<string, any> = {};
  for (const [name, t] of Object.entries(tools)) {
    if (!(t as any)?.requiresConfirmation) {
      out[name] = t;
      continue;
    }
    out[name] = {
      ...t,
      execute: async (args: unknown, opts: { toolCallId?: string }) => {
        const toolCallId = opts?.toolCallId ?? randomUUID();
        await parkForApproval(deps, {
          threadId,
          toolCallId,
          toolName: name,
          args,
          resume,
        });
        return { [HITL_PARKED]: toolCallId };
      },
    };
  }
  return out;
}

/** One platform-owned step (§2.1, §5.6): a single SDK round-trip with
 *  maxSteps: 1. The SDK executes the step's tool calls and reports a
 *  structured result; whether to continue is the engine loop's decision,
 *  never the SDK's. */
export interface StepResult {
  text: string;
  finishReason: string;
  usage: Record<string, number> | undefined;
  /** Assistant + tool messages this step produced — appended to the
   *  conversation (in memory AND storage) before the next step. */
  responseMessages: Array<{ role: string; content: unknown }>;
  /** The step's executed tool calls and their results. */
  toolResults: Array<{ toolCallId: string; toolName: string; result: unknown }>;
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
  },
): Promise<StepResult> {
  // Ownership rule (§3.1): user args spread FIRST, platform keys LAST. The
  // user's stream callbacks are stripped here and re-chained by the platform
  // (onChunk below, onFinish at finalize) so the SDK can never own them.
  const {
    onChunk: _userOnChunk,
    onFinish: _userOnFinish,
    onStepFinish: userOnStepFinish,
    ...userArgs
  } = agent.args as Record<string, any>;

  const shared = {
    ...userArgs,
    model: call.model,
    messages: call.messages,
    tools: call.tools,
    abortSignal: call.abortSignal,
    maxSteps: 1, // the loop owns continuation
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
    // streamText is lazy: drain the full stream so onChunk fires per part.
    //
    // A provider failure — an aborted call included — arrives as an `error`
    // part and the stream then ends NORMALLY, while result.text/usage/... never
    // settle. Awaiting them without rethrowing here hangs the worker forever,
    // holding the thread's run lock, so the error is carried out of the drain
    // and thrown: the engine loop turns it into a stop or a redrive (§2.8).
    let streamError: unknown;
    for await (const part of result.fullStream) {
      if ((part as any)?.type === 'error' && streamError === undefined) {
        streamError = (part as any).error;
      }
    }
    if (streamError !== undefined) throw streamError;

    const [text, usage, finishReason, response, steps] = await Promise.all([
      result.text,
      result.usage,
      result.finishReason,
      result.response,
      result.steps,
    ]);
    return {
      text,
      finishReason,
      usage: usage as any,
      responseMessages: (response?.messages ?? []) as any,
      toolResults: (steps?.at(-1)?.toolResults ?? []) as any,
    };
  }

  const result = await generateText(shared as any);
  return {
    text: result.text,
    finishReason: result.finishReason,
    usage: result.usage as any,
    responseMessages: (result.response?.messages ?? []) as any,
    toolResults: (result.steps?.at(-1)?.toolResults ?? []) as any,
  };
}

/** Resolve a parked HITL request at segment start (§2.5): consume the answer
 *  from the handoff key — or convert an expired request into the timeout
 *  denial ("user had no response", §2.5) — append the tool result, and flip
 *  the thread back to RUNNING. Returns false when the request is still within
 *  its TTL and unanswered: the dispatch is an at-least-once redelivery and
 *  the thread stays parked. `tools` must be the UNWRAPPED toolset. */
async function resumePendingHitl(
  deps: RuntimePorts,
  threadId: string,
  pending: PendingHitl,
  tools: Record<string, any>,
  signal: AbortSignal,
): Promise<boolean> {
  const raw = await deps.kv.get(hitlKey(pending.toolCallId));
  const expired = Date.now() - pending.requestedAt >= deps.config.hitlTtlMs;
  if (!raw && !expired) return false; // still parked — redelivery no-op (§2.8)

  await deps.kv.del(hitlKey(pending.toolCallId));

  let result: unknown;
  if (raw) {
    const answer = JSON.parse(raw) as HitlAnswer;
    if (answer.approved) {
      // The verdict arrives from a different process than the one that ran
      // the model — the tool failure is surfaced TO THE MODEL as the tool
      // result, so the conversation always stays executable (§2.5)
      const target = tools[pending.toolName];
      try {
        result = target
          ? await target.execute(pending.arguments, {
              toolCallId: pending.toolCallId,
              abortSignal: signal,
            })
          : { error: `Unknown tool: ${pending.toolName}` };
      } catch (err) {
        result = { error: err instanceof Error ? err.message : String(err) };
      }
    } else {
      result = { denied: true };
    }
  } else {
    result = { responded: false, cancelled: true, reason: 'timeout' };
  }

  await deps.storage.messages.append(threadId, {
    role: 'tool',
    content: [
      { type: 'tool-result', toolCallId: pending.toolCallId, toolName: pending.toolName, result },
    ],
  });

  await deps.kv.set(`agent:state:${threadId}`, 'RUNNING');
  await deps.storage.threads.setState(threadId, 'RUNNING');
  await publish(deps, threadId, 'STATE_CHANGE', { state: 'RUNNING' });
  if (!raw) await publish(deps, threadId, 'INPUT_EXPIRED', { toolCallId: pending.toolCallId });
  return true;
}

interface HitlAnswer {
  approved: boolean;
  payload?: unknown;
}

/** The engine (§2.1, §5.6). Worker-side only — runs are dispatched via the
 *  queue (§2.8) and may outlive any HTTP response.
 *
 *  Execution is a platform-owned loop of single-round-trip steps
 *  (`executeStep`, maxSteps: 1): after EVERY step the produced messages are
 *  persisted, so a worker that dies mid-run resumes from the last step, and
 *  every continuation decision — tool results ready, budget spent, step
 *  ceiling, HITL park, user stop — is made here between steps, never inside
 *  the SDK.
 *
 *  Concurrency: acquires the per-thread run lock (`agent:lock:{threadId}`,
 *  SET NX + lease) before any work — two workers can never run one thread,
 *  and a crashed worker's lock expires instead of blocking forever (§3.4).
 *  Returns 'lock-conflict' when the lock is held, so callers can tell a
 *  genuine no-op apart from a completed run. */
export interface ExecuteInput {
  threadId: string;
  model: string;
  /** This dispatch's run id (§2.1). A job without one keeps the old
   *  behavior: no staleness check, and no redrive on a lock conflict. */
  runId?: string;
  tokenBudget?: number;
  providerOptions?: ProviderOptions;
}

/** 'executed'      — this worker ran the segment (or it was a legitimate no-op).
 *  'lock-conflict' — someone else holds the thread's run lock; nothing ran.
 *  'stale'         — a NEWER run owns the thread; this job must do nothing. */
export type ExecuteOutcome = 'executed' | 'lock-conflict' | 'stale';

export async function execute(
  deps: RuntimePorts,
  agent: RegisteredAgent,
  input: ExecuteInput,
): Promise<ExecuteOutcome> {
  const { threadId, runId } = input;
  const abort = new AbortController();

  validateTokenBudget(input.tokenBudget, 'tokenBudget');

  /** True once the thread has started a NEWER run than this one (§2.1). */
  const stale = async () =>
    runId !== undefined && (await deps.kv.get(runIdKey(threadId))) !== runId;

  // The lock carries the run id, so a later conflict can tell a duplicate
  // delivery of THIS run apart from an older run that is still finishing.
  const locked = await deps.kv.set(runLockKey(threadId), runId ?? randomUUID(), {
    onlyIfNotExists: true,
    exSeconds: deps.config.runLockLeaseSeconds,
  });
  if (!locked) return 'lock-conflict'; // another worker owns this thread (§2.8)

  // Token budget (§2.1 safety cap) — precedence: execute input → spec →
  // config. Checked BETWEEN steps: the finished step is always kept in full,
  // nothing is aborted mid-generation. This is NOT a user stop.
  const tokenBudget = input.tokenBudget ?? agent.spec.tokenBudget ?? deps.config.tokenBudget;

  // Provider-specific options (§3.1): spec default <- execute input,
  // shallow per-provider namespace; the execute input wins.
  const providerOptions = mergeProviderOptions(agent.spec.providerOptions, input.providerOptions);

  // Two ways a run ends early, one behavior — everything tears down at once:
  //   1. the state key reads CANCELLED: the user pressed stop (§2.1);
  //   2. the run id has moved on: the user pressed stop and then sent another
  //      message, which put RUNNING back over CANCELLED before this poll could
  //      read it. The state key lies in that window; the run id never does.
  const controlPoll = setInterval(async () => {
    try {
      if ((await deps.kv.get(`agent:state:${threadId}`)) === 'CANCELLED' || (await stale())) {
        abort.abort();
      }
    } catch {
      // transient kv errors must never kill the poller
    }
  }, deps.config.stopPollMs);

  try {
    // At-least-once idempotency (§2.8): a job whose run already ended — or
    // was stopped — must be a no-op on redelivery. A MISSING thread is the
    // same no-op: it was deleted (§3.2) and must never be resurrected.
    // A newer run already owns this thread: this job has nothing to do, and
    // must not touch state on the live run's behalf (§2.1).
    if (await stale()) return 'stale';

    const durable = await deps.storage.threads.get(threadId);
    if (
      !durable ||
      durable.state === 'CANCELLED' ||
      durable.state === 'COMPLETED' ||
      durable.state === 'FAILED'
    ) {
      return 'executed';
    }

    // Platform-owned toolset: HITL (§2.5) over the user's set; spawnSubagent
    // added ONLY when the spec opts in (§2.7). rawTools keeps the real
    // implementations — the resumed segment executes the approved tool.
    const sub = agent.spec.subagents
      ? agent.spec.subagents === true
        ? {}
        : agent.spec.subagents
      : null;
    const rawTools: Record<string, any> = {
      ...(agent.args.tools ?? {}),
      ...(sub
        ? {
            spawnSubagent: spawnSubagentTool({
              threadId,
              depth: 0,
              sem: agent.sem,
              ports: deps,
              sub,
            }),
          }
        : {}),
    };
    const resume: ResumeInfo = {
      agent: agent.name,
      model: input.model,
      ...(input.tokenBudget !== undefined ? { tokenBudget: input.tokenBudget } : {}),
      ...(providerOptions ? { providerOptions } : {}),
    };
    const tools = withHitl(deps, threadId, rawTools, resume);

    // §2.5 resume: a WAITING thread at segment start is either the /respond
    // continuation or a redelivery of the original job while still parked.
    if (durable?.state === 'WAITING_FOR_INPUT') {
      const pending = await loadPendingHitl(deps, threadId);
      if (!pending) {
        // WAITING without a pending request cannot be continued — fail into
        // the §2.8 policy rather than corrupting the conversation.
        throw new Error(`Thread ${threadId} is WAITING_FOR_INPUT without a pending INPUT_REQUIRED`);
      }
      const resumed = await resumePendingHitl(deps, threadId, pending, rawTools, abort.signal);
      if (!resumed) return 'executed'; // still parked — nothing to do yet
    }

    // Durable compaction pass — history always fits the model budget (§2.6);
    // the budget uses the resolved model's contextWindow (§3.3)
    const history = await compactContext(deps, threadId, input.model);
    const model = deps.resolveModel(input.model);

    // Prompt caching (§2.6): stamp the stable prefix once — appended step
    // messages extend the prompt without invalidating the breakpoints.
    let messages = history.map((m) => ({ role: m.role, content: m.content }) as any);
    if (deps.config.promptCaching) {
      messages = markPromptCaching(messages);
    }

    const userArgs = agent.args as Record<string, any>;
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
    let stepsLeft = deps.config.maxSteps;

    while (stepsLeft > 0 && !abort.signal.aborted) {
      stepsLeft--;
      let step: StepResult;
      try {
        step = await executeStep(agent, {
          kind: agent.kind,
          model: model.instance(),
          messages,
          tools,
          providerOptions,
          abortSignal: abort.signal,
          onChunk:
            agent.kind === 'stream-text'
              ? async (chunk) => {
                  // One canonical path for every client: durable log + live Pub/Sub (§2.1, §2.2)
                  await publish(deps, threadId, 'CHUNK', chunk);
                  userArgs.onChunk?.({ chunk }); // user callback still fires
                }
              : undefined,
        });
      } catch (err) {
        if (abort.signal.aborted) break; // user stop mid-step — finalize below
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
        await deps.storage.messages.append(threadId, { role: m.role as any, content: m.content });
      }
      messages.push(...step.responseMessages);

      // Token attribution (§4), accumulated across the segment's steps
      const a = attributeTokens(step.usage as any);
      attribution.inputTokens += a.inputTokens;
      attribution.cachedInputTokens += a.cachedInputTokens;
      attribution.outputTokens += a.outputTokens;
      attribution.totalTokens += a.totalTokens;
      tokensUsed += a.totalTokens;
      lastText = step.text ?? '';
      lastFinishReason = step.finishReason;

      // §2.5 park: a requiresConfirmation tool returned the sentinel — the
      // segment ends here on WAITING_FOR_INPUT (set by parkForApproval); the
      // /respond continuation (or TTL expiry) runs a fresh segment.
      if ((step.toolResults ?? []).some((r: any) => isParked(r?.result))) {
        parked = true;
        break;
      }

      // Budget check BETWEEN steps (§2.1) — the step that crossed the line
      // is kept in full; the next one never starts.
      if (tokenBudget && tokensUsed >= tokenBudget) break;

      // 'tool-calls' → the SDK executed the step's tools; the loop feeds the
      // results back. Anything else ('stop', 'length', …) ends the run.
      if (step.finishReason !== 'tool-calls') break;
    }

    if (parked) {
      // The segment ends holding the park: bill the steps up to the park
      // (§4) — the resumed segment records its own usage. NO state flip:
      // WAITING_FOR_INPUT (or CANCELLED if the user stopped meanwhile) stands.
      await deps.storage.usage.record(threadId, { agentId: agent.name, ...attribution });
      return 'executed';
    }

    const stopReason = abort.signal.aborted
      ? 'cancelled'
      : tokenBudget && tokensUsed >= tokenBudget
        ? 'token_budget'
        : lastFinishReason === 'tool-calls'
          ? 'max_steps' // step ceiling hit (§2.1)
          : 'completed';

    await finalize(deps, agent, threadId, {
      state: abort.signal.aborted ? 'CANCELLED' : 'COMPLETED',
      stopReason,
      tokensUsed,
      attribution,
      oneShotText: agent.kind === 'generate-text' ? lastText : undefined,
      runId,
    });

    return 'executed';
  } finally {
    clearInterval(controlPoll);
    await deps.kv.del(runLockKey(threadId)); // release — success, failure, or stop
  }
}

export interface FinalizeInput {
  state: ExecutionState;
  stopReason: 'completed' | 'token_budget' | 'max_steps' | 'cancelled';
  tokensUsed: number;
  attribution: TokenAttribution;
  /** generate-text flavor only: publish the final text as one TEXT_RESULT. */
  oneShotText?: string;
  /** The run this finalize speaks for (§2.1). State is written only while
   *  that run is still the thread's current one. */
  runId?: string;
}

/** Finalize a finished run (§5.6): attribute the segment's total tokens
 *  (input + cached + output, §4), then flip state on both homes and publish.
 *  Message persistence already happened per step inside the loop — finalize
 *  never touches messages.
 *
 *  A budget break is NOT a user stop: the run completes with stopReason
 *  'token_budget' and the usage it actually spent. */
export async function finalize(
  deps: RuntimePorts,
  agent: RegisteredAgent,
  threadId: string,
  f: FinalizeInput,
): Promise<void> {
  // Tokens this segment actually spent are always ours to record (§4), even
  // when the run was replaced part-way through.
  await deps.storage.usage.record(threadId, { agentId: agent.name, ...f.attribution });

  // Past that, a replaced run stays silent. Its CANCELLED would otherwise land
  // on top of the next run's RUNNING and wedge the thread: the new worker
  // would read a terminal state and no-op, and nobody would ever answer the
  // message the user just sent (§2.1).
  if (f.runId !== undefined && (await deps.kv.get(runIdKey(threadId))) !== f.runId) return;

  if (f.oneShotText !== undefined) {
    // One-shot flavor: no CHUNK stream — publish the final text as one event
    await publish(deps, threadId, 'TEXT_RESULT', { text: f.oneShotText });
  }

  await deps.kv.set(`agent:state:${threadId}`, f.state);
  await deps.storage.threads.setState(threadId, f.state);
  await publish(deps, threadId, 'STATE_CHANGE', {
    state: f.state,
    stopReason: f.stopReason,
    tokensUsed: f.tokensUsed,
    usage: f.attribution,
  });
}

/** A lock conflict has two very different causes (§2.8), and only one of them
 *  is a no-op:
 *
 *  - the lock carries THIS run's id → an at-least-once duplicate of a job that
 *    is already executing. Drop it.
 *  - the lock belongs to an OLDER run that has not finished tearing down →
 *    this job never ran. Dropping it strands the message the user just sent,
 *    so come back once the lock clears. Bounded by maxAttempts, then FAILED,
 *    so a wedged lock reports itself instead of spinning forever. */
async function redriveOnLockConflict(
  deps: RuntimePorts,
  agent: RegisteredAgent,
  input: ExecuteInput,
  maxAttempts: number,
): Promise<void> {
  if (!input.runId) return; // legacy dispatch, no identity — old drop behavior
  if ((await deps.kv.get(runLockKey(input.threadId))) === input.runId) return; // own duplicate
  if ((await deps.kv.get(runIdKey(input.threadId))) !== input.runId) return; // already replaced

  const tries = await deps.kv.incr(redriveKey(input.threadId));
  if (tries <= maxAttempts) {
    return deps.queue.enqueue(
      {
        threadId: input.threadId,
        runId: input.runId,
        model: input.model,
        agent: agent.name,
        tokenBudget: input.tokenBudget,
        providerOptions: input.providerOptions,
      },
      { delaySeconds: deps.config.runRedriveDelaySeconds },
    );
  }

  await deps.kv.del(redriveKey(input.threadId));
  await deps.kv.set(`agent:state:${input.threadId}`, 'FAILED');
  await deps.storage.threads.setState(input.threadId, 'FAILED');
  await publish(deps, input.threadId, 'STATE_CHANGE', { state: 'FAILED' });
}

/** §2.8 failure policy: transient errors redrive through the queue; exhausted
 *  attempts finalize FAILED (hot cache + durable). A user stop is never
 *  retried, and a successful run resets the attempt counter.
 *
 *  `exec` is an injection seam for tests (default: the real execute). */
export async function executeWithPolicy(
  deps: RuntimePorts,
  agent: RegisteredAgent,
  input: ExecuteInput,
  policy?: { maxAttempts?: number },
  exec: typeof execute = execute,
): Promise<void> {
  const maxAttempts = policy?.maxAttempts ?? deps.config.runMaxAttempts;
  try {
    const outcome = await exec(deps, agent, input);

    // Only a run THIS worker executed may reset the retry budget — a
    // lock-conflict no-op must never clear it while the owning worker runs (§2.8)
    if (outcome === 'executed') {
      await deps.kv.del(`agent:attempts:${input.threadId}`);
      await deps.kv.del(redriveKey(input.threadId));
      return;
    }

    // A newer run owns the thread: this job is a genuine no-op.
    if (outcome === 'stale') return;

    await redriveOnLockConflict(deps, agent, input, maxAttempts);
  } catch (err) {
    // A user stop already finalized the thread — never retry a stop
    if ((await deps.kv.get(`agent:state:${input.threadId}`)) === 'CANCELLED') return;

    const attempts = await deps.kv.incr(`agent:attempts:${input.threadId}`);
    if (attempts < maxAttempts) {
      return deps.queue.enqueue({
        threadId: input.threadId,
        model: input.model,
        agent: agent.name,
        tokenBudget: input.tokenBudget,
        providerOptions: input.providerOptions,
      });
    }

    // Attempts exhausted: finalize FAILED on BOTH the hot cache and durable
    // truth, or subsequent runs would still treat the thread as active (§2.1)
    await deps.kv.set(`agent:state:${input.threadId}`, 'FAILED');
    await deps.storage.threads.setState(input.threadId, 'FAILED');
    await publish(deps, input.threadId, 'STATE_CHANGE', { state: 'FAILED' });
    await deps.kv.del(`agent:attempts:${input.threadId}`);
  }
}
