import { randomUUID } from 'node:crypto';
import type { RuntimePorts } from '../ports/runtime.js';
import type { ExecutionState, ProviderOptions, ResumeInfo } from './types.js';
import { compactContext } from './context.js';
import type { TokenAttribution } from './usage.js';
import { markPromptCaching } from './cache.js';
import { mergeProviderOptions } from './types.js';
import type { RegisteredAgent } from './agent.js';
import {
  hitlKey,
  loadOpenHitls,
  withHitl,
  type PendingHitl,
} from './hitl.js';
import { publish, setThreadState } from './publish.js';
import { runNestedAgent, spawnSubagentTool, type SubagentCtx } from './subagent.js';
import { redriveKey, runIdKey } from './keys.js';
import { withRunState, type AgentRunState } from './state.js';
import { runLoop, type RunLedger } from './loop.js';

export { countTokens } from './usage.js';
// executeStep and the loop live in ./loop.js so a nested run can share them
// without engine ↔ subagent becoming a cycle (§2.7).
export { executeStep, isParked, runLoop, type LoopInput, type LoopOutcome, type RunLedger, type StepResult } from './loop.js';

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

/** Is this approval settled yet (§2.7)? Read-only on purpose: with several
 *  open at once, nothing may be executed until EVERY one is ready, or a
 *  redelivery would run half of them and then leave the thread parked with
 *  those verdicts already consumed. */
async function verdictReady(
  deps: RuntimePorts,
  pending: PendingHitl,
): Promise<'answered' | 'expired' | 'open'> {
  if (await deps.kv.get(hitlKey(pending.toolCallId))) return 'answered';
  return Date.now() - pending.requestedAt >= deps.config.hitlTtlMs ? 'expired' : 'open';
}

/** Turn a settled approval into the tool result the conversation will carry
 *  (§2.5): run the approved tool, record the denial, or convert an expired
 *  request into the timeout denial ("user had no response").
 *
 *  The tool failure is surfaced TO THE MODEL as the tool result — the verdict
 *  arrives from a different process than the one that ran the model, so the
 *  conversation always stays executable. */
async function settleVerdict(
  deps: RuntimePorts,
  threadId: string,
  pending: PendingHitl,
  target: { execute?: (args: unknown, opts: unknown) => Promise<unknown> } | undefined,
  signal: AbortSignal,
): Promise<unknown> {
  const raw = await deps.kv.get(hitlKey(pending.toolCallId));
  await deps.kv.del(hitlKey(pending.toolCallId));

  if (!raw) {
    await publish(deps, threadId, 'INPUT_EXPIRED', { toolCallId: pending.toolCallId });
    return { responded: false, cancelled: true, reason: 'timeout' };
  }

  const answer = JSON.parse(raw) as HitlAnswer;
  if (!answer.approved) return { denied: true };

  try {
    return target?.execute
      ? await target.execute(pending.arguments, {
          toolCallId: pending.toolCallId,
          abortSignal: signal,
        })
      : { error: `Unknown tool: ${pending.toolName}` };
  } catch (err) {
    return { error: err instanceof Error ? err.message : String(err) };
  }
}

/** Land a settled verdict and unwind whatever was waiting on it (§2.7).
 *
 *  The verdict belongs to the stream that asked — the main agent's, or a
 *  nested run's. When a nested run asked, its own loop is re-entered from its
 *  persisted turns and its result is handed to the call waiting one level up,
 *  repeating until the main agent's `spawnSubagent` call is answered.
 *
 *  Returns false when the unwind parked again: the thread stays
 *  WAITING_FOR_INPUT and a later dispatch picks up from the new request. */
async function unwindVerdict(
  deps: RuntimePorts,
  threadId: string,
  pending: PendingHitl,
  result: unknown,
  subCtx: SubagentCtx | null,
  signal: AbortSignal,
): Promise<boolean> {
  await deps.storage.messages.append(threadId, {
    role: 'tool',
    agentId: pending.agentId,
    content: [
      { type: 'tool-result', toolCallId: pending.toolCallId, toolName: pending.toolName, result },
    ],
  });

  // `producer` is whoever must now run to produce the next result. Undefined
  // means the main agent, whose loop the caller re-enters itself.
  let producer = pending.nested;
  for (let i = 0; i < pending.frames.length; i += 1) {
    const frame = pending.frames[i]!;
    if (!producer || !subCtx) break;

    const outcome = await runNestedAgent(subCtx, producer, null, signal, pending.frames.slice(i));
    if (outcome.parked) return false; // parked again, one level down
    if (outcome.aborted) return false; // user stop mid-unwind (§2.1)

    await deps.admin.runs.patch(producer.agentId, {
      state: 'COMPLETED',
      result: { text: outcome.text },
      endedAt: new Date(),
    });
    await publish(deps, threadId, 'SUBAGENT_COMPLETED', { agentId: producer.agentId });

    // Hand the capped result to the call one level up (§2.6)
    await deps.storage.messages.append(threadId, {
      role: 'tool',
      agentId: frame.agentId,
      content: [
        {
          type: 'tool-result',
          toolCallId: frame.toolCallId,
          toolName: 'spawnSubagent',
          result: {
            agentId: producer.agentId,
            result: outcome.text.slice(0, deps.config.subagentResultCapChars),
          },
        },
      ],
    });
    producer = frame.nested;
  }
  return true;
}

/** Sum this segment onto the run's record and stamp how it ended (§2.9). */
async function closeRunRecord(
  deps: RuntimePorts,
  runId: string,
  f: FinalizeInput,
): Promise<void> {
  try {
    const prior = await deps.admin.runs.get(runId);
    if (!prior) return; // a run started before §2.9, or a foreign dispatch
    const endedAt = new Date();
    await deps.admin.runs.patch(runId, {
      state: f.state,
      stopReason: f.stopReason,
      ...(f.error ? { error: f.error } : {}),
      endedAt,
      durationMs: endedAt.getTime() - new Date(prior.startedAt).getTime(),
      steps: prior.steps + (f.steps ?? 0),
      inputTokens: prior.inputTokens + f.attribution.inputTokens,
      cachedInputTokens: prior.cachedInputTokens + f.attribution.cachedInputTokens,
      outputTokens: prior.outputTokens + f.attribution.outputTokens,
      totalTokens: prior.totalTokens + f.attribution.totalTokens,
    });
  } catch {
    // Observability must never be able to fail a run that otherwise succeeded.
  }
}

/** Finalise a run as FAILED on both homes AND keep why (§2.9). The reason used
 *  to be dropped entirely, so an operator could see that something failed but
 *  never what. */
async function failRun(
  deps: RuntimePorts,
  threadId: string,
  runId: string | undefined,
  error: string,
): Promise<void> {
  await deps.kv.set(`agent:state:${threadId}`, 'FAILED');
  await setThreadState(deps, threadId, 'FAILED');
  if (runId) {
    await closeRunRecord(deps, runId, {
      state: 'FAILED',
      stopReason: 'completed',
      tokensUsed: 0,
      attribution: { inputTokens: 0, cachedInputTokens: 0, outputTokens: 0, totalTokens: 0 },
      error,
      runId,
    });
  }
  await publish(deps, threadId, 'STATE_CHANGE', { state: 'FAILED', error });
}

/** Resolve every parked request at segment start (§2.5, §2.7) and flip the
 *  thread back to RUNNING. Returns false when at least one approval is still
 *  open within its TTL: the dispatch is an at-least-once redelivery and the
 *  thread stays parked. `rawTools` must be the UNWRAPPED main toolset. */
async function resumePendingHitl(
  deps: RuntimePorts,
  threadId: string,
  open: PendingHitl[],
  rawTools: Record<string, any>,
  subCtx: SubagentCtx | null,
  signal: AbortSignal,
): Promise<boolean> {
  // Readiness first, side effects second: the thread resumes only when EVERY
  // open approval has been answered or has expired (§2.7).
  const states = await Promise.all(open.map((p) => verdictReady(deps, p)));
  if (states.includes('open')) return false; // redelivery no-op (§2.8)

  let expiredAny = false;
  for (const pending of open) {
    // A nested run's tools come from the delegation config, not the main
    // agent's set — the approved tool has to be resolved where it lives.
    const target =
      pending.agentId === null
        ? rawTools[pending.toolName]
        : (subCtx?.sub.tools as Record<string, any> | undefined)?.[pending.toolName];

    const result = await settleVerdict(deps, threadId, pending, target, signal);
    if ((result as { reason?: string })?.reason === 'timeout') expiredAny = true;
    if (!(await unwindVerdict(deps, threadId, pending, result, subCtx, signal))) return false;
  }

  await deps.kv.set(`agent:state:${threadId}`, 'RUNNING');
  await setThreadState(deps, threadId, 'RUNNING');
  await publish(deps, threadId, 'STATE_CHANGE', { state: 'RUNNING' });
  void expiredAny;
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
  /** Epoch ms at enqueue, for the queue-wait measurement (§2.9). */
  enqueuedAt?: number;
  /** The run's state (§2.10) — carried so a redrive keeps it. */
  state?: AgentRunState;
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

    const resume: ResumeInfo = {
      agent: agent.name,
      model: input.model,
      ...(runId ? { runId } : {}),
      ...(input.tokenBudget !== undefined ? { tokenBudget: input.tokenBudget } : {}),
      ...(providerOptions ? { providerOptions } : {}),
    };

    // One ledger for the whole run: a nested run's spend counts against the
    // same safety cap the main agent is checked against (§2.7).
    const ledger: RunLedger = { tokensUsed: 0 };

    // How long the dispatch sat in the queue before a worker took it (§2.9).
    if (runId && input.enqueuedAt) {
      await deps.admin.runs
        .patch(runId, { queuedMs: Date.now() - input.enqueuedAt })
        .catch(() => undefined);
    }

    // Platform-owned toolset: HITL (§2.5) over the user's set; spawnSubagent
    // added ONLY when the spec opts in (§2.7). rawTools keeps the real
    // implementations — the resolved park executes the approved tool.
    const sub = agent.spec.subagents
      ? agent.spec.subagents === true
        ? {}
        : agent.spec.subagents
      : null;
    const subCtx: SubagentCtx | null = sub
      ? {
          threadId,
          depth: 0,
          sem: agent.sem,
          ports: deps,
          sub,
          agent,
          ledger,
          resume,
          agentId: null, // spawned by the main agent
          frames: [],
          tokenBudget,
          providerOptions,
          abortSignal: abort.signal,
          state: input.state,
        }
      : null;
    const rawTools: Record<string, any> = {
      ...(agent.args.tools ?? {}),
      ...(subCtx ? { spawnSubagent: spawnSubagentTool(subCtx) } : {}),
    };
    // The main agent's own toolset: nothing is waiting on its parks (§2.7).
    // Every tool also sees the run's state (§2.10).
    const tools = withRunState(
      withHitl(deps, threadId, rawTools, { resume, agentId: null, frames: [] }),
      input.state ?? {},
    );

    // §2.5 resume: a WAITING thread at segment start is either the /respond
    // continuation or a redelivery of the original job while still parked.
    if (durable?.state === 'WAITING_FOR_INPUT') {
      // Every approval still open, not just the latest: one parent step can
      // park several nested runs at once (§2.7).
      const open = await loadOpenHitls(deps, threadId);
      if (open.length === 0) {
        // WAITING without a pending request cannot be continued — fail into
        // the §2.8 policy rather than corrupting the conversation.
        throw new Error(`Thread ${threadId} is WAITING_FOR_INPUT without a pending INPUT_REQUIRED`);
      }
      const resumed = await resumePendingHitl(
        deps, threadId, open, rawTools, subCtx, abort.signal,
      );
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

    const loop = await runLoop(
      deps,
      agent,
      threadId,
      {
        agentId: null, // the main agent's stream (§2.7)
        runId,
        kind: agent.kind,
        model: model.instance(),
        messages,
        tools,
        maxSteps: deps.config.maxSteps,
        abortSignal: abort.signal,
        providerOptions,
        tokenBudget,
        cacheSystemPrompt: deps.config.promptCaching,
        onChunk: async (chunk) => {
          // One canonical path for every client: durable log + live Pub/Sub (§2.1, §2.2)
          await publish(deps, threadId, 'CHUNK', chunk);
          userArgs.onChunk?.({ chunk }); // user callback still fires
        },
      },
      ledger,
    );

    const { attribution, parked } = loop;
    const tokensUsed = ledger.tokensUsed;
    const lastText = loop.text;
    const lastFinishReason = loop.finishReason;

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
      steps: loop.steps,
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
  /** Loop iterations this segment completed (§2.9). */
  steps?: number;
  /** Why it failed, when it did (§2.9). */
  error?: string;
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

  // Close the run's durable record (§2.9). Additive: a run that parked and
  // resumed finalises once, but its steps and tokens accrued over several
  // segments, so they are summed onto what is already there. The run lock
  // (§3.4) makes this read-modify-write single-writer.
  if (f.runId) await closeRunRecord(deps, f.runId, f);

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
  await setThreadState(deps, threadId, f.state);
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
        enqueuedAt: Date.now(),
        model: input.model,
        agent: agent.name,
        tokenBudget: input.tokenBudget,
        providerOptions: input.providerOptions,
      },
      { delaySeconds: deps.config.runRedriveDelaySeconds },
    );
  }

  await deps.kv.del(redriveKey(input.threadId));
  await failRun(deps, input.threadId, input.runId, 'the run lock never cleared');
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
        // A retry is the SAME run trying again (§2.1). Dropping the id here
        // left the retried job unable to notice it had been replaced, and
        // unable to redrive if it found the lock held.
        runId: input.runId,
        enqueuedAt: Date.now(),
        model: input.model,
        agent: agent.name,
        tokenBudget: input.tokenBudget,
        providerOptions: input.providerOptions,
      });
    }

    // Attempts exhausted: finalize FAILED on BOTH the hot cache and durable
    // truth, or subsequent runs would still treat the thread as active (§2.1)
    await failRun(
      deps,
      input.threadId,
      input.runId,
      err instanceof Error ? err.message : String(err),
    );
    await deps.kv.del(`agent:attempts:${input.threadId}`);
  }
}
