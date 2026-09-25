import type { RunFinishInfo, RuntimePorts } from '../ports/runtime.js';
import type { ExecutionState, ProviderOptions, ResumeInfo, RunPatch, UsageTotals } from './types.js';
import { wireId } from './types.js';
import { compactContext } from './context.js';
import type { TokenAttribution } from './usage.js';
import { markPromptCaching } from './cache.js';
import { promptMessages, repairDanglingToolCalls } from './messages.js';
import { mergeProviderOptions } from './types.js';
import type { RegisteredAgent } from './agent.js';
import {
  hitlKey,
  loadOpenHitls,
  withHitl,
  hitlDeadline,
  type PendingHitl,
} from './hitl.js';
import { ACTIVE_STATES, publish, publishEvent, runStatePayload, transition, withPublishEvent } from './publish.js';
import { runNestedAgent, spawnSubagentTool, type SubagentCtx } from './subagent.js';
import { attemptsKey, COUNTER_TTL_SECONDS, counterScope, redriveKey, runIdKey } from './keys.js';
import { withRunState, type AgentRunState } from './state.js';
import { runLoop, type RunLedger } from './loop.js';
import { enqueueJob, Lease, parseLockValue, runLockKey, RunLockLostError } from './lease.js';

export { countTokens } from './usage.js';
// executeStep and the loop live in ./loop.js so a nested run can share them
// without engine ↔ subagent becoming a cycle (§2.7).
export { executeStep, isParked, runLoop, type LoopInput, type LoopOutcome, type RunLedger, type StepResult } from './loop.js';


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
  return Date.now() >= hitlDeadline(pending, deps.config) ? 'expired' : 'open';
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
  state: AgentRunState,
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
          // The resumed tool gets the same context a live one does (§2.10).
          state,
          publishEvent: (type: string, payload: unknown, options?: { durable?: boolean }) =>
            publishEvent(deps, threadId, type, payload, options),
          // What the human sent back with the approval (§2.5): answers to
          // the questions the tool asked, a corrected value, a reason.
          approval: { payload: answer.payload },
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
): Promise<Date> {
  let endedAt = new Date();
  try {
    const prior = await deps.admin.runs.get(runId);
    if (!prior) return endedAt; // a run started before §2.9, or a foreign dispatch
    // stop() already records when the user ended this run. Worker teardown
    // may add usage, but must not move that timestamp or undo cancellation.
    const cancelled = prior.state === 'CANCELLED';
    if (cancelled && prior.endedAt) endedAt = new Date(prior.endedAt);
    await deps.admin.runs.patch(runId, {
      state: cancelled ? 'CANCELLED' : f.state,
      stopReason: cancelled ? 'cancelled' : f.stopReason,
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
  return endedAt;
}

/** Close a run record that can never be worked on again (§2.9): the thread is
 *  gone, or a newer run replaced this one. Without this the record stays
 *  open for ever and every "in flight" count carries it. */
export async function closeIfOpen(
  deps: RuntimePorts,
  runId: string | undefined,
  stopReason: FinalizeInput['stopReason'],
) {
  if (!runId) return;
  const rec = await deps.admin.runs.get(runId).catch(() => null);
  if (!rec || rec.endedAt) return;
  await closeRunRecord(deps, runId, {
    state: 'CANCELLED',
    stopReason,
    tokensUsed: 0,
    attribution: { inputTokens: 0, cachedInputTokens: 0, outputTokens: 0, totalTokens: 0 },
    runId,
  });
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
  // Only while the thread is still this run's and still going: a stop or a
  // newer run that got there first keeps its own ending (§3.4).
  if (!(await transition(deps, threadId, { from: ACTIVE_STATES, to: 'FAILED', runId }))) return;
  let endedAt = new Date();
  if (runId) {
    endedAt = await closeRunRecord(deps, runId, {
      state: 'FAILED',
      stopReason: 'failed',
      tokensUsed: 0,
      attribution: { inputTokens: 0, cachedInputTokens: 0, outputTokens: 0, totalTokens: 0 },
      error,
      runId,
    });
  }
  await publish(deps, threadId, 'STATE_CHANGE', {
    state: 'FAILED', stopReason: 'failed', error, endedAt, ...(runId ? { runId } : {}),
  });
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
  state: AgentRunState,
  runId: string | undefined,
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

    const result = await settleVerdict(deps, threadId, pending, target, signal, state);
    if ((result as { reason?: string })?.reason === 'timeout') expiredAny = true;
    if (!(await unwindVerdict(deps, threadId, pending, result, subCtx, signal))) return false;
  }

  // A stop that landed while the verdicts were applied wins: the thread stays
  // CANCELLED and this segment goes no further (§3.4).
  if (!(await transition(deps, threadId, { from: ['WAITING_FOR_INPUT'], to: 'RUNNING', runId }))) {
    return false;
  }
  await publish(deps, threadId, 'STATE_CHANGE', await runStatePayload(deps, 'RUNNING', runId));
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
 *  SET NX + lease) before any work, and renews it while it works — two
 *  workers can never run one thread, and a crashed worker's lock expires
 *  instead of blocking forever (§3.4). Returns 'lock-conflict' when the lock
 *  is held, so callers can tell a genuine no-op apart from a completed run,
 *  and 'lock-lost' when the lock could not be kept. */
export interface ExecuteInput {
  threadId: string;
  model: string;
  /** This dispatch's run id (§2.1). A job without one keeps the old
   *  behavior: no staleness check, and no redrive on a lock conflict. */
  runId?: string;
  /** The job's delivery id (see `RunJob.dispatchId`). Absent on a call that
   *  did not come off the queue; the lock then makes one up. */
  dispatchId?: string;
  /** Epoch ms at enqueue, for the queue-wait measurement (§2.9). */
  enqueuedAt?: number;
  /** The run's state (§2.10) — carried so a redrive keeps it. */
  state?: AgentRunState;
  tokenBudget?: number;
  /** The run's money cap (§4), carried on the dispatch so the worker enforces
   *  what the caller asked for. */
  costBudgetMicros?: number;
  providerOptions?: ProviderOptions;
}

/** 'executed'      — this worker ran the segment (or it was a legitimate no-op).
 *  'lock-conflict' — someone else holds the thread's run lock; nothing ran.
 *  'stale'         — a NEWER run owns the thread; this job must do nothing.
 *  'lock-lost'     — this worker held the lock and could not keep it; the
 *                    segment ended early and the job should come back once
 *                    the lock is free. Every step it finished is persisted. */
export type ExecuteOutcome = 'executed' | 'lock-conflict' | 'stale' | 'lock-lost';

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

  // The lock names this run and this delivery of it (§3.4), so a later
  // conflict can tell a duplicate of THIS job apart from another delivery of
  // the same run and from an older run that is still finishing.
  const lease = await Lease.acquire(deps, threadId, runId, input.dispatchId);
  if (!lease) return 'lock-conflict'; // another worker owns this thread (§2.8)
  // Renewed until released, not only while the model runs: finalize is a
  // write too. A lease that cannot be kept ends the segment at once — and is
  // told apart from a user stop, which ends it the same way.
  lease.keep(() => abort.abort());

  // Token budget (§2.1 safety cap) — precedence: execute input → spec →
  // config. Checked BETWEEN steps: the finished step is always kept in full,
  // nothing is aborted mid-generation. This is NOT a user stop.
  const tokenBudget = input.tokenBudget ?? agent.spec.tokenBudget ?? deps.config.tokenBudget;
  // The money cap (§4) resolves the same way, widest last.
  const costBudget =
    input.costBudgetMicros ?? agent.spec.costBudgetMicros ?? deps.config.costBudgetMicros;

  // Provider-specific options (§3.1): spec default <- execute input,
  // shallow per-provider namespace; the execute input wins.
  // Three levels, widest first: runtime config → agent spec → this run.
  // Each wins over the one before it, per provider namespace (§3.1).
  const providerOptions = mergeProviderOptions(
    mergeProviderOptions(deps.config.providerOptions, agent.spec.providerOptions),
    input.providerOptions,
  );

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
    try {
      // At-least-once idempotency (§2.8): a job whose run already ended — or
      // was stopped — must be a no-op on redelivery. A MISSING thread is the
      // same no-op: it was deleted (§3.2) and must never be resurrected.
      // A newer run already owns this thread: this job has nothing to do, and
      // must not touch state on the live run's behalf (§2.1).
      // Its own record is closed, so it does not count as in flight for ever.
      if (await stale()) {
        await closeIfOpen(deps, runId, 'replaced');
        return 'stale';
      }

      const durable = await deps.storage.threads.get(threadId);
      if (!durable) {
        await closeIfOpen(deps, runId, 'orphaned');
        return 'executed';
      }
      if (
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
        ...(input.costBudgetMicros !== undefined
          ? { costBudgetMicros: input.costBudgetMicros }
          : {}),
        ...(providerOptions ? { providerOptions } : {}),
        // Carried so the resumed segment scopes its storage the same way this
        // one does (§2.10).
        ...(input.state ? { state: input.state } : {}),
      };

      // One ledger for the whole run: a nested run's spend counts against the
      // same safety cap the main agent is checked against (§2.7).
      const ledger: RunLedger = { tokensUsed: 0 };

      // Pickup (§2.8): the run has a worker now. A QUEUED thread becomes
      // RUNNING on every home, its record takes the moment work started, and
      // the wire says so, so a client's clock measures work rather than
      // waiting. The wait itself is kept on the record: the latest dispatch's,
      // so a resume's or a retry's wait shows as its own.
      const pickedUp = new Date();
      if (runId) {
        const patch: RunPatch = {};
        if (input.enqueuedAt) patch.queuedMs = pickedUp.getTime() - input.enqueuedAt;
        if (durable.state === 'QUEUED') {
          // A stop (or a newer run) that landed since the read above wins:
          // this job has nothing to pick up (§3.4).
          if (!(await transition(deps, threadId, {
            from: ['QUEUED'], to: 'RUNNING', runId, model: input.model,
          }))) {
            return 'executed';
          }
          let startedAt = pickedUp;
          const rec = await deps.admin.runs.get(runId).catch(() => null);
          if (rec && rec.queuedMs != null) {
            startedAt = new Date(rec.startedAt); // a retry: the run started when it first ran
          } else {
            patch.startedAt = pickedUp;
          }
          patch.state = 'RUNNING';
          await deps.admin.runs.patch(runId, patch).catch(() => undefined);
          await publish(deps, threadId, 'STATE_CHANGE', { state: 'RUNNING', runId, startedAt });
          durable.state = 'RUNNING';
        } else if (patch.queuedMs !== undefined) {
          await deps.admin.runs.patch(runId, patch).catch(() => undefined);
        }
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
            costBudgetMicros: costBudget,
            billingRunId: runId,
            providerOptions,
            abortSignal: abort.signal,
            fenced: () => lease.lost,
            state: input.state,
          }
        : null;
      const rawTools: Record<string, any> = {
        ...(agent.args.tools ?? {}),
        ...(subCtx ? { spawnSubagent: spawnSubagentTool(subCtx) } : {}),
      };
      // The main agent's own toolset: nothing is waiting on its parks (§2.7).
      // Every tool also sees the run's state (§2.10).
      // Every tool also sees the run's state (§2.10) and can publish its own
      // events on the thread.
      const tools = withRunState(
        withPublishEvent(
          deps,
          threadId,
          withHitl(deps, threadId, rawTools, { resume, agentId: null, frames: [] }),
        ),
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
          deps, threadId, open, rawTools, subCtx, abort.signal, input.state ?? {}, runId,
        );
        if (!resumed) return 'executed'; // still parked — nothing to do yet
      }

      // Durable compaction pass — history always fits the model budget (§2.6);
      // the budget uses the resolved model's contextWindow (§3.3)
      const history = await compactContext(deps, threadId, input.model);
      const model = deps.resolveModel(input.model);

      // Prompt caching (§2.6): stamp the stable prefix once — appended step
      // messages extend the prompt without invalidating the breakpoints.
      let messages = repairDanglingToolCalls(promptMessages(history) as any[]);
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
          costBudgetMicros: costBudget,
          billingRunId: runId,
          modelKey: input.model,
          modelId: wireId(model, input.model),
          agentName: agent.name,
          cacheSystemPrompt: deps.config.promptCaching,
          fenced: () => lease.lost,
          onChunk: async (chunk) => {
            // One canonical path for every client: durable log + live Pub/Sub (§2.1, §2.2)
            await publish(deps, threadId, 'CHUNK', chunk);
            userArgs.onChunk?.({ chunk }); // user callback still fires
          },
        },
        ledger,
      );

      // A lost lock aborts the run the way a stop does, but it is not a stop:
      // another worker may own the thread now, so nothing below may write.
      if (lease.lost) return 'lock-lost';

      const { attribution, parked } = loop;
      const tokensUsed = ledger.tokensUsed;
      const lastText = loop.text;
      const lastFinishReason = loop.finishReason;

      if (parked) {
        // The segment ends holding the park. Every call it made was already
        // recorded and priced as it happened (§4), so there is nothing left to
        // bill here. NO state flip: WAITING_FOR_INPUT (or CANCELLED if the user
        // stopped meanwhile) stands.
        if (runId) {
          try {
            const prior = await deps.admin.runs.get(runId);
            if (prior) await deps.admin.runs.patch(runId, {
              steps: prior.steps + loop.steps,
              inputTokens: prior.inputTokens + attribution.inputTokens,
              cachedInputTokens: prior.cachedInputTokens + attribution.cachedInputTokens,
              outputTokens: prior.outputTokens + attribution.outputTokens,
              totalTokens: prior.totalTokens + attribution.totalTokens,
            });
          } catch { /* Operational history must not fail a parked run. */ }
        }
        return 'executed';
      }

      const stopReason = abort.signal.aborted
        ? 'cancelled'
        : loop.costExhausted
          ? 'cost_budget' // the money cap (§4)
          : tokenBudget && tokensUsed >= tokenBudget
            ? 'token_budget'
            : lastFinishReason === 'tool-calls'
              ? 'max_steps' // step ceiling hit (§2.1)
              : 'completed';

      const state = abort.signal.aborted ? 'CANCELLED' : 'COMPLETED';
      await finalize(deps, agent, threadId, {
        state,
        stopReason,
        tokensUsed,
        attribution,
        oneShotText: agent.kind === 'generate-text' ? lastText : undefined,
        runId,
        steps: loop.steps,
      });

      if (typeof userArgs.onFinish === 'function') {
        // The whole run's bill, read back from the rows the loop wrote (§4):
        // every segment and every nested run, priced and grouped into lines, so
        // a settle hook charges in one pass without keeping its own tally.
        // The run is already finalized: a callback that throws is the
        // caller's bug to see in the log, not a reason to fail a finished run.
        try {
          await userArgs.onFinish({
            threadId,
            runId,
            state,
            stopReason,
            tokensUsed,
            attribution,
            steps: loop.steps,
            usage: await runBill(deps, threadId, runId),
          } satisfies RunFinishInfo);
        } catch (err) {
          (deps.log ?? console).error('onFinish threw', { runId, err: String(err) });
        }
      }

      return 'executed';
    } catch (err) {
      // Whatever failed after the lease was lost is that loss showing through
      // (an aborted call, a fenced write): the job comes back, it is not a
      // failed attempt.
      if (lease.lost || err instanceof RunLockLostError) return 'lock-lost';
      throw err;
    }
  } finally {
    clearInterval(controlPoll);
    // Release — success, failure, or stop — only while the lock is still this
    // worker's. One another worker took after this one's lapsed is theirs.
    await lease.release();
  }
}

export interface FinalizeInput {
  state: ExecutionState;
  /** Why the run ended. 'failed' for a failure; 'replaced', 'orphaned' and
   *  'deleted' close a record that can never be worked on again. */
  stopReason:
    | 'completed' | 'token_budget' | 'cost_budget' | 'max_steps' | 'cancelled'
    | 'failed' | 'replaced' | 'orphaned' | 'deleted';
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
  // Nothing to bill here: every model call recorded and priced its own row as
  // it happened, inside the loop (§4). Even a run that was replaced part-way
  // through has already had its calls written.
  //
  // The thread moves only while it is still this run's and still RUNNING
  // (§3.4). A stop that got there first has already written CANCELLED and
  // said so; a newer run owns the thread. Either way this finish loses and
  // stays silent: publishing it would land on top of the stop, or wedge the
  // newer run (§2.1).
  const won = await transition(deps, threadId, { from: ['RUNNING'], to: f.state, runId: f.runId });
  // The run's record closes either way (§2.9): the segment's steps and tokens
  // are real. Additive: a run that parked and resumed finalises once, but its
  // steps and tokens accrued over several segments. A stop already recorded
  // on it is kept (closeRunRecord never undoes one).
  const endedAt = f.runId ? await closeRunRecord(deps, f.runId, f) : new Date();
  if (!won) return;

  if (f.oneShotText !== undefined) {
    // One-shot flavor: no CHUNK stream — publish the final text as one event
    await publish(deps, threadId, 'TEXT_RESULT', { text: f.oneShotText });
  }

  // The run's identity and end time ride the terminal event, so a client
  // closes the right timer without reading the run record back.
  await publish(deps, threadId, 'STATE_CHANGE', {
    state: f.state,
    stopReason: f.stopReason,
    tokensUsed: f.tokensUsed,
    usage: f.attribution,
    endedAt,
    ...(f.runId ? { runId: f.runId } : {}),
    ...(f.error ? { error: f.error } : {}),
  });
}

/** A lock conflict (§2.8). The lock names the run and the delivery that hold
 *  it (§3.4), so a conflict can say which of these it is:
 *
 *  - the same job, delivered twice by the queue (same run, same dispatch): a
 *    duplicate of work already running. Drop it.
 *  - another delivery of the same run — a retry, an approval's answer or its
 *    expiry (§2.5) — arriving while the current holder winds down. It has
 *    work to do once the lock clears, so it comes back. Dropping it would
 *    leave the thread waiting (or RUNNING) with nobody working on it.
 *  - an OLDER run that has not finished tearing down: this job never ran.
 *    Dropping it strands the message the user just sent, so it comes back.
 *
 *  Because the lock is renewed while its holder runs, a held lock means a
 *  live worker, and waiting for it is right. The job comes back with a
 *  growing delay until it has waited at least one lease and used its
 *  attempts; only then is the lock taken to be wedged and the run FAILED. */
async function redriveOnLockConflict(
  deps: RuntimePorts,
  agent: RegisteredAgent,
  input: ExecuteInput,
  maxAttempts: number,
): Promise<void> {
  if (!input.runId) return; // legacy dispatch, no identity — old drop behavior
  // A thread that has ended has nothing left for this job, whoever holds the
  // lock: redriving it would only fail the ended run again.
  const durable = await deps.storage.threads.get(input.threadId);
  const state = durable?.state;
  if (!durable || state === 'CANCELLED' || state === 'COMPLETED' || state === 'FAILED') return;
  const holder = parseLockValue(await deps.kv.get(runLockKey(input.threadId)));
  if (holder.runId === input.runId) {
    if (holder.dispatchId !== null && holder.dispatchId === input.dispatchId) return; // the same job, twice
    if (state !== 'WAITING_FOR_INPUT' && (holder.dispatchId === null || !input.dispatchId)) {
      // A lock or a job from before dispatch ids: the two deliveries cannot be
      // told apart, so the old rule stands — a duplicate of a running segment.
      return;
    }
    // Otherwise: another delivery of this run, waiting on the current holder.
  }
  if ((await deps.kv.get(runIdKey(input.threadId))) !== input.runId) return; // already replaced

  const scope = counterScope(input.threadId, input.runId);
  const tries = await deps.kv.incrWithExpiry(redriveKey(scope), COUNTER_TTL_SECONDS);
  const { delaySeconds, waitedSeconds } = redriveDelay(deps, tries);
  if (tries <= maxAttempts || waitedSeconds < deps.config.runLockLeaseSeconds) {
    return enqueueJob(
      deps,
      {
        threadId: input.threadId,
        runId: input.runId,
        enqueuedAt: Date.now(),
        model: input.model,
        agent: agent.name,
        tokenBudget: input.tokenBudget,
        // A redrive is the SAME run trying again, so it keeps the caps it was
        // dispatched with — a retry that lost its money cap would be unbounded.
        costBudgetMicros: input.costBudgetMicros,
        providerOptions: input.providerOptions,
        state: input.state,
      },
      { delaySeconds },
    );
  }

  await deps.kv.del(redriveKey(scope));
  await failRun(deps, input.threadId, input.runId, 'the run lock never cleared');
}

/** How long the given try waits (`runRedriveDelaySeconds`, doubled on each
 *  try, capped at the lease), and how long the tries before it waited in all. */
function redriveDelay(deps: RuntimePorts, tries: number) {
  const lease = deps.config.runLockLeaseSeconds;
  // A zero base would never grow, and never give up.
  let delaySeconds = deps.config.runRedriveDelaySeconds || 1;
  let waitedSeconds = 0;
  for (let i = 1; i < tries; i++) {
    waitedSeconds += delaySeconds;
    delaySeconds = Math.min(delaySeconds * 2, lease);
  }
  return { delaySeconds, waitedSeconds };
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
  const scope = counterScope(input.threadId, input.runId);
  try {
    const outcome = await exec(deps, agent, input);

    // Only a run THIS worker executed may reset the retry budget — a
    // lock-conflict no-op must never clear it while the owning worker runs (§2.8)
    if (outcome === 'executed') {
      await deps.kv.del(attemptsKey(scope));
      await deps.kv.del(redriveKey(scope));
      return;
    }

    // A newer run owns the thread: this job is a genuine no-op.
    if (outcome === 'stale') return;

    await redriveOnLockConflict(deps, agent, input, maxAttempts);
  } catch (err) {
    const log = deps.log ?? console;
    // A user stop already finalized the thread — never retry a stop
    if ((await deps.kv.get(`agent:state:${input.threadId}`)) === 'CANCELLED') return;
    // A run that a newer one replaced failed after it stopped mattering: its
    // error is not the thread's, so it spends no attempt and fails nothing.
    if (input.runId && (await deps.kv.get(runIdKey(input.threadId))) !== input.runId) return;

    const attempts = await deps.kv.incrWithExpiry(attemptsKey(scope), COUNTER_TTL_SECONDS);
    if (attempts < maxAttempts) {
      // A retry is the SAME run trying again (§2.1): it keeps the id, so it
      // can notice it was replaced and redrive if it finds the lock held.
      const delayMs = retryBackoffMs(deps, attempts);
      (log as { warn?: (m: string, ...r: unknown[]) => void }).warn?.('run failed; retry scheduled', {
        threadId: input.threadId, runId: input.runId, err: String(err), attempt: attempts, maxAttempts, delayMs,
      });
      await requeue(deps, agent, input, delayMs);
      return;
    }

    // Attempts exhausted: finalize FAILED on BOTH the hot cache and durable
    // truth, or subsequent runs would still treat the thread as active (§2.1)
    log.error('run failed; attempts spent', { threadId: input.threadId, runId: input.runId, err: String(err), attempts });
    await failRun(
      deps,
      input.threadId,
      input.runId,
      err instanceof Error ? err.message : String(err),
    );
    await deps.kv.del(attemptsKey(scope));
  }
}

/** Put the same run back on the queue as a retry (§2.8). The thread reads
 *  QUEUED while it waits, so a client sees a run waiting to retry rather than
 *  one that looks like it is working. */
async function requeue(
  deps: RuntimePorts,
  agent: RegisteredAgent,
  input: ExecuteInput,
  delayMs: number,
): Promise<void> {
  if (input.runId) await markQueued(deps, input.threadId, input.runId, input.model);
  await enqueueJob(
    deps,
    {
      threadId: input.threadId,
      runId: input.runId,
      enqueuedAt: Date.now(),
      model: input.model,
      agent: agent.name,
      tokenBudget: input.tokenBudget,
      // A retry is the SAME run trying again, so it keeps the caps it was
      // dispatched with — a retry that lost its money cap would be unbounded.
      costBudgetMicros: input.costBudgetMicros,
      providerOptions: input.providerOptions,
      state: input.state,
    },
    delayMs > 0 ? { delaySeconds: Math.ceil(delayMs / 1000) } : undefined,
  );
}

/** Move a RUNNING thread back to QUEUED for a retry, on every home, and say so
 *  on the wire. A parked thread keeps WAITING_FOR_INPUT: the park machinery
 *  reads that state. A replaced run leaves the live run's state alone. */
async function markQueued(deps: RuntimePorts, threadId: string, runId: string, model: string) {
  if ((await deps.kv.get(runIdKey(threadId))) !== runId) return;
  // Only a RUNNING thread that is still this run's (§3.4).
  if (!(await transition(deps, threadId, { from: ['RUNNING'], to: 'QUEUED', runId, model }))) return;
  await deps.admin.runs.patch(runId, { state: 'QUEUED' }).catch(() => undefined);
  await publish(deps, threadId, 'STATE_CHANGE', { state: 'QUEUED', runId, enqueuedAt: new Date() });
}

/** How long the nth retry waits (§2.8): the base doubled per attempt, capped,
 *  with up to a quarter of jitter so a fleet that failed together does not
 *  retry together. */
function retryBackoffMs(deps: RuntimePorts, attempt: number): number {
  const base = deps.config.runRetryBackoffMs;
  if (base <= 0) return 0;
  const max = deps.config.runRetryBackoffMaxMs;
  let d = base;
  for (let i = 1; i < attempt; i++) {
    d *= 2;
    if (max > 0 && d >= max) {
      d = max;
      break;
    }
  }
  if (max > 0 && d > max) d = max;
  return d + Math.floor(Math.random() * (d / 4 + 1));
}


/** Sum every model call a run made, nested runs included (§4). Best effort
 *  like every other read on the finish path: a storage hiccup must not turn a
 *  finished run into a failed one, so a failure comes back as zero totals and
 *  the error is logged. */
async function runBill(
  deps: RuntimePorts,
  threadId: string,
  runId?: string,
): Promise<UsageTotals> {
  const empty: UsageTotals = {
    inputTokens: 0, cachedInputTokens: 0, outputTokens: 0, totalTokens: 0,
    costMicros: 0, unpriced: 0, lines: [],
  };
  if (!runId) return empty;
  try {
    return await deps.storage.usage.total(threadId, { runId });
  } catch (err) {
    (deps.log ?? console).error('run bill not read', { run: runId, err });
    return empty;
  }
}
