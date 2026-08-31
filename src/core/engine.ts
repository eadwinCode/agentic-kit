import { generateText, streamText } from 'ai';
import { randomUUID } from 'node:crypto';
import type { RuntimePorts } from '../ports/runtime.js';
import type { ExecutionState } from './types.js';
import { compactContext } from './context.js';
import type { RegisteredAgent } from './agent.js';
import { suspendForApproval, type SuspendInput } from './hitl.js';
import { publish } from './publish.js';
import { countTokens } from './usage.js';
import { spawnSubagentTool } from './subagent.js';

export { countTokens } from './usage.js';

const runLockKey = (threadId: string) => `agent:lock:${threadId}`;

/** Tools the engine treats as destructive: parked behind suspendForApproval
 *  (§2.5) instead of executing directly. Purely a marker — see withHitl. */
export function markRequiresConfirmation<T extends object>(t: T): T {
  return Object.assign(t, { requiresConfirmation: true });
}

/** Wrap every marked tool so the engine suspends instead of executing (§2.5). */
function withHitl(deps: RuntimePorts, threadId: string, tools: Record<string, any>) {
  const out: Record<string, any> = {};
  for (const [name, t] of Object.entries(tools)) {
    if (!(t as any)?.requiresConfirmation) {
      out[name] = t;
      continue;
    }
    out[name] = {
      ...t,
      execute: async (args: unknown, opts: { toolCallId?: string; abortSignal?: AbortSignal }) => {
        const response = await suspendForApproval(deps, {
          threadId,
          toolCallId: opts?.toolCallId ?? randomUUID(),
          toolName: name,
          args,
          ttlMs: deps.config.hitlTtlMs,
          signal: opts?.abortSignal ?? new AbortController().signal,
        } satisfies SuspendInput);
        if (!response) return { responded: false, cancelled: true, reason: 'aborted' };
        if ('responded' in response) return response; // timeout outcome already recorded
        if (!response.approved) return { denied: true };
        return (t as any).execute(args, opts);
      },
    };
  }
  return out;
}

/** The engine (§2.1, §5.6). Worker-side only — runs are dispatched via the
 *  queue (§2.8) and may outlive any HTTP response.
 *
 *  Concurrency: acquires the per-thread run lock (`agent:lock:{threadId}`,
 *  SET NX + lease) before any work — two workers can never run one thread,
 *  and a crashed worker's lock expires instead of blocking forever (§3.4).
 *  Returns 'lock-conflict' when the lock is held, so callers can tell a
 *  genuine no-op apart from a completed run. */
export async function execute(
  deps: RuntimePorts,
  agent: RegisteredAgent,
  input: { threadId: string; model: string; tokenBudget?: number },
): Promise<'executed' | 'lock-conflict'> {
  const { threadId } = input;
  const abort = new AbortController();

  const locked = await deps.kv.set(runLockKey(threadId), randomUUID(), {
    onlyIfNotExists: true,
    exSeconds: deps.config.runLockLeaseSeconds,
  });
  if (!locked) return 'lock-conflict'; // another worker owns this thread (§2.8)

  // Token budget (§2.1 safety cap) — precedence: execute input → spec →
  // config. Tracked cumulatively on every onStepFinish; the loop breaks
  // BEFORE the next step once spent. This is NOT a user stop.
  const tokenBudget = input.tokenBudget ?? agent.spec.tokenBudget ?? deps.config.tokenBudget;
  let tokensUsed = 0;
  let budgetExceeded = false;

  // One signal, one behavior: the moment the state key reads CANCELLED —
  // the user pressed stop (§2.1) — everything tears down immediately.
  const controlPoll = setInterval(async () => {
    try {
      if ((await deps.kv.get(`agent:state:${threadId}`)) === 'CANCELLED') abort.abort();
    } catch {
      // transient kv errors must never kill the poller
    }
  }, 500);

  try {
    // Durable compaction pass — history always fits the model budget (§2.6);
    // the budget uses the resolved model's contextWindow (§3.3)
    const history = await compactContext(deps, threadId, input.model);
    const model = deps.resolveModel(input.model);

    // Platform-owned tool wrapping: HITL (§2.5) over the user's set.
    // spawnSubagent is added ONLY when the spec opts in (§2.7).
    const sub = agent.spec.subagents
      ? agent.spec.subagents === true
        ? {}
        : agent.spec.subagents
      : null;
    const tools = withHitl(deps, threadId, {
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
    });

    // Ownership rule (§3.1): user args spread FIRST, platform keys LAST —
    // persistence, billing, the stop signal, and the safety cap cannot be
    // opted out of.
    const shared = {
      ...agent.args,
      model: model.instance(),
      messages: history.map((m) => ({ role: m.role, content: m.content }) as any),
      tools,
      abortSignal: abort.signal,
      maxSteps: deps.config.maxSteps,
      onStepFinish: (step: any) => {
        agent.args.onStepFinish?.(step); // user callback still fires
        // Budget tracking — break the loop BEFORE the next step
        tokensUsed += countTokens(step.usage);
        if (tokenBudget && tokensUsed >= tokenBudget) {
          budgetExceeded = true;
          abort.abort(); // not a user stop — see finalize below
        }
      },
      onChunk: async ({ chunk }: any) => {
        // One canonical path for every client: durable log + live Pub/Sub (§2.1, §2.2)
        await publish(deps, threadId, 'CHUNK', chunk);
        agent.args.onChunk?.({ chunk }); // user callback still fires
      },
      onFinish: async (finishParams: any) => {
        await finalize(deps, agent, input, finishParams, abort, {
          budgetExceeded,
          tokensUsed,
        });
        agent.args.onFinish?.(finishParams); // user callback still fires
      },
    };

    if (agent.kind === 'stream-text') {
      const result = streamText(shared);
      // streamText is lazy: drain the full stream so chunk/finish callbacks
      // run, while still propagating provider errors to retry policy.
      for await (const _part of result.fullStream) {
        // All processing happens in onChunk/onFinish above.
      }
    } else {
      const result = await generateText(shared);
      // One-shot flavor: no CHUNK stream — publish the final text as one event
      await publish(deps, threadId, 'TEXT_RESULT', { text: result.text });
    }

    return 'executed';
  } finally {
    clearInterval(controlPoll);
    await deps.kv.del(runLockKey(threadId)); // release — success, failure, or stop
  }
}

/** Finalize a finished run (§5.6): persist the completed assistant turn(s)
 *  BEFORE the state transition, attribute total tokens, then flip state on
 *  both homes and publish.
 *
 *  Hardened:
 *  - Some providers omit streaming usage — the SDK reports NaN for those.
 *    Never let optional metering keep a completed run stuck in RUNNING.
 *  - A budget break is NOT a user stop: the run completes with
 *    stopReason 'token_budget' and the partial usage it managed to spend. */
export async function finalize(
  deps: RuntimePorts,
  agent: RegisteredAgent,
  input: { threadId: string },
  finishParams: {
    usage?: { inputTokens?: number; outputTokens?: number; promptTokens?: number; completionTokens?: number };
    finishReason: string;
    response: { messages: Array<{ role: string; content: unknown }> };
  },
  abort: AbortController,
  budget: { budgetExceeded: boolean; tokensUsed: number },
): Promise<void> {
  const { threadId } = input;

  // Persist the completed assistant turn(s) — including tool calls and tool
  // results — BEFORE the state transition, so redrives, HITL resumes, and
  // replay always see a valid history (§2.2, §2.8). On a budget-broken run
  // the recorded usage is partial by definition: the budget was spent.
  for (const message of finishParams.response.messages) {
    await deps.storage.messages.append(threadId, {
      role: message.role as any,
      content: message.content,
    });
  }

  const totalTokens = countTokens(finishParams.usage);

  // Token attribution only (§4): total tokens used. Pricing is downstream.
  await deps.storage.usage.record(threadId, {
    agentId: agent.name,
    totalTokens,
  });

  const finalState: ExecutionState =
    abort.signal.aborted && !budget.budgetExceeded ? 'CANCELLED' : 'COMPLETED';
  const stopReason = budget.budgetExceeded
    ? 'token_budget'
    : abort.signal.aborted
      ? 'cancelled'
      : finishParams.finishReason === 'tool-calls'
        ? 'max_steps' // safety cap hit (§2.1)
        : 'completed';

  await deps.kv.set(`agent:state:${threadId}`, finalState);
  await deps.storage.threads.setState(threadId, finalState);
  await publish(deps, threadId, 'STATE_CHANGE', {
    state: finalState,
    stopReason,
    tokensUsed: budget.tokensUsed,
  });
}

/** §2.8 failure policy: transient errors redrive through the queue; exhausted
 *  attempts finalize FAILED (hot cache + durable). A user stop is never
 *  retried, and a successful run resets the attempt counter.
 *
 *  `exec` is an injection seam for tests (default: the real execute). */
export async function executeWithPolicy(
  deps: RuntimePorts,
  agent: RegisteredAgent,
  input: { threadId: string; model: string; tokenBudget?: number },
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
    }
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
