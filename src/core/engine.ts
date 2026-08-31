import { streamText, tool } from 'ai';
import { randomUUID } from 'node:crypto';
import { z } from 'zod';
import type { RuntimePorts } from '../ports/runtime.js';
import type { ExecutionState } from './types.js';
import { compactContext } from './context.js';
import { calculateCost } from './billing.js';
import { MAX_CONCURRENT_SUBAGENTS, Semaphore, spawnSubagentTool } from './subagent.js';
import { suspendForApproval, type SuspendInput } from './hitl.js';
import { publish } from './publish.js';

export { publish, publishNotice } from './publish.js';
export { calculateCost } from './billing.js';

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
 *  and a crashed worker's lock expires instead of blocking forever (§3.4). */
export async function execute(
  deps: RuntimePorts,
  input: { threadId: string; model: string },
): Promise<void> {
  const { threadId, model } = input;
  const abort = new AbortController();

  const locked = await deps.kv.set(runLockKey(threadId), randomUUID(), {
    onlyIfNotExists: true,
    exSeconds: deps.config.runLockLeaseSeconds,
  });
  if (!locked) return; // another worker owns this thread — at-least-once dispatch (§2.8)

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
    // Durable compaction pass — history always fits the model budget (§2.6)
    const history = await compactContext(deps, threadId, model);

    const result = streamText({
      model: (deps.models[model] || deps.models['gpt-4o']) as any,
      messages: history.map(
        (m) => ({ role: m.role, content: m.content }) as any,
      ),
      abortSignal: abort.signal,
      maxSteps: deps.config.maxSteps,
      tools: withHitl(deps, threadId, {
        executeTask: tool({
          description: 'Executes a long running task',
          parameters: z.object({ stepName: z.string() }),
          execute: async ({ stepName }, opts) => {
            // Checkpoint: stop takes effect here even without the signal
            if (opts?.abortSignal?.aborted) throw new Error('EXECUTION_CANCELLED');
            return { status: 'SUCCESS', result: `Completed ${stepName}` };
          },
        }),
        sendEmail: markRequiresConfirmation(
          tool({
            description: 'Sends an email (destructive — requires user approval)',
            parameters: z.object({
              to: z.string().email(),
              subject: z.string(),
              body: z.string(),
            }),
            execute: async ({ to, subject, body }) => ({ status: 'SENT', to, subject, body }),
          }),
        ),
        spawnSubagent: spawnSubagentTool({
          threadId,
          depth: 0,
          sem: new Semaphore(deps.config.subagentMaxConcurrent),
          ports: deps,
        }),
      }),
      onChunk: async ({ chunk }) => {
        // One canonical path for every client: durable log + live Pub/Sub (§2.1, §2.2)
        await publish(deps, threadId, 'CHUNK', chunk);
      },
      onFinish: async ({ usage, finishReason, response }) => {
        // Persist the completed assistant turn(s) — including tool calls and
        // tool results — BEFORE the state transition, so redrives, HITL
        // resumes, and replay always see a valid history (§2.2, §2.8)
        for (const message of response.messages) {
          await deps.storage.messages.append(threadId, {
            role: message.role,
            content: message.content,
          });
        }

        const finalState: ExecutionState = abort.signal.aborted ? 'CANCELLED' : 'COMPLETED';
        const stopReason = abort.signal.aborted
          ? 'cancelled'
          : finishReason === 'tool-calls'
            ? 'max_steps' // safety cap hit (§2.1)
            : 'completed';

        // Unpriced models are marked, never silently mispriced (§4)
        const costUSD = calculateCost(deps, model, usage.promptTokens, usage.completionTokens);
        if (costUSD === null) {
          await publish(deps, threadId, 'BILLING_UNPRICED', {
            model,
            promptTokens: usage.promptTokens,
            completionTokens: usage.completionTokens,
          });
        }
        await deps.storage.usage.record(threadId, {
          model,
          promptTokens: usage.promptTokens,
          completionTokens: usage.completionTokens,
          costUSD: costUSD ?? 0,
        });
        await deps.kv.set(`agent:state:${threadId}`, finalState);
        await deps.storage.threads.setState(threadId, finalState);
        await publish(deps, threadId, 'STATE_CHANGE', { state: finalState, stopReason });
      },
    });

    await result.text; // keep the worker invocation alive until the stream drains
  } finally {
    clearInterval(controlPoll);
    await deps.kv.del(runLockKey(threadId)); // release — success, failure, or stop
  }
}

/** §2.8 failure policy: transient errors redrive through the queue; exhausted
 *  attempts finalize FAILED (hot cache + durable). A user stop is never
 *  retried, and a successful run resets the attempt counter. */
export async function executeWithPolicy(
  deps: RuntimePorts,
  input: { threadId: string; model: string },
  policy?: { maxAttempts?: number },
): Promise<void> {
  const maxAttempts = policy?.maxAttempts ?? deps.config.runMaxAttempts;
  try {
    await execute(deps, input);
    // Success resets the retry budget — past failures must not count forever
    await deps.kv.del(`agent:attempts:${input.threadId}`);
  } catch (err) {
    // A user stop already finalized the thread — never retry a stop
    if ((await deps.kv.get(`agent:state:${input.threadId}`)) === 'CANCELLED') return;

    const attempts = await deps.kv.incr(`agent:attempts:${input.threadId}`);
    if (attempts < maxAttempts) {
      return deps.queue.enqueue({ threadId: input.threadId, model: input.model });
    }

    // Attempts exhausted: finalize FAILED on BOTH the hot cache and durable
    // truth, or subsequent runs would still treat the thread as active (§2.1)
    await deps.kv.set(`agent:state:${input.threadId}`, 'FAILED');
    await deps.storage.threads.setState(input.threadId, 'FAILED');
    await publish(deps, input.threadId, 'STATE_CHANGE', { state: 'FAILED' });
    await deps.kv.del(`agent:attempts:${input.threadId}`);
  }
}
