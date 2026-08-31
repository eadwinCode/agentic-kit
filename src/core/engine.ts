import { streamText, tool } from 'ai';
import { randomUUID } from 'node:crypto';
import { z } from 'zod';
import type { RuntimePorts } from '../ports/runtime.js';
import type { ExecutionState } from './types.js';
import { compactContext } from './context.js';
import { MAX_CONCURRENT_SUBAGENTS, Semaphore, spawnSubagentTool } from './subagent.js';
import { suspendForApproval, type SuspendInput } from './hitl.js';
import { publish } from './publish.js';

export { publish, publishNotice } from './publish.js';

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
 *  queue (§2.8) and may outlive any HTTP response. */
export async function execute(
  deps: RuntimePorts,
  input: { threadId: string; model: string },
): Promise<void> {
  const { threadId, model } = input;
  const abort = new AbortController();

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
      onFinish: async ({ usage, finishReason }) => {
        const finalState: ExecutionState = abort.signal.aborted ? 'CANCELLED' : 'COMPLETED';
        const stopReason = abort.signal.aborted
          ? 'cancelled'
          : finishReason === 'tool-calls'
            ? 'max_steps' // safety cap hit (§2.1)
            : 'completed';

        await deps.storage.usage.record(threadId, {
          model,
          promptTokens: usage.promptTokens,
          completionTokens: usage.completionTokens,
          costUSD: calculateCost(model, usage.promptTokens, usage.completionTokens),
        });
        await deps.kv.set(`agent:state:${threadId}`, finalState);
        await deps.storage.threads.setState(threadId, finalState);
        await publish(deps, threadId, 'STATE_CHANGE', { state: finalState, stopReason });
      },
    });

    await result.text; // keep the worker invocation alive until the stream drains
  } finally {
    clearInterval(controlPoll);
  }
}

/** §2.8 failure policy: transient errors redrive through the queue; exhausted
 *  attempts finalize FAILED. A user stop is never retried. */
export async function executeWithPolicy(
  deps: RuntimePorts,
  input: { threadId: string; model: string },
  policy?: { maxAttempts?: number },
): Promise<void> {
  const maxAttempts = policy?.maxAttempts ?? deps.config.runMaxAttempts;
  try {
    await execute(deps, input);
  } catch (err) {
    // A user stop already finalized the thread — never retry a stop
    if ((await deps.kv.get(`agent:state:${input.threadId}`)) === 'CANCELLED') return;

    const attempts = await deps.kv.incr(`agent:attempts:${input.threadId}`);
    if (attempts < maxAttempts) {
      return deps.queue.enqueue({ threadId: input.threadId, model: input.model });
    }

    await deps.storage.threads.setState(input.threadId, 'FAILED');
    await publish(deps, input.threadId, 'STATE_CHANGE', { state: 'FAILED' });
    await deps.kv.del(`agent:attempts:${input.threadId}`);
  }
}

function calculateCost(model: string, promptTokens: number, completionTokens: number): number {
  // Simplified pricing calculation logic (§4) — override via billing hooks if needed
  const rates: Record<string, { prompt: number; completion: number }> = {
    'gpt-4o': { prompt: 0.0000025, completion: 0.00001 },
    'gpt-4o-mini': { prompt: 0.00000015, completion: 0.0000006 },
    'claude-3-5-sonnet': { prompt: 0.000003, completion: 0.000015 },
  };
  const rate = rates[model] ?? rates['gpt-4o'];
  return promptTokens * rate.prompt + completionTokens * rate.completion;
}
