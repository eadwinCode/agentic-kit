import { streamText, tool } from 'ai';
import { z } from 'zod';
import type { RuntimePorts } from '../ports/runtime.js';
import { publish } from './engine.js';
import { calculateCost } from './billing.js';

export const MAX_SUBAGENT_DEPTH = 2; // main (0) → sub (1) → sub-sub (2)
export const MAX_CONCURRENT_SUBAGENTS = 3; // per run

export interface SubagentCtx {
  threadId: string;
  depth: number;      // 0 = called from the main agent
  sem: Semaphore;     // per-run concurrency cap
  ports: RuntimePorts; // the §3.2 ports bundle
}

/** Run-scoped semaphore: sibling subagents queue instead of running away (§2.7) */
export class Semaphore {
  private active = 0;
  private waiters: (() => void)[] = [];
  constructor(private readonly limit: number) {}

  async acquire(): Promise<() => void> {
    await new Promise<void>((resolve) => {
      if (this.active < this.limit) {
        this.active++;
        resolve();
      } else {
        this.waiters.push(() => {
          this.active++;
          resolve();
        });
      }
    });
    let released = false;
    return () => {
      if (released) return;
      released = true;
      this.active--;
      this.waiters.shift()?.();
    };
  }
}

export function spawnSubagentTool(ctx: SubagentCtx) {
  return tool({
    description: 'Delegates a self-contained task to a subagent with an isolated context',
    parameters: z.object({
      name: z.string().describe('Short name for the sub-task'),
      instructions: z
        .string()
        .describe('Complete, self-contained brief: goal, constraints, expected output format'),
      model: z.string().optional(),
    }),
    execute: async (args: { name: string; instructions: string; model?: string }, opts: { toolCallId?: string; abortSignal?: AbortSignal }): Promise<any> => {
      const { name, instructions, model } = args;
      if (ctx.depth >= ctx.ports.config.subagentMaxDepth) {
        return { error: `Max subagent depth (${ctx.ports.config.subagentMaxDepth}) reached` };
      }

      const release = await ctx.sem.acquire();
      try {
        const run = await ctx.ports.storage.runs.create(ctx.threadId, {
          name,
          model: model ?? 'gpt-4o',
          depth: ctx.depth + 1,
          state: 'RUNNING',
        });
        await publish(ctx.ports, ctx.threadId, 'SUBAGENT_STARTED', {
          agentId: run.id, name, depth: ctx.depth + 1,
        });

        try {
          const result = await runSubagent({
            threadId: ctx.threadId,
            depth: ctx.depth,
            sem: ctx.sem,
            ports: ctx.ports,
            agentId: run.id,
            name,
            instructions,
            model: model ?? 'gpt-4o',
            abortSignal: opts.abortSignal, // cancellation propagates from the parent (§2.7)
          });

          await ctx.ports.storage.runs.update(run.id, {
            state: 'COMPLETED',
            result: { text: result },
          });
          await publish(ctx.ports, ctx.threadId, 'SUBAGENT_COMPLETED', { agentId: run.id });

          // The parent receives a capped result, keeping its own context small (§2.6)
          return {
            agentId: run.id,
            result: result.slice(0, ctx.ports.config.subagentResultCapChars),
          };
        } catch (err) {
          const state =
            (await ctx.ports.kv.get(`agent:state:${ctx.threadId}`)) === 'CANCELLED'
              ? 'CANCELLED'
              : 'FAILED';
          await ctx.ports.storage.runs.update(run.id, { state });
          await publish(ctx.ports, ctx.threadId, 'SUBAGENT_FAILED', { agentId: run.id, state });
          throw err; // propagate: the parent step sees the failure / abort
        }
      } finally {
        release();
      }
    },
  });
}

async function runSubagent(
  opts: SubagentCtx & {
    agentId: string;
    name: string;
    instructions: string;
    model: string;
    abortSignal?: AbortSignal;
  },
): Promise<string> {
  const { threadId, depth, sem, ports, agentId, name, instructions, model, abortSignal } = opts;

  // The key that actually executes — requested model, falling back to gpt-4o.
  // Billing prices THIS key, not the requested one (§4).
  const resolved = model in ports.models ? model : 'gpt-4o';

  const result = streamText({
    model: ports.models[resolved] as any,
    // Isolated context: seeded with the brief only — never the parent history
    system: `You are the "${name}" subagent. Complete the task, then stop.`,
    prompt: instructions,
    abortSignal, // stop tears this down immediately (§2.7)
    maxSteps: ports.config.subagentMaxSteps,
    tools: {
      // Nesting up to subagentMaxDepth. Default toolset is restricted:
      // destructive tools (requiresConfirmation) are not included (§2.5, §2.7)
      spawnSubagent: spawnSubagentTool({
        threadId,
        depth: depth + 1,
        sem,
        ports,
      }),
    },
    onChunk: async ({ chunk }) => {
      // Namespaced into the shared thread event log → same multi-user pipeline (§2.2)
      await publish(ports, threadId, 'SUBAGENT_CHUNK', { agentId, chunk });
    },
    onFinish: async ({ usage }) => {
      // Billing attribution per subagent (§4) — unpriced models are marked
      const costUSD = calculateCost(ports, resolved, usage.promptTokens, usage.completionTokens);
      if (costUSD === null) {
        await publish(ports, threadId, 'BILLING_UNPRICED', { agentId, model: resolved });
      }
      await ports.storage.usage.record(threadId, {
        agentId,
        model: resolved,
        promptTokens: usage.promptTokens,
        completionTokens: usage.completionTokens,
        costUSD: costUSD ?? 0,
      });
    },
  });

  return result.text;
}
