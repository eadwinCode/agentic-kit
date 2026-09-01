import { generateText, streamText, tool } from 'ai';
import { z } from 'zod';
import type { SubagentsConfig } from './types.js';
import type { RuntimePorts } from '../ports/runtime.js';
import { publish } from './publish.js';
import { attributeTokens } from './usage.js';

export interface SubagentCtx {
  threadId: string;
  depth: number; // 0 = called from the main agent
  sem: Semaphore; // per-run concurrency cap
  ports: RuntimePorts; // the §3.2 ports bundle
  /** Delegation config carried from the parent's spec (§2.7): flavor,
   *  default model, and extra tools for every spawned child. */
  sub: SubagentsConfig;
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
    execute: async (
      args: { name: string; instructions: string; model?: string },
      opts: { toolCallId?: string; abortSignal?: AbortSignal },
    ): Promise<any> => {
      const { name, instructions, model } = args;
      const depth = ctx.depth + 1;
      if (depth > ctx.ports.config.subagentMaxDepth) {
        return { error: `Max subagent depth (${ctx.ports.config.subagentMaxDepth}) reached` };
      }

      const release = await ctx.sem.acquire();
      try {
        const run = await ctx.ports.storage.runs.create(ctx.threadId, {
          name,
          model: model ?? ctx.sub.model ?? 'gpt-4o',
          depth,
          state: 'RUNNING',
        });
        await publish(ctx.ports, ctx.threadId, 'SUBAGENT_STARTED', {
          agentId: run.id, name, depth,
        });

        try {
          const job = {
            agentId: run.id,
            name,
            instructions,
            model: model ?? ctx.sub.model ?? 'gpt-4o',
            depth,
            abortSignal: opts.abortSignal, // cancellation propagates from the parent (§2.7)
          };

          const result =
            ctx.sub.kind === 'generate-text'
              ? await runGenerateSubagent(ctx, job) // single completion, lifecycle events only
              : await runStreamSubagent(ctx, job); // live SUBAGENT_CHUNK fan-out (§2.2)

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

/** stream-text child: live SUBAGENT_CHUNK fan-out (§2.2) */
async function runStreamSubagent(
  ctx: SubagentCtx,
  job: {
    agentId: string;
    name: string;
    instructions: string;
    model: string;
    depth: number;
    abortSignal?: AbortSignal;
  },
): Promise<string> {
  const { threadId, depth, sem, ports, sub, agentId, name, instructions, model, abortSignal } = {
    ...ctx,
    ...job,
  };

  const result = streamText({
    model: (ports.resolveModel(model) || ports.resolveModel('gpt-4o')).instance(),
    // Isolated context: seeded with the brief only — never the parent history
    system: `You are the "${name}" subagent. Complete the task, then stop.`,
    prompt: instructions,
    abortSignal, // stop tears this down immediately (§2.7)
    maxSteps: ports.config.subagentMaxSteps,
    tools: {
      // Nesting up to subagentMaxDepth, inheriting the delegation config.
      // Default toolset is restricted: destructive tools are not included (§2.5, §2.7)
      spawnSubagent: spawnSubagentTool({ threadId, depth: depth + 1, sem, ports, sub }),
    },
    onChunk: async ({ chunk }) => {
      // Namespaced into the shared thread event log → same multi-user pipeline (§2.2)
      await publish(ports, threadId, 'SUBAGENT_CHUNK', { agentId, chunk });
    },
    onFinish: async ({ usage }) => {
      // Billing attribution per subagent (§4): total tokens used
      await ports.storage.usage.record(threadId, {
        agentId,
        ...attributeTokens(usage),
      });
    },
  });

  return result.text;
}

/** generate-text child: single completion, lifecycle events only (§2.7) */
async function runGenerateSubagent(
  ctx: SubagentCtx,
  job: {
    agentId: string;
    name: string;
    instructions: string;
    model: string;
    depth: number;
    abortSignal?: AbortSignal;
  },
): Promise<string> {
  const { threadId, depth, sem, ports, sub, agentId, name, instructions, model, abortSignal } = {
    ...ctx,
    ...job,
  };

  const result = await generateText({
    model: (ports.resolveModel(model) || ports.resolveModel('gpt-4o')).instance(),
    system: `You are the "${name}" subagent. Complete the task, then stop.`,
    prompt: instructions,
    abortSignal,
    maxSteps: ports.config.subagentMaxSteps,
    tools: {
      spawnSubagent: spawnSubagentTool({ threadId, depth: depth + 1, sem, ports, sub }),
    },
  });

  // Billing attribution per subagent (§4): total tokens used
  await ports.storage.usage.record(threadId, {
    agentId,
    ...attributeTokens(result.usage),
  });

  return result.text;
}
