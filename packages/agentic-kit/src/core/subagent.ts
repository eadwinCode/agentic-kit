import { randomUUID } from 'node:crypto';
import { tool } from 'ai';
import { z } from 'zod';
import type {
  NestedDescriptor,
  ProviderOptions,
  ResumeInfo,
  RunRecord,
  SubagentsConfig,
} from './types.js';
import type { RuntimePorts } from '../ports/runtime.js';
import type { RegisteredAgent } from './agent.js';
import { publish } from './publish.js';
import { HITL_PARKED, withHitl, type HitlFrame } from './hitl.js';
import { withRunState, type AgentRunState } from './state.js';
import { markPromptCaching } from './cache.js';
import { runLoop, type LoopOutcome, type RunLedger } from './loop.js';

/** Everything a nested run needs from the run that spawned it (§2.7). The
 *  thread, lock, run id, abort signal and token ledger are the parent's; the
 *  message stream, step ceiling and toolset are the child's own. */
export interface SubagentCtx {
  threadId: string;
  depth: number; // 0 = called from the main agent
  sem: Semaphore; // per-run concurrency cap
  ports: RuntimePorts; // the §3.2 ports bundle
  /** Delegation config carried from the parent's spec (§2.7): flavor,
   *  default model, and extra tools for every spawned child. */
  sub: SubagentsConfig;
  /** The registered agent whose generation args every nested run inherits
   *  (§3.1) — its `system` and `tools` are overridden per child. */
  agent: RegisteredAgent;
  /** The run-wide token ledger (§2.7): a child's spend counts against the
   *  same safety cap the main agent is checked against. */
  ledger: RunLedger;
  /** Dispatch ticket persisted with any park raised beneath here (§2.5). */
  resume: ResumeInfo;
  /** The stream this spawner writes to — `null` when it is the main agent. */
  agentId: string | null;
  /** Calls already waiting on an approval above this level, innermost first. */
  frames: HitlFrame[];
  /** This spawner's own descriptor — absent when it is the main agent. Goes
   *  onto the frame it pushes, so an unwind can re-enter it (§2.7). */
  descriptor?: NestedDescriptor;
  tokenBudget?: number;
  providerOptions?: ProviderOptions;
  abortSignal?: AbortSignal;
  /** The run's state, handed down unchanged (§2.10). */
  state?: AgentRunState;
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
        // A nested run is a run (§2.9): same table, distinguished by depth and
        // a parent. Its id is also the agentId its messages and events carry.
        const run = await ctx.ports.admin.runs.start({
          id: randomUUID(),
          threadId: ctx.threadId,
          // The spawner: another nested run, or the dispatched run itself.
          parentRunId: ctx.agentId ?? ctx.resume.runId ?? null,
          depth,
          agent: name,
          model: model ?? ctx.sub.model ?? 'gpt-4o',
          // A nested run's "prompt" is the brief it was delegated (§2.7).
          ...(ctx.ports.config.recordPayloads
            ? {
                prompt:
                  instructions.length > ctx.ports.config.payloadCapChars
                    ? `${instructions.slice(0, ctx.ports.config.payloadCapChars)}…`
                    : instructions,
                runState: ctx.state ?? null,
              }
            : {}),
        });
        await publish(ctx.ports, ctx.threadId, 'SUBAGENT_STARTED', {
          agentId: run.id, name, depth,
        });

        const descriptor: NestedDescriptor = {
          agentId: run.id,
          name,
          model: model ?? ctx.sub.model ?? 'gpt-4o',
          depth,
        };

        try {
          const outcome = await runNestedAgent(
            ctx,
            descriptor,
            instructions,
            opts.abortSignal ?? ctx.abortSignal,
            // This call is now the innermost thing waiting on any approval the
            // child raises (§2.7).
            [
              {
                agentId: ctx.agentId,
                toolCallId: opts.toolCallId ?? run.id,
                ...(ctx.descriptor ? { nested: ctx.descriptor } : {}),
              },
              ...ctx.frames,
            ],
          );

          if (outcome.parked) {
            // The child is suspended, not finished: leave its SubagentRun
            // RUNNING and hand the parent the sentinel so its segment ends
            // too (§2.5). The child is re-entered on approval, from its own
            // persisted turns — it never restarts.
            return { [HITL_PARKED]: opts.toolCallId ?? run.id };
          }

          await closeNested(ctx, run, outcome, {
            state: 'COMPLETED',
            result: { text: outcome.text },
          });
          await publish(ctx.ports, ctx.threadId, 'SUBAGENT_COMPLETED', { agentId: run.id });

          // The parent receives a capped result, keeping its own context small (§2.6)
          return {
            agentId: run.id,
            result: outcome.text.slice(0, ctx.ports.config.subagentResultCapChars),
          };
        } catch (err) {
          const cancelled =
            (await ctx.ports.kv.get(`agent:state:${ctx.threadId}`)) === 'CANCELLED';
          const state = cancelled ? 'CANCELLED' : 'FAILED';
          const message = err instanceof Error ? err.message : String(err);
          await closeNested(ctx, run, null, { state, error: message });
          // Carry the reason: a bare state tells an operator a child died but
          // not why, and a nested run's failure is otherwise invisible.
          await publish(ctx.ports, ctx.threadId, 'SUBAGENT_FAILED', {
            agentId: run.id,
            state,
            error: message,
          });

          // A user stop tears the whole run down (§2.1), so that one keeps
          // propagating.
          if (cancelled) throw err;

          // Anything else is reported TO THE PARENT as the delegation's
          // result, the same way an approved tool's failure is reported to the
          // model rather than thrown (§2.5): a delegated task that went wrong
          // is news the agent can act on — retry, try another way, tell the
          // user — not a reason to kill the run it was part of.
          return { agentId: run.id, error: message };
        }
      } finally {
        release();
      }
    },
  });
}

/** Close a nested run's record with the same detail a dispatched run gets
 *  (§2.9): how it ended, how long it took, what it cost. */
/** Stamp a nested run's prompt when caching is on, exactly as the main path
 *  does. */
function maybeCache(ports: RuntimePorts, messages: any[]): any[] {
  return ports.config.promptCaching ? markPromptCaching(messages) : messages;
}

async function closeNested(
  ctx: SubagentCtx,
  run: RunRecord,
  outcome: LoopOutcome | null,
  end: { state: RunRecord['state']; result?: unknown; error?: string },
): Promise<void> {
  const endedAt = new Date();
  await ctx.ports.admin.runs.patch(run.id, {
    ...end,
    endedAt,
    durationMs: endedAt.getTime() - new Date(run.startedAt).getTime(),
    ...(outcome
      ? {
          steps: outcome.steps,
          inputTokens: outcome.attribution.inputTokens,
          cachedInputTokens: outcome.attribution.cachedInputTokens,
          outputTokens: outcome.attribution.outputTokens,
          totalTokens: outcome.attribution.totalTokens,
        }
      : {}),
  });
}

/** The toolset a nested run sees (§2.7): the delegation config's extra tools,
 *  HITL-wrapped exactly like the parent's, plus nesting while depth allows.
 *  Default is `spawnSubagent` alone — destructive tools reach a child only
 *  when a workflow grants them. */
function nestedTools(
  ctx: SubagentCtx,
  d: NestedDescriptor,
  frames: HitlFrame[],
  abortSignal?: AbortSignal,
): Record<string, any> {
  const raw: Record<string, any> = {
    ...(ctx.sub.tools ?? {}),
    spawnSubagent: spawnSubagentTool({
      ...ctx,
      depth: d.depth,
      agentId: d.agentId,
      frames,
      descriptor: d,
      abortSignal,
    }),
  };
  // A nested run's tools see the same state as its parent's (§2.10).
  return withRunState(
    withHitl(ctx.ports, ctx.threadId, raw, {
      resume: ctx.resume,
      agentId: d.agentId,
      frames,
      nested: d,
    }),
    ctx.state ?? {},
  );
}

/** The delegation tool lets the MODEL name the child's model, so an unknown
 *  registry key is ordinary bad input rather than a failure. `resolveModel`
 *  throws on one (§3.3) — a `||` fallback can never catch that — so the child
 *  falls back to the model its parent is already running on, which is
 *  resolvable by construction. */
function resolveNestedModel(ctx: SubagentCtx, name: string) {
  try {
    return ctx.ports.resolveModel(name);
  } catch {
    return ctx.ports.resolveModel(ctx.resume.model);
  }
}

/** Run — or RE-ENTER — a nested agent (§2.7).
 *
 *  Its turns live in the thread's message log under its own `agentId`, so a
 *  child that parked is resumed from exactly where it stopped rather than
 *  replayed from its brief. Replaying an LLM call is not a safe substitute:
 *  the model can take a different path and never make the call the human
 *  approved, and it re-pays for everything before the park. */
export async function runNestedAgent(
  ctx: SubagentCtx,
  d: NestedDescriptor,
  /** Seeds the stream on first entry; ignored once the child has turns. */
  instructions: string | null,
  abortSignal: AbortSignal | undefined,
  frames: HitlFrame[],
): Promise<LoopOutcome> {
  const { ports, threadId } = ctx;

  const persisted = await ports.storage.messages.list(threadId, { agentId: d.agentId });
  if (persisted.length === 0) {
    if (instructions === null) {
      throw new Error(`Nested run ${d.agentId} has no turns and no brief to seed from`);
    }
    // Isolated context (§2.7): the brief is the only input — parent history
    // is never forwarded.
    persisted.push(
      await ports.storage.messages.append(threadId, {
        role: 'user',
        content: instructions,
        agentId: d.agentId,
      }),
    );
  }

  const outcome = await runLoop(
    ports,
    ctx.agent,
    threadId,
    {
      agentId: d.agentId,
      // Its OWN run id, not its parent's: a nested run is a run (§2.7, §2.9),
      // so its steps belong to its own record. Attributing them upward mixed
      // two agents' steps into one timeline and made the indexes collide.
      runId: d.agentId,
      kind: ctx.sub.kind ?? 'stream-text',
      model: resolveNestedModel(ctx, d.model).instance(),
      // Stamped like the parent's (§2.6). A nested run re-sends its whole
      // brief and history on every step, so it is exactly the shape caching
      // is for — it was the one prompt in the system going out unstamped.
      messages: maybeCache(
        ports,
        persisted.map((m) => ({ role: m.role, content: m.content }) as any),
      ),
      tools: nestedTools(ctx, d, frames, abortSignal),
      maxSteps: ports.config.subagentMaxSteps,
      abortSignal: abortSignal ?? new AbortController().signal,
      providerOptions: ctx.providerOptions,
      tokenBudget: ctx.tokenBudget,
      system: `You are the "${d.name}" subagent. Complete the task, then stop.`,
      cacheSystemPrompt: ports.config.promptCaching,
      onChunk: async (chunk) => {
        // Namespaced into the shared thread event log → same multi-user pipeline (§2.2)
        await publish(ports, threadId, 'SUBAGENT_CHUNK', { agentId: d.agentId, chunk });
      },
    },
    ctx.ledger,
  );

  // Billing attribution per subagent (§4); the run-wide ledger was already
  // advanced inside the loop.
  await ports.storage.usage.record(threadId, { agentId: d.agentId, ...outcome.attribution });

  return outcome;
}
