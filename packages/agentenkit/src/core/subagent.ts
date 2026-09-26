import { randomUUID } from 'node:crypto';
import { tool } from 'ai';
import { z } from 'zod';
import type {
  NestedDescriptor,
  ProviderOptions,
  ResolvedModel,
  ResumeInfo,
  RunRecord,
  SubagentsConfig,
} from './types.js';
import { wireId, type SubagentProfile } from './types.js';
import type { RuntimePorts } from '../ports/runtime.js';
import type { RegisteredAgent } from './agent.js';
import { publish, withPublishEvent } from './publish.js';
import { HITL_PARKED, RunStoppedError, withHitl, type HitlFrame, type ParkBox } from './hitl.js';
import { withRunState, type AgentRunState } from './state.js';
import { withToolRun } from './builtin/run.js';
import { markPromptCaching } from './cache.js';
import { promptMessages, repairDanglingToolCalls } from './messages.js';
import { runLoop, type LoopOutcome, type RunLedger } from './loop.js';

/** Everything a nested run needs from the run that spawned it (§2.7). The
 *  thread, lock, run id, abort signal and token ledger are the parent's; the
 *  message stream, step ceiling and toolset are the child's own. */
export interface SubagentCtx {
  threadId: string;
  depth: number; // 0 = called from the main agent
  /** This run's subagent cap (§2.7): see RunSlots. */
  slots: RunSlots;
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
  /** The run's money cap, shared with the parent (§4). */
  costBudgetMicros?: number;
  /** The DISPATCHED run every call beneath here is billed to, so one run's
   *  bill is one query however deep the delegation went (§4). */
  billingRunId?: string;
  providerOptions?: ProviderOptions;
  abortSignal?: AbortSignal;
  /** True once the run lock is gone (see LoopInput.fenced). */
  fenced?: () => boolean;
  /** The segment's park box, shared by every depth (see ParkBox). */
  parks?: ParkBox;
  /** Calls whose tool failed, shared by every depth (see chunkPayload). */
  toolErrors?: Map<string, string>;
  /** The run's state, handed down unchanged (§2.10). */
  state?: AgentRunState;
}

/** A concurrency cap: sibling subagents queue instead of running away (§2.7).
 *  See RunSlots for how a run uses them. */
export class Semaphore {
  private active = 0;
  private waiters: (() => void)[] = [];
  constructor(private readonly limit: number) {}

  /** Take a slot, waiting for one. A wait the signal aborts gives up with the
   *  signal's reason instead of waiting on for ever. */
  async acquire(signal?: AbortSignal): Promise<() => void> {
    await new Promise<void>((resolve, reject) => {
      if (signal?.aborted) {
        reject(signal.reason ?? new Error('aborted'));
        return;
      }
      if (this.active < Math.max(this.limit, 1)) {
        this.active++;
        resolve();
        return;
      }
      const take = () => {
        signal?.removeEventListener('abort', giveUp);
        this.active++;
        resolve();
      };
      const giveUp = () => {
        this.waiters = this.waiters.filter((w) => w !== take);
        reject(signal?.reason ?? new Error('aborted'));
      };
      signal?.addEventListener('abort', giveUp, { once: true });
      this.waiters.push(take);
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

/** One run's subagent cap (§2.7): `subagentMaxConcurrent` children at a time at
 *  each depth. Made per run, so one run's children never wait on another
 *  run's. And each depth has slots of its own: a parent holds its slot while
 *  its child runs, so if parent and child shared one pool, a full level of
 *  parents would each wait for a slot only a finished child can free, and
 *  never finish. */
export class RunSlots {
  private byDepth = new Map<number, Semaphore>();
  constructor(private readonly limit: number) {}

  acquire(depth: number, signal?: AbortSignal): Promise<() => void> {
    let sem = this.byDepth.get(depth);
    if (!sem) {
      sem = new Semaphore(this.limit);
      this.byDepth.set(depth, sem);
    }
    return sem.acquire(signal);
  }
}

/** A named specialist (§2.7), or undefined when the config has no profiles
 *  or none by that name. */
function profileFor(ctx: SubagentCtx, name: string | undefined): SubagentProfile | undefined {
  const profiles = ctx.sub.profiles;
  return name && profiles && Object.hasOwn(profiles, name) ? profiles[name] : undefined;
}

/** The unwrapped toolset a nested run owns: its profile's when it has one,
 *  the shared delegation tools otherwise. The resolved park executes the
 *  approved tool from here. */
export function nestedRawTools(ctx: SubagentCtx, d: NestedDescriptor | undefined): Record<string, any> {
  return ((d && profileFor(ctx, d.name)?.tools) ?? ctx.sub.tools ?? {}) as Record<string, any>;
}

function nestedModelName(ctx: SubagentCtx, profile: SubagentProfile | undefined, requested?: string): string {
  return requested || profile?.model || ctx.sub.model || 'gpt-4o';
}

/** The specialists, sorted, for the tool's description and its errors. */
function profileNames(ctx: SubagentCtx): string[] {
  return Object.keys(ctx.sub.profiles ?? {}).sort();
}

/** The delegation tool's description. With profiles, the model is told
 *  exactly who it can delegate to. */
function spawnDescription(ctx: SubagentCtx): string {
  const desc = 'Delegates a self-contained task to a subagent with an isolated context';
  const names = profileNames(ctx);
  if (names.length === 0) return desc;
  const list = names
    .map((name) => {
      const d = ctx.sub.profiles![name]!.description;
      return d ? `${name} (${d})` : name;
    })
    .join('; ');
  return `${desc}. name MUST be one of the available subagents: ${list}`;
}

export function spawnSubagentTool(ctx: SubagentCtx) {
  return tool({
    description: spawnDescription(ctx),
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
      // With profiles, an unknown name is ordinary bad input reported to the
      // model, never a crash (§2.7).
      const profile = profileFor(ctx, name);
      if (profileNames(ctx).length > 0 && !profile) {
        return { error: `Unknown subagent ${JSON.stringify(name)}; use one of: ${profileNames(ctx).join(', ')}` };
      }

      const release = await ctx.slots.acquire(depth, opts.abortSignal ?? ctx.abortSignal);
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
          model: nestedModelName(ctx, profile, model),
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
          model: nestedModelName(ctx, profile, model),
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

          // Stopped or cut short, the child did not finish: its partial text
          // is not a result. The catch below records which.
          if (outcome.aborted) throw new RunStoppedError(new Error('stopped'));
          if (outcome.interrupted) {
            throw new Error(`step ${outcome.steps + 1} ended without a finish`);
          }

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
            err instanceof RunStoppedError ||
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
          if (cancelled) throw new RunStoppedError(err);

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

export async function closeNested(
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
    ...nestedRawTools(ctx, d),
    spawnSubagent: spawnSubagentTool({
      ...ctx,
      depth: d.depth,
      agentId: d.agentId,
      frames,
      descriptor: d,
      abortSignal,
    }),
  };
  // A nested run's tools see the same state as its parent's (§2.10), and
  // bill the parent's run on the shared ledger.
  return withToolRun(
    withRunState(
      withPublishEvent(
        ctx.ports,
        ctx.threadId,
        withHitl(ctx.ports, ctx.threadId, raw, {
          resume: ctx.resume,
          agentId: d.agentId,
          frames,
          nested: d,
          parks: ctx.parks,
          toolErrors: ctx.toolErrors,
        }),
      ),
      ctx.state ?? {},
    ),
    {
      deps: ctx.ports,
      threadId: ctx.threadId,
      ...(ctx.billingRunId ? { runId: ctx.billingRunId } : {}),
      agentId: d.agentId,
      agentName: d.name,
      ...(ctx.state ? { state: ctx.state } : {}),
      ledger: ctx.ledger,
    },
  );
}

/** The delegation tool lets the MODEL name the child's model, so an unknown
 *  registry key is ordinary bad input rather than a failure. `resolveModel`
 *  throws on one (§3.3) — a `||` fallback can never catch that — so the child
 *  falls back to the model its parent is already running on, which is
 *  resolvable by construction. */
/** The registry key comes back with the model: pricing is keyed by the key
 *  that was actually resolved, not the one the delegation asked for. */
function resolveNestedModel(
  ctx: SubagentCtx,
  name: string,
): { resolved: ResolvedModel; modelKey: string } {
  try {
    return { resolved: ctx.ports.resolveModel(name), modelKey: name };
  } catch {
    return { resolved: ctx.ports.resolveModel(ctx.resume.model), modelKey: ctx.resume.model };
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

  const { resolved, modelKey } = resolveNestedModel(ctx, d.model);
  // A profile brings its own persona and step cap (§2.7); the descriptor
  // carries the name, so a re-entry after an approval finds the same one.
  const profile = profileFor(ctx, d.name);
  let maxSteps = ports.config.subagentMaxSteps;
  if (profile?.maxSteps && profile.maxSteps > 0 && profile.maxSteps < maxSteps) maxSteps = profile.maxSteps;
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
      model: resolved.instance(),
      // Stamped like the parent's (§2.6). A nested run re-sends its whole
      // brief and history on every step, so it is exactly the shape caching
      // is for — it was the one prompt in the system going out unstamped.
      messages: maybeCache(
        ports,
        repairDanglingToolCalls(promptMessages(persisted) as any[]),
      ),
      tools: nestedTools(ctx, d, frames, abortSignal),
      maxSteps,
      abortSignal: abortSignal ?? new AbortController().signal,
      fenced: ctx.fenced,
      providerOptions: ctx.providerOptions,
      tokenBudget: ctx.tokenBudget,
      // Money is capped and billed at the RUN, not per child (§2.7, §4): the
      // cap is the parent's, and every call a child makes lands on the
      // parent's bill under its own agentId.
      costBudgetMicros: ctx.costBudgetMicros,
      billingRunId: ctx.billingRunId,
      modelKey,
      modelId: wireId(resolved, modelKey),
      agentName: d.name,
      system: profile?.system || `You are the "${d.name}" subagent. Complete the task, then stop.`,
      ...(profile?.systemFn ? { systemFn: profile.systemFn } : {}),
      ...(profile?.prepareStep ? { prepareStep: profile.prepareStep } : {}),
      state: ctx.state ?? {},
      cacheSystemPrompt: ports.config.promptCaching,
      toolErrors: ctx.toolErrors,
      publishChunk: async (chunk) => {
        // Namespaced into the shared thread event log → same multi-user pipeline (§2.2)
        await publish(ports, threadId, 'SUBAGENT_CHUNK', { agentId: d.agentId, chunk });
      },
    },
    ctx.ledger,
  );

  // Nothing to bill here: every call the child made recorded and priced its
  // own row as it happened, tagged with this child's agentId (§4). The
  // run-wide ledger was advanced inside the loop too.
  return outcome;
}
