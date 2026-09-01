import type { AgentEvent } from './core/types.js';
import { resolveConfig } from './core/types.js';
import type {
  AgentCore,
  AgentHandle,
  AgentKind,
  RespondInput,
  RespondResult,
  RunJob,
  RuntimeOptions,
  RuntimePorts,
  ThreadSnapshot,
} from './ports/runtime.js';
import type { ThreadUsage } from './ports/runtime.js';
import { contextUsage } from './core/context.js';
import { createGenerateTextAgent, createStreamTextAgent } from './core/agent.js';
import * as adminReads from './core/admin.js';
import { bindStorage, type AgentRunState } from './core/state.js';
import { openDefaultAdminStore } from './admin/default.js';
import { reclaimIfOrphaned } from './core/reclaim.js';
import { respond } from './core/hitl.js';
import { deleteThread } from './core/deleteThread.js';

/** Bind the ports to the core behaviors (§3.3). This is the package's public
 *  entry point — the only place where anything is wired together. */
export async function setupAgentCore(opts: RuntimeOptions): Promise<AgentCore> {
  // Operational history is the platform's own (§2.9). Nothing configured means
  // SQLite on disk: history that survives a restart should be the default, not
  // something you have to remember to switch on.
  //
  // Opened here, eagerly, which is why this function is async: a store that
  // cannot be opened is a startup error you see immediately, rather than a
  // surprise on the first run — and losing run history silently looks exactly
  // like having no traffic.
  const admin = opts.admin ?? (await openDefaultAdminStore());

  const shared = {
    admin,
    bus: opts.bus,
    queue: opts.queue,
    kv: opts.kv,
    resolveModel: (modelName: string) => opts.resolveModel(modelName),
    config: resolveConfig(opts.config),
  };

  /** Ports for ONE run: the caller's storage with that run's state bound, so
   *  every query, insert and update their implementation makes can see it
   *  (§2.10). Called per entry point rather than once, because state belongs
   *  to a run and a runtime outlives many. */
  const scope = (state: AgentRunState = {}, runId?: string): RuntimePorts => ({
    ...shared,
    storage: bindStorage(opts.storage, { state, runId }),
  });

  // For reads that are not on behalf of any particular run.
  const deps = scope();

  // Handle registry — keyed by spec.name, resolved by the queue dispatch.
  const registry = new Map<string, AgentHandle>();
  // The first registered stream-text handle is the default for jobs that
  // omit `agent` (§5).
  let defaultAgent: string | null = null;

  const register = (
    name: string,
    kind: AgentKind,
    spec: import('./ports/runtime.js').StreamTextAgentSpec | import('./ports/runtime.js').GenerateTextAgentSpec,
  ): AgentHandle => {
    const handle: AgentHandle =
      kind === 'stream-text'
        ? createStreamTextAgent(scope, spec)
        : createGenerateTextAgent(scope, spec);
    registry.set(name, handle);
    return handle;
  };

  const core: AgentCore = {
    resolveModel: (modelName: string) => deps.resolveModel(modelName),

    listThreads: () => deps.storage.threads.list(),

    deleteThread: (threadId: string) => deleteThread(deps, threadId),

    getThreadSnapshot: async (threadId: string): Promise<ThreadSnapshot | null> => {
      const thread = await deps.storage.threads.get(threadId);
      if (!thread) return null;

      const messages = await deps.storage.messages.list(threadId, undefined);
      const runs = await deps.admin.runs.listByThread(threadId);
      const events = await deps.storage.events.listSince(threadId, -1);

      const lastEventSeq = events.at(-1)?.seq ?? -1;
      let activeEvents: AgentEvent[] = [];
      if (thread.state === 'RUNNING' || thread.state === 'WAITING_FOR_INPUT') {
        let boundary = -1;
        for (let index = events.length - 1; index >= 0; index -= 1) {
          const event = events[index];
          if (
            event.type === 'STATE_CHANGE' &&
            (event.payload as { state?: string } | null)?.state === 'RUNNING'
          ) {
            boundary = index;
            break;
          }
        }
        activeEvents = events.slice(Math.max(0, boundary));
      }

      return { thread, messages, runs, lastEventSeq, activeEvents };
    },

    admin: {
      overview: (range) => adminReads.overview(deps, range),
      listRuns: (filter) => adminReads.listRuns(deps, filter),
      stats: (range) => adminReads.runStats(deps, range),
      getRun: (runId: string) => adminReads.getRun(deps, runId),
      listRunsByThread: (threadId: string) => deps.admin.runs.listByThread(threadId),
      listSteps: (runId: string) => adminReads.listSteps(deps, runId),
    },

    getThreadUsage: async (threadId: string): Promise<ThreadUsage | null> => {
      const thread = await deps.storage.threads.get(threadId);
      if (!thread) return null;
      const [tokens, context] = await Promise.all([
        deps.storage.usage.total(threadId),
        contextUsage(deps, threadId, thread.model),
      ]);
      return { tokens, context, model: thread.model };
    },

    hitl: {
      respond: (input: RespondInput): Promise<RespondResult> => {
        // Heal an orphaned wait first — if reclamation claims the thread, the
        // response is rejected as late (§2.5)
        return (async () => {
          await reclaimIfOrphaned(deps, input.threadId);
          return respond(deps, input);
        })();
      },
      reclaimIfOrphaned: (threadId: string) => reclaimIfOrphaned(deps, threadId),
    },

    events: {
      since: (threadId: string, sinceSeq: number): Promise<AgentEvent[]> =>
        deps.storage.events.listSince(threadId, sinceSeq),
      subscribe: async (threadId: string, handler: (event: AgentEvent) => void) =>
        deps.bus.subscribe(threadId, handler),
    },

    createStreamTextAgent: (spec) => {
      const handle = register(spec.name, 'stream-text', spec);
      if (defaultAgent === null) defaultAgent = spec.name;
      return handle;
    },

    createGenerateTextAgent: (spec) => register(spec.name, 'generate-text', spec),

    getAgent: (name: string) => registry.get(name) ?? null,

    worker: {
      handleJob: async (job: RunJob) => {
        // Missing `agent` → the default handle (first registered stream-text)
        const agent =
          (job.agent ? registry.get(job.agent) : null) ??
          (defaultAgent ? registry.get(defaultAgent) : null);
        if (!agent) return { accepted: false, reason: 'unknown-agent' };

        // executeWithPolicy: run lock (idempotent under at-least-once
        // delivery, §3.4) + §2.8 failure policy — redrive < maxAttempts,
        // else finalize FAILED; a user stop is never retried.
        await agent.executeWithPolicy({
          threadId: job.threadId,
          // The dispatch's identity (§2.1) — without it the worker cannot tell
          // it has been replaced by a newer run, and a blocked job is dropped.
          runId: job.runId,
          // Carries the queue wait through to the run record (§2.9).
          enqueuedAt: job.enqueuedAt,
          // Rehydrated from the ticket: this worker never saw the caller (§2.10).
          state: job.state,
          model: job.model,
          tokenBudget: job.tokenBudget,
          providerOptions: job.providerOptions,
        });
        return { accepted: true };
      },
    },
  };
  return core;
}

