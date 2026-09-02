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
import { followEvents, toSseStream } from './core/follow.js';
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

    // Reads take the run state too (§2.10). A run carries it on its ticket,
    // but a read has no ticket to carry — so a tenant-scoped Storage would see
    // an empty context here and either return nothing or, worse, everything.
    listThreads: (state) => scope(state).storage.threads.list(),

    deleteThread: (threadId: string, state) => deleteThread(scope(state), threadId),

    getThreadSnapshot: async (
      threadId: string,
      state?: AgentRunState,
    ): Promise<ThreadSnapshot | null> => {
      const deps = scope(state);
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
        const active = events.slice(Math.max(0, boundary));

        // Everything up to the last committed step is ALREADY in `messages`
        // (§2.2). Replaying those chunks too would render each finished step's
        // text twice — once from the durable message, once from the stream
        // that produced it. Only the in-flight step's chunks are missing from
        // durable history, so only those are transient.
        //
        // Chunks alone: a park (INPUT_REQUIRED) is published DURING the step,
        // before its messages commit, so slicing the whole window by this
        // boundary would drop the very approval a reconnecting client needs.
        const lastCommitted = active.reduce(
          (seq, e) => (e.type === 'STEP_COMMITTED' ? e.seq : seq),
          -1,
        );
        const isStream = (type: string) => type === 'CHUNK' || type === 'SUBAGENT_CHUNK';
        activeEvents = active.filter(
          (e) => !(isStream(e.type) && e.seq !== 0 && e.seq <= lastCommitted),
        );
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
      listThreads: (filter) => adminReads.listThreads(deps, filter),
      getThread: (threadId: string) => adminReads.getThread(deps, threadId),
    },

    getThreadUsage: async (
      threadId: string,
      state?: AgentRunState,
    ): Promise<ThreadUsage | null> => {
      const deps = scope(state);
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
        // Scoped like the run it resumes: answering an approval reads and
        // writes the caller's rows (§2.10).
        const scoped = scope(input.state);
        // Heal an orphaned wait first — if reclamation claims the thread, the
        // response is rejected as late (§2.5)
        return (async () => {
          await reclaimIfOrphaned(scoped, input.threadId);
          return respond(scoped, input);
        })();
      },
      reclaimIfOrphaned: (threadId: string, state?: AgentRunState) =>
        reclaimIfOrphaned(scope(state), threadId),
    },

    events: {
      since: (threadId: string, sinceSeq: number, state?: AgentRunState): Promise<AgentEvent[]> =>
        scope(state).storage.events.listSince(threadId, sinceSeq),
      subscribe: async (threadId: string, handler: (event: AgentEvent) => void) =>
        deps.bus.subscribe(threadId, handler),
      // The replay-then-tail dance lives here rather than in every route
      // handler: subscribe first, never emit at or below the cursor, and let a
      // seq-0 notice through without moving it (§2.2).
      follow: (threadId, options = {}) =>
        followEvents(scope(options.state), threadId, options),
      sse: (threadId, options = {}) =>
        toSseStream(followEvents(scope(options.state), threadId, options), options),
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

