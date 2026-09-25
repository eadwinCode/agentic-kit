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
import type { ReclaimReport, ThreadUsage } from './ports/runtime.js';
import type { RunFilter } from './ports/admin.js';
import type { RunRecord } from './core/types.js';
import type { RegisteredAgent } from './core/agent.js';
import { settleLate } from './core/settle.js';
import { failLostRun } from './core/engine.js';
import { contextUsage } from './core/context.js';
import { createGenerateTextAgent, createStreamTextAgent } from './core/agent.js';
import * as adminReads from './core/admin.js';
import { bindStorage, type AgentRunState } from './core/state.js';
import { threadSnapshot } from './core/snapshot.js';
import { MemoryRunStreams } from './adapters/memory.js';
import type { Logger } from './ports/runtime.js';
import { currentRunId } from './core/keys.js';
import { followEvents, followThread, parseCursor, toFollowSse, type ThreadCursor } from './core/follow.js';
import { openDefaultAdminStore } from './admin/default.js';
import { reclaimIfOrphaned } from './core/reclaim.js';
import { respond } from './core/hitl.js';
import { deleteThread } from './core/deleteThread.js';
import { publishEvent, ACTIVE_STATES } from './core/publish.js';

/** A job named an agent this process does not have. Thrown, not answered, so
 *  a queue that retries on failure keeps the job. */
export class UnknownAgentError extends Error {
  constructor(readonly agent?: string) {
    super(`no agent registered for this job: ${JSON.stringify(agent ?? '')}`);
    this.name = 'UnknownAgentError';
  }
}

/** The cursor a follow starts from: a ThreadCursor or its wire string, or a
 *  bare record seq (`since`) as older clients send. */
function cursorOf(options: { cursor?: ThreadCursor | string | null; since?: number }): ThreadCursor | null {
  if (typeof options.cursor === 'string') return parseCursor(options.cursor);
  if (options.cursor) return options.cursor;
  return options.since !== undefined ? { seq: options.since } : null;
}

let warnedInMemory = false;

/** The run streams when none were passed: in memory, which only this
 *  process can read. Said once, loudly, since a web server and a worker in
 *  separate processes would each see only their own. */
function inMemoryStreams(log?: Logger): MemoryRunStreams {
  if (!warnedInMemory) {
    warnedInMemory = true;
    ((log ?? console) as { warn?: (m: string) => void }).warn?.(
      'agentenkit: no `streams` port was passed, so run streams are kept in memory and only this process can ' +
        'read them. Pass one (RedisRunStreams, a Postgres or SQL store) when web servers and workers run apart.',
    );
  }
  return new MemoryRunStreams();
}

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
  const admin = opts.admin ?? (await openDefaultAdminStore(opts.log));

  const shared = {
    admin,
    bus: opts.bus,
    queue: opts.queue,
    kv: opts.kv,
    streams: opts.streams ?? inMemoryStreams(opts.log),
    resolveModel: (modelName: string) => opts.resolveModel(modelName),
    pricer: opts.pricer,
    log: opts.log,
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
  // The same agents as the engine sees them: what a stop or the sweep needs
  // to settle a run by the agent name on its record (§5.6).
  const agents = new Map<string, RegisteredAgent>();
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
        ? createStreamTextAgent(scope, spec, agents)
        : createGenerateTextAgent(scope, spec, agents);
    registry.set(name, handle);
    return handle;
  };

  /** Call `fn` for every run the filter matches, a page at a time. */
  const SWEEP_PAGE = 500;
  const eachRun = async (filter: RunFilter, fn: (rec: RunRecord) => Promise<void>) => {
    let before: RunFilter['before'];
    for (;;) {
      const page = await deps.admin.runs.list({ ...filter, ...(before ? { before } : {}), limit: SWEEP_PAGE });
      for (const rec of page) await fn(rec);
      if (page.length < SWEEP_PAGE) return;
      const last = page[page.length - 1]!;
      before = { startedAt: last.startedAt, id: last.id };
    }
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
    ): Promise<ThreadSnapshot | null> => threadSnapshot(scope(state), threadId),

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
        deps.storage.usage.total(threadId, {}),
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
      // handler (§2.2): the record from the cursor, the run stream from its
      // offset, one SNAPSHOT when that stream is gone.
      follow: (threadId, options = {}) =>
        followThread(scope(options.state), threadId, { ...options, cursor: cursorOf(options) }),
      sse: (threadId, options = {}) => {
        const cursor = cursorOf(options);
        return toFollowSse(followThread(scope(options.state), threadId, { ...options, cursor }), cursor, options);
      },
      followRecord: (threadId, options = {}) =>
        followEvents(scope(options.state), threadId, options),
      publishEvent: (threadId, type, payload, options = {}) =>
        publishEvent(scope(options.state), threadId, type, payload, options),
    },

    streams: {
      read: (streamId, after, signal) => {
        if (!deps.streams) throw new Error('this runtime has no run streams');
        return deps.streams.read(streamId, after, signal);
      },
      snapshot: async (streamId, after) => (deps.streams ? deps.streams.snapshot(streamId, after) : null),
    },

    createStreamTextAgent: (spec) => {
      const handle = register(spec.name, 'stream-text', spec);
      if (defaultAgent === null) defaultAgent = spec.name;
      return handle;
    },

    createGenerateTextAgent: (spec) => register(spec.name, 'generate-text', spec),

    getAgent: (name: string) => registry.get(name) ?? null,

    reclaimStuckRuns: async (olderThanMs: number): Promise<ReclaimReport> => {
      const report: ReclaimReport = { checked: 0, redispatched: 0, settled: 0, errors: 0 };
      const until = new Date(Date.now() - olderThanMs);
      const log = deps.log ?? console;
      await eachRun({ state: ['QUEUED', 'RUNNING'], until, depth: 0 }, async (rec) => {
        if (rec.endedAt) return;
        report.checked++;
        try {
          if (await reclaimIfOrphaned(scope(rec.runState ?? {}, rec.id), rec.threadId)) report.redispatched++;
        } catch (err) {
          report.errors++;
          log.error('stuck run not reclaimed', { threadId: rec.threadId, runId: rec.id, err });
        }
      });
      await eachRun({ unsettled: true, until, depth: 0 }, async (rec) => {
        if (!rec.endedAt || rec.endedAt > until) return;
        report.checked++;
        const agent = agents.get(rec.agent) ?? (defaultAgent ? agents.get(defaultAgent) : undefined);
        if (!agent) return;
        try {
          if (await settleLate(scope(rec.runState ?? {}, rec.id), agent, rec.threadId, rec.id)) report.settled++;
        } catch (err) {
          report.errors++;
          log.error('unsettled run not settled', { threadId: rec.threadId, runId: rec.id, err });
        }
      });
      return report;
    },

    worker: {
      handleJob: async (job: RunJob, options: { signal?: AbortSignal } = {}) => {
        // Missing `agent` → the default handle (first registered stream-text)
        const agent =
          (job.agent ? registry.get(job.agent) : null) ??
          (defaultAgent ? registry.get(defaultAgent) : null);
        // An error, not a quiet refusal: a queue that retries on failure keeps
        // the job for a process that has the agent, rather than deleting it.
        if (!agent) throw new UnknownAgentError(job.agent);

        // executeWithPolicy: run lock (idempotent under at-least-once
        // delivery, §3.4) + §2.8 failure policy — redrive < maxAttempts,
        // else finalize FAILED; a user stop is never retried.
        await agent.executeWithPolicy({
          threadId: job.threadId,
          // The dispatch's identity (§2.1) — without it the worker cannot tell
          // it has been replaced by a newer run, and a blocked job is dropped.
          runId: job.runId,
          dispatchId: job.dispatchId,
          // Carries the queue wait through to the run record (§2.9).
          enqueuedAt: job.enqueuedAt,
          // Rehydrated from the ticket: this worker never saw the caller (§2.10).
          state: job.state,
          model: job.model,
          tokenBudget: job.tokenBudget,
          costBudgetMicros: job.costBudgetMicros,
          providerOptions: job.providerOptions,
          maxSteps: job.maxSteps,
          dispatchedAt: job.dispatchedAt,
          partitionKey: job.partitionKey,
          kind: job.kind,
          // A shutdown hands the job back without spending an attempt.
          ...(options.signal ? { signal: options.signal } : {}),
        });
        return { accepted: true };
      },

      handleDeadJob: async (job: RunJob, attempts: number, cause: unknown) => {
        const log = deps.log ?? console;
        const agent = agents.get(job.agent ?? '') ?? (defaultAgent ? agents.get(defaultAgent) : undefined);
        if (!agent) {
          log.error("dead job for an unknown agent; its run cannot be failed", { job, cause: String(cause) });
          return;
        }
        const why = cause instanceof Error ? cause.message : String(cause);
        const reason = `the run's job was dropped by the queue after ${attempts} deliveries: ${why}`;
        try {
          const failed = await failLostRun(scope(job.state ?? {}, job.runId), agent, job.threadId, job.runId, reason);
          if (failed) log.error('dead job: run failed', { threadId: job.threadId, runId: job.runId, cause: why });
          else ((log as { warn?: (m: string, ...r: unknown[]) => void }).warn)?.(
            'dead job: run had already moved on; nothing to fail', { threadId: job.threadId, runId: job.runId },
          );
        } catch (err) {
          log.error('dead job: run not failed', { threadId: job.threadId, runId: job.runId, err });
        }
      },
    },

    ports: (state?: AgentRunState) => scope(state ?? {}),
  };
  return core;
}

