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
import { createGenerateTextAgent, createStreamTextAgent } from './core/agent.js';
import { reclaimIfOrphaned } from './core/reclaim.js';
import { respond } from './core/hitl.js';

/** Bind the ports to the core behaviors (§3.3). This is the package's public
 *  entry point — the only place where anything is wired together. */
export function setupAgentCore(opts: RuntimeOptions): AgentCore {
  const deps: RuntimePorts = {
    storage: opts.storage,
    bus: opts.bus,
    queue: opts.queue,
    kv: opts.kv,
    resolveModel: (modelName: string) => opts.resolveModel(modelName),
    config: resolveConfig(opts.config),
  };

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
        ? createStreamTextAgent(deps, spec)
        : createGenerateTextAgent(deps, spec);
    registry.set(name, handle);
    return handle;
  };

  const core: AgentCore = {
    resolveModel: (modelName: string) => deps.resolveModel(modelName),

    listThreads: () => deps.storage.threads.list(),

    getThreadSnapshot: async (threadId: string): Promise<ThreadSnapshot | null> => {
      const thread = await deps.storage.threads.get(threadId);
      if (!thread) return null;

      const messages = await deps.storage.messages.list(threadId);
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

      return { thread, messages, lastEventSeq, activeEvents };
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
          model: job.model,
          tokenBudget: job.tokenBudget,
        });
        return { accepted: true };
      },
    },
  };
  return core;
}

/** Deprecated alias for `setupAgentCore` — kept for the one-minor
 *  migration window (§7). */
export function createAgentRuntime(opts: RuntimeOptions): AgentCore {
  return setupAgentCore(opts);
}
