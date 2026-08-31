import type { AgentEvent } from './core/types.js';
import { resolveConfig } from './core/types.js';
import type {
  AgentRuntime,
  RespondInput,
  RespondResult,
  RunInput,
  RunResult,
  RuntimeOptions,
  StopResult,
  ThreadSnapshot,
} from './ports/runtime.js';
import { execute, executeWithPolicy } from './core/engine.js';
import { reclaimIfOrphaned } from './core/reclaim.js';
import { respond } from './core/hitl.js';
import { run } from './core/run.js';
import { stop } from './core/stop.js';

/** Bind the ports to the core behaviors (§3.3). This is the package's public
 *  entry point — the only place where anything is wired together. */
export function createAgentRuntime(opts: RuntimeOptions): AgentRuntime {
  const deps = {
    storage: opts.storage,
    bus: opts.bus,
    queue: opts.queue,
    kv: opts.kv,
    models: opts.models,
    config: resolveConfig(opts.config),
  };

  return {
    run: (input: RunInput): Promise<RunResult> => run(deps, input),
    stop: (threadId: string): Promise<StopResult> => stop(deps, threadId),
    getThreadSnapshot: async (threadId: string): Promise<ThreadSnapshot | null> => {
      const thread = await deps.storage.threads.get(threadId);
      if (!thread) return null;
      const [messages, events] = await Promise.all([
        deps.storage.messages.list(threadId),
        deps.storage.events.listSince(threadId, -1),
      ]);
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
    engine: {
      execute: (input: { threadId: string; model: string }) => execute(deps, input),
      executeWithPolicy: (input, policy) => executeWithPolicy(deps, input, policy),
    },
  };
}
