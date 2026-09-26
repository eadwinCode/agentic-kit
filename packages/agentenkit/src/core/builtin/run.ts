import type { RuntimePorts } from '../../ports/runtime.js';
import type { NewUsage } from '../types.js';
import { recordCall, type RunLedger } from '../usage.js';

/** What a tool call knows about the run it is part of, beyond `state`: the
 *  run's own ports (its storage is bound to the run's state, so a tenant's
 *  usage rows land in the tenant's store), the thread, the run, and the ledger
 *  its spend is booked on so tool costs count against the run's caps. */
export interface ToolRun {
  deps: RuntimePorts;
  threadId: string;
  /** The dispatched run: a nested run's calls are billed to its parent. */
  runId?: string;
  /** null for the main agent, the nested run's id otherwise. */
  agentId: string | null;
  agentName?: string;
  ledger?: RunLedger;
}

/** Where a tool call's options carry its ToolRun. A symbol, so it never
 *  clashes with a field the AI SDK or an app adds. */
export const TOOL_RUN = Symbol.for('agentenkit.toolRun');

/** Give every tool its ToolRun, and `threadId` / `runId` in plain sight on
 *  the options, next to `state`. */
export function withToolRun(tools: Record<string, any>, run: ToolRun): Record<string, any> {
  const out: Record<string, any> = {};
  for (const [name, t] of Object.entries(tools)) {
    out[name] =
      typeof t?.execute === 'function'
        ? {
            ...t,
            execute: (args: unknown, opts: object) =>
              t.execute(args, { ...opts, threadId: run.threadId, runId: run.runId, [TOOL_RUN]: run }),
          }
        : t;
  }
  return out;
}

/** The ToolRun a call was given, if it runs inside an agentenkit run. */
export function toolRunOf(opts: unknown): ToolRun | undefined {
  return (opts as { [TOOL_RUN]?: ToolRun } | undefined)?.[TOOL_RUN];
}

/** Book one row of a tool's spend: on the run's ledger when there is one, so
 *  it counts against the run's caps, else stored on its own. */
export async function recordToolUsage(run: ToolRun, usage: Omit<NewUsage, 'runId' | 'agentId' | 'agentName'>) {
  const row: NewUsage = {
    ...usage,
    ...(run.runId ? { runId: run.runId } : {}),
    agentId: run.agentId,
    ...(run.agentName ? { agentName: run.agentName } : {}),
  };
  return run.ledger ? run.ledger.record(run.deps, run.threadId, row) : recordCall(run.deps, run.threadId, row);
}

/** A usage row for one use of a paid tool service: no tokens, the tool and
 *  the adapter named where a price table looks. */
export function toolUseRow(tool: string, adapter: string, uses = 1): Omit<NewUsage, 'runId' | 'agentId' | 'agentName'> {
  return {
    kind: 'tool',
    step: 0,
    model: `tool:${tool}`,
    modelId: adapter,
    inputTokens: 0,
    cacheReadInputTokens: 0,
    cacheWriteInputTokens: 0,
    outputTokens: 0,
    reasoningTokens: 0,
    totalTokens: 0,
    outcome: 'finished',
    providerMetadata: { tool, adapter, uses },
  };
}
