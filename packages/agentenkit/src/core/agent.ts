import type { RuntimePorts } from '../ports/runtime.js';
import type { AgentRunState } from './state.js';
import type { ExecuteInput } from './engine.js';
import type {
  AgentHandle,
  AgentKind,
  GenerateTextAgentSpec,
  ProviderOptions,
  RunInput,
  StreamTextAgentSpec,
  SubagentsConfig,
} from '../ports/runtime.js';
import { execute, executeWithPolicy } from './engine.js';
import { run } from './run.js';
import { stop } from './stop.js';

/** The registry entry behind a handle: the bound generation flavor, the
 *  spec-level defaults the engine reads, and the user's generation args
 *  (spread first, platform keys last — §3.1). */
export interface RegisteredAgent {
  name: string;
  kind: AgentKind;
  spec: {
    model?: string;
    subagents?: boolean | SubagentsConfig;
    tokenBudget?: number;
    /** Default per-run money cap (§4), in millionths of the pricer's
     *  currency. */
    costBudgetMicros?: number;
    /** Additional provider-specific options (§3.1) — merged per run. */
    providerOptions?: ProviderOptions;
  };
  /** The user's generation args — spread first, platform keys last (§3.1). */
  args: Record<string, any>;
}

/** Normalize the delegation config: `false`/`undefined` = off; `true` = defaults. */
export function normalizeSubagents(
  spec: boolean | SubagentsConfig | undefined,
): SubagentsConfig | null {
  if (!spec) return null;
  return spec === true ? {} : spec;
}

/** Resolves the ports for one call, binding that call's state to storage
 *  (§2.10). A handle outlives many runs, so it cannot hold fixed ports. */
export type ScopeFn = (state?: AgentRunState, runId?: string) => RuntimePorts;

/** Finds a registered agent by name: a stop needs it to settle a run no
 *  worker holds (§5.6). */
export type AgentLookup = (name: string) => RegisteredAgent | null;

function createHandle(scope: ScopeFn, agent: RegisteredAgent, lookup: AgentLookup): AgentHandle {
  return {
    name: agent.name,
    kind: agent.kind,
    execute: (input: ExecuteInput) => execute(scope(input.state, input.runId), agent, input),
    executeWithPolicy: (input: ExecuteInput, policy?: { maxAttempts?: number }) =>
      executeWithPolicy(scope(input.state, input.runId), agent, input, policy),
    run: (input: RunInput) => run(scope(input.state), agent, input),
    stop: (threadId: string, state?: AgentRunState) => stop(scope(state), threadId, { agent: lookup }),
  };
}

/** Build a handle, and put its registry entry in `agents` when given, so a
 *  lookup by name finds it. */
export function createStreamTextAgent(
  scope: ScopeFn,
  spec: StreamTextAgentSpec,
  agents?: Map<string, RegisteredAgent>,
): AgentHandle {
  const { name, model, subagents, tokenBudget, costBudgetMicros, providerOptions, ...args } = spec;
  return registered(scope, {
    name,
    kind: 'stream-text',
    spec: { model, subagents, tokenBudget, costBudgetMicros, providerOptions },
    args: args as Record<string, any>,
  }, agents);
}

function registered(scope: ScopeFn, agent: RegisteredAgent, agents?: Map<string, RegisteredAgent>): AgentHandle {
  agents?.set(agent.name, agent);
  const lookup: AgentLookup = (name) => agents?.get(name) ?? (name === agent.name ? agent : null);
  return createHandle(scope, agent, lookup);
}

export function createGenerateTextAgent(
  scope: ScopeFn,
  spec: GenerateTextAgentSpec,
  agents?: Map<string, RegisteredAgent>,
): AgentHandle {
  const { name, model, subagents, tokenBudget, costBudgetMicros, providerOptions, ...args } = spec;
  return registered(scope, {
    name,
    kind: 'generate-text',
    spec: { model, subagents, tokenBudget, costBudgetMicros, providerOptions },
    args: args as Record<string, any>,
  }, agents);
}
