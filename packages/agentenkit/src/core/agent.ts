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
import { Semaphore } from './subagent.js';
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
  sem: Semaphore;
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

function createHandle(scope: ScopeFn, agent: RegisteredAgent): AgentHandle {
  return {
    name: agent.name,
    kind: agent.kind,
    execute: (input: ExecuteInput) => execute(scope(input.state, input.runId), agent, input),
    executeWithPolicy: (input: ExecuteInput, policy?: { maxAttempts?: number }) =>
      executeWithPolicy(scope(input.state, input.runId), agent, input, policy),
    run: (input: RunInput) => run(scope(input.state), agent, input),
    stop: (threadId: string, state?: AgentRunState) => stop(scope(state), threadId),
  };
}

export function createStreamTextAgent(
  scope: ScopeFn,
  spec: StreamTextAgentSpec,
): AgentHandle {
  const { name, model, subagents, tokenBudget, costBudgetMicros, providerOptions, ...args } = spec;
  return createHandle(scope, {
    name,
    kind: 'stream-text',
    spec: { model, subagents, tokenBudget, costBudgetMicros, providerOptions },
    args: args as Record<string, any>,
    sem: new Semaphore(scope().config.subagentMaxConcurrent),
  });
}

export function createGenerateTextAgent(
  scope: ScopeFn,
  spec: GenerateTextAgentSpec,
): AgentHandle {
  const { name, model, subagents, tokenBudget, costBudgetMicros, providerOptions, ...args } = spec;
  return createHandle(scope, {
    name,
    kind: 'generate-text',
    spec: { model, subagents, tokenBudget, costBudgetMicros, providerOptions },
    args: args as Record<string, any>,
    sem: new Semaphore(scope().config.subagentMaxConcurrent),
  });
}
