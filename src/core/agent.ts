import type { RuntimePorts } from '../ports/runtime.js';
import type {
  AgentHandle,
  AgentKind,
  GenerateTextAgentSpec,
  ProviderOptions,
  RunInput,
  StopResult,
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

function createHandle(deps: RuntimePorts, agent: RegisteredAgent): AgentHandle {
  return {
    name: agent.name,
    kind: agent.kind,
    execute: (input: { threadId: string; model: string; tokenBudget?: number }) =>
      execute(deps, agent, input),
    executeWithPolicy: (
      input: { threadId: string; model: string; tokenBudget?: number },
      policy?: { maxAttempts?: number },
    ) => executeWithPolicy(deps, agent, input, policy),
    run: (input: RunInput) => run(deps, agent, input),
    stop: (threadId: string) => stop(deps, threadId),
  };
}

export function createStreamTextAgent(
  deps: RuntimePorts,
  spec: StreamTextAgentSpec,
): AgentHandle {
  const { name, model, subagents, tokenBudget, providerOptions, ...args } = spec;
  return createHandle(deps, {
    name,
    kind: 'stream-text',
    spec: { model, subagents, tokenBudget, providerOptions },
    args: args as Record<string, any>,
    sem: new Semaphore(deps.config.subagentMaxConcurrent),
  });
}

export function createGenerateTextAgent(
  deps: RuntimePorts,
  spec: GenerateTextAgentSpec,
): AgentHandle {
  const { name, model, subagents, tokenBudget, providerOptions, ...args } = spec;
  return createHandle(deps, {
    name,
    kind: 'generate-text',
    spec: { model, subagents, tokenBudget, providerOptions },
    args: args as Record<string, any>,
    sem: new Semaphore(deps.config.subagentMaxConcurrent),
  });
}
