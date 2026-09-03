import { generateText } from 'ai';
import type { RuntimePorts } from '../ports/runtime.js';
import type { ContextUsage, MessageDTO } from './types.js';
import { publish } from './publish.js';
import { fillTokens, providerMeta, recordCall } from './usage.js';
import { wireId } from './types.js';

/** Universal context ceiling across all models (§2.6) */
export const CONTEXT_TOKEN_CEILING = 265_000;

const DEFAULT_NATIVE_WINDOWS: Record<string, number> = {
  'gpt-4o': 128_000,
  'gpt-4o-mini': 128_000,
  'claude-3-5-sonnet': 200_000,
  'gemini-1.5-pro': 1_000_000,
};

/** Effective budget = min(native window, ceiling). The model's declared
 *  `contextWindow` (via `resolveModel`, §3.3) wins over the fallback tables;
 *  models below the ceiling keep their native window. */
export function contextBudget(deps: RuntimePorts, model: string): number {
  let declared: number | undefined;
  try {
    declared = deps.resolveModel(model).contextWindow;
  } catch {
    // unknown registry key — fall through to the tables below
  }
  const native =
    declared ??
    deps.config.nativeWindows?.[model] ??
    DEFAULT_NATIVE_WINDOWS[model] ??
    deps.config.contextCeilingTokens;
  return Math.min(native, deps.config.contextCeilingTokens);
}

/** The one token estimate the platform uses where a real count is
 *  unavailable: how full the context is (§2.6), and the output of a call that
 *  was cut off before the provider could report one (§4). One rule for both,
 *  so the two never disagree. */
export const estimateTokens = (content: unknown) =>
  Math.ceil(JSON.stringify(content).length / 4);

/** Read-only view of the §2.6 budget math — what compactContext would see on
 *  the next run, without summarizing anything. */
export async function contextUsage(
  deps: RuntimePorts,
  threadId: string,
  model: string,
): Promise<ContextUsage> {
  const budgetTokens = contextBudget(deps, model) - deps.config.contextOutputReserveTokens;
  // The main agent's stream only — a nested run's turns are its own (§2.7)
  const history = await deps.storage.messages.list(threadId, { agentId: null });
  return {
    usedTokens: history.reduce((sum, m) => sum + estimateTokens(m.content), 0),
    budgetTokens,
    compactAtTokens: Math.floor(budgetTokens * deps.config.compactionTrigger),
    messages: history.length,
  };
}

// Returns a history array guaranteed to fit the model's budget. Compaction is
// durable: the summary is persisted as a Message, so every client and every
// reconnect replay (§2.2) reconstructs the exact same context.
export async function compactContext(
  deps: RuntimePorts,
  threadId: string,
  model: string,
): Promise<MessageDTO[]> {
  const budget = contextBudget(deps, model) - deps.config.contextOutputReserveTokens;
  // Scoped to the main agent: unscoped, delegated turns would be compacted
  // into — and then fed back through — the parent's prompt (§2.7)
  const history = await deps.storage.messages.list(threadId, { agentId: null });

  const total = history.reduce((sum, m) => sum + estimateTokens(m.content), 0);
  if (total <= budget * deps.config.compactionTrigger) return history;

  // Keep the most recent tail verbatim ...
  const tail: typeof history = [];
  let tailTokens = 0;
  for (const m of [...history].reverse()) {
    if (tailTokens + estimateTokens(m.content) > budget * deps.config.contextTailShare) break;
    tail.unshift(m);
    tailTokens += estimateTokens(m.content);
  }
  const older = history.slice(0, history.length - tail.length);
  if (older.length === 0) return history; // single oversized turn — blocked by the input guards above

  // ... and summarize everything before it with a cheap model, named in config
  // so a registry that has never heard of 'gpt-4o-mini' can point this at its
  // own (§2.6). Naming the key in the error matters: resolveModel throws from
  // deep inside compaction, on a run that never mentioned this model, so the
  // bare "Unknown model" says nothing about where it came from.
  const compactionModel = deps.config.compactionModel;
  let compactor;
  try {
    compactor = deps.resolveModel(compactionModel);
  } catch (err) {
    throw new Error(
      `compactionModel ${JSON.stringify(compactionModel)} could not be resolved: ` +
        `${err instanceof Error ? err.message : String(err)}`,
    );
  }
  const { text, usage, ...rest } = await generateText({
    model: compactor.instance(),
    prompt:
      'Summarize the following conversation history into a dense context brief ' +
      '(decisions, open threads, key facts) for an AI agent:\n\n' +
      older.map((m) => `${m.role}: ${JSON.stringify(m.content)}`).join('\n'),
  });

  const summary = await deps.storage.messages.append(threadId, {
    role: 'system',
    content: { type: 'CONTEXT_SUMMARY', text },
  });

  // Compaction is a model call the platform made on its own account (§2.6),
  // so it gets its own priced row like any other (§4). Kind 'compaction' keeps
  // it separable: nobody asked for this call, and it is worth being able to
  // see what the platform's own housekeeping costs.
  //
  // The cache hit is reported in provider metadata, never in `usage` —
  // attributing without it books every cached prompt at the full input price.
  const meta =
    (rest as any).providerMetadata ?? (rest as any).experimental_providerMetadata;
  await recordCall(deps, threadId, {
    agentId: null,
    kind: 'compaction',
    step: 0,
    model: compactionModel,
    modelId: wireId(compactor, compactionModel),
    outcome: 'finished',
    providerMetadata: providerMeta(meta, (rest as any).response),
    ...fillTokens(usage, meta),
  });
  await publish(deps, threadId, 'CONTEXT_COMPACTED', { summarizedMessages: older.length });

  return [summary, ...tail];
}
