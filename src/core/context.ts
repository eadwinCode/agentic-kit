import { generateText } from 'ai';
import type { RuntimePorts } from '../ports/runtime.js';
import type { MessageDTO } from './types.js';
import { publish } from './publish.js';
import { countTokens } from './usage.js';

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

const estimateTokens = (content: unknown) => Math.ceil(JSON.stringify(content).length / 4);

// Returns a history array guaranteed to fit the model's budget. Compaction is
// durable: the summary is persisted as a Message, so every client and every
// reconnect replay (§2.2) reconstructs the exact same context.
export async function compactContext(
  deps: RuntimePorts,
  threadId: string,
  model: string,
): Promise<MessageDTO[]> {
  const budget = contextBudget(deps, model) - deps.config.contextOutputReserveTokens;
  const history = await deps.storage.messages.list(threadId);

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

  // ... and summarize everything before it with a cheap model
  const { text, usage } = await generateText({
    model: deps.resolveModel('gpt-4o-mini').instance(),
    prompt:
      'Summarize the following conversation history into a dense context brief ' +
      '(decisions, open threads, key facts) for an AI agent:\n\n' +
      older.map((m) => `${m.role}: ${JSON.stringify(m.content)}`).join('\n'),
  });

  const summary = await deps.storage.messages.append(threadId, {
    role: 'system',
    content: { type: 'CONTEXT_SUMMARY', text },
  });

  // Token attribution: total tokens used (§4)
  await deps.storage.usage.record(threadId, {
    agentId: null,
    totalTokens: countTokens(usage),
  });
  await publish(deps, threadId, 'CONTEXT_COMPACTED', { summarizedMessages: older.length });

  return [summary, ...tail];
}
