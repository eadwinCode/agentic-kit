import { generateText } from 'ai';
import type { RuntimePorts } from '../ports/runtime.js';
import type { ContextUsage, MessageDTO } from './types.js';
import { publish } from './publish.js';
import { fillTokens, providerMeta, recordCall } from './usage.js';
import { wireId } from './types.js';
import { promptHistory, summaryOf } from './messages.js';

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
  const history = promptHistory(await deps.storage.messages.list(threadId, { agentId: null }));
  return {
    usedTokens: history.reduce((sum, m) => sum + estimateTokens(m.content), 0),
    budgetTokens,
    compactAtTokens: Math.floor(budgetTokens * deps.config.compactionTrigger),
    messages: history.length,
  };
}

/** What a compaction needs to know about the run it serves. */
export interface CompactOptions {
  /** The run the compaction call is billed to (§4), so it counts in the run's
   *  bill and against its cost cap. */
  runId?: string;
  /** Cancels the summary call on a user stop. */
  abortSignal?: AbortSignal;
}

// Returns a history array guaranteed to fit the model's budget. Compaction is
// durable: the summary is persisted as a Message, so every client and every
// reconnect replay (§2.2) reconstructs the exact same context.
//
// The summary records the last message it covers, and the prompt carries the
// summary and only what came after (see promptHistory), so a thread compacts
// once each time it grows past the trigger, not on every run.
export async function compactContext(
  deps: RuntimePorts,
  threadId: string,
  model: string,
  opts: CompactOptions = {},
): Promise<MessageDTO[]> {
  const budget = contextBudget(deps, model) - deps.config.contextOutputReserveTokens;
  // Scoped to the main agent: unscoped, delegated turns would be compacted
  // into — and then fed back through — the parent's prompt (§2.7)
  const history = promptHistory(await deps.storage.messages.list(threadId, { agentId: null }));

  const total = history.reduce((sum, m) => sum + estimateTokens(m.content), 0);
  if (total <= budget * deps.config.compactionTrigger) return history;

  // Keep the most recent tail verbatim ...
  let tailStart = history.length;
  let tailTokens = 0;
  for (let i = history.length - 1; i >= 0; i -= 1) {
    const t = estimateTokens(history[i]!.content);
    if (tailTokens + t > budget * deps.config.contextTailShare) break;
    tailStart = i;
    tailTokens += t;
  }
  // ... starting at a user turn. Cut anywhere else and the tail can open on a
  // tool result whose call went into the summary, which no provider accepts.
  // The first user turn inside the budget, or failing that the last one before.
  tailStart = userTurnAtOrAfter(history, tailStart);
  const older = history.slice(0, tailStart);
  const tail = history.slice(tailStart);
  const coversUpTo = [...older].reverse().find((m) => !summaryOf(m))?.id;
  if (!coversUpTo) {
    // Nothing new to summarize: one turn is larger than the tail's share. It
    // goes as it is; the estimate is rough, and the provider decides.
    warnOverBudget(deps, threadId, total, budget);
    return history;
  }

  // ... and summarize everything before it with a cheap model, named in config
  // so a registry that has never heard of 'gpt-4o-mini' can point this at its
  // own (§2.6): the last summary and the turns since, never the whole history
  // again. Naming the key in the error matters: resolveModel throws from deep
  // inside compaction, on a run that never mentioned this model, so the bare
  // "Unknown model" says nothing about where it came from.
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
    ...(opts.abortSignal ? { abortSignal: opts.abortSignal } : {}),
  });

  const summary = await deps.storage.messages.append(threadId, {
    role: 'system',
    content: { type: 'CONTEXT_SUMMARY', text, coversUpTo },
  });

  // Compaction is a model call the platform made on its own account (§2.6),
  // so it gets its own priced row like any other (§4). Kind 'compaction' keeps
  // it separable: nobody asked for this call, and it is worth being able to
  // see what the platform's own housekeeping costs. It is billed to the run it
  // served, so it is on that run's bill and under its cap.
  //
  // The cache hit is reported in provider metadata, never in `usage` —
  // attributing without it books every cached prompt at the full input price.
  const meta =
    (rest as any).providerMetadata ?? (rest as any).experimental_providerMetadata;
  await recordCall(deps, threadId, {
    ...(opts.runId ? { runId: opts.runId } : {}),
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

  const out = [summary, ...tail];
  // Still over the window: a tail turn alone is larger than it. The next run
  // compacts from this summary on, so this never loops; the call goes as it is
  // and the provider decides, but it is worth saying.
  warnOverBudget(deps, threadId, out.reduce((sum, m) => sum + estimateTokens(m.content), 0), budget);
  return out;
}

/** Move a tail start onto a user turn: the first one at or after `start`, or
 *  failing that the last one before it. With no user turn at all, the tail is
 *  empty. */
function userTurnAtOrAfter(history: MessageDTO[], start: number): number {
  for (let i = start; i < history.length; i += 1) if (history[i]!.role === 'user') return i;
  for (let i = Math.min(start, history.length) - 1; i >= 0; i -= 1) {
    if (history[i]!.role === 'user') return i;
  }
  return history.length;
}

/** Log a prompt estimated past the whole window. */
function warnOverBudget(deps: RuntimePorts, threadId: string, used: number, budget: number) {
  if (used <= budget) return;
  const log = (deps.log ?? console) as { warn?: (m: string, ...r: unknown[]) => void };
  log.warn?.('prompt larger than the context window after compaction', {
    threadId, estimatedTokens: used, budgetTokens: budget,
  });
}
