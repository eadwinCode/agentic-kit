import type { NewUsage, UsageTotals } from './types.js';
import type { RuntimePorts } from '../ports/runtime.js';

/** Token attribution (§4): the four canonical counters. Same shape as
 *  UsageTotals. */

export interface TokenAttribution {
  /** Fresh (uncached) prompt tokens. */
  inputTokens: number;
  /** Prompt tokens served from the provider's prompt cache (§2.6). */
  cachedInputTokens: number;
  outputTokens: number;
  /** input + cached + output. */
  totalTokens: number;
}

/** Where a cache hit is reported, per provider. The AI SDK's `usage` carries
 *  only prompt/completion/total — cache counts live in provider metadata, so
 *  reading `usage` alone can never see one. */
export type ProviderMetadataLike =
  | Record<string, Record<string, unknown> | undefined>
  | undefined;

type UsageLike = {
  inputTokens?: number;
  cachedInputTokens?: number;
  outputTokens?: number;
  promptTokens?: number;
  completionTokens?: number;
  totalTokens?: number;
} | undefined;

const pick = (...vals: (number | undefined)[]): number => {
  for (const v of vals) {
    if (typeof v === 'number' && Number.isFinite(v)) return v;
  }
  return 0;
};

/** Attribute one usage report into the four canonical counters, NaN-guarded.
 *
 *  Handles both AI-SDK namings (inputTokens/outputTokens and the older
 *  promptTokens/completionTokens), and reads cache hits out of PROVIDER
 *  METADATA, which is the only place they appear — `usage` has no field for
 *  them, so attributing from it alone reports zero cache hits forever.
 *
 *  The two providers disagree about what "prompt tokens" means, and getting it
 *  wrong double-counts:
 *   - OpenAI's promptTokens INCLUDES the cached ones, so the fresh count is
 *     the difference.
 *   - Anthropic reports cache reads alongside input, not inside it. */
export function attributeTokens(
  usage: UsageLike,
  meta?: ProviderMetadataLike,
): TokenAttribution {
  const u = usage ?? {};
  const reportedInput = pick(u.inputTokens, u.promptTokens);
  const outputTokens = pick(u.outputTokens, u.completionTokens);

  const openaiCached = pick(meta?.openai?.cachedPromptTokens as number | undefined);
  const anthropicCached = pick(
    meta?.anthropic?.cacheReadInputTokens as number | undefined,
  );
  const cachedInputTokens = pick(u.cachedInputTokens) + openaiCached + anthropicCached;

  const inputTokens =
    openaiCached > 0 ? Math.max(0, reportedInput - openaiCached) : reportedInput;

  const totalFromProvider = pick(u.totalTokens);
  const summed = inputTokens + cachedInputTokens + outputTokens;
  return {
    inputTokens,
    cachedInputTokens,
    outputTokens,
    totalTokens: totalFromProvider > 0 ? totalFromProvider : summed,
  };
}

/** Total tokens used — input + cached + output, NaN-guarded. */
export function countTokens(usage: UsageLike, meta?: ProviderMetadataLike): number {
  return attributeTokens(usage, meta).totalTokens;
}


/** Cache WRITE tokens, which no provider reports in `usage`: Anthropic puts
 *  them in provider metadata, and they are a separate line on the bill — a
 *  cache write costs more than a fresh input token, so a pricer needs them
 *  apart from the rest. */
export function cacheWriteTokens(meta?: ProviderMetadataLike): number {
  return pick(
    meta?.anthropic?.cacheCreationInputTokens as number | undefined,
    meta?.bedrock?.cacheWriteInputTokens as number | undefined,
  );
}

/** Reasoning tokens, where the provider separates them out. Usually already
 *  inside the output count, which is why the shipped price table prices them
 *  at zero by default. */
export function reasoningTokens(usage: UsageLike, meta?: ProviderMetadataLike): number {
  return pick(
    (usage as { reasoningTokens?: number } | undefined)?.reasoningTokens,
    meta?.openai?.reasoningTokens as number | undefined,
  );
}

/** Everything a pricer needs from one model call, in the shape a usage row
 *  carries it. `attributeTokens` already sorts out the providers'
 *  disagreements about input and cache reads; this adds the two counters that
 *  only pricing cares about. */
export function fillTokens(
  usage: UsageLike,
  meta?: ProviderMetadataLike,
): Pick<
  NewUsage,
  | 'inputTokens'
  | 'cacheReadInputTokens'
  | 'cacheWriteInputTokens'
  | 'outputTokens'
  | 'reasoningTokens'
  | 'totalTokens'
> {
  const a = attributeTokens(usage, meta);
  return {
    inputTokens: a.inputTokens,
    cacheReadInputTokens: a.cachedInputTokens,
    cacheWriteInputTokens: cacheWriteTokens(meta),
    outputTokens: a.outputTokens,
    reasoningTokens: reasoningTokens(usage, meta),
    totalTokens: a.totalTokens,
  };
}

/** Flatten what the AI SDK reports into the single map a pricer reads.
 *  Provider namespaces keep their names; the response id and headers get
 *  reserved keys, because an AI gateway bills through a header and a receipt
 *  pricer has to be able to find it. */
export function providerMeta(
  meta?: ProviderMetadataLike,
  response?: { id?: string; headers?: Record<string, string> },
): Record<string, unknown> | undefined {
  const out: Record<string, unknown> = { ...(meta ?? {}) };
  if (response?.id) out.responseId = response.id;
  if (response?.headers && Object.keys(response.headers).length > 0) {
    out.responseHeaders = response.headers;
  }
  return Object.keys(out).length > 0 ? out : undefined;
}

/** Price one model call and store its usage row (§4). Called after every call
 *  the platform makes: a step of the main run, a step of a nested run, a
 *  compaction pass, streamed or not, finished or cut short.
 *
 *  Pricing happens here, before the row is stored, so cost sits on the row
 *  beside the tokens and no reader has to work it out again. A pricer that
 *  fails, or has nothing to say, leaves the row unpriced rather than failing
 *  the run: a bill that is short a line is recoverable, a run that died over a
 *  price list is not. Storage failures are logged for the same reason — the
 *  tokens of a stopped run were still spent. */
export async function recordCall(
  deps: RuntimePorts,
  threadId: string,
  usage: NewUsage,
): Promise<NewUsage> {
  const log = deps.log ?? console;
  const priced = { ...usage };
  if (deps.pricer && !priced.cost) {
    try {
      priced.cost = (await deps.pricer.price(priced)) ?? null;
    } catch (err) {
      log.error('usage not priced', { run: priced.runId, model: priced.model, err });
    }
  }
  try {
    await deps.storage.usage.record(threadId, priced);
  } catch (err) {
    log.error('usage not recorded', { run: priced.runId, thread: threadId, err });
  }
  return priced;
}


/** Zero totals — what a thread with no usage rows reports. */
export const emptyTotals = (): UsageTotals => ({
  inputTokens: 0,
  cachedInputTokens: 0,
  outputTokens: 0,
  totalTokens: 0,
  costMicros: 0,
  unpriced: 0,
  lines: [],
});

/** Sum usage rows into the shape `total` must return: the four counters, the
 *  money, and one line per agent and model.
 *
 *  A storage adapter that can group in the database should do that instead.
 *  This is for the ones that cannot, and for anyone writing their own adapter:
 *  feed every matching row through it and you get exactly what the port
 *  promises, lines in first-seen order. */
export function sumUsage(rows: Iterable<NewUsage>): UsageTotals {
  const out = emptyTotals();
  const index = new Map<string, number>();
  for (const u of rows) {
    out.inputTokens += u.inputTokens;
    out.cachedInputTokens += u.cacheReadInputTokens;
    out.outputTokens += u.outputTokens;
    out.totalTokens += u.totalTokens;
    if (u.cost) {
      out.costMicros += u.cost.micros;
      out.currency ??= u.cost.currency;
    } else {
      out.unpriced += 1;
    }

    const key = [u.agentId ?? '', u.agentName ?? '', u.model ?? '', u.modelId ?? ''].join('\u0000');
    let i = index.get(key);
    if (i === undefined) {
      i = out.lines.length;
      index.set(key, i);
      out.lines.push({
        agentId: u.agentId ?? null,
        agentName: u.agentName ?? null,
        model: u.model ?? null,
        modelId: u.modelId ?? null,
        inputTokens: 0, cacheReadInputTokens: 0, cacheWriteInputTokens: 0,
        outputTokens: 0, reasoningTokens: 0, calls: 0, estimated: 0, costMicros: 0,
      });
    }
    const line = out.lines[i]!;
    line.inputTokens += u.inputTokens;
    line.cacheReadInputTokens += u.cacheReadInputTokens;
    line.cacheWriteInputTokens += u.cacheWriteInputTokens;
    line.outputTokens += u.outputTokens;
    line.reasoningTokens += u.reasoningTokens;
    line.calls += 1;
    if (u.estimated) line.estimated += 1;
    if (u.cost) line.costMicros += u.cost.micros;
  }
  return out;
}
