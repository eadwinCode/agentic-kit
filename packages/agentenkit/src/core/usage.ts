import type { NewUsage, UsageLine, UsageTotals } from './types.js';
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

  // Always the sum, never the provider's own total: Anthropic's leaves the
  // cache reads out, so trusting it would make the same count mean different
  // things per provider. The Go runtime computes it the same way.
  return {
    inputTokens,
    cachedInputTokens,
    outputTokens,
    totalTokens: inputTokens + cachedInputTokens + outputTokens,
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
//
// A run's own calls go through its RunLedger.record instead, which also holds
// the run to one currency and books the spend against its caps.
export async function recordCall(
  deps: RuntimePorts,
  threadId: string,
  usage: NewUsage,
): Promise<NewUsage> {
  const priced = await price(deps, usage);
  await store(deps, threadId, priced);
  return priced;
}

/** Put the pricer's cost on a usage row, leaving it unpriced when the pricer
 *  fails or has nothing to say. */
async function price(deps: RuntimePorts, usage: NewUsage): Promise<NewUsage> {
  const priced = { ...usage };
  if (deps.pricer && !priced.cost) {
    try {
      priced.cost = (await deps.pricer.price(priced)) ?? null;
    } catch (err) {
      (deps.log ?? console).error('usage not priced', { run: priced.runId, model: priced.model, err });
    }
  }
  return priced;
}

/** Write a usage row. A failure is logged, not thrown: the tokens of a
 *  stopped run were still spent. */
async function store(deps: RuntimePorts, threadId: string, usage: NewUsage): Promise<void> {
  try {
    await deps.storage.usage.record(threadId, usage);
  } catch (err) {
    (deps.log ?? console).error('usage not recorded', { run: usage.runId, thread: threadId, err });
  }
}

/** What a run has spent, main agent and nested runs together (§2.7): its
 *  tokens, its money and the one currency that money is in. Shared by
 *  reference so a child's spend counts against the run's caps the moment it
 *  happens — a budget that ignores delegated work is not a budget.
 *
 *  A run's ledger starts from what the run already spent (`seedRunLedger`):
 *  its earlier segments, before a park or a retry, count against the same
 *  caps. After that it is kept in memory, so the caps are checked without
 *  reading every usage row back after every step. */
export class RunLedger {
  tokensUsed = 0;
  costMicros = 0;
  currency: string | undefined;

  /** Price one call, store its row and book it (§4). A run is priced in one
   *  currency: a call priced in another is stored unpriced, with an error
   *  logged, rather than mixed into a bill that cannot add it up. */
  async record(deps: RuntimePorts, threadId: string, usage: NewUsage): Promise<NewUsage> {
    const priced = await price(deps, usage);
    if (priced.cost) {
      this.currency ??= priced.cost.currency;
      if (priced.cost.currency !== this.currency) {
        (deps.log ?? console).error('usage priced in a second currency; stored unpriced', {
          run: priced.runId, model: priced.model, currency: priced.cost.currency, runCurrency: this.currency,
        });
        priced.cost = null;
      }
    }
    await store(deps, threadId, priced);
    this.tokensUsed += priced.totalTokens;
    if (priced.cost) this.costMicros += priced.cost.micros;
    return priced;
  }
}

/** Start a ledger from the run's usage rows (§4). A failed read is logged and
 *  the ledger starts from zero: a run is not failed over its caps'
 *  bookkeeping. */
export async function seedRunLedger(deps: RuntimePorts, threadId: string, runId?: string): Promise<RunLedger> {
  const ledger = new RunLedger();
  if (!runId) return ledger;
  try {
    const spent = await deps.storage.usage.total(threadId, { runId });
    ledger.tokensUsed = spent.totalTokens;
    ledger.costMicros = spent.costMicros;
    ledger.currency = spent.currency;
  } catch (err) {
    (deps.log ?? console).error('run spend not read; the caps count from zero this segment', { run: runId, err });
  }
  return ledger;
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

/** One grouped row, as a SQL adapter reads it: a line, the currency its
 *  cost is in, its summed total tokens and how many of its calls were
 *  unpriced. */
export interface UsageGroup {
  line: UsageLine;
  currency?: string | null;
  totalTokens: number;
  unpriced: number;
}

/** Rebuilds UsageTotals from grouped rows that a SQL adapter read GROUP BY
 *  agent, model AND currency. Grouping by currency is what keeps a sum honest;
 *  merging here is what keeps one agent's spend on one model a single line.
 *
 *  The rules, the same in every adapter and in the Go runtime: `costMicros`
 *  is summed in the first currency seen, and a group priced in another
 *  currency counts as unpriced there instead of being added. `costs` keeps
 *  every currency's own total. A line takes the currency of the first priced
 *  group on it; a group priced in a different currency gets a line of its
 *  own, and unpriced groups join the first line of their agent and model. */
export class UsageMerger {
  private readonly out = emptyTotals();
  private readonly lines = new Map<string, number[]>();
  private readonly costs = new Map<string, number>();

  add(g: UsageGroup): void {
    const out = this.out;
    const l: UsageLine = { ...g.line };
    const currency = g.currency || '';
    out.inputTokens += l.inputTokens;
    out.cachedInputTokens += l.cacheReadInputTokens;
    out.outputTokens += l.outputTokens;
    out.totalTokens += g.totalTokens;
    out.unpriced += g.unpriced;

    if (!currency) {
      l.costMicros = 0;
      delete l.currency;
    } else {
      const priced = l.calls - g.unpriced;
      out.costs ??= [];
      let i = this.costs.get(currency);
      if (i === undefined) {
        i = out.costs.length;
        this.costs.set(currency, i);
        out.costs.push({ currency, costMicros: 0, calls: 0 });
      }
      out.costs[i]!.costMicros += l.costMicros;
      out.costs[i]!.calls += priced;
      out.currency ??= currency;
      if (currency === out.currency) {
        out.costMicros += l.costMicros;
      } else {
        // Another unit: it cannot be added to costMicros, so its calls count
        // as unpriced there and costMicros stays a floor.
        out.unpriced += priced;
      }
      l.currency = currency;
    }

    const key = [l.agentId ?? '', l.agentName ?? '', l.model ?? '', l.modelId ?? ''].join('\u0000');
    const at = (this.lines.get(key) ?? []).find((i) => {
      const c = out.lines[i]!.currency ?? '';
      return !currency || c === currency || c === '';
    });
    if (at === undefined) {
      this.lines.set(key, [...(this.lines.get(key) ?? []), out.lines.length]);
      out.lines.push(l);
      return;
    }
    const line = out.lines[at]!;
    if (!line.currency && currency) line.currency = currency;
    line.inputTokens += l.inputTokens;
    line.cacheReadInputTokens += l.cacheReadInputTokens;
    line.cacheWriteInputTokens += l.cacheWriteInputTokens;
    line.outputTokens += l.outputTokens;
    line.reasoningTokens += l.reasoningTokens;
    line.calls += l.calls;
    line.estimated += l.estimated;
    line.costMicros += l.costMicros;
  }

  totals(): UsageTotals {
    return this.out;
  }
}

/** Sum usage rows into the shape `total` must return: the four counters, the
 *  money, and one line per agent and model.
 *
 *  A storage adapter that can group in the database should do that instead
 *  (see UsageMerger). This is for the ones that cannot, and for anyone writing
 *  their own adapter: feed every matching row through it and you get exactly
 *  what the port promises, lines in first-seen order. */
export function sumUsage(rows: Iterable<NewUsage>): UsageTotals {
  const merge = new UsageMerger();
  for (const u of rows) {
    merge.add({
      line: {
        agentId: u.agentId ?? null,
        agentName: u.agentName ?? null,
        model: u.model ?? null,
        modelId: u.modelId ?? null,
        inputTokens: u.inputTokens,
        cacheReadInputTokens: u.cacheReadInputTokens,
        cacheWriteInputTokens: u.cacheWriteInputTokens,
        outputTokens: u.outputTokens,
        reasoningTokens: u.reasoningTokens,
        calls: 1,
        estimated: u.estimated ? 1 : 0,
        costMicros: u.cost?.micros ?? 0,
      },
      currency: u.cost?.currency ?? null,
      totalTokens: u.totalTokens,
      unpriced: u.cost ? 0 : 1,
    });
  }
  return merge.totals();
}
