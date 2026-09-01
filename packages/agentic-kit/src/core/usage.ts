/** Token attribution (§4): the platform records the counters; USD/credit
 *  pricing is a downstream concern computed over them. */

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
