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
 *  Handles both AI-SDK namings (inputTokens/outputTokens and the older
 *  promptTokens/completionTokens) and surfaces cached-input hits where the
 *  provider reports them (0 otherwise). */
export function attributeTokens(usage: UsageLike): TokenAttribution {
  const u = usage ?? {};
  const inputTokens = pick(u.inputTokens, u.promptTokens);
  const cachedInputTokens = pick(u.cachedInputTokens);
  const outputTokens = pick(u.outputTokens, u.completionTokens);
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
export function countTokens(usage: UsageLike): number {
  return attributeTokens(usage).totalTokens;
}
