/** Total tokens used — input + cached + output, NaN-guarded. Providers that
 *  omit streaming usage report NaN; optional metering must never keep a
 *  completed run stuck in RUNNING. `cachedInputTokens` are a subset of input
 *  on providers that report them — never counted twice. */
export function countTokens(
  usage:
    | {
        inputTokens?: number;
        outputTokens?: number;
        promptTokens?: number;
        completionTokens?: number;
        cachedInputTokens?: number;
      }
    | undefined,
): number {
  const pick = (...vals: (number | undefined)[]) => {
    for (const v of vals) {
      if (typeof v === 'number' && Number.isFinite(v)) return v;
    }
    return 0;
  };
  const u = usage ?? {};
  return pick(u.inputTokens, u.promptTokens) + pick(u.cachedInputTokens) + pick(u.outputTokens, u.completionTokens);
}
