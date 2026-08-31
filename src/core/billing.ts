import type { RuntimePorts } from '../ports/runtime.js';

/** Default per-token rates (§4). Users override/extend via
 *  `config.billingRates` — their models, their pricing. */
export const DEFAULT_RATES: Record<string, { prompt: number; completion: number }> = {
  'gpt-4o': { prompt: 0.0000025, completion: 0.00001 },
  'gpt-4o-mini': { prompt: 0.00000015, completion: 0.0000006 },
  'claude-3-5-sonnet': { prompt: 0.000003, completion: 0.000015 },
  'gemini-1.5-pro': { prompt: 0.00000125, completion: 0.000005 },
};

/** Returns the cost in USD, or null when no rate is configured for the model.
 *  Null never silently falls back to another model's rate — callers record
 *  cost 0 and publish a BILLING_UNPRICED warning (§4). */
export function calculateCost(
  deps: RuntimePorts,
  model: string,
  promptTokens: number,
  completionTokens: number,
): number | null {
  const rates = { ...DEFAULT_RATES, ...(deps.config.billingRates ?? {}) };
  const rate = rates[model];
  if (!rate) return null;
  return promptTokens * rate.prompt + completionTokens * rate.completion;
}
