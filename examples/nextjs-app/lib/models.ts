import { createOpenAI } from '@ai-sdk/openai';

/** §2.3 — users register any `ai`-SDK models here. Keys feed the UI dropdown
 *  and `modelPrices` below prices them (§4). */
export const modelRegistry = {
  'gpt-4o': createOpenAI({
    apiKey: process.env.OPENAI_API_KEY,
    compatibility: 'strict',
  })('gpt-4o'),
  'gpt-4o-mini': createOpenAI({
    apiKey: process.env.OPENAI_API_KEY,
    compatibility: 'strict',
  })('gpt-4o-mini'),
};


/** The wire id each key resolves to (§4). It goes onto every usage row, so a
 *  price list keyed by wire ids still matches when the key is an alias. A key
 *  with no entry here is its own id. */
export const modelIds: Record<string, string> = {
  'gpt-4o': 'gpt-4o-2024-11-20',
  'gpt-4o-mini': 'gpt-4o-mini-2024-07-18',
};

/** The price list (§4), in dollars per MILLION tokens — typed straight off the
 *  provider's pricing page. The runtime prices every model call against this
 *  before the usage row is stored, so cost lives on the row next to the tokens
 *  and `getThreadUsage` returns money as well as counters.
 *
 *  Keys can be the registry key or the wire id; both are tried. A model that
 *  is not here is stored UNPRICED rather than priced at zero, so a missing
 *  price shows up as a gap in the bill rather than as free work. */
export const modelPrices = {
  'gpt-4o': { inputPerMillion: 2.5, cacheReadPerMillion: 1.25, outputPerMillion: 10 },
  'gpt-4o-mini': { inputPerMillion: 0.15, cacheReadPerMillion: 0.075, outputPerMillion: 0.6 },
};
