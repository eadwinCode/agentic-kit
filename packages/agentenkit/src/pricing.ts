/** The pricers that ship with the platform (§4).
 *
 *  A pricer turns one model call into money. The runtime calls it after every
 *  call, before the usage row is stored, so cost lives on the row next to the
 *  tokens instead of being worked out by whoever reads it later.
 *
 *  Three cover almost everyone:
 *
 *    `table`   — a price list, keyed by model. The common case.
 *    `receipt` — read the cost the provider already computed and sent back.
 *    `chain`   — try several in order; the first that answers wins.
 *
 *  Anything else is a `Pricer` of your own: an object with a `price` method. */
import type { Cost, NewUsage } from './core/types.js';
import type { Pricer } from './ports/runtime.js';

/** The currency the shipped pricers use unless told otherwise. */
export const USD = 'USD';

/** What one model costs, per MILLION tokens, in the table's currency. A price
 *  of 3 for input means $3.00 per million input tokens, which is how every
 *  provider publishes them, so a price list can be typed straight off their
 *  pricing page. */
export interface ModelPrice {
  inputPerMillion?: number;
  cacheReadPerMillion?: number;
  cacheWritePerMillion?: number;
  outputPerMillion?: number;
  /** Usually 0. Most providers already count reasoning tokens inside the
   *  output count, so charging them again here bills the same tokens twice.
   *  Set it only when your provider reports reasoning tokens SEPARATELY from
   *  output. */
  reasoningPerMillion?: number;
}

/** A price list keyed by model: the registry key a run asked for
 *  ('claude-sonnet-4@high'), or the wire id `resolveModel` declared
 *  ('claude-sonnet-4-20250514'). Both work — a lookup tries them in that
 *  order, then the registry key with any '@variant' suffix removed. */
export type PriceTable = Record<string, ModelPrice>;

/** Find a model's price, trying the registry key, the wire id, then the
 *  registry key without its '@variant' suffix. */
export function lookupPrice(
  prices: PriceTable,
  model?: string | null,
  modelId?: string | null,
): ModelPrice | undefined {
  // Own keys only: a model named 'constructor' or 'toString' must not find
  // something off the object's prototype.
  const own = (k?: string | null) => (k && Object.hasOwn(prices, k) ? prices[k] : undefined);
  const base = model?.split('@')[0];
  return own(model) ?? own(modelId) ?? (base !== model ? own(base) : undefined);
}

/** Warned once per model and rate, so a price list missing a cache rate says
 *  so without filling the log. */
const warned = new Set<string>();
function warnOnce(key: string, msg: string, model?: string | null) {
  if (warned.has(key)) return;
  warned.add(key);
  console.warn(msg, { model });
}

/** A price-list pricer (§4).
 *
 *  A cache rate that is missing (or 0) is taken as not given: those tokens
 *  are priced at `inputPerMillion`, and a warning is logged once per model.
 *  Cache tokens are never free, and a price list that forgot them must not
 *  bill them at 0.
 *
 *  A model the table does not know is not priced. Its row is stored with no
 *  cost and `UsageTotals.unpriced` counts it, so a missing price shows up as
 *  a gap in the bill rather than as a silent zero. */
export function table(prices: PriceTable, currency: string = USD): Pricer {
  return {
    price(u: NewUsage): Cost | null {
      const p = lookupPrice(prices, u.model, u.modelId);
      if (!p) return null;
      // tokens / 1_000_000 × pricePerMillion is the cost in currency units,
      // and micros is that × 1_000_000. The two cancel: tokens ×
      // pricePerMillion IS the micro-unit cost, with no scaling in between.
      let cacheRead = p.cacheReadPerMillion ?? 0;
      let cacheWrite = p.cacheWritePerMillion ?? 0;
      if (!cacheRead && u.cacheReadInputTokens > 0) {
        cacheRead = p.inputPerMillion ?? 0;
        warnOnce(`${u.model}\u0000read`, 'price table has no cache read rate; cache reads priced as input', u.model);
      }
      if (!cacheWrite && u.cacheWriteInputTokens > 0) {
        cacheWrite = p.inputPerMillion ?? 0;
        warnOnce(`${u.model}\u0000write`, 'price table has no cache write rate; cache writes priced as input', u.model);
      }
      const micros =
        u.inputTokens * (p.inputPerMillion ?? 0) +
        u.cacheReadInputTokens * cacheRead +
        u.cacheWriteInputTokens * cacheWrite +
        u.outputTokens * (p.outputPerMillion ?? 0) +
        u.reasoningTokens * (p.reasoningPerMillion ?? 0);
      return { micros: Math.round(micros), currency, source: 'table' };
    },
  };
}

/** Pulls a cost out of what the provider attached to the finish: a gateway
 *  receipt, a billing header, whatever your provider sends. Return null when
 *  this call carried no receipt, and the next pricer in a chain gets its
 *  turn. */
export type ReceiptReader = (meta: Record<string, unknown>) => number | null;

/** Price a call from the number the provider already computed.
 *
 *  This is the most accurate pricer there is: no price list to keep in step
 *  with a provider's changes, and discounts and gateway markups are already
 *  inside the figure. */
export function receipt(read: ReceiptReader, currency: string = USD): Pricer {
  return {
    price(u: NewUsage): Cost | null {
      if (!u.providerMetadata) return null;
      const micros = read(u.providerMetadata);
      // No receipt, or one that makes no sense (not a number, negative, not
      // finite): not believed, and the call is left for the next pricer.
      if (typeof micros !== 'number' || !Number.isFinite(micros) || micros < 0) return null;
      return { micros: Math.round(micros), currency, source: 'receipt' };
    },
  };
}

/** Try each pricer in order and take the first answer. Put the accurate one
 *  first and the fallback last:
 *
 *      chain(
 *        receipt(gatewayReceiptMicros), // the real figure, when sent
 *        table(priceList),              // otherwise the price list
 *      )
 *
 *  A pricer that throws is skipped rather than believed. If none answers,
 *  chain rethrows the first error it saw, or returns null when they simply
 *  had nothing to say. */
export function chain(...pricers: Array<Pricer | undefined>): Pricer {
  return {
    async price(u: NewUsage): Promise<Cost | null> {
      let firstError: unknown;
      for (const p of pricers) {
        if (!p) continue;
        try {
          const cost = await p.price(u);
          if (cost) return cost;
        } catch (err) {
          firstError ??= err;
        }
      }
      if (firstError !== undefined) throw firstError;
      return null;
    },
  };
}

/** Price every call the same, whatever the model. Useful for a credit system
 *  that charges per call rather than per token, and for tests. */
export function fixed(micros: number, currency: string = USD): Pricer {
  return { price: () => ({ micros, currency, source: 'table' }) };
}

/** Convert a currency amount to micros: `micros(0.25)` is 250_000. */
export const micros = (amount: number): number => Math.round(amount * 1_000_000);

/** Convert micros back to a currency amount: `amount(250_000)` is 0.25. */
export const amount = (m: number): number => m / 1_000_000;

/** Render micros for a human: '0.2500 USD'. Display only; never do arithmetic
 *  on the string. */
export const format = (m: number, currency: string = USD): string =>
  `${amount(m).toFixed(4)} ${currency}`;
