import { describe, expect, it } from 'bun:test';
import * as pricing from '../src/pricing.js';
import type { NewUsage } from '../src/core/types.js';
import type { Pricer } from '../src/ports/runtime.js';

// The shipped pricers (§4). The same cases run in the Go package
// (pricing/pricing_test.go).

const table = pricing.table({
  // Anthropic's published Sonnet prices, per million tokens.
  'claude-sonnet-4': {
    inputPerMillion: 3, cacheReadPerMillion: 0.3, cacheWritePerMillion: 3.75, outputPerMillion: 15,
  },
  'gpt-4o-2024-11-20': { inputPerMillion: 2.5, outputPerMillion: 10 },
});

const usage = (u: Partial<NewUsage>): NewUsage => ({
  kind: 'step', step: 1, outcome: 'finished',
  inputTokens: 0, cacheReadInputTokens: 0, cacheWriteInputTokens: 0, outputTokens: 0,
  reasoningTokens: 0, totalTokens: 0, ...u,
});
const price = (p: Pricer, u: Partial<NewUsage>) => Promise.resolve(p.price(usage(u)));

describe('table', () => {
  it('prices per million tokens into micros', async () => {
    expect((await price(table, { model: 'claude-sonnet-4', inputTokens: 1_000_000 }))?.micros).toBe(3_000_000);
    // 12_000×3 + 40_000×0.30 + 8_000×3.75 + 900×15 = 36_000 + 12_000 + 30_000 + 13_500
    const got = await price(table, {
      model: 'claude-sonnet-4', inputTokens: 12_000, cacheReadInputTokens: 40_000,
      cacheWriteInputTokens: 8_000, outputTokens: 900,
    });
    expect(got).toEqual({ micros: 91_500, currency: 'USD', source: 'table' });
  });

  it('falls back to the wire id then the base key', async () => {
    expect((await price(table, { model: 'fast', modelId: 'gpt-4o-2024-11-20', outputTokens: 1_000_000 }))?.micros)
      .toBe(10_000_000);
    expect((await price(table, { model: 'claude-sonnet-4@high', inputTokens: 1_000_000 }))?.micros).toBe(3_000_000);
    // A model nobody knows is not priced, rather than priced at zero.
    expect(await price(table, { model: 'who-knows', inputTokens: 1_000 })).toBeNull();
  });

  it('a missing cache rate is priced as input', async () => {
    // gpt-4o has no cache rates in this table: its cache reads and writes are
    // priced as input, never at 0.
    const got = await price(table, {
      model: 'gpt-4o-2024-11-20', cacheReadInputTokens: 1_000_000, cacheWriteInputTokens: 1_000_000,
    });
    expect(got?.micros).toBe(5_000_000); // 2 × $2.50
  });

  it('a model named like an object key is not found', async () => {
    for (const name of ['constructor', 'toString', '__proto__']) {
      expect(await price(table, { model: name, inputTokens: 1_000 })).toBeNull();
    }
  });
});

describe('receipt', () => {
  it('reads what the provider already computed', async () => {
    const p = pricing.receipt((meta) => {
      const headers = meta.responseHeaders as Record<string, string> | undefined;
      return headers?.['x-cost-micros'] === '4200' ? 4200 : null;
    });
    const got = await price(p, { providerMetadata: { responseHeaders: { 'x-cost-micros': '4200' } } });
    expect(got?.micros).toBe(4200);
    expect(got?.source).toBe('receipt');
    // No receipt on this call: say nothing, so the next pricer can try.
    expect(await price(p, { providerMetadata: {} })).toBeNull();
  });

  it('a negative receipt is left unpriced', async () => {
    expect(await price(pricing.receipt(() => -5), { providerMetadata: { x: 1 } })).toBeNull();
    // Nor one that is not a finite number; a fraction is rounded to whole micros.
    expect(await price(pricing.receipt(() => Number.NaN), { providerMetadata: { x: 1 } })).toBeNull();
    expect((await price(pricing.receipt(() => 41.6), { providerMetadata: { x: 1 } }))?.micros).toBe(42);
  });
});

describe('chain', () => {
  it('takes the first answer and skips failures', async () => {
    const boom: Pricer = { price: () => { throw new Error('price service down'); } };
    const silent: Pricer = { price: () => null };
    expect((await price(pricing.chain(boom, silent, table), { model: 'claude-sonnet-4', inputTokens: 1_000_000 }))?.micros)
      .toBe(3_000_000);
    // Nobody could price it: the error is what comes back.
    await expect(price(pricing.chain(boom, silent), {})).rejects.toThrow('price service down');
    // Nothing to say and nothing wrong is not an error.
    expect(await price(pricing.chain(silent), {})).toBeNull();
  });
});

describe('money helpers', () => {
  it('micros round trip', () => {
    expect(pricing.micros(0.25)).toBe(250_000);
    expect(pricing.amount(250_000)).toBe(0.25);
    expect(pricing.format(250_000)).toBe('0.2500 USD');
  });
});
