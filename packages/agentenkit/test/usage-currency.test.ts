import { describe, expect, it } from 'bun:test';
import { attributeTokens, sumUsage, UsageMerger } from '../src/core/usage.js';
import type { NewUsage, UsageLine } from '../src/core/types.js';

// Workstream H: money is never converted. costMicros is one currency (the
// first seen); a call priced in another is left out of it and counted as
// unpriced, and costs keeps each currency's own total. The same cases run in
// the Go package (usage_currency_test.go).

const row = (cost?: { micros: number; currency: string }): NewUsage => ({
  agentName: 'a', model: 'm', kind: 'step', step: 1, outcome: 'finished',
  inputTokens: 10, cacheReadInputTokens: 0, cacheWriteInputTokens: 0, outputTokens: 0,
  reasoningTokens: 0, totalTokens: 10,
  ...(cost ? { cost: { ...cost, source: 'table' as const } } : {}),
});

describe('usage totals by currency (§4)', () => {
  it('the aggregator keeps one total per currency', () => {
    const total = sumUsage([row({ micros: 100, currency: 'USD' }), row({ micros: 900, currency: 'EUR' }), row()]);
    expect(total.currency).toBe('USD'); // the first currency seen
    expect(total.costMicros).toBe(100); // only that currency is summed
    expect(total.unpriced).toBe(2); // one unpriced, one in another currency
    expect(total.costs).toHaveLength(2);
    expect(total.costs![1]).toEqual({ currency: 'EUR', costMicros: 900, calls: 1 });
    expect(total.lines).toHaveLength(2); // a line per agent, model and currency
    expect(total.lines[0]!.calls).toBe(2); // the unpriced call joins the first line
    expect(total.lines[1]!.currency).toBe('EUR');
  });

  it('the line merger keeps one total per currency', () => {
    const m = new UsageMerger();
    const line = (costMicros: number, calls: number): UsageLine => ({
      agentName: 'a', model: 'm', inputTokens: 5, cacheReadInputTokens: 0, cacheWriteInputTokens: 0,
      outputTokens: 0, reasoningTokens: 0, calls, estimated: 0, costMicros,
    });
    m.add({ line: line(0, 1), currency: null, totalTokens: 5, unpriced: 1 }); // the line takes a currency later
    m.add({ line: line(100, 2), currency: 'USD', totalTokens: 5, unpriced: 0 });
    m.add({ line: line(700, 1), currency: 'EUR', totalTokens: 5, unpriced: 0 });
    const total = m.totals();
    expect(total.currency).toBe('USD');
    expect(total.costMicros).toBe(100);
    expect(total.unpriced).toBe(2);
    expect(total.totalTokens).toBe(15);
    expect(total.lines).toHaveLength(2);
    expect(total.lines[0]!.currency).toBe('USD'); // the unpriced line took the first currency
    expect(total.lines[0]!.calls).toBe(3);
    expect(total.costs![1]!.costMicros).toBe(700);
  });
});

describe('total tokens (§4)', () => {
  it('total tokens is always the sum', () => {
    // Anthropic's own total leaves the cache reads out; the count must not.
    const got = attributeTokens(
      { inputTokens: 10, outputTokens: 5, totalTokens: 15 },
      { anthropic: { cacheReadInputTokens: 30 } },
    );
    expect(got.totalTokens).toBe(45);
  });
});
