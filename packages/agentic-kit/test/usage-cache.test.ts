import { describe, expect, it } from 'bun:test';
import { attributeTokens, countTokens } from '../src/core/usage.js';
import { markPromptCaching } from '../src/core/cache.js';

describe('attributeTokens (§4)', () => {
  it('sums input + cached + output', () => {
    const a = attributeTokens({ inputTokens: 100, cachedInputTokens: 40, outputTokens: 50 });
    expect(a).toEqual({
      inputTokens: 100,
      cachedInputTokens: 40,
      outputTokens: 50,
      totalTokens: 190,
    });
  });

  it('maps the legacy prompt/completion naming', () => {
    const a = attributeTokens({ promptTokens: 10, completionTokens: 5 });
    expect(a.inputTokens).toBe(10);
    expect(a.outputTokens).toBe(5);
    expect(a.totalTokens).toBe(15);
  });

  it('prefers the provider total when reported', () => {
    const a = attributeTokens({ inputTokens: 10, outputTokens: 5, totalTokens: 15 });
    expect(a.totalTokens).toBe(15);
  });

  it('NaN-guards omitted usage (providers that omit streaming usage)', () => {
    const a = attributeTokens({ inputTokens: NaN, outputTokens: NaN });
    expect(a).toEqual({ inputTokens: 0, cachedInputTokens: 0, outputTokens: 0, totalTokens: 0 });
    expect(attributeTokens(undefined).totalTokens).toBe(0);
  });
});

describe('countTokens (§2.1 budget)', () => {
  it('counts input + cached + output, NaN-guarded', () => {
    expect(countTokens({ inputTokens: 10, cachedInputTokens: 5, outputTokens: 3 })).toBe(18);
    expect(countTokens({ inputTokens: NaN, completionTokens: NaN })).toBe(0);
    expect(countTokens(undefined)).toBe(0);
  });
});

describe('markPromptCaching (§2.6)', () => {
  const messages = [
    { role: 'system', content: 'you are helpful' },
    { role: 'user', content: 'hi' },
    { role: 'assistant', content: 'hello' },
    { role: 'user', content: 'bye' },
  ];

  it('stamps ephemeral breakpoints on the system message and the last message', () => {
    const out = markPromptCaching(messages);
    const stamp = { anthropic: { cacheControl: { type: 'ephemeral' } } };

    // cacheControl attaches to the last content part of the marked messages
    expect((out[0] as any).content.at(-1).providerMetadata).toEqual(stamp);
    expect((out.at(-1) as any).content.at(-1).providerMetadata).toEqual(stamp);
    // Middle messages are untouched
    expect((out[1] as any).providerMetadata).toBeUndefined();
    expect((out[2] as any).providerMetadata).toBeUndefined();
  });

  it('converts string content to a text part when stamping', () => {
    const out = markPromptCaching([{ role: 'user', content: 'hi' }]);
    const content = (out[0] as any).content;
    expect(Array.isArray(content)).toBe(true);
    expect(content.at(-1).providerMetadata).toEqual({
      anthropic: { cacheControl: { type: 'ephemeral' } },
    });
  });

  it('does not mutate the input messages', () => {
    const original = [{ role: 'user', content: 'hi' }];
    const copy = JSON.parse(JSON.stringify(original));
    markPromptCaching(original);
    expect(original).toEqual(copy);
  });
});
