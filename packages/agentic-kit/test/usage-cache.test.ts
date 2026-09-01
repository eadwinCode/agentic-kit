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

  // A cache hit is reported ONLY in provider metadata — `usage` has no field
  // for it. Attributing from usage alone reports zero cache hits forever, and
  // books every cached prompt at the full input price.
  it('reads an OpenAI cache hit out of provider metadata', () => {
    const a = attributeTokens(
      { promptTokens: 1200, completionTokens: 50, totalTokens: 1250 },
      { openai: { cachedPromptTokens: 1024 } },
    );
    // OpenAI's promptTokens INCLUDES the cached ones, so the fresh input is the
    // difference. Adding them instead would bill 2224 input tokens on a 1250
    // token call.
    expect(a.inputTokens).toBe(176);
    expect(a.cachedInputTokens).toBe(1024);
    expect(a.outputTokens).toBe(50);
  });

  it('reads an Anthropic cache hit, which sits alongside input', () => {
    const a = attributeTokens(
      { promptTokens: 30, completionTokens: 12 },
      { anthropic: { cacheReadInputTokens: 4000, cacheCreationInputTokens: 0 } },
    );
    // Anthropic does NOT fold cache reads into input, so input stays as reported
    expect(a.inputTokens).toBe(30);
    expect(a.cachedInputTokens).toBe(4000);
    expect(a.totalTokens).toBe(4042);
  });

  it('never lets a cached count drive input negative', () => {
    const a = attributeTokens(
      { promptTokens: 100, completionTokens: 5 },
      { openai: { cachedPromptTokens: 500 } },
    );
    expect(a.inputTokens).toBe(0);
  });

  it('ignores metadata from a provider that reports no cache', () => {
    const a = attributeTokens(
      { promptTokens: 10, completionTokens: 5 },
      { openai: { someOtherField: 'x' } },
    );
    expect(a.cachedInputTokens).toBe(0);
    expect(a.inputTokens).toBe(10);
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

    // cacheControl attaches to the last content part of the marked messages.
    // The SDK reads `providerOptions ?? experimental_providerMetadata`; a bare
    // `providerMetadata` on a part is read by nothing, so asserting only that
    // one passes while the breakpoint does nothing.
    expect((out[0] as any).providerOptions).toEqual(stamp);
    expect((out.at(-1) as any).content.at(-1).providerOptions).toEqual(stamp);
    expect((out[0] as any).experimental_providerMetadata).toEqual(stamp);
    expect((out.at(-1) as any).content.at(-1).experimental_providerMetadata).toEqual(stamp);
    // Middle messages are untouched
    expect((out[1] as any).providerOptions).toBeUndefined();
    expect((out[2] as any).providerOptions).toBeUndefined();
  });

  // A system message's content MUST stay a string — the SDK validates the
  // prompt and rejects parts there, so splitting it the way a user message
  // allows makes every run throw InvalidPromptError. It carries its marker on
  // the message instead.
  it('stamps a system message on the message, leaving its content a string', () => {
    const out = markPromptCaching([
      { role: 'system', content: 'you are helpful' },
      { role: 'user', content: 'hi' },
    ]);
    const stamp = { anthropic: { cacheControl: { type: 'ephemeral' } } };

    expect((out[0] as any).content).toBe('you are helpful');
    expect((out[0] as any).providerOptions).toEqual(stamp);
    expect((out[0] as any).experimental_providerMetadata).toEqual(stamp);
    // ... while a user message still stamps its last content part
    expect(Array.isArray((out[1] as any).content)).toBe(true);
    expect((out[1] as any).content.at(-1).providerOptions).toEqual(stamp);
  });

  it('converts string content to a text part when stamping', () => {
    const out = markPromptCaching([{ role: 'user', content: 'hi' }]);
    const content = (out[0] as any).content;
    expect(Array.isArray(content)).toBe(true);
    expect(content.at(-1).providerOptions).toEqual({
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
