/** Prompt caching (§2.6): providers like Anthropic cache a prompt prefix only
 *  when it carries explicit cache breakpoints; OpenAI-family models cache
 *  automatically for prompts ≥1024 tokens and ignore these markers.
 *
 *  `markPromptCaching` stamps ephemeral cache breakpoints on the stable
 *  prefix of a prompt — the system message and the tail message of the
 *  conversation-so-far — so the next run with the same prefix is served from
 *  the provider cache (tracked as cachedInputTokens, §4 usage attribution).
 *
 *  The stamp uses the AI SDK's provider-metadata channel; unknown providers
 *  ignore it harmlessly. */
export interface PromptCachingOptions {
  /** Provider namespace for the metadata stamp. Default: 'anthropic'. */
  provider?: string;
}

interface CacheablePart {
  type?: string;
  text?: string;
  [key: string]: unknown;
}

interface CacheableMessage {
  role: string;
  content: unknown;
  [key: string]: unknown;
}

const cacheStamp = (provider: string) => ({
  [provider]: { cacheControl: { type: 'ephemeral' } },
});

function toParts(content: unknown): CacheablePart[] {
  if (typeof content === 'string') return [{ type: 'text', text: content }];
  if (Array.isArray(content)) return content as CacheablePart[];
  return [];
}

/** Stamp the marker where the SDK will actually read it.
 *
 *  Two rules, and breaking either one is silent:
 *   - The SDK reads `providerOptions ?? experimental_providerMetadata`. A bare
 *     `providerMetadata` is read by nothing, so stamping that alone leaves the
 *     breakpoint inert — set, and never seen by a provider.
 *   - A SYSTEM message's content must stay a string; it carries its metadata on
 *     the message itself. Splitting it into parts the way a user message allows
 *     makes the whole prompt fail validation, and every run throws. */
function stampCacheBreakpoint<T extends CacheableMessage>(message: T, provider: string): T {
  if (message.role === 'system') {
    return {
      ...message,
      providerOptions: cacheStamp(provider),
      experimental_providerMetadata: cacheStamp(provider),
    };
  }

  const parts = toParts(message.content);
  if (parts.length === 0) return message;
  const stamped = [...parts];
  const last = stamped[stamped.length - 1]!;
  stamped[stamped.length - 1] = {
    ...last,
    providerOptions: cacheStamp(provider),
    experimental_providerMetadata: cacheStamp(provider),
  };
  return { ...message, content: stamped };
}

/** Mark the stable prefix of a prompt for provider-side caching:
 *  the system message (if any) plus the last message of the history. */
export function markPromptCaching<T extends CacheableMessage>(
  messages: T[],
  opts: { provider?: string } = {},
): T[] {
  const provider = opts.provider ?? 'anthropic';
  const out = messages.map((m) => ({ ...m }));

  const systemIndex = out.findIndex((m) => m.role === 'system');
  if (systemIndex >= 0) out[systemIndex] = stampCacheBreakpoint(out[systemIndex], provider);

  if (out.length > 0) {
    const last = out.length - 1;
    out[last] = stampCacheBreakpoint(out[last], provider);
  }

  return out;
}
