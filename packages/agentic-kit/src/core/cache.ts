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

function stampCacheBreakpoint<T extends CacheableMessage>(message: T, provider: string): T {
  const parts = toParts(message.content);
  if (parts.length === 0) return message;
  const stamped = [...parts];
  const last = stamped[stamped.length - 1]!;
  stamped[stamped.length - 1] = {
    ...last,
    providerMetadata: { [provider]: { cacheControl: { type: 'ephemeral' } } },
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
