# Context and tokens

## Compaction

Before every run segment the platform checks whether the history still fits, and
summarizes the older part if it does not. History always fits the model's
budget; you never have to prune by hand.

```
budget = min(model contextWindow, contextCeilingTokens) - contextOutputReserveTokens

if estimate(history) > budget × compactionTrigger:
    keep the last  budget × contextTailShare  verbatim
    summarize everything before it into one system message
```

| Setting | Default | Meaning |
| :--- | :--- | :--- |
| `contextCeilingTokens` | 265000 | Universal ceiling |
| `contextOutputReserveTokens` | 16000 | Held back for the completion |
| `compactionTrigger` | 0.8 | Compact past this share of budget |
| `contextTailShare` | 0.25 | Share kept verbatim |

The summary is persisted as a `system` message and a `CONTEXT_COMPACTED` event
is published. Reading current load:

```ts
const usage = await runtime.getThreadUsage(threadId);
usage.context; // { usedTokens, budgetTokens, compactAtTokens, messages }
```

## Prompt caching

On by default. The engine stamps cache breakpoints on the stable prefix of the
prompt — the system prompt and the tail of the compacted history — so a provider
that supports marking serves the prefix from its cache.

```ts
config: { promptCaching: true }
```

Three details worth knowing, because each was a bug before it was a feature:

- **The system prompt is carried as a stamped message, not the SDK's `system:`
  string.** That string reaches the provider with no metadata channel, so a
  system prompt passed that way can never hold a breakpoint — and it is usually
  the largest, most stable part of the prompt.
- **Nested runs are stamped too.** A child re-sends its whole brief and history
  on every step, which is exactly the shape caching rewards.
- **OpenAI models ignore the markers** and cache automatically at ≥1024 prompt
  tokens. The markers are for providers that require them, Anthropic in
  particular.

Turning it off (`promptCaching: false`) restores the plain `system:` parameter.

## Token attribution

Four counters per run, recorded per step and totalled per thread:

```ts
const { tokens } = await runtime.getThreadUsage(threadId);
// { inputTokens, cachedInputTokens, outputTokens, totalTokens }
```

### The part that bites

Providers disagree about what the prompt count means, and getting it wrong
doubles the bill:

| Provider | Reports | So input is |
| :--- | :--- | :--- |
| OpenAI | `promptTokens` **includes** the cached ones | `promptTokens − cached` |
| Anthropic | `cacheReadInputTokens` sits **alongside** input | as reported |

The library handles both. `totalTokens` matches what the provider billed either
way.

Cache hits are reported **only** in provider metadata — the SDK's `usage` object
has no field for them. Any code that attributes spend from `usage` alone reports
zero cache hits for ever and books every cached prompt at full price.

## Budgets

```ts
await chat.run({ prompt: 'hi', tokenBudget: 50_000 });
```

Order: run input → agent spec → `config.tokenBudget`. Undefined means unbounded
apart from `maxSteps`. Spend is checked between steps against a ledger shared
with nested runs.

## Billing gates

Reject a run before it costs anything:

```ts
config: {
  billingPreCheck: async (threadId) => {
    const org = await orgForThread(threadId);
    return org.credits > 0 ? { ok: true } : { ok: false, error: 'Out of credits' };
  },
}
```

A rejected run writes nothing and returns `accepted: false` with your message.
