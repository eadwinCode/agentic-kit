# Context and tokens

## Compaction

Before every run segment the platform checks whether the history still fits, and
summarizes the older part if it does not. History always fits the model's
budget; you never have to prune by hand.

```
budget = min(model contextWindow, contextCeilingTokens) - contextOutputReserveTokens

prompt = the latest summary + the messages after the last one it covers
if estimate(prompt) > budget × compactionTrigger:
    keep the last  budget × contextTailShare  verbatim, starting at a user turn
    summarize everything before it (the last summary included) into one new
    system message that records the last message it covers
```

The summary records which message it covers up to, and from then on the
prompt carries the summary and only what came after. So a thread compacts
once each time it grows past the trigger, never on every run after, and the
summary call only ever reads the last summary plus the turns since. The kept
tail always starts at a user turn, so it can never open on a tool result whose
call went into the summary. A single turn larger than the whole window is sent
as it is, with a warning logged: the estimate is rough, and the provider
decides.

| Setting | Default | Meaning |
| :--- | :--- | :--- |
| `contextCeilingTokens` | 265000 | Universal ceiling |
| `contextOutputReserveTokens` | 16000 | Held back for the completion |
| `compactionTrigger` | 0.8 | Compact past this share of budget |
| `contextTailShare` | 0.25 | Share kept verbatim |
| `compactionModel` | gpt-4o-mini | Registry key of the model that writes the summary |

The summary is written by a cheap model, and that model is resolved through
your own `resolveModel` — so a registry that has never heard of `gpt-4o-mini`
must name its own, or the first thread to outgrow its window fails:

```ts
config: { compactionModel: 'claude-haiku' }
```

The call is billed like any other, under `kind: 'compaction'`, so what the
platform's own housekeeping costs is visible on its own (see
[Cost and pricing](./cost-and-pricing.md)). It is billed to the run it served,
so it is on that run's bill and counts toward its cost cap, and a stop cancels
it like any other call of the run.

The summary is persisted as a `system` message and a `CONTEXT_COMPACTED` entry
is added to the thread record. Reading current load:

```ts
const usage = await runtime.getThreadUsage(threadId);
usage.context; // { usedTokens, budgetTokens, compactAtTokens, messages }
```

### When the provider says the prompt is too long

The estimate is rough, so a provider can still refuse a prompt the platform
thought would fit ("prompt is too long", `context_length_exceeded`, …). The
run does not fail on it: the thread is compacted at once, keeping only the
latest user turn verbatim, and the run goes on from the steps it already
saved. This happens once per segment; a second refusal fails the step like
any other error. `isContextOverflow(err)` (Go: `core.IsContextOverflow`) is
the check.

### Compacting on request

`compactThread` (Go: `CompactThread`) summarizes a thread now, whatever its
size, for a `/compact` command. Only the latest user turn stays as it is. It
is refused while a run is queued or running, and the summary call is recorded
without a run.

```ts
const { compacted, reason } = await runtime.compactThread(threadId, state);
```

### A history larger than the summarizer

A thread that was never compacted can be larger than the compaction model's
own window. The older history then goes in parts of half that window, each
summarized on its own billed call, and the parts are merged the same way
until one summary is left. A single message larger than a part keeps its
start and end.

## Prompt caching

On by default. The engine stamps cache breakpoints on the stable prefix of the
prompt — the system prompt and the tail of the compacted history — so a provider
that supports marking serves the prefix from its cache.

```ts
config: { promptCaching: true }
```

Three details worth knowing, because each was a bug before it was a feature:

- **The system prompt is carried as a stamped message, not the SDK's
  `instructions` string.** That string reaches the provider with no metadata channel, so a
  system prompt passed that way can never hold a breakpoint — and it is usually
  the largest, most stable part of the prompt.
- **Nested runs are stamped too.** A child re-sends its whole brief and history
  on every step, which is exactly the shape caching rewards.
- **OpenAI models ignore the markers** and cache automatically at ≥1024 prompt
  tokens. The markers are for providers that require them, Anthropic in
  particular.

Turning it off (`promptCaching: false`) sends the system prompt as the SDK's
plain `instructions` parameter.

## Token attribution

One usage row per **model call** — a step of the main run, a step of a nested
run, a compaction pass — totalled per thread:

```ts
const { tokens } = await runtime.getThreadUsage(threadId);
// { inputTokens, cachedInputTokens, outputTokens, totalTokens,
//   costMicros, currency, unpriced, lines }
```

The money is there too once a pricer is configured, and `lines` breaks the
same spend down by agent and model. See [Cost and pricing](./cost-and-pricing.md).

### The part that bites

Providers disagree about what the prompt count means, and getting it wrong
doubles the bill:

| Provider | Reports | So input is |
| :--- | :--- | :--- |
| OpenAI | `promptTokens` **includes** the cached ones | `promptTokens − cached` |
| Anthropic | `cacheReadInputTokens` sits **alongside** input | as reported |

The library handles both. `totalTokens` is always `input + cached + output`,
worked out by the library rather than taken from the provider, whose own total
does not mean the same thing everywhere (Anthropic's leaves the cache reads
out).

Cache hits are reported **only** in provider metadata — the SDK's `usage` object
has no field for them. Any code that attributes spend from `usage` alone reports
zero cache hits for ever and books every cached prompt at full price.

## Budgets

```ts
await chat.run({ prompt: 'hi', tokenBudget: 50_000 });
```

Order: run input → agent spec → `config.tokenBudget`. Undefined means unbounded
apart from `maxSteps`. Spend is checked between steps against a ledger shared
with nested runs. The ledger counts the **whole run**: each segment starts from
what the run already spent (before a park, before a retry, and the
platform's compaction calls), so a run that parks three times does not get
three budgets.

There is a money cap in the same shape, once a pricer is configured:

```ts
await chat.run({ prompt: 'hi', costBudgetMicros: 250_000 }); // about $0.25
```

Same rules, same place, and `COST_BUDGET_EXHAUSTED` in place of
`TOKEN_BUDGET_EXHAUSTED`. See [Cost and pricing](./cost-and-pricing.md).

## Billing gates

Reject a run before it costs anything:

```ts
config: {
  billingPreCheck: async ({ threadId, state, publishEvent }) => {
    const org = await orgById(state.orgId);
    if (org.credits > 0) return { ok: true };
    await publishEvent('CREDIT_LIMIT', { resetAt: org.periodEndsAt }, { durable: true });
    return { ok: false, error: 'Out of credits' };
  },
}
```

A rejected run writes no message and returns `accepted: false` with your
error. It does publish: your own event, if the check sent one, and the
platform's `RUN_REFUSED` with the error, so the chat can show the refusal
where the user is looking rather than only in an HTTP response. A refusal
when the user sends is live only, so a user who keeps pressing Send does not
grow the thread; a refusal at pickup belongs to a run and is kept in the
thread record. Your own event is kept only with `{ durable: true }`, as above.

Mid-run, the budget is the credit check. When the run's cumulative spend
crosses `tokenBudget` between steps, the platform publishes
`TOKEN_BUDGET_EXHAUSTED` (`tokensUsed`, `tokenBudget`, `agentId`) and then
finalizes with `stopReason: 'token_budget'`.

With a pricer configured you can cap the number the account actually cares
about instead of deriving a token budget from it: pass the remaining credit as
`costBudgetMicros` and the run stops on `COST_BUDGET_EXHAUSTED` /
`stopReason: 'cost_budget'`.
