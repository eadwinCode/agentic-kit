# Cost and pricing

The platform already writes a usage row for every model call it makes. Give it
a **pricer** and those rows carry the money too — so you read spend from the
store the engine already fills, and never keep a second table in step with it.

```ts
import { pricing, setupAgentCore } from 'agentenkit';

const runtime = await setupAgentCore({
  storage, queue, bus, kv, resolveModel,
  pricer: pricing.table({
    'gpt-4o': { inputPerMillion: 2.5, cacheReadPerMillion: 1.25, outputPerMillion: 10 },
  }),
});
```

```go
rt, err := agentenkit.SetupAgentCore(ctx, agentenkit.RuntimeOptions{
    Storage: storage, Queue: queue, Bus: bus, Kv: kv, ResolveModel: resolveModel,
    Pricer: pricing.Table{
        "gpt-4o": {InputPerMillion: 2.5, CacheReadPerMillion: 1.25, OutputPerMillion: 10},
    },
})
```

That is the whole setup. From here every read that reports tokens also reports
money.

## One row per model call

A usage row is one **call to a model**: a step of the main run, a step of a
nested run, a compaction pass, streamed or not, finished or cut short. The row
carries everything pricing needs, so a pricer never has to reach back into the
run to find out what happened.

| Field | What it holds |
| :--- | :--- |
| `runId` | The **dispatched** run this call is billed to. A nested run's calls carry their parent's id, so one run's whole bill is one query. |
| `agentId` / `agentName` | Which stream made the call — empty `agentId` is the main run — and the name a bill line should say. |
| `kind` | `step`, or `compaction` for the platform's own housekeeping call. |
| `step` | The 1-based iteration inside its loop. `0` for a compaction. |
| `model` / `modelId` | The registry key the call asked for, and the wire id it resolved to. |
| `inputTokens`, `cacheReadInputTokens`, `cacheWriteInputTokens`, `outputTokens`, `reasoningTokens` | The counters. |
| `outcome` | `finished`, `aborted` (a stop cut it mid-stream) or `error`. |
| `estimated` | The tokens were estimated, because no finish ever arrived. |
| `providerMetadata` | What the provider attached to the finish, plus `responseId` and `responseHeaders`. |
| `cost` | `{ micros, currency, source }`, filled by the pricer. Absent means unpriced. |

`totalTokens` is `input + cache reads + output` — the counter the token budget
is measured against. Cache **writes** and reasoning tokens sit outside it on
purpose: a cache write is a separate line on the provider's bill, and reasoning
tokens are usually already inside the output count. Both are on the row so a
pricer can charge them properly.

## Money in micros

Cost is an integer number of **micros**: millionths of one currency unit.
`1_000_000` micros is one dollar when the currency is `USD`. Money summed as a
float drifts; money summed as an integer does not.

```ts
pricing.micros(0.25);   // 250_000
pricing.amount(250_000) // 0.25
pricing.format(250_000) // '0.2500 USD'
```

Price in **one currency per deployment**. Totals are summed, never converted.
If rows in two currencies do meet on one thread, a total is reported in the
first currency seen and the rows in any other currency count as `unpriced`,
so the figure stays a floor in one unit rather than a sum of two.

## The three pricers that ship

### `table` — a price list

The common case. Prices are per **million** tokens, in the currency you are
working in, so a list can be typed straight off a provider's pricing page.

```ts
const prices = pricing.table({
  'claude-sonnet-4': {
    inputPerMillion: 3,
    cacheReadPerMillion: 0.3,
    cacheWritePerMillion: 3.75,
    outputPerMillion: 15,
  },
});
```

A lookup tries the registry key, then the wire id from `resolveModel`, then the
key with any `@variant` suffix removed — so `claude-sonnet-4@high` finds
`claude-sonnet-4`.

**A model the table does not know is stored unpriced, not priced at zero.** A
missing price shows up as a gap in the bill (`unpriced` above zero) rather than
as free work.

`reasoningPerMillion` defaults to zero deliberately. Most providers already
count reasoning tokens inside the output count, so charging them again bills the
same tokens twice. Set it only when your provider reports them separately.

### `receipt` — the number the provider already computed

The most accurate pricer there is: no price list to keep in step with a
provider's changes, and discounts and gateway markups are already inside the
figure. Gateways usually return it in a response header, which the platform puts
on the row under `responseHeaders`.

```ts
const fromGateway = pricing.receipt((meta) => {
  const headers = meta.responseHeaders as Record<string, string> | undefined;
  const cents = headers?.['x-gateway-cost-cents'];
  return cents ? Math.round(Number(cents) * 10_000) : null; // null: no receipt here
});
```

```go
fromGateway := pricing.Receipt(func(meta map[string]any) (int64, bool) {
    headers, ok := meta["responseHeaders"].(map[string]string)
    if !ok {
        return 0, false // no receipt on this call
    }
    cents, err := strconv.ParseFloat(headers["x-gateway-cost-cents"], 64)
    if err != nil {
        return 0, false
    }
    return int64(cents * 10_000), true
})
```

### `chain` — the accurate one first, the fallback last

```ts
pricer: pricing.chain(
  fromGateway,            // the real figure, when the gateway sent one
  pricing.table(prices),  // otherwise the daily price list
);
```

The first pricer that answers wins. A pricer that fails is skipped rather than
believed, and if none answers the call is stored unpriced.

### Writing your own

A pricer is one method. In Go it is a `ports.Pricer`, or `ports.PricerFunc`
around a plain function.

```ts
const perCall: Pricer = { price: (u) => ({ micros: 500, currency: 'USD', source: 'estimate' }) };
```

It runs on the run's own path, so keep it fast and side-effect free. A price
list lookup is the intended shape; a network call is not.

## Reading spend back

One method, one filter. An empty filter reads the whole thread; a run id reads
one dispatched run, nested runs included.

```ts
const thread = await storage.usage.total(threadId, {});
const run    = await storage.usage.total(threadId, { runId });
```

Both return the counters, the money, and **lines** — the same spend grouped by
agent and model, which is the shape a bill wants:

```ts
{
  inputTokens: 1_240, cachedInputTokens: 8_000, outputTokens: 310, totalTokens: 9_550,
  costMicros: 12_500, currency: 'USD',
  unpriced: 0,          // calls with no cost: above zero, costMicros is a floor
  lines: [
    { agentId: null, agentName: 'chat', model: 'gpt-4o', modelId: 'gpt-4o-2024-11-20',
      inputTokens: 1_000, cacheReadInputTokens: 8_000, cacheWriteInputTokens: 0,
      outputTokens: 250, reasoningTokens: 0,
      calls: 4, estimated: 0, costMicros: 10_000 },
    { agentId: 'sub_1', agentName: 'researcher', /* … */ calls: 1, costMicros: 2_500 },
  ],
}
```

`unpriced` is what tells "this thread spent nothing" apart from "nobody priced
this thread". Do not read a zero cost as free work without checking it.

A hook that bills from `RunFinishInfo.usage` has one more thing to check: in
Go, `UsageErr` is set when the platform could not read the run's rows back,
and `Usage` is then zero-valued. Refuse to settle in that case (return the
error from `OnSettle`) rather than charging nothing; the run fails and can be
retried instead of going free.

The same totals reach you three other ways:

- `getThreadUsage(threadId)` → `usage.tokens`, for a thread header.
- `admin.getRun(runId)` → `detail.usage`, for spend per run.
- a spec's `onFinish` → `info.usage`, for billing at settle time.

## Billing at settle time

`onFinish` fires once, after the platform has finalized the run, with the whole
run's bill already read back and grouped. A credit system charges straight off
the lines:

```ts
const chat = runtime.createStreamTextAgent({
  name: 'chat',
  model: 'gpt-4o',
  onFinish: async (info) => {
    await credits.recordAndBill({
      idempotencyKey: info.runId!,          // at-least-once dispatch: be idempotent
      entries: info.usage.lines.map((line) => ({
        agent: line.agentName,
        model: line.modelId,
        inputTokens: line.inputTokens,
        outputTokens: line.outputTokens,
        cacheReadInputTokens: line.cacheReadInputTokens,
        cacheWriteInputTokens: line.cacheWriteInputTokens,
        costMicros: line.costMicros,
      })),
    });
  },
});
```

```go
OnFinish: func(info agentenkit.RunFinishInfo) {
    entries := make([]port.AIUsageEntry, 0, len(info.Usage.Lines))
    for _, line := range info.Usage.Lines {
        cost := line.CostMicros
        entries = append(entries, port.AIUsageEntry{
            Agent: line.AgentName, Model: line.ModelID,
            InputTokens: line.InputTokens, OutputTokens: line.OutputTokens,
            CacheReadInputTokens:  line.CacheReadInputTokens,
            CacheWriteInputTokens: line.CacheWriteInputTokens,
            CostMicros:            &cost,
        })
    }
    _ = credits.RecordAndBill(ctx, port.AIUsageRecord{IdempotencyKey: info.RunID, Entries: entries})
},
```

A run that parked and resumed finishes **once**, and `info.usage` covers every
segment, so a settle hook never has to add up parts itself.

## A budget in money

Once every call is priced, a run can be capped by spend the same way it is
capped by tokens:

```ts
await chat.run({ prompt: 'hi', costBudgetMicros: 250_000 }); // stop after about $0.25
```

```go
chat.Run(ctx, agentenkit.RunInput{Prompt: "hi", CostBudgetMicros: 250_000})
```

Order: run input → agent spec → `config.costBudgetMicros`. It is checked
**between steps**, so the step that crossed the line is kept in full and the
next one never starts. The platform then publishes `COST_BUDGET_EXHAUSTED`
(`costMicros`, `costBudgetMicros`, `currency`, `agentId`) and finalizes with
`stopReason: 'cost_budget'` — the same shape as the token cap. A money break is
not a stop: the run **completes**, and says why.

The check reads the run's spend back from the store rather than from a counter
in the worker, so a run that parked and resumed in another process keeps the cap
it started with, and a nested run's calls count against the same cap.

**It needs a pricer.** With none configured nothing is ever priced, so nothing
is ever spent and the cap can never fire.

## Calls that were cut off

A run stopped mid-stream was still billed by the provider for what it had
already produced. That call is recorded rather than dropped:

- `outcome` is `aborted` (a user stop) or `error` (the provider failed).
- The input count is the previous call's on this run — the prompt was certainly
  sent — or an estimate of the prompt when nothing has finished yet.
- The output count is an estimate over the text that actually streamed.
- `estimated` is `true`, and each line counts how many of its calls are guesses.

The estimate is the same one compaction measures context fill with, so the
context budget and the cost of a cut-off step can never disagree.

## What it means for storage

`UsageStore` keeps two methods, but `total` now takes a filter and the row is
wider. If you have written your own `Storage`:

```ts
usage: {
  record(threadId, usage, ctx): Promise<void>;
  total(threadId, filter, ctx): Promise<UsageTotals>;  // filter is new
}
```

- Store the row as given. An implementation never prices anything itself.
- `total` must honour `filter.runId`, and must return `lines` grouped by agent
  **and** model. If you cannot group in the database, `sumUsage(rows)` (TS) or
  `ports.UsageAggregator` (Go) does it for you and returns exactly the right
  shape.
- A NULL cost means unpriced. Count those into `unpriced`; do not store a zero.

The shipped adapters take care of themselves. Their tables gain the new columns
and a `(runId, createdAt)` index; SQLite and Postgres migrate an existing store
in place on construction. **Prisma users must migrate:** the `TokenUsage` model
in [the example schema](https://github.com/eadwinCode/agentic-kit/blob/main/examples/nextjs-app/prisma/schema.prisma)
has the new columns, and `prisma migrate dev` plus `prisma generate` picks them
up. `cachedInputTokens` keeps its name and its meaning — cache **reads** — so
existing rows still say what they always said.

## In the UI

`getThreadUsage` returns cost alongside tokens, so the React hook's `usage`
object carries it with no change of its own:

```tsx
import { formatCost } from 'use-agentenkit';

const { usage } = useAgentThread();
const cost = formatCost(usage?.tokens); // '$0.0125', '≥ $0.0125', or null
```

`formatCost` returns `null` when there is nothing to show, so a header leaves
the slot empty rather than printing `$0.00` for a server with no pricer
configured. It renders `≥` when some calls went unpriced and the figure is a
floor.
