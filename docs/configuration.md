# Configuration reference

## `setupAgentCore(options)`

```ts
const runtime = await setupAgentCore({
  storage,          // required — your database
  queue,            // required — durable dispatch
  bus,              // required — live fan-out
  kv,               // required — hot state
  resolveModel,     // required — registry key → { instance, contextWindow, modelId }
  admin,            // optional — defaults to SQLite, or Postgres via env
  pricer,           // optional — prices every model call (§4)
  log,              // optional — defaults to console
  config,           // optional — everything below
});
```

`await` matters: the operational store is opened here, and a store that cannot
be opened should be a startup error.

## `config` — every setting

### Human in the loop

| Setting | Default | Meaning |
| :--- | ---: | :--- |
| `hitlTtlMs` | `900000` (15 min) | How long a parked approval stays answerable. On expiry it resolves as a timeout denial and the run continues. |
| `reclaimGraceMs` | `60000` | Grace beyond the TTL before orphan reclamation may claim a thread. |

### Run limits (Go runtime additions)

| Key | Default | What it does |
| :--- | :--- | :--- |
| `RunLockLease` | `2m` | The per-thread run lock's lease. The worker renews it every sixth of the lease while it holds it, so an expired lock means a dead worker. A worker whose renewals have failed for two thirds of the lease stops. A job blocked by a held lock is redriven with a growing delay and gives up only after waiting at least one lease. |
| `RunRetryBackoff` / `RunRetryBackoffMax` | `5s` / `2m` | A failed run waits this long before its first retry, twice as long each time after, with jitter. |
| `StepTimeout` | off | Bounds one model round trip; a step past it fails and the run takes the retry policy. |
| `SegmentTimeout` | off | Bounds one worker segment; a segment past it settles the run `FAILED` with a reason. |
| `MaxQueueWait` | off | A job picked up later than this fails with the reason instead of running. |
| `MaxQueueDepth` | off | A new run is refused (`RefusedQueueFull`) before anything is written once this many jobs are ready and waiting. |

### Run limits

| Setting | Default | Meaning |
| :--- | ---: | :--- |
| `maxSteps` | `25` | Model round trips per run. The safety cap against runaway loops. |
| `tokenBudget` | `undefined` | Default per-run token budget. Unbounded apart from `maxSteps`. |
| `costBudgetMicros` | `undefined` | Default per-run money cap, in millionths of the pricer's currency. Needs a `pricer`. See [Cost and pricing](./cost-and-pricing.md). |
| `runMaxAttempts` | `3` | Queue redrive attempts before a run finalizes `FAILED`. |
| `stopPollMs` | `500` | How often a running worker re-reads the stop signal. Also the window in which it notices a newer run replaced it. |
| `runRetryBackoffMs` / `runRetryBackoffMaxMs` | `5000` / `120000` | A failed run waits this long before its first retry, twice as long each time after, with jitter. The thread shows `QUEUED` while it waits. |
| `runRedriveDelaySeconds` | `2` | First delay before re-dispatching a job that found the run lock held. It doubles on each try, up to the lease. |
| `runLockLeaseSeconds` | `120` (2 min) | Lease on the per-thread run lock. The worker renews it every sixth of the lease while it holds it, so an expired lock means a dead worker. A job blocked by a held lock gives up only after waiting at least one lease. Parked approvals hold no lock. |

### Subagents

| Setting | Default | Meaning |
| :--- | ---: | :--- |
| `subagentMaxDepth` | `2` | Nesting cap. |
| `subagentMaxConcurrent` | `3` | Children running at once per run. |
| `subagentMaxSteps` | `10` | Model round trips per child. |
| `subagentResultCapChars` | `8000` | Characters of a child's result handed to the parent. |

### Context and caching

| Setting | Default | Meaning |
| :--- | ---: | :--- |
| `contextCeilingTokens` | `265000` | Universal ceiling. |
| `contextOutputReserveTokens` | `16000` | Held back for the completion. |
| `compactionTrigger` | `0.8` | Compact past this share of budget. |
| `contextTailShare` | `0.25` | Share of budget kept verbatim. |
| `compactionModel` | `'gpt-4o-mini'` | Registry key of the cheap model that writes the summary. Resolved through your own `resolveModel`, so a registry without that key must name its own. |
| `promptCaching` | `true` | Stamp cache breakpoints on the stable prefix. |
| `nativeWindows` | — | Per-model windows below the ceiling. A `contextWindow` from `resolveModel` wins. |

### Operational history

| Setting | Default | Meaning |
| :--- | ---: | :--- |
| `recordPayloads` | `true` | Record prompts, step text and tool arguments/results. Turn off when payloads carry sensitive data. |
| `payloadCapChars` | `2000` | Characters kept per recorded value. |

### Provider options

| Setting | Default | Meaning |
| :--- | ---: | :--- |
| `providerOptions` | — | Provider-specific settings applied to every run, keyed by provider namespace. An agent spec overrides it, and a run input overrides both. See [Provider options](./provider-options.md). |

### Billing

| Setting | Default | Meaning |
| :--- | ---: | :--- |
| `billingPreCheck` | — | `({ threadId, state, publishEvent }) => { ok, error? }`. Reject a run before it costs anything; the check can publish on the thread, and the platform publishes `RUN_REFUSED`. |

Pricing is not a `config` setting — `pricer` sits on `setupAgentCore` beside the
ports, because it decides what goes into the store rather than how the loop
behaves. See [Cost and pricing](./cost-and-pricing.md).

## Environment variables

| Variable | Read by | Meaning |
| :--- | :--- | :--- |
| `AGENTIC_KIT_ADMIN_DATABASE_URL` | core | Postgres for the operational store. Unset means SQLite. |
| `AGENTIC_KIT_ADMIN_DB` | core | Path to the SQLite operational store. |
| `TEST_ADMIN_PG` | test suite | Postgres URL for the admin store tests. |
| `SKIP_PG_TESTS` | test suite | Set to `1` to opt out of them deliberately. |

Everything else — provider keys, your database URL, queue credentials — is read
by *your* code, not the library's.

## `useAgentThread(options)`

See [React](./react.md#everything-else-you-can-change) for the full table. In
brief: `routes`, `baseUrl`, `fetch`, `headers`, `openStream`, `defaultModel`,
`persistence`, `labels`, `format`, `onEvent`, `threadsRefreshMs`,
`loadThreadsOnMount`, `initialThreadId`.
