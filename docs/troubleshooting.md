# Troubleshooting

Symptoms first, since that is what you have.

## Runs

### A run is accepted but nothing happens

The job is on the queue and nothing is consuming it.

- Is the worker endpoint reachable from the queue? A local URL will not be.
- Does the queue's callback URL match the route you actually deployed?
- In development, is `INLINE_WORKER` set, or the `InlineQueue` bound with
  `queue.bind(...)`?

Log inside the worker handler before calling `handleJob`. If that line never
runs, the problem is delivery, not the runtime.

### Every job resolves to an unknown agent

The worker route imported the runtime from a module that does not also register
the agents. The registry lives in the runtime's closure, and frameworks that
give each route its own module instance will hand you a runtime with an empty
one.

Put `setupAgentCore` and every `create*Agent` call in one module.

### `run()` returns `accepted: false`

Either the thread has an active run — stop it first, or wait — or your
`billingPreCheck` rejected it. The `error` says which, and the thread carries a durable `RUN_REFUSED` event with the same text. No message was written.

### A thread is stuck in `RUNNING` with no worker

The worker died holding the run lock. It clears when the lease expires
(`runLockLeaseSeconds`). If that is routinely too long, shorten it — but keep it
above your longest run segment, or a second worker will start on a thread that is
still being advanced.

### A stopped run wedges the thread

Fixed, but worth knowing the shape: stop and a fast resend both wrote the same
thread-state key, so whichever landed second won. Runs now carry an identity, and
a worker whose id is no longer current retires without writing.

If you see this on a current version, check that your `Kv` implements its
conditional set correctly.

### A run hangs forever with no error

A provider error can leave the SDK's `result.text` promise unsettled while the
stream itself drains normally. The engine drains the full stream and rethrows
rather than awaiting a promise that will never settle. If you call the SDK
directly anywhere, do the same.

## Streaming and the UI

### A second tab does not see messages from the first

Every client learns about a turn from the bus. Check that:

- your bus adapter's subscribe actually delivers (test it in isolation),
- the stream route subscribes **before** replaying the durable log — otherwise
  events published in between are lost,
- the client is not filtering out `MESSAGE_APPENDED`.

### Text appears twice after a reconnect

A finished step's chunks are being replayed on top of its durable message.
`getThreadSnapshot` excludes stream chunks at or before the last committed step
for exactly this reason. If you built your own snapshot endpoint, apply the same
rule.

### Nothing arrives after a while, but the connection is open

Check your cursor. The stream must never send an event at or below the client's
last seen `seq`, and must never let a bus-only notice (`seq === 0`) advance it. A
single out-of-range `seq` will silently deafen a client to everything after it.

Proxies also buffer server-sent events. Set `Cache-Control: no-cache`, and
disable buffering at the proxy — `X-Accel-Buffering: no` for nginx.

### Reasoning never shows

Most models do not emit it. OpenAI's `gpt-4o` sends none, and the o-series
reports reasoning token counts without the text. Anthropic extended thinking and
DeepSeek R1 do send it.

## Approvals

### An approval never expires

Expiry is scheduled as a delayed queue message. If your queue drops
`delaySeconds`, it never fires.

Test the delayed path specifically. In-memory queues accept every option, so a
green suite proves nothing here — QStash, for one, rejects delay headers on its
enqueue path but accepts them on publish.

### `respond` returns `delivered: false`

The wait is gone: expired, already answered, or the thread moved on. Show it as
"too late" rather than an error.

### Answering one approval does not resume the run

Correct, if a sibling is still open. The thread stays `WAITING_FOR_INPUT` until
every open approval is settled. Requests may be answered in any order.

## Tokens and caching

### `cachedInputTokens` is always zero

Either the model is not caching, or something is attributing spend from the
SDK's `usage` object alone. Cache hits appear **only** in provider metadata —
`usage` has no field for them.

For a provider that requires explicit markers, also check `promptCaching` is on
and that the prefix is genuinely stable between calls.

### Token totals look doubled

Provider conventions differ: OpenAI's `promptTokens` includes the cached ones,
Anthropic's `cacheReadInputTokens` sits alongside input. Adding both on OpenAI
double-counts. If you compute your own totals, handle each separately.

## Storage

### Two workers advanced the same thread

`threads.claimState` is not atomic. It must be one conditional `UPDATE` with
exactly one winner. Read-then-write passes tests and fails in production.

### A subagent's turns leak into the main conversation

`messages.list` was called unscoped where it needed `{ agentId: null }`.
Compaction and the edit lookup both require the main agent's stream; unscoped,
context isolation is gone.

### Postgres refuses new connections in development

Your runtime module is re-evaluated on hot reload and opens a new pool each
time. Cache the client on `globalThis` in development.

## Packaging

### The package imports in the repo but not when installed

Check that the paths in `exports` and `main` match what the build actually
emits. A workspace can resolve straight to source and hide a broken export map
entirely.

Verify by packing and installing into a throwaway project — the repository's CI
does exactly this, in `.github/scripts/verify-package.sh`. Wipe `node_modules`
and the lockfile between attempts: the same name and version otherwise reuses a
cached install and passes for the wrong reason.

### `node:sqlite` will not resolve in a bundler

A dynamic import with a variable specifier becomes a context module in webpack.
The package marks it `webpackIgnore`, so no consumer configuration is needed. If
you have added `externals` entries for it, try removing them.

## Still stuck

Open an issue with the thread's event log (`runtime.events.since(threadId, -1)`)
and the run record (`runtime.admin.getRun(runId)`). Between them they usually
show exactly where a run stopped doing what you expected:
<https://github.com/eadwinCode/agentic-kit/issues>
