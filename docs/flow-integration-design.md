# Design: running agentic-kit on Clevix Flow

Status: draft for discussion. First written 2026-10-27; updated 2026-10-28 for
Flow v0.7.0. Parts 1 to 3 (`FlowQueue`, `FlowBus`, `FlowStreams`) are built,
in TS and Go, and tested on a local engine (2026-10-29). Part 4 is not.

## Goal

[Clevix Flow](https://eadwincode.github.io/flow-sdk/) (`../flow-sdk`) is our own
durable workflow engine on Cloudflare. We want an app to be able to run
agentic-kit on Flow by picking Flow adapters in `setupAgentCore`, the same way it
picks QStash or Redis today. Both runtimes (TS and Go) get the same adapters.

## What changed with Flow v0.7.0

The first draft was written against Flow v0.6.5 and listed Flow changes we would
need. All of them shipped on 2026-10-28:

- **Flow SDK v0.7.0** (TS and Go), from the plan in
  `../flow-sdk/planing/agentic-kit-support.md` and the spec
  `../flow-sdk/planing/agentic-kit-spec.md`.
- **Engine v2.1.1**, released and deployed to production the same day. The new
  calls fail with `404` on an older engine.

| We asked for | Flow v0.7.0 has |
| :--- | :--- |
| Start a run and learn if a singleton key is taken | `flow.start` / `client.Start`: `{ started: false, reason: "singleton", activeRunId }` |
| Find and count runs | Run tags, `runs.list`, `runs.counts`, `runs.singleton` |
| Cancel at once, with no race | `runs.cancel` by run ID, singleton key or tag |
| Go `Subscribe` | `client.Realtime().Subscribe` (uses `github.com/coder/websocket`) |
| One socket for many channels | `channels` (1 to 100) on `token` and `subscribe` |
| Publish many at once | `publishMany` (1 to 100 messages) |
| Attempts in `onFailure` | `attempts` in the failure event |
| How long a sleep may be | Written down: no limit |
| Stored history for `RunStreams` | `flow.streams`: open, append, read from an offset, snapshot, close, delete |

So the adapters no longer need workarounds, and `RunStreams` on Flow is now
possible too.

## Summary

| Piece | Port | Works on Flow v0.7.0? |
| :--- | :--- | :--- |
| `FlowQueue` | `Queue` | Yes, every method, `find` and `stats` too |
| `FlowBus` | `EventBus` | Yes, TS and Go |
| `FlowStreams` | `RunStreams` | Yes, with two limits to handle (below) |
| Agents from Flow workflows | none (a helper) | Yes |
| `Storage`, `Kv` | | No, Flow has nothing like them. Out of scope |

No change to agentic-kit's core is needed for any of this. An app on Flow still
needs a database (`Storage`) and a kv store.

## Part 1: `FlowQueue` (the `Queue` port)

### How a job travels

1. `enqueue(job, opts)` starts one run of our function directly:

   ```ts
   const res = await flow.start('agentenkit-run', {
     data: { job, key: opts.key ?? job.dispatchId, priority, notBefore },
   });
   if (!res.started && res.reason === 'singleton') throw new DuplicateJobError(key);
   ```

   We do **not** pass `id`. The same `id` always lands on the same run, for as
   long as Flow keeps it (3 days after it ends). `reclaim:{runId}` can be needed
   twice for the same run, and the second one would be lost. The singleton key
   is free as soon as the run ends, which is what the port's key means.

2. The app serves one Flow function, made by a helper we ship:

   ```ts
   flow.createFunction(
     {
       id: 'agentenkit-run',
       singleton: { key: 'event.data.key', mode: 'skip' },
       tags: { runId: 'event.data.job.runId' },
       priority: { run: 'event.data.priority' },
       concurrency: { limit: N, key: 'event.data.job.partitionKey' },  // optional
       onFailure: ({ event, error, attempts }) => runtime.worker.handleDeadJob(/* … */),
     },
     { event: 'agentenkit/job' },
     async ({ event, step }) => {
       if (event.data.notBefore) await step.sleepUntil('delay', event.data.notBefore);
       return step.run('handle', () => runtime.worker.handleJob(event.data.job));
     },
   );
   ```

   `createFunction` needs a trigger, so it keeps `agentenkit/job`, but we never
   send that event. Runs come from `start`, and their event
   (`inngest/function.invoked`) carries the same `data`.

### Port to Flow, method by method

| Queue port | On Flow |
| :--- | :--- |
| `enqueue(job)` | `flow.start` of `agentenkit-run`. `started: false` with `"singleton"` throws `DuplicateJobError`. `"paused"` and `"not_found"` throw (see question 3). |
| `delaySeconds` | `step.sleepUntil(notBefore)` inside the run. A sleeping run still holds its singleton key, so dedupe and cancel keep working while the job waits. Sleeps have no upper limit. |
| `key` | `singleton` with `mode: 'skip'`. A job with no key uses its `dispatchId`, so unkeyed jobs never share one key. |
| `cancel(key)` | `flow.runs.cancel({ functionId, singletonKey: key })`. It reads the runs directly, so it has no lag and no race with a run being created. No `cancelOn` event is needed. |
| `priority` | `priority.run`, scaled to seconds and kept within ±600 (for example `priority * 60`, so `PRIORITY_LOW` is -600). It only does anything when the function has a concurrency limit. `start` applies it. |
| `partitionKey` | A concurrency limit keyed on it, so one tenant's backlog cannot starve the others. |
| Dead job | `onFailure` calls `runtime.worker.handleDeadJob(job, attempts, cause)`, with Flow's `attempts`. |
| `find(runId)` | The run tag `runId`, then `flow.runs.list({ functionId, tags: { runId }, status: <active ones>, limit: 1 })`. |
| `stats()` | `flow.runs.counts({ functionId })`: `ready` = queued, `delayed` = sleeping, `inFlight` = running, `dead` = failed since `since`. `oldestReadyMs` from the oldest queued run in `runs.list`. `paused` is `false`: `start` says when the function is paused. |
| Payload cap | A run's data is at most 1 MB. A bigger job throws `PayloadTooLargeError` before the call. |

### Things to watch

1. **14 minutes per call.** `timeouts.step` is at most 14 minutes. A park ends
   its segment, so HITL waits are fine. But one segment of nonstop work longer
   than that times out, and Flow retries the step. The run lock makes the retry
   a no-op, but on serverless the first call may be killed. So the helper has
   two modes:
   - `await` (default): `handleJob` runs inside the step. The dashboard shows
     each job and its result. Flow's streamed answers keep proxies from cutting
     long calls. Keep segments under the step timeout.
   - `detach`: the step answers at once and the work goes on in the
     background (`waitUntil`), as the QStash route in `examples/nextjs-app`
     does today. For long-lived servers.
2. **Retries in two places.** agentic-kit already retries a failed run itself
   (it enqueues a `retry` job). `handleJob` only throws for things a queue should
   retry: an unknown agent (`UnknownAgentError`) and infrastructure errors. So
   Flow's step retries cover delivery, not run failures. Keep Flow's default
   retries. A shutdown can throw `RetryAfterError` to hand the job back.
3. **`find` lags a few seconds.** `runs.list` reads an index that is a few
   seconds behind. Reclaim only asks `find` about a run whose lease has run out,
   so a few seconds do not matter. At worst reclaim enqueues a job that finds
   the run lock held and does nothing.
4. **Don't set `timeouts.finish`.** Time asleep counts toward it, and a park's
   expiry can be days or weeks away.
5. **Function ID.** `agentenkit-run` by default; an option lets an app change
   it. IDs are at most 64 characters and `onFailure` adds `-failure`, so the
   option allows at most 56.

The old items "dedupe is silent", "a cancel can miss" and "long sleeps" are gone:
`start`, `runs.cancel` and the written sleep limit fix them.

## Part 2: `FlowBus` (the `EventBus` port)

### How it works

- **Channel:** one per thread, `agentenkit.thread.<threadId>`. Channel names allow
  only letters, digits, `:` `_` `-` `.`, up to 256. A thread id with other
  characters is hashed into the name.
- **Topic:** one, `events`.
- **`publish(threadId, event)`:** `flow.realtime.publish({ channel, topic: 'events', data: event })`.
- **`subscribe(threadId, handler)`:** `flow.realtime.subscribe` (TS) or
  `client.Realtime().Subscribe` (Go). Both make a new token before every
  connect. Later subscribers to the same thread in the process share the
  socket. Each subscription gets its own queue, as in the other buses. The last
  one to leave closes the socket.

### Sharing sockets across threads

A socket can now hold up to 100 channels. But the channel list is fixed when the
socket connects, so adding a thread means a new socket or a reconnect. Start
simple: one socket per followed thread, as above. A process that follows many
threads at once can later group them, up to 100 per socket, and reconnect when
the set changes. The gap read covers the short reconnect.

### Why this is safe

The `EventBus` contract is at-most-once with no ordering. A follower that sees a
gap reads it back from storage before it goes on. So the things realtime does
not promise are not needed here:

- Messages that come while a socket reconnects (1 s up to 30 s backoff) are lost.
  The follower's gap read covers them.
- Every socket ends after 1 hour and the client gets a new token at once. Same as
  above.

### Limits and how we handle them

| Flow limit | What we do |
| :--- | :--- |
| 64 KB per publish | An event over the cap is parked in the kv for a minute, and the bus sends a reference to that key. The Go Postgres bus already does this for NOTIFY's 8000 bytes. |
| One HTTP call per publish | Fine for the little the bus carries: record entries and notices such as `STATE_CHANGE`. `publishMany` is there if we ever need to batch. |
| 1,000 sockets per channel | Plenty: it counts processes, not users, since a process shares its socket. |

## Part 3: `FlowStreams` (the `RunStreams` port)

New with v0.7.0. Flow streams keep their items and let a reader start from any
offset, which is what `RunStreams` needs. The two shapes match almost one to
one.

| `RunStreams` | On Flow |
| :--- | :--- |
| `open(streamId, meta, ttlMs)` | `flow.streams.open(id, { meta, ttlMs })`. Our default TTL is 24 hours; Flow allows 1 second to 7 days. |
| `append(streamId, events)` | `flow.streams.append(id, items)`, which gives back the offsets. Up to 100 items a call, so bigger batches are split. |
| `read(streamId, after, signal)` | `flow.streams.read` (TS async iterable) / `Streams().Read` (Go `iter.Seq2`). It reconnects from the last offset by itself. |
| `snapshot(streamId, after)` | `flow.streams.snapshot`. |
| `close(streamId, end, graceMs)` | `flow.streams.close`. Our default grace is 10 minutes; Flow allows up to 7 days. |
| `delete(streamId)` | `flow.streams.delete`. |
| `StreamGoneError`, `StreamClosedError` | Flow's errors of the same names (Go: `ErrStreamGone`, `ErrStreamClosed`), mapped to ours. |

Stream IDs follow the same charset as channels; an ID with other characters is
hashed, as for the bus.

### Limits to handle

1. **64 KB per item.** A big tool output in one item would be refused. The
   adapter parks an item over about 60 KB in the kv (for 8 days, past Flow's
   longest time to live) and appends a reference; readers get the event back.
   This is what was built: splitting would need every reader to join the
   pieces.
2. **20,000 items and 32 MB per stream.** A stream is one segment. Text goes
   out in 50 ms windows, so nonstop text fills 20,000 items in about 16
   minutes. In `await` mode a segment ends before that (the 14-minute step).
   In `detach` mode a very long segment could hit it. The append then fails
   with `413 StreamFull`. Options: end the segment and start a new stream, or
   make the text window larger on Flow. Not handled yet: the append fails, and
   the docs say to keep segments short.

## Part 4: agents from Flow workflows (optional)

The other way round: a Flow workflow that runs an agent and waits for it.

```ts
const { runId } = await step.run('start-agent', () =>
  chat.run({ prompt, runId: `wf-${event.id}` }),     // the same id on a replay
);
const done = await step.waitForEvent('agent-done', {
  event: 'agentenkit/run.finished', match: 'data.runId', timeout: '7d',
});
```

We ship a small helper that makes an `onFinish` hook for an agent's spec. It sends
`agentenkit/run.finished` with the `runId`, the end state, the stop reason and the
usage totals. The event `id` is the `runId`, so a settle that is retried sends it
only once. A run that parks for approval finishes only once, so the workflow
waits through the park.

## Packaging

- **TS.** `@clevix/flow` installs from the flow-sdk release `.tgz`. agentenkit
  lists it as an optional peer dependency (`>=0.7.0`), like `@upstash/qstash`,
  and imports it. The repo uses the v0.7.0 `.tgz` URL as a dev dependency for
  tests. New entry point: `agentenkit/adapters/flow` (queue, bus, streams, and
  the function and `onFinish` helpers).
- **Go.** The package is `adapters/clevixflow`, not `adapters/flow`, so it does
  not clash with the Flow SDK's own package name (`flow`) in an app that
  imports both. `go-agentenkit` is one module, so this adds
  `github.com/eadwinCode/flow-sdk v0.7.0` to its `go.mod`, as `go-redis` is now.
  That brings in `github.com/coder/websocket` too. flow-sdk's `go.mod` says
  `go 1.27`, so go-agentenkit now needs Go 1.27 (decided 2026-10-29). CI, the
  README and the Go examples moved with it.
- **Engine.** Needs v2.1.1 or later. Production has it. A self-hosted or local
  engine must be up to date, or the new calls answer `404`.
- Both runtimes get the same adapters and the same tests.

## Changes to Flow

None needed for the adapters. One thing may come up later: a way to handle a
stream that fills up (see Part 3, limit 2).

## Questions to decide

1. `await` or `detach` as the default worker mode?
2. What should `enqueue` do when the function is paused (`"paused"`)? Throw so
   the caller sees it, or treat it like `QueueFullError` for fresh dispatches
   only?
3. A full stream: end the segment, or a larger text window?
4. Is Part 4 worth shipping now, or later?

## Build order

1. Done: `FlowQueue` and the function helper, in TS and Go, with `find`,
   `stats` and `cancel`.
2. Done: `FlowBus` in TS and Go, with the kv reference for big events.
3. Done: `FlowStreams` in TS and Go, with the kv reference for big items.
4. Done: the example apps on Flow (`examples/flow-queue-ts`, `flow-queue-go`),
   and sections in [Ports and adapters](./ports-and-adapters.md) and
   [Setup](./setup.md#on-clevix-flow).
5. Part 4, if wanted.

The engine tests (`TEST_FLOW_BASE_URL`, `TEST_FLOW_APP_ID`,
`TEST_FLOW_SIGNING_KEY`) run the shared run streams suite and the bus against a
real engine, in both runtimes. CI has no engine, so they skip there.
