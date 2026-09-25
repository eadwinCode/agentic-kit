# Ports and adapters

The engine imports no database driver. Four interfaces stand between it and your
stack; implement any of them for anything.

| Port | Role | Reference adapters |
| :--- | :--- | :--- |
| `Storage` | threads, messages, events, usage | `PrismaStorage`, `SqliteStorage`, `MemoryStorage` |
| `Queue` | durable run dispatch | `QStashQueue`, `InlineQueue`, `MemoryQueue` |
| `EventBus` | live fan-out | `RedisBus`, `UpstashBus`, `MemoryBus` |
| `Kv` | hot state, handoff keys, counters | `RedisKv`, `UpstashKv`, `MemoryKv` |

The `Memory*` adapters are a complete implementation used by the test suite, and
double as a template.

The Go runtime also ships all four over **one Postgres**
(`adapters/postgres`): the storage, a `Kv`, an `EventBus` over
LISTEN/NOTIFY and a `Queue` over a jobs table. See
[One Postgres for everything](#one-postgres-for-everything-go).

## Storage

```ts
interface Storage {
  threads: {
    get(threadId, ctx): Promise<ThreadDTO | null>;
    create(init: { model?: string } | undefined, ctx): Promise<ThreadDTO>;
    list(ctx): Promise<ThreadDTO[]>;
    setState(threadId, state, ctx): Promise<void>;
    delete(threadId, ctx): Promise<void>;
    claimState(/* … */): Promise<boolean>;   // must be atomic
  };
  messages: {
    append(threadId, message, ctx): Promise<MessageDTO>;
    list(threadId, opts, ctx): Promise<MessageDTO[]>;
    deleteFrom(threadId, messageId, ctx): Promise<number>;
  };
  events: {
    append(threadId, event, ctx): Promise<void>;
    listSince(threadId, sinceSeq, ctx): Promise<AgentEvent[]>;
    latest(threadId, type, ctx): Promise<AgentEvent | null>;
    listByType(threadId, type, ctx): Promise<AgentEvent[]>;
  };
  usage: {
    // One row per MODEL CALL, priced before it reaches you (§4).
    record(threadId, usage, ctx): Promise<void>;
    // An empty filter sums the thread; { runId } sums one run. The result
    // carries `lines`, grouped by agent and model.
    total(threadId, filter, ctx): Promise<UsageTotals>;
  };
}
```

Every method takes a trailing `ctx` carrying the run's
[state](./run-state.md) — that is how a query scopes itself to a tenant.

### `messages.list` scoping

The `opts` argument decides **whose** turns come back, and getting it wrong is a
correctness bug rather than a display one:

- `{ agentId: null }` — the main agent's stream. Compaction and the edit lookup
  must use this. Unscoped, a subagent's turns leak into the parent's prompt and
  context isolation is gone.
- `{ agentId: 'sub_1' }` — that nested run's own stream.
- omitted — every row on the thread, for UI hydration.

### `claimState` must be atomic

One conditional `UPDATE`, one winner. This is the primitive that makes
concurrent workers safe. Implemented as read-then-write it will pass your tests
and fail in production.

## Queue

```ts
interface Queue {
  enqueue(job: RunJob, opts?: EnqueueOptions): Promise<void>;
}
```

```go
type Queue interface {
	Enqueue(ctx context.Context, job RunJob, opts *EnqueueOptions) error
	// Cancel drops every waiting job enqueued under a key.
	Cancel(ctx context.Context, key string) error
	// Find returns a run's waiting or running job, or nil.
	Find(ctx context.Context, runID string) (*QueuedJob, error)
	// Stats counts what waits, runs and died.
	Stats(ctx context.Context) (QueueStats, error)
}
```

At-least-once. The engine is idempotent under redelivery through the per-thread
run lock.

The Go port carries a little more than `Enqueue`, because an engine that
cannot see its queue cannot refuse work, withdraw a park's expiry once the
park is answered, or tell a dead worker's run from a live one:

- `EnqueueOptions.Key` dedupes: at most one live job per key
  (`ErrDuplicateJob` for a second), and `Cancel(key)` withdraws it. The
  engine keys a park's expiry and its resume, and a thread's reclaim.
- `EnqueueOptions.Priority` orders ready jobs: higher first. The engine's own
  housekeeping (expiries, reclaims, redrives) goes in at `PriorityLow`, so a
  user's message never queues behind it.
- `RunJob.Kind` says why a job exists (dispatch, retry, redrive, resume,
  expiry, reclaim); `RunJob.PartitionKey` is the caller's tenant;
  `RunJob.DispatchedAt` is the run's first dispatch, carried onto every
  retry so the run keeps its place in line.
- `Stats` and `Find` feed the engine's overload refusal
  (`AgentConfig.MaxQueueDepth`), the admin overview, and the stuck-run sweep.
  An adapter with no read side answers `ErrUnsupported` and the engine treats
  that as "unknown", never as a failure.

An adapter that cannot honour `delaySeconds` may deliver immediately, but **must
never throw for it** — a HITL expiry is scheduled from inside a parked tool call,
and an adapter that rejects the option breaks approvals rather than degrading
them.

> A real case: QStash rejects delay headers on its *enqueue* path but accepts
> them on *publish*. The in-memory queue accepted everything, so the test suite
> was green and production was not. If your queue distinguishes paths, test the
> delayed one.

## EventBus

```ts
interface EventBus {
  publish(threadId: string, event: AgentEvent): Promise<void>;
  subscribe(threadId: string, handler: (e: AgentEvent) => void): Promise<() => void>;
}
```

At-most-once, deliberately. A dropped frame is recovered by replaying the
durable event log from the client's cursor, so the bus does not need delivery
guarantees — which is what lets it be Redis pub/sub, Ably, or Postgres
`LISTEN/NOTIFY`.

## Kv

```ts
interface Kv {
  get(key: string): Promise<string | null>;
  // onlyIfNotExists is SET NX: how the run lock is taken
  set(key: string, value: string, opts?: { exSeconds?: number; onlyIfNotExists?: boolean }): Promise<boolean>;
  del(key: string): Promise<void>;
  incr(key: string): Promise<number>;
  // compare-and-act: how the run lock is renewed and freed
  setIfValue(key: string, expected: string, value: string, opts?: { exSeconds?: number }): Promise<boolean>;
  delIfValue(key: string, expected: string): Promise<boolean>;
}
```

Hot cache and coordination: thread state, run identity, HITL handoff keys, and
the per-thread `seq` counter. Everything here is reconstructible except while a
run is in flight.

`setIfValue` and `delIfValue` act only while the key still holds `expected`,
and each must be **one atomic step**: a Lua script on Redis, a conditional
`UPDATE`/`DELETE` in SQL. A `get` followed by a `set` is not enough. Together
they are what stop a worker from renewing or freeing a run lock that another
worker took after its own lapsed. A custom `Kv` written before these existed
must add them.

## The invariants

Break one of these and the failure is subtle rather than loud.

1. `events.append` receives its `seq` from `kv.incr('agent:seq:{threadId}')` —
   monotonic per thread. Clients use it as a cursor, so a repeated or
   out-of-order value causes replay bugs.
2. `threads.claimState` is atomic — exactly one caller wins.
3. `queue.enqueue` is at-least-once, and never throws for `delaySeconds`.
4. `bus` is at-most-once; the watchdog compensates.
5. Durable thread state lives in `storage.threads`; the kv copy is a hot cache.
   Writes go to **both**.
6. Every run carries an id. A worker whose id is no longer current has been
   replaced and must not write state on the live run's behalf.

## One Postgres for everything (Go)

A single database can run the whole operational side. Each adapter is a few
tables under the storage's prefix:

```go
db, _ := sql.Open("pgx", url)
storage, _ := postgres.New(ctx, db)
kv, _ := postgres.NewKv(ctx, db)
queue, _ := postgres.NewQueue(ctx, db, postgres.QueueOptions{})
bus := postgres.NewBus(db, pgxlisten.New(url), storage.Events(), kv, postgres.BusOptions{})
// …SetupAgentCore, then:
queue.Bind(rt.Worker.Handler())
```

**Kv.** `SET NX` is one `INSERT … ON CONFLICT DO UPDATE … WHERE expired`
and `Incr` increments inside the conflict clause, so two workers never both
take a lock or the same seq. Expiry is enforced on read; `DeleteExpired` is
housekeeping.

**EventBus.** One `LISTEN` connection per process (`pgxlisten`, over pgx),
fan-out by thread id in memory. NOTIFY payloads are capped at 8000 bytes,
which a tool result can exceed: an event that does not fit, durable or not,
is parked in the kv for a minute and travels as a reference to that key. The
kv needs no storage scope to read back, so a storage that requires a tenant
on every read still gets its oversized events. Each subscription keeps the
last durable seq it delivered; when the `LISTEN` connection comes back after
a drop, the bus replays what each subscriber missed from `Storage.Events`,
scoped with the run state on the subscriber's own context, and delivers
nothing twice. At-most-once still, and the client's cursor replay stays the
last line.

**Queue.** `Enqueue` is one insert; a delay is a future `runAt`. The consumer
claims with `SELECT … FOR UPDATE SKIP LOCKED`, so several processes can
share the table, and renews the row's lease while the job runs. A worker
that dies mid-job loses its lease and the job is redelivered — at-least-once,
which the run lock makes safe. A handler that returns an error hands the job
back after a growing backoff (`RetryBackoff`, `RetryBackoffMax`); after
`MaxAttempts` the row is kept as dead with the reason, the `DeadHandler`
bound with the worker fails its run, and an operator can list, redrive or
purge it. The claim spreads across partitions: the partition with the fewest
jobs in flight goes first, then priority, then dispatch time, so one tenant's
burst cannot hold the head of the line. Every consumer of a `Namespace` takes
only that namespace's rows, and `Pause`/`Resume` hold every consumer of it
through a control row. With a `Listener` the consumer wakes on an enqueue
instead of polling; `Poll` is then the backstop. `MaxDepth` refuses a fresh
dispatch with `ErrQueueFull`, `MaxPayloadBytes` refuses an oversized ticket,
`MaxAge` keeps a row that waited too long as dead instead of running it,
`MaxRunTime` cancels a handler that ignores its context, and `Close` gives
running handlers `DrainTimeout` to finish before it cancels them. The queue
logs what it does through `Log`.

Tests: `TEST_ADMIN_PG=postgres://… go test ./...` runs the platform end to end
on these adapters, including the over-cap frames and a concurrent `Incr`.

## Writing your own

Start from `MemoryStorage` — it is complete and short. Then run the package's
suite against your implementation; the tests are written against the ports, not
the adapters.

The one thing to test that unit tests rarely reach: two workers calling
`claimState` on the same thread at the same moment. Exactly one must win.
