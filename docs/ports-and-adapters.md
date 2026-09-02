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
    record(threadId, usage, ctx): Promise<void>;
    total(threadId, ctx): Promise<UsageTotals>;
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

At-least-once. The engine is idempotent under redelivery through the per-thread
run lock.

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
  set(key: string, value: string, opts?: { ttlSeconds?: number }): Promise<void>;
  del(key: string): Promise<void>;
  incr(key: string): Promise<number>;
  // plus the conditional set used for locks
}
```

Hot cache and coordination: thread state, run identity, HITL handoff keys, and
the per-thread `seq` counter. Everything here is reconstructible except while a
run is in flight.

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
which a tool result can exceed: an event that does not fit travels as
`{threadId, seq}` and the subscriber reads it back from `Storage.Events`; a
notice (seq 0) that does not fit is parked in the kv for a minute and
referenced by key. At-most-once still: a dropped notification is recovered
by the client's cursor replay.

**Queue.** `Enqueue` is one insert; a delay is a future `runAt`. The consumer
claims with `SELECT … FOR UPDATE SKIP LOCKED`, so several processes can
share the table, and renews the row's lease while the job runs. A worker
that dies mid-job loses its lease and the job is redelivered — at-least-once,
which the run lock makes safe. A handler that returns an error hands the job
back at once; after `MaxAttempts` it is dropped.

Tests: `TEST_ADMIN_PG=postgres://… go test ./...` runs the platform end to end
on these adapters, including the over-cap frames and a concurrent `Incr`.

## Writing your own

Start from `MemoryStorage` — it is complete and short. Then run the package's
suite against your implementation; the tests are written against the ports, not
the adapters.

The one thing to test that unit tests rarely reach: two workers calling
`claimState` on the same thread at the same moment. Exactly one must win.
