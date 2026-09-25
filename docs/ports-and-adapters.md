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
    transition(threadId, t: ThreadTransition, ctx): Promise<boolean>; // must be atomic
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

### `claimState` and `transition` must be atomic

One conditional `UPDATE`, one winner. This is the primitive that makes
concurrent workers safe. Implemented as read-then-write it will pass your tests
and fail in production.

`transition` is the one every run state change goes through. Besides the
state, it keeps the thread's current run (a `runId` column beside `state`):

```ts
interface ThreadTransition {
  from: ExecutionState[];  // the change lands only from one of these
  to: ExecutionState;
  runId?: string;          // …and only while the thread belongs to this run
  newRunId?: string;       // run admission: the thread now belongs to this run
}
```

A row with no run recorded yet (from before the column existed) matches any
`runId`. With Prisma it is one `updateMany`:

```ts
transition: async (threadId, t, ctx) => {
  const { count } = await this.db.thread.updateMany({
    where: {
      id: threadId, state: { in: t.from },
      ...(t.runId ? { OR: [{ runId: t.runId }, { runId: null }] } : {}),
    },
    data: { state: t.to, ...(t.newRunId ? { runId: t.newRunId } : {}) },
  });
  return count === 1;
},
```

A custom storage written before `transition` existed must add it, and a
`runId` column on its thread table.

## Queue

```ts
interface Queue {
  enqueue(job: RunJob, opts?: EnqueueOptions): Promise<void>;
  /** Drop every waiting job enqueued under a key. */
  cancel(key: string): Promise<void>;
  /** A run's waiting or running job, or null. */
  find(runId: string): Promise<QueuedJob | null>;
  /** Count what waits, runs and died. */
  stats(): Promise<QueueStats>;
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

The port carries a little more than `enqueue`, because an engine that cannot
see its queue cannot refuse work, withdraw a park's expiry once the park is
answered, or tell a dead worker's run from a live one. Both runtimes have the
same port:

- `EnqueueOptions.key` (Go: `Key`) dedupes: at most one live job per key
  (`DuplicateJobError` / `ErrDuplicateJob` for a second), and `cancel(key)`
  withdraws it. The engine keys a park's expiry and its resume, a thread's
  reclaim, and the late settle of a run.
- `EnqueueOptions.priority` orders ready jobs: higher first. The engine's own
  housekeeping (expiries, reclaims, redrives) goes in at `PRIORITY_LOW`
  (Go: `PriorityLow`), so a user's message never queues behind it.
- `RunJob.kind` says why a job exists (absent for a fresh dispatch; `retry`,
  `redrive`, `resume`, `expiry`, `reclaim`). In Go, `RunJob.PartitionKey` is
  the caller's tenant and `RunJob.DispatchedAt` is the run's first dispatch,
  carried onto every retry so the run keeps its place in line.
- `stats` and `find` feed the admin overview and the stuck-run sweep (and, in
  Go, the overload refusal, `AgentConfig.MaxQueueDepth`). An adapter with no
  read side throws `UnsupportedError` (Go: `ErrUnsupported`) and the engine
  treats that as "unknown", never as a failure.

The shipped queues: `MemoryQueue` for tests; `InlineQueue` for development,
which keys, cancels and counts like a real queue, holds a job that comes due
before `bind` rather than dropping it, and waits out a delay of any length (a
single timer cannot hold more than about 24.8 days); `QStashQueue`, where a
key becomes QStash's deduplication id (remembered for ten minutes), priority
is ignored, `cancel` does nothing (a delivered job is a correct no-op) and
`find` / `stats` are unsupported.

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

At-most-once, deliberately. A dropped frame is recovered from the durable event
log, so the bus does not need delivery guarantees — which is what lets it be
Redis pub/sub, Ably, or Postgres `LISTEN/NOTIFY`. Nor does it need ordering: a
follower that sees seq N+1 before N (another process published N and is still
writing it, or the bus dropped it) reads the gap back from storage before it
yields N+1, so a client never misses an event and never sees one twice.

The reference buses keep one subscriber connection per process, shared by every
subscription, and give each subscription its own queue, so one slow client
holds up nobody. Token deltas are merged before they are published (one event
per 50 ms of text rather than one per token), in the same event shape.

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
  // a counter that expires if nobody clears it: the retry counters
  incrWithExpiry(key: string, exSeconds: number): Promise<number>;
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
2. `threads.claimState` and `threads.transition` are atomic — exactly one
   caller wins.
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

**Schema.** The storage, kv and queue tables are set up by the same migrator
as the admin store, once per database: each prefix has its own ledger,
`<prefix>migrations`, and a start with nothing to do reads it and moves on.
Before, every start ran its `ALTER TABLE` statements, and each one takes a lock
on the whole table even when the column is already there. A unique index that
old duplicate rows block (events, messages by `(threadId, seq)`) is skipped
with a warning and tried again on the next start. Appends to one thread's
messages take the thread's row lock, so two cannot take the same seq.

**Queue.** `Enqueue` is one statement: it counts, inserts and notifies, so
`MaxDepth` is checked against the table the insert sees. The consumer
claims with `SELECT … FOR UPDATE SKIP LOCKED`, so several processes can
share the table, and renews the row's lease while the job runs. A worker
that dies mid-job loses its lease and the job is redelivered — at-least-once,
which the run lock makes safe. A handler that returns an error hands the job
back after a growing backoff (`RetryBackoff`, `RetryBackoffMax`); after
`MaxAttempts` the row is kept as dead with the reason, the `DeadHandler`
bound with the worker fails its run, and an operator can list, redrive or
purge it. The claim spreads across partitions: the partition with the fewest
jobs in flight goes first, then priority, then dispatch time, so one tenant's
burst cannot hold the head of the line. It never sorts the backlog: the
partitions are walked through an index and each partition's head is an index
lookup, so a claim costs the same with ten waiting jobs as with a million. A
row whose payload cannot be read is kept dead and reaches the `DeadHandler`
like any other, with the thread and run from its own columns. `Cancel` and
`Purge` leave dead rows alone (`PurgeFilter.IncludeDead` asks for them). Every consumer of a `Namespace` takes
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

**SQLite.** `openSqlite` (Go: `sqlite.Open`) turns on WAL and a five-second
`busy_timeout`, so a second process waits for the write lock instead of
failing with `SQLITE_BUSY`; call `tuneSqlite` (Go: `sqlite.Tune`) on a handle
you open yourself. A message's seq is taken inside its insert, a thread's
delete is one transaction that fails for a thread that is not there, and the
admin migrations take their lock up front (`BEGIN IMMEDIATE`).

**Memory.** `MemoryBus` keeps the latest 10,000 published events for tests to
read, not every event ever, and drops a thread's subscriber list when the
last one leaves.
