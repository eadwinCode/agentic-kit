# agentenkit

A durable runtime for AI agent runs, as a **library**.

It does not own your prompts, models, or tools — the [AI SDK](https://sdk.vercel.ai) does.
It owns the **lifecycle of a run**: that a run outlives the request that started it,
survives a worker dying mid-step, can be stopped, parked for a human, resumed exactly
where it stopped, nested, metered, and watched by several people at once.

**[Full documentation](https://eadwincode.github.io/agentic-kit/)** — getting started, concepts, the HTTP contract,
ports, production settings and troubleshooting.

The `§` numbers below refer to a behavioural specification that no longer lives
in the repository; the [documentation](../../docs/README.md) and the test suite
now describe that behaviour.

## Running locally, with nothing to stand up

There is no `createDevRuntime`. Assembling the platform is four adapters and one wire,
and seeing them is the point — swapping any of them for the durable equivalent later is
then obvious rather than magic.

```ts
import { Database } from 'bun:sqlite';           // or node:sqlite — see openSqlite
import { setupAgentCore } from 'agentenkit';
import { SqliteStorage } from 'agentenkit/adapters/sqlite';
import { InlineQueue } from 'agentenkit/adapters/inline';
import { MemoryBus, MemoryKv } from 'agentenkit/adapters/memory';
import { SqliteAdminStore } from 'agentenkit/admin/sqlite';
import { openai } from '@ai-sdk/openai';

// One file holds both: your tables, and — prefixed `agentic_` — the platform's
// own operational history (§2.9).
const db = new Database('agentic-kit.sqlite');
const queue = new InlineQueue();

export const runtime = await setupAgentCore({
  storage: new SqliteStorage(db),      // ← later: PrismaStorage, or your own
  admin: new SqliteAdminStore(db),     // ← later: PostgresAdminStore
  bus: new MemoryBus(),                // ← later: RedisBus
  kv: new MemoryKv(),                  // ← later: RedisKv
  queue,                               // ← later: QStashQueue
  resolveModel: (name) => ({ instance: () => openai(name), contextWindow: 128_000 }),
});

// The queue and the worker each need the other, so the queue is attached once
// the core exists. Nothing dispatches until this line runs.
queue.bind((job) => runtime.worker.handleJob(job));

export const chat = runtime.createStreamTextAgent({ name: 'chat', model: 'gpt-4o' });
await chat.run({ prompt: 'hi' });   // persisted, dispatched, and running
```

`InlineQueue` is what makes this a real platform rather than a demo: it dispatches on a
later tick, so `enqueue` returns immediately and a run still outlives the request that
started it — and it honours `delaySeconds`, so parked approvals expire and blocked jobs
redrive exactly as they do against a durable queue. What it does not do is survive a
restart.

`openSqlite(file)` picks a driver at runtime — `bun:sqlite` where it exists,
`node:sqlite` otherwise — if you would rather not import one yourself. Pass `':memory:'`
for a database that dies with the process.

## Install

```bash
bun add agentenkit ai zod
# optional reference adapters:
bun add @prisma/client @upstash/redis @upstash/qstash redis
```

## Going to production

Same shape, durable pieces. Each line above swaps for one here.

```ts
import { Client } from '@upstash/qstash';
import { createClient } from 'redis';
import { PrismaClient } from '@prisma/client';
import { setupAgentCore } from 'agentenkit';
import { PrismaStorage } from 'agentenkit/adapters/prisma';
import { RedisBus, RedisKv } from 'agentenkit/adapters/redis';
import { QStashQueue } from 'agentenkit/adapters/qstash';

const redis = createClient({ url: process.env.REDIS_URL });
await redis.connect();

export const runtime = await setupAgentCore({
  storage: new PrismaStorage(new PrismaClient()),  // ← or Mongo / Dynamo / your DB
  bus: new RedisBus(redis),                        // ← Ably / Kafka / Postgres LISTEN…
  queue: new QStashQueue(
    new Client({ token: process.env.QSTASH_TOKEN! }),
    { url: 'https://app.example.com/api/queue/agent-run' },
  ),
  kv: new RedisKv(redis),
  // `admin` omitted on purpose: with AGENTIC_KIT_ADMIN_DATABASE_URL set, the
  // platform opens Postgres for its own history (§2.9). Pass one to override.
  resolveModel: (name) => ({ instance: () => models[name], contextWindow: 128_000 }),
});

export const chat = runtime.createStreamTextAgent({ name: 'chat', model: 'gpt-4o' });
```

### Operations

Runs belong to an agent handle; reads belong to the runtime.

```ts
await chat.run({ prompt: 'hi', state: { orgId } });      // persist + enqueue → 202 (§5.1)
await chat.stop(threadId);                               // one write: state → CANCELLED (§2.1)
await runtime.hitl.respond({ threadId, toolCallId, approved, payload, state });  // §2.5
await runtime.getThreadSnapshot(threadId, state);        // hydrate a client (§2.2)
const { stream, headers } = runtime.events.sse(threadId, { since, signal }); // SSE, any framework
for await (const e of runtime.events.follow(threadId, { signal })) { … }     // or iterate
const missed = await runtime.events.since(threadId, lastSeq);              // raw replay
const unsub  = await runtime.events.subscribe(threadId, handler);          // raw tail
await runtime.worker.handleJob(job);                     // queue consumer (§2.8)
```

The Next.js routes in [`examples/nextjs-app`](../../examples/nextjs-app) show the HTTP
wiring — each handler is a few lines over the runtime.

## Run state (§2.10)

Whatever you attach to a run reaches **every** storage call it makes, every tool, and
every nested run — including in a worker that picks the job up hours later, in another
process, after an approval. The platform never reads it.

```ts
await chat.run({ prompt: 'hi', state: { orgId: 'acme', userId: 'u1' } });
```

```ts
// Your Storage sees it on every method, as a trailing argument.
async list(threadId: string, opts, ctx) {
  return this.db.message.findMany({ where: { threadId, orgId: ctx.state.orgId } });
}
```

Type your own fields by augmenting the interface — no `<TState>` rippling through
`Storage`, `AgentCore`, tools and subagents before it reaches the one place it is read:

```ts
declare module 'agentenkit' {
  interface AgentRunState { orgId: string; userId: string }
}
```

## Ports (§3.2) — your data

| Port | Role | Reference adapter |
| :--- | :--- | :--- |
| `Storage` | threads / messages / events / usage — incl. atomic `claimState` | `PrismaStorage`, `SqliteStorage` |
| `EventBus` | live fan-out + HITL death notices (at-most-once) | `RedisBus`, `UpstashBus` |
| `Queue` | durable run dispatch (at-least-once) | `QStashQueue`, `InlineQueue` (dev) |
| `Kv` | hot state cache, HITL handoff keys, seq/attempt counters | `RedisKv`, `UpstashKv` |

Implement any of them for your own stack — `core/` imports nothing else. The
[`Memory*` adapters](./src/adapters/memory.ts) are a complete implementation used by the
test suite and double as a template.

## The admin store (§2.9) — not yours

Run records, step timings and a thread index are the **platform's** data, in the
platform's own tables. You do not implement `AdminStore`; you read it back:

```ts
await runtime.admin.overview();          // threads and runs by state, plus what's in flight
await runtime.admin.listRuns({ state: ['FAILED'], since });
await runtime.admin.stats({ since });    // p50/p95 duration and queue wait, tokens, failures
await runtime.admin.getRun(runId);       // one run: steps, nested runs, timeline
```

Configure nothing and it is SQLite on disk (`AGENTIC_KIT_ADMIN_DB` moves the file).
Set `AGENTIC_KIT_ADMIN_DATABASE_URL` and it is Postgres — point it at its own database
or the one you already have; the `agentic_` table prefix keeps them apart. `pg` is an
optional peer dependency, needed only on that path.

```bash
AGENTIC_KIT_ADMIN_DATABASE_URL=postgresql://user:pass@host/db
```

The store is opened when `setupAgentCore` runs, which is why that call is `await`ed: a
store that cannot be opened is a startup error rather than a surprise on the first run.
Quietly falling back to memory would lose every record, and a dashboard showing nothing
looks exactly like no traffic.

Keeping it separate is what lets a dashboard answer "what is running right now" without
reading your database at all.

## Adapter invariants (§3.4)

1. `events.append` receives `seq` from `kv.incr('agent:seq:{threadId}')` — monotonic per thread.
2. `threads.claimState` must be atomic (one conditional UPDATE) — exactly one caller wins.
3. `queue.enqueue` is at-least-once and the engine is idempotent. An adapter that cannot
   honour `delaySeconds` may deliver immediately, but must never throw for it — a HITL
   expiry is scheduled from inside a parked tool call.
4. `bus` is at-most-once; the §2.5 watchdog pattern compensates.
5. Durable thread state lives in `storage.threads`; the kv copy is a hot cache. Writes go to both.
6. Every run carries an id (§2.1). A worker whose id is no longer current has been
   replaced and must not write state on the live run's behalf.

## Behaviour map

| Spec | Where in the package |
| :--- | :--- |
| §2.1 Detached execution, stop, run identity | `core/run.ts`, `core/stop.ts`, `core/engine.ts`, `core/keys.ts` |
| §2.2 Multi-user stream | `ports/bus.ts` + `runtime.events` |
| §2.5 HITL park, respond, expiry, reclaim | `core/hitl.ts`, `core/reclaim.ts` |
| §2.6 Context ceiling & compaction | `core/context.ts` |
| §2.7 Subagents as nested runs | `core/subagent.ts`, `core/loop.ts` |
| §2.8 Queue dispatch + redrive/FAIL policy | `ports/queue.ts`, `core/engine.ts` |
| §2.9 Operational history | `ports/admin.ts`, `admin/*`, `core/admin.ts` |
| §2.10 Run state | `core/state.ts` |
| §5 Reference HTTP integration | `examples/nextjs-app` |

## Development

Requires [Bun](https://bun.sh) ≥ 1.1.

```bash
bun install
bun test          # runs test/*.test.ts directly on Bun (no build step)
bun run typecheck # tsc --noEmit over src + tests
bun run build     # tsc emit → dist/ (published artifact)
```
