# Setting up `setupAgentCore`

Everything the runtime needs, and everything you can change about it.

```ts
const runtime = await setupAgentCore({
  storage,        // required
  queue,          // required
  bus,            // required
  kv,             // required
  resolveModel,   // required
  admin,          // optional
  config,         // optional
});
```

The call is `async` because the operational store is opened here. A store that
cannot be opened is a startup error rather than a surprise on the first run —
quietly falling back to memory would lose every record, and a dashboard showing
nothing looks exactly like no traffic.

---

## `storage` — your data

Threads, messages, events and usage. See
[Ports and adapters](./ports-and-adapters.md) for the full interface and its
invariants.

```ts
storage: new PrismaStorage(prisma)
```

Reference implementations: `PrismaStorage`, `SqliteStorage`, `MemoryStorage`.
Write your own for anything else — the engine imports no driver.

Every method receives a trailing context carrying the run's
[state](./run-state.md), which is how a query scopes itself to a tenant.

## `queue` — dispatch

```ts
queue: new QStashQueue(new Client({ token }), { url: `${appUrl}/api/queue/agent-run` })
```

At-least-once. The engine is idempotent under redelivery through the per-thread
run lock.

| Adapter | Use |
| :--- | :--- |
| `QStashQueue` | production, serverless |
| `InlineQueue` | development — dispatches on a later tick, honours delays, does not survive a restart |
| `MemoryQueue` | tests |

`InlineQueue` needs wiring after the core exists, because the two need each
other:

```ts
const queue = new InlineQueue();
const runtime = await setupAgentCore({ queue, /* … */ });
queue.bind((job) => runtime.worker.handleJob(job));
```

Nothing dispatches until `bind` runs.

> An adapter must never **throw** for `delaySeconds`. Delivering immediately is
> an acceptable degradation; rejecting the option breaks approval expiry, which
> is scheduled from inside a parked tool call.

## `bus` — live fan-out

```ts
bus: new RedisBus(redis)
```

At-most-once, deliberately. A dropped frame is recovered by replaying the
durable event log from the client's cursor — which is what lets the bus be Redis
pub/sub, Upstash, Ably, or Postgres `LISTEN/NOTIFY`.

`RedisBus` takes an optional heartbeat interval, which drives the watchdog:

```ts
new RedisBus(redis, 60_000)   // default
```

## `kv` — hot state

```ts
kv: new RedisKv(redis)
```

Thread state cache, run identity, HITL handoff keys, and the per-thread `seq`
counter. Everything here is reconstructible except while a run is in flight.

The `seq` counter is the one to be careful about: `events.append` takes its
`seq` from `kv.incr`, clients use it as a cursor, and a repeated or
out-of-order value causes replay bugs that look like missing messages.

## `resolveModel` — your models, your shape

The one required function. Models can come from a config file, a database, or
provider SDKs; the platform only ever sees the resolved pair.

```ts
resolveModel: (name) => {
  const model = registry[name];
  if (!model) throw new Error(`Unknown model: ${name}`);
  return { instance: () => model, contextWindow: 128_000 };
}
```

| Field | Meaning |
| :--- | :--- |
| `instance()` | Returns the AI SDK model. Called per step, so it can be lazy. |
| `contextWindow` | Feeds compaction. Wins over the `nativeWindows` config table. |

**Throw for an unknown key.** A model name can reach this function from a run
request or from a subagent the *model itself* named. Falling back silently means
a hallucinated model name quietly becomes a different one.

Per-tenant models are just a closure over your registry:

```ts
resolveModel: (name) => {
  const [tenant, model] = name.includes(':') ? name.split(':') : [null, name];
  return {
    instance: () => (tenant ? tenantModels[tenant][model] : registry[model]),
    contextWindow: windows[model] ?? 128_000,
  };
}
```

## `admin` — operational history

Omit it and it is chosen from the environment:

| Environment | Store |
| :--- | :--- |
| `AGENTIC_KIT_ADMIN_DATABASE_URL` set | Postgres, tables prefixed `agentic_` |
| otherwise | SQLite at `AGENTIC_KIT_ADMIN_DB`, or `agentic-kit-admin.sqlite` |

Pass one to decide for yourself:

```ts
admin: new SqliteAdminStore(db)         // same file as your data, if you like
admin: new PostgresAdminStore(pool)     // explicit
admin: new MemoryAdminStore()           // tests — nothing touches the disk
```

**Use `MemoryAdminStore` in tests.** The default writes a file, so a suite that
forgets this shares one database across every test.

This is not a port. You do not implement `AdminStore`; you read it back through
`runtime.admin.*`. See [Observability](./observability.md).

## What you get back

Beyond the agent factories, the runtime exposes the reads and the event stream:

```ts
runtime.events.sse(threadId, { since, signal })    // SSE stream + headers, any framework
runtime.events.follow(threadId, { signal })        // the same events, as an async iterable
runtime.getThreadSnapshot(threadId, state)         // hydrate a client
runtime.admin.overview()                           // operational reads
runtime.worker.handleJob(job)                      // the queue consumer
```

The stream carries the replay-then-tail rules with it, so a route handler is a
cursor and a response — see [HTTP API](./http-api.md#live-stream).

## `config` — behaviour

Every field is optional and merges over the defaults.

```ts
config: {
  hitlTtlMs: 5 * 60_000,
  maxSteps: 12,
  recordPayloads: false,
  billingPreCheck: async (threadId) => ({ ok: await hasCredit(threadId) }),
}
```

The full table is in [Configuration](./configuration.md). The settings most
worth deciding deliberately:

| Setting | Why |
| :--- | :--- |
| `maxSteps` | Your ceiling on a runaway loop. The default of 25 is generous. |
| `tokenBudget` | Unbounded by default. |
| `runLockLeaseSeconds` | Must exceed your longest run segment. |
| `hitlTtlMs` | How long a human has. 15 minutes suits a chat; a back-office approval may want a day. |
| `recordPayloads` | On by default. Turn it off when prompts carry sensitive data. |
| `billingPreCheck` | The only hook that can refuse a run before it costs anything. |
| `providerOptions` | Provider-specific settings for every run — see [Provider options](./provider-options.md). |

---

## Assembling it

### Development

```ts
import { Database } from 'bun:sqlite';
import { setupAgentCore } from '@agentic-kit/core';
import { SqliteStorage } from '@agentic-kit/core/adapters/sqlite';
import { InlineQueue } from '@agentic-kit/core/adapters/inline';
import { MemoryBus, MemoryKv } from '@agentic-kit/core/adapters/memory';
import { SqliteAdminStore } from '@agentic-kit/core/admin/sqlite';

const db = new Database('agentic-kit.sqlite');
const queue = new InlineQueue();

export const runtime = await setupAgentCore({
  storage: new SqliteStorage(db),
  admin: new SqliteAdminStore(db),
  bus: new MemoryBus(),
  kv: new MemoryKv(),
  queue,
  resolveModel: (name) => ({ instance: () => openai(name), contextWindow: 128_000 }),
});
queue.bind((job) => runtime.worker.handleJob(job));
```

### Production

```ts
const redis = createClient({ url: process.env.REDIS_URL });
await redis.connect();

export const runtime = await setupAgentCore({
  storage: new PrismaStorage(new PrismaClient()),
  bus: new RedisBus(redis),
  kv: new RedisKv(redis),
  queue: new QStashQueue(
    new Client({ token: process.env.QSTASH_TOKEN! }),
    { url: `${process.env.APP_URL}/api/queue/agent-run` },
  ),
  // admin omitted — AGENTIC_KIT_ADMIN_DATABASE_URL decides
  resolveModel,
  config: { maxSteps: 12, runLockLeaseSeconds: 900 },
});
```

### Tests

```ts
export async function testRuntime(model: LanguageModelV1) {
  const queue = new MemoryQueue();
  const runtime = await setupAgentCore({
    storage: new MemoryStorage(),
    bus: new MemoryBus(),
    kv: new MemoryKv(),
    queue,
    admin: new MemoryAdminStore(),   // never touch the disk
    resolveModel: () => ({ instance: () => model, contextWindow: 128_000 }),
    config: { hitlTtlMs: 1_000 },    // make expiry observable
  });
  return { runtime, queue };
}
```

Drive the worker by hand — `await runtime.worker.handleJob(queue.items.at(-1)!)` —
and you can step a run one dispatch at a time.

---

## Registering agents in the same module

The agent registry lives inside the runtime's closure, and a queue job resolves
its handle **by name** from that registry.

In a framework that gives each route its own module instance — Next.js does — a
worker route importing the runtime from a module that does not *also* register
the agents will resolve every job to an unknown agent.

```ts
// lib/runtime.ts — one module, imported everywhere
export const runtime = await setupAgentCore({ /* … */ });

export const chat = runtime.createStreamTextAgent({ name: 'chat', model: 'gpt-4o' });
export const support = runtime.createStreamTextAgent({ name: 'support', model: 'gpt-4o' });
```

## Reusing connections in development

A framework that re-evaluates this module on hot reload opens a fresh database
pool and Redis connection every time, and nothing closes them. After enough
edits Postgres refuses new clients and the app dies with it.

```ts
const dev = process.env.NODE_ENV !== 'production';
const cache = globalThis as unknown as { agentRedis?: RedisClient; agentPrisma?: PrismaClient };

const redis = cache.agentRedis ?? createClient({ url: process.env.REDIS_URL });
if (!redis.isOpen) await redis.connect();
const prisma = cache.agentPrisma ?? new PrismaClient();
if (dev) { cache.agentRedis = redis; cache.agentPrisma = prisma; }
```

Production evaluates the module once, so the cache is never populated there.

## Several runtimes in one process

Nothing stops it — each has its own registry and its own ports. Two reasons it
comes up:

- **A separate worker service.** It needs the same runtime module, because a job
  resolves its handle by name.
- **Isolated stacks per region or tier.** Give each its own storage and queue.

Do not build one runtime per tenant. Tenancy belongs in
[run state](./multi-tenancy.md), not in a runtime per customer — a runtime per
tenant multiplies connection pools by your customer count.
