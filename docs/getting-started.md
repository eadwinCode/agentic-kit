# Getting started

## Requirements

- [Bun](https://bun.sh) ≥ 1.1, or Node ≥ 20 (contributing to the repository
  itself needs Bun ≥ 1.4 — see its readme)
- A model provider through the [AI SDK](https://sdk.vercel.ai) (v4)

## Install

```bash
bun add agentenkit ai zod
```

The React hook is separate, and only needed if you want it:

```bash
bun add use-agentenkit
```

Reference adapters lean on optional peers. Install only the ones you use:

```bash
bun add @prisma/client redis @upstash/qstash @upstash/redis pg
```

## A runtime you can run right now

There is no `createDevRuntime`. Assembling the platform is four adapters and one
wire, and seeing them is the point — swapping any one of them for its durable
equivalent later is then obvious rather than magic.

```ts
// lib/runtime.ts
import { Database } from 'bun:sqlite';
import { setupAgentCore } from 'agentenkit';
import { SqliteStorage } from 'agentenkit/adapters/sqlite';
import { InlineQueue } from 'agentenkit/adapters/inline';
import { MemoryBus, MemoryKv } from 'agentenkit/adapters/memory';
import { SqliteAdminStore } from 'agentenkit/admin/sqlite';
import { openai } from '@ai-sdk/openai';

// One file holds both your tables and — under an `agentic_` prefix — the
// platform's own operational history.
const db = new Database('agentic-kit.sqlite');
const queue = new InlineQueue();

export const runtime = await setupAgentCore({
  storage: new SqliteStorage(db),
  admin: SqliteAdminStore.open(db),
  bus: new MemoryBus(),
  kv: new MemoryKv(),
  queue,
  resolveModel: (name) => ({
    instance: () => openai(name),
    contextWindow: 128_000,
  }),
});

// The queue and the worker each need the other, so the queue is wired once the
// core exists. Nothing dispatches until this line runs.
queue.bind((job) => runtime.worker.handleJob(job));

export const chat = runtime.createStreamTextAgent({ name: 'chat', model: 'gpt-4o' });
```

Then:

```ts
const { accepted, threadId, runId } = await chat.run({ prompt: 'hi' });
```

That call persists the message, marks the thread `RUNNING`, and enqueues a job.
It does **not** wait for the model. The run continues after the call returns.

### Why `await setupAgentCore`

The operational store is opened there. A store that cannot be opened is a
startup error rather than a surprise on the first run — falling back to memory
would silently lose every record, and a dashboard showing nothing looks exactly
like no traffic.

### What `InlineQueue` is and is not

It dispatches on a later tick, so `enqueue` returns immediately and a run really
does outlive the request that started it. It honours `delaySeconds`, so parked
approvals expire and blocked jobs redrive exactly as they do against a durable
queue. What it does not do is survive a restart. Use it in development.

### Choosing a SQLite driver

`openSqlite(file)` picks one at runtime — `bun:sqlite` where it exists,
`node:sqlite` otherwise — if you would rather not import one yourself. Pass
`':memory:'` for a database that dies with the process.

## Watching a run

Nothing above prints anything, because a run's output goes to the event log and
the bus, not to the caller. The shortest way to see it:

```ts
const unsubscribe = await runtime.events.subscribe(threadId, (event) => {
  if (event.type === 'CHUNK' && event.payload?.type === 'text-delta') {
    process.stdout.write(event.payload.textDelta);
  }
});
```

For a browser, expose the [HTTP endpoints](./http-api.md) and use
[`use-agentenkit`](./react.md), which does the hydrate-then-tail dance for you.

## Next

- [Core concepts](./concepts.md) — what a thread, a run and a step actually are.
- [HTTP API](./http-api.md) — the endpoints to expose.
- [Production](./production.md) — swapping the four adapters for durable ones.
