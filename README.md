<h1 align="center">agentic-kit</h1>

<p align="center">
  A durable runtime for AI agent runs — and the React hook that talks to it.
</p>

<p align="center">
  <a href="https://github.com/eadwinCode/agentic-kit/actions/workflows/ci.yml">
    <img src="https://github.com/eadwinCode/agentic-kit/actions/workflows/ci.yml/badge.svg" alt="CI">
  </a>
  <img src="https://img.shields.io/badge/license-MIT-blue.svg" alt="MIT">
  <img src="https://img.shields.io/badge/types-TypeScript-3178c6.svg" alt="TypeScript">
</p>

---

**agentic-kit is not an agent framework.** It does not own your prompts, models
or tools — the [AI SDK](https://sdk.vercel.ai) does. It owns the **lifecycle of
a run**: that a run outlives the request that started it, survives a worker
dying mid-step, can be stopped, parked for a human, resumed exactly where it
stopped, nested, metered, and watched by several people at once.

```ts
const { threadId, runId } = await chat.run({ prompt: 'send the quarterly report' });
// returns in milliseconds — the run continues without you
```

Everything vendor-specific lives behind four small interfaces — storage, queue,
event bus, key-value — so the engine never imports a database driver.

## Packages

| Package | | |
| :--- | :--- | :--- |
| **`@agentic-kit/core`** | The server runtime | [readme](./packages/agentic-kit/README.md) |
| **`use-agentkit`** | The React hook | [readme](./packages/use-agentkit/README.md) |

The hook is one way to build a UI over the core, not a requirement. The event
log is a public contract you can build any client over.

## Install

```bash
bun add @agentic-kit/core ai zod
bun add use-agentkit          # optional, for React
```

## A runtime with nothing to stand up

No Docker, no cloud queue, no `createDevRuntime`. Four adapters and one wire —
and seeing them is the point, because swapping any one for its durable
equivalent later is then obvious rather than magic.

```ts
import { Database } from 'bun:sqlite';
import { setupAgentCore } from '@agentic-kit/core';
import { SqliteStorage } from '@agentic-kit/core/adapters/sqlite';
import { InlineQueue } from '@agentic-kit/core/adapters/inline';
import { MemoryBus, MemoryKv } from '@agentic-kit/core/adapters/memory';
import { SqliteAdminStore } from '@agentic-kit/core/admin/sqlite';
import { openai } from '@ai-sdk/openai';

const db = new Database('agentic-kit.sqlite');
const queue = new InlineQueue();

export const runtime = await setupAgentCore({
  storage: new SqliteStorage(db),    // ← later: PrismaStorage, or your own
  admin: new SqliteAdminStore(db),   // ← later: Postgres, via one env var
  bus: new MemoryBus(),              // ← later: RedisBus
  kv: new MemoryKv(),                // ← later: RedisKv
  queue,                             // ← later: QStashQueue
  resolveModel: (name) => ({ instance: () => openai(name), contextWindow: 128_000 }),
});

queue.bind((job) => runtime.worker.handleJob(job));

export const chat = runtime.createStreamTextAgent({ name: 'chat', model: 'gpt-4o' });
await chat.run({ prompt: 'hi' });   // persisted, dispatched, running
```

And the client:

```tsx
'use client';
import { useAgentThread } from 'use-agentkit';

export function Chat() {
  const { entries, run, stop, agentState, pendingInputs, respondToInput } = useAgentThread();
  const running = agentState === 'RUNNING' || agentState === 'WAITING_FOR_INPUT';

  return (
    <>
      {entries.map((e) => <p key={e.id}>{e.role}: {e.text}</p>)}
      {pendingInputs.map((i) => (
        <button key={i.toolCallId} onClick={() => respondToInput(i.toolCallId, true)}>
          Approve {i.toolName}
        </button>
      ))}
      <button onClick={() => (running ? stop() : run('hello'))}>
        {running ? 'Stop' : 'Send'}
      </button>
    </>
  );
}
```

## What you get

| | |
| :--- | :--- |
| **Detached runs** | `run()` persists, enqueues and returns. The model call happens somewhere else, later. |
| **Resumable steps** | Each step's messages are durable before the next begins. A worker that dies resumes rather than restarts. |
| **Stop that sticks** | Every run carries an identity, so a stop and a fast resend cannot race each other. |
| **Human in the loop** | A parked approval is a durable state holding no process — no worker, no lock, no memory. It expires on a queue timer, watched or not. |
| **Subagents** | A subagent *is* a run. Same loop, same table, same persistence — so it can use tools, park for a human, and be resumed. |
| **Multi-client sync** | Hydrate from the durable log, tail the bus from a cursor. Two tabs, a reload, a mid-run reconnect all converge. |
| **Context compaction** | History always fits the model's budget. You never prune by hand. |
| **Prompt caching** | Breakpoints on the stable prefix, with provider-correct token attribution. |
| **Operational history** | Runs, steps, timings and token splits in the platform's own tables, so a dashboard never touches your database. |
| **Your database** | Four interfaces. Postgres, Mongo, Dynamo, SQLite — the engine does not know. |

## Documentation

Full docs live in **[`docs/`](./docs/README.md)**.

| | |
| :--- | :--- |
| [Getting started](./docs/getting-started.md) | Install and first run |
| [Core concepts](./docs/concepts.md) | Threads, runs, steps, the loop |
| [setupAgentCore](./docs/setup.md) | Every option, fully |
| [HTTP API](./docs/http-api.md) | The endpoints you expose |
| [Agents and tools](./docs/agents-and-tools.md) | Registering what runs |
| [Human in the loop](./docs/human-in-the-loop.md) | Approvals |
| [Subagents](./docs/subagents.md) | Nested runs |
| [Context and tokens](./docs/context-and-tokens.md) | Compaction, caching, spend |
| [Provider options](./docs/provider-options.md) | Provider-specific settings |
| [Run state](./docs/run-state.md) | Carrying context through a run |
| [Multi-tenancy](./docs/multi-tenancy.md) | A worked isolation story |
| [Ports and adapters](./docs/ports-and-adapters.md) | Wiring your stack |
| [Observability](./docs/observability.md) | The operational store |
| [React](./docs/react.md) | `use-agentkit` |
| [Configuration](./docs/configuration.md) | Every setting |
| [Production](./docs/production.md) | Deployment and a checklist |
| [Troubleshooting](./docs/troubleshooting.md) | Symptoms and causes |

The `§` references throughout the source point at a behavioural specification
that no longer lives in the repository. The pages above are the written
description of that behaviour; the test suite is the executable one.

## Repository

```
packages/
  agentic-kit/        @agentic-kit/core — the runtime
    src/core/         the engine: loop, HITL, subagents, compaction, run identity
    src/ports/        the four interfaces you implement
    src/adapters/     reference adapters (Prisma, Redis, QStash, Upstash, SQLite, memory)
    src/admin/        the platform's own operational store
  use-agentkit/       the React hook
examples/
  nextjs-app/         a full integration — an example, not the product
docs/                 the documentation
```

## Development

Requires [Bun](https://bun.sh) ≥ 1.4 to work in this repository. The text
lockfile needs 1.2, and the test suite's mock model comes from `ai/test`,
which needs a newer `node:http` shim than 1.2 provides. CI runs 1.4 and
`latest`.

The published packages declare `engines.bun >= 1.1` — that floor is for
*consuming* them and is not exercised by CI.

```bash
bun install
bun run test        # both packages
bun run typecheck
bun run build
```

Some tests need Postgres for the operational store. Start one, point
`TEST_ADMIN_PG` at it, or set `SKIP_PG_TESTS=1` to opt out deliberately — an
unreachable database fails loudly rather than passing with nothing asserted.

```bash
docker run -d -p 5433:5432 \
  -e POSTGRES_PASSWORD=password -e POSTGRES_DB=agentic_admin_test postgres:16
```

The example app additionally expects Postgres, Redis and a local QStash; see
[its readme](./examples/nextjs-app/README.md).

## Status

Pre-1.0, and honest about it. The engine is covered by a suite that drives real
adapters, and the packages are verified by installing them from a tarball in CI
— but the API will move before 1.0. Pin exact versions.

## License

MIT
