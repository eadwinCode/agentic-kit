# @agent/core

Headless multi-tenant real-time AI agent platform: durable queue-dispatched runs, coordinated multi-user streaming, HITL (human-in-the-loop) suspension, subagent delegation, and context-window compaction — as a **library**. Bring your own storage, queue, and pub/sub by implementing four small ports.

The [technical specification](./agent-platform-technical-spec.md) (§ numbers below refer to it) is the behavioral source of truth.

## Install

```bash
npm install @agent/core ai zod
# optional reference adapters:
npm install @prisma/client @upstash/redis @upstash/qstash
```

## Quick start

```typescript
import { Client } from '@upstash/qstash';
import { Redis } from '@upstash/redis';
import { PrismaClient } from '@prisma/client';
import { createAgentRuntime, PrismaStorage, UpstashBus, UpstashKv, QStashQueue } from '@agent/core';

const redis = new Redis({ url: ..., token: ... });

export const runtime = createAgentRuntime({
  storage: new PrismaStorage(new PrismaClient()),  // ← swap for Mongo/Dynamo/your DB
  bus: new UpstashBus(redis),                      // ← Ably / Kafka / Postgres LISTEN…
  queue: new QStashQueue(
    new Client({ token: process.env.QSTASH_TOKEN! }),
    { url: 'https://app.example.com/api/queue/agent-run' },
  ),
  kv: new UpstashKv(redis),
  models: modelRegistry,                           // any `ai`-SDK models (§2.3)
});
```

### Framework-agnostic operations

```typescript
await runtime.run({ prompt: 'hi', model: 'gpt-4o' });      // persist + enqueue → 202 (§5.1)
await runtime.stop(threadId);                              // one write: state → CANCELLED (§2.1)
await runtime.hitl.respond({ threadId, toolCallId, approved, payload });  // §2.5
await runtime.hitl.reclaimIfOrphaned(threadId);            // §2.5 (listeners call this)
const missed = await runtime.events.since(threadId, lastSeq);             // SSE replay (§2.2)
const unsub = await runtime.events.subscribe(threadId, handler);          // live tail
await runtime.engine.executeWithPolicy({ threadId, model });// worker-side only (§2.8)
```

The Next.js routes in [`examples/nextjs-app`](./examples/nextjs-app) show the HTTP wiring — each handler is a few lines over the runtime.

## Ports (§3.2)

| Port | Role | Reference adapter |
| :--- | :--- | :--- |
| `Storage` | threads / messages / events / usage / runs — incl. atomic `claimState` | `PrismaStorage` |
| `EventBus` | live fan-out + HITL death notices (at-most-once) | `UpstashBus` |
| `Queue` | durable run dispatch (at-least-once) | `QStashQueue` |
| `Kv` | hot state cache, HITL handoff keys, seq/attempt counters | `UpstashKv` |

Implement any of them for your own stack — `core/` imports nothing else. The [`Memory*` adapters](./src/adapters/memory.ts) are a complete in-memory implementation used by the test suite; they double as a template.

### Adapter invariants (§3.4)

1. `events.append` receives `seq` from `kv.incr('agent:seq:{threadId}')` — monotonic per thread.
2. `threads.claimState` must be atomic (one conditional UPDATE) — exactly one caller wins.
3. `queue.enqueue` is at-least-once; the engine is idempotent via the state guard.
4. `bus` is at-most-once; the §2.5 watchdog pattern compensates.
5. Durable thread state lives in `storage.threads`; the kv copy is a hot cache. Writes go to both.

## Behavior map

| Spec | Where in the package |
| :--- | :--- |
| §2.1 Detached execution & stop | `core/run.ts`, `core/stop.ts`, `core/engine.ts` |
| §2.2 Multi-user stream | `ports/bus.ts` + `runtime.events` |
| §2.5 HITL (waitForEvent / respond / orphan reclaim) | `core/hitl.ts`, `core/reclaim.ts` |
| §2.6 Context ceiling & compaction | `core/context.ts` |
| §2.7 Subagents (depth, semaphore, no timeout) | `core/subagent.ts` |
| §2.8 Queue dispatch + redrive/FAIL policy | `ports/queue.ts`, `core/engine.ts` (`executeWithPolicy`) |
| §5 Reference HTTP integration | `examples/nextjs-app` |

## Development

```bash
npm install
npm test    # tsc build + node --test over the in-memory adapters
```
