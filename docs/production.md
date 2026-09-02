# Production

## The swap

The development runtime and the production one are the same shape. Each line
swaps for a durable equivalent.

| Development | Production |
| :--- | :--- |
| `SqliteStorage` | `PrismaStorage`, or your own |
| `InlineQueue` | `QStashQueue`, SQS, BullMQ |
| `MemoryBus` | `RedisBus`, `UpstashBus`, Ably |
| `MemoryKv` | `RedisKv`, `UpstashKv` |
| SQLite admin store | Postgres, via `AGENTIC_KIT_ADMIN_DATABASE_URL` |

```ts
import { Client } from '@upstash/qstash';
import { createClient } from 'redis';
import { PrismaClient } from '@prisma/client';
import { setupAgentCore } from '@agentic-kit/core';
import { PrismaStorage } from '@agentic-kit/core/adapters/prisma';
import { RedisBus, RedisKv } from '@agentic-kit/core/adapters/redis';
import { QStashQueue } from '@agentic-kit/core/adapters/qstash';

const redis = createClient({ url: process.env.REDIS_URL });
await redis.connect();

export const runtime = await setupAgentCore({
  storage: new PrismaStorage(new PrismaClient()),
  bus: new RedisBus(redis),
  queue: new QStashQueue(
    new Client({ token: process.env.QSTASH_TOKEN! }),
    { url: `${process.env.APP_URL}/api/queue/agent-run` },
  ),
  kv: new RedisKv(redis),
  // `admin` omitted: with AGENTIC_KIT_ADMIN_DATABASE_URL set, the platform
  // opens Postgres for its own history.
  resolveModel: (name) => ({ instance: () => models[name], contextWindow: 128_000 }),
});

export const chat = runtime.createStreamTextAgent({ name: 'chat', model: 'gpt-4o' });
```

## Deployment shapes

**Serverless.** The natural fit: `run()` returns in milliseconds, and the queue
delivers to a worker route that acknowledges immediately and finishes the work
in the background. Use your platform's "keep running after the response"
primitive — `waitUntil` on Vercel, `ctx.waitUntil` on Cloudflare. Watch your
function timeout against `runLockLeaseSeconds`.

**Long-lived server.** Also fine. Point the queue at your own HTTP endpoint, or
implement `Queue` over an in-process worker pool.

**Separate worker process.** Run the queue consumer as its own service. It needs
the same runtime module — the same `setupAgentCore` call and the same agent
registrations — because a job resolves its handle by name from the registry.

## Connection reuse in dev

A framework that re-evaluates your runtime module on hot reload will open a new
database pool and a new Redis connection every time, and nothing closes them.
After enough edits Postgres refuses new clients and the app dies with it. Cache
both on `globalThis` in development; production evaluates the module once.

```ts
const dev = process.env.NODE_ENV !== 'production';
const cache = globalThis as unknown as { agentRedis?: RedisClient; agentPrisma?: PrismaClient };

const redis = cache.agentRedis ?? createClient({ url: process.env.REDIS_URL });
if (!redis.isOpen) await redis.connect();
const prisma = cache.agentPrisma ?? new PrismaClient();
if (dev) { cache.agentRedis = redis; cache.agentPrisma = prisma; }
```

## Security

**The queue endpoint executes agents.** Anyone who can `POST` a job to it can
run one on any thread. Verify the signature:

```ts
export const POST = verifySignatureAppRouter(handler);
```

For a queue without signing, use a shared secret header and compare in constant
time. Never leave it open.

**Authorize every other route yourself.** The library has no opinion about who
your users are. `threadId` is not a secret and must not be treated as one — a
user who knows another user's thread id must not be able to read it. The natural
place for that check is your `Storage` implementation, scoped by
[run state](./run-state.md).

**Turn off payload recording when payloads are sensitive.**

```ts
config: { recordPayloads: false }
```

**Keep the admin reads behind your own authorization.** `runtime.admin.*` is
operator data. The sample dashboard in the example app has no auth at all — it is
a sample.

## Scaling

**Workers scale horizontally.** The per-thread run lock means only one worker
advances a given thread at a time; different threads run in parallel freely.

**The bus is per-thread pub/sub.** Every subscriber to a thread receives its
events. A thread with a very large audience is a fan-out problem — that is when
you move from Redis pub/sub to something built for it.

**The operational store grows with every step.** Plan retention. `recordPayloads`
is the main lever on row size; `payloadCapChars` caps the worst case.

**`runLockLeaseSeconds` must exceed your longest run segment.** Too short and a
second worker starts on a thread that is still being advanced. Too long and a
genuinely dead worker blocks the thread until the lease expires. Parked
approvals hold no lock, so a long human wait does not enter into it.

## Rollout checklist

- [ ] Queue endpoint verifies signatures
- [ ] Every other route authorizes the caller
- [ ] `Storage` scopes by tenant via run state, and throws when it is absent
- [ ] Run state passed to reads too — `listThreads`, `getThreadSnapshot`,
      `getThreadUsage`, `deleteThread`, `stop`, `hitl.respond`
- [ ] `threads.claimState` is one atomic conditional update
- [ ] The queue honours `delaySeconds` — or at least never throws for it
- [ ] `runLockLeaseSeconds` > longest run segment
- [ ] `maxSteps` and `tokenBudget` set to values you are willing to pay for
- [ ] `billingPreCheck` wired if runs are billable
- [ ] `recordPayloads` decided deliberately
- [ ] `AGENTIC_KIT_ADMIN_DATABASE_URL` set, and the store reachable at boot
- [ ] Admin reads behind authorization
- [ ] Connections cached in development
- [ ] Retention planned for events and the operational store

## Upgrading

Both packages are pre-1.0. Pin exact versions and read the release notes; the
event log shape and the port signatures are the two surfaces most likely to
move.
