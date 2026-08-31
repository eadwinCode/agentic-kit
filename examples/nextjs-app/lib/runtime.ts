import { createClient } from 'redis';
import { Client } from '@upstash/qstash';
import { PrismaClient } from '@prisma/client';
import { createAgentRuntime } from '@agent/core';
import { PrismaStorage } from '@agent/core/adapters/prisma';
import { RedisBus, RedisKv } from '@agent/core/adapters/redis';
import { QStashQueue } from '@agent/core/adapters/qstash';
import { modelRegistry } from './models'; // §2.3 — users register any `ai`-SDK models

// Local Docker / self-hosted Redis — no Upstash required.
// (Using Upstash instead? Swap in UpstashKv/UpstashBus from '@agent/core/adapters/upstash'.)
const redis = createClient({ url: process.env.REDIS_URL ?? 'redis://localhost:6379' });
await redis.connect();

/** The ONLY vendor-wiring file in the example app (spec §5). Swap any adapter
 *  here — Mongo/Dynamo storage, SQS/BullMQ queue, Ably/Kafka bus — and every
 *  route below keeps working unchanged. */
export const runtime = createAgentRuntime({
  storage: new PrismaStorage(new PrismaClient()),
  bus: new RedisBus(redis), // emits HEARTBEAT notices driving the §2.5 watchdog
  queue: new QStashQueue(
    new Client({ token: process.env.QSTASH_TOKEN! }),
    { url: `${process.env.APP_URL!}/api/queue/agent-run` },
  ),
  kv: new RedisKv(redis),
  models: modelRegistry,
});
