import { createClient } from 'redis';
import { Client } from '@upstash/qstash';
import { PrismaClient } from '@prisma/client';
import {
  createAgentRuntime,
  PrismaStorage,
  QStashQueue,
  RedisBus,
  RedisKv,
} from '@agent/core';
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

/** Local dev without QStash cloud: when INLINE_WORKER=1, the run route
 *  executes the engine in-process (same code path as the worker) instead of
 *  relying on the queue delivery. Set to 0 in production. */
export const INLINE_WORKER = process.env.INLINE_WORKER === '1';
