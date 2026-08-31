import { Client } from '@upstash/qstash';
import { Redis } from '@upstash/redis';
import { PrismaClient } from '@prisma/client';
import { createAgentRuntime } from '@agent/core';
import { PrismaStorage } from '@agent/core/adapters/prisma';
import { UpstashBus, UpstashKv } from '@agent/core/adapters/upstash';
import { QStashQueue } from '@agent/core/adapters/qstash';
import { modelRegistry } from './models'; // §2.3 — users register any `ai`-SDK models

const redis = new Redis({
  url: process.env.UPSTASH_REDIS_REST_URL!,
  token: process.env.UPSTASH_REDIS_REST_TOKEN!,
});

/** The ONLY vendor-wiring file in the example app (spec §5). Swap any adapter
 *  here — Mongo/Dynamo storage, SQS/BullMQ queue, Ably/Kafka bus — and every
 *  route below keeps working unchanged. */
export const runtime = createAgentRuntime({
  storage: new PrismaStorage(new PrismaClient()),
  bus: new UpstashBus(redis, {
    // Upstash Pub/Sub over REST needs a WebSocket subscriber; wire yours here.
    // Replay-only usage works without it (runtime.events.since).
    subscribe: async (threadId, handler) => {
      void threadId; void handler;
      return () => undefined;
    },
  }),
  queue: new QStashQueue(
    new Client({ token: process.env.QSTASH_TOKEN! }),
    { url: `${process.env.APP_URL!}/api/queue/agent-run` },
  ),
  kv: new UpstashKv(redis),
  models: modelRegistry,
});
