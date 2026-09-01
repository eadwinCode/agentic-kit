import { createClient } from 'redis';
import { Client } from '@upstash/qstash';
import { PrismaClient } from '@prisma/client';
import { z } from 'zod';
import { tool } from 'ai';
import {
  markRequiresConfirmation,
  PrismaStorage,
  QStashQueue,
  RedisBus,
  RedisKv,
  setupAgentCore,
} from '@agent/core';
import { modelRegistry } from './models'; // §2.3 — models in your shape

// Local Docker / self-hosted Redis — no Upstash required.
// (Using Upstash instead? Swap in UpstashKv/UpstashBus from '@agent/core/adapters/upstash'.)
const redis = createClient({ url: process.env.REDIS_URL ?? 'redis://localhost:6379' });
await redis.connect();

/** The ONLY vendor-wiring file in the example app (spec §5). Swap any adapter
 *  here — Mongo/Dynamo storage, SQS/BullMQ queue, Ably/Kafka bus — and every
 *  route below keeps working unchanged. */
export const runtime = setupAgentCore({
  storage: new PrismaStorage(new PrismaClient()),
  bus: new RedisBus(redis), // emits HEARTBEAT notices driving the §2.5 watchdog
  queue: new QStashQueue(
    new Client({ token: process.env.QSTASH_TOKEN! }),
    { url: `${process.env.APP_URL!}/api/queue/agent-run` },
  ),
  kv: new RedisKv(redis),

  // Demo settings (§2.5): a 5-minute approval window so expiry is observable —
  // an unanswered park is resolved as the timeout denial ("user had no
  // response, action cancelled") shortly after the TTL.
  config: {
    hitlTtlMs: 5 * 60_000,
    reclaimGraceMs: 15_000,
  },

  // Models can come in any shape — the platform only ever sees
  // ResolvedModel { instance, contextWindow } (§3.3).
  resolveModel: (modelName) => {
    const m = modelRegistry[modelName as keyof typeof modelRegistry];
    if (!m) throw new Error(`Unknown model: ${modelName}`);
    return {
      instance: () => m,
      contextWindow: 265_000,
    };
  },
});

// ⚠ Registered IN THIS FILE, right where the runtime is built: Next.js gives
// each route bundle its own module instance, and the registry lives inside
// setupAgentCore's closure. A worker route that imported the runtime from a
// file that doesn't ALSO register the agents would resolve every queue job to
// 'unknown-agent' (§5: `RunJob.agent` resolves via the same instance that
// created the registry).
const sendEmail = markRequiresConfirmation(
  tool({
    description: 'Sends an email (destructive — requires user approval)',
    parameters: z.object({
      to: z.string().email(),
      subject: z.string(),
      body: z.string(),
    }),
    execute: async ({ to, subject, body }) => ({ status: 'SENT', to, subject, body }),
  }),
);

/** Registered agent handles (§4). The worker dispatches queue jobs back to
 *  these by name (`RunJob.agent`). */
export const chat = runtime.createStreamTextAgent({
  name: 'chat',
  model: 'gpt-4o',
  subagents: true, // opt-in: platform injects the scoped spawnSubagent tool (§2.7)
  tools: { sendEmail },
});

/** Local dev without QStash cloud: when INLINE_WORKER=1, the run route
 *  executes the engine in-process (same code path as the worker) instead of
 *  relying on the queue delivery. Set to 0 in production. */
export const INLINE_WORKER = process.env.INLINE_WORKER === '1';
