import { createClient } from 'redis';
import { Client } from '@upstash/qstash';
import { PrismaClient } from '@prisma/client';
import { z } from 'zod';
import { tool } from 'ai';
import {
  BraveWebSearch,
  JinaReader,
  JinaWebSearch,
  markRequiresConfirmation,
  PageReader,
  pricing,
  PrismaStorage,
  QStashQueue,
  RedisBus,
  RedisKv,
  RedisRunStreams,
  setupAgentCore,
} from 'agentenkit';
import { modelIds, modelPrices, modelRegistry, modelWindows } from './models'; // §2.3 — models in your shape

// ⚠ Next dev re-evaluates this module on every hot reload, and a fresh
// evaluation would open its own Prisma pool and Redis connection that nothing
// ever closes. After enough edits Postgres refuses new clients ("sorry, too
// many clients already") and the app dies with it. Cache both on globalThis so
// a reload reuses what is already open. Production evaluates this module once,
// so the cache is never populated there.
const dev = process.env.NODE_ENV !== 'production';
const cache = globalThis as unknown as {
  agentRedis?: ReturnType<typeof createClient>;
  agentPrisma?: PrismaClient;
};

// Local Docker / self-hosted Redis — no Upstash required.
// (Using Upstash instead? Swap in UpstashKv/UpstashBus from 'agentenkit/adapters/upstash'.)
const redis =
  cache.agentRedis ??
  createClient({ url: process.env.REDIS_URL ?? 'redis://localhost:6379' });
if (!redis.isOpen) await redis.connect();

const prisma = cache.agentPrisma ?? new PrismaClient();

if (dev) {
  cache.agentRedis = redis;
  cache.agentPrisma = prisma;
}

// The web tools' adapters. Each key is passed in here, at setup; nothing
// reads it later. Brave when its key is set, else Jina; with neither, the
// agent can still read pages (our PageReader is free) but not search.
const braveKey = process.env.BRAVE_API_KEY;
const jinaKey = process.env.JINA_API_KEY;
const search = braveKey
  ? new BraveWebSearch({ apiKey: braveKey })
  : jinaKey
    ? new JinaWebSearch({ apiKey: jinaKey })
    : undefined;
// Our own reader by default. JinaReader reads pages built with JavaScript and
// PDFs, for a small price: set WEB_READER=jina to use it.
const fetcher = process.env.WEB_READER === 'jina' && jinaKey ? new JinaReader({ apiKey: jinaKey }) : new PageReader();

/** The ONLY vendor-wiring file in the example app (spec §5). Swap any adapter
 *  here — Mongo/Dynamo storage, SQS/BullMQ queue, Ably/Kafka bus — and every
 *  route below keeps working unchanged. */
export const runtime = await setupAgentCore({
  storage: new PrismaStorage(prisma),
  bus: new RedisBus(redis), // emits HEARTBEAT notices driving the §2.5 watchdog
  queue: new QStashQueue(
    new Client({ token: process.env.QSTASH_TOKEN! }),
    { url: `${process.env.APP_URL!}/api/queue/agent-run` },
  ),
  kv: new RedisKv(redis),
  // A run's live events: one short-lived Redis Stream per run segment, which
  // any process can read, so a tab that reconnects mid-run picks up where it
  // left off. Kept 10 minutes after the segment ends (config.streamGraceMs).
  streams: new RedisRunStreams(redis),

  // Demo settings (§2.5): a 5-minute approval window so expiry is observable —
  // an unanswered park is resolved as the timeout denial ("user had no
  // response, action cancelled") shortly after the TTL.
  config: {
    hitlTtlMs: 5 * 60_000,
    reclaimGraceMs: 15_000,
  },

  // Money (§4): every model call is priced before its usage row is stored, so
  // spend is read back from the same store the tokens come from — no second
  // table, no wrapper around the model. Swap `pricing.table` for
  // `pricing.chain(pricing.receipt(...), pricing.table(...))` if your gateway
  // sends the real figure back and you want that instead of a price list.
  // Tool use is priced too, per search, and counts against a run's money cap.
  pricer: pricing.chain(
    pricing.table(modelPrices),
    pricing.tools({ brave: { perUse: 0.005 }, 'jina-search': { perUse: 0.0005 } }),
  ),

  // The adapters behind the built-in web tools (see webTools below).
  tools: { ...(search ? { search } : {}), fetcher },

  // Models can come in any shape — the platform only ever sees
  // ResolvedModel { instance, contextWindow, modelId } (§3.3).
  resolveModel: (modelName) => {
    const m = modelRegistry[modelName as keyof typeof modelRegistry];
    if (!m) throw new Error(`Unknown model: ${modelName}`);
    return {
      instance: () => m,
      contextWindow: modelWindows[modelName] ?? 128_000,
      // The wire id this key resolves to, recorded on every usage row (§4).
      modelId: modelIds[modelName],
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

// The built-in web tools: one name and one input shape in every runtime, so
// any model can use them. A search result's id (s1r2) opens the page with
// web_fetch, and a `prompt` has a small model read the page and answer, so
// the page never fills the main context.
const webTools = runtime.builtinTools(search ? ['web_search', 'web_fetch'] : ['web_fetch'], {
  webSearch: { maxUses: 10 },
  webFetch: { maxUses: 20 },
});

/** Registered agent handles (§4). The worker dispatches queue jobs back to
 *  these by name (`RunJob.agent`). */
export const chat = runtime.createStreamTextAgent({
  name: 'chat',
  model: 'gpt-4o',
  system:
    'You are a helpful assistant. For anything current or that you are unsure of, search the web with ' +
    'web_search, then open the most useful results with web_fetch, passing the result id and a prompt that ' +
    'says what you need from the page. Cite the pages you used. For a research task with several parts, ' +
    'hand each part to a subagent with spawnSubagent.',
  // Opt-in delegation (§2.7): the platform injects the scoped spawnSubagent
  // tool, and `tools` here are merged into every child and HITL-wrapped just
  // like the parent's — so a subagent parks for approval too, and is resumed
  // where it stopped rather than restarted. The web tools go to children as
  // well: a researcher subagent searches and reads on its own, billed to the
  // same run.
  subagents: { tools: { sendEmail, ...webTools } },
  tools: { sendEmail, ...webTools },
});

/** Local dev without QStash cloud: when INLINE_WORKER=1, the run route
 *  executes the engine in-process (same code path as the worker) instead of
 *  relying on the queue delivery. Set to 0 in production. */
export const INLINE_WORKER = process.env.INLINE_WORKER === '1';
