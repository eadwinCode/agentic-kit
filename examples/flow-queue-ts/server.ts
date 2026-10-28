/**
 * agentic-kit with Clevix Flow as its queue: every job is a Flow run you can
 * watch in the Flow dashboard. The agent reads pages with web_fetch and saves
 * notes with saveNote, which waits for your approval. SQLite on disk, and the
 * shared page from ../tools-ui.
 *
 * The same app in Go is ../flow-queue-go.
 */
import { Database } from 'bun:sqlite';
import { existsSync, readFileSync } from 'node:fs';
import { join } from 'node:path';
import { createOpenAI } from '@ai-sdk/openai';
import { Flow, serve } from '@clevix/flow';
import { tool } from 'ai';
import { z } from 'zod';
import { PageReader, markRequiresConfirmation, pricing, setupAgentCore } from 'agentenkit';
import { FlowQueue, agentRunFunction } from 'agentenkit/adapters/flow';
import { MemoryBus } from 'agentenkit/adapters/memory';
import { SqliteKv, SqliteRunStreams, SqliteStorage } from 'agentenkit/adapters/sqlite';
import { SqliteAdminStore } from 'agentenkit/admin/sqlite';

// Keys come from this folder's .env, then the repo's root .env. A key
// already in the environment wins.
loadEnv(join(import.meta.dir, '.env'));
loadEnv(join(import.meta.dir, '../../.env'));
const port = Number(process.env.PORT || 3107);
const openaiKey = process.env.OPENAI_API_KEY;
if (!openaiKey) {
  console.error('Set OPENAI_API_KEY in examples/flow-queue-ts/.env (or the repo root .env).');
  process.exit(1);
}
if (!process.env.FLOW_SIGNING_KEY) {
  console.error('Set FLOW_SIGNING_KEY in examples/flow-queue-ts/.env: make an app in the Flow dashboard and copy its signing key.');
  process.exit(1);
}
const model = process.env.MODEL || 'gpt-4o-mini';
const openai = createOpenAI({ apiKey: openaiKey });
// The models this app prices, per million tokens.
const modelPrices = {
  'gpt-4o-mini': { inputPerMillion: 0.15, cacheReadPerMillion: 0.075, outputPerMillion: 0.6 },
  'gpt-4o': { inputPerMillion: 2.5, cacheReadPerMillion: 1.25, outputPerMillion: 10 },
};

// The Flow client reads FLOW_SIGNING_KEY and FLOW_BASE_URL from the
// environment. Empty FLOW_BASE_URL is the hosted engine.
const flow = new Flow({ id: process.env.FLOW_APP_ID || 'agentenkit-flow-ts' });

const db = new Database(process.env.DB_FILE || join(import.meta.dir, 'flow-queue.sqlite'));
const storage = new SqliteStorage(db);

const runtime = await setupAgentCore({
  storage,
  streams: new SqliteRunStreams(db),
  admin: SqliteAdminStore.open(db),
  // One process, so an in-memory bus is enough.
  bus: new MemoryBus(),
  kv: new SqliteKv(db),
  // Each job starts a run of the Flow function below.
  queue: new FlowQueue(flow),
  resolveModel: (name) => {
    if (!(name in modelPrices) && name !== model) throw new Error(`unknown model ${name}`);
    return { instance: () => openai.chat(name), contextWindow: 128_000 };
  },
  config: {
    compactionModel: 'gpt-4o-mini',
    // Flow waits at most 14 minutes for one job, so a segment ends before
    // that. A longer run goes on in a new segment.
    segmentTimeoutMs: 13 * 60_000,
  },
  tools: { fetcher: new PageReader() },
  pricer: pricing.chain(pricing.table(modelPrices), pricing.tools({ 'page-reader': {} })),
});

// The Flow function that runs the jobs, served at /api/flow. Up to 10 jobs
// run at once; priorities only count under a limit like this.
const flowHandler = serve({ client: flow, functions: [agentRunFunction(flow, runtime, { concurrency: 10 })] });

// Parked behind your approval. While it waits, its expiry is a sleeping run
// in the Flow dashboard; your answer cancels it and starts the resume.
const saveNote = markRequiresConfirmation(
  tool({
    description: 'Save a short note for the user (needs their approval).',
    inputSchema: z.object({ title: z.string(), text: z.string() }),
    execute: async ({ title, text }) => ({ status: 'SAVED', title, text }),
  }),
);

const chat = runtime.createStreamTextAgent({
  name: 'notes',
  model,
  system:
    'You read web pages with web_fetch and answer from them. When the user asks you to keep something, ' +
    'save it with saveNote: a short title and the text.',
  tools: { ...runtime.builtinTools(['web_fetch'], { webFetch: { maxUses: 10 } }), saveNote },
});

/** What the shared page shows about this app. */
const info = {
  title: 'Flow queue',
  subtitle: 'agentic-kit on Clevix Flow: each job is a Flow run.',
  runtime: 'ts',
  tools: ['web_fetch', 'saveNote'],
  prompts: [
    'Summarise https://en.wikipedia.org/wiki/Ada_Lovelace in three sentences.',
    'Read https://en.wikipedia.org/wiki/Grace_Hopper and save a note with her three biggest firsts.',
  ],
  notes: [
    `Flow app: ${flow.id}`,
    `engine: ${flow.settings().baseUrl}`,
    `model: ${model}`,
  ],
};

// ---- the HTTP contract the React hook expects, and /api/flow ----

const ui = join(import.meta.dir, '../tools-ui/dist');
if (!existsSync(join(ui, 'index.html'))) {
  console.error('Build the page first: bun run --cwd examples/tools-ui build');
  process.exit(1);
}

const json = (body: unknown, status = 200) => Response.json(body, { status });

Bun.serve({
  port,
  // This machine only: the agent spends your keys. HOST=0.0.0.0 opens it to
  // your network, for an engine on another machine.
  hostname: process.env.HOST || '127.0.0.1',
  // The event stream stays open for as long as a tab watches the thread, and
  // Flow's calls can take minutes.
  idleTimeout: 0,
  async fetch(req) {
    try {
      return await route(req);
    } catch (err) {
      // Bad input is a 400 with a message, as in the Go app.
      if (err instanceof BadRequest) return json({ error: err.message }, 400);
      throw err;
    }
  },
});
console.log(`${info.title} (TypeScript) on http://${process.env.HOST || '127.0.0.1'}:${port}; Flow calls /api/flow`);

class BadRequest extends Error {}

async function route(req: Request): Promise<Response> {
  const url = new URL(req.url);
  if (url.pathname === '/api/flow') return flowHandler(req);
  const q = url.searchParams;
  const body = async () => {
    const b = await req.json().catch(() => null);
    if (!b || typeof b !== 'object') throw new BadRequest('the body must be a JSON object');
    return b as Record<string, any>;
  };
  const threadId = () => {
    const id = q.get('threadId');
    if (!id) throw new BadRequest('threadId is required');
    return id;
  };

  switch (`${req.method} ${url.pathname}`) {
    case 'GET /api/info':
      return json(info);
    case 'POST /api/agent/run': {
      const b = await body();
      const res = await chat.run({
        threadId: b.threadId, prompt: b.prompt, editMessageId: b.editMessageId,
        clientMessageId: b.clientMessageId,
      });
      return json(res, res.accepted ? 202 : 409);
    }
    case 'POST /api/agent/control': {
      const res = await chat.stop((await body()).threadId);
      return json(res, res.accepted ? 200 : 409);
    }
    case 'POST /api/agent/respond': {
      const res = await runtime.hitl.respond((await body()) as Parameters<typeof runtime.hitl.respond>[0]);
      return json(res, res.delivered ? 200 : 409);
    }
    case 'GET /api/agent/stream': {
      const id = threadId();
      void runtime.hitl.reclaimIfOrphaned(id);
      const { stream, headers } = runtime.events.sse(id, {
        cursor: req.headers.get('last-event-id') ?? q.get('cursor') ?? q.get('since'),
        lastMessageId: q.get('lastMessageId') ?? undefined,
        signal: req.signal,
      });
      return new Response(stream, { headers });
    }
    case 'GET /api/agent/history': {
      const snap = await runtime.getThreadSnapshot(threadId());
      return snap ? json(snap) : json({ error: 'Thread not found' }, 404);
    }
    case 'GET /api/agent/usage': {
      const usage = await runtime.getThreadUsage(threadId());
      return usage ? json(usage) : json({ error: 'Thread not found' }, 404);
    }
    case 'GET /api/threads':
      return json({ threads: await threadList() });
    case 'DELETE /api/threads': {
      const res = await runtime.deleteThread(threadId());
      return json(res, res.accepted ? 200 : res.error === 'Thread not found' ? 404 : 409);
    }
  }
  if (url.pathname.startsWith('/api/')) return json({ error: 'not found' }, 404);
  // The page: a file from the build, else index.html.
  const file = Bun.file(join(ui, url.pathname === '/' ? 'index.html' : url.pathname));
  return new Response((await file.exists()) ? file : Bun.file(join(ui, 'index.html')));
}

/** The thread list, newest first, titled by each thread's first message. */
async function threadList() {
  const threads = await runtime.listThreads();
  return Promise.all(
    threads.map(async (t) => {
      const first = (await storage.messages.list(t.id)).find((m) => m.role === 'user');
      const text = typeof first?.content === 'string'
        ? first.content
        : ((first?.content as Array<{ type: string; text?: string }> | undefined)?.find((p) => p.type === 'text')?.text ?? '');
      return { ...t, title: text.slice(0, 40) };
    }),
  );
}

/** KEY=VALUE lines from a .env file, for keys not already set. */
function loadEnv(path: string) {
  if (!existsSync(path)) return;
  for (const line of readFileSync(path, 'utf8').split('\n')) {
    const m = line.trim().match(/^(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*)$/);
    if (!m || process.env[m[1]!] !== undefined) continue;
    process.env[m[1]!] = m[2]!.replace(/^(['"])(.*)\1$/, '$2');
  }
}
