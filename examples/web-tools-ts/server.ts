/**
 * web_search and web_fetch, and nothing else: the smallest app that shows
 * the built-in web tools. One file, nothing to stand up. SQLite on disk, a
 * queue in this process, and the shared page from ../tools-ui.
 *
 * The same app in Go is ../web-tools-go.
 */
import { Database } from 'bun:sqlite';
import { existsSync, readFileSync } from 'node:fs';
import { join } from 'node:path';
import { createOpenAI } from '@ai-sdk/openai';
import { BraveWebSearch, JinaReader, JinaWebSearch, PageReader, pricing, setupAgentCore, type Search } from 'agentenkit';
import { InlineQueue } from 'agentenkit/adapters/inline';
import { MemoryBus, MemoryKv } from 'agentenkit/adapters/memory';
import { SqliteRunStreams, SqliteStorage } from 'agentenkit/adapters/sqlite';
import { SqliteAdminStore } from 'agentenkit/admin/sqlite';

// Keys come from this folder's .env (Bun reads it), then the repo's root .env.
loadEnv(join(import.meta.dir, '../../.env'));
const port = Number(process.env.PORT || 3101);
const openaiKey = process.env.OPENAI_API_KEY;
if (!openaiKey) {
  console.error('Set OPENAI_API_KEY in examples/web-tools-ts/.env (or the repo root .env).');
  process.exit(1);
}
const model = process.env.MODEL || 'gpt-4o-mini';
const openai = createOpenAI({ apiKey: openaiKey, compatibility: 'strict' });

// The web tools' adapters. Each key is passed in here, at setup; nothing
// reads it later. Brave when its key is set, else Jina. With neither, the
// agent can still read pages (our PageReader is free) but not search.
const braveKey = process.env.BRAVE_API_KEY;
const jinaKey = process.env.JINA_API_KEY;
const search: Search | undefined = braveKey
  ? new BraveWebSearch({ apiKey: braveKey })
  : jinaKey
    ? new JinaWebSearch({ apiKey: jinaKey })
    : undefined;
// Our own reader by default. WEB_READER=jina reads through Jina Reader,
// which handles pages built with JavaScript, and PDFs.
const fetcher = process.env.WEB_READER === 'jina' && jinaKey ? new JinaReader({ apiKey: jinaKey }) : new PageReader();

const db = new Database(process.env.DB_FILE || join(import.meta.dir, 'web-tools.sqlite'));
const storage = new SqliteStorage(db);
const queue = new InlineQueue();

const runtime = await setupAgentCore({
  storage,
  streams: new SqliteRunStreams(db),
  admin: SqliteAdminStore.open(db),
  bus: new MemoryBus(),
  kv: new MemoryKv(),
  queue,
  resolveModel: (name) => ({ instance: () => openai(name), contextWindow: 128_000 }),
  // web_fetch with a prompt has a small model read the page; this is it.
  config: { compactionModel: 'gpt-4o-mini' },
  tools: { ...(search ? { search } : {}), fetcher },
  // Money on every usage row: the model calls, and each search.
  pricer: pricing.chain(
    pricing.table({
      'gpt-4o-mini': { inputPerMillion: 0.15, cacheReadPerMillion: 0.075, outputPerMillion: 0.6 },
      'gpt-4o': { inputPerMillion: 2.5, cacheReadPerMillion: 1.25, outputPerMillion: 10 },
    }),
    pricing.tools({ brave: { perUse: 0.005 }, 'jina-search': { perUse: 0.0005 } }),
  ),
});
queue.bind((job) => runtime.worker.handleJob(job));

// The built-in tools: one name and one input shape in every runtime, so any
// model can use them. A search result's id (s1r2) opens the page in
// web_fetch, and a prompt has a small model read the page and answer, so the
// page never fills the main context.
const webTools = runtime.builtinTools(search ? ['web_search', 'web_fetch'] : ['web_fetch'], {
  webSearch: { maxUses: 10 },
  webFetch: { maxUses: 20 },
});

const chat = runtime.createStreamTextAgent({
  name: 'researcher',
  model,
  system:
    'You answer questions from the web. Search with web_search, then open the most useful results with ' +
    'web_fetch, passing the result id and a prompt that says what you need from the page. Cite the pages ' +
    'you used. For a question with several parts, hand each part to a subagent with spawnSubagent.',
  tools: webTools,
  // Subagents get the web tools too, and are billed to the same run.
  subagents: { tools: webTools },
});

/** What the shared page shows about this app. */
const info = {
  title: 'Web tools',
  subtitle: 'web_search and web_fetch: search the web and read pages, on any model.',
  runtime: 'ts',
  tools: search ? ['web_search', 'web_fetch'] : ['web_fetch'],
  prompts: [
    'What is the latest version of the MCP spec? Cite the page.',
    'Summarise https://en.wikipedia.org/wiki/Ada_Lovelace in three sentences.',
    'Compare the three biggest vector databases, one subagent each.',
  ],
  notes: [
    search ? `search: ${search.name}` : 'No BRAVE_API_KEY or JINA_API_KEY: web_fetch only, no search',
    `reader: ${fetcher.name}`,
    `model: ${model}`,
  ],
};

// ---- the HTTP contract the React hook expects ----

const ui = join(import.meta.dir, '../tools-ui/dist');
if (!existsSync(join(ui, 'index.html'))) {
  console.error('Build the page first: bun run --cwd examples/tools-ui build');
  process.exit(1);
}

const json = (body: unknown, status = 200) => Response.json(body, { status });

Bun.serve({
  port,
  // The event stream stays open for as long as a tab watches the thread.
  idleTimeout: 0,
  async fetch(req) {
    const url = new URL(req.url);
    const q = url.searchParams;
    switch (`${req.method} ${url.pathname}`) {
      case 'GET /api/info':
        return json(info);
      case 'POST /api/agent/run': {
        const body = await req.json();
        const res = await chat.run({
          threadId: body.threadId, prompt: body.prompt, editMessageId: body.editMessageId,
          clientMessageId: body.clientMessageId,
        });
        return json(res, res.accepted ? 202 : 409);
      }
      case 'POST /api/agent/control': {
        const res = await chat.stop((await req.json()).threadId);
        return json(res, res.accepted ? 200 : 409);
      }
      case 'POST /api/agent/respond': {
        const res = await runtime.hitl.respond(await req.json());
        return json(res, res.delivered ? 200 : 409);
      }
      case 'GET /api/agent/stream': {
        const threadId = q.get('threadId')!;
        void runtime.hitl.reclaimIfOrphaned(threadId);
        const { stream, headers } = runtime.events.sse(threadId, {
          cursor: req.headers.get('last-event-id') ?? q.get('cursor') ?? q.get('since'),
          lastMessageId: q.get('lastMessageId') ?? undefined,
          signal: req.signal,
        });
        return new Response(stream, { headers });
      }
      case 'GET /api/agent/history': {
        const snap = await runtime.getThreadSnapshot(q.get('threadId')!);
        return snap ? json(snap) : json({ error: 'Thread not found' }, 404);
      }
      case 'GET /api/agent/usage': {
        const usage = await runtime.getThreadUsage(q.get('threadId')!);
        return usage ? json(usage) : json({ error: 'Thread not found' }, 404);
      }
      case 'GET /api/threads':
        return json({ threads: await threadList() });
      case 'DELETE /api/threads': {
        const res = await runtime.deleteThread(q.get('threadId')!);
        return json(res, res.accepted ? 200 : res.error === 'Thread not found' ? 404 : 409);
      }
    }
    if (url.pathname.startsWith('/api/')) return json({ error: 'not found' }, 404);
    // The page: a file from the build, else index.html.
    const file = Bun.file(join(ui, url.pathname === '/' ? 'index.html' : url.pathname));
    return new Response((await file.exists()) ? file : Bun.file(join(ui, 'index.html')));
  },
});
console.log(`Web tools (TypeScript) on http://localhost:${port}`);

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
