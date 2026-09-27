/**
 * bash, code_execution and text_editor, and nothing else: the smallest app
 * that shows the built-in sandbox tools. One file, nothing else to stand up:
 * SQLite on disk, a queue in this process, and the shared page from
 * ../tools-ui. The sandbox is Docker by default.
 *
 * The same app in Go is ../sandbox-tools-go.
 */
import { Database } from 'bun:sqlite';
import { existsSync, readFileSync } from 'node:fs';
import { join } from 'node:path';
import { createOpenAI } from '@ai-sdk/openai';
import { DockerSandbox, E2BSandbox, LocalSandbox, pricing, setupAgentCore, type SandboxProvider } from 'agentenkit';
import { InlineQueue } from 'agentenkit/adapters/inline';
import { MemoryBus, MemoryKv } from 'agentenkit/adapters/memory';
import { SqliteRunStreams, SqliteStorage } from 'agentenkit/adapters/sqlite';
import { SqliteAdminStore } from 'agentenkit/admin/sqlite';

// Keys come from this folder's .env (Bun reads it), then the repo's root .env.
loadEnv(join(import.meta.dir, '../../.env'));
const port = Number(process.env.PORT || 3103);
const openaiKey = process.env.OPENAI_API_KEY;
if (!openaiKey) {
  console.error('Set OPENAI_API_KEY in examples/sandbox-tools-ts/.env (or the repo root .env).');
  process.exit(1);
}
const model = process.env.MODEL || 'gpt-4o-mini';
const openai = createOpenAI({ apiKey: openaiKey, compatibility: 'strict' });

// Where the commands run: one sandbox per thread, kept between messages and
// ended after 30 idle minutes. SANDBOX picks it:
//   docker (the default)  a container per thread; needs a Docker daemon
//   e2b                   a hosted sandbox; needs E2B_API_KEY (the key is passed here, at setup)
//   local                 a folder on this machine, with NO isolation: only for trying things out
const kind = process.env.SANDBOX || 'docker';
let sandbox: SandboxProvider;
if (kind === 'e2b') {
  if (!process.env.E2B_API_KEY) {
    console.error('SANDBOX=e2b needs E2B_API_KEY.');
    process.exit(1);
  }
  sandbox = new E2BSandbox({ apiKey: process.env.E2B_API_KEY });
} else if (kind === 'local') {
  sandbox = new LocalSandbox();
} else if (kind === 'docker') {
  sandbox = new DockerSandbox({ image: process.env.SANDBOX_IMAGE || 'python:3.12-slim' });
} else {
  console.error(`SANDBOX must be docker, e2b or local, not ${JSON.stringify(kind)}.`);
  process.exit(1);
}
// A local sandbox is a folder on this machine, and its paths reach the rest
// of it: a program or a file view could read this app's .env. So there every
// tool asks first. Elsewhere bash and file changes ask; running code and
// viewing files do not.
const local = kind === 'local';

const db = new Database(process.env.DB_FILE || join(import.meta.dir, 'sandbox-tools.sqlite'));
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
  tools: { sandbox },
  // Money on every usage row: the model calls, and E2B time per second (2
  // vCPUs). A Docker or local sandbox runs on your machine and costs nothing.
  pricer: pricing.chain(
    pricing.table({
      'gpt-4o-mini': { inputPerMillion: 0.15, cacheReadPerMillion: 0.075, outputPerMillion: 0.6 },
      'gpt-4o': { inputPerMillion: 2.5, cacheReadPerMillion: 1.25, outputPerMillion: 10 },
    }),
    pricing.tools({ e2b: { perSecond: 0.000028 }, docker: {}, local: {} }),
  ),
});
queue.bind((job) => runtime.worker.handleJob(job));

// The built-in tools: one name and one input shape in every runtime, so any
// model can use them. bash keeps its folder from one command to the next;
// code_execution runs Python or JavaScript and lists the files it made;
// text_editor views and changes files by exact replacement, with undo.
const sandboxTools = runtime.builtinTools(['bash', 'code_execution', 'text_editor'], {
  bash: { maxUses: 30 },
  codeExecution: { maxUses: 20, ...(local ? { approval: true } : {}) },
  textEditor: local ? { approval: true } : {},
});

const chat = runtime.createStreamTextAgent({
  name: 'coder',
  model,
  system:
    'You work in a sandbox: a separate machine kept for this conversation, with Python. Run Python with ' +
    'code_execution to calculate things, use bash for shell commands, and text_editor to write and change ' +
    'files. Say what you did and show the results.',
  tools: sandboxTools,
  // Subagents get the same tools and share the thread's sandbox.
  subagents: { tools: sandboxTools },
});

/** What the shared page shows about this app. */
const info = {
  title: 'Sandbox tools',
  subtitle: 'bash, code_execution and text_editor, in one sandbox per conversation.',
  runtime: 'ts',
  tools: ['bash', 'code_execution', 'text_editor'],
  prompts: [
    'Use Python to find the first 20 prime numbers and save them to primes.txt.',
    'Make a file notes.md with three short lines, change the second line, then show me the file.',
    'Create app/main.py that prints hello, then run it with bash.',
    'What version of Python is in the sandbox, and what files are in the working folder?',
  ],
  notes: [
    `sandbox: ${kind}`,
    local ? 'local sandbox: no isolation, so every tool asks first' : 'bash and file changes ask first',
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
console.log(`Sandbox tools (TypeScript) on http://localhost:${port}`);

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
