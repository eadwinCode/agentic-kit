import { spawn, type ChildProcess } from 'node:child_process';
import { promises as fs } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { envelope } from '../src/adapters/e2b.js';

// A stand-in for E2B's API and the daemon in each sandbox (envd), speaking
// the same requests: enough to run the sandbox suite against E2BSandbox with
// no account. Each sandbox is a temp folder; /home/user is that folder.
// The Go package has the same fake (e2b_fake_test.go).

interface FakeBox {
  dir: string;
  startedAt: Date;
  endAt: Date;
  metadata: Record<string, string>;
  envs: Record<string, string>;
  procs: Map<number, ChildProcess>;
}

export interface FakeE2B {
  url: string;
  /** Every API call, as "METHOD /path". */
  calls: string[];
  /** The last create request's body. */
  lastCreate?: any;
  stop(): void;
}

const TOKEN = 'fake-token';

export async function startFakeE2B(): Promise<FakeE2B> {
  const root = await fs.mkdtemp(join(tmpdir(), 'e2b-fake-'));
  const boxes = new Map<string, FakeBox>();
  let n = 0;
  const fake: FakeE2B = { url: '', calls: [], stop: () => server.stop(true) };

  const map = (box: FakeBox, p: string) =>
    p.startsWith('/home/user') ? join(box.dir, p.slice('/home/user'.length)) : join(box.dir, '__root', p);
  const json = (data: unknown, status = 200) => Response.json(data, { status });
  const rpcError = (code: string, status: number) => json({ code, message: code }, status);
  const created = (id: string) => ({ sandboxID: id, domain: null, envdVersion: '0.4.0', envdAccessToken: TOKEN });

  const server = Bun.serve({
    port: 0,
    async fetch(req) {
      const url = new URL(req.url);
      const path = url.pathname;
      fake.calls.push(`${req.method} ${path}`);
      let m: RegExpMatchArray | null;

      // --- the API ---
      if (req.method === 'POST' && path === '/v2/sandboxes') {
        if (req.headers.get('x-api-key') !== 'test-key') return json({ message: 'bad key' }, 401);
        const body = (fake.lastCreate = await req.json()) as any;
        const id = `fake${++n}`;
        const dir = join(root, id);
        await fs.mkdir(dir, { recursive: true });
        boxes.set(id, {
          dir,
          startedAt: new Date(),
          endAt: new Date(Date.now() + body.timeout * 1000),
          metadata: body.metadata ?? {},
          envs: body.envVars ?? {},
          procs: new Map(),
        });
        return json(created(id), 201);
      }
      if ((m = path.match(/^\/v2\/sandboxes\/([^/]+)\/connect$/)) && req.method === 'POST') {
        return boxes.has(m[1]!) ? json(created(m[1]!)) : json({ message: 'not found' }, 404);
      }
      if ((m = path.match(/^\/sandboxes\/([^/]+)\/timeout$/)) && req.method === 'POST') {
        const box = boxes.get(m[1]!);
        if (!box) return json({ message: 'not found' }, 404);
        box.endAt = new Date(Date.now() + ((await req.json()) as any).timeout * 1000);
        return new Response(null, { status: 204 });
      }
      if ((m = path.match(/^\/sandboxes\/([^/]+)$/))) {
        const box = boxes.get(m[1]!);
        if (!box) return json({ message: 'not found' }, 404);
        if (req.method === 'DELETE') {
          boxes.delete(m[1]!);
          for (const p of box.procs.values()) p.kill('SIGKILL');
          await fs.rm(box.dir, { recursive: true, force: true });
          return new Response(null, { status: 204 });
        }
        return json({
          sandboxID: m[1], templateID: 'base', startedAt: box.startedAt.toISOString(),
          endAt: box.endAt.toISOString(), metadata: box.metadata, state: 'running',
        });
      }

      // --- envd, reached through sandboxUrl with the sandbox in a header ---
      const box = boxes.get(req.headers.get('e2b-sandbox-id') ?? '');
      if (!box) return new Response('sandbox not found', { status: 502 });
      if (req.headers.get('x-access-token') !== TOKEN) return json({ message: 'bad token' }, 401);

      if (path === '/files') {
        const file = map(box, url.searchParams.get('path')!);
        if (req.method === 'GET') {
          const stat = await fs.stat(file).catch(() => null);
          if (!stat?.isFile()) return json({ code: 404, message: 'file not found' }, 404);
          return new Response(await fs.readFile(file));
        }
        const form = await req.formData();
        const blob = form.get('file') as Blob;
        await fs.mkdir(dirname(file), { recursive: true });
        await fs.writeFile(file, new Uint8Array(await blob.arrayBuffer()));
        return json([{ name: file.split('/').pop(), type: 'file', path: url.searchParams.get('path') }]);
      }
      if (path.startsWith('/filesystem.Filesystem/')) {
        const body = (await req.json()) as { path: string };
        const target = map(box, body.path);
        const stat = await fs.stat(target).catch(() => null);
        switch (path.split('/').pop()) {
          case 'ListDir': {
            if (!stat?.isDirectory()) return rpcError('not_found', 404);
            const entries = [];
            for (const d of await fs.readdir(target, { withFileTypes: true })) {
              const size = d.isFile() ? (await fs.stat(join(target, d.name))).size : 0;
              entries.push({ name: d.name, type: d.isDirectory() ? 'FILE_TYPE_DIRECTORY' : 'FILE_TYPE_FILE', size: String(size) });
            }
            return json({ entries });
          }
          case 'MakeDir':
            if (stat) return rpcError('already_exists', 409);
            await fs.mkdir(target, { recursive: true });
            return json({ entry: { name: body.path } });
          case 'Stat':
            return stat ? json({ entry: { name: body.path } }) : rpcError('not_found', 404);
          case 'Remove':
            if (!stat) return rpcError('not_found', 404);
            await fs.rm(target, { recursive: true, force: true });
            return json({});
        }
      }
      if (path === '/process.Process/SendSignal') {
        const body = (await req.json()) as any;
        const proc = box.procs.get(body.process.pid);
        if (proc) {
          try {
            process.kill(-proc.pid!, 'SIGKILL');
          } catch {
            proc.kill('SIGKILL');
          }
        }
        return json({});
      }
      if (path === '/process.Process/Start') {
        if (req.headers.get('content-type') !== 'application/connect+json') return json({ message: 'want connect+json' }, 415);
        const raw = new Uint8Array(await req.arrayBuffer());
        const size = new DataView(raw.buffer).getUint32(1);
        const start = JSON.parse(new TextDecoder().decode(raw.subarray(5, 5 + size))) as any;
        const p = start.process;
        const cwd = map(box, p.cwd);
        const child = spawn(p.cmd, p.args, {
          cwd,
          env: { PATH: process.env.PATH, HOME: box.dir, ...box.envs, ...p.envs },
          detached: true,
          stdio: ['ignore', 'pipe', 'pipe'],
        });
        box.procs.set(child.pid!, child);
        const stream = new ReadableStream<Uint8Array>({
          start(controller) {
            // The client may hang up first; what comes after goes nowhere.
            const send = (message: unknown, flags = 0) => {
              try {
                controller.enqueue(envelope(message, flags));
              } catch {}
            };
            send({ event: { start: { pid: child.pid } } });
            child.stdout!.on('data', (b: Buffer) => send({ event: { data: { stdout: b.toString('base64') } } }));
            child.stderr!.on('data', (b: Buffer) => send({ event: { data: { stderr: b.toString('base64') } } }));
            child.on('close', (code, signal) => {
              box.procs.delete(child.pid!);
              // Protobuf JSON leaves a zero out, as envd does.
              const exitCode = signal ? -1 : code;
              send({ event: { end: exitCode ? { exitCode, exited: !signal } : { exited: true } } });
              send({}, 2);
              try {
                controller.close();
              } catch {}
            });
          },
          cancel() {
            child.kill('SIGKILL');
          },
        });
        return new Response(stream, { headers: { 'content-type': 'application/connect+json' } });
      }
      return json({ message: `no route ${path}` }, 404);
    },
  });
  fake.url = `http://localhost:${server.port}`;
  return fake;
}
