import {
  DEFAULT_COMMAND_TIMEOUT_MS,
  SandboxFileNotFoundError,
  SandboxGoneError,
  TIMED_OUT_EXIT_CODE,
  type CommandResult,
  type CreateSandboxOptions,
  type FileEntry,
  type RunCommandOptions,
  type Sandbox,
  type SandboxFileSystem,
  type SandboxInfo,
  type SandboxNetwork,
  type SandboxProvider,
} from '../ports/sandbox.js';
import {
  OutputCollector,
  backgroundCommand,
  looksTimedOut,
  resolveIn,
  runArgv,
  sortEntries,
  timeoutSeconds,
  toBytes,
} from './sandbox-shell.js';

export interface E2BSandboxOptions {
  apiKey: string;
  /** The template each sandbox starts from. Default `base`. */
  template?: string;
  /** `none` (the default), `all`, or `{ allow: [...] }`: only these hosts. */
  network?: SandboxNetwork;
  /** Default `e2b.app`. */
  domain?: string;
  /** Default `https://api.<domain>`. */
  apiUrl?: string;
  /** Sends every call to one sandbox here instead of
   *  `https://49983-<id>.<domain>`, naming the sandbox in a header. For
   *  tests and proxies. */
  sandboxUrl?: string;
  /** For tests. */
  fetch?: typeof fetch;
}

const ENVD_PORT = 49983;
const WORKDIR = '/home/user';

interface Created {
  sandboxID: string;
  domain?: string | null;
  envdAccessToken?: string | null;
}

/** Hosted sandboxes on E2B: each is a small VM that starts in under a
 *  second. It talks to E2B's HTTP API directly, so it needs no SDK; the Go
 *  runtime has the same adapter (adapters/e2b). A sandbox shuts itself down
 *  at its timeout, which the runtime pushes back while the thread uses it. */
export class E2BSandbox implements SandboxProvider {
  readonly name = 'e2b';
  private readonly domain: string;
  private readonly apiUrl: string;

  constructor(private readonly options: E2BSandboxOptions) {
    if (!options.apiKey) throw new Error('E2BSandbox: apiKey is required');
    this.domain = options.domain ?? 'e2b.app';
    this.apiUrl = options.apiUrl ?? `https://api.${this.domain}`;
  }

  get doFetch(): typeof fetch {
    return this.options.fetch ?? fetch;
  }

  /** A call to E2B's API. */
  async api(method: string, path: string, body?: unknown): Promise<Response> {
    return this.doFetch(`${this.apiUrl}${path}`, {
      method,
      headers: { 'x-api-key': this.options.apiKey, ...(body !== undefined ? { 'content-type': 'application/json' } : {}) },
      ...(body !== undefined ? { body: JSON.stringify(body) } : {}),
    });
  }

  async create(options: CreateSandboxOptions): Promise<Sandbox> {
    const network = options.network ?? this.options.network ?? 'none';
    const metadata: Record<string, string> = { 'agentenkit.threadId': options.metadata.threadId };
    if (options.metadata.runId) metadata['agentenkit.runId'] = options.metadata.runId;
    const res = await this.api('POST', '/v2/sandboxes', {
      templateID: options.template ?? this.options.template ?? 'base',
      timeout: seconds(options.timeoutMs),
      metadata,
      ...(options.envs ? { envVars: options.envs } : {}),
      ...(typeof network === 'object'
        ? { allow_internet_access: true, network: { allowOut: network.allow, denyOut: ['0.0.0.0/0'] } }
        : { allow_internet_access: network === 'all' }),
      ...(options.extra ?? {}),
    });
    if (!res.ok) throw new Error(`e2b: could not start a sandbox: ${res.status} ${(await res.text()).slice(0, 300)}`);
    return this.box((await res.json()) as Created);
  }

  async connect(sandboxId: string): Promise<Sandbox> {
    const res = await this.api('POST', `/v2/sandboxes/${encodeURIComponent(sandboxId)}/connect`, {});
    if (res.status === 404 || res.status === 410) throw new SandboxGoneError(sandboxId);
    if (!res.ok) throw new Error(`e2b: could not reach sandbox ${sandboxId}: ${res.status} ${(await res.text()).slice(0, 300)}`);
    return this.box((await res.json()) as Created);
  }

  private box(c: Created): E2BBox {
    const envdUrl = this.options.sandboxUrl ?? `https://${ENVD_PORT}-${c.sandboxID}.${c.domain || this.domain}`;
    return new E2BBox(this, c.sandboxID, c.domain || this.domain, envdUrl, c.envdAccessToken ?? undefined);
  }
}

class E2BBox implements Sandbox {
  readonly provider = 'e2b';
  readonly filesystem: SandboxFileSystem;

  constructor(
    private readonly e2b: E2BSandbox,
    readonly sandboxId: string,
    private readonly domain: string,
    private readonly envdUrl: string,
    private readonly accessToken?: string,
  ) {
    this.filesystem = new E2BFiles(this);
  }

  headers(extra: Record<string, string> = {}): Record<string, string> {
    return {
      'e2b-sandbox-id': this.sandboxId,
      'e2b-sandbox-port': String(ENVD_PORT),
      ...(this.accessToken ? { 'x-access-token': this.accessToken } : {}),
      ...extra,
    };
  }

  /** A call to the sandbox's own daemon (envd). */
  async envd(path: string, init: RequestInit & { headers?: Record<string, string> } = {}): Promise<Response> {
    const res = await this.e2b.doFetch(`${this.envdUrl}${path}`, { ...init, headers: this.headers(init.headers) });
    // E2B's proxy answers 502 for a sandbox that is not running any more.
    if (res.status === 502) throw new SandboxGoneError(this.sandboxId);
    return res;
  }

  /** A unary Connect call to envd, in JSON. */
  async rpc(method: string, body: unknown): Promise<{ ok: true; data: any } | { ok: false; code: string; message: string }> {
    const res = await this.envd(`/${method}`, {
      method: 'POST',
      headers: { 'content-type': 'application/json', 'connect-protocol-version': '1' },
      body: JSON.stringify(body),
    });
    const data = (await res.json().catch(() => ({}))) as any;
    if (res.ok) return { ok: true, data };
    return { ok: false, code: String(data?.code ?? res.status), message: String(data?.message ?? '') };
  }

  path(p: string) {
    return resolveIn(WORKDIR, p);
  }

  async runCommand(command: string, options: RunCommandOptions = {}): Promise<CommandResult> {
    const limit = options.timeoutMs ?? DEFAULT_COMMAND_TIMEOUT_MS;
    const cmd = options.background ? backgroundCommand(command) : command;
    // `timeout` inside the sandbox stops the command and what it started;
    // the timer here is the backstop, and kills it through envd.
    const [file, ...args] = runArgv(cmd, options.background ? undefined : timeoutSeconds(limit), true);
    const started = Date.now();
    const out = new OutputCollector(options.background ? undefined : options.onStdout);
    const err = new OutputCollector(options.background ? undefined : options.onStderr);
    const request = new AbortController();
    let pid: number | undefined;
    let timedOut = false;
    const stop = async () => {
      if (pid !== undefined) {
        await this.rpc('process.Process/SendSignal', { process: { pid }, signal: 'SIGNAL_SIGKILL' }).catch(() => undefined);
      }
      request.abort();
    };
    const timer = setTimeout(() => {
      timedOut = true;
      void stop();
    }, limit + 1_000);
    const onAbort = () => void stop();
    if (options.signal?.aborted) {
      clearTimeout(timer);
      throw options.signal.reason;
    }
    options.signal?.addEventListener('abort', onAbort, { once: true });
    let exitCode = -1;
    try {
      const res = await this.envd('/process.Process/Start', {
        method: 'POST',
        headers: {
          'content-type': 'application/connect+json',
          'connect-protocol-version': '1',
          'keepalive-ping-interval': '50',
        },
        body: envelope({
          process: { cmd: file, args, cwd: this.path(options.cwd ?? '.'), envs: options.env ?? {} },
          stdin: false,
        }),
        signal: request.signal,
      });
      if (!res.ok) {
        const text = await res.text();
        if (res.status === 404) throw new SandboxGoneError(this.sandboxId);
        throw new Error(`e2b: could not run the command: ${res.status} ${text.slice(0, 300)}`);
      }
      for await (const frame of frames(res.body!)) {
        if (frame.end) {
          const e = frame.message?.error;
          if (e) {
            if (e.code === 'not_found' || e.code === 'unavailable') throw new SandboxGoneError(this.sandboxId, e.message);
            throw new Error(`e2b: ${e.code}: ${e.message}`);
          }
          break;
        }
        const event = frame.message?.event ?? {};
        if (event.start) pid = event.start.pid;
        if (event.data?.stdout) out.push(base64Bytes(event.data.stdout));
        if (event.data?.stderr) err.push(base64Bytes(event.data.stderr));
        if (event.end) exitCode = event.end.exitCode ?? 0;
      }
    } catch (e) {
      if (!request.signal.aborted) throw e;
      if (!timedOut) throw options.signal?.reason ?? e;
    } finally {
      clearTimeout(timer);
      options.signal?.removeEventListener('abort', onAbort);
    }
    const durationMs = Date.now() - started;
    if (options.background) return { stdout: '', stderr: '', exitCode, durationMs };
    timedOut ||= looksTimedOut(exitCode, durationMs, limit);
    return {
      stdout: out.end(),
      stderr: err.end(),
      exitCode: timedOut ? TIMED_OUT_EXIT_CODE : exitCode,
      durationMs,
      ...(timedOut ? { timedOut: true } : {}),
      ...(out.truncated || err.truncated ? { truncated: true } : {}),
    };
  }

  async getUrl(options: { port: number; protocol?: 'http' | 'https' }): Promise<string> {
    return `${options.protocol ?? 'https'}://${options.port}-${this.sandboxId}.${this.domain}`;
  }

  async getInfo(): Promise<SandboxInfo> {
    const res = await this.e2b.api('GET', `/sandboxes/${encodeURIComponent(this.sandboxId)}`);
    if (res.status === 404) throw new SandboxGoneError(this.sandboxId);
    if (!res.ok) throw new Error(`e2b: ${res.status} ${(await res.text()).slice(0, 300)}`);
    const d = (await res.json()) as {
      startedAt: string;
      endAt?: string;
      state?: string;
      metadata?: Record<string, string>;
    };
    const metadata: Record<string, string> = {};
    for (const [k, v] of Object.entries(d.metadata ?? {})) {
      if (k.startsWith('agentenkit.')) metadata[k.slice('agentenkit.'.length)] = v;
    }
    return {
      id: this.sandboxId,
      provider: 'e2b',
      status: !d.state || d.state === 'running' ? 'running' : 'stopped',
      createdAt: new Date(d.startedAt),
      ...(d.endAt ? { expiresAt: new Date(d.endAt) } : {}),
      workdir: WORKDIR,
      metadata,
    };
  }

  async setTimeout(timeoutMs: number): Promise<void> {
    const res = await this.e2b.api('POST', `/sandboxes/${encodeURIComponent(this.sandboxId)}/timeout`, {
      timeout: seconds(timeoutMs),
    });
    if (res.status === 404) throw new SandboxGoneError(this.sandboxId);
    if (!res.ok) throw new Error(`e2b: could not set the sandbox's time: ${res.status} ${(await res.text()).slice(0, 300)}`);
  }

  async destroy(): Promise<void> {
    const res = await this.e2b.api('DELETE', `/sandboxes/${encodeURIComponent(this.sandboxId)}`);
    if (!res.ok && res.status !== 404) throw new Error(`e2b: could not end ${this.sandboxId}: ${res.status}`);
  }
}

class E2BFiles implements SandboxFileSystem {
  constructor(private readonly box: E2BBox) {}

  async readFile(path: string): Promise<string> {
    return new TextDecoder().decode(await this.readFileBytes(path));
  }

  async readFileBytes(path: string): Promise<Uint8Array> {
    const res = await this.box.envd(`/files?${new URLSearchParams({ path: this.box.path(path) })}`);
    if (res.status === 404) throw new SandboxFileNotFoundError(path);
    if (!res.ok) throw new Error(`e2b: could not read ${path}: ${res.status} ${(await res.text()).slice(0, 300)}`);
    return new Uint8Array(await res.arrayBuffer());
  }

  async writeFile(path: string, content: string | Uint8Array): Promise<void> {
    const full = this.box.path(path);
    const form = new FormData();
    // A copy, so its buffer is a plain ArrayBuffer as Blob wants.
    form.append('file', new Blob([toBytes(content).slice()]), full);
    const res = await this.box.envd(`/files?${new URLSearchParams({ path: full })}`, { method: 'POST', body: form });
    if (!res.ok) throw new Error(`e2b: could not write ${path}: ${res.status} ${(await res.text()).slice(0, 300)}`);
  }

  async readdir(path: string): Promise<FileEntry[]> {
    const r = await this.box.rpc('filesystem.Filesystem/ListDir', { path: this.box.path(path), depth: 1 });
    if (!r.ok) {
      if (r.code === 'not_found' || r.code === 'invalid_argument') throw new SandboxFileNotFoundError(path);
      throw new Error(`e2b: could not list ${path}: ${r.code} ${r.message}`);
    }
    const entries: FileEntry[] = [];
    for (const e of (r.data.entries ?? []) as Array<{ name: string; type?: string; size?: string | number }>) {
      entries.push(
        e.type === 'FILE_TYPE_DIRECTORY'
          ? { name: e.name, type: 'directory' }
          : { name: e.name, type: 'file', size: Number(e.size ?? 0) },
      );
    }
    return sortEntries(entries);
  }

  async mkdir(path: string): Promise<void> {
    const r = await this.box.rpc('filesystem.Filesystem/MakeDir', { path: this.box.path(path) });
    if (!r.ok && r.code !== 'already_exists') throw new Error(`e2b: could not make ${path}: ${r.code} ${r.message}`);
  }

  async exists(path: string): Promise<boolean> {
    const r = await this.box.rpc('filesystem.Filesystem/Stat', { path: this.box.path(path) });
    if (r.ok) return true;
    if (r.code === 'not_found') return false;
    throw new Error(`e2b: could not check ${path}: ${r.code} ${r.message}`);
  }

  async remove(path: string): Promise<void> {
    const r = await this.box.rpc('filesystem.Filesystem/Remove', { path: this.box.path(path) });
    if (!r.ok && r.code !== 'not_found') throw new Error(`e2b: could not remove ${path}: ${r.code} ${r.message}`);
  }
}

/** E2B takes whole seconds. */
function seconds(ms: number): number {
  return Math.max(1, Math.ceil(ms / 1000));
}

function base64Bytes(s: string): Uint8Array {
  return Uint8Array.from(Buffer.from(s, 'base64'));
}

/** One Connect streaming message: a flags byte, a 4-byte length, the JSON. */
export function envelope(message: unknown, flags = 0): Uint8Array<ArrayBuffer> {
  const json = new TextEncoder().encode(JSON.stringify(message));
  const out = new Uint8Array(5 + json.length);
  out[0] = flags;
  new DataView(out.buffer).setUint32(1, json.length);
  out.set(json, 5);
  return out;
}

/** The messages of a Connect stream. The last has `end: true` and says
 *  whether the call failed. */
export async function* frames(body: ReadableStream<Uint8Array>): AsyncGenerator<{ end: boolean; message: any }> {
  let buf = new Uint8Array(0);
  const reader = body.getReader();
  try {
    for (;;) {
      while (buf.length >= 5) {
        const size = new DataView(buf.buffer, buf.byteOffset).getUint32(1);
        if (buf.length < 5 + size) break;
        const flags = buf[0]!;
        const payload = buf.subarray(5, 5 + size);
        buf = buf.slice(5 + size);
        const message = payload.length ? JSON.parse(new TextDecoder().decode(payload)) : {};
        yield { end: (flags & 0x02) !== 0, message };
      }
      const { value, done } = await reader.read();
      if (done) return;
      const next = new Uint8Array(buf.length + value.length);
      next.set(buf);
      next.set(value, buf.length);
      buf = next;
    }
  } finally {
    reader.releaseLock();
  }
}
