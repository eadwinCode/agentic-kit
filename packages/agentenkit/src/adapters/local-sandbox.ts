import { spawn } from 'node:child_process';
import { promises as fs } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
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
  type SandboxProvider,
} from '../ports/sandbox.js';
import { OutputCollector, backgroundCommand, randomId, resolveIn, runArgv, sortEntries, toBytes } from './sandbox-shell.js';

export interface LocalSandboxOptions {
  /** Where the sandboxes' folders go. Default: `agentenkit-sandboxes` in the
   *  system temp folder. */
  rootDir?: string;
  /** Environment variables every command sees, on top of PATH, HOME, LANG. */
  env?: Record<string, string>;
  /** Where the production warning goes. Defaults to `console`. */
  log?: { warn(message: string, ...rest: unknown[]): void };
}

interface Meta {
  createdAt: number;
  expiresAt: number;
  metadata: Record<string, string>;
  envs: Record<string, string>;
}

const ID = /^local-[a-z0-9]+$/;

/** A sandbox that is only a folder on this machine: for development, where
 *  you want the sandbox tools without Docker or an account. Commands run as
 *  you, with your files in reach. There is NO isolation, and the network
 *  setting is not applied, so it warns when NODE_ENV is production.
 *
 *  Commands see only PATH, HOME (the sandbox folder) and LANG from your
 *  environment, so your API keys do not leak into what the model runs. The
 *  Go runtime has the same adapter (adapters/localsandbox). */
export class LocalSandbox implements SandboxProvider {
  readonly name = 'local';
  private readonly root: string;

  constructor(private readonly options: LocalSandboxOptions = {}) {
    this.root = options.rootDir ?? join(tmpdir(), 'agentenkit-sandboxes');
    if (process.env.NODE_ENV === 'production') {
      (options.log ?? console).warn(
        'LocalSandbox runs commands on this machine with no isolation: use DockerSandbox or E2BSandbox in production',
      );
    }
  }

  async create(options: CreateSandboxOptions): Promise<Sandbox> {
    await this.sweep();
    const id = `local-${randomId()}`;
    const dir = join(this.root, id);
    await fs.mkdir(join(dir, 'work'), { recursive: true });
    const now = Date.now();
    const metadata: Record<string, string> = { threadId: options.metadata.threadId };
    if (options.metadata.runId) metadata.runId = options.metadata.runId;
    await writeMeta(dir, { createdAt: now, expiresAt: now + options.timeoutMs, metadata, envs: options.envs ?? {} });
    return new LocalBox(id, dir, this.options.env ?? {});
  }

  async connect(sandboxId: string): Promise<Sandbox> {
    if (!ID.test(sandboxId)) throw new SandboxGoneError(sandboxId, 'not a local sandbox id');
    const dir = join(this.root, sandboxId);
    const meta = await readMeta(dir);
    if (!meta) throw new SandboxGoneError(sandboxId);
    if (meta.expiresAt <= Date.now()) {
      await fs.rm(dir, { recursive: true, force: true });
      throw new SandboxGoneError(sandboxId, 'it timed out');
    }
    return new LocalBox(sandboxId, dir, this.options.env ?? {});
  }

  /** Removes sandboxes past their time. There is no process watching them,
   *  so each create tidies up after the others. */
  private async sweep() {
    const names = await fs.readdir(this.root).catch(() => [] as string[]);
    const now = Date.now();
    for (const name of names) {
      if (!ID.test(name)) continue;
      const meta = await readMeta(join(this.root, name));
      if (meta && meta.expiresAt <= now) await fs.rm(join(this.root, name), { recursive: true, force: true });
    }
  }
}

async function readMeta(dir: string): Promise<Meta | null> {
  try {
    return JSON.parse(await fs.readFile(join(dir, 'sandbox.json'), 'utf8')) as Meta;
  } catch {
    return null;
  }
}

async function writeMeta(dir: string, meta: Meta) {
  await fs.writeFile(join(dir, 'sandbox.json'), JSON.stringify(meta));
}

class LocalBox implements Sandbox {
  readonly provider = 'local';
  readonly filesystem: SandboxFileSystem;
  private readonly workdir: string;

  constructor(
    readonly sandboxId: string,
    private readonly dir: string,
    private readonly env: Record<string, string>,
  ) {
    this.workdir = join(dir, 'work');
    this.filesystem = new LocalFiles(this.workdir);
  }

  private async meta(): Promise<Meta> {
    const meta = await readMeta(this.dir);
    if (!meta) throw new SandboxGoneError(this.sandboxId);
    return meta;
  }

  async runCommand(command: string, options: RunCommandOptions = {}): Promise<CommandResult> {
    const meta = await this.meta();
    const cwd = resolveIn(this.workdir, options.cwd ?? '.');
    const env: Record<string, string> = {
      PATH: process.env.PATH ?? '/usr/local/bin:/usr/bin:/bin',
      HOME: this.workdir,
      LANG: process.env.LANG ?? 'C.UTF-8',
      ...this.env,
      ...meta.envs,
      ...options.env,
    };
    if (options.background) {
      const r = await this.exec(backgroundCommand(command), cwd, env, DEFAULT_COMMAND_TIMEOUT_MS, {});
      return { ...r, stdout: '', stderr: '' };
    }
    return this.exec(command, cwd, env, options.timeoutMs ?? DEFAULT_COMMAND_TIMEOUT_MS, options);
  }

  private exec(
    command: string,
    cwd: string,
    env: Record<string, string>,
    limitMs: number,
    options: RunCommandOptions,
  ): Promise<CommandResult> {
    const started = Date.now();
    const [file, ...args] = runArgv(command);
    return new Promise((resolve, reject) => {
      // Its own process group, so the time limit stops what it started too.
      // Cast: some apps' types (Next.js) make NODE_ENV a required field.
      const child = spawn(file!, args, { cwd, env: env as NodeJS.ProcessEnv, detached: true, stdio: ['ignore', 'pipe', 'pipe'] });
      const out = new OutputCollector(options.onStdout);
      const err = new OutputCollector(options.onStderr);
      child.stdout!.on('data', (b: Buffer) => out.push(b));
      child.stderr!.on('data', (b: Buffer) => err.push(b));
      let timedOut = false;
      let aborted = false;
      const kill = () => {
        try {
          process.kill(-child.pid!, 'SIGKILL');
        } catch {
          child.kill('SIGKILL');
        }
      };
      const timer = setTimeout(() => {
        timedOut = true;
        kill();
      }, limitMs);
      const onAbort = () => {
        aborted = true;
        kill();
      };
      options.signal?.addEventListener('abort', onAbort, { once: true });
      if (options.signal?.aborted) onAbort();
      child.on('error', (e) => {
        clearTimeout(timer);
        options.signal?.removeEventListener('abort', onAbort);
        reject(e);
      });
      child.on('close', (code, signal) => {
        clearTimeout(timer);
        options.signal?.removeEventListener('abort', onAbort);
        if (aborted) return reject(options.signal!.reason ?? new Error('aborted'));
        const exitCode = timedOut ? TIMED_OUT_EXIT_CODE : (code ?? 128 + signalNumber(signal));
        resolve({
          stdout: out.end(),
          stderr: err.end(),
          exitCode,
          durationMs: Date.now() - started,
          ...(timedOut ? { timedOut: true } : {}),
          ...(out.truncated || err.truncated ? { truncated: true } : {}),
        });
      });
    });
  }

  async getUrl(options: { port: number; protocol?: 'http' | 'https' }): Promise<string> {
    await this.meta();
    return `${options.protocol ?? 'http'}://localhost:${options.port}`;
  }

  async getInfo(): Promise<SandboxInfo> {
    const meta = await this.meta();
    return {
      id: this.sandboxId,
      provider: 'local',
      status: 'running',
      createdAt: new Date(meta.createdAt),
      expiresAt: new Date(meta.expiresAt),
      workdir: this.workdir,
      metadata: meta.metadata,
    };
  }

  async setTimeout(timeoutMs: number): Promise<void> {
    const meta = await this.meta();
    await writeMeta(this.dir, { ...meta, expiresAt: Date.now() + timeoutMs });
  }

  async destroy(): Promise<void> {
    await fs.rm(this.dir, { recursive: true, force: true });
  }
}

function signalNumber(signal: NodeJS.Signals | null): number {
  const numbers: Partial<Record<NodeJS.Signals, number>> = { SIGHUP: 1, SIGINT: 2, SIGKILL: 9, SIGTERM: 15 };
  return (signal && numbers[signal]) || 0;
}

const isMissing = (e: unknown) => (e as { code?: string })?.code === 'ENOENT' || (e as { code?: string })?.code === 'ENOTDIR';

class LocalFiles implements SandboxFileSystem {
  constructor(private readonly workdir: string) {}

  private path(p: string) {
    return resolveIn(this.workdir, p);
  }

  async readFile(path: string): Promise<string> {
    return new TextDecoder().decode(await this.readFileBytes(path));
  }

  async readFileBytes(path: string): Promise<Uint8Array> {
    const full = this.path(path);
    try {
      if (!(await fs.stat(full)).isFile()) throw new SandboxFileNotFoundError(path);
      return new Uint8Array(await fs.readFile(full));
    } catch (e) {
      if (isMissing(e)) throw new SandboxFileNotFoundError(path);
      throw e;
    }
  }

  async writeFile(path: string, content: string | Uint8Array): Promise<void> {
    const full = this.path(path);
    await fs.mkdir(dirname(full), { recursive: true });
    await fs.writeFile(full, toBytes(content));
  }

  async readdir(path: string): Promise<FileEntry[]> {
    const full = this.path(path);
    try {
      const dirents = await fs.readdir(full, { withFileTypes: true });
      const entries: FileEntry[] = [];
      for (const d of dirents) {
        if (d.isDirectory()) entries.push({ name: d.name, type: 'directory' });
        else {
          const size = await fs.stat(join(full, d.name)).then((s) => s.size).catch(() => 0);
          entries.push({ name: d.name, type: 'file', size });
        }
      }
      return sortEntries(entries);
    } catch (e) {
      if (isMissing(e)) throw new SandboxFileNotFoundError(path);
      throw e;
    }
  }

  async mkdir(path: string): Promise<void> {
    await fs.mkdir(this.path(path), { recursive: true });
  }

  async exists(path: string): Promise<boolean> {
    return fs.lstat(this.path(path)).then(() => true, () => false);
  }

  async remove(path: string): Promise<void> {
    await fs.rm(this.path(path), { recursive: true, force: true });
  }
}
