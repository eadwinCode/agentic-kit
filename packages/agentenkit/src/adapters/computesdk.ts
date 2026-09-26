import {
  DEFAULT_COMMAND_TIMEOUT_MS,
  SandboxFileNotFoundError,
  SandboxGoneError,
  SandboxUnsupportedError,
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
import {
  FILE_SCRIPTS,
  NOT_FOUND_EXIT,
  backgroundCommand,
  capOutput,
  decodeBase64,
  encodeBase64,
  looksTimedOut,
  parseListing,
  resolveIn,
  shellQuote,
  toBytes,
} from './sandbox-shell.js';

/** The part of a ComputeSDK sandbox this adapter uses. Any ComputeSDK
 *  provider's sandbox has it; nothing is imported from ComputeSDK, so it is
 *  not a dependency. */
export interface ComputeSdkSandboxLike {
  readonly sandboxId: string;
  readonly provider: string;
  runCommand(
    command: string,
    options?: { cwd?: string; env?: Record<string, string>; timeout?: number; background?: boolean; onStdout?: (d: string) => void; onStderr?: (d: string) => void },
  ): Promise<{ stdout: string; stderr: string; exitCode: number; durationMs: number }>;
  getInfo(): Promise<{ id: string; provider: string; status: string; createdAt: Date; timeout: number; metadata?: Record<string, any> }>;
  getUrl(options: { port: number; protocol?: string }): Promise<string>;
  destroy(): Promise<void>;
  readonly filesystem: {
    readFile(path: string): Promise<string>;
    writeFile(path: string, content: string): Promise<void>;
  };
}

/** A ComputeSDK provider, as `e2b({ apiKey })` or `daytona({ ... })` from
 *  the `@computesdk/*` packages returns it. */
export interface ComputeSdkProviderLike {
  readonly name?: string;
  readonly sandbox: {
    create(options?: Record<string, unknown>): Promise<ComputeSdkSandboxLike>;
    getById(sandboxId: string): Promise<ComputeSdkSandboxLike | null>;
  };
}

export interface ComputeSdkSandboxOptions {
  /** A ComputeSDK provider: `e2b({ apiKey })`, `daytona(...)`, `modal(...)`… */
  provider: ComputeSdkProviderLike;
  /** The folder relative paths are taken from. Default `/tmp/agentenkit`. */
  workdir?: string;
  /** How long a sandbox lives. ComputeSDK cannot push a sandbox's time back,
   *  so each one ends this long after it was made, used or not. Default 1
   *  hour. */
  timeoutMs?: number;
}

/** Sandboxes from any ComputeSDK provider (Daytona, Modal, Vercel,
 *  Cloudflare, …). TS only: ComputeSDK is a TypeScript library.
 *
 *  Files that are not plain text go through `base64` in the sandbox, since
 *  ComputeSDK's file calls take text only. */
export class ComputeSdkSandbox implements SandboxProvider {
  readonly name: string;

  constructor(private readonly options: ComputeSdkSandboxOptions) {
    this.name = `computesdk:${options.provider.name ?? 'provider'}`;
  }

  private get workdir() {
    return this.options.workdir ?? '/tmp/agentenkit';
  }

  async create(options: CreateSandboxOptions): Promise<Sandbox> {
    const sandbox = await this.options.provider.sandbox.create({
      timeout: this.options.timeoutMs ?? 60 * 60_000,
      metadata: { threadId: options.metadata.threadId, ...(options.metadata.runId ? { runId: options.metadata.runId } : {}) },
      ...(options.envs ? { envs: options.envs } : {}),
      ...(options.template ? { templateId: options.template } : {}),
      ...(options.image ? { image: options.image } : {}),
      ...(options.extra ?? {}),
    });
    const box = new ComputeSdkBox(this.name, sandbox, this.workdir);
    await box.filesystem.mkdir('.');
    return box;
  }

  async connect(sandboxId: string): Promise<Sandbox> {
    // Only "not found" means gone; any other error is thrown as it is, so a
    // passing network fault does not throw away a live sandbox.
    const sandbox = await this.options.provider.sandbox.getById(sandboxId);
    if (!sandbox) throw new SandboxGoneError(sandboxId);
    return new ComputeSdkBox(this.name, sandbox, this.workdir);
  }
}

class ComputeSdkBox implements Sandbox {
  readonly filesystem: SandboxFileSystem;

  constructor(
    readonly provider: string,
    private readonly inner: ComputeSdkSandboxLike,
    private readonly workdir: string,
  ) {
    this.filesystem = new ComputeSdkFiles(this, inner);
  }

  get sandboxId() {
    return this.inner.sandboxId;
  }

  path(p: string) {
    return resolveIn(this.workdir, p);
  }

  /** Runs a FILE_SCRIPTS script on one path. */
  script(script: string, path: string) {
    return this.inner.runCommand(`sh -c ${shellQuote(script)} sandbox ${shellQuote(this.path(path))}`);
  }

  async runCommand(command: string, options: RunCommandOptions = {}): Promise<CommandResult> {
    if (options.signal?.aborted) throw options.signal.reason;
    const limit = options.timeoutMs ?? DEFAULT_COMMAND_TIMEOUT_MS;
    const r = await this.inner.runCommand(options.background ? backgroundCommand(command) : command, {
      cwd: this.path(options.cwd ?? '.'),
      ...(options.env ? { env: options.env } : {}),
      timeout: limit,
      ...(options.onStdout ? { onStdout: options.onStdout } : {}),
      ...(options.onStderr ? { onStderr: options.onStderr } : {}),
    });
    if (options.background) return { stdout: '', stderr: '', exitCode: r.exitCode, durationMs: r.durationMs };
    const timedOut = looksTimedOut(r.exitCode, r.durationMs, limit);
    const stdout = capOutput(r.stdout);
    const stderr = capOutput(r.stderr);
    return {
      stdout: stdout.text,
      stderr: stderr.text,
      exitCode: timedOut ? TIMED_OUT_EXIT_CODE : r.exitCode,
      durationMs: r.durationMs,
      ...(timedOut ? { timedOut: true } : {}),
      ...(stdout.truncated || stderr.truncated ? { truncated: true } : {}),
    };
  }

  getUrl(options: { port: number; protocol?: 'http' | 'https' }): Promise<string> {
    return this.inner.getUrl(options);
  }

  async getInfo(): Promise<SandboxInfo> {
    const info = await this.inner.getInfo();
    const metadata: Record<string, string> = {};
    for (const [k, v] of Object.entries(info.metadata ?? {})) if (typeof v === 'string') metadata[k] = v;
    const createdAt = new Date(info.createdAt);
    return {
      id: info.id,
      provider: this.provider,
      status: info.status === 'running' ? 'running' : info.status === 'stopped' ? 'stopped' : 'error',
      createdAt,
      ...(info.timeout ? { expiresAt: new Date(createdAt.getTime() + info.timeout) } : {}),
      workdir: this.workdir,
      metadata,
    };
  }

  async setTimeout(): Promise<void> {
    throw new SandboxUnsupportedError(this.provider, "changing a sandbox's time");
  }

  destroy(): Promise<void> {
    return this.inner.destroy();
  }
}

class ComputeSdkFiles implements SandboxFileSystem {
  constructor(
    private readonly box: ComputeSdkBox,
    private readonly inner: ComputeSdkSandboxLike,
  ) {}

  private async run(script: string, path: string) {
    const r = await this.box.script(script, path);
    if (r.exitCode === NOT_FOUND_EXIT) throw new SandboxFileNotFoundError(path);
    if (r.exitCode !== 0) throw new Error(`${this.box.provider}: ${r.stderr.trim() || `exit ${r.exitCode}`}`);
    return r;
  }

  async readFile(path: string): Promise<string> {
    return new TextDecoder().decode(await this.readFileBytes(path));
  }

  async readFileBytes(path: string): Promise<Uint8Array> {
    return decodeBase64((await this.run(FILE_SCRIPTS.readBase64, path)).stdout);
  }

  async writeFile(path: string, content: string | Uint8Array): Promise<void> {
    const full = this.box.path(path);
    await this.run(FILE_SCRIPTS.mkdir, full.slice(0, full.lastIndexOf('/')) || '/');
    if (typeof content === 'string') return this.inner.filesystem.writeFile(full, content);
    // Bytes go through base64, in pieces well under the longest argument a
    // command line takes.
    const b64 = encodeBase64(toBytes(content));
    await this.box.script(': > "$1"', path);
    for (let i = 0; i < b64.length; i += 60_000) {
      const piece = b64.slice(i, i + 60_000);
      const r = await this.box.script(`printf %s ${shellQuote(piece)} | base64 -d >> "$1"`, path);
      if (r.exitCode !== 0) throw new Error(`${this.box.provider}: could not write ${path}: ${r.stderr.trim()}`);
    }
  }

  async readdir(path: string): Promise<FileEntry[]> {
    return parseListing((await this.run(FILE_SCRIPTS.list, path)).stdout);
  }

  async mkdir(path: string): Promise<void> {
    await this.run(FILE_SCRIPTS.mkdir, path);
  }

  async exists(path: string): Promise<boolean> {
    return (await this.box.script(FILE_SCRIPTS.exists, path)).exitCode === 0;
  }

  async remove(path: string): Promise<void> {
    await this.run(FILE_SCRIPTS.remove, path);
  }
}
