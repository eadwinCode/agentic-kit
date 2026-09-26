import { spawn } from 'node:child_process';
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
  type SandboxNetwork,
  type SandboxProvider,
} from '../ports/sandbox.js';
import {
  FILE_SCRIPTS,
  NOT_FOUND_EXIT,
  OutputCollector,
  backgroundCommand,
  looksTimedOut,
  parseListing,
  randomId,
  resolveIn,
  runArgv,
  timeoutSeconds,
  toBytes,
} from './sandbox-shell.js';

export interface DockerSandboxOptions {
  /** The image each sandbox runs. It needs `sh`; bash, Python and
   *  `timeout` are used when there. Default `python:3.12-slim`. */
  image?: string;
  /** `none` (the default) or `all`. Docker has no allow list by domain. */
  network?: 'none' | 'all';
  /** The folder commands start in. Default `/workspace`. */
  workdir?: string;
  cpu?: number;
  memoryMiB?: number;
  /** Ports to publish on 127.0.0.1, so `getUrl` can give them. Needs
   *  `network: 'all'`. */
  ports?: number[];
  /** Environment variables every command sees. */
  env?: Record<string, string>;
  /** More `docker run` arguments, placed before the image. */
  runArgs?: string[];
  /** The docker CLI. Default `docker` on the PATH. */
  docker?: string;
}

const DEADLINE_FILE = '/tmp/.agentenkit-deadline';

/** The container's main process: it waits until the deadline in
 *  DEADLINE_FILE has passed and then exits, and `--rm` removes the
 *  container. So a sandbox nobody uses ends by itself, even when the app
 *  that made it is gone. `setTimeout` moves the deadline. */
const WATCHDOG =
  `echo "$1" > ${DEADLINE_FILE}; ` +
  `while [ "$(date +%s)" -lt "$(cat ${DEADLINE_FILE} 2>/dev/null || echo 0)" ]; do sleep 2; done`;

/** What the docker CLI says when the container is not there any more. */
const GONE = /Error response from daemon: (No such container|container \S+ is not running)|No such object/;

/** Sandboxes as Docker containers on the machine the docker CLI talks to:
 *  for self-hosting. One container per thread, removed when it has been idle
 *  past its time. Uses the docker CLI, so it needs no SDK and follows your
 *  DOCKER_HOST and contexts. The Go runtime has the same adapter
 *  (adapters/docker). */
export class DockerSandbox implements SandboxProvider {
  readonly name = 'docker';
  private readonly docker: string;

  constructor(private readonly options: DockerSandboxOptions = {}) {
    this.docker = options.docker ?? 'docker';
  }

  async create(options: CreateSandboxOptions): Promise<Sandbox> {
    const network: SandboxNetwork = options.network ?? this.options.network ?? 'none';
    if (typeof network === 'object') throw new SandboxUnsupportedError('docker', 'a network allow list');
    const name = `agentenkit-${randomId()}`;
    const workdir = this.options.workdir ?? '/workspace';
    const deadline = Math.floor((Date.now() + options.timeoutMs) / 1000);
    const args = [
      'run', '-d', '--rm', '--init', '--name', name,
      '--label', 'agentenkit=sandbox',
      '--label', `agentenkit.threadId=${options.metadata.threadId}`,
      ...(options.metadata.runId ? ['--label', `agentenkit.runId=${options.metadata.runId}`] : []),
      '-w', workdir,
      '--network', network === 'all' ? 'bridge' : 'none',
      '--security-opt', 'no-new-privileges',
    ];
    const cpu = options.resources?.cpu ?? this.options.cpu;
    const memory = options.resources?.memoryMiB ?? this.options.memoryMiB;
    if (cpu) args.push('--cpus', String(cpu));
    if (memory) args.push('--memory', `${memory}m`);
    for (const port of this.options.ports ?? []) args.push('-p', `127.0.0.1::${port}`);
    for (const [k, v] of Object.entries({ ...this.options.env, ...options.envs })) args.push('-e', `${k}=${v}`);
    args.push(...(this.options.runArgs ?? []));
    args.push(options.image ?? this.options.image ?? 'python:3.12-slim', 'sh', '-c', WATCHDOG, 'sandbox', String(deadline));
    const r = await cli(this.docker, args);
    if (r.code !== 0) throw new Error(`docker: could not start a sandbox: ${text(r.stderr).trim()}`);
    return new DockerBox(this.docker, name, workdir);
  }

  async connect(sandboxId: string): Promise<Sandbox> {
    const r = await cli(this.docker, ['inspect', '-f', '{{.State.Running}}\t{{.Config.WorkingDir}}', sandboxId]);
    const [running, workdir] = text(r.stdout).trim().split('\t');
    if (r.code !== 0 || running !== 'true') throw new SandboxGoneError(sandboxId);
    return new DockerBox(this.docker, sandboxId, workdir || '/');
  }
}

class DockerBox implements Sandbox {
  readonly provider = 'docker';
  readonly filesystem: SandboxFileSystem;

  constructor(
    private readonly docker: string,
    readonly sandboxId: string,
    private readonly workdir: string,
  ) {
    this.filesystem = new DockerFiles(this);
  }

  /** `docker exec` a script with `sh -c`; a container that is gone throws. */
  async script(script: string, args: string[], stdin?: Uint8Array): Promise<Exec> {
    const r = await cli(this.docker, ['exec', ...(stdin ? ['-i'] : []), this.sandboxId, 'sh', '-c', script, 'sandbox', ...args], { stdin });
    this.checkGone(r);
    return r;
  }

  checkGone(r: Exec) {
    if (r.code !== 0 && GONE.test(text(r.stderr))) throw new SandboxGoneError(this.sandboxId);
  }

  path(p: string) {
    return resolveIn(this.workdir, p);
  }

  async runCommand(command: string, options: RunCommandOptions = {}): Promise<CommandResult> {
    const limit = options.timeoutMs ?? DEFAULT_COMMAND_TIMEOUT_MS;
    const args = ['exec', '-w', this.path(options.cwd ?? '.')];
    for (const [k, v] of Object.entries(options.env ?? {})) args.push('-e', `${k}=${v}`);
    const cmd = options.background ? backgroundCommand(command) : command;
    args.push(this.sandboxId, ...runArgv(cmd, options.background ? undefined : timeoutSeconds(limit)));
    const started = Date.now();
    const out = new OutputCollector(options.background ? undefined : options.onStdout);
    const err = new OutputCollector(options.background ? undefined : options.onStderr);
    // `timeout` inside the container stops the command; this is the backstop
    // for an image without it.
    const r = await cli(this.docker, args, { out, err, limitMs: limit + 5_000, signal: options.signal });
    const stdout = out.end();
    const stderr = err.end();
    if (r.code !== 0 && GONE.test(stderr)) throw new SandboxGoneError(this.sandboxId);
    const durationMs = Date.now() - started;
    if (options.background) return { stdout: '', stderr: '', exitCode: r.code, durationMs };
    const timedOut = r.killed || looksTimedOut(r.code, durationMs, limit);
    return {
      stdout,
      stderr,
      exitCode: timedOut ? TIMED_OUT_EXIT_CODE : r.code,
      durationMs,
      ...(timedOut ? { timedOut: true } : {}),
      ...(out.truncated || err.truncated ? { truncated: true } : {}),
    };
  }

  async getUrl(options: { port: number; protocol?: 'http' | 'https' }): Promise<string> {
    const r = await cli(this.docker, ['port', this.sandboxId, `${options.port}/tcp`]);
    this.checkGone(r);
    const hostPort = text(r.stdout).trim().split('\n')[0];
    if (r.code !== 0 || !hostPort) {
      throw new SandboxUnsupportedError('docker', `a URL for port ${options.port} (publish it with the ports option)`);
    }
    return `${options.protocol ?? 'http'}://${hostPort.replace('0.0.0.0', '127.0.0.1')}`;
  }

  async getInfo(): Promise<SandboxInfo> {
    const r = await cli(this.docker, [
      'inspect', '-f', '{{.Created}}\t{{.State.Status}}\t{{json .Config.Labels}}', this.sandboxId,
    ]);
    if (r.code !== 0) throw new SandboxGoneError(this.sandboxId);
    const [created, status, labels] = text(r.stdout).trim().split('\t');
    const metadata: Record<string, string> = {};
    for (const [k, v] of Object.entries((JSON.parse(labels || '{}') ?? {}) as Record<string, string>)) {
      if (k.startsWith('agentenkit.')) metadata[k.slice('agentenkit.'.length)] = v;
    }
    const deadline = Number(text((await this.script(`cat ${DEADLINE_FILE}`, [])).stdout).trim());
    return {
      id: this.sandboxId,
      provider: 'docker',
      status: status === 'running' ? 'running' : status === 'exited' ? 'stopped' : 'error',
      createdAt: new Date(created!),
      ...(deadline ? { expiresAt: new Date(deadline * 1000) } : {}),
      workdir: this.workdir,
      metadata,
    };
  }

  async setTimeout(timeoutMs: number): Promise<void> {
    const deadline = Math.floor((Date.now() + timeoutMs) / 1000);
    const r = await this.script(`echo "$1" > ${DEADLINE_FILE}`, [String(deadline)]);
    if (r.code !== 0) throw new Error(`docker: could not set the sandbox's time: ${text(r.stderr).trim()}`);
  }

  async destroy(): Promise<void> {
    const r = await cli(this.docker, ['rm', '-f', this.sandboxId]);
    if (r.code !== 0 && !GONE.test(text(r.stderr))) {
      throw new Error(`docker: could not remove ${this.sandboxId}: ${text(r.stderr).trim()}`);
    }
  }
}

class DockerFiles implements SandboxFileSystem {
  constructor(private readonly box: DockerBox) {}

  private async run(script: string, path: string, stdin?: Uint8Array): Promise<Exec> {
    const r = await this.box.script(script, [this.box.path(path)], stdin);
    if (r.code === NOT_FOUND_EXIT) throw new SandboxFileNotFoundError(path);
    if (r.code !== 0) throw new Error(`docker: ${text(r.stderr).trim() || `exit ${r.code}`}`);
    return r;
  }

  async readFile(path: string): Promise<string> {
    return text((await this.run(FILE_SCRIPTS.read, path)).stdout);
  }

  async readFileBytes(path: string): Promise<Uint8Array> {
    return (await this.run(FILE_SCRIPTS.read, path)).stdout;
  }

  async writeFile(path: string, content: string | Uint8Array): Promise<void> {
    await this.run(FILE_SCRIPTS.write, path, toBytes(content));
  }

  async readdir(path: string): Promise<FileEntry[]> {
    return parseListing(text((await this.run(FILE_SCRIPTS.list, path)).stdout));
  }

  async mkdir(path: string): Promise<void> {
    await this.run(FILE_SCRIPTS.mkdir, path);
  }

  async exists(path: string): Promise<boolean> {
    return (await this.box.script(FILE_SCRIPTS.exists, [this.box.path(path)])).code === 0;
  }

  async remove(path: string): Promise<void> {
    await this.run(FILE_SCRIPTS.remove, path);
  }
}

interface Exec {
  code: number;
  stdout: Uint8Array;
  stderr: Uint8Array;
  /** Stopped by `limitMs`. */
  killed?: boolean;
}

const text = (b: Uint8Array) => new TextDecoder().decode(b);

/** Runs the docker CLI. With `out` / `err` the output goes there as it
 *  comes; otherwise it is collected whole. */
function cli(
  docker: string,
  args: string[],
  opts: { stdin?: Uint8Array; out?: OutputCollector; err?: OutputCollector; limitMs?: number; signal?: AbortSignal } = {},
): Promise<Exec> {
  return new Promise((resolve, reject) => {
    const child = spawn(docker, args, { stdio: [opts.stdin ? 'pipe' : 'ignore', 'pipe', 'pipe'] });
    const stdout: Buffer[] = [];
    const stderr: Buffer[] = [];
    child.stdout!.on('data', (b: Buffer) => (opts.out ? opts.out.push(b) : stdout.push(b)));
    child.stderr!.on('data', (b: Buffer) => (opts.err ? opts.err.push(b) : stderr.push(b)));
    let killed = false;
    let aborted = false;
    const timer = opts.limitMs
      ? setTimeout(() => {
          killed = true;
          child.kill('SIGKILL');
        }, opts.limitMs)
      : undefined;
    const onAbort = () => {
      aborted = true;
      child.kill('SIGKILL');
    };
    opts.signal?.addEventListener('abort', onAbort, { once: true });
    if (opts.signal?.aborted) onAbort();
    const done = () => {
      if (timer) clearTimeout(timer);
      opts.signal?.removeEventListener('abort', onAbort);
    };
    child.on('error', (e) => {
      done();
      reject(new Error(`docker: could not run ${docker}: ${e.message}`));
    });
    child.on('close', (code) => {
      done();
      if (aborted) return reject(opts.signal!.reason ?? new Error('aborted'));
      resolve({ code: code ?? -1, stdout: new Uint8Array(Buffer.concat(stdout)), stderr: new Uint8Array(Buffer.concat(stderr)), killed });
    });
    if (opts.stdin) child.stdin!.end(Buffer.from(opts.stdin));
  });
}
