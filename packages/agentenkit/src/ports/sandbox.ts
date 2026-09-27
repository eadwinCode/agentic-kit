import type { AgentRunState } from '../core/state.js';

/** The sandbox port: a place apart from the app where an agent's commands run
 *  and its files live. A container, a hosted machine, or a folder on this
 *  machine in development.
 *
 *  The shape and names follow ComputeSDK's Sandbox, so an adapter over it is
 *  thin, with three additions: `readFileBytes` (charts and images need it),
 *  `setTimeout` (how an idle sandbox ends by itself) and `SandboxInfo.workdir`.
 *  The Go runtime has the same port (ports/sandbox.go). */

/** How long a command may run when the caller does not say. */
export const DEFAULT_COMMAND_TIMEOUT_MS = 120_000;

/** The most output kept from one command, per stream. Past it the rest is
 *  dropped and the result says `truncated: true`, so a command that prints
 *  without end cannot fill the app's memory. */
export const MAX_COMMAND_OUTPUT_BYTES = 4 * 1024 * 1024;

/** The exit code of a command stopped at its time limit, as `timeout(1)`
 *  gives it. */
export const TIMED_OUT_EXIT_CODE = 124;

/** What the sandbox may reach on the network. `none` is the default. */
export type SandboxNetwork = 'none' | 'all' | { allow: string[] };

export interface CreateSandboxOptions {
  /** How long the sandbox lives unless `setTimeout` pushes it back. It then
   *  shuts itself down, even when the app that made it is gone. */
  timeoutMs: number;
  /** What the sandbox is for. Adapters keep it as labels or metadata where
   *  the provider has them, and a multi-tenant adapter can read `state` to
   *  pick a template or key per tenant (§2.10). */
  metadata: { threadId: string; runId?: string; state?: AgentRunState };
  /** Environment variables every command sees. */
  envs?: Record<string, string>;
  /** A provider template (E2B) or image (Docker). Adapters have their own
   *  default. */
  template?: string;
  image?: string;
  network?: SandboxNetwork;
  resources?: { cpu?: number; memoryMiB?: number; diskMiB?: number };
  /** Provider-only settings, passed through as they are. */
  extra?: Record<string, unknown>;
}

/** Makes sandboxes and finds them again. One per app, passed to
 *  `setupAgentCore({ tools: { sandbox } })`. */
export interface SandboxProvider {
  /** The adapter's name: 'docker', 'e2b', 'local'. */
  readonly name: string;
  create(options: CreateSandboxOptions): Promise<Sandbox>;
  /** Finds a sandbox made earlier, by this process or another. Throws
   *  SandboxGoneError when it no longer exists. */
  connect(sandboxId: string): Promise<Sandbox>;
}

export interface RunCommandOptions {
  /** Where the command runs. A relative path is taken from the sandbox's
   *  work folder, which is also the default. */
  cwd?: string;
  env?: Record<string, string>;
  /** Stops the command after this long: the result then has `timedOut: true`
   *  and exit code 124, with the output it printed so far. Default 120 s. */
  timeoutMs?: number;
  /** Starts the command and returns at once, leaving it running (a dev
   *  server, say). Its output is not kept. */
  background?: boolean;
  /** Output as it comes, decoded as UTF-8. */
  onStdout?: (chunk: string) => void;
  onStderr?: (chunk: string) => void;
  /** Stops the command and rejects with the signal's reason. */
  signal?: AbortSignal;
}

export interface CommandResult {
  stdout: string;
  stderr: string;
  exitCode: number;
  durationMs: number;
  /** True when the command was stopped at its time limit. */
  timedOut?: boolean;
  /** True when output past MAX_COMMAND_OUTPUT_BYTES was dropped. */
  truncated?: boolean;
}

export interface FileEntry {
  name: string;
  type: 'file' | 'directory';
  size?: number;
}

/** The sandbox's files. A relative path is taken from the work folder. */
export interface SandboxFileSystem {
  /** Throws SandboxFileNotFoundError when there is no such file. */
  readFile(path: string): Promise<string>;
  /** Not in ComputeSDK: charts and images are bytes. */
  readFileBytes(path: string): Promise<Uint8Array>;
  /** Makes the parent folders it needs, and replaces a file already there. */
  writeFile(path: string, content: string | Uint8Array): Promise<void>;
  /** The folder's entries, sorted by name. Throws SandboxFileNotFoundError
   *  when there is no such folder. */
  readdir(path: string): Promise<FileEntry[]>;
  /** Makes the folder and its parents; fine when it is there already. */
  mkdir(path: string): Promise<void>;
  exists(path: string): Promise<boolean>;
  /** Removes a file, or a folder with everything in it; fine when it is not
   *  there. */
  remove(path: string): Promise<void>;
}

export interface SandboxInfo {
  id: string;
  provider: string;
  status: 'running' | 'stopped' | 'error';
  createdAt: Date;
  /** When it will shut itself down, when the provider says. */
  expiresAt?: Date;
  /** The folder commands start in and relative paths are taken from. */
  workdir: string;
  metadata?: Record<string, string>;
}

export interface Sandbox {
  readonly sandboxId: string;
  readonly provider: string;
  readonly filesystem: SandboxFileSystem;
  runCommand(command: string, options?: RunCommandOptions): Promise<CommandResult>;
  /** A URL for a port the sandbox serves on. Throws SandboxUnsupportedError
   *  where the adapter cannot give one. */
  getUrl(options: { port: number; protocol?: 'http' | 'https' }): Promise<string>;
  getInfo(): Promise<SandboxInfo>;
  /** Not in ComputeSDK: the sandbox now shuts itself down `timeoutMs` from
   *  now. The runtime calls it as a thread uses its sandbox, so one left idle
   *  ends by itself. Throws SandboxUnsupportedError where the provider
   *  cannot change it. */
  setTimeout(timeoutMs: number): Promise<void>;
  /** Ends the sandbox and its files. Fine when it is already gone. */
  destroy(): Promise<void>;
}

/** The sandbox no longer exists: it timed out, was destroyed, or its
 *  provider lost it. */
export class SandboxGoneError extends Error {
  override readonly name = 'SandboxGoneError';
  constructor(readonly sandboxId: string, why?: string) {
    super(`sandbox ${sandboxId} is gone${why ? `: ${why}` : ''}`);
  }
}

/** There is no file or folder at this path. */
export class SandboxFileNotFoundError extends Error {
  override readonly name = 'SandboxFileNotFoundError';
  constructor(readonly path: string) {
    super(`sandbox: no such file or folder: ${path}`);
  }
}

/** The adapter cannot do this. */
export class SandboxUnsupportedError extends Error {
  override readonly name = 'SandboxUnsupportedError';
  constructor(provider: string, what: string) {
    super(`${provider}: ${what} is not supported`);
  }
}
