import type { RuntimePorts } from '../../ports/runtime.js';
import { SandboxGoneError, SandboxUnsupportedError, type Sandbox, type SandboxProvider } from '../../ports/sandbox.js';
import { toolRunOf, type ToolRun } from './run.js';

/** One sandbox per thread, kept between messages (`sandboxScope: 'thread'`,
 *  the default), or one per run (`'run'`).
 *
 *  The first sandbox call in a thread makes it and keeps its id in the kv
 *  under the thread. Every later call, in this run or the next (a new
 *  message, a resume after an approval, a retry, another worker), connects to
 *  the same one, so files stay. The run lock means one run at a time uses it;
 *  a run's subagents share it, as they share the thread.
 *
 *  It ends when the thread is deleted, when it has sat idle past
 *  `sandboxIdleTtlMs`, or at `sandboxMaxLifetimeMs` after it was made. Each
 *  use pushes the sandbox's own timeout back, so an idle one shuts itself
 *  down even when no app is running to end it. The next call then makes a
 *  fresh one and says the old files are lost. The Go runtime does the same
 *  (core/sandbox.go).
 *
 *  With `sandboxScope: 'run'` the record also names the run that made the
 *  sandbox. The run's later calls and its subagents share it; when the run
 *  ends (finished, failed, stopped) the runtime ends it (releaseRunSandbox),
 *  and when the run parks it does too, unless `sandboxKeepOnPark`. The next
 *  run, or the resumed one, makes a fresh one. A sandbox an earlier run left
 *  behind (a crash, a stop with no worker) is ended by the next run that
 *  finds it, and by its own timeout before that. */

export interface ThreadSandbox {
  sandbox: Sandbox;
  /** True when this call made the sandbox. */
  created: boolean;
  /** True when the thread had a sandbox that is gone now: idle too long,
   *  past its lifetime, or ended on its own. Its files are lost, and a tool
   *  should tell the model so it does not expect them. */
  lost: boolean;
  /** True when the provider gives every new sandbox the work folder back
   *  (`restoresWorkdir`): after `lost`, the project files are there again
   *  and only what was outside the work folder is gone. */
  restored: boolean;
}

/** What the kv keeps for a thread's sandbox. */
interface SandboxRecord {
  id: string;
  provider: string;
  createdAt: number;
  lastUsedAt: number;
  /** The run that made the sandbox (`sandboxScope: 'run'` only). */
  runId?: string;
  /** A sandbox the run gave back when it parked: it is gone, and the resumed
   *  run's next call must say so. */
  released?: boolean;
}

export const sandboxKey = (threadId: string) => `agent:tool:sandbox:${threadId}`;

/** A sandbox's own timeout is pushed back at most this often, and set this
 *  much past the idle limit so it never ends before the runtime counts it as
 *  idle. */
const PUSH_EVERY_MS = 60_000;

/** Handles this process already holds, by sandbox id, so a call does not
 *  reconnect every time. */
const handles = new Map<string, { sandbox: Sandbox; pushedAt: number }>();
const MAX_HANDLES = 1_000;
/** Calls under way, by thread: parallel subagents get one sandbox, not one
 *  each. */
const pending = new Map<string, Promise<ThreadSandbox>>();

/** The thread's sandbox, for a tool you write yourself. Pass the options
 *  your tool's `execute` was given:
 *
 *    execute: async ({ path }, opts) => {
 *      const { sandbox } = await sandboxFor(opts);
 *      return sandbox.filesystem.readFile(path);
 *    }
 */
export function sandboxFor(toolOptions: unknown): Promise<ThreadSandbox> {
  const run = toolRunOf(toolOptions);
  if (!run) return Promise.reject(new Error('sandboxFor: this tool is not running inside an agentenkit run'));
  return threadSandbox(run);
}

/** Runs `fn` on the thread's sandbox. When the sandbox turns out to be gone
 *  part way (it ended on its own), a fresh one is made and `fn` runs once
 *  more, told `lost: true`:
 *
 *    execute: (args, opts) => withSandbox(opts, ({ sandbox, lost }) => ...)
 */
export function withSandbox<T>(toolOptions: unknown, fn: (ts: ThreadSandbox) => Promise<T>): Promise<T> {
  const run = toolRunOf(toolOptions);
  if (!run) return Promise.reject(new Error('withSandbox: this tool is not running inside an agentenkit run'));
  return withThreadSandbox(run, fn);
}

export async function withThreadSandbox<T>(run: ToolRun, fn: (ts: ThreadSandbox) => Promise<T>): Promise<T> {
  const ts = await threadSandbox(run);
  try {
    return await fn(ts);
  } catch (e) {
    if (!(e instanceof SandboxGoneError)) throw e;
    handles.delete(ts.sandbox.sandboxId);
    // Only while the record still names the sandbox that is gone: a parallel
    // call may already have made the next one.
    const key = sandboxKey(run.threadId);
    const raw = await run.deps.kv.get(key);
    if (parse(raw)?.id === ts.sandbox.sandboxId) await run.deps.kv.delIfValue(key, raw!);
    return fn({ ...(await threadSandbox(run)), lost: true });
  }
}

export function threadSandbox(run: ToolRun): Promise<ThreadSandbox> {
  const provider = run.deps.tools?.sandbox;
  if (!provider) return Promise.reject(new Error('No sandbox: pass setupAgentCore({ tools: { sandbox } })'));
  const key = run.threadId;
  let p = pending.get(key);
  if (!p) {
    p = acquire(run, provider).finally(() => pending.delete(key));
    pending.set(key, p);
  }
  return p.then((ts) => {
    run.onSandbox?.(ts.sandbox);
    return ts;
  });
}

async function acquire(run: ToolRun, provider: SandboxProvider): Promise<ThreadSandbox> {
  const { kv, config } = run.deps;
  const idle = config.sandboxIdleTtlMs;
  const lifetime = config.sandboxMaxLifetimeMs;
  const now = Date.now();
  let rec = parse(await kv.get(sandboxKey(run.threadId)));
  // A sandbox from another adapter cannot be reached from this one.
  if (rec && rec.provider !== provider.name) rec = null;

  let lost = false;
  if (rec && config.sandboxScope === 'run') {
    if (rec.released) {
      // Given back at a park: for the same run, its files are gone.
      lost = rec.runId === run.runId;
      rec = null;
    } else if (rec.runId !== run.runId) {
      // Left by an earlier run that never gave it back. This run starts
      // fresh; that is the normal case here, not a loss.
      await destroyQuietly(run.deps, provider, rec.id);
      rec = null;
    }
  }
  if (rec && (now - rec.lastUsedAt > idle || now - rec.createdAt > lifetime)) {
    await destroyQuietly(run.deps, provider, rec.id);
    rec = null;
    lost = true;
  }

  let sandbox: Sandbox | undefined;
  let created = false;
  if (rec) {
    try {
      sandbox = await connect(provider, rec.id);
      const held = handles.get(rec.id)!;
      if (now - held.pushedAt >= PUSH_EVERY_MS) {
        await sandbox.setTimeout(Math.min(idle + PUSH_EVERY_MS, lifetime - (now - rec.createdAt))).catch((e) => {
          if (!(e instanceof SandboxUnsupportedError)) throw e;
        });
        held.pushedAt = now;
      }
      rec.lastUsedAt = now;
    } catch (e) {
      if (!(e instanceof SandboxGoneError)) throw e;
      handles.delete(rec.id);
      rec = null;
      lost = true;
    }
  }
  if (!rec) {
    sandbox = await provider.create({
      // The app's defaults, then the two the runtime owns.
      ...config.sandboxDefaults,
      timeoutMs: Math.min(idle + PUSH_EVERY_MS, lifetime),
      metadata: {
        threadId: run.threadId,
        ...(run.runId ? { runId: run.runId } : {}),
        ...(run.state ? { state: run.state } : {}),
      },
    });
    remember(sandbox, now);
    rec = {
      id: sandbox.sandboxId, provider: provider.name, createdAt: now, lastUsedAt: now,
      ...(config.sandboxScope === 'run' && run.runId ? { runId: run.runId } : {}),
    };
    created = true;
  }
  await kv.set(sandboxKey(run.threadId), JSON.stringify(rec), { exSeconds: Math.ceil(lifetime / 1000) });
  return { sandbox: sandbox!, created, lost, restored: provider.restoresWorkdir === true };
}

async function connect(provider: SandboxProvider, id: string): Promise<Sandbox> {
  const held = handles.get(id);
  if (held) return held.sandbox;
  const sandbox = await provider.connect(id);
  // A fresh handle pushes the timeout on its first use.
  remember(sandbox, 0);
  return sandbox;
}

/** Drops every handle this process holds, as a fresh worker would start.
 *  @internal For tests. */
export function forgetSandboxHandles() {
  handles.clear();
}

function remember(sandbox: Sandbox, pushedAt: number) {
  if (handles.size >= MAX_HANDLES) handles.delete(handles.keys().next().value!);
  handles.set(sandbox.sandboxId, { sandbox, pushedAt });
}

async function destroyQuietly(deps: RuntimePorts, provider: SandboxProvider, id: string) {
  handles.delete(id);
  try {
    await (await provider.connect(id)).destroy();
  } catch (e) {
    if (!(e instanceof SandboxGoneError)) (deps.log ?? console).error('sandbox not destroyed', { sandboxId: id, err: e });
  }
}

/** Ends the thread's sandbox, when it has one. Part of deleting a thread;
 *  never throws, since the thread is gone either way. */
export async function destroyThreadSandbox(deps: RuntimePorts, threadId: string): Promise<void> {
  const provider = deps.tools?.sandbox;
  const rec = parse(await deps.kv.get(sandboxKey(threadId)).catch(() => null));
  if (provider && rec && rec.provider === provider.name) await destroyQuietly(deps, provider, rec.id);
  await deps.kv.del(sandboxKey(threadId)).catch(() => undefined);
}

/** Ends the sandbox a run made, when the run is done with it
 *  (`sandboxScope: 'run'` only; otherwise it does nothing). `parked` says the
 *  run is waiting rather than ended: then the sandbox is kept when
 *  `sandboxKeepOnPark`, and otherwise a mark is left so the resumed run is
 *  told its files are gone. A sandbox another run made is left alone. Never
 *  throws: the sandbox's own timeout ends it anyway. */
export async function releaseRunSandbox(
  deps: RuntimePorts,
  threadId: string,
  runId: string | undefined,
  parked: boolean,
): Promise<void> {
  const provider = deps.tools?.sandbox;
  if (!provider || !runId || deps.config.sandboxScope !== 'run') return;
  if (parked && deps.config.sandboxKeepOnPark) return;
  try {
    const key = sandboxKey(threadId);
    const raw = await deps.kv.get(key);
    const rec = parse(raw);
    if (!rec || rec.released || rec.runId !== runId || rec.provider !== provider.name) return;
    await destroyQuietly(deps, provider, rec.id);
    if (!parked) {
      await deps.kv.delIfValue(key, raw!);
      return;
    }
    await deps.kv.set(key, JSON.stringify({ ...rec, released: true }), {
      exSeconds: Math.ceil(deps.config.sandboxMaxLifetimeMs / 1000),
    });
  } catch (err) {
    (deps.log ?? console).error('sandbox release not recorded', { threadId, runId, err });
  }
}

/** Runs one tool call and, when it used the sandbox and the app set
 *  `afterSandboxCall`, calls the hook once after it. What the hook throws
 *  becomes the call's error, unless the call failed on its own. `call` gets
 *  the ToolRun to hand the tool, which notes the sandbox it used. A park is
 *  not a failure: the hook sees no error for it. */
export async function callWithSandboxHook<T>(
  run: ToolRun | undefined,
  toolName: string,
  toolCallId: string | undefined,
  isPark: (err: unknown) => boolean,
  call: (run: ToolRun | undefined) => Promise<T>,
): Promise<T> {
  const hook = run?.deps.tools?.afterSandboxCall;
  if (!run || !hook) return call(run);
  let used: Sandbox | undefined;
  const tracked: ToolRun = { ...run, onSandbox: (s) => { used = s; } };
  let out: T | undefined;
  let failed = false;
  let error: unknown;
  try {
    out = await call(tracked);
  } catch (err) {
    failed = true;
    error = err;
  }
  if (used) {
    const ownError = failed && !isPark(error);
    try {
      await hook({
        threadId: run.threadId,
        ...(run.runId ? { runId: run.runId } : {}),
        agentId: run.agentId,
        toolName,
        ...(toolCallId ? { toolCallId } : {}),
        sandbox: used,
        ...(ownError ? { error } : {}),
      });
    } catch (hookErr) {
      if (!failed) throw hookErr;
    }
  }
  if (failed) throw error;
  return out as T;
}

function parse(raw: string | null): SandboxRecord | null {
  if (!raw) return null;
  try {
    const r = JSON.parse(raw) as SandboxRecord;
    return typeof r.id === 'string' && typeof r.provider === 'string' ? r : null;
  } catch {
    return null;
  }
}
