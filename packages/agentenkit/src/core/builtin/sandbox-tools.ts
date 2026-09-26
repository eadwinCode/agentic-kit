import { createHash } from 'node:crypto';
import { SandboxFileNotFoundError, type FileEntry, type Sandbox } from '../../ports/sandbox.js';
import { parkForInput } from '../hitl.js';
import { publishEvent } from '../publish.js';
import { recordToolUsage, toolUseRow, type ToolRun } from './run.js';
import { withThreadSandbox, type ThreadSandbox } from './sandbox.js';

/** bash, code_execution and text_editor (spec T4): the built-in tools that
 *  work in the thread's sandbox. The Go runtime has the same three
 *  (core/builtin_sandbox.go), with the same inputs, results and wording. */

/** Whether a call waits for a person to approve it first: `true` always,
 *  `false` never, or a check that decides per call from the input. An
 *  approved call runs when the approval comes back; a denied one tells the
 *  model it was denied. */
export type ApprovalRule<I> = boolean | ((input: I) => boolean | Promise<boolean>);

export interface BashInput {
  command?: string;
  restart?: boolean;
}

export interface CodeExecutionInput {
  language?: 'python' | 'javascript';
  code?: string;
}

export interface TextEditorInput {
  command?: 'view' | 'create' | 'str_replace' | 'insert' | 'undo_edit';
  path?: string;
  view_range?: [number, number];
  file_text?: string;
  old_str?: string;
  new_str?: string;
  insert_line?: number;
}

export interface BashOptions {
  /** How long one command may run. Default 120 s. */
  timeoutMs?: number;
  /** Commands allowed in one run. Default: no limit. */
  maxUses?: number;
  /** Default `true`: every command waits for approval. */
  approval?: ApprovalRule<BashInput>;
}

export interface CodeExecutionOptions {
  /** How long one program may run. Default 60 s. */
  timeoutMs?: number;
  maxUses?: number;
  /** Default `false`: it runs sealed in the sandbox. */
  approval?: ApprovalRule<CodeExecutionInput>;
}

export interface TextEditorOptions {
  maxUses?: number;
  /** Default: changes wait for approval, `view` does not. */
  approval?: ApprovalRule<TextEditorInput>;
}

/** Where the tools keep their own files, in the sandbox's work folder: the
 *  bash folder, the programs code_execution ran, the editor's undo history.
 *  Hidden, so a plain `ls` does not show it. */
const STATE_DIR = '.agentenkit';
/** Changes the editor can take back, per file. */
const UNDO_DEPTH = 10;

const LOST_NOTE =
  "The sandbox this conversation used before has ended, so files from earlier are gone. This is a fresh one.";

const failed = (error: string, note?: string) => (note ? { error, note } : { error });

/** Count one use and say whether it is over the run's limit. */
async function overLimit(run: ToolRun, tool: string, maxUses: number | undefined): Promise<boolean> {
  if (!maxUses) return false;
  const scope = run.runId ?? run.threadId;
  const ttl = Math.ceil(run.deps.config.hitlTtlMs / 1000) + 24 * 60 * 60;
  return (await run.deps.kv.incrWithExpiry(`agent:tool:uses:${scope}:${tool}`, ttl)) > maxUses;
}

/** Ask for approval when the rule says so and this call is not already
 *  the approved one coming back. */
async function needsApproval<I>(rule: ApprovalRule<I>, input: I, opts: { approval?: unknown }): Promise<boolean> {
  if (opts.approval) return false;
  return typeof rule === 'function' ? Boolean(await rule(input)) : rule;
}

/** Text over `max` UTF-16 units keeps its start and its end, with a line
 *  saying how much was cut from the middle: the start says what ran, the end
 *  holds the error. Never splits a character. The Go runtime cuts the same. */
export function cutMiddle(text: string, max: number): { text: string; cut: boolean } {
  if (text.length <= max) return { text, cut: false };
  let head = Math.floor(max / 2);
  let tail = text.length - (max - head);
  if (isHigh(text.charCodeAt(head - 1))) head--;
  if (isLow(text.charCodeAt(tail))) tail++;
  return { text: `${text.slice(0, head)}\n… ${tail - head} characters cut …\n${text.slice(tail)}`, cut: true };
}
const isHigh = (c: number) => c >= 0xd800 && c <= 0xdbff;
const isLow = (c: number) => c >= 0xdc00 && c <= 0xdfff;

/** Output as it comes, on the thread as live-only `tool.output` events, a
 *  few at a time so a chatty command does not flood the bus. */
function liveOutput(run: ToolRun, tool: string, toolCallId: string | undefined) {
  let buf: Array<{ stream: 'stdout' | 'stderr'; text: string }> = [];
  let timer: ReturnType<typeof setTimeout> | undefined;
  let chain: Promise<unknown> = Promise.resolve();
  const flush = () => {
    timer = undefined;
    if (buf.length === 0) return;
    const parts = buf;
    buf = [];
    for (const stream of ['stdout', 'stderr'] as const) {
      const text = parts.filter((p) => p.stream === stream).map((p) => p.text).join('');
      if (!text) continue;
      const payload = { toolCallId: toolCallId ?? null, tool, stream, text, ...(run.agentId ? { agentId: run.agentId } : {}) };
      chain = chain.then(() => publishEvent(run.deps, run.threadId, 'tool.output', payload)).catch(() => undefined);
    }
  };
  const push = (stream: 'stdout' | 'stderr') => (text: string) => {
    buf.push({ stream, text });
    timer ??= setTimeout(flush, 100);
  };
  return {
    onStdout: push('stdout'),
    onStderr: push('stderr'),
    done: async () => {
      if (timer) clearTimeout(timer);
      flush();
      await chain;
    },
  };
}

function capOutput(run: ToolRun, r: { stdout: string; stderr: string; truncated?: boolean }) {
  const max = run.deps.config.builtinToolResultCapChars;
  const out = cutMiddle(r.stdout, max);
  const err = cutMiddle(r.stderr, max);
  return { stdout: out.text, stderr: err.text, truncated: Boolean(r.truncated || out.cut || err.cut) };
}

const seconds = (ms: number) => Math.round(ms) / 1000;

// --- bash -----------------------------------------------------------------

/** The folder a bash call ends in is kept in the sandbox, and the next call
 *  starts there: the "session" the model expects, without a shell that has
 *  to outlive the process. */
function bashScript(command: string, restart: boolean): string {
  return [
    `__ak="$PWD/${STATE_DIR}"; mkdir -p "$__ak"`,
    ...(restart ? ['rm -f "$__ak/bash-cwd"'] : []),
    `if [ -f "$__ak/bash-cwd" ]; then cd -- "$(cat "$__ak/bash-cwd")" 2>/dev/null; fi`,
    `trap 'pwd > "$__ak/bash-cwd"' EXIT`,
    command,
  ].join('\n');
}

export async function runBash(
  options: BashOptions,
  input: BashInput,
  run: ToolRun,
  opts: { toolCallId?: string; abortSignal?: AbortSignal; approval?: unknown },
): Promise<unknown> {
  const command = typeof input.command === 'string' ? input.command : '';
  const restart = input.restart === true;
  if (!command.trim() && !restart) return failed('bash: command is required');
  if (await needsApproval(options.approval ?? true, input, opts)) throw parkForInput({ payload: input });
  if (await overLimit(run, 'bash', options.maxUses)) return failed(`bash: this run has used its ${options.maxUses} commands`);
  const live = liveOutput(run, 'bash', opts.toolCallId);
  try {
    return await withThreadSandbox(run, async ({ sandbox, lost }) => {
      const r = await sandbox.runCommand(bashScript(command || ':', restart), {
        timeoutMs: options.timeoutMs ?? 120_000,
        onStdout: live.onStdout,
        onStderr: live.onStderr,
        ...(opts.abortSignal ? { signal: opts.abortSignal } : {}),
      });
      await recordToolUsage(run, toolUseRow('bash', sandbox.provider, 1, seconds(r.durationMs)));
      const out = capOutput(run, r);
      return {
        stdout: out.stdout,
        stderr: out.stderr,
        exitCode: r.exitCode,
        ...(out.truncated ? { truncated: true } : {}),
        ...(r.timedOut ? { timedOut: true, error: `bash: the command was stopped after ${seconds(options.timeoutMs ?? 120_000)} s` } : {}),
        ...(lost ? { note: LOST_NOTE } : {}),
      };
    });
  } finally {
    await live.done();
  }
}

// --- code_execution -------------------------------------------------------

const RUNNERS = {
  python: { ext: 'py', run: 'python3' },
  javascript: { ext: 'js', run: 'node' },
} as const;

/** The media type of a file the program made, from its name. */
export function mediaTypeOf(path: string): string {
  const ext = path.slice(path.lastIndexOf('.') + 1).toLowerCase();
  return (
    {
      png: 'image/png', jpg: 'image/jpeg', jpeg: 'image/jpeg', gif: 'image/gif', webp: 'image/webp',
      svg: 'image/svg+xml', pdf: 'application/pdf', csv: 'text/csv', json: 'application/json',
      txt: 'text/plain', md: 'text/markdown', html: 'text/html',
    } as Record<string, string>
  )[ext] ?? 'application/octet-stream';
}

export async function runCodeExecution(
  options: CodeExecutionOptions,
  input: CodeExecutionInput,
  run: ToolRun,
  opts: { toolCallId?: string; abortSignal?: AbortSignal; approval?: unknown },
): Promise<unknown> {
  const code = typeof input.code === 'string' ? input.code : '';
  if (!code.trim()) return failed('code_execution: code is required');
  const language = input.language ?? 'python';
  const runner = RUNNERS[language];
  if (!runner) return failed(`code_execution: language must be python or javascript, not ${JSON.stringify(language)}`);
  if (await needsApproval(options.approval ?? false, input, opts)) throw parkForInput({ payload: input });
  if (await overLimit(run, 'code_execution', options.maxUses)) {
    return failed(`code_execution: this run has used its ${options.maxUses} runs`);
  }
  const limit = options.timeoutMs ?? 60_000;
  const live = liveOutput(run, 'code_execution', opts.toolCallId);
  try {
    return await withThreadSandbox(run, async ({ sandbox, lost }) => {
      const name = `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
      const file = `${STATE_DIR}/code/${name}.${runner.ext}`;
      await sandbox.filesystem.writeFile(file, code);
      // A marker made just before the run: the files newer than it are the
      // ones the program made or changed.
      const script = [
        `touch "${STATE_DIR}/code/${name}.start"`,
        `${runner.run} "${file}"`,
        '__ec=$?',
        `find . -path "./${STATE_DIR}" -prune -o -type f -newer "${STATE_DIR}/code/${name}.start" -print > "${STATE_DIR}/code/${name}.files" 2>/dev/null`,
        'exit $__ec',
      ].join('\n');
      const r = await sandbox.runCommand(script, {
        timeoutMs: limit,
        onStdout: live.onStdout,
        onStderr: live.onStderr,
        ...(opts.abortSignal ? { signal: opts.abortSignal } : {}),
      });
      await recordToolUsage(run, toolUseRow('code_execution', sandbox.provider, 1, seconds(r.durationMs)));
      const listed = await sandbox.filesystem.readFile(`${STATE_DIR}/code/${name}.files`).catch(() => '');
      const files = listed
        .split('\n')
        .map((l) => l.replace(/^\.\//, ''))
        .filter(Boolean)
        .sort()
        .map((path) => ({ path, mediaType: mediaTypeOf(path) }));
      const out = capOutput(run, r);
      const error = r.timedOut
        ? `code_execution: the program was stopped after ${seconds(limit)} s`
        : r.exitCode !== 0
          ? `code_execution: the program exited with code ${r.exitCode}`
          : undefined;
      return {
        stdout: out.stdout,
        stderr: out.stderr,
        exitCode: r.exitCode,
        ...(error ? { error } : {}),
        files,
        ...(out.truncated ? { truncated: true } : {}),
        ...(lost ? { note: LOST_NOTE } : {}),
      };
    });
  } finally {
    await live.done();
  }
}

// --- text_editor ----------------------------------------------------------

const EDITS = new Set(['create', 'str_replace', 'insert', 'undo_edit']);

/** The editor's undo history for one file: earlier versions, newest last;
 *  null where the file did not exist yet. */
const historyPath = (path: string) =>
  `${STATE_DIR}/history/${createHash('sha256').update(path).digest('hex').slice(0, 32)}.json`;

async function readHistory(sandbox: Sandbox, path: string): Promise<Array<string | null>> {
  try {
    return JSON.parse(await sandbox.filesystem.readFile(historyPath(path))) as Array<string | null>;
  } catch {
    return [];
  }
}

async function remember(sandbox: Sandbox, path: string, before: string | null) {
  const history = [...(await readHistory(sandbox, path)), before].slice(-UNDO_DEPTH);
  await sandbox.filesystem.writeFile(historyPath(path), JSON.stringify(history));
}

async function readOrNull(sandbox: Sandbox, path: string): Promise<string | null> {
  try {
    return await sandbox.filesystem.readFile(path);
  } catch (e) {
    if (e instanceof SandboxFileNotFoundError) return null;
    throw e;
  }
}

/** Lines of `text` numbered from `from`, as `cat -n` shows them. */
function numbered(lines: string[], from: number): string {
  return lines.map((l, i) => `${String(from + i).padStart(6)}\t${l}`).join('\n');
}

/** A folder's contents two levels deep, hidden entries left out; null when
 *  `path` is not a folder. */
async function listFolder(sandbox: Sandbox, path: string): Promise<string | null> {
  try {
    await sandbox.filesystem.readdir(path);
  } catch (e) {
    if (e instanceof SandboxFileNotFoundError) return null;
    throw e;
  }
  const out: string[] = [];
  const walk = async (dir: string, prefix: string, depth: number) => {
    let entries: FileEntry[];
    try {
      entries = await sandbox.filesystem.readdir(dir);
    } catch {
      return;
    }
    for (const e of entries) {
      if (e.name.startsWith('.')) continue;
      out.push(`${prefix}${e.name}${e.type === 'directory' ? '/' : ''}`);
      if (e.type === 'directory' && depth < 2) await walk(`${dir}/${e.name}`, `${prefix}${e.name}/`, depth + 1);
    }
  };
  await walk(path, '', 1);
  return out.join('\n');
}

const count = (text: string, part: string) => {
  let n = 0;
  for (let i = text.indexOf(part); i !== -1; i = text.indexOf(part, i + part.length)) n++;
  return n;
};

async function edit(sandbox: Sandbox, run: ToolRun, input: TextEditorInput): Promise<Record<string, unknown>> {
  const path = input.path!;
  const max = run.deps.config.builtinToolResultCapChars;
  switch (input.command) {
    case 'view': {
      // A folder first: reading one as a file fails differently on each
      // adapter, listing a file fails the same on all.
      const listing = await listFolder(sandbox, path);
      if (listing !== null) {
        const shown = cutMiddle(listing, max);
        return { path, content: shown.text, ...(shown.cut ? { truncated: true } : {}) };
      }
      const text = await readOrNull(sandbox, path);
      if (text === null) return failed(`text_editor: ${path} does not exist`);
      const lines = text.split('\n');
      let [from, to] = input.view_range ?? [1, -1];
      if (to === -1 || to > lines.length) to = lines.length;
      if (!Number.isInteger(from) || !Number.isInteger(to) || from < 1 || from > Math.max(lines.length, 1) || to < from) {
        return failed(`text_editor: view_range must be two line numbers within 1 and ${lines.length}`);
      }
      const shown = cutMiddle(numbered(lines.slice(from - 1, to), from), max);
      return { path, content: shown.text, totalLines: lines.length, ...(shown.cut ? { truncated: true } : {}) };
    }
    case 'create': {
      if (typeof input.file_text !== 'string') return failed('text_editor: create needs file_text');
      const before = await readOrNull(sandbox, path);
      await remember(sandbox, path, before);
      await sandbox.filesystem.writeFile(path, input.file_text);
      return { ok: true, message: `${before === null ? 'Created' : 'Replaced'} ${path}.` };
    }
    case 'str_replace': {
      if (typeof input.old_str !== 'string' || input.old_str === '') return failed('text_editor: str_replace needs old_str');
      const text = await readOrNull(sandbox, path);
      if (text === null) return failed(`text_editor: ${path} does not exist`);
      const n = count(text, input.old_str);
      if (n === 0) return failed(`text_editor: old_str was not found in ${path}; it must match exactly, whitespace included`);
      if (n > 1) return failed(`text_editor: old_str matches ${n} places in ${path}; include more of the text around it so it matches one`);
      await remember(sandbox, path, text);
      const i = text.indexOf(input.old_str);
      await sandbox.filesystem.writeFile(path, text.slice(0, i) + (input.new_str ?? '') + text.slice(i + input.old_str.length));
      return { ok: true, message: `Replaced 1 place in ${path}.` };
    }
    case 'insert': {
      if (typeof input.new_str !== 'string') return failed('text_editor: insert needs new_str');
      const text = await readOrNull(sandbox, path);
      if (text === null) return failed(`text_editor: ${path} does not exist`);
      const lines = text.split('\n');
      const at = input.insert_line;
      if (!Number.isInteger(at) || at! < 0 || at! > lines.length) {
        return failed(`text_editor: insert_line must be within 0 and ${lines.length}`);
      }
      await remember(sandbox, path, text);
      lines.splice(at!, 0, ...input.new_str.split('\n'));
      await sandbox.filesystem.writeFile(path, lines.join('\n'));
      return { ok: true, message: `Inserted after line ${at} of ${path}.` };
    }
    case 'undo_edit': {
      const history = await readHistory(sandbox, path);
      if (history.length === 0) return failed(`text_editor: there is no change to ${path} to undo`);
      const before = history.pop()!;
      if (before === null) await sandbox.filesystem.remove(path);
      else await sandbox.filesystem.writeFile(path, before);
      await sandbox.filesystem.writeFile(historyPath(path), JSON.stringify(history));
      return { ok: true, message: before === null ? `Removed ${path}, which the change had created.` : `Undid the last change to ${path}.` };
    }
    default:
      return failed('text_editor: command must be view, create, str_replace, insert or undo_edit');
  }
}

export async function runTextEditor(
  options: TextEditorOptions,
  input: TextEditorInput,
  run: ToolRun,
  opts: { approval?: unknown },
): Promise<unknown> {
  if (typeof input.command !== 'string' || !['view', ...EDITS].includes(input.command)) {
    return failed('text_editor: command must be view, create, str_replace, insert or undo_edit');
  }
  if (typeof input.path !== 'string' || !input.path.trim()) return failed('text_editor: path is required');
  const rule = options.approval ?? ((i: TextEditorInput) => EDITS.has(i.command ?? ''));
  if (await needsApproval(rule, input, opts)) throw parkForInput({ payload: input });
  if (await overLimit(run, 'text_editor', options.maxUses)) {
    return failed(`text_editor: this run has used its ${options.maxUses} edits and views`);
  }
  return withThreadSandbox(run, async ({ sandbox, lost }: ThreadSandbox) => {
    const result = await edit(sandbox, run, input);
    await recordToolUsage(run, toolUseRow('text_editor', sandbox.provider, 1, 0));
    return lost ? { ...result, note: LOST_NOTE } : result;
  });
}
