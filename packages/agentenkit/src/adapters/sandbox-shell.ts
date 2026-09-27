import { posix } from 'node:path';
import { MAX_COMMAND_OUTPUT_BYTES, type FileEntry } from '../ports/sandbox.js';

/** Pieces the sandbox adapters share. The Go adapters use the same scripts
 *  (adapters/internal/sandboxsh), so a command behaves the same in both. */

/** Runs `$1` in bash (sh where there is no bash), under `timeout` for `$2`
 *  seconds when that is given and the sandbox has `timeout`. `$3` is `-l`
 *  for a login shell. Called as `sh -c RUN_SCRIPT sandbox <command> <secs> <-l>`. */
export const RUN_SCRIPT =
  'if command -v bash >/dev/null 2>&1; then s=bash; else s=sh; fi; ' +
  'if [ -n "$2" ] && command -v timeout >/dev/null 2>&1; then exec timeout -k 2 "$2" "$s" $3 -c "$1"; fi; ' +
  'exec "$s" $3 -c "$1"';

/** The argv that runs `command` through RUN_SCRIPT. */
export function runArgv(command: string, timeoutSeconds?: number, login = false): string[] {
  return ['sh', '-c', RUN_SCRIPT, 'sandbox', command, timeoutSeconds ? String(timeoutSeconds) : '', login ? '-l' : ''];
}

/** Starts `command` apart from the caller and returns at once. */
export function backgroundCommand(command: string): string {
  return `nohup sh -c ${shellQuote(command)} >/dev/null 2>&1 &`;
}

/** Whole seconds for `timeout(1)`, never less than one. */
export function timeoutSeconds(ms: number): number {
  return Math.max(1, Math.ceil(ms / 1000));
}

/** `timeout` stops a command with 124 (TERM) or 137 (KILL after -k). */
export function looksTimedOut(exitCode: number, elapsedMs: number, limitMs: number): boolean {
  return (exitCode === 124 || exitCode === 137) && elapsedMs >= limitMs - 250;
}

export function shellQuote(s: string): string {
  return `'${s.replace(/'/g, `'\\''`)}'`;
}

/** A relative path is taken from the work folder. */
export function resolveIn(workdir: string, path: string): string {
  return path.startsWith('/') ? posix.normalize(path) : posix.join(workdir, path);
}

/** The exit code the file scripts give for "no such file or folder". */
export const NOT_FOUND_EXIT = 44;

/** `sh -c <script> sandbox <path>` scripts for adapters that reach files
 *  only through commands. Each exits 44 when the path is not there. */
export const FILE_SCRIPTS = {
  read: '[ -f "$1" ] || exit 44; cat -- "$1"',
  readBase64: '[ -f "$1" ] || exit 44; base64 < "$1"',
  write: 'mkdir -p -- "$(dirname -- "$1")" && cat > "$1"',
  list:
    '[ -d "$1" ] || exit 44; cd -- "$1" || exit 44; ' +
    'for f in * .[!.]* ..?*; do [ -e "$f" ] || [ -L "$f" ] || continue; ' +
    'if [ -d "$f" ]; then printf \'d\\t0\\t%s\\n\' "$f"; ' +
    'else printf \'f\\t%s\\t%s\\n\' "$(wc -c < "$f" | tr -d \' \')" "$f"; fi; done',
  mkdir: 'mkdir -p -- "$1"',
  exists: '[ -e "$1" ] || [ -L "$1" ]',
  remove: 'rm -rf -- "$1"',
} as const;

/** Reads what FILE_SCRIPTS.list printed. */
export function parseListing(out: string): FileEntry[] {
  const entries: FileEntry[] = [];
  for (const line of out.split('\n')) {
    if (!line) continue;
    const [kind, size, ...rest] = line.split('\t');
    const name = rest.join('\t');
    entries.push(kind === 'd' ? { name, type: 'directory' } : { name, type: 'file', size: Number(size) || 0 });
  }
  return sortEntries(entries);
}

export function sortEntries(entries: FileEntry[]): FileEntry[] {
  return entries.sort((a, b) => (a.name < b.name ? -1 : a.name > b.name ? 1 : 0));
}

/** Collects one output stream: keeps up to MAX_COMMAND_OUTPUT_BYTES, hands
 *  each piece on as text as it comes, never splitting a character. */
export class OutputCollector {
  private readonly decoder = new TextDecoder();
  private readonly parts: string[] = [];
  private kept = 0;
  truncated = false;

  constructor(private readonly onText?: (chunk: string) => void) {}

  push(chunk: Uint8Array): void {
    let bytes = chunk;
    if (this.kept + bytes.length > MAX_COMMAND_OUTPUT_BYTES) {
      bytes = bytes.subarray(0, Math.max(0, MAX_COMMAND_OUTPUT_BYTES - this.kept));
      this.truncated = true;
    }
    this.kept += bytes.length;
    if (bytes.length === 0) return;
    const text = this.decoder.decode(bytes, { stream: true });
    if (text) this.add(text);
  }

  end(): string {
    const rest = this.decoder.decode();
    // Past the cap, a character cut in half is dropped rather than shown
    // as a broken one.
    if (rest && !this.truncated) this.add(rest);
    return this.parts.join('');
  }

  private add(text: string) {
    this.parts.push(text);
    this.onText?.(text);
  }
}

/** Output from an adapter that hands it over whole, cut to
 *  MAX_COMMAND_OUTPUT_BYTES the way OutputCollector cuts a stream. */
export function capOutput(text: string): { text: string; truncated: boolean } {
  const bytes = new TextEncoder().encode(text);
  if (bytes.length <= MAX_COMMAND_OUTPUT_BYTES) return { text, truncated: false };
  return { text: new TextDecoder().decode(bytes.subarray(0, MAX_COMMAND_OUTPUT_BYTES)).replace(/\uFFFD$/, ''), truncated: true };
}

/** Random lowercase letters and digits, for sandbox names. */
export function randomId(length = 12): string {
  const alphabet = 'abcdefghijklmnopqrstuvwxyz0123456789';
  const bytes = crypto.getRandomValues(new Uint8Array(length));
  let out = '';
  for (const b of bytes) out += alphabet[b % alphabet.length];
  return out;
}

/** Base64 without the line breaks some `base64` tools add. */
export function decodeBase64(text: string): Uint8Array {
  return Uint8Array.from(Buffer.from(text.replace(/\s+/g, ''), 'base64'));
}

export function encodeBase64(bytes: Uint8Array): string {
  return Buffer.from(bytes).toString('base64');
}

export function toBytes(content: string | Uint8Array): Uint8Array {
  return typeof content === 'string' ? new TextEncoder().encode(content) : content;
}
