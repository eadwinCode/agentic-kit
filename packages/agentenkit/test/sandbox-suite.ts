import { afterAll, describe, expect, it } from 'bun:test';
import {
  SandboxFileNotFoundError,
  SandboxGoneError,
  MAX_COMMAND_OUTPUT_BYTES,
  type Sandbox,
  type SandboxProvider,
} from '../src/ports/sandbox.js';

// The promise every sandbox adapter keeps, run against each one. The Go
// package runs the same cases under the same names (sandbox_test.go).

export interface SandboxSuiteOptions {
  /** False for an adapter that cannot push a sandbox's time back. */
  setTimeout?: boolean;
  /** Per-case time limit, for adapters that are slow to start one. */
  caseTimeoutMs?: number;
}

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));
const HOUR = 60 * 60_000;

export function sandboxSuite(name: string, make: () => SandboxProvider | Promise<SandboxProvider>, opts: SandboxSuiteOptions = {}) {
  const limit = opts.caseTimeoutMs ?? 30_000;
  let provider: SandboxProvider | undefined;
  let shared: Sandbox | undefined;
  const made: Sandbox[] = [];

  const getProvider = async () => (provider ??= await make());
  const fresh = async () => {
    const s = await (await getProvider()).create({ timeoutMs: HOUR, metadata: { threadId: 't1', runId: 'r1' } });
    made.push(s);
    return s;
  };
  const box = async () => (shared ??= await fresh());

  describe(`sandbox (${name})`, () => {
    it('runs a command and returns its output', async () => {
      const r = await (await box()).runCommand('echo hello; echo oops >&2');
      expect(r.stdout).toBe('hello\n');
      expect(r.stderr).toBe('oops\n');
      expect(r.exitCode).toBe(0);
      expect(r.timedOut).toBeFalsy();
    }, limit);

    it('returns the exit code', async () => {
      expect((await (await box()).runCommand('exit 3')).exitCode).toBe(3);
    }, limit);

    it('stops a command at its time limit', async () => {
      const started = Date.now();
      const r = await (await box()).runCommand('echo start; sleep 30', { timeoutMs: 1_000 });
      expect(r.timedOut).toBe(true);
      expect(r.exitCode).toBe(124);
      expect(r.stdout).toBe('start\n');
      expect(Date.now() - started).toBeLessThan(10_000);
    }, limit);

    it('runs in the work folder by default, or in cwd', async () => {
      const s = await box();
      await s.filesystem.writeFile('top.txt', 'top');
      await s.filesystem.writeFile('sub/inner.txt', 'inner');
      expect((await s.runCommand('cat top.txt')).stdout).toBe('top');
      expect((await s.runCommand('cat inner.txt', { cwd: 'sub' })).stdout).toBe('inner');
    }, limit);

    it('passes env to the command', async () => {
      const r = await (await box()).runCommand('echo "$GREETING"', { env: { GREETING: 'hi there' } });
      expect(r.stdout).toBe('hi there\n');
    }, limit);

    it('streams output as it comes', async () => {
      const chunks: Array<{ text: string; at: number }> = [];
      const r = await (await box()).runCommand('echo one; sleep 1; echo two', {
        onStdout: (text) => chunks.push({ text, at: Date.now() }),
      });
      const ended = Date.now();
      expect(chunks.map((c) => c.text).join('')).toBe('one\ntwo\n');
      expect(r.stdout).toBe('one\ntwo\n');
      // "one" arrived well before the command ended.
      expect(ended - chunks[0]!.at).toBeGreaterThan(500);
    }, limit);

    it('cuts output past the cap and says so', async () => {
      const r = await (await box()).runCommand(`head -c ${MAX_COMMAND_OUTPUT_BYTES + 1000} /dev/zero | tr '\\0' a`);
      expect(r.truncated).toBe(true);
      expect(r.stdout.length).toBe(MAX_COMMAND_OUTPUT_BYTES);
      expect(r.exitCode).toBe(0);
    }, limit);

    it('writes and reads text files', async () => {
      const s = await box();
      await s.filesystem.writeFile('notes/a.txt', 'héllo ✓\nline two');
      expect(await s.filesystem.readFile('notes/a.txt')).toBe('héllo ✓\nline two');
      // Commands see the same files.
      expect((await s.runCommand('cat notes/a.txt')).stdout).toBe('héllo ✓\nline two');
      await s.filesystem.writeFile('notes/a.txt', 'replaced');
      expect(await s.filesystem.readFile('notes/a.txt')).toBe('replaced');
    }, limit);

    it('writes and reads binary files', async () => {
      const s = await box();
      const bytes = new Uint8Array(70_000);
      for (let i = 0; i < bytes.length; i++) bytes[i] = (i * 7) % 256;
      await s.filesystem.writeFile('bin/data.bin', bytes);
      expect(await s.filesystem.readFileBytes('bin/data.bin')).toEqual(bytes);
      expect((await s.runCommand('wc -c < bin/data.bin')).stdout.trim()).toBe('70000');
    }, limit);

    it('lists, makes and removes folders', async () => {
      const s = await box();
      await s.filesystem.mkdir('d/e');
      await s.filesystem.mkdir('d/e'); // fine when it is there
      await s.filesystem.writeFile('d/f.txt', 'x');
      expect(await s.filesystem.readdir('d')).toEqual([
        { name: 'e', type: 'directory' },
        { name: 'f.txt', type: 'file', size: 1 },
      ]);
      expect(await s.filesystem.exists('d/e')).toBe(true);
      await s.filesystem.remove('d');
      expect(await s.filesystem.exists('d')).toBe(false);
      await s.filesystem.remove('d'); // fine when it is not there
    }, limit);

    it('a missing file is not found', async () => {
      const s = await box();
      await expect(s.filesystem.readFile('nope.txt')).rejects.toBeInstanceOf(SandboxFileNotFoundError);
      await expect(s.filesystem.readFileBytes('nope.txt')).rejects.toBeInstanceOf(SandboxFileNotFoundError);
      await expect(s.filesystem.readdir('nope')).rejects.toBeInstanceOf(SandboxFileNotFoundError);
      expect(await s.filesystem.exists('nope.txt')).toBe(false);
    }, limit);

    it('runs a command in the background', async () => {
      const s = await box();
      const started = Date.now();
      await s.runCommand('sleep 1; echo done > bg.txt', { background: true });
      expect(Date.now() - started).toBeLessThan(1_000);
      let there = false;
      for (let i = 0; i < 50 && !there; i++) {
        there = await s.filesystem.exists('bg.txt');
        if (!there) await sleep(200);
      }
      expect(there).toBe(true);
    }, limit);

    it('getInfo describes the sandbox', async () => {
      const s = await box();
      const info = await s.getInfo();
      expect(info.id).toBe(s.sandboxId);
      expect(info.provider).toBe(s.provider);
      expect(info.status).toBe('running');
      expect(info.workdir.startsWith('/')).toBe(true);
      expect(info.metadata?.threadId).toBe('t1');
    }, limit);

    if (opts.setTimeout !== false) {
      it('setTimeout pushes back the end', async () => {
        const s = await box();
        await s.setTimeout(10 * 60_000);
        const info = await s.getInfo();
        const left = info.expiresAt!.getTime() - Date.now();
        expect(left).toBeGreaterThan(8 * 60_000);
        expect(left).toBeLessThan(12 * 60_000);
      }, limit);
    }

    it('connects to a sandbox made earlier', async () => {
      const s = await fresh();
      await s.filesystem.writeFile('kept.txt', 'still here');
      const again = await (await getProvider()).connect(s.sandboxId);
      expect(again.sandboxId).toBe(s.sandboxId);
      expect(await again.filesystem.readFile('kept.txt')).toBe('still here');
    }, limit);

    it('a destroyed sandbox is gone', async () => {
      const s = await fresh();
      await s.destroy();
      await expect((await getProvider()).connect(s.sandboxId)).rejects.toBeInstanceOf(SandboxGoneError);
      await s.destroy(); // fine when it is already gone
    }, limit);

    afterAll(async () => {
      for (const s of made) await s.destroy().catch(() => undefined);
    });
  });
}
