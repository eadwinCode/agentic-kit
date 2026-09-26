import { afterAll, describe, expect, it } from 'bun:test';
import { spawnSync } from 'node:child_process';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { LocalSandbox } from '../src/adapters/local-sandbox.js';
import { DockerSandbox } from '../src/adapters/docker.js';
import { E2BSandbox } from '../src/adapters/e2b.js';
import { ComputeSdkSandbox, type ComputeSdkProviderLike, type ComputeSdkSandboxLike } from '../src/adapters/computesdk.js';
import { SandboxUnsupportedError, type Sandbox } from '../src/ports/sandbox.js';
import { sandboxSuite } from './sandbox-suite.js';
import { startFakeE2B } from './e2b-fake.js';

// The sandbox adapters, each through the shared suite. Docker runs when
// TEST_DOCKER_SANDBOX=1 (it needs a docker daemon); live E2B when
// E2B_API_KEY is set. The Go package runs the same (sandbox_test.go).

const temp = (name: string) => mkdtempSync(join(tmpdir(), `${name}-`));

sandboxSuite('local', () => new LocalSandbox({ rootDir: temp('local-sandbox') }));

const fake = await startFakeE2B();
afterAll(() => fake.stop());
sandboxSuite('e2b (fake)', () => new E2BSandbox({ apiKey: 'test-key', apiUrl: fake.url, sandboxUrl: fake.url }));

/** A ComputeSDK provider made of local sandboxes: what ComputeSdkSandbox
 *  sees from any real one. */
function fakeComputeSdk(): ComputeSdkProviderLike {
  const local = new LocalSandbox({ rootDir: temp('computesdk') });
  const wrap = (s: Sandbox): ComputeSdkSandboxLike => ({
    sandboxId: s.sandboxId,
    provider: 'fake',
    runCommand: async (command, o = {}) => {
      const r = await s.runCommand(command, {
        ...(o.cwd ? { cwd: o.cwd } : {}),
        ...(o.env ? { env: o.env } : {}),
        ...(o.timeout ? { timeoutMs: o.timeout } : {}),
        ...(o.onStdout ? { onStdout: o.onStdout } : {}),
        ...(o.onStderr ? { onStderr: o.onStderr } : {}),
      });
      // A real provider hands over all the output; the local sandbox under
      // this fake has already cut it, so give back a little more.
      return { ...r, stdout: r.truncated ? `${r.stdout}more` : r.stdout };
    },
    getInfo: async () => {
      const info = await s.getInfo();
      return { id: info.id, provider: 'fake', status: 'running', createdAt: info.createdAt, timeout: 0, metadata: info.metadata };
    },
    getUrl: (o) => s.getUrl({ port: o.port }),
    destroy: () => s.destroy(),
    filesystem: {
      readFile: (p) => s.filesystem.readFile(p),
      writeFile: (p, c) => s.filesystem.writeFile(p, c),
    },
  });
  return {
    name: 'fake',
    sandbox: {
      create: async (o) => wrap(await local.create({ timeoutMs: Number(o?.timeout), metadata: o?.metadata as any })),
      getById: async (id) => local.connect(id).then(wrap, () => null),
    },
  };
}
// Absolute paths reach the machine itself here, so each run gets its own
// work folder.
sandboxSuite('computesdk (fake)', () => new ComputeSdkSandbox({ provider: fakeComputeSdk(), workdir: temp('computesdk-work') }), {
  setTimeout: false,
});

if (process.env.TEST_DOCKER_SANDBOX === '1') {
  // Pulling the image the first time can take a while.
  spawnSync('docker', ['pull', '-q', 'python:3.12-slim'], { stdio: 'ignore' });
  sandboxSuite('docker', () => new DockerSandbox(), { caseTimeoutMs: 60_000 });
}

if (process.env.E2B_API_KEY) {
  sandboxSuite('e2b', () => new E2BSandbox({ apiKey: process.env.E2B_API_KEY! }), { caseTimeoutMs: 60_000 });
}

describe('sandbox adapters', () => {
  it('E2BSandbox sends the template, time, metadata and network it was given', async () => {
    const e2b = new E2BSandbox({ apiKey: 'test-key', apiUrl: fake.url, sandboxUrl: fake.url, template: 'code-interpreter' });
    const s = await e2b.create({ timeoutMs: 90_500, metadata: { threadId: 't9', runId: 'r9' }, network: { allow: ['pypi.org'] } });
    expect(fake.lastCreate).toEqual({
      templateID: 'code-interpreter',
      timeout: 91,
      metadata: { 'agentenkit.threadId': 't9', 'agentenkit.runId': 'r9' },
      allow_internet_access: true,
      network: { allowOut: ['pypi.org'], denyOut: ['0.0.0.0/0'] },
    });
    await s.destroy();
    await e2b.create({ timeoutMs: 1_000, metadata: { threadId: 't9' } });
    expect(fake.lastCreate.allow_internet_access).toBe(false);
  });

  it('E2BSandbox needs an API key', () => {
    expect(() => new E2BSandbox({ apiKey: '' })).toThrow('E2BSandbox: apiKey is required');
  });

  it('DockerSandbox has no network allow list', async () => {
    const docker = new DockerSandbox({ docker: 'false' });
    await expect(docker.create({ timeoutMs: 1_000, metadata: { threadId: 't' }, network: { allow: ['pypi.org'] } }))
      .rejects.toBeInstanceOf(SandboxUnsupportedError);
  });

  it('LocalSandbox commands do not see the app environment', async () => {
    process.env.AGENTENKIT_SECRET_FOR_TEST = 'do-not-leak';
    try {
      const s = await new LocalSandbox({ rootDir: temp('local-env') }).create({ timeoutMs: 60_000, metadata: { threadId: 't' } });
      expect((await s.runCommand('echo "[$AGENTENKIT_SECRET_FOR_TEST]"')).stdout).toBe('[]\n');
      await s.destroy();
    } finally {
      delete process.env.AGENTENKIT_SECRET_FOR_TEST;
    }
  });

  it('LocalSandbox ends a sandbox past its time', async () => {
    const local = new LocalSandbox({ rootDir: temp('local-expiry') });
    const s = await local.create({ timeoutMs: 50, metadata: { threadId: 't' } });
    await new Promise((r) => setTimeout(r, 80));
    await expect(local.connect(s.sandboxId)).rejects.toThrow('is gone');
  });
});
