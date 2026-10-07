import { describe, expect, it } from 'bun:test';
import { simulateReadableStream, tool } from 'ai';
import type { LanguageModelV1StreamPart } from './v1-mock.js';
import { MockLanguageModelV1 } from './v1-mock.js';
import { z } from 'zod';
import { setupAgentCore } from '../src/runtime.js';
import { MemoryAdminStore } from '../src/admin/memory.js';
import { MemoryBus, MemoryKv, MemoryQueue, MemorySandbox, MemoryStorage } from '../src/adapters/memory.js';
import { forgetSandboxHandles, sandboxKey, withSandbox, type ThreadSandbox } from '../src/core/builtin/sandbox.js';
import { parkForInput } from '../src/core/hitl.js';
import { resolveConfig, type AgentConfig } from '../src/core/types.js';
import type { SandboxCall } from '../src/ports/tools.js';
import type { SandboxProvider } from '../src/ports/sandbox.js';

// A sandbox per run, a provider that restores the work folder, the app's
// create options and the hook after each sandbox call. The same cases run in
// the Go package (sandbox_run_scope_test.go), under the same names.

const IDLE = 30 * 60_000;

type Call = { id: string; name: string; args: Record<string, unknown> };
type Step = Call[] | string;

const touch = (id: string, name: string): Call[] => [{ id, name: 'touch', args: { name } }];

/** One step per model call, across runs: calls, or a text answer. */
function stepsModel(steps: Step[]) {
  let i = 0;
  return new MockLanguageModelV1({
    provider: 'mock',
    modelId: 'mock',
    doStream: async () => {
      const step = steps[Math.min(i++, steps.length - 1)]!;
      const chunks: LanguageModelV1StreamPart[] = typeof step === 'string'
        ? [{ type: 'text-delta', textDelta: step }]
        : step.map((c) => ({ type: 'tool-call', toolCallType: 'function', toolCallId: c.id, toolName: c.name, args: JSON.stringify(c.args) }));
      chunks.push({
        type: 'finish', finishReason: typeof step === 'string' ? 'stop' : 'tool-calls', usage: { promptTokens: 1, completionTokens: 1 },
      });
      return { stream: simulateReadableStream({ chunks }), rawCall: { rawPrompt: null, rawSettings: {} } };
    },
  });
}

async function harness(
  steps: Step[],
  config: Partial<AgentConfig> = {},
  tools: { sandbox?: SandboxProvider; afterSandboxCall?: (c: SandboxCall) => void | Promise<void> } = {},
) {
  forgetSandboxHandles();
  const sandboxes = new MemorySandbox();
  const kv = new MemoryKv();
  const queue = new MemoryQueue();
  const storage = new MemoryStorage();
  const seen: ThreadSandbox[] = [];
  const model = stepsModel(steps);
  const runtime = await setupAgentCore({
    storage, bus: new MemoryBus(), queue, kv,
    admin: new MemoryAdminStore(),
    tools: { sandbox: tools.sandbox ?? sandboxes, ...(tools.afterSandboxCall ? { afterSandboxCall: tools.afterSandboxCall } : {}) },
    resolveModel: () => ({ instance: () => model, contextWindow: 128_000 }),
    config: resolveConfig({ stopPollMs: 5, promptCaching: false, hitlTtlMs: 60 * 60_000, ...config }),
  });
  const agent = runtime.createStreamTextAgent({
    name: 'coder',
    model: 'gpt-4o',
    tools: {
      touch: tool({
        description: 'Write a file in the sandbox',
        inputSchema: z.object({ name: z.string() }),
        execute: (args, opts) =>
          withSandbox(opts, async (ts) => {
            await ts.sandbox.filesystem.writeFile(`${args.name}.txt`, 'x');
            seen.push(ts);
            return 'ok';
          }),
      }),
      wait: tool({
        description: 'Wait for a job',
        inputSchema: z.object({}),
        execute: async (_args, opts: any): Promise<string> => {
          if (opts.approval) return 'answered';
          throw parkForInput({ reason: 'job' });
        },
      }),
      plain: tool({
        description: 'No sandbox',
        inputSchema: z.object({}),
        execute: async () => 'no sandbox',
      }),
    },
  });
  const run = async (threadId?: string) => {
    const r = await agent.run({ prompt: 'go', ...(threadId ? { threadId } : {}) });
    await runtime.worker.handleJob(queue.items.shift()!);
    return r.threadId;
  };
  const respond = async (threadId: string, toolCallId: string) => {
    queue.items.shift(); // the park's expiry job; the answer comes first
    const res = await runtime.hitl.respond({ threadId, toolCallId, approved: true });
    expect(res.delivered).toBe(true);
    while (queue.items.length) await runtime.worker.handleJob(queue.items.shift()!);
  };
  const state = async (threadId: string) => (await storage.threads.get(threadId))!.state;
  return { runtime, sandboxes, kv, storage, seen, run, respond, state };
}

const RUN: Partial<AgentConfig> = { sandboxScope: 'run' };

describe('a sandbox per run', () => {
  it("a run's sandbox ends with the run and the next run starts fresh", async () => {
    const h = await harness([touch('c1', 'a'), 'done', touch('c2', 'b'), 'done'], RUN);
    const threadId = await h.run();
    expect(h.sandboxes.destroyed).toEqual(['mem-1']);
    expect(await h.kv.get(sandboxKey(threadId))).toBeNull();

    await h.run(threadId);
    expect(h.seen[1]!.sandbox.sandboxId).toBe('mem-2');
    expect(h.seen[1]!.lost).toBe(false); // a new run's fresh sandbox is not a loss
    expect(h.sandboxes.destroyed).toEqual(['mem-1', 'mem-2']);
  });

  it('a park gives the sandbox back and the resumed run is told its files are gone', async () => {
    const h = await harness([touch('c1', 'a'), [{ id: 'c2', name: 'wait', args: {} }], touch('c3', 'b'), 'done'], RUN);
    const threadId = await h.run();
    expect(await h.state(threadId)).toBe('WAITING_FOR_INPUT');
    expect(h.sandboxes.destroyed).toEqual(['mem-1']);

    await h.respond(threadId, 'c2');
    expect(h.seen[1]!.sandbox.sandboxId).toBe('mem-2');
    expect(h.seen[1]!.lost).toBe(true);
    expect(await h.state(threadId)).toBe('COMPLETED');
    expect(h.sandboxes.destroyed).toEqual(['mem-1', 'mem-2']);
  });

  it('sandboxKeepOnPark keeps it through the park', async () => {
    const h = await harness(
      [touch('c1', 'a'), [{ id: 'c2', name: 'wait', args: {} }], touch('c3', 'b'), 'done'],
      { ...RUN, sandboxKeepOnPark: true },
    );
    const threadId = await h.run();
    expect(h.sandboxes.destroyed).toEqual([]);
    await h.respond(threadId, 'c2');
    expect(h.seen[1]!.sandbox.sandboxId).toBe('mem-1');
    expect(h.seen[1]!.lost).toBe(false);
    expect(h.sandboxes.destroyed).toEqual(['mem-1']);
  });

  it('a sandbox an earlier run left is ended by the next run', async () => {
    const h = await harness([touch('c1', 'a'), 'done', touch('c2', 'b'), 'done'], RUN);
    const threadId = await h.run();
    // As a crash would leave it: a live sandbox, recorded for another run.
    const left = await h.sandboxes.create({ timeoutMs: 1, metadata: { threadId } });
    await h.kv.set(sandboxKey(threadId), JSON.stringify({
      id: left.sandboxId, provider: h.sandboxes.name, createdAt: Date.now(), lastUsedAt: Date.now(), runId: 'old-run',
    }));
    await h.run(threadId);
    expect(h.seen[1]!.lost).toBe(false);
    expect(h.sandboxes.destroyed).toEqual(['mem-1', left.sandboxId, 'mem-3']);
  });

  it('the thread scope is the default and keeps the sandbox', async () => {
    const h = await harness([touch('c1', 'a'), 'done']);
    const threadId = await h.run();
    expect(h.sandboxes.destroyed).toEqual([]);
    expect(await h.kv.get(sandboxKey(threadId))).not.toBeNull();
  });

  it('a provider that restores the work folder marks the new sandbox restored', async () => {
    const sandboxes = new MemorySandbox();
    const restoring = Object.assign(Object.create(sandboxes), { restoresWorkdir: true }) as SandboxProvider;
    const h = await harness([touch('c1', 'a'), 'done', touch('c2', 'b'), 'done'], {}, { sandbox: restoring });
    const threadId = await h.run();
    sandboxes.end('mem-1');
    forgetSandboxHandles();
    await h.run(threadId);
    expect(h.seen[1]!.lost).toBe(true);
    expect(h.seen[1]!.restored).toBe(true);
  });

  it("the app's create options reach every sandbox", async () => {
    const h = await harness([touch('c1', 'a'), 'done'], {
      sandboxDefaults: { network: 'all', envs: { CI: '1' }, template: 'node', timeoutMs: 1 } as any,
    });
    const threadId = await h.run();
    expect(h.sandboxes.created).toHaveLength(1);
    const made = h.sandboxes.created[0]!;
    expect(made.network).toBe('all');
    expect(made.envs).toEqual({ CI: '1' });
    expect(made.template).toBe('node');
    expect(made.timeoutMs).toBe(IDLE + 60_000); // the runtime's own wins
    expect(made.metadata.threadId).toBe(threadId);
  });

  it('afterSandboxCall runs once after each call that used the sandbox', async () => {
    const calls: SandboxCall[] = [];
    const h = await harness(
      [[{ id: 'c1', name: 'touch', args: { name: 'a' } }, { id: 'c2', name: 'plain', args: {} }], 'done'],
      {},
      { afterSandboxCall: (c) => { calls.push(c); } },
    );
    const threadId = await h.run();
    expect(calls).toHaveLength(1);
    expect(calls[0]!.toolName).toBe('touch');
    expect(calls[0]!.toolCallId).toBe('c1');
    expect(calls[0]!.threadId).toBe(threadId);
    expect(calls[0]!.sandbox.sandboxId).toBe('mem-1');
    expect(calls[0]!.error).toBeUndefined();
  });

  it("an error from afterSandboxCall reaches the model as the call's error", async () => {
    const h = await harness([touch('c1', 'a'), 'done'], {}, {
      afterSandboxCall: () => { throw new Error('your change was not saved: disk full'); },
    });
    const threadId = await h.run();
    const results = h.storage.messages.store.get(threadId)!
      .flatMap((m) => (Array.isArray(m.content) ? m.content : []))
      .filter((p: any) => p.type === 'tool-result' && p.toolCallId === 'c1');
    expect(JSON.stringify(results)).toContain('your change was not saved: disk full');
  });
});
