import { describe, expect, it } from 'bun:test';
import { setupAgentCore } from '../src/runtime.js';
import { MemoryAdminStore } from '../src/admin/memory.js';
import { MemoryBus, MemoryKv, MemoryQueue, MemoryStorage } from '../src/adapters/memory.js';
import { resolveConfig } from '../src/core/types.js';

// Workstream J: the sending client names its turn, and the name comes back on
// MESSAGE_APPENDED so it swaps its optimistic copy by id. The same case runs
// in the Go package (client_message_test.go).
describe('run (§2.2)', () => {
  it("the client's message id comes back on its turn", async () => {
    const bus = new MemoryBus();
    const runtime = await setupAgentCore({
      storage: new MemoryStorage(), bus, queue: new MemoryQueue(), kv: new MemoryKv(), admin: new MemoryAdminStore(),
      resolveModel: () => ({ instance: () => { throw new Error('no model'); }, contextWindow: 128_000 }),
      config: resolveConfig({}),
    });
    const chat = runtime.createStreamTextAgent({ name: 'chat' });
    const ran = await chat.run({ prompt: 'hi', clientMessageId: 'cm-1' });
    const appended = bus.published.filter((e) => e.threadId === ran.threadId && e.type === 'MESSAGE_APPENDED');
    expect(appended).toHaveLength(1);
    expect((appended[0]!.payload as any).clientMessageId).toBe('cm-1');
  });
});
