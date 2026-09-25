import { describe, expect, it } from 'bun:test';
import { simulateReadableStream } from 'ai';
import type { LanguageModelV1StreamPart } from '@ai-sdk/provider';
import { MockLanguageModelV1 } from 'ai/test';
import { setupAgentCore } from '../src/runtime.js';
import { MemoryAdminStore } from '../src/admin/memory.js';
import { MemoryBus, MemoryKv, MemoryQueue, MemoryRunStreams, MemoryStorage } from '../src/adapters/memory.js';
import { followFrame, formatCursor, parseCursor, type FollowFrame } from '../src/core/follow.js';
import { openSegment } from '../src/core/segment.js';
import { resolveConfig } from '../src/core/types.js';

// Workstream S6: a follow reads the thread record and its run streams as one
// sequence. The Go package runs the same cases under the same names
// (run_follow_test.go).

function textModel(...replies: string[]) {
  let n = 0;
  return new MockLanguageModelV1({
    provider: 'mock',
    modelId: 'mock',
    doStream: async () => {
      const chunks: LanguageModelV1StreamPart[] = [
        { type: 'text-delta', textDelta: replies[Math.min(n++, replies.length - 1)]! },
        { type: 'finish', finishReason: 'stop', usage: { promptTokens: 10, completionTokens: 5 } },
      ];
      return { stream: simulateReadableStream({ chunks }), rawCall: { rawPrompt: null, rawSettings: {} } };
    },
  });
}

async function makeRuntime(model: any) {
  const queue = new MemoryQueue();
  const bus = new MemoryBus();
  const streams = new MemoryRunStreams();
  const runtime = await setupAgentCore({
    storage: new MemoryStorage(), admin: new MemoryAdminStore(), bus, queue, kv: new MemoryKv(), streams,
    resolveModel: () => ({ instance: () => model, contextWindow: 128_000 }),
    config: resolveConfig({ streamFlushMs: 0 }),
  });
  const chat = runtime.createStreamTextAgent({ name: 'chat', model: 'gpt-4o' });
  return { runtime, queue, bus, streams, chat };
}

/** Read frames until `done` holds, or fail after a second. */
async function readUntil(gen: AsyncGenerator<FollowFrame>, done: (frames: FollowFrame[]) => boolean) {
  const frames: FollowFrame[] = [];
  const timeout = setTimeout(() => void gen.return(undefined as never), 1000);
  for await (const frame of gen) {
    frames.push(frame);
    if (done(frames)) break;
  }
  clearTimeout(timeout);
  await gen.return(undefined as never);
  return frames;
}

const texts = (frames: FollowFrame[]) =>
  frames.flatMap((f) => (f.kind === 'stream' && f.item.type === 'TEXT_MESSAGE_CONTENT' ? [f.item.delta] : []));
const ends = (frames: FollowFrame[]) =>
  frames.filter((f) => f.kind === 'stream' && f.item.type === 'RUN_FINISHED').length;

describe('run follow', () => {
  it("follow moves to the next run's stream on its start notice", async () => {
    const r = await makeRuntime(textModel('first', 'second'));
    const ran = await r.chat.run({ prompt: 'a' });
    const gen = r.runtime.events.follow(ran.threadId);
    const reading = readUntil(gen, (f) => ends(f) === 2);
    await new Promise((resolve) => setTimeout(resolve, 10));
    await r.runtime.worker.handleJob(r.queue.items.shift()!);
    await r.chat.run({ threadId: ran.threadId, prompt: 'b' });
    await r.runtime.worker.handleJob(r.queue.items.shift()!);
    const frames = await reading;
    expect(texts(frames)).toEqual(['first', 'second']);
    const streams = [...new Set(frames.flatMap((f) => (f.kind === 'stream' ? [f.streamId] : [])))];
    expect(streams).toHaveLength(2); // one per run
  });

  it('a reconnect with Last-Event-ID resumes inside the stream', async () => {
    const r = await makeRuntime(textModel('x'));
    const seg = (await openSegment(r.runtime.ports(), 't1', 'run1'))!;
    await seg.push([
      { type: 'TEXT_MESSAGE_START', messageId: 'm1', role: 'assistant' },
      { type: 'TEXT_MESSAGE_CONTENT', messageId: 'm1', delta: 'seen ' },
    ]);
    const [, , seen] = (await r.streams.snapshot('run1:1'))!.items;
    await seg.push([{ type: 'TEXT_MESSAGE_CONTENT', messageId: 'm1', delta: 'new' }]);
    await seg.close({ type: 'RUN_FINISHED', status: 'finished' });

    // The browser sends back the id of the last frame it got.
    const gen = r.runtime.events.follow('t1', { cursor: `-1 run1:1 ${seen!.offset}` });
    const frames = await readUntil(gen, (f) => ends(f) === 1);
    expect(texts(frames)).toEqual(['new']);
  });

  it('a reconnect to a gone stream gets one SNAPSHOT with only the newer messages, on the same connection', async () => {
    const r = await makeRuntime(textModel('one', 'two'));
    const ran = await r.chat.run({ prompt: 'a' });
    await r.runtime.worker.handleJob(r.queue.items.shift()!);
    const had = (await r.runtime.getThreadSnapshot(ran.threadId))!.messages.at(-1)!.id;
    const second = await r.chat.run({ threadId: ran.threadId, prompt: 'b' });
    await r.runtime.worker.handleJob(r.queue.items.shift()!);
    await r.streams.delete(`${ran.runId}:1`); // past its grace window

    const gen = r.runtime.events.follow(ran.threadId, {
      cursor: { seq: 0, streamId: `${ran.runId}:1`, offset: '1' },
      lastMessageId: had,
    });
    const frames = await readUntil(gen, (f) => f.some((x) => x.kind === 'snapshot'));
    const snap = frames.find((f) => f.kind === 'snapshot');
    expect(snap).toBeDefined();
    if (snap?.kind !== 'snapshot') throw new Error('no snapshot');
    expect(snap.snapshot.messages.map((m) => m.role)).toEqual(['user', 'assistant']); // run b's only
    expect(snap.snapshot.stream?.streamId).toBe(`${second.runId}:1`);
  });

  it("a cursor naming another thread's stream is never read", async () => {
    const r = await makeRuntime(textModel('secret', 'mine'));
    const other = await r.chat.run({ prompt: 'a' });
    await r.runtime.worker.handleJob(r.queue.items.shift()!);
    const own = await r.chat.run({ prompt: 'b' }); // a thread of its own
    await r.runtime.worker.handleJob(r.queue.items.shift()!);

    const gen = r.runtime.events.follow(own.threadId, { cursor: `0 ${other.runId}:1 1` });
    const frames = await readUntil(gen, (f) => f.some((x) => x.kind === 'snapshot'));
    expect(texts(frames)).not.toContain('secret');
    const snap = frames.find((f) => f.kind === 'snapshot');
    if (snap?.kind !== 'snapshot') throw new Error('no snapshot');
    expect(snap.snapshot.thread.id).toBe(own.threadId); // its own thread instead
  });

  it('the sse id carries the record seq and the stream position', () => {
    const at = { seq: 4 };
    const thread = followFrame(
      { kind: 'thread', event: { threadId: 't', seq: 5, type: 'X', payload: null, createdAt: new Date(0) } }, at,
    );
    expect(thread.startsWith('id: 5 - -\n')).toBe(true);
    const item = followFrame(
      { kind: 'stream', streamId: 'r1:2', item: { type: 'TEXT_MESSAGE_CONTENT', messageId: 'm', delta: 'd', offset: '9' } }, at,
    );
    expect(item.startsWith('id: 5 r1:2 9\n')).toBe(true);
    const notice = followFrame(
      { kind: 'thread', event: { threadId: 't', seq: 0, type: 'HEARTBEAT', payload: null, createdAt: new Date(0) } }, at,
    );
    expect(notice).not.toContain('id:'); // leaves the browser's cursor as it was
  });

  it('a cursor reads its wire form and a bare seq', () => {
    expect(parseCursor('5 r1:2 9')).toEqual({ seq: 5, streamId: 'r1:2', offset: '9' });
    expect(parseCursor('5 - -')).toEqual({ seq: 5 });
    expect(parseCursor('12')).toEqual({ seq: 12 });
    expect(parseCursor('junk')).toBeNull();
    expect(formatCursor({ seq: 3, streamId: 'r:1', offset: '1-0' })).toBe('3 r:1 1-0');
  });

  it('stream content does not go on the bus while a segment is open', async () => {
    const r = await makeRuntime(textModel('hello'));
    const ran = await r.chat.run({ prompt: 'a' });
    await r.runtime.worker.handleJob(r.queue.items.shift()!);
    const onBus = r.bus.published.filter((e) => e.threadId === ran.threadId).map((e) => e.type);
    expect(onBus).not.toContain('CHUNK');
    expect(onBus).not.toContain('STEP_COMMITTED');
    expect(onBus).toContain('STATE_CHANGE'); // thread notices still go out
    expect(onBus).toContain('RUN_STARTED');
  });
});
