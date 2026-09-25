import { afterEach, beforeEach, describe, expect, it } from 'bun:test';
import { act, cleanup, renderHook, waitFor } from '@testing-library/react';
import { useAgentThread } from '../src/useAgentThread.js';
import type { ThreadSnapshot } from '../src/types.js';
import type { AgentRunConfigLike } from './helpers.js';
import { harness } from './helpers.js';

// Workstream S7: the hook reads a thread's record and its run streams as the
// server's follow sends them (thread, stream and snapshot frames).

afterEach(() => {
  cleanup();
  window.localStorage.clear();
});

beforeEach(() => {
  window.history.replaceState({}, '', 'http://localhost/');
});

async function mount(over: AgentRunConfigLike = {}) {
  const h = harness(over);
  const view = renderHook(() => useAgentThread({ initialThreadId: 't1', ...h.config }));
  await waitFor(() => expect(view.result.current.historyLoading).toBe(false));
  return { ...h, view };
}

const text = (streamId: string, offset: string, delta: string) => ({
  kind: 'stream', streamId, item: { type: 'TEXT_MESSAGE_CONTENT', messageId: 'x', delta, offset },
});

const running: ThreadSnapshot = {
  thread: { id: 't1', state: 'RUNNING' },
  messages: [{ id: 'm1', role: 'user', content: 'go', agentId: null }],
  runs: [{ id: 'r1', agent: 'chat', depth: 0, state: 'RUNNING', startedAt: '2026-09-05T10:00:00Z' }],
  lastEventSeq: 4,
  activeEvents: [],
  stream: {
    streamId: 'r1:1',
    runId: 'r1',
    items: [
      { type: 'RUN_STARTED', offset: '1' },
      { type: 'TEXT_MESSAGE_START', messageId: 'x', role: 'assistant', offset: '2' },
      { type: 'TEXT_MESSAGE_CONTENT', messageId: 'x', delta: 'Hel', offset: '3' },
    ],
    end: null,
    offset: '3',
  },
};

describe('run streams in the hook', () => {
  it('hydrates the text in flight from the snapshot stream and connects after it', async () => {
    const { view, streams } = await mount({ snapshot: running });
    await waitFor(() => expect(view.result.current.entries.at(-1)!.text).toBe('Hel'));
    expect(streams[0]!.url).toContain('cursor=4%20r1%3A1%203');
    expect(streams[0]!.url).toContain('lastMessageId=m1');
    expect(streams[0]!.handlers.getCursor()).toBe('4 r1:1 3');
  });

  it('items already applied in the same stream are dropped', async () => {
    const { view, emit } = await mount({ snapshot: running });
    await act(async () => {
      emit(text('r1:1', '3', 'Hel')); // the snapshot had it
      emit(text('r1:1', '4', 'lo'));
      emit(text('r1:1', '4', 'lo')); // resent
    });
    await waitFor(() => expect(view.result.current.entries.at(-1)!.text).toBe('Hello'));
  });

  it('a new stream id starts a fresh cursor', async () => {
    const { view, emit, streams } = await mount({ snapshot: running });
    await act(async () => {
      emit({ kind: 'stream', streamId: 'r1:1', item: { type: 'RUN_FINISHED', status: 'parked', offset: '4' } });
      emit(text('r1:2', '1', 'lo')); // offset 1 again, but a new stream
    });
    await waitFor(() => expect(view.result.current.entries.at(-1)!.text).toBe('Hello'));
    expect(streams[0]!.handlers.getCursor()).toBe('4 r1:2 1');
  });

  it('tool calls and a subagent arrive from stream items', async () => {
    const { view, emit } = await mount({ snapshot: running });
    await act(async () => {
      emit({ kind: 'stream', streamId: 'r1:1', item: { type: 'TOOL_CALL_END', toolCallId: 'c1', toolName: 'look', args: { q: 1 }, offset: '4' } });
      emit({ kind: 'stream', streamId: 'r1:1', item: { type: 'TOOL_CALL_RESULT', toolCallId: 'c1', toolName: 'look', result: 'seen', offset: '5' } });
      emit({ kind: 'stream', streamId: 'r1:1', item: { type: 'SUBAGENT_STARTED', subagentId: 'sub_1', name: 'helper', depth: 1, offset: '6' } });
      emit({
        kind: 'stream', streamId: 'r1:1',
        item: { type: 'SUBAGENT_EVENT', subagentId: 'sub_1', event: { type: 'TEXT_MESSAGE_CONTENT', messageId: 'y', delta: 'working' }, offset: '7' },
      });
      emit({ kind: 'stream', streamId: 'r1:1', item: { type: 'SUBAGENT_FINISHED', subagentId: 'sub_1', status: 'completed', offset: '8' } });
    });
    await waitFor(() => {
      const r = view.result.current;
      const call = r.entries.flatMap((e) => e.parts ?? []).find((p: any) => p.toolCallId === 'c1') as any;
      expect(call).toBeDefined();
      expect(r.subagents[0]).toMatchObject({ agentId: 'sub_1', name: 'helper', status: 'COMPLETED' });
      expect(r.subagents[0]!.text).toContain('working');
    });
  });

  it('a SNAPSHOT merges in place and shows the text once', async () => {
    const { view, emit } = await mount({ snapshot: running });
    await act(async () => {
      emit({
        kind: 'snapshot',
        snapshot: {
          thread: { id: 't1', state: 'COMPLETED' },
          // Only the newer message: the user turn is already on screen.
          messages: [{ id: 'm2', role: 'assistant', content: [{ type: 'text', text: 'Hello' }], agentId: null }],
          runs: [],
          lastEventSeq: 6,
          activeEvents: [],
          stream: null,
        },
      });
    });
    await waitFor(() => {
      const entries = view.result.current.entries;
      expect(entries.map((e) => e.role)).toEqual(['user', 'assistant']);
      expect(entries.at(-1)!.text).toBe('Hello'); // not "HelHello"
      expect(view.result.current.agentState).toBe('COMPLETED');
    });
  });

  it('CUSTOM events reach onCustom once, in order', async () => {
    const seen: Array<[string, unknown]> = [];
    const { emit } = await mount({ snapshot: running, onCustom: (name, value) => seen.push([name, value]) });
    await act(async () => {
      emit({ kind: 'stream', streamId: 'r1:1', item: { type: 'CUSTOM', name: 'PROGRESS', value: { pct: 10 }, offset: '4' } });
      emit({ kind: 'stream', streamId: 'r1:1', item: { type: 'CUSTOM', name: 'PROGRESS', value: { pct: 10 }, offset: '4' } }); // resent
      emit({ kind: 'thread', event: { seq: 5, type: 'INVOICE_CREATED', payload: { id: 'inv_1' } } }); // durable
      emit({ kind: 'stream', streamId: 'r1:1', item: { type: 'CUSTOM', name: 'PROGRESS', value: { pct: 90 }, offset: '5' } });
    });
    expect(seen).toEqual([
      ['PROGRESS', { pct: 10 }],
      ['INVOICE_CREATED', { id: 'inv_1' }],
      ['PROGRESS', { pct: 90 }],
    ]);
  });
});
