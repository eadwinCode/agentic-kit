import { afterEach, beforeEach, describe, expect, it } from 'bun:test';
import { act, cleanup, renderHook, waitFor } from '@testing-library/react';
import { useAgentThread } from '../src/useAgentThread.js';
import { browserPersistence } from '../src/config.js';
import type { ThreadSnapshot } from '../src/types.js';
import type { AgentRunConfigLike } from './helpers.js';
import { harness } from './helpers.js';

// Workstream J: the hook never shows text twice, never mixes two threads,
// and shows every server event it receives.

afterEach(() => {
  cleanup();
  window.localStorage.clear();
});

beforeEach(() => {
  window.history.replaceState({}, '', 'http://localhost/');
});

async function mount(over: AgentRunConfigLike = {}) {
  const h = harness(over);
  let renders = 0;
  const view = renderHook(() => {
    renders++;
    return useAgentThread({ initialThreadId: 't1', ...h.config });
  });
  await waitFor(() => expect(view.result.current.historyLoading).toBe(false));
  return { ...h, view, renders: () => renders };
}

const running: ThreadSnapshot = {
  thread: { id: 't1', state: 'RUNNING' },
  messages: [{ id: 'm1', role: 'user', content: 'go', agentId: null }],
  runs: [{ id: 'r1', agent: 'chat', depth: 0, state: 'RUNNING', startedAt: '2026-09-05T10:00:00Z' }],
  lastEventSeq: 4,
  activeEvents: [],
};

const json = (body: unknown, status = 200) =>
  new Response(JSON.stringify(body), { status, headers: { 'Content-Type': 'application/json' } });

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

describe('event order (J1)', () => {
  it('drops an event it already has', async () => {
    const { view, emit } = await mount();
    await act(async () => {
      emit({ seq: 8, type: 'CHUNK', payload: { type: 'text-delta', textDelta: 'once' } });
      emit({ seq: 8, type: 'CHUNK', payload: { type: 'text-delta', textDelta: 'once' } }); // resent
      emit({ seq: 5, type: 'CHUNK', payload: { type: 'text-delta', textDelta: 'old' } }); // the snapshot had it
    });
    await waitFor(() => expect(view.result.current.entries.at(-1)!.text).toBe('once'));
  });

  it('hands a custom transport the cursor to resume from', async () => {
    const { streams, emit } = await mount();
    expect(streams[0]!.handlers.getCursor()).toBe(7); // the snapshot's
    await act(async () => {
      emit({ seq: 9, type: 'STATE_CHANGE', payload: { state: 'RUNNING' } });
    });
    expect(streams[0]!.handlers.getCursor()).toBe(9);
  });
});

describe('sending (J2, J9, J12)', () => {
  it('refuses a second run while one is going', async () => {
    const { view, calls } = await mount({ snapshot: running });
    const before = view.result.current.entries;
    let result: any;
    await act(async () => {
      result = await view.result.current.run('another');
    });
    expect(result.accepted).toBe(false);
    expect(view.result.current.entries).toEqual(before); // the run's cards and turns stay
    expect(view.result.current.error).toBeTruthy();
    expect(calls.some((c) => c.url.includes('/api/agent/run'))).toBe(false);
  });

  it('sends one request for two fast sends', async () => {
    const { view, calls } = await mount();
    await act(async () => {
      void view.result.current.run('one');
      void view.result.current.run('two');
    });
    expect(calls.filter((c) => c.url.includes('/api/agent/run'))).toHaveLength(1);
  });

  it('sends no model unless asked', async () => {
    const { view, calls } = await mount();
    await act(async () => {
      await view.result.current.run('hi');
    });
    const body = JSON.parse(String(calls.find((c) => c.url.includes('/api/agent/run'))!.init?.body));
    expect('model' in body).toBe(false); // the server uses the agent's own model
  });

  it('swaps its optimistic turn by the id it sent, not by text', async () => {
    const { view, calls, emit } = await mount();
    await act(async () => {
      await view.result.current.run('same words');
    });
    const sent = JSON.parse(String(calls.find((c) => c.url.includes('/api/agent/run'))!.init?.body));
    expect(typeof sent.clientMessageId).toBe('string');
    await act(async () => {
      // Another tab said the same words first: that is a turn of its own.
      emit({ seq: 8, type: 'MESSAGE_APPENDED', payload: { id: 'other', role: 'user', content: 'same words', clientMessageId: 'theirs' } });
      emit({ seq: 9, type: 'MESSAGE_APPENDED', payload: { id: 'mine', role: 'user', content: 'same words', clientMessageId: sent.clientMessageId } });
    });
    const ids = view.result.current.entries.filter((e) => e.text === 'same words').map((e) => e.id);
    expect(ids).toEqual(['mine', 'other']);
  });

  it('checks the status before reading the body', async () => {
    const h = harness();
    const fetch = (input: RequestInfo | URL, init?: RequestInit) =>
      String(input).includes('/api/agent/run')
        ? Promise.resolve(new Response('<html>Bad gateway</html>', { status: 502 }))
        : h.config.fetch!(input, init);
    const { view } = await mount({ fetch });
    let result: any;
    await act(async () => {
      result = await view.result.current.run('hi');
    });
    expect(result.error).toContain('502');
  });

  it('refuses an edit of a turn the server has not confirmed', async () => {
    const { view, calls } = await mount();
    let result: any;
    await act(async () => {
      result = await view.result.current.run('fixed', { editMessageId: 'optimistic:user:cm-1' });
    });
    expect(result.accepted).toBe(false);
    expect(calls.some((c) => c.url.includes('/api/agent/run'))).toBe(false);
  });
});

describe('switching threads (J3, J4)', () => {
  it("clears the old thread's view before the new one loads", async () => {
    const h = harness();
    let release!: () => void;
    const gate = new Promise<void>((resolve) => { release = resolve; });
    const fetch = async (input: RequestInfo | URL, init?: RequestInit) => {
      if (String(input).includes('threadId=t2')) await gate;
      return h.config.fetch!(input, init);
    };
    const { view } = await mount({ fetch });
    expect(view.result.current.entries).toHaveLength(2);
    await act(async () => {
      view.result.current.selectThread('t2');
    });
    expect(view.result.current.entries).toEqual([]); // not t1's messages under t2
    release();
  });

  it('a slow 404 from the thread it left does not touch the new one', async () => {
    const h = harness();
    let release!: () => void;
    const gate = new Promise<void>((resolve) => { release = resolve; });
    let first = true;
    const fetch = async (input: RequestInfo | URL, init?: RequestInit) => {
      if (String(input).includes('history') && String(input).includes('threadId=t1') && first) {
        first = false;
        await gate;
        return json({ error: 'gone' }, 404);
      }
      return h.config.fetch!(input, init);
    };
    const view = renderHook(() => useAgentThread({ initialThreadId: 't1', ...h.config, fetch }));
    await act(async () => {
      view.result.current.selectThread('t2');
    });
    await waitFor(() => expect(view.result.current.historyLoading).toBe(false));
    await act(async () => {
      release();
      await sleep(10);
    });
    expect(view.result.current.threadId).toBe('t2');
    expect(view.result.current.entries.length).toBeGreaterThan(0);
  });
});

describe('the connection (J5)', () => {
  it('says where the stream stands, and ignores a frame that is not an event', async () => {
    const { view, streams } = await mount();
    expect(view.result.current.connection).toBe('open');
    await act(async () => {
      streams[0]!.handlers.onError(new Error('blip'));
    });
    expect(view.result.current.connection).toBe('reconnecting');
    await act(async () => {
      streams[0]!.handlers.onMessage('not json');
      streams[0]!.handlers.onOpen?.();
    });
    expect(view.result.current.connection).toBe('open');
  });

  it('reads the thread again and reopens after the stream closes for good', async () => {
    const { view, streams, calls } = await mount();
    const reads = () => calls.filter((c) => c.url.includes('history')).length;
    const before = reads();
    await act(async () => {
      streams[0]!.handlers.onClose?.();
    });
    expect(view.result.current.connection).toBe('closed');
    await waitFor(() => expect(streams).toHaveLength(2), { timeout: 3_000 });
    expect(reads()).toBe(before + 1);
    expect(streams[1]!.url).toContain('since=7');
    expect(view.result.current.connection).toBe('open');
  });
});

describe('server events (J6, J7, J8)', () => {
  it('shows a generate-text answer', async () => {
    const { view, emit } = await mount();
    await act(async () => {
      emit({ seq: 8, type: 'TEXT_RESULT', payload: { text: 'the whole answer' } });
    });
    expect(view.result.current.entries.at(-1)!.text).toBe('the whole answer');
  });

  it('starts afresh when the thread is deleted', async () => {
    const { view, emit } = await mount();
    await act(async () => {
      emit({ seq: 0, type: 'THREAD_DELETED', payload: { threadId: 't1' } });
    });
    expect(view.result.current.threadId).toBeUndefined();
    expect(view.result.current.entries).toEqual([]);
  });

  it('drops a card answered in another tab', async () => {
    const { view, emit } = await mount();
    await act(async () => {
      emit({ seq: 8, type: 'INPUT_REQUIRED', payload: { toolCallId: 'c1', toolName: 'wipe', reason: 'approval' } });
    });
    expect(view.result.current.pendingInputs).toHaveLength(1);
    await act(async () => {
      emit({ seq: 0, type: 'HITL_RESPONSE', payload: { toolCallId: 'c1', approved: true } });
    });
    expect(view.result.current.pendingInputs).toHaveLength(0);
  });

  it('shows a refusal and a failure reason', async () => {
    const { view, emit } = await mount();
    await act(async () => {
      emit({ seq: 8, type: 'RUN_REFUSED', payload: { reason: 'billing', error: 'Out of credits' } });
    });
    expect(view.result.current.error).toBe('Out of credits');
    await act(async () => {
      emit({ seq: 9, type: 'STATE_CHANGE', payload: { state: 'FAILED', error: 'provider down' } });
    });
    expect(view.result.current.activity.detail).toBe('provider down');
  });

  it("starts the clock from the frame's time when the event carries none", async () => {
    const { view, emit } = await mount();
    await act(async () => {
      emit({ seq: 8, type: 'STATE_CHANGE', payload: { state: 'RUNNING', runId: 'r9' }, createdAt: '2026-09-05T11:00:00Z' });
    });
    expect(view.result.current.currentRun).toEqual({ id: 'r9', startedAt: '2026-09-05T11:00:00Z' });
  });

  it("keeps a work park's label when its state change follows", async () => {
    const { view, emit } = await mount({ snapshot: running });
    await act(async () => {
      emit({ seq: 5, type: 'INPUT_REQUIRED', payload: { toolCallId: 'c1', toolName: 'build', reason: 'work' } });
      emit({ seq: 6, type: 'STATE_CHANGE', payload: { state: 'WAITING_FOR_INPUT', runId: 'r1' } });
    });
    expect(view.result.current.activity.label).toBe('Waiting for work to finish');
  });
});

describe('rendering (J10, J11)', () => {
  it('shows many deltas in few renders', async () => {
    const { view, emit, renders } = await mount({ snapshot: running });
    const before = renders();
    await act(async () => {
      for (let i = 0; i < 50; i++) {
        emit({ seq: 5 + i, type: 'CHUNK', payload: { type: 'text-delta', textDelta: 'x' } });
      }
    });
    await waitFor(() => expect(view.result.current.entries.at(-1)!.text).toBe('x'.repeat(50)));
    expect(renders() - before).toBeLessThan(10);
  });

  it("keeps the router's own history state", () => {
    window.history.replaceState({ router: 'tree' }, '', 'http://localhost/');
    const p = browserPersistence();
    p.save('t9');
    expect(window.history.state).toEqual({ router: 'tree' });
    expect(window.location.search).toContain('threadId=t9');
    p.clear();
    expect(window.history.state).toEqual({ router: 'tree' });
  });
});

describe('the thread list (J12)', () => {
  it('only the latest answer lands', async () => {
    const h = harness();
    let n = 0;
    const fetch = async (input: RequestInfo | URL, init?: RequestInit) => {
      if (String(input).endsWith('/api/threads')) {
        const mine = ++n;
        await sleep(mine === 1 ? 60 : 5); // the first answer is the slow one
        return json({ threads: [{ id: `list-${mine}`, title: '', state: 'IDLE', model: 'm', updatedAt: '' }] });
      }
      return h.config.fetch!(input, init);
    };
    const { view } = await mount({ fetch });
    await act(async () => {
      void view.result.current.loadThreads();
      void view.result.current.loadThreads();
      await sleep(100);
    });
    expect(view.result.current.threads.map((t) => t.id)).toEqual(['list-2']);
  });
});
