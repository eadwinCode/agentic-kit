import { afterEach, beforeEach, describe, expect, it } from 'bun:test';
import { act, cleanup, renderHook, waitFor } from '@testing-library/react';
import { useAgentThread } from '../src/useAgentThread.js';
import type { AgentRunConfigLike } from './helpers.js';
import { harness } from './helpers.js';

afterEach(() => {
  cleanup();
  window.localStorage.clear();
});

beforeEach(() => {
  window.history.replaceState({}, '', 'http://localhost/');
});

/** Mount the hook against fakes, and wait until hydration has finished. */
async function mount(over: AgentRunConfigLike = {}) {
  const h = harness(over);
  const view = renderHook(() => useAgentThread({ initialThreadId: 't1', ...h.config }));
  await waitFor(() => expect(view.result.current.historyLoading).toBe(false));
  return { ...h, view };
}

describe('hydration (§2.2)', () => {
  it('renders the durable history, then opens the stream at the snapshot cursor', async () => {
    const { view, streams, calls } = await mount();

    expect(view.result.current.entries.map((e) => e.text)).toEqual(['hello', 'hi there']);
    expect(view.result.current.agentState).toBe('COMPLETED');

    // The stream must resume AFTER what the snapshot already contained, or the
    // client renders those events twice.
    expect(streams).toHaveLength(1);
    expect(streams[0]!.url).toContain('since=7');
    expect(calls.some((c) => c.url.includes('/api/agent/history'))).toBe(true);
  });

  it('replays the active run so an in-flight answer is not lost', async () => {
    const { view } = await mount({
      snapshot: {
        thread: { id: 't1', state: 'RUNNING' },
        messages: [{ id: 'm1', role: 'user', content: 'count', agentId: null }],
        runs: [],
        lastEventSeq: 4,
        activeEvents: [
          { seq: 3, type: 'STATE_CHANGE', payload: { state: 'RUNNING' } },
          { seq: 4, type: 'CHUNK', payload: { type: 'text-delta', textDelta: 'one ' } },
        ],
      },
    });

    expect(view.result.current.agentState).toBe('RUNNING');
    expect(view.result.current.entries.at(-1)!.text).toBe('one ');
  });

  it('forgets a thread the server no longer has', async () => {
    const { view } = await mount({ historyStatus: 404 });
    expect(view.result.current.threadId).toBeUndefined();
    expect(view.result.current.entries).toEqual([]);
    expect(view.result.current.agentState).toBe('IDLE');
  });
});

describe('the live stream (§2.2)', () => {
  it('accumulates text deltas into one assistant entry', async () => {
    const { view, emit } = await mount();
    const before = view.result.current.entries.length;

    await act(async () => {
      emit({ seq: 8, type: 'CHUNK', payload: { type: 'text-delta', textDelta: 'par' } });
      emit({ seq: 9, type: 'CHUNK', payload: { type: 'text-delta', textDelta: 'tial' } });
    });

    expect(view.result.current.entries).toHaveLength(before + 1);
    expect(view.result.current.entries.at(-1)!.text).toBe('partial');
    expect(view.result.current.activity.phase).toBe('responding');
  });

  // Reasoning streams separately from the answer so a UI can fold it away.
  it("keeps the model's thinking in its own entry", async () => {
    const { view, emit } = await mount();

    await act(async () => {
      emit({ seq: 8, type: 'CHUNK', payload: { type: 'reasoning', textDelta: 'let me think. ' } });
      emit({ seq: 9, type: 'CHUNK', payload: { type: 'reasoning', textDelta: 'and again.' } });
      emit({ seq: 10, type: 'CHUNK', payload: { type: 'text-delta', textDelta: 'the answer' } });
    });

    const tail = view.result.current.entries.slice(-2);
    expect(tail.map((e) => e.kind)).toEqual(['reasoning', 'text']);
    expect(tail[0]!.text).toBe('let me think. and again.');
    expect(tail[1]!.text).toBe('the answer');
  });

  it('shows a tool call once, even when the snapshot already had it', async () => {
    const { view, emit } = await mount();

    await act(async () => {
      emit({ seq: 8, type: 'CHUNK', payload: { type: 'tool-call', toolCallId: 'c1', toolName: 'lookup', args: { q: 'x' } } });
      // A redelivery of the same call must not render a second time.
      emit({ seq: 9, type: 'CHUNK', payload: { type: 'tool-call', toolCallId: 'c1', toolName: 'lookup', args: { q: 'x' } } });
    });

    expect(view.result.current.entries.filter((e) => e.kind === 'tool')).toHaveLength(1);
  });

  // The park sentinel is an internal marker, never a result.
  it('never renders the HITL park sentinel as a tool result', async () => {
    const { view, emit } = await mount();
    const before = view.result.current.entries.length;

    await act(async () => {
      emit({
        seq: 8,
        type: 'CHUNK',
        payload: { type: 'tool-result', toolCallId: 'c9', toolName: 'sendEmail', result: { __hitl_parked__: true } },
      });
    });

    expect(view.result.current.entries).toHaveLength(before);
  });

  it('closes the stream when the component unmounts', async () => {
    const { view, streams } = await mount();
    expect(streams[0]!.closed).toBe(false);
    view.unmount();
    expect(streams[0]!.closed).toBe(true);
  });
});

describe('several clients on one thread (§2.2)', () => {
  it('adds a message another client sent', async () => {
    const { view, emit } = await mount();

    await act(async () => {
      emit({
        seq: 8,
        type: 'MESSAGE_APPENDED',
        payload: { id: 'm9', role: 'user', content: 'from the other tab', agentId: null },
      });
    });

    expect(view.result.current.entries.at(-1)).toMatchObject({
      id: 'm9',
      role: 'user',
      text: 'from the other tab',
    });
  });

  it('replaces its own optimistic copy rather than showing it twice', async () => {
    const { view, emit } = await mount();

    await act(async () => {
      void view.result.current.run('sent from here');
    });
    const optimistic = view.result.current.entries.at(-1)!;
    expect(optimistic.id).toStartWith('optimistic:user:');

    await act(async () => {
      emit({
        seq: 8,
        type: 'MESSAGE_APPENDED',
        payload: { id: 'real-id', role: 'user', content: 'sent from here', agentId: null },
      });
    });

    const matching = view.result.current.entries.filter((e) => e.text === 'sent from here');
    expect(matching).toHaveLength(1);
    // The durable id lands, so the message can be edited.
    expect(matching[0]!.id).toBe('real-id');
  });

  it('truncates when an edit drops history elsewhere', async () => {
    const { view, emit } = await mount();
    expect(view.result.current.entries).toHaveLength(2);

    await act(async () => {
      emit({ seq: 8, type: 'MESSAGES_DROPPED', payload: { fromMessageId: 'm2' } });
    });

    expect(view.result.current.entries.map((e) => e.id)).toEqual(['m1']);
  });
});

describe('approvals (§2.5)', () => {
  it('surfaces a park, and does not duplicate it on replay', async () => {
    const { view, emit } = await mount();

    await act(async () => {
      emit({
        seq: 8,
        type: 'INPUT_REQUIRED',
        payload: { toolCallId: 'c1', toolName: 'sendEmail', arguments: { to: 'a@b.com' } },
      });
      emit({
        seq: 9,
        type: 'INPUT_REQUIRED',
        payload: { toolCallId: 'c1', toolName: 'sendEmail', arguments: { to: 'a@b.com' } },
      });
    });

    expect(view.result.current.pendingInputs).toHaveLength(1);
    expect(view.result.current.agentState).toBe('WAITING_FOR_INPUT');
  });

  it('drops the answered card after confirmation and posts the verdict', async () => {
    const { view, emit, calls } = await mount();

    await act(async () => {
      emit({ seq: 8, type: 'INPUT_REQUIRED', payload: { toolCallId: 'c1', toolName: 'sendEmail', arguments: {} } });
      emit({ seq: 9, type: 'INPUT_REQUIRED', payload: { toolCallId: 'c2', toolName: 'sendEmail', arguments: {} } });
    });
    expect(view.result.current.pendingInputs).toHaveLength(2);

    await act(async () => {
      await view.result.current.respondToInput('c1', true, { ok: 1 });
    });

    // A sibling approval is still open, so the card list shrinks by one only.
    expect(view.result.current.pendingInputs.map((p) => p.toolCallId)).toEqual(['c2']);
    const post = calls.find((c) => c.url.includes('/api/agent/respond'))!;
    expect(JSON.parse(String(post.init?.body))).toMatchObject({
      threadId: 't1',
      toolCallId: 'c1',
      approved: true,
      payload: { ok: 1 },
    });
  });

  it('clears only the expired request', async () => {
    const { view, emit } = await mount();

    await act(async () => {
      emit({ seq: 8, type: 'INPUT_REQUIRED', payload: { toolCallId: 'c1', toolName: 't', arguments: {} } });
      emit({ seq: 9, type: 'INPUT_REQUIRED', payload: { toolCallId: 'c2', toolName: 't', arguments: {} } });
      emit({ seq: 10, type: 'INPUT_EXPIRED', payload: { toolCallId: 'c1' } });
    });

    expect(view.result.current.pendingInputs.map((p) => p.toolCallId)).toEqual(['c2']);
  });
});

describe('control failures', () => {
  for (const kind of ['refusal', 'http', 'network', 'invalid-json'] as const) {
    for (const control of ['stop', 'respond'] as const) it(`reports ${control} ${kind} without losing run state or approval cards`, async () => {
      const base = harness();
      const { view, emit } = await mount({
        fetch: async (input, init) => {
          if (init?.method !== 'POST') return base.config.fetch!(input, init);
          if (kind === 'network') throw new Error('offline');
          if (kind === 'invalid-json') return new Response('invalid', { status: 200 });
          return new Response(JSON.stringify({ accepted: false, delivered: false, error: 'request refused' }), {
            status: kind === 'http' ? 503 : 200,
          });
        },
      });
      await act(async () => {
        emit({ seq: 8, type: 'INPUT_REQUIRED', payload: { toolCallId: 'c1', toolName: 'wipe', arguments: {} } });
      });
      await act(async () => {
        const ok = control === 'stop' ? await view.result.current.stop() : await view.result.current.respondToInput('c1', true);
        expect(ok).toBe(false);
      });
      expect(view.result.current.agentState).toBe('WAITING_FOR_INPUT');
      expect(view.result.current.pendingInputs.map((p) => p.toolCallId)).toEqual(['c1']);
      expect(view.result.current.activity.detail).toBeTruthy();
      expect(view.result.current.activity.label).toBe(control === 'stop' ? 'Could not stop run' : 'Could not send response');
    });
  }

  for (const terminal of ['COMPLETED', 'FAILED', 'CANCELLED'] as const) it(`keeps the card during delivery and does not replace a newer ${terminal} activity`, async () => {
    const base = harness();
    let complete!: (res: Response) => void;
    const { view, emit } = await mount({
      fetch: (input, init) => init?.method === 'POST'
        ? new Promise<Response>((resolve) => { complete = resolve; })
        : base.config.fetch!(input, init),
    });
    await act(async () => {
      emit({ seq: 8, type: 'INPUT_REQUIRED', payload: { toolCallId: 'c1', toolName: 'wipe', arguments: {} } });
    });
    let pending!: Promise<boolean>;
    await act(async () => { pending = view.result.current.respondToInput('c1', true); });
    expect(view.result.current.pendingInputs).toHaveLength(1);
    await act(async () => {
      emit({ seq: 9, type: 'STATE_CHANGE', payload: { state: terminal } });
      complete(new Response(JSON.stringify({ delivered: true })));
      expect(await pending).toBe(true);
    });
    expect(view.result.current.pendingInputs).toHaveLength(0);
    expect(view.result.current.activity.phase).toBe(terminal === 'CANCELLED' ? 'stopped' : terminal === 'FAILED' ? 'failed' : 'completed');
  });
});

describe('subagents (§2.7)', () => {
  it('tracks a nested run through its life', async () => {
    const { view, emit } = await mount();

    await act(async () => {
      emit({ seq: 8, type: 'SUBAGENT_STARTED', payload: { agentId: 'sub_1', name: 'mailer', depth: 1 } });
      emit({ seq: 9, type: 'SUBAGENT_CHUNK', payload: { agentId: 'sub_1', chunk: { textDelta: 'working' } } });
    });
    expect(view.result.current.subagents).toMatchObject([
      { agentId: 'sub_1', name: 'mailer', depth: 1, status: 'RUNNING', text: 'working' },
    ]);

    await act(async () => {
      emit({ seq: 10, type: 'SUBAGENT_FAILED', payload: { agentId: 'sub_1', error: 'unknown model' } });
    });
    // A failed child reports to its parent; the reason has to survive.
    expect(view.result.current.subagents[0]).toMatchObject({
      status: 'FAILED',
      error: 'unknown model',
    });
  });
});

describe('run and stop (§2.1)', () => {
  it('posts the prompt and adopts the thread the server returns', async () => {
    const { view, calls } = await mount();

    await act(async () => {
      await view.result.current.run('do the thing', { model: 'gpt-4o-mini' });
    });

    const post = calls.find((c) => c.url.includes('/api/agent/run'))!;
    expect(JSON.parse(String(post.init?.body))).toMatchObject({
      threadId: 't1',
      prompt: 'do the thing',
      model: 'gpt-4o-mini',
    });
  });

  it('passes anything else straight through to the run route', async () => {
    const { view, calls } = await mount();

    await act(async () => {
      await view.result.current.run('hi', {
        providerOptions: { openai: { serviceTier: 'flex' } },
      });
    });

    const body = JSON.parse(String(calls.find((c) => c.url.includes('/api/agent/run'))!.init?.body));
    expect(body.providerOptions).toEqual({ openai: { serviceTier: 'flex' } });
  });

  it('reports a rejected run instead of leaving the UI stuck on RUNNING', async () => {
    const { view } = await mount({ runResult: { accepted: false, error: 'Thread has an active run' } });

    await act(async () => {
      const r = await view.result.current.run('hi');
      expect(r.accepted).toBe(false);
    });

    expect(view.result.current.agentState).toBe('FAILED');
    expect(view.result.current.activity.detail).toContain('Thread has an active run');
  });

  it('stops the open thread', async () => {
    const { view, calls } = await mount();

    await act(async () => {
      await view.result.current.stop();
    });

    const post = calls.find((c) => c.url.includes('/api/agent/control'))!;
    expect(JSON.parse(String(post.init?.body))).toEqual({ threadId: 't1' });
  });
});

describe('configuration', () => {
  it('calls the routes it was given, not the defaults', async () => {
    const { view, calls, streams } = await mount({
      baseUrl: 'https://api.example.com',
      routes: {
        run: '/v2/start',
        history: ({ threadId }: { threadId: string }) => `/v2/threads/${threadId}`,
        stream: ({ threadId, since }: { threadId: string; since: number }) =>
          `/v2/threads/${threadId}/live?from=${since}`,
      },
    });

    // A FUNCTION route builds the whole URL and is used verbatim — baseUrl is
    // deliberately not prefixed, or it could never point at another host.
    expect(calls.some((c) => c.url === '/v2/threads/t1')).toBe(true);
    expect(streams[0]!.url).toBe('/v2/threads/t1/live?from=7');

    // A STRING route is a path, so it does get the prefix.
    await act(async () => {
      await view.result.current.run('hi');
    });
    expect(calls.some((c) => c.url === 'https://api.example.com/v2/start')).toBe(true);
  });

  it('sends the headers it was given, resolved per request', async () => {
    let n = 0;
    const { view, calls } = await mount({ headers: () => ({ authorization: `Bearer ${++n}` }) });

    await act(async () => {
      await view.result.current.run('hi');
    });

    const seen = calls.map((c) => (c.init?.headers as Record<string, string>)?.authorization);
    expect(seen.filter(Boolean).length).toBeGreaterThan(1);
    // A function is called per request, so a rotating token stays fresh.
    expect(new Set(seen.filter(Boolean)).size).toBeGreaterThan(1);
  });

  it('lets the app claim an event before the built-in reducer', async () => {
    const seen: string[] = [];
    const { view, emit } = await mount({
      onEvent: (event: { type: string }) => {
        seen.push(event.type);
        // Claiming CHUNK means the hook must not also render it.
        if (event.type === 'CHUNK') return true;
      },
    });
    const before = view.result.current.entries.length;

    await act(async () => {
      emit({ seq: 8, type: 'CHUNK', payload: { type: 'text-delta', textDelta: 'ignored' } });
    });

    expect(seen).toContain('CHUNK');
    expect(view.result.current.entries).toHaveLength(before);
  });

  it('uses the labels it was given', async () => {
    const { view, emit } = await mount({ labels: { responding: 'Antwoordt' } });

    await act(async () => {
      emit({ seq: 8, type: 'CHUNK', payload: { type: 'text-delta', textDelta: 'x' } });
    });

    expect(view.result.current.activity.label).toBe('Antwoordt');
  });
});

describe('thread persistence', () => {
  it('remembers the open thread so a reload reopens it', async () => {
    await mount();
    expect(window.location.search).toContain('threadId=t1');
    expect(window.localStorage.getItem('use-agentenkit:last-thread')).toBe('t1');
  });

  it('keeps it in memory only when persistence is off', async () => {
    await mount({ persistence: false });
    expect(window.location.search).toBe('');
    expect(window.localStorage.getItem('use-agentenkit:last-thread')).toBeNull();
  });
});

describe('structured parts (live)', () => {
  it('flips a tool card from running to done when its result lands', async () => {
    const { view, emit } = await mount();
    await act(async () => {
      emit({ seq: 8, type: 'CHUNK', payload: { type: 'tool-call', toolCallId: 'c9', toolName: 'lookup', args: { q: 'x' } } });
    });
    const call = view.result.current.entries.at(-1)!;
    expect(call.parts).toEqual([
      { type: 'tool-call', toolCallId: 'c9', toolName: 'lookup', args: { q: 'x' }, state: 'running' },
    ]);
    await act(async () => {
      emit({ seq: 9, type: 'CHUNK', payload: { type: 'tool-result', toolCallId: 'c9', toolName: 'lookup', result: { found: true } } });
    });
    const entries = view.result.current.entries;
    const settled = entries.find((e) => e.id === call.id)!;
    expect(settled.parts[0]).toMatchObject({ state: 'done', result: { found: true } });
    expect(entries.at(-1)!.parts).toEqual([
      { type: 'tool-result', toolCallId: 'c9', toolName: 'lookup', result: { found: true } },
    ]);
  });

  it('streams text and thinking into parts', async () => {
    const { view, emit } = await mount();
    await act(async () => {
      emit({ seq: 8, type: 'CHUNK', payload: { type: 'reasoning', textDelta: 'hm ' } });
      emit({ seq: 9, type: 'CHUNK', payload: { type: 'reasoning', textDelta: 'ok' } });
      emit({ seq: 10, type: 'CHUNK', payload: { type: 'text-delta', textDelta: 'an' } });
      emit({ seq: 11, type: 'CHUNK', payload: { type: 'text-delta', textDelta: 'swer' } });
    });
    const [thought, answer] = view.result.current.entries.slice(-2);
    expect(thought!.parts).toEqual([{ type: 'reasoning', text: 'hm ok' }]);
    expect(answer!.parts).toEqual([{ type: 'text', text: 'answer' }]);
  });

  it('sends attachments and shows them on the optimistic turn', async () => {
    const { view, calls } = await mount();
    await act(async () => {
      await view.result.current.run('what is this?', {
        attachments: [{ url: 'https://cdn/cat.png', mediaType: 'image/png' }],
        runId: 'run-1',
        maxSteps: 3,
      });
    });
    const optimistic = view.result.current.entries.find((e) => e.id.startsWith('optimistic:user:'))!;
    expect(optimistic.parts).toEqual([
      { type: 'text', text: 'what is this?' },
      { type: 'image', image: 'https://cdn/cat.png', mimeType: 'image/png' },
    ]);
    const runCall = calls.find((c) => c.url.includes('/api/agent/run'))!;
    const body = JSON.parse(String(runCall.init?.body));
    expect(body.attachments).toEqual([{ url: 'https://cdn/cat.png', mediaType: 'image/png' }]);
    expect(body.runId).toBe('run-1');
    expect(body.maxSteps).toBe(3);
  });
});
