import type { AgentRunConfig, StreamHandlers } from '../src/config.js';
import type { StreamEvent, ThreadSnapshot } from '../src/types.js';

export type AgentRunConfigLike = Partial<AgentRunConfig> & {
  /** What the history route returns. */
  snapshot?: ThreadSnapshot;
  /** Status for the history route — 404 exercises the "thread is gone" path. */
  historyStatus?: number;
  /** What the run route returns. */
  runResult?: { accepted: boolean; threadId?: string; error?: string };
};

export interface FakeStream {
  url: string;
  closed: boolean;
  handlers: StreamHandlers;
}

const defaultSnapshot: ThreadSnapshot = {
  thread: { id: 't1', state: 'COMPLETED' },
  messages: [
    { id: 'm1', role: 'user', content: 'hello', agentId: null },
    { id: 'm2', role: 'assistant', content: [{ type: 'text', text: 'hi there' }], agentId: null },
  ],
  runs: [],
  lastEventSeq: 7,
  activeEvents: [],
};

/** Fakes for the two things the hook talks to.
 *
 *  Both are ordinary config options, so nothing global is patched and no real
 *  network or EventSource is involved — the transport being injectable is the
 *  same property that makes the hook testable. */
export function harness(over: AgentRunConfigLike = {}) {
  const { snapshot, historyStatus, runResult, ...config } = over;
  const calls: Array<{ url: string; init?: RequestInit }> = [];
  const streams: FakeStream[] = [];

  const json = (body: unknown, status = 200) =>
    Promise.resolve(
      new Response(JSON.stringify(body), {
        status,
        headers: { 'Content-Type': 'application/json' },
      }),
    );

  const fetchFake = (input: RequestInfo | URL, init?: RequestInit) => {
    const url = String(input);
    calls.push({ url, init });

    if (url.includes('history') || url.includes('/v2/threads/t1')) {
      if (historyStatus === 404) return json({ error: 'Thread not found' }, 404);
      return json(snapshot ?? defaultSnapshot);
    }
    if (url.includes('usage')) {
      return json({
        tokens: { inputTokens: 10, cachedInputTokens: 0, outputTokens: 5, totalTokens: 15 },
        context: { usedTokens: 10, budgetTokens: 100, compactAtTokens: 80, messages: 2 },
        model: 'gpt-4o',
      });
    }
    if (url.includes('threads')) return json({ threads: [] });
    if (url.includes('run') || url.includes('start')) {
      return json(runResult ?? { accepted: true, threadId: 't1', runId: 'r1' });
    }
    return json({ ok: true });
  };

  const openStream = (url: string, handlers: StreamHandlers) => {
    const stream: FakeStream = { url, closed: false, handlers };
    streams.push(stream);
    return {
      close() {
        stream.closed = true;
      },
    };
  };

  /** Push an event as the server would. */
  const emit = (event: StreamEvent) => {
    const live = streams.find((s) => !s.closed);
    if (!live) throw new Error('emit() called with no open stream');
    live.handlers.onMessage(JSON.stringify(event));
  };

  return {
    calls,
    streams,
    emit,
    config: {
      fetch: fetchFake,
      openStream,
      // The list is not what these tests are about, and polling it would make
      // them flaky.
      loadThreadsOnMount: false,
      threadsRefreshMs: false as const,
      ...config,
    } as AgentRunConfig,
  };
}
