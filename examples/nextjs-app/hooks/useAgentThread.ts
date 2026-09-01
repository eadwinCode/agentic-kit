'use client';

import { useCallback, useEffect, useRef, useState } from 'react';

export type AgentState =
  | 'IDLE'
  | 'RUNNING'
  | 'WAITING_FOR_INPUT'
  | 'CANCELLED'
  | 'COMPLETED'
  | 'FAILED';

export type MessageRole = 'user' | 'assistant' | 'system' | 'tool';

export interface ChatEntry {
  id: string;
  kind: 'text' | 'tool';
  role: MessageRole;
  text: string;
  agentId?: string | null;
}

export type ActivityPhase =
  | 'idle'
  | 'loading'
  | 'thinking'
  | 'responding'
  | 'tool-call'
  | 'tool-result'
  | 'waiting-input'
  | 'completed'
  | 'stopped'
  | 'failed';

export interface AgentActivity {
  phase: ActivityPhase;
  label: string;
  detail?: string;
}

export type SubagentStatus =
  | 'RUNNING'
  | 'WAITING_FOR_INPUT'
  | 'COMPLETED'
  | 'FAILED'
  | 'CANCELLED';

export interface SubagentView {
  agentId: string;
  name: string;
  /** 1 = spawned by the main agent (§2.7). */
  depth: number;
  status: SubagentStatus;
  text: string;
  /** Why it died — SUBAGENT_FAILED carries the reason. */
  error?: string;
}

export interface UsageTotals {
  inputTokens: number;
  cachedInputTokens: number;
  outputTokens: number;
  totalTokens: number;
}

export interface ContextUsage {
  usedTokens: number;
  budgetTokens: number;
  compactAtTokens: number;
  messages: number;
}

export interface ThreadUsage {
  tokens: UsageTotals;
  context: ContextUsage;
  model: string;
}

export interface PendingInput {
  toolCallId: string;
  toolName: string;
  /** The stream that asked — null when the main agent did (§2.7). */
  agentId?: string | null;
  /** The nested run's own name and depth, straight off the park, so the card
   *  can say "mailer" rather than an opaque id. */
  agentName?: string;
  depth?: number;
  arguments: unknown;
}

export interface ThreadListItem {
  id: string;
  title: string;
  state: AgentState;
  model: string;
  updatedAt: string;
}

interface SnapshotMessage {
  id: string;
  role: MessageRole;
  content: unknown;
  agentId?: string | null;
}

interface StreamEvent {
  seq: number;
  type: string;
  payload: any;
}

interface SnapshotRun {
  id: string;
  agent: string;
  /** 0 is the dispatched run itself; nested runs are 1+ (§2.7). */
  depth: number;
  state: SubagentStatus;
}

interface ThreadSnapshotResponse {
  thread: { id: string; state: AgentState };
  messages: SnapshotMessage[];
  runs: SnapshotRun[];
  lastEventSeq: number;
  activeEvents: StreamEvent[];
}

const LAST_THREAD_KEY = 'agent-example:last-thread';
const truncate = (s: string, n = 220) => (s.length > n ? `${s.slice(0, n)}…` : s);
const json = (value: unknown) => {
  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
};

function contentToText(content: unknown): string {
  if (typeof content === 'string') return content;
  if (!Array.isArray(content)) return json(content);

  return content
    .map((part: any) => {
      if (part?.type === 'text') return part.text ?? '';
      if (part?.type === 'reasoning') return part.text ?? part.reasoning ?? '';
      if (part?.type === 'tool-call') {
        return `⚙ ${part.toolName ?? 'tool'}(${truncate(json(part.args ?? {}), 120)})`;
      }
      if (part?.type === 'tool-result') return `↳ ${truncate(json(part.result))}`;
      return '';
    })
    .filter(Boolean)
    .join('\n');
}

function messageToEntry(message: SnapshotMessage): ChatEntry | null {
  const text = contentToText(message.content);
  if (!text) return null;
  const parts = Array.isArray(message.content) ? message.content : [];
  const containsText = parts.some((part: any) => part?.type === 'text' && part.text);
  const containsToolActivity = parts.some(
    (part: any) => part?.type === 'tool-call' || part?.type === 'tool-result',
  );
  return {
    id: message.id,
    kind:
      message.role === 'tool' || message.role === 'system' || (containsToolActivity && !containsText)
        ? 'tool'
        : 'text',
    role: message.role,
    text,
    agentId: message.agentId ?? null,
  };
}

function stateActivity(state: AgentState): AgentActivity {
  switch (state) {
    case 'RUNNING':
      return { phase: 'thinking', label: 'Thinking' };
    case 'WAITING_FOR_INPUT':
      return { phase: 'waiting-input', label: 'Waiting for approval' };
    case 'COMPLETED':
      return { phase: 'completed', label: 'Completed' };
    case 'CANCELLED':
      return { phase: 'stopped', label: 'Stopped' };
    case 'FAILED':
      return { phase: 'failed', label: 'Failed' };
    default:
      return { phase: 'idle', label: 'Idle' };
  }
}

/** Hydrates durable messages first, then resumes the canonical event stream at
 * the snapshot cursor. The URL/localStorage pointer makes refreshes recover the
 * same conversation while the server remains the source of truth. */
export function useAgentThread(initialThreadId?: string) {
  const [threadId, setThreadId] = useState<string | undefined>(initialThreadId);
  const [entries, setEntries] = useState<ChatEntry[]>([]);
  const [agentState, setAgentState] = useState<AgentState>('IDLE');
  const [activity, setActivity] = useState<AgentActivity>(stateActivity('IDLE'));
  const [historyLoading, setHistoryLoading] = useState(false);
  // A parent step can park several nested runs at once (§2.7), so the run
  // waits on a SET of approvals — the thread resumes when the last is answered.
  const [pendingInputs, setPendingInputs] = useState<PendingInput[]>([]);
  const [subagents, setSubagents] = useState<SubagentView[]>([]);
  const [threads, setThreads] = useState<ThreadListItem[]>([]);
  const [threadsLoading, setThreadsLoading] = useState(true);
  const [usage, setUsage] = useState<ThreadUsage | null>(null);
  const threadRef = useRef<string | undefined>(threadId);
  threadRef.current = threadId;
  /** Tool calls already visible from the durable messages. The snapshot's
   *  activeEvents replay the same step's CHUNKs (§2.2), so without this a
   *  reconnect renders every finished tool call twice. */
  const seenToolCalls = useRef<Set<string>>(new Set());

  /** Tokens spent (§4) and context load (§2.6). Read after hydration and
   *  again whenever a run ends — both only change when a run writes. */
  const loadUsage = useCallback(async (id?: string) => {
    const target = id ?? threadRef.current;
    if (!target) return;
    try {
      const res = await fetch(`/api/agent/usage?threadId=${encodeURIComponent(target)}`);
      if (!res.ok) return;
      const data = (await res.json()) as ThreadUsage;
      if (threadRef.current === target) setUsage(data);
    } catch {
      // usage is a read-only extra — never break the conversation over it
    }
  }, []);

  /** Thread picker / sidebar: best-effort refresh, most recent first. */
  const loadThreads = useCallback(async () => {
    try {
      setThreadsLoading(true);
      const res = await fetch('/api/threads');
      if (!res.ok) return;
      const data = await res.json();
      setThreads(data.threads ?? []);
    } catch {
      // sidebar is best-effort — ignore transport errors
    } finally {
      setThreadsLoading(false);
    }
  }, []);

  useEffect(() => {
    void loadThreads();
  }, [loadThreads]);

  /** Slow-cadence sidebar refresh: other tabs (or the worker) can flip thread
   *  states without this tab seeing the event. */
  useEffect(() => {
    const id = setInterval(() => void loadThreads(), 30_000);
    return () => clearInterval(id);
  }, [loadThreads]);

  /** Start a new thread: clear the pointer so the next Run creates one. */
  const newThread = useCallback(() => {
    setThreadId(undefined);
    setEntries([]);
    setSubagents([]);
    setPendingInputs([]);
    setUsage(null);
    setAgentState('IDLE');
    setActivity(stateActivity('IDLE'));
    window.localStorage.removeItem(LAST_THREAD_KEY);
    const url = new URL(window.location.href);
    url.searchParams.delete('threadId');
    window.history.replaceState({}, '', url);
  }, []);

  /** Select an existing thread — hydration + SSE resume run in the
   *  threadId effect below. */
  const selectThread = useCallback((id: string) => {
    setThreadId(id);
  }, []);

  /** Delete a thread (§3.2): the platform cascades messages, events, usage
   *  and runs. If it's the open thread, reset the view; 404 counts as
   *  success (someone in another tab beat us to it). */
  const deleteThread = useCallback(
    async (id: string) => {
      try {
        const res = await fetch(`/api/threads?threadId=${encodeURIComponent(id)}`, {
          method: 'DELETE',
        });
        if (!res.ok && res.status !== 404) return false;
        if (threadRef.current === id) newThread();
        void loadThreads();
        return true;
      } catch {
        return false;
      }
    },
    [newThread, loadThreads],
  );

  useEffect(() => {
    if (initialThreadId || threadRef.current) return;
    const fromUrl = new URLSearchParams(window.location.search).get('threadId');
    const saved = window.localStorage.getItem(LAST_THREAD_KEY);
    const recovered = fromUrl || saved || undefined;
    if (recovered) setThreadId(recovered);
  }, [initialThreadId]);

  const applyEvent = useCallback((data: StreamEvent) => {
    const p = data.payload ?? {};

    switch (data.type) {
      case 'STATE_CHANGE': {
        const nextState = p.state as AgentState;
        setAgentState(nextState);
        if (nextState === 'RUNNING') {
          // The park was resolved: every child that was waiting is re-entered
          // where it stopped (§2.7).
          setSubagents((prev) =>
            prev.map((s) =>
              s.status === 'WAITING_FOR_INPUT' ? { ...s, status: 'RUNNING' } : s,
            ),
          );
        }
        setActivity((current) => {
          if (
            nextState === 'RUNNING' &&
            ['thinking', 'responding', 'tool-call', 'tool-result'].includes(current.phase)
          ) {
            return current;
          }
          return stateActivity(nextState);
        });
        if (nextState !== 'WAITING_FOR_INPUT') setPendingInputs([]);
        // Terminal states land in the durable thread row — refresh the
        // sidebar so it stops claiming a finished run is still RUNNING.
        if (nextState === 'COMPLETED' || nextState === 'FAILED' || nextState === 'CANCELLED') {
          void loadThreads();
          void loadUsage();
        }
        break;
      }

      // Another tab sent a message on this thread (§2.2). The sending tab
      // added it to its own state before the request went out, so this is
      // where every OTHER tab learns what was asked.
      case 'MESSAGE_APPENDED': {
        const entry = messageToEntry({
          id: String(p.id),
          role: p.role as MessageRole,
          content: p.content,
          agentId: (p.agentId ?? null) as string | null,
        } as SnapshotMessage);
        if (!entry) break;
        setEntries((prev) => {
          // Already have it — a replayed event, or our own optimistic copy
          // now confirmed. Replace the optimistic one so the real id lands
          // (editing a message needs it), otherwise it would show twice.
          if (prev.some((e) => e.id === entry.id)) return prev;
          const optimistic = prev.findIndex(
            (e) => e.id.startsWith('optimistic:user:') && e.text === entry.text,
          );
          if (optimistic !== -1) {
            const next = [...prev];
            next[optimistic] = entry;
            return next;
          }
          return [...prev, entry];
        });
        break;
      }

      // An edit dropped that turn and everything after it. The editing tab
      // already truncated its own view; this is for the others.
      case 'MESSAGES_DROPPED': {
        setEntries((prev) => {
          const at = prev.findIndex((e) => e.id === p.fromMessageId);
          return at === -1 ? prev : prev.slice(0, at);
        });
        break;
      }

      case 'CHUNK': {
        if (p?.type === 'text-delta' && typeof p.textDelta === 'string') {
          setActivity({ phase: 'responding', label: 'Responding' });
          setEntries((prev) => {
            const last = prev.at(-1);
            if (last?.id.startsWith('live:assistant:') && !last.agentId) {
              return [...prev.slice(0, -1), { ...last, text: last.text + p.textDelta }];
            }
            return [
              ...prev,
              {
                id: `live:assistant:${data.seq}`,
                kind: 'text',
                role: 'assistant',
                text: p.textDelta,
              },
            ];
          });
        } else if (p?.type === 'reasoning' || p?.type === 'source') {
          setActivity({
            phase: 'thinking',
            label: p.type === 'source' ? 'Reviewing sources' : 'Thinking',
          });
        } else if (
          p?.type === 'tool-call-streaming-start' ||
          p?.type === 'tool-call-delta'
        ) {
          setActivity({
            phase: 'tool-call',
            label: 'Preparing tool call',
            detail: p.toolName,
          });
        } else if (p?.type === 'tool-call') {
          if (p.toolCallId && seenToolCalls.current.has(p.toolCallId)) break; // already durable
          if (p.toolCallId) seenToolCalls.current.add(p.toolCallId);
          setActivity({ phase: 'tool-call', label: 'Calling tool', detail: p.toolName });
          setEntries((prev) => [
            ...prev,
            {
              id: `live:tool-call:${data.seq}`,
              kind: 'tool',
              role: 'tool',
              text: `⚙ ${p.toolName}(${truncate(json(p.args ?? {}), 120)})`,
            },
          ]);
        } else if (p?.type === 'tool-result') {
          // The park sentinel is an internal marker, not a result (§2.5) —
          // it is never persisted and must never be shown.
          if (p.result && typeof p.result === 'object' && '__hitl_parked__' in p.result) break;
          if (p.toolCallId && seenToolCalls.current.has(p.toolCallId)) break; // already durable
          if (p.toolCallId) seenToolCalls.current.add(p.toolCallId);
          setActivity({ phase: 'tool-result', label: 'Tool completed', detail: p.toolName });
          setEntries((prev) => [
            ...prev,
            {
              id: `live:tool-result:${data.seq}`,
              kind: 'tool',
              role: 'tool',
              text: `↳ ${p.toolName ? `${p.toolName}: ` : ''}${truncate(json(p.result))}`,
            },
          ]);
        }
        break;
      }

      case 'INPUT_REQUIRED':
        setAgentState('WAITING_FOR_INPUT');
        setActivity({ phase: 'waiting-input', label: 'Waiting for approval', detail: p.toolName });
        setPendingInputs((prev) =>
          prev.some((r) => r.toolCallId === p.toolCallId)
            ? prev // replayed on reconnect (§2.2)
            : [
                ...prev,
                {
                  toolCallId: p.toolCallId,
                  toolName: p.toolName,
                  agentId: p.agentId ?? null,
                  agentName: p.nested?.name,
                  depth: p.nested?.depth,
                  arguments: p.arguments,
                },
              ],
        );
        // The child that asked is suspended, not working (§2.7).
        if (p.agentId) {
          setSubagents((prev) =>
            prev.map((s) =>
              s.agentId === p.agentId ? { ...s, status: 'WAITING_FOR_INPUT' } : s,
            ),
          );
        }
        break;

      case 'INPUT_EXPIRED':
        // Only this request expired; any sibling approval is still open.
        setPendingInputs((prev) => prev.filter((r) => r.toolCallId !== p.toolCallId));
        setActivity({ phase: 'failed', label: 'Approval expired' });
        break;

      case 'SUBAGENT_STARTED':
        setActivity({ phase: 'tool-call', label: 'Subagent working', detail: p.name });
        setEntries((prev) => [
          ...prev,
          {
            id: `live:subagent:${p.agentId}:${data.seq}`,
            kind: 'tool',
            role: 'tool',
            text: `▸ subagent "${p.name}" started`,
          },
        ]);
        setSubagents((prev) =>
          prev.some((s) => s.agentId === p.agentId)
            ? // Already hydrated from its persisted turns — name it.
              prev.map((s) =>
                s.agentId === p.agentId
                  ? { ...s, name: p.name, depth: p.depth ?? s.depth }
                  : s,
              )
            : [
                ...prev,
                {
                  agentId: p.agentId,
                  name: p.name,
                  depth: p.depth ?? 1,
                  status: 'RUNNING',
                  text: '',
                },
              ],
        );
        break;

      case 'SUBAGENT_CHUNK':
        setSubagents((prev) =>
          prev.map((s) =>
            s.agentId === p.agentId
              ? { ...s, text: s.text + (p.chunk?.textDelta ?? '') }
              : s,
          ),
        );
        break;

      case 'SUBAGENT_COMPLETED':
        setActivity({ phase: 'tool-result', label: 'Subagent completed', detail: p.name });
        setSubagents((prev) =>
          prev.map((s) => (s.agentId === p.agentId ? { ...s, status: 'COMPLETED' } : s)),
        );
        break;

      case 'SUBAGENT_FAILED':
        setSubagents((prev) =>
          prev.map((s) =>
            s.agentId === p.agentId
              ? { ...s, status: (p.state as SubagentStatus) ?? 'FAILED', error: p.error }
              : s,
          ),
        );
        break;
    }
  }, [loadThreads, loadUsage]);

  useEffect(() => {
    if (!threadId) return;

    window.localStorage.setItem(LAST_THREAD_KEY, threadId);
    const url = new URL(window.location.href);
    url.searchParams.set('threadId', threadId);
    window.history.replaceState({}, '', url);

    let cancelled = false;
    let es: EventSource | undefined;
    setHistoryLoading(true);
    setActivity({ phase: 'loading', label: 'Loading conversation' });

    void (async () => {
      try {
        const response = await fetch(`/api/agent/history?threadId=${encodeURIComponent(threadId)}`);
        if (response.status === 404) {
          window.localStorage.removeItem(LAST_THREAD_KEY);
          const cleanUrl = new URL(window.location.href);
          cleanUrl.searchParams.delete('threadId');
          window.history.replaceState({}, '', cleanUrl);
          setThreadId(undefined);
          setEntries([]);
          setUsage(null);
          setAgentState('IDLE');
          setActivity(stateActivity('IDLE'));
          return;
        }
        if (!response.ok) throw new Error(`History request failed (${response.status})`);
        const snapshot = (await response.json()) as ThreadSnapshotResponse;
        if (cancelled) return;

        // A nested run's turns live in the same log under its own agentId
        // (§2.7). They are its transcript, not the main conversation's.
        const mainMessages = snapshot.messages.filter((m) => (m.agentId ?? null) === null);
        const durableEntries = mainMessages
          .map(messageToEntry)
          .filter((entry): entry is ChatEntry => entry !== null);
        setEntries(durableEntries);

        // Rebuild each child's card from what it actually wrote, so a reload
        // no longer loses a subagent's output. SUBAGENT_STARTED names them
        // during the replay below.
        seenToolCalls.current = new Set(
          snapshot.messages.flatMap((m) =>
            (Array.isArray(m.content) ? m.content : [])
              .map((part: any) => part?.toolCallId)
              .filter((id: unknown): id is string => typeof id === 'string'),
          ),
        );

        // Name, depth and final state come from the durable SubagentRun rows;
        // the SUBAGENT_* events only replay while a run is unfinished, so on a
        // completed thread they are all a client has (§2.7).
        const byAgent = new Map<string, SubagentView>(
          // Nested runs only: depth 0 is this thread's own dispatched run,
          // which the transcript already represents (§2.9).
          (snapshot.runs ?? [])
            .filter((r) => r.depth > 0)
            .map((r) => [
              r.id,
              { agentId: r.id, name: r.agent, depth: r.depth, status: r.state, text: '' },
            ]),
        );
        for (const m of snapshot.messages) {
          const id = m.agentId ?? null;
          if (id === null) continue;
          const view = byAgent.get(id) ?? {
            agentId: id, name: id.slice(0, 8), depth: 1, status: 'RUNNING' as const, text: '',
          };
          if (m.role === 'assistant') {
            const text = contentToText(m.content);
            if (text) view.text = view.text ? `${view.text}\n${text}` : text;
          }
          byAgent.set(id, view);
        }
        // Trailing break so live deltas from a resumed child start on their
        // own line instead of running into what it already wrote.
        for (const view of byAgent.values()) if (view.text) view.text += '\n';
        setSubagents([...byAgent.values()]);
        setPendingInputs([]);
        void loadUsage(threadId);
        setAgentState(snapshot.thread.state);
        setActivity(stateActivity(snapshot.thread.state));

        for (const event of snapshot.activeEvents) applyEvent(event);

        es = new EventSource(
          `/api/agent/stream?threadId=${encodeURIComponent(threadId)}&since=${snapshot.lastEventSeq}`,
        );
        es.onmessage = (event) => applyEvent(JSON.parse(event.data) as StreamEvent);
        es.onerror = () => {
          // EventSource reconnects automatically; keep the last meaningful
          // activity instead of presenting a transient network failure.
        };
      } catch (error) {
        if (cancelled) return;
        setAgentState('FAILED');
        setActivity({
          phase: 'failed',
          label: 'Could not load conversation',
          detail: error instanceof Error ? error.message : String(error),
        });
      } finally {
        if (!cancelled) setHistoryLoading(false);
      }
    })();

    return () => {
      cancelled = true;
      es?.close();
    };
  }, [applyEvent, loadUsage, threadId]);

  const run = useCallback(async (
    prompt: string,
    model = 'gpt-4o',
    editMessageId?: string,
  ) => {
    setEntries((prev) => {
      // An edit replaces that turn and everything it led to, mirroring what
      // the server just did to the durable history.
      const at = editMessageId ? prev.findIndex((e) => e.id === editMessageId) : -1;
      const kept = at === -1 ? prev : prev.slice(0, at);
      return [
        ...kept,
        { id: `optimistic:user:${Date.now()}`, kind: 'text', role: 'user', text: prompt },
      ];
    });
    setSubagents([]);
    setPendingInputs([]);
    setAgentState('RUNNING');
    setActivity({ phase: 'thinking', label: 'Thinking' });

    try {
      const response = await fetch('/api/agent/run', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ threadId: threadRef.current, prompt, model, editMessageId }),
      });
      const data = await response.json();
      if (!response.ok || !data.accepted) {
        throw new Error(data.error ?? `Run request failed (${response.status})`);
      }
      setThreadId(data.threadId);
      void loadThreads(); // sidebar reflects the new thread immediately
      return data;
    } catch (error) {
      setAgentState('FAILED');
      setActivity({
        phase: 'failed',
        label: 'Could not start run',
        detail: error instanceof Error ? error.message : String(error),
      });
      return { accepted: false, threadId: threadRef.current, error: String(error) };
    }
  }, []);

  const stop = useCallback(async () => {
    if (!threadRef.current) return;
    await fetch('/api/agent/control', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ threadId: threadRef.current }),
    });
  }, []);

  const respondToInput = useCallback(
    async (toolCallId: string, approved: boolean, payload?: unknown) => {
      if (!threadRef.current) return;
      // Drop this card straight away; the run only moves once the LAST open
      // approval is answered, so the others stay on screen (§2.7).
      setPendingInputs((prev) => prev.filter((r) => r.toolCallId !== toolCallId));
      setActivity({ phase: 'thinking', label: approved ? 'Approval sent' : 'Request denied' });
      await fetch('/api/agent/respond', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ threadId: threadRef.current, toolCallId, approved, payload }),
      });
    },
    [],
  );

  return {
    threadId,
    entries,
    agentState,
    activity,
    historyLoading,
    pendingInputs,
    subagents,
    threads,
    threadsLoading,
    usage,
    loadThreads,
    newThread,
    selectThread,
    deleteThread,
    run,
    stop,
    respondToInput,
  };
}
