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

export interface SubagentView {
  agentId: string;
  name: string;
  status: 'RUNNING' | 'COMPLETED' | 'FAILED' | 'CANCELLED';
  text: string;
}

export interface PendingInput {
  toolCallId: string;
  toolName: string;
  agentId?: string | null;
  arguments: unknown;
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

interface ThreadSnapshotResponse {
  thread: { id: string; state: AgentState };
  messages: SnapshotMessage[];
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
  const [pendingInput, setPendingInput] = useState<PendingInput | null>(null);
  const [subagents, setSubagents] = useState<SubagentView[]>([]);
  const threadRef = useRef<string | undefined>(threadId);
  threadRef.current = threadId;

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
        setActivity((current) => {
          if (
            nextState === 'RUNNING' &&
            ['thinking', 'responding', 'tool-call', 'tool-result'].includes(current.phase)
          ) {
            return current;
          }
          return stateActivity(nextState);
        });
        if (nextState !== 'WAITING_FOR_INPUT') setPendingInput(null);
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
        setPendingInput({
          toolCallId: p.toolCallId,
          toolName: p.toolName,
          agentId: p.agentId ?? null,
          arguments: p.arguments,
        });
        break;

      case 'INPUT_EXPIRED':
        setPendingInput(null);
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
            ? prev
            : [...prev, { agentId: p.agentId, name: p.name, status: 'RUNNING', text: '' }],
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
              ? { ...s, status: (p.state as SubagentView['status']) ?? 'FAILED' }
              : s,
          ),
        );
        break;
    }
  }, []);

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
          setAgentState('IDLE');
          setActivity(stateActivity('IDLE'));
          return;
        }
        if (!response.ok) throw new Error(`History request failed (${response.status})`);
        const snapshot = (await response.json()) as ThreadSnapshotResponse;
        if (cancelled) return;

        const durableEntries = snapshot.messages
          .map(messageToEntry)
          .filter((entry): entry is ChatEntry => entry !== null);
        setEntries(durableEntries);
        setSubagents([]);
        setPendingInput(null);
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
  }, [applyEvent, threadId]);

  const run = useCallback(async (prompt: string, model = 'gpt-4o') => {
    setEntries((prev) => [
      ...prev,
      { id: `optimistic:user:${Date.now()}`, kind: 'text', role: 'user', text: prompt },
    ]);
    setSubagents([]);
    setPendingInput(null);
    setAgentState('RUNNING');
    setActivity({ phase: 'thinking', label: 'Thinking' });

    try {
      const response = await fetch('/api/agent/run', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ threadId: threadRef.current, prompt, model }),
      });
      const data = await response.json();
      if (!response.ok || !data.accepted) {
        throw new Error(data.error ?? `Run request failed (${response.status})`);
      }
      setThreadId(data.threadId);
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
    async (approved: boolean, payload?: unknown) => {
      if (!threadRef.current || !pendingInput) return;
      setActivity({ phase: 'thinking', label: approved ? 'Approval sent' : 'Request denied' });
      await fetch('/api/agent/respond', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          threadId: threadRef.current,
          toolCallId: pendingInput.toolCallId,
          approved,
          payload,
        }),
      });
      setPendingInput(null);
    },
    [pendingInput],
  );

  return {
    threadId,
    entries,
    agentState,
    activity,
    historyLoading,
    pendingInput,
    subagents,
    run,
    stop,
    respondToInput,
  };
}
