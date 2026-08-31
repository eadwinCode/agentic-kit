'use client';

import { useCallback, useEffect, useRef, useState } from 'react';

export type AgentState =
  | 'IDLE'
  | 'RUNNING'
  | 'WAITING_FOR_INPUT'
  | 'CANCELLED'
  | 'COMPLETED'
  | 'FAILED';

export interface ChatEntry {
  kind: 'text' | 'tool';
  text: string;
  agentId?: string | null;
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

const truncate = (s: string, n = 220) => (s.length > n ? `${s.slice(0, n)}…` : s);

/** §5.3 — unified client hook. Streams one canonical event path (§2.2): every
 *  tab viewing the same threadId sees identical output. EventSource reconnects
 *  automatically; the server replays from Last-Event-ID. */
export function useAgentThread(initialThreadId?: string) {
  const [threadId, setThreadId] = useState<string | undefined>(initialThreadId);
  const [entries, setEntries] = useState<ChatEntry[]>([]);
  const [agentState, setAgentState] = useState<AgentState>('IDLE');
  const [pendingInput, setPendingInput] = useState<PendingInput | null>(null);
  const [subagents, setSubagents] = useState<SubagentView[]>([]);
  const threadRef = useRef<string | undefined>(threadId);
  threadRef.current = threadId;

  useEffect(() => {
    if (!threadId) return;

    const es = new EventSource(`/api/agent/stream?threadId=${threadId}`);
    es.onmessage = (ev) => {
      const data = JSON.parse(ev.data) as { type: string; payload: any };
      const p = data.payload ?? {};

      switch (data.type) {
        case 'STATE_CHANGE':
          setAgentState(p.state);
          if (p.state !== 'WAITING_FOR_INPUT') setPendingInput(null);
          break;

        case 'CHUNK': {
          if (p?.type === 'text-delta' && typeof p.textDelta === 'string') {
            setEntries((prev) => {
              const last = prev.at(-1);
              if (last && last.kind === 'text' && !last.agentId) {
                return [...prev.slice(0, -1), { ...last, text: last.text + p.textDelta }];
              }
              return [...prev, { kind: 'text', text: p.textDelta }];
            });
          } else if (p?.type === 'tool-call') {
            setEntries((prev) => [
              ...prev,
              { kind: 'tool', text: `⚙ ${p.toolName}(${truncate(JSON.stringify(p.args ?? {}), 120)})` },
            ]);
          } else if (p?.type === 'tool-result') {
            setEntries((prev) => [
              ...prev,
              { kind: 'tool', text: `↳ ${truncate(JSON.stringify(p.result))}` },
            ]);
          }
          break;
        }

        case 'INPUT_REQUIRED':
          setPendingInput({
            toolCallId: p.toolCallId,
            toolName: p.toolName,
            agentId: p.agentId ?? null,
            arguments: p.arguments,
          });
          break;

        case 'INPUT_EXPIRED':
          setPendingInput(null);
          break;

        case 'SUBAGENT_STARTED':
          setEntries((prev) => [
            ...prev,
            { kind: 'tool', text: `▸ subagent "${p.name}" started` },
          ]);
          setSubagents((prev) => [
            ...prev,
            { agentId: p.agentId, name: p.name, status: 'RUNNING', text: '' },
          ]);
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
    };

    return () => es.close();
  }, [threadId]);

  const run = useCallback(async (prompt: string, model = 'gpt-4o') => {
    setEntries([]);
    setSubagents([]);
    setPendingInput(null);
    setAgentState('RUNNING');
    const res = await fetch('/api/agent/run', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ threadId: threadRef.current, prompt, model }),
    });
    const data = await res.json();
    if (data.accepted) setThreadId(data.threadId);
    else setAgentState('IDLE');
    return data;
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

  return { threadId, entries, agentState, pendingInput, subagents, run, stop, respondToInput };
}
