'use client';

import { useCallback, useEffect, useRef, useState } from 'react';
import {
  resolveConfig,
  routeUrl,
  withQuery,
  type AgentRunConfig,
  type ResolvedConfig,
} from './config.js';
import { mergeConfig, useAgentRunConfig } from './context.js';
import { answeredToolCalls, messageToEntries, messageToEntry, stateActivity } from './format.js';
import type {
  AgentActivity,
  AgentState,
  Attachment,
  ChatEntry,
  EntryPart,
  MessageRole,
  PendingInput,
  RunResult,
  StreamEvent,
  SubagentStatus,
  SubagentView,
  ThreadListItem,
  ThreadSnapshot,
  ThreadUsage,
} from './types.js';

export interface UseAgentThreadOptions extends AgentRunConfig {
  /** Open this thread instead of whatever persistence remembers. */
  initialThreadId?: string;
}

export interface UseAgentThread {
  threadId: string | undefined;
  entries: ChatEntry[];
  agentState: AgentState;
  activity: AgentActivity;
  historyLoading: boolean;
  pendingInputs: PendingInput[];
  subagents: SubagentView[];
  threads: ThreadListItem[];
  threadsLoading: boolean;
  usage: ThreadUsage | null;
  loadThreads: () => Promise<void>;
  loadUsage: (threadId?: string) => Promise<void>;
  newThread: () => void;
  selectThread: (threadId: string) => void;
  deleteThread: (threadId: string) => Promise<boolean>;
  run: (prompt: string, options?: RunOptions) => Promise<RunResult>;
  stop: () => Promise<void>;
  respondToInput: (toolCallId: string, approved: boolean, payload?: unknown) => Promise<void>;
}

export interface RunOptions {
  model?: string;
  /** Replace this user turn and everything it led to, then answer again. */
  editMessageId?: string;
  /** Images sent with the prompt; they become image parts on the user turn. */
  attachments?: Attachment[];
  /** Name the run yourself, so your own records can be keyed by it before
   *  the server answers. Reusing an id is refused. */
  runId?: string;
  /** Cap this run's round trips below the server's configured ceiling. */
  maxSteps?: number;
  /** Anything else the run route accepts — merged into the request body. */
  [key: string]: unknown;
}

/** Mark the call a result belongs to as done (or failed) on the entry that
 *  announced it, so a tool card can flip state in place. */
function settleToolCall(entries: ChatEntry[], toolCallId: string, result: unknown): ChatEntry[] {
  const failed = !!result && typeof result === 'object' && 'error' in (result as object);
  let touched = false;
  const next = entries.map((entry) => {
    if (!entry.parts.some((p) => p.type === 'tool-call' && p.toolCallId === toolCallId)) return entry;
    touched = true;
    return {
      ...entry,
      parts: entry.parts.map((p): EntryPart =>
        p.type === 'tool-call' && p.toolCallId === toolCallId
          ? { ...p, state: failed ? 'error' : 'done', result }
          : p,
      ),
    };
  });
  return touched ? next : entries;
}

/** Hydrates durable messages first, then resumes the canonical event stream at
 *  the snapshot cursor, so a reload — or a second tab — rebuilds the same
 *  conversation with the server as the only source of truth.
 *
 *  Every endpoint, label and formatter is replaceable through the options or a
 *  surrounding provider; see `AgentRunConfig`. */
export function useAgentThread(options: UseAgentThreadOptions = {}): UseAgentThread {
  const { initialThreadId, ...config } = options;

  // Provider first, own options over it. Resolved every render rather than
  // memoized: the options object is almost always an inline literal, so a
  // dependency array on it would either churn or go stale.
  const resolved = resolveConfig(mergeConfig(useAgentRunConfig(), config));

  // Callbacks below must stay stable across renders, so they read config from
  // a ref instead of listing it in deps. A route changed after mount therefore
  // applies to the NEXT request; an open stream keeps the URL it has.
  const cfgRef = useRef<ResolvedConfig>(resolved);
  cfgRef.current = resolved;

  const [threadId, setThreadId] = useState<string | undefined>(initialThreadId);
  const [entries, setEntries] = useState<ChatEntry[]>([]);
  const [agentState, setAgentState] = useState<AgentState>('IDLE');
  const [activity, setActivity] = useState<AgentActivity>(() =>
    stateActivity('IDLE', resolved.labels),
  );
  const [historyLoading, setHistoryLoading] = useState(false);
  // A parent step can park several nested runs at once, so the run waits on a
  // SET of approvals — the thread resumes when the last one is answered.
  const [pendingInputs, setPendingInputs] = useState<PendingInput[]>([]);
  const [subagents, setSubagents] = useState<SubagentView[]>([]);
  const [threads, setThreads] = useState<ThreadListItem[]>([]);
  const [threadsLoading, setThreadsLoading] = useState(resolved.loadThreadsOnMount);
  const [usage, setUsage] = useState<ThreadUsage | null>(null);
  const threadRef = useRef<string | undefined>(threadId);
  threadRef.current = threadId;
  /** Tool calls already visible from the durable messages. The snapshot's
   *  activeEvents can replay the same step's chunks, so without this a
   *  reconnect renders a finished tool call twice. */
  const seenToolCalls = useRef<Set<string>>(new Set());
  /** Results already visible from the durable messages, kept apart from the
   *  calls: a live result must still render after its live call did. */
  const seenToolResults = useRef<Set<string>>(new Set());

  /** One place where the caller's headers and fetch are applied. */
  const request = useCallback(async (url: string, init: RequestInit = {}) => {
    const cfg = cfgRef.current;
    const extra = await cfg.headers();
    return cfg.fetch(url, {
      ...init,
      headers: { ...Object.fromEntries(new Headers(extra).entries()), ...(init.headers ?? {}) },
    });
  }, []);

  const postJson = useCallback(
    (url: string, body: unknown) =>
      request(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      }),
    [request],
  );

  /** Tokens spent and context load. Read after hydration and again whenever a
   *  run ends — both only change when a run writes. */
  const loadUsage = useCallback(
    async (id?: string) => {
      const cfg = cfgRef.current;
      const target = id ?? threadRef.current;
      if (!target) return;
      try {
        const res = await request(
          routeUrl(cfg.routes.usage, { threadId: target }, cfg.baseUrl),
        );
        if (!res.ok) return;
        const data = (await res.json()) as ThreadUsage;
        if (threadRef.current === target) setUsage(data);
      } catch {
        // usage is a read-only extra — never break the conversation over it
      }
    },
    [request],
  );

  /** Thread picker / sidebar: best-effort refresh, most recent first. */
  const loadThreads = useCallback(async () => {
    const cfg = cfgRef.current;
    try {
      setThreadsLoading(true);
      const res = await request(cfg.baseUrl + cfg.routes.threads);
      if (!res.ok) return;
      const data = await res.json();
      setThreads(data.threads ?? []);
    } catch {
      // sidebar is best-effort — ignore transport errors
    } finally {
      setThreadsLoading(false);
    }
  }, [request]);

  useEffect(() => {
    if (!resolved.loadThreadsOnMount) return;
    void loadThreads();
  }, [loadThreads, resolved.loadThreadsOnMount]);

  /** Slow-cadence sidebar refresh: other tabs (or the worker) can flip thread
   *  states without this tab seeing the event. */
  useEffect(() => {
    const every = resolved.threadsRefreshMs;
    if (every === false) return;
    const id = setInterval(() => void loadThreads(), every);
    return () => clearInterval(id);
  }, [loadThreads, resolved.threadsRefreshMs]);

  /** Start a new thread: clear the pointer so the next run creates one. */
  const newThread = useCallback(() => {
    setThreadId(undefined);
    setEntries([]);
    setSubagents([]);
    setPendingInputs([]);
    setUsage(null);
    setAgentState('IDLE');
    setActivity(stateActivity('IDLE', cfgRef.current.labels));
    cfgRef.current.persistence?.clear();
  }, []);

  /** Select an existing thread — hydration and stream resume run in the
   *  threadId effect below. */
  const selectThread = useCallback((id: string) => {
    setThreadId(id);
  }, []);

  /** Delete a thread: the platform cascades messages, events, usage and runs.
   *  If it is the open thread, reset the view; 404 counts as success (someone
   *  in another tab beat us to it). */
  const deleteThread = useCallback(
    async (id: string) => {
      const cfg = cfgRef.current;
      try {
        const res = await request(
          routeUrl(cfg.routes.deleteThread, { threadId: id }, cfg.baseUrl),
          { method: 'DELETE' },
        );
        if (!res.ok && res.status !== 404) return false;
        if (threadRef.current === id) newThread();
        void loadThreads();
        return true;
      } catch {
        return false;
      }
    },
    [newThread, loadThreads, request],
  );

  // Recover the last thread when the caller named none.
  useEffect(() => {
    if (initialThreadId || threadRef.current) return;
    const recovered = cfgRef.current.persistence?.load();
    if (recovered) setThreadId(recovered);
  }, [initialThreadId]);

  const applyEvent = useCallback(
    (data: StreamEvent) => {
      const cfg = cfgRef.current;
      const { labels, format } = cfg;
      const p = data.payload ?? {};

      // The app sees every event first, and can claim it.
      if (cfg.onEvent?.(data) === true) return;

      switch (data.type) {
        case 'STATE_CHANGE': {
          const nextState = p.state as AgentState;
          setAgentState(nextState);
          if (nextState === 'RUNNING') {
            // The park was resolved: every child that was waiting is re-entered
            // where it stopped.
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
            return stateActivity(nextState, labels);
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

        // Another client sent a message on this thread. The sending client
        // added it to its own state before the request went out, so this is
        // where every OTHER one learns what was asked.
        case 'MESSAGE_APPENDED': {
          const entry = messageToEntry(
            {
              id: String(p.id),
              role: p.role as MessageRole,
              content: p.content,
              agentId: (p.agentId ?? null) as string | null,
            },
            format,
          );
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

        // An edit dropped that turn and everything after it. The editing client
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
            setActivity({ phase: 'responding', label: labels.responding });
            setEntries((prev) => {
              const last = prev.at(-1);
              if (last?.kind === 'text' && last.id.startsWith('live:assistant:') && !last.agentId) {
                const text = last.text + p.textDelta;
                return [...prev.slice(0, -1), { ...last, text, parts: [{ type: 'text', text }] }];
              }
              return [
                ...prev,
                {
                  id: `live:assistant:${data.seq}`,
                  kind: 'text',
                  role: 'assistant',
                  text: p.textDelta,
                  parts: [{ type: 'text', text: p.textDelta }],
                },
              ];
            });
          } else if (p?.type === 'reasoning') {
            // The model's thinking, streamed like the answer but kept apart so
            // a UI can show it live and fold it away after. Providers that do
            // not expose reasoning never send these.
            setActivity({ phase: 'thinking', label: labels.thinking });
            if (typeof p.textDelta === 'string' && p.textDelta) {
              setEntries((prev) => {
                const last = prev.at(-1);
                if (last?.kind === 'reasoning' && last.id.startsWith('live:reasoning:')) {
                  const text = last.text + p.textDelta;
                  return [...prev.slice(0, -1), { ...last, text, parts: [{ type: 'reasoning', text }] }];
                }
                return [
                  ...prev,
                  {
                    id: `live:reasoning:${data.seq}`,
                    kind: 'reasoning',
                    role: 'assistant',
                    text: p.textDelta,
                    parts: [{ type: 'reasoning', text: p.textDelta }],
                  },
                ];
              });
            }
          } else if (p?.type === 'source') {
            setActivity({ phase: 'thinking', label: labels.reviewingSources });
          } else if (p?.type === 'tool-call-streaming-start' || p?.type === 'tool-call-delta') {
            setActivity({
              phase: 'tool-call',
              label: labels.preparingToolCall,
              detail: p.toolName,
            });
          } else if (p?.type === 'tool-call') {
            if (p.toolCallId && seenToolCalls.current.has(p.toolCallId)) break; // already durable
            if (p.toolCallId) seenToolCalls.current.add(p.toolCallId);
            setActivity({ phase: 'tool-call', label: labels.callingTool, detail: p.toolName });
            setEntries((prev) => [
              ...prev,
              {
                id: `live:tool-call:${data.seq}`,
                kind: 'tool',
                role: 'tool',
                text: format.toolCall(p.toolName, p.args ?? {}),
                parts: [
                  {
                    type: 'tool-call',
                    toolCallId: p.toolCallId,
                    toolName: p.toolName,
                    args: p.args ?? {},
                    state: 'running',
                  },
                ],
              },
            ]);
          } else if (p?.type === 'tool-result') {
            // The park sentinel is an internal marker, not a result — it is
            // never persisted and must never be shown.
            if (p.result && typeof p.result === 'object' && '__hitl_parked__' in p.result) break;
            if (p.toolCallId && seenToolResults.current.has(p.toolCallId)) break; // already durable
            if (p.toolCallId) seenToolResults.current.add(p.toolCallId);
            setActivity({ phase: 'tool-result', label: labels.toolCompleted, detail: p.toolName });
            setEntries((prev) => [
              ...settleToolCall(prev, p.toolCallId, p.result),
              {
                id: `live:tool-result:${data.seq}`,
                kind: 'tool',
                role: 'tool',
                text: format.toolResult(p.toolName, p.result),
                parts: [
                  { type: 'tool-result', toolCallId: p.toolCallId, toolName: p.toolName, result: p.result },
                ],
              },
            ]);
          }
          break;
        }

        case 'INPUT_REQUIRED': {
          const reason: string = p.reason ?? 'approval';
          setAgentState('WAITING_FOR_INPUT');
          setActivity({
            phase: 'waiting-input',
            label: reason === 'approval' ? labels.waitingApproval : labels.waitingWork,
            detail: p.toolName,
          });
          setPendingInputs((prev) =>
            prev.some((r) => r.toolCallId === p.toolCallId)
              ? prev // replayed on reconnect
              : [
                  ...prev,
                  {
                    toolCallId: p.toolCallId,
                    toolName: p.toolName,
                    agentId: p.agentId ?? null,
                    agentName: p.nested?.name,
                    depth: p.nested?.depth,
                    arguments: p.arguments,
                    reason,
                    ...(p.expiresAt ? { expiresAt: p.expiresAt } : {}),
                  },
                ],
          );
          // The child that asked is suspended, not working.
          if (p.agentId) {
            setSubagents((prev) =>
              prev.map((s) =>
                s.agentId === p.agentId ? { ...s, status: 'WAITING_FOR_INPUT' } : s,
              ),
            );
          }
          break;
        }

        case 'INPUT_EXPIRED':
          // Only this request expired; any sibling approval is still open.
          setPendingInputs((prev) => prev.filter((r) => r.toolCallId !== p.toolCallId));
          setActivity({ phase: 'failed', label: labels.approvalExpired });
          break;

        case 'SUBAGENT_STARTED':
          setActivity({ phase: 'tool-call', label: labels.subagentWorking, detail: p.name });
          setEntries((prev) => [
            ...prev,
            {
              id: `live:subagent:${p.agentId}:${data.seq}`,
              kind: 'tool',
              role: 'tool',
              text: format.subagentStarted(p.name),
              parts: [{ type: 'text', text: format.subagentStarted(p.name) }],
            },
          ]);
          setSubagents((prev) =>
            prev.some((s) => s.agentId === p.agentId)
              ? // Already hydrated from its persisted turns — name it.
                prev.map((s) =>
                  s.agentId === p.agentId ? { ...s, name: p.name, depth: p.depth ?? s.depth } : s,
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
              s.agentId === p.agentId ? { ...s, text: s.text + (p.chunk?.textDelta ?? '') } : s,
            ),
          );
          break;

        case 'SUBAGENT_COMPLETED':
          setActivity({ phase: 'tool-result', label: labels.subagentCompleted, detail: p.name });
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
    },
    [loadThreads, loadUsage],
  );

  useEffect(() => {
    if (!threadId) return;
    const cfg = cfgRef.current;
    cfg.persistence?.save(threadId);

    let cancelled = false;
    let stream: { close(): void } | undefined;
    setHistoryLoading(true);
    setActivity({ phase: 'loading', label: cfg.labels.loading });

    void (async () => {
      try {
        const res = await request(
          routeUrl(cfg.routes.history, { threadId }, cfg.baseUrl),
        );
        if (res.status === 404) {
          cfg.persistence?.clear();
          setThreadId(undefined);
          setEntries([]);
          setUsage(null);
          setAgentState('IDLE');
          setActivity(stateActivity('IDLE', cfg.labels));
          return;
        }
        if (!res.ok) throw new Error(`History request failed (${res.status})`);
        const snapshot = (await res.json()) as ThreadSnapshot;
        if (cancelled) return;

        // A nested run's turns live in the same log under its own agentId.
        // They are its transcript, not the main conversation's.
        const mainMessages = snapshot.messages.filter((m) => (m.agentId ?? null) === null);
        const answered = answeredToolCalls(snapshot.messages);
        setEntries(mainMessages.flatMap((m) => messageToEntries(m, cfg.format, answered)));

        // Rebuild each child's card from what it actually wrote, so a reload
        // does not lose a subagent's output.
        const durableParts = snapshot.messages.flatMap((m) =>
          Array.isArray(m.content) ? (m.content as any[]) : [],
        );
        seenToolCalls.current = new Set(
          durableParts
            .filter((part) => part?.type === 'tool-call')
            .map((part) => part.toolCallId)
            .filter((id: unknown): id is string => typeof id === 'string'),
        );
        seenToolResults.current = new Set(answered);

        // Name, depth and final state come from the durable run rows; the
        // SUBAGENT_* events only replay while a run is unfinished, so on a
        // completed thread they are all a client has.
        const byAgent = new Map<string, SubagentView>(
          // Nested runs only: depth 0 is this thread's own dispatched run,
          // which the transcript already represents.
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
            agentId: id,
            name: id.slice(0, 8),
            depth: 1,
            status: 'RUNNING' as const,
            text: '',
          };
          if (m.role === 'assistant') {
            const text = messageToEntry(m, cfg.format)?.text ?? '';
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
        setActivity(stateActivity(snapshot.thread.state, cfg.labels));

        for (const event of snapshot.activeEvents) applyEvent(event);

        stream = cfg.openStream(
          routeUrl(cfg.routes.stream, { threadId, since: snapshot.lastEventSeq }, cfg.baseUrl),
          {
            onMessage: (raw) => applyEvent(JSON.parse(raw) as StreamEvent),
            onError: () => {
              // EventSource reconnects on its own; keep the last meaningful
              // activity instead of presenting a transient network failure.
            },
          },
        );
      } catch (error) {
        if (cancelled) return;
        setAgentState('FAILED');
        setActivity({
          phase: 'failed',
          label: cfgRef.current.labels.loadFailed,
          detail: error instanceof Error ? error.message : String(error),
        });
      } finally {
        if (!cancelled) setHistoryLoading(false);
      }
    })();

    return () => {
      cancelled = true;
      stream?.close();
    };
  }, [applyEvent, loadUsage, request, threadId]);

  const run = useCallback(
    async (prompt: string, options: RunOptions = {}): Promise<RunResult> => {
      const cfg = cfgRef.current;
      const { model = cfg.defaultModel, editMessageId, attachments, ...rest } = options;

      setEntries((prev) => {
        // An edit replaces that turn and everything it led to, mirroring what
        // the server is about to do to the durable history.
        const at = editMessageId ? prev.findIndex((e) => e.id === editMessageId) : -1;
        const kept = at === -1 ? prev : prev.slice(0, at);
        const parts: EntryPart[] = prompt ? [{ type: 'text', text: prompt }] : [];
        for (const a of attachments ?? []) {
          parts.push({ type: 'image', image: a.url, mimeType: a.mediaType });
        }
        return [
          ...kept,
          { id: `optimistic:user:${Date.now()}`, kind: 'text', role: 'user', text: prompt, parts },
        ];
      });
      setSubagents([]);
      setPendingInputs([]);
      setAgentState('RUNNING');
      setActivity({ phase: 'thinking', label: cfg.labels.thinking });

      try {
        const response = await postJson(cfg.baseUrl + cfg.routes.run, {
          threadId: threadRef.current,
          prompt,
          model,
          editMessageId,
          attachments,
          ...rest,
        });
        const data = (await response.json()) as RunResult;
        if (!response.ok || !data.accepted) {
          throw new Error(data.error ?? `Run request failed (${response.status})`);
        }
        if (data.threadId) setThreadId(data.threadId);
        void loadThreads(); // the sidebar reflects a new thread immediately
        return data;
      } catch (error) {
        setAgentState('FAILED');
        setActivity({
          phase: 'failed',
          label: cfg.labels.runFailed,
          detail: error instanceof Error ? error.message : String(error),
        });
        return { accepted: false, threadId: threadRef.current, error: String(error) };
      }
    },
    [loadThreads, postJson],
  );

  const stop = useCallback(async () => {
    if (!threadRef.current) return;
    const cfg = cfgRef.current;
    await postJson(cfg.baseUrl + cfg.routes.stop, { threadId: threadRef.current });
  }, [postJson]);

  const respondToInput = useCallback(
    async (toolCallId: string, approved: boolean, payload?: unknown) => {
      if (!threadRef.current) return;
      const cfg = cfgRef.current;
      // Drop this card straight away; the run only moves once the LAST open
      // approval is answered, so the others stay on screen.
      setPendingInputs((prev) => prev.filter((r) => r.toolCallId !== toolCallId));
      setActivity({
        phase: 'thinking',
        label: approved ? cfg.labels.approvalSent : cfg.labels.requestDenied,
      });
      await postJson(cfg.baseUrl + cfg.routes.respond, {
        threadId: threadRef.current,
        toolCallId,
        approved,
        payload,
      });
    },
    [postJson],
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
    loadUsage,
    newThread,
    selectThread,
    deleteThread,
    run,
    stop,
    respondToInput,
  };
}

export { withQuery };
