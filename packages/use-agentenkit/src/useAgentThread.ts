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
import { isToolError, messageToEntries, messageToEntry, stateActivity, toolCallOutcomes } from './format.js';
import { formatCursor, streamItemEvents } from './frames.js';
import type {
  AgentActivity,
  AgentState,
  Attachment,
  ChatEntry,
  ConnectionState,
  EntryPart,
  MessageRole,
  FollowFrame,
  PendingInput,
  RunResult,
  SnapshotMessage,
  StreamEvent,
  SubagentStatus,
  SubagentView,
  ThreadListItem,
  ThreadRun,
  ThreadSnapshot,
  ThreadUsage,
  WireStreamItem,
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
  /** The thread's latest run with its clocks, for a "running for" timer.
   *  Hydrated from the snapshot, then kept from `STATE_CHANGE` alone; null
   *  before the first run. */
  currentRun: ThreadRun | null;
  /** Where the live stream stands. `closed` means the transport gave up; the
   *  hook re-reads the thread and opens a new stream on its own. */
  connection: ConnectionState;
  /** Why the last send did not go through, or why the server refused it
   *  (RUN_REFUSED). A send that fails leaves the conversation as it was and
   *  says so here, rather than marking the thread FAILED. Cleared by the next
   *  send that is accepted. */
  error: string | null;
  loadThreads: () => Promise<void>;
  loadUsage: (threadId?: string) => Promise<void>;
  newThread: () => void;
  selectThread: (threadId: string) => void;
  deleteThread: (threadId: string) => Promise<boolean>;
  /** Refused, with `error` set, while a run is queued, running or waiting on
   *  an approval, or while an earlier send is still in flight. */
  run: (prompt: string, options?: RunOptions) => Promise<RunResult>;
  /** False on refusal or transport failure; activity.detail explains why. */
  stop: () => Promise<boolean>;
  respondToInput: (toolCallId: string, approved: boolean, payload?: unknown) => Promise<boolean>;
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

/** States in which a thread already has a run: a new one is refused. */
const ACTIVE: readonly AgentState[] = ['QUEUED', 'RUNNING', 'WAITING_FOR_INPUT'];

/** How long a closed stream waits before the thread is read again, doubled
 *  per try up to the cap. */
const RECONNECT_BASE_MS = 1_000;
const RECONNECT_MAX_MS = 30_000;

/** Mark the call a result belongs to as done (or failed) on the entry that
 *  announced it, so a tool card can flip state in place. */
function settleToolCall(entries: ChatEntry[], toolCallId: string, result: unknown): ChatEntry[] {
  const failed = isToolError(result);
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

/** The run a STATE_CHANGE speaks for, with the clock it carries:
 *  `enqueuedAt` while the run waits for a worker, `startedAt` once one has
 *  it, `endedAt` once it ended. A server that leaves a clock off still gets
 *  a timer: the frame's own `createdAt` is when the change happened. An event
 *  without a run id says nothing about timing and leaves what is known. */
function runFromStateChange(prev: ThreadRun | null, state: AgentState, p: any, at?: string): ThreadRun | null {
  if (typeof p?.runId !== 'string') return prev;
  const same = prev?.id === p.runId;
  const next: ThreadRun = { id: p.runId };
  const clock = (key: 'enqueuedAt' | 'startedAt' | 'endedAt', now: boolean) =>
    typeof p[key] === 'string' ? p[key] : same && prev?.[key] ? prev[key] : now ? at : undefined;
  const enqueuedAt = clock('enqueuedAt', state === 'QUEUED');
  if (enqueuedAt) next.enqueuedAt = enqueuedAt;
  const startedAt = clock('startedAt', state === 'RUNNING' || state === 'WAITING_FOR_INPUT');
  if (startedAt) next.startedAt = startedAt;
  if (state === 'COMPLETED' || state === 'FAILED' || state === 'CANCELLED') {
    const endedAt = clock('endedAt', true);
    if (endedAt) next.endedAt = endedAt;
  }
  return next;
}

/** The snapshot's own dispatched run: the latest at depth 0. */
function latestRun(runs: readonly ThreadSnapshot['runs'][number][] | undefined): ThreadRun | null {
  let latest: ThreadSnapshot['runs'][number] | undefined;
  for (const r of runs ?? []) {
    if (r.depth !== 0) continue;
    if (!latest || (r.startedAt ?? '') > (latest.startedAt ?? '')) latest = r;
  }
  if (!latest) return null;
  const run: ThreadRun = { id: latest.id };
  if (latest.enqueuedAt) run.enqueuedAt = latest.enqueuedAt;
  // A run still waiting has no start of its own: its record's startedAt is
  // the enqueue time until a worker picks it up.
  if (latest.startedAt && latest.state !== 'QUEUED') run.startedAt = latest.startedAt;
  if (latest.endedAt) run.endedAt = latest.endedAt;
  return run;
}

/** Streamed text not yet shown: deltas are gathered here and shown once per
 *  frame, rather than re-rendering the conversation on every token. */
type Pending = { kind: 'text' | 'reasoning'; text: string; serial: number };

/** Add streamed text to the conversation: onto the live entry of its kind
 *  when that is the last one, as a new live entry otherwise. */
function appendDelta(prev: ChatEntry[], d: Pending): ChatEntry[] {
  const prefix = d.kind === 'text' ? 'live:assistant:' : 'live:reasoning:';
  const last = prev.at(-1);
  if (last?.kind === d.kind && last.id.startsWith(prefix) && !last.agentId) {
    const text = last.text + d.text;
    return [...prev.slice(0, -1), { ...last, text, parts: [{ type: d.kind, text }] }];
  }
  return [
    ...prev,
    { id: `${prefix}${d.serial}`, kind: d.kind, role: 'assistant', text: d.text, parts: [{ type: d.kind, text: d.text }] },
  ];
}

/** Run `fn` on the next frame, or soon after where there are no frames. */
function nextFrame(fn: () => void): () => void {
  if (typeof window !== 'undefined' && typeof window.requestAnimationFrame === 'function') {
    const id = window.requestAnimationFrame(fn);
    return () => window.cancelAnimationFrame(id);
  }
  const id = setTimeout(fn, 16);
  return () => clearTimeout(id);
}

/** Hydrates durable messages first, then resumes the canonical event stream at
 *  the snapshot cursor, so a reload — or a second tab — rebuilds the same
 *  conversation with the server as the only source of truth.
 *
 *  Every endpoint, label and formatter is replaceable through the options or a
 *  surrounding provider; see `AgentRunConfig`. */
/** The platform's own event types. Any other type on the thread is an
 *  app's event, handed to `onCustom` as well. */
const PLATFORM_TYPES: ReadonlySet<string> = new Set([
  'CHUNK', 'STATE_CHANGE', 'STEP_COMMITTED', 'STEP_FINISHED', 'INPUT_REQUIRED', 'INPUT_EXPIRED',
  'HITL_RESPONSE', 'MESSAGE_APPENDED', 'MESSAGES_DROPPED', 'CONTEXT_COMPACTED', 'SUBAGENT_STARTED',
  'SUBAGENT_CHUNK', 'SUBAGENT_COMPLETED', 'SUBAGENT_FAILED', 'TEXT_RESULT', 'THREAD_DELETED', 'HEARTBEAT',
  'RUN_REFUSED', 'TOKEN_BUDGET_EXHAUSTED', 'COST_BUDGET_EXHAUSTED', 'RUN_STARTED', 'RUN_ENDED',
  'RECORD_CHANGED', 'SNAPSHOT',
]);

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
  const [activity, setActivityState] = useState<AgentActivity>(() =>
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
  const [currentRun, setCurrentRun] = useState<ThreadRun | null>(null);
  const [connection, setConnection] = useState<ConnectionState>('connecting');
  const [error, setError] = useState<string | null>(null);
  /** Bumped to read the thread again and open a new stream after the old one
   *  closed for good. */
  const [reloadKey, setReloadKey] = useState(0);
  const threadRef = useRef<string | undefined>(threadId);
  threadRef.current = threadId;
  /** What the last render showed, for `run()` to put back if a send fails. */
  const shownRef = useRef({ entries, agentState, activity, pendingInputs, subagents, currentRun });
  shownRef.current = { entries, agentState, activity, pendingInputs, subagents, currentRun };
  /** The activity as last set, so a delta only re-renders when the phase
   *  actually changes. */
  const activityRef = useRef(activity);
  const setActivity = useCallback((next: AgentActivity | ((current: AgentActivity) => AgentActivity)) => {
    setActivityState((current) => {
      const value = typeof next === 'function' ? next(current) : next;
      activityRef.current = value;
      return value;
    });
  }, []);
  /** When each run ended, as its terminal STATE_CHANGE said. A stop is
   *  accepted before the worker that held the run has torn down, so a
   *  snapshot taken in between can still lack the end; the event's word
   *  stands. */
  const runEndings = useRef<Map<string, string>>(new Map());
  /** Tool calls already visible from the durable messages. The snapshot's
   *  activeEvents can replay the same step's chunks, so without this a
   *  reconnect renders a finished tool call twice. */
  const seenToolCalls = useRef<Set<string>>(new Set());
  /** Results already visible from the durable messages, kept apart from the
   *  calls: a live result must still render after its live call did. */
  const seenToolResults = useRef<Set<string>>(new Set());
  /** The seq of the last event applied. An event at or below it is one this
   *  client already has — a replay, or a transport that resent — and is
   *  dropped before anything sees it. Notices (seq 0) always pass. */
  const lastSeqRef = useRef(-1);
  /** Numbers the live entries. Not the event's seq: a run stream item has
   *  none (it arrives as seq 0), and two entries with one id share a React
   *  key. */
  const liveSerial = useRef(0);
  const nextLive = () => ++liveSerial.current;
  /** The run stream being read: its id, the offset of the last item
   *  applied, and every offset applied from it. An item already applied —
   *  one a snapshot covered, or a transport resent — is dropped. A new stream
   *  starts afresh. */
  const streamRef = useRef<{ streamId?: string; offset?: string; seen: Set<string> }>({ seen: new Set() });
  /** The messages the latest snapshot held, so a SNAPSHOT frame, which
   *  carries only the newer ones, can be laid over them. */
  const messagesRef = useRef<SnapshotMessage[]>([]);
  /** Streamed text waiting for the next frame. */
  const pendingDeltas = useRef<Pending[]>([]);
  const cancelFlush = useRef<(() => void) | null>(null);
  /** A send is on its way: a second one would race it (two threads made by
   *  two fast sends on a new thread). */
  const sending = useRef(false);
  /** Only the latest thread-list request may land. */
  const threadsRequest = useRef(0);
  /** Closed streams in a row, for the reconnect backoff. */
  const reconnectTries = useRef(0);
  /** The thread the per-thread state belongs to. */
  const shownThread = useRef<string | undefined>(threadId);

  /** Show the streamed text gathered so far, in one render. */
  const flushDeltas = useCallback(() => {
    cancelFlush.current?.();
    cancelFlush.current = null;
    const batch = pendingDeltas.current;
    if (batch.length === 0) return;
    pendingDeltas.current = [];
    setEntries((prev) => batch.reduce(appendDelta, prev));
  }, []);

  const queueDelta = useCallback(
    (d: Pending) => {
      const batch = pendingDeltas.current;
      const last = batch.at(-1);
      if (last?.kind === d.kind) last.text += d.text;
      else batch.push({ ...d });
      cancelFlush.current ??= nextFrame(() => {
        cancelFlush.current = null;
        flushDeltas();
      });
    },
    [flushDeltas],
  );

  /** Everything that belongs to one thread, back to empty. */
  const clearThreadState = useCallback(() => {
    cancelFlush.current?.();
    cancelFlush.current = null;
    pendingDeltas.current = [];
    lastSeqRef.current = -1;
    streamRef.current = { seen: new Set() };
    messagesRef.current = [];
    // runEndings stays: it is keyed by run id, and a stop's end must outlive
    // a switch away and back.
    seenToolCalls.current = new Set();
    seenToolResults.current = new Set();
    setEntries([]);
    setSubagents([]);
    setPendingInputs([]);
    setUsage(null);
    setCurrentRun(null);
    setError(null);
  }, []);

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

  /** Thread picker / sidebar: best-effort refresh, most recent first. Only
   *  the latest request lands: an older, slower answer never replaces it. */
  const loadThreads = useCallback(async () => {
    const cfg = cfgRef.current;
    const mine = ++threadsRequest.current;
    try {
      setThreadsLoading(true);
      const res = await request(cfg.baseUrl + cfg.routes.threads);
      if (!res.ok || mine !== threadsRequest.current) return;
      const data = await res.json();
      if (mine === threadsRequest.current) setThreads(data.threads ?? []);
    } catch {
      // sidebar is best-effort — ignore transport errors
    } finally {
      if (mine === threadsRequest.current) setThreadsLoading(false);
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
    clearThreadState();
    shownThread.current = undefined;
    setThreadId(undefined);
    setAgentState('IDLE');
    setActivity(stateActivity('IDLE', cfgRef.current.labels));
    cfgRef.current.persistence?.clear();
  }, [clearThreadState, setActivity]);

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
      // Already applied: a replay the snapshot covered, or a transport that
      // resent after reconnecting. Dropped before anything sees it.
      if (data.seq !== 0 && data.seq <= lastSeqRef.current) return;
      if (data.seq !== 0) lastSeqRef.current = data.seq;

      const cfg = cfgRef.current;
      const { labels, format } = cfg;
      const p = data.payload ?? {};

      // Streamed text is gathered and shown once per frame; the activity only
      // changes when the phase does.
      if (data.type === 'CHUNK' && (p?.type === 'text-delta' || p?.type === 'reasoning')) {
        if (cfg.onEvent?.(data) === true) return;
        const kind = p.type === 'text-delta' ? 'text' : 'reasoning';
        const phase = kind === 'text' ? 'responding' : 'thinking';
        if (activityRef.current.phase !== phase) {
          setActivity({ phase, label: kind === 'text' ? labels.responding : labels.thinking });
        }
        // Providers that do not expose reasoning send none; an empty one is
        // only a phase change.
        if (typeof p.textDelta === 'string' && p.textDelta) {
          queueDelta({ kind, text: p.textDelta, serial: nextLive() });
        }
        return;
      }
      // Anything else lands after the text before it.
      flushDeltas();

      // The app sees every event first, and can claim it.
      if (cfg.onEvent?.(data) === true) return;

      switch (data.type) {
        case 'STATE_CHANGE': {
          const nextState = p.state as AgentState;
          setAgentState(nextState);
          if (typeof p.runId === 'string' && typeof p.endedAt === 'string') {
            runEndings.current.set(p.runId, p.endedAt);
          }
          setCurrentRun((prev) => runFromStateChange(prev, nextState, p, data.createdAt));
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
            // The park already said what it waits on: a tool waiting on its
            // own work is not "waiting for approval".
            if (nextState === 'WAITING_FOR_INPUT' && current.phase === 'waiting-input') return current;
            const next = stateActivity(nextState, labels);
            // A failure says why.
            return nextState === 'FAILED' && typeof p.error === 'string' ? { ...next, detail: p.error } : next;
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

        // Another client sent a message on this thread, or this one's send
        // was confirmed. The sending client added it to its own state before
        // the request went out, so this is where every OTHER one learns what
        // was asked.
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
            // (editing a message needs it), otherwise it would show twice. It
            // is found by the id this client gave it; a server that does not
            // echo one is matched by text.
            if (prev.some((e) => e.id === entry.id)) return prev;
            const optimistic =
              typeof p.clientMessageId === 'string'
                ? prev.findIndex((e) => e.id === `optimistic:user:${p.clientMessageId}`)
                : prev.findIndex((e) => e.id.startsWith('optimistic:user:') && e.text === entry.text);
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

        // A generate-text agent streams nothing: its whole answer arrives here.
        case 'TEXT_RESULT': {
          if (typeof p.text !== 'string' || !p.text) break;
          setEntries((prev) => [
            ...prev,
            {
              id: `live:text-result:${nextLive()}`,
              kind: 'text',
              role: 'assistant',
              text: p.text,
              parts: [{ type: 'text', text: p.text }],
            },
          ]);
          break;
        }

        // The thread is gone, deleted here or elsewhere: start afresh.
        case 'THREAD_DELETED': {
          if (!p.threadId || p.threadId === threadRef.current) {
            newThread();
            void loadThreads();
          }
          break;
        }

        // An approval was answered, here or in another tab: its card goes.
        case 'HITL_RESPONSE': {
          setPendingInputs((prev) => prev.filter((r) => r.toolCallId !== p.toolCallId));
          break;
        }

        // The server would not start the run: billing, a full queue.
        case 'RUN_REFUSED': {
          const why = typeof p.error === 'string' ? p.error : String(p.reason ?? '');
          setError(why || labels.runRefused);
          setActivity({ phase: 'failed', label: labels.runRefused, ...(why ? { detail: why } : {}) });
          break;
        }

        case 'CHUNK': {
          if (p?.type === 'source') {
            setActivity({ phase: 'thinking', label: labels.reviewingSources });
          } else if (p?.type === 'tool-call-streaming-start' || p?.type === 'tool-call-delta') {
            if (activityRef.current.phase !== 'tool-call' || activityRef.current.detail !== p.toolName) {
              setActivity({ phase: 'tool-call', label: labels.preparingToolCall, detail: p.toolName });
            }
          } else if (p?.type === 'tool-call') {
            if (p.toolCallId && seenToolCalls.current.has(p.toolCallId)) break; // already durable
            if (p.toolCallId) seenToolCalls.current.add(p.toolCallId);
            setActivity({ phase: 'tool-call', label: labels.callingTool, detail: p.toolName });
            setEntries((prev) => [
              ...prev,
              {
                id: `live:tool-call:${nextLive()}`,
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
                id: `live:tool-result:${nextLive()}`,
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
              id: `live:subagent:${p.agentId}:${nextLive()}`,
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
          // The child's answer only: its thinking and its tool traffic are
          // not what it said.
          if (p.chunk?.type !== 'text-delta' || typeof p.chunk.textDelta !== 'string') break;
          setSubagents((prev) =>
            prev.map((s) =>
              s.agentId === p.agentId ? { ...s, text: s.text + p.chunk.textDelta } : s,
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
    [flushDeltas, loadThreads, loadUsage, newThread, queueDelta, setActivity],
  );

  /** One item from a run stream. A new stream starts its own cursor; an
   *  item already applied from this one is dropped. The reducer sees it as
   *  the thread event it stands for (see streamItemEvents). */
  const applyStreamItem = useCallback(
    (streamId: string, item: WireStreamItem) => {
      const at = streamRef.current;
      if (at.streamId !== streamId) streamRef.current = { streamId, seen: new Set() };
      const cur = streamRef.current;
      if (cur.seen.has(item.offset)) return;
      cur.seen.add(item.offset);
      cur.offset = item.offset;
      if (item.type === 'CUSTOM') cfgRef.current.onCustom?.(String(item.name), item.value);
      for (const event of streamItemEvents(item)) applyEvent(event);
    },
    [applyEvent],
  );

  /** Show a snapshot: the durable messages, the subagent cards, the run's
   *  clocks, the unfinished run's record entries and its run stream. The
   *  history read and a SNAPSHOT frame both come through here. */
  const hydrate = useCallback(
    (snapshot: ThreadSnapshot, forThread: string) => {
      const cfg = cfgRef.current;
      // The snapshot is the whole durable truth: a re-read after a closed
      // stream starts from it too, with nothing streamed half-shown.
      cancelFlush.current?.();
      cancelFlush.current = null;
      pendingDeltas.current = [];

      // A nested run's turns live in the same log under its own agentId.
      // They are its transcript, not the main conversation's.
      messagesRef.current = snapshot.messages;
      const mainMessages = snapshot.messages.filter((m) => (m.agentId ?? null) === null);
      // A durable result settles its call, as done or as failed: a denied
      // approval or a stop never streams a result, so this is where a
      // reload learns how those calls ended.
      const outcomes = toolCallOutcomes(snapshot.messages);
      setEntries(mainMessages.flatMap((m) => messageToEntries(m, cfg.format, outcomes)));

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
      seenToolResults.current = new Set(outcomes.keys());

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
            // A nested run is never queued: it runs inside its parent's
            // segment. The type allows QUEUED for the dispatched run only.
            {
              agentId: r.id,
              name: r.agent,
              depth: r.depth,
              status: r.state === 'QUEUED' ? 'RUNNING' : r.state,
              text: '',
            },
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
      void loadUsage(forThread);
      // The latest run's clocks. An end this client already saw on the
      // wire stands over a snapshot that does not carry it yet.
      const run = latestRun(snapshot.runs);
      if (run && !run.endedAt) {
        const ended = runEndings.current.get(run.id);
        if (ended) run.endedAt = ended;
      }
      setCurrentRun(run);
      setAgentState(snapshot.thread.state);
      setActivity(stateActivity(snapshot.thread.state, cfg.labels));

      // The active run's record entries are at or below the snapshot's
      // cursor, so the cursor starts before them; after, it is the
      // snapshot's.
      lastSeqRef.current = -1;
      for (const event of snapshot.activeEvents) applyEvent(event);
      // The run stream's items the messages do not have yet, then the
      // stream's own offset: a live read picks up after it.
      streamRef.current = { seen: new Set() };
      if (snapshot.stream) {
        for (const item of snapshot.stream.items) applyStreamItem(snapshot.stream.streamId, item);
        streamRef.current.streamId = snapshot.stream.streamId;
        if (snapshot.stream.offset) streamRef.current.offset = snapshot.stream.offset;
      }
      flushDeltas();
      lastSeqRef.current = Math.max(lastSeqRef.current, snapshot.lastEventSeq);
    },
    [applyEvent, applyStreamItem, flushDeltas, loadUsage, setActivity],
  );

  /** Where the hook is, as the server reads it. */
  const cursor = useCallback(
    () => formatCursor({ seq: lastSeqRef.current, streamId: streamRef.current.streamId, offset: streamRef.current.offset }),
    [],
  );

  /** One frame off the wire: a thread event, a run stream item, or a
   *  SNAPSHOT that lays the messages it carries over the ones on screen.
   *  A plain event is what a server older than run streams sends. */
  const applyFrame = useCallback(
    (frame: FollowFrame | StreamEvent, forThread: string) => {
      if (!('kind' in frame)) {
        applyEvent(frame);
        return;
      }
      switch (frame.kind) {
        case 'thread':
          if (!PLATFORM_TYPES.has(frame.event.type) && (frame.event.seq === 0 || frame.event.seq > lastSeqRef.current)) {
            cfgRef.current.onCustom?.(frame.event.type, frame.event.payload);
          }
          applyEvent(frame.event);
          return;
        case 'stream':
          applyStreamItem(frame.streamId, frame.item);
          return;
        case 'snapshot': {
          const known = new Set(messagesRef.current.map((m) => m.id));
          const messages = [...messagesRef.current, ...frame.snapshot.messages.filter((m) => !known.has(m.id))];
          hydrate({ ...frame.snapshot, messages }, forThread);
          return;
        }
      }
    },
    [applyEvent, applyStreamItem, hydrate],
  );

  useEffect(() => {
    if (!threadId) return;
    const cfg = cfgRef.current;
    cfg.persistence?.save(threadId);

    // Another thread's messages, cards and run must not stay on screen while
    // this one loads. A thread this client just created keeps what it shows:
    // that is its own optimistic turn.
    if (shownThread.current !== undefined && shownThread.current !== threadId) clearThreadState();
    shownThread.current = threadId;

    let cancelled = false;
    let stream: { close(): void } | undefined;
    let retry: ReturnType<typeof setTimeout> | undefined;
    // A slow answer for a thread the user already left must not land.
    const abort = new AbortController();
    setHistoryLoading(true);
    setConnection('connecting');
    setActivity({ phase: 'loading', label: cfg.labels.loading });

    void (async () => {
      try {
        const res = await request(routeUrl(cfg.routes.history, { threadId }, cfg.baseUrl), {
          signal: abort.signal,
        });
        if (cancelled) return;
        if (res.status === 404) {
          cfg.persistence?.clear();
          clearThreadState();
          shownThread.current = undefined;
          setThreadId(undefined);
          setAgentState('IDLE');
          setActivity(stateActivity('IDLE', cfg.labels));
          return;
        }
        if (!res.ok) throw new Error(`History request failed (${res.status})`);
        const snapshot = (await res.json()) as ThreadSnapshot;
        if (cancelled) return;

        hydrate(snapshot, threadId);

        stream = cfg.openStream(
          routeUrl(
            cfg.routes.stream,
            {
              threadId,
              since: lastSeqRef.current,
              cursor: cursor(),
              lastMessageId: messagesRef.current.at(-1)?.id,
            },
            cfg.baseUrl,
          ),
          {
            onMessage: (raw) => {
              let frame: FollowFrame | StreamEvent;
              try {
                frame = JSON.parse(raw) as FollowFrame | StreamEvent;
              } catch {
                return; // a frame that is not an event: nothing to show
              }
              applyFrame(frame, threadId);
            },
            onError: () => {
              // The transport retries on its own; keep the last meaningful
              // activity rather than showing a passing network blip.
              if (!cancelled) setConnection('reconnecting');
            },
            onOpen: () => {
              if (cancelled) return;
              reconnectTries.current = 0;
              setConnection('open');
            },
            onClose: () => {
              // The transport gave up for good (a 401, a 404): read the
              // thread again and open a new stream from where it stands,
              // waiting longer each time it happens in a row.
              if (cancelled) return;
              setConnection('closed');
              const wait = Math.min(RECONNECT_BASE_MS * 2 ** reconnectTries.current, RECONNECT_MAX_MS);
              reconnectTries.current += 1;
              retry = setTimeout(() => setReloadKey((k) => k + 1), wait);
            },
            getCursor: cursor,
          },
        );
        setConnection('open');
      } catch (err) {
        if (cancelled) return;
        setAgentState('FAILED');
        setActivity({
          phase: 'failed',
          label: cfgRef.current.labels.loadFailed,
          detail: err instanceof Error ? err.message : String(err),
        });
      } finally {
        if (!cancelled) setHistoryLoading(false);
      }
    })();

    return () => {
      cancelled = true;
      abort.abort();
      if (retry) clearTimeout(retry);
      stream?.close();
      flushDeltas();
    };
  }, [applyFrame, clearThreadState, cursor, flushDeltas, hydrate, request, setActivity, threadId, reloadKey]);

  const run = useCallback(
    async (prompt: string, options: RunOptions = {}): Promise<RunResult> => {
      const cfg = cfgRef.current;
      const { model = cfg.defaultModel, editMessageId, attachments, ...rest } = options;
      const shown = shownRef.current;

      // One run at a time: a send while one is going would wipe its approval
      // cards, and two fast sends on a new thread would make two threads.
      if (sending.current || ACTIVE.includes(shown.agentState)) {
        setError(cfg.labels.runBusy);
        return { accepted: false, threadId: threadRef.current, error: cfg.labels.runBusy };
      }
      // A turn the server has not confirmed has no id it knows.
      if (editMessageId?.startsWith('optimistic:')) {
        setError(cfg.labels.editUnconfirmed);
        return { accepted: false, threadId: threadRef.current, error: cfg.labels.editUnconfirmed };
      }
      sending.current = true;

      // Named here, echoed back on the turn's MESSAGE_APPENDED, so the real
      // message replaces exactly this optimistic one.
      const clientMessageId = `cm-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
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
          { id: `optimistic:user:${clientMessageId}`, kind: 'text', role: 'user', text: prompt, parts },
        ];
      });
      setSubagents([]);
      setPendingInputs([]);
      // A new run is starting: the last one's clocks are no longer the
      // thread's. The server accepts it as QUEUED; the stream's RUNNING names
      // the run and its start once a worker has it.
      setCurrentRun(null);
      setAgentState('QUEUED');
      setActivity({ phase: 'queued', label: cfg.labels.queued });

      try {
        const response = await postJson(cfg.baseUrl + cfg.routes.run, {
          threadId: threadRef.current,
          prompt,
          // Left out when unset: the server then uses the agent's own model.
          ...(model !== undefined ? { model } : {}),
          editMessageId,
          attachments,
          clientMessageId,
          ...rest,
        });
        // The status first: an error page is not JSON, and its parse error
        // would hide what actually went wrong.
        const data = (await response.json().catch(() => null)) as RunResult | null;
        if (!response.ok || !data?.accepted) {
          throw new Error(data?.error ?? `Run request failed (${response.status})`);
        }
        setError(null);
        if (data.threadId) setThreadId(data.threadId);
        // The stream usually names the run first; a late answer must not
        // wipe the start it already carried.
        if (data.runId) setCurrentRun((prev) => (prev?.id === data.runId ? prev : { id: data.runId! }));
        void loadThreads(); // the sidebar reflects a new thread immediately
        return data;
      } catch (err) {
        // Nothing was sent: the conversation goes back to exactly what it
        // was, and the reason is shown apart from the thread's own state.
        const why = err instanceof Error ? err.message : String(err);
        setEntries(shown.entries);
        setSubagents(shown.subagents);
        setPendingInputs(shown.pendingInputs);
        setCurrentRun(shown.currentRun);
        setAgentState(shown.agentState);
        setActivity(shown.activity);
        setError(why);
        return { accepted: false, threadId: threadRef.current, error: why };
      } finally {
        sending.current = false;
      }
    },
    [loadThreads, postJson, setActivity],
  );

  const stop = useCallback(async () => {
    const requestedThread = threadRef.current;
    if (!requestedThread) return false;
    const cfg = cfgRef.current;
    try {
      const response = await postJson(cfg.baseUrl + cfg.routes.stop, { threadId: requestedThread });
      await checkControlResponse(response, 'accepted', cfg.labels.stopFailed);
      return true;
    } catch (err) {
      if (threadRef.current === requestedThread) setActivity({
        phase: 'failed', label: cfg.labels.stopFailed,
        detail: err instanceof Error ? err.message : String(err),
      });
      return false;
    }
  }, [postJson, setActivity]);

  const respondToInput = useCallback(
    async (toolCallId: string, approved: boolean, payload?: unknown) => {
      const requestedThread = threadRef.current;
      if (!requestedThread) return false;
      const cfg = cfgRef.current;
      try {
        const response = await postJson(cfg.baseUrl + cfg.routes.respond, {
          threadId: requestedThread, toolCallId, approved, payload,
        });
        await checkControlResponse(response, 'delivered', cfg.labels.responseFailed);
        if (threadRef.current === requestedThread) {
          setPendingInputs((prev) => prev.filter((r) => r.toolCallId !== toolCallId));
          // A stream event may already have resumed or finished the run.
          // A delayed HTTP acknowledgement must not replace that activity.
          setActivity((current) => current.phase === 'waiting-input' ||
            (current.phase === 'failed' && current.label === cfg.labels.responseFailed)
            ? { phase: 'thinking', label: approved ? cfg.labels.approvalSent : cfg.labels.requestDenied }
            : current);
        }
        return true;
      } catch (err) {
        if (threadRef.current === requestedThread) setActivity({
          phase: 'failed', label: cfg.labels.responseFailed,
          detail: err instanceof Error ? err.message : String(err),
        });
        return false;
      }
    },
    [postJson, setActivity],
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
    currentRun,
    connection,
    error,
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

async function checkControlResponse(response: Response, field: 'accepted' | 'delivered', label: string): Promise<void> {
  const data = await response.json().catch(() => null);
  // Support custom routes returning { ok: true }, but an explicit refusal
  // always wins over that legacy envelope.
  if (!response.ok || !data || data[field] === false ||
      (data[field] !== true && data.ok !== true)) {
    throw new Error(data?.error ?? data?.reason ?? `${label} (${response.status})`);
  }
}
