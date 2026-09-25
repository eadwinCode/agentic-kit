import type { StreamEvent } from './types.js';

/** Just the call signature. `typeof fetch` also carries runtime-specific
 *  extras (Bun adds `preconnect`) that a plain wrapper cannot satisfy. */
export type FetchLike = (
  input: RequestInfo | URL,
  init?: RequestInit,
) => Promise<Response>;

/** A route is either a path the hook appends its own query to, or a function
 *  that builds the whole URL. The function form is the escape hatch: it covers
 *  path parameters (`/threads/:id/history`), a different query vocabulary, or
 *  an entirely different host — cases a fixed path cannot express. */
export type Route<P = void> = P extends void
  ? string
  : string | ((params: P) => string);

export interface AgentRoutes {
  /** POST { threadId?, prompt, model, editMessageId? } → RunResult */
  run: string;
  /** POST { threadId } — stop the active run */
  stop: string;
  /** POST { threadId, toolCallId, approved, payload? } — answer an approval */
  respond: string;
  /** GET, server-sent events. Resumes from `cursor` ("<seq> <streamId>
   *  <offset>"); `since` is the record seq alone, for an older server.
   *  `lastMessageId` lets a catch-up carry only the messages after it. */
  stream: Route<{ threadId: string; since: number; cursor: string; lastMessageId?: string }>;
  /** GET → ThreadSnapshot */
  history: Route<{ threadId: string }>;
  /** GET → ThreadUsage */
  usage: Route<{ threadId: string }>;
  /** GET → { threads: ThreadListItem[] } */
  threads: string;
  /** DELETE — remove a thread and everything under it */
  deleteThread: Route<{ threadId: string }>;
}

export const defaultRoutes: AgentRoutes = {
  run: '/api/agent/run',
  stop: '/api/agent/control',
  respond: '/api/agent/respond',
  stream: '/api/agent/stream',
  history: '/api/agent/history',
  usage: '/api/agent/usage',
  threads: '/api/threads',
  deleteThread: '/api/threads',
};

/** Every user-facing string the hook produces, in one place so an app can
 *  translate or reword them without forking the state machine. */
export interface ActivityLabels {
  idle: string;
  loading: string;
  /** The run is accepted and waiting for a worker. */
  queued: string;
  thinking: string;
  responding: string;
  reviewingSources: string;
  preparingToolCall: string;
  callingTool: string;
  toolCompleted: string;
  waitingApproval: string;
  /** A tool parked itself on work in progress; no one is asked anything. */
  waitingWork: string;
  approvalExpired: string;
  approvalSent: string;
  requestDenied: string;
  subagentWorking: string;
  subagentCompleted: string;
  completed: string;
  stopped: string;
  failed: string;
  runFailed: string;
  /** `run()` was called while a run is still going, or a send is in flight. */
  runBusy: string;
  /** An edit named a message the server has not confirmed yet. */
  editUnconfirmed: string;
  /** The server refused the run (billing, a full queue): RUN_REFUSED. */
  runRefused: string;
  stopFailed: string;
  responseFailed: string;
  loadFailed: string;
}

export const defaultLabels: ActivityLabels = {
  idle: 'Idle',
  loading: 'Loading conversation',
  queued: 'Waiting to start',
  thinking: 'Thinking',
  responding: 'Responding',
  reviewingSources: 'Reviewing sources',
  preparingToolCall: 'Preparing tool call',
  callingTool: 'Calling tool',
  toolCompleted: 'Tool completed',
  waitingApproval: 'Waiting for approval',
  waitingWork: 'Waiting for work to finish',
  approvalExpired: 'Approval expired',
  approvalSent: 'Approval sent',
  requestDenied: 'Request denied',
  subagentWorking: 'Subagent working',
  subagentCompleted: 'Subagent completed',
  completed: 'Completed',
  stopped: 'Stopped',
  failed: 'Failed',
  runFailed: 'Could not start run',
  runBusy: 'A run is already in progress',
  editUnconfirmed: 'That message is not saved yet',
  runRefused: 'Run refused',
  stopFailed: 'Could not stop run',
  responseFailed: 'Could not send response',
  loadFailed: 'Could not load conversation',
};

/** How a message turns into display text. The defaults are plain and
 *  ASCII-marked; override to render your own tool cards. */
export interface EntryFormat {
  truncate(text: string, max: number): string;
  toolCall(toolName: string, args: unknown): string;
  toolResult(toolName: string | undefined, result: unknown): string;
  subagentStarted(name: string): string;
}

const json = (value: unknown) => {
  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
};

export const defaultFormat: EntryFormat = {
  truncate: (text, max) => (text.length > max ? `${text.slice(0, max)}…` : text),
  toolCall: (toolName, args) =>
    `⚙ ${toolName}(${defaultFormat.truncate(json(args ?? {}), 120)})`,
  toolResult: (toolName, result) =>
    `↳ ${toolName ? `${toolName}: ` : ''}${defaultFormat.truncate(json(result), 220)}`,
  subagentStarted: (name) => `▸ subagent "${name}" started`,
};

/** Where the open thread id is remembered between reloads. This is app policy,
 *  not library policy — an app with its own router should supply its own, and
 *  one that wants a clean slate every load can pass `false`. */
export interface ThreadPersistence {
  load(): string | undefined;
  save(threadId: string): void;
  clear(): void;
}

/** The default: a `threadId` query parameter so a conversation is linkable,
 *  backed by localStorage so a bare visit reopens the last one. */
export function browserPersistence(storageKey = 'use-agentenkit:last-thread'): ThreadPersistence {
  const canUseDom = () => typeof window !== 'undefined';
  const read = (): string | null => {
    try {
      return window.localStorage.getItem(storageKey);
    } catch {
      return null; // private mode, or storage disabled
    }
  };
  return {
    load: () => {
      if (!canUseDom()) return undefined;
      const fromUrl = new URLSearchParams(window.location.search).get('threadId');
      return fromUrl || read() || undefined;
    },
    save: (threadId) => {
      if (!canUseDom()) return;
      try {
        window.localStorage.setItem(storageKey, threadId);
      } catch {
        // storage is a convenience; never break the conversation over it
      }
      const url = new URL(window.location.href);
      url.searchParams.set('threadId', threadId);
      // The router's own state is passed through: replacing it with {} breaks
      // the Next.js App Router, which keeps its tree there.
      window.history.replaceState(window.history.state, '', url);
    },
    clear: () => {
      if (!canUseDom()) return;
      try {
        window.localStorage.removeItem(storageKey);
      } catch {
        // as above
      }
      const url = new URL(window.location.href);
      url.searchParams.delete('threadId');
      window.history.replaceState(window.history.state, '', url);
    },
  };
}

export interface StreamHandlers {
  /** One event frame, still encoded. */
  onMessage: (data: string) => void;
  /** A transient failure: the transport is reconnecting on its own. */
  onError: (error: unknown) => void;
  /** The stream is open, first time or after a reconnect. Optional for a
   *  transport to call: the hook takes a stream it was handed as open. */
  onOpen?: () => void;
  /** The stream is closed for good: the transport gave up (a 401, a 404).
   *  The hook re-reads the thread and opens a new stream, with a backoff. */
  onClose?: () => void;
  /** Where the hook is: "<seq> <streamId> <offset>", the id of the last
   *  frame it applied. A transport that reconnects on its own resumes after
   *  it, so nothing is shown twice or missed. */
  getCursor: () => string;
}

export interface StreamSubscription {
  close(): void;
}

/** Open the event stream. Handlers go in and something closeable comes back —
 *  rather than an EventSource look-alike, so an implementation is free to use
 *  fetch streaming, a WebSocket, or a test double without imitating the
 *  browser's property API. */
export type OpenStream = (url: string, handlers: StreamHandlers) => StreamSubscription;

export const browserEventStream: OpenStream = (url, { onMessage, onError, onOpen, onClose }) => {
  const source = new EventSource(url);
  source.onopen = () => onOpen?.();
  source.onmessage = (event) => onMessage(event.data);
  // EventSource retries a dropped connection on its own, and gives up for
  // good on an HTTP error (a 401, a 404): it is then CLOSED, and only a new
  // stream can carry on.
  source.onerror = (event) => (source.readyState === EventSource.CLOSED ? onClose?.() : onError(event));
  return { close: () => source.close() };
};

export interface AgentRunConfig {
  /** Override any subset; the rest keep their defaults. */
  routes?: Partial<AgentRoutes>;
  /** Prefix applied to string routes — for an API on another origin. Routes
   *  given as functions build their own URL and are left alone. */
  baseUrl?: string;
  /** Swap the transport: auth wrappers, retries, a test double. */
  fetch?: FetchLike;
  /** Sent with every request the hook makes. A function is called per request,
   *  so a token that rotates stays fresh. */
  headers?: HeadersInit | (() => HeadersInit | Promise<HeadersInit>);
  /** Open the event stream. The default uses the browser's EventSource, which
   *  CANNOT send headers — so `headers` does not reach the stream. An API
   *  behind a bearer token needs its own implementation here (or a cookie). */
  openStream?: OpenStream;
  /** Used by `run()` when the caller names no model. Unset (the default)
   *  sends none, and the server uses the agent's own model. */
  defaultModel?: string;
  /** `false` keeps the thread id in memory only. */
  persistence?: ThreadPersistence | false;
  labels?: Partial<ActivityLabels>;
  format?: Partial<EntryFormat>;
  /** Every event, before the hook interprets it. Return `true` to say the
   *  event is handled and stop the built-in reducer — that is how an app adds
   *  its own event types, or overrides one. */
  onEvent?: (event: StreamEvent) => boolean | void;
  /** An app's own event, by name: one a tool sent with `publishEvent`, from
   *  the run stream or, when durable, from the thread record. */
  onCustom?: (name: string, value: unknown) => void;
  /** Background refresh for the thread list, in ms. `false` disables it —
   *  other tabs and the worker can change thread state without this tab
   *  seeing an event, which is the only reason it exists. */
  threadsRefreshMs?: number | false;
  /** Load the thread list on mount. Off for a single-thread embed. */
  loadThreadsOnMount?: boolean;
}

export interface ResolvedConfig {
  routes: AgentRoutes;
  baseUrl: string;
  fetch: FetchLike;
  headers: () => HeadersInit | Promise<HeadersInit>;
  openStream: OpenStream;
  defaultModel: string | undefined;
  persistence: ThreadPersistence | null;
  labels: ActivityLabels;
  format: EntryFormat;
  onEvent: ((event: StreamEvent) => boolean | void) | undefined;
  onCustom: ((name: string, value: unknown) => void) | undefined;
  threadsRefreshMs: number | false;
  loadThreadsOnMount: boolean;
}

export function resolveConfig(config: AgentRunConfig = {}): ResolvedConfig {
  // Hoisted: TypeScript cannot narrow `config.headers` inside the closure.
  const headers = config.headers;
  return {
    routes: { ...defaultRoutes, ...config.routes },
    baseUrl: config.baseUrl ?? '',
    // Bound to globalThis: an unbound reference throws "Illegal invocation"
    // in the browser.
    fetch: config.fetch ?? ((...args) => globalThis.fetch(...args)),
    headers: typeof headers === 'function' ? headers : () => headers ?? {},
    openStream: config.openStream ?? browserEventStream,
    defaultModel: config.defaultModel,
    persistence:
      config.persistence === false ? null : (config.persistence ?? browserPersistence()),
    labels: { ...defaultLabels, ...config.labels },
    format: { ...defaultFormat, ...config.format },
    onEvent: config.onEvent,
    onCustom: config.onCustom,
    threadsRefreshMs: config.threadsRefreshMs ?? 30_000,
    loadThreadsOnMount: config.loadThreadsOnMount ?? true,
  };
}

/** Append query parameters without disturbing a relative path or a query the
 *  route already carries. `new URL` cannot be used here — it would force every
 *  route to be absolute. */
export function withQuery(
  path: string,
  params: Record<string, string | number | undefined>,
): string {
  const search = Object.entries(params)
    .filter(([, value]) => value !== undefined)
    .map(([key, value]) => `${encodeURIComponent(key)}=${encodeURIComponent(String(value))}`)
    .join('&');
  if (!search) return path;
  return `${path}${path.includes('?') ? '&' : '?'}${search}`;
}

/** Resolve one route to a URL. A string gets the base prefix and the hook's
 *  standard query; a function is trusted to return a complete URL. */
export function routeUrl<P extends Record<string, string | number | undefined>>(
  route: string | ((params: P) => string),
  params: P,
  baseUrl: string,
): string {
  if (typeof route === 'function') return route(params);
  return withQuery(baseUrl + route, params);
}
