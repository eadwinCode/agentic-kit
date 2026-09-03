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
  /** GET, server-sent events. Resumes from `since`. */
  stream: Route<{ threadId: string; since: number }>;
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
  loadFailed: string;
}

export const defaultLabels: ActivityLabels = {
  idle: 'Idle',
  loading: 'Loading conversation',
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
      window.history.replaceState({}, '', url);
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
      window.history.replaceState({}, '', url);
    },
  };
}

export interface StreamHandlers {
  /** One event frame, still encoded. */
  onMessage: (data: string) => void;
  onError: (error: unknown) => void;
}

export interface StreamSubscription {
  close(): void;
}

/** Open the event stream. Handlers go in and something closeable comes back —
 *  rather than an EventSource look-alike, so an implementation is free to use
 *  fetch streaming, a WebSocket, or a test double without imitating the
 *  browser's property API. */
export type OpenStream = (url: string, handlers: StreamHandlers) => StreamSubscription;

export const browserEventStream: OpenStream = (url, { onMessage, onError }) => {
  const source = new EventSource(url);
  source.onmessage = (event) => onMessage(event.data);
  source.onerror = (event) => onError(event);
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
  /** Used by `run()` when the caller names no model. */
  defaultModel?: string;
  /** `false` keeps the thread id in memory only. */
  persistence?: ThreadPersistence | false;
  labels?: Partial<ActivityLabels>;
  format?: Partial<EntryFormat>;
  /** Every event, before the hook interprets it. Return `true` to say the
   *  event is handled and stop the built-in reducer — that is how an app adds
   *  its own event types, or overrides one. */
  onEvent?: (event: StreamEvent) => boolean | void;
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
  defaultModel: string;
  persistence: ThreadPersistence | null;
  labels: ActivityLabels;
  format: EntryFormat;
  onEvent: ((event: StreamEvent) => boolean | void) | undefined;
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
    defaultModel: config.defaultModel ?? 'gpt-4o',
    persistence:
      config.persistence === false ? null : (config.persistence ?? browserPersistence()),
    labels: { ...defaultLabels, ...config.labels },
    format: { ...defaultFormat, ...config.format },
    onEvent: config.onEvent,
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
