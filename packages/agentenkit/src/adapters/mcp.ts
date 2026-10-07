import { spawn, type ChildProcessWithoutNullStreams } from 'node:child_process';
import { jsonSchema, tool, type Tool } from 'ai';
import { markRequiresConfirmation } from '../core/engine.js';

/** The MCP version we ask for. A server may answer with an older one; we
 *  only use initialize, tools/list and tools/call, which every version has. */
export const MCP_PROTOCOL_VERSION = '2025-06-18';

/** How to reach an MCP server: a local command we start and talk to over
 *  stdin/stdout, or a URL that speaks MCP over HTTP ("streamable HTTP"). */
export type MCPTransport =
  | { type: 'stdio'; command: string; args?: string[]; env?: Record<string, string>; cwd?: string }
  | { type: 'http'; url: string; headers?: Record<string, string>; fetch?: typeof fetch };

export interface MCPServerOptions {
  /** A short name for the server, like 'github'. It is the default prefix of
   *  the tool names, so two servers can both have a `search` tool. */
  name: string;
  transport: MCPTransport;
  /** Put before each tool's name. Default: `${name}_`. Pass '' for none. */
  prefix?: string;
  /** Only these tools (by the server's own names). Default: all of them. */
  tools?: string[];
  /** Park these tools behind an approval before they run (§2.5): true for
   *  all, or a list of the server's own tool names. */
  requiresConfirmation?: boolean | string[];
  /** How long one request may take, in ms. Default 60000. */
  timeoutMs?: number;
}

export interface MCPConnection {
  readonly name: string;
  /** Ready to spread into an agent's `tools`. */
  readonly tools: Record<string, Tool>;
  /** Stops the server process, or ends the HTTP session. */
  close(): Promise<void>;
}

interface RpcError { code: number; message: string }
interface RpcMessage { jsonrpc: '2.0'; id?: number | string; method?: string; params?: unknown; result?: unknown; error?: RpcError }

interface MCPToolInfo { name: string; description?: string; inputSchema?: Record<string, unknown> }
interface MCPContent { type: string; text?: string; mimeType?: string; resource?: { uri?: string; text?: string } }

const INIT_PARAMS = { protocolVersion: MCP_PROTOCOL_VERSION, capabilities: {}, clientInfo: { name: 'agentenkit', version: '1' } };

/** One way to send a JSON-RPC request and get its answer back. */
interface Channel {
  request(method: string, params: unknown, signal?: AbortSignal): Promise<unknown>;
  notify(method: string, params?: unknown): Promise<void>;
  close(): Promise<void>;
}

/** Connects to one MCP server, lists its tools and turns each into an
 *  ordinary AI SDK tool that calls the server. Keep the connection for as
 *  long as the workers run, and close it on shutdown.
 *
 * ```ts
 * const github = await connectMCP({ name: 'github', transport: { type: 'stdio', command: 'github-mcp-server', args: ['stdio'] } });
 * runtime.createStreamTextAgent({ name: 'dev', tools: { ...github.tools } });
 * ``` */
export async function connectMCP(options: MCPServerOptions): Promise<MCPConnection> {
  if (!options.name) throw new Error('connectMCP: name is required');
  const timeoutMs = options.timeoutMs ?? 60_000;
  const channel = options.transport.type === 'stdio'
    ? stdioChannel(options.name, options.transport, timeoutMs)
    : httpChannel(options.name, options.transport, timeoutMs);
  try {
    await channel.request('initialize', INIT_PARAMS);
    await channel.notify('notifications/initialized');
    const infos = await listTools(channel);
    const tools = buildTools(options, infos, channel);
    return { name: options.name, tools, close: () => channel.close() };
  } catch (err) {
    await channel.close().catch(() => {});
    throw err;
  }
}

/** Connects to several servers at once and merges their tools. Two tools
 *  with the same final name throw, so one never hides the other. */
export async function connectMCPServers(servers: MCPServerOptions[]): Promise<MCPConnection> {
  const settled = await Promise.allSettled(servers.map(connectMCP));
  const conns = settled.flatMap((s) => (s.status === 'fulfilled' ? [s.value] : []));
  const close = async () => { await Promise.allSettled(conns.map((c) => c.close())); };
  const failed = settled.find((s) => s.status === 'rejected') as PromiseRejectedResult | undefined;
  if (failed) { await close(); throw failed.reason; }
  const tools: Record<string, Tool> = {};
  for (const c of conns) {
    for (const [name, t] of Object.entries(c.tools)) {
      if (tools[name]) { await close(); throw new Error(`connectMCPServers: two tools are called ${JSON.stringify(name)}; give a server a prefix`); }
      tools[name] = t;
    }
  }
  return { name: servers.map((s) => s.name).join(','), tools, close };
}

async function listTools(channel: Channel): Promise<MCPToolInfo[]> {
  const out: MCPToolInfo[] = [];
  let cursor: string | undefined;
  do {
    const res = (await channel.request('tools/list', cursor ? { cursor } : {})) as { tools?: MCPToolInfo[]; nextCursor?: string };
    out.push(...(res.tools ?? []));
    cursor = res.nextCursor || undefined;
  } while (cursor);
  return out;
}

/** Tool names providers accept: letters, digits, _ and -, at most 64. */
export function mcpToolName(prefix: string, name: string): string {
  return (prefix + name).replace(/[^a-zA-Z0-9_-]/gu, '_').slice(0, 64);
}

function buildTools(options: MCPServerOptions, infos: MCPToolInfo[], channel: Channel): Record<string, Tool> {
  const prefix = options.prefix ?? `${options.name}_`;
  const only = options.tools ? new Set(options.tools) : null;
  const confirm = options.requiresConfirmation;
  const out: Record<string, Tool> = {};
  for (const info of infos) {
    if (only && !only.has(info.name)) continue;
    const name = mcpToolName(prefix, info.name);
    if (out[name]) throw new Error(`connectMCP(${options.name}): two tools are called ${JSON.stringify(name)}`);
    const schema = info.inputSchema && typeof info.inputSchema === 'object' ? info.inputSchema : { type: 'object', properties: {} };
    const t = tool({
      description: info.description ?? '',
      inputSchema: jsonSchema(schema as Parameters<typeof jsonSchema>[0]),
      execute: async (args: unknown, opts: { abortSignal?: AbortSignal }): Promise<unknown> => {
        try {
          const res = (await channel.request('tools/call', { name: info.name, arguments: args ?? {} }, opts.abortSignal)) as {
            content?: MCPContent[]; structuredContent?: unknown; isError?: boolean;
          };
          const text = contentText(res);
          return res.isError ? { error: text || 'the tool failed' } : text;
        } catch (err) {
          return { error: `${options.name}: ${(err as Error).message}` };
        }
      },
    });
    out[name] = confirm === true || (Array.isArray(confirm) && confirm.includes(info.name)) ? markRequiresConfirmation(t) : t;
  }
  if (only) {
    for (const want of only) {
      if (!infos.some((i) => i.name === want)) throw new Error(`connectMCP(${options.name}): the server has no tool called ${JSON.stringify(want)}`);
    }
  }
  return out;
}

/** What the model reads back: the text parts joined, and a short note for
 *  anything that is not text. With no content, the structured result. */
export function contentText(res: { content?: MCPContent[]; structuredContent?: unknown }): string {
  const parts: string[] = [];
  for (const c of res.content ?? []) {
    if (c.type === 'text') parts.push(c.text ?? '');
    else if (c.type === 'resource' && c.resource?.text !== undefined) parts.push(c.resource.text);
    else parts.push(`[${c.type}${c.mimeType ? ` ${c.mimeType}` : ''}${c.resource?.uri ? ` ${c.resource.uri}` : ''}]`);
  }
  if (parts.length === 0 && res.structuredContent !== undefined) return JSON.stringify(res.structuredContent);
  return parts.join('\n');
}

/** Waits for an answer, gives up after timeoutMs or when signal aborts. */
function withDeadline<T>(p: Promise<T>, timeoutMs: number, signal: AbortSignal | undefined, onGiveUp: () => void, what: string): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const done = () => { clearTimeout(timer); signal?.removeEventListener('abort', onAbort); };
    const timer = setTimeout(() => { done(); onGiveUp(); reject(new Error(`${what} timed out after ${timeoutMs}ms`)); }, timeoutMs);
    const onAbort = () => { done(); onGiveUp(); reject(new Error(`${what} was stopped`)); };
    if (signal?.aborted) return onAbort();
    signal?.addEventListener('abort', onAbort, { once: true });
    p.then((v) => { done(); resolve(v); }, (e) => { done(); reject(e); });
  });
}

function stdioChannel(server: string, t: Extract<MCPTransport, { type: 'stdio' }>, timeoutMs: number): Channel {
  const child: ChildProcessWithoutNullStreams = spawn(t.command, t.args ?? [], {
    env: { ...process.env, ...t.env },
    ...(t.cwd ? { cwd: t.cwd } : {}),
    stdio: ['pipe', 'pipe', 'pipe'],
  });
  let nextId = 1;
  let closed: Error | null = null;
  // Settles on 'exit', or on 'error' when the command never started.
  const gone = new Promise<void>((r) => { child.once('exit', () => r()); child.once('error', () => r()); });
  const waiting = new Map<number, { resolve: (v: unknown) => void; reject: (e: Error) => void }>();
  const failAll = (err: Error) => {
    closed ??= err;
    for (const w of waiting.values()) w.reject(err);
    waiting.clear();
  };
  const write = (msg: RpcMessage) => { if (!closed) child.stdin.write(JSON.stringify(msg) + '\n'); };

  let buf = '';
  child.stdout.setEncoding('utf8');
  child.stdout.on('data', (chunk: string) => {
    buf += chunk;
    let nl: number;
    while ((nl = buf.indexOf('\n')) >= 0) {
      const line = buf.slice(0, nl).trim();
      buf = buf.slice(nl + 1);
      if (!line) continue;
      let msg: RpcMessage;
      try { msg = JSON.parse(line); } catch { continue; } // a log line on stdout
      handle(msg);
    }
  });
  child.stderr.on('data', () => {}); // drain, or a chatty server blocks
  child.on('error', (e) => failAll(new Error(`${server}: ${e.message}`)));
  child.on('exit', (code) => failAll(new Error(`${server}: the server exited (code ${code})`)));
  child.stdin.on('error', () => {});

  function handle(msg: RpcMessage) {
    if (msg.method !== undefined) {
      // A request from the server. We offer no client features, so answer
      // ping and turn down the rest.
      if (msg.id === undefined) return;
      if (msg.method === 'ping') write({ jsonrpc: '2.0', id: msg.id, result: {} });
      else write({ jsonrpc: '2.0', id: msg.id, error: { code: -32601, message: `method not found: ${msg.method}` } });
      return;
    }
    const w = typeof msg.id === 'number' ? waiting.get(msg.id) : undefined;
    if (!w) return;
    waiting.delete(msg.id as number);
    if (msg.error) w.reject(new Error(msg.error.message));
    else w.resolve(msg.result);
  }

  return {
    request(method, params, signal) {
      if (closed) return Promise.reject(closed);
      const id = nextId++;
      const p = new Promise<unknown>((resolve, reject) => waiting.set(id, { resolve, reject }));
      write({ jsonrpc: '2.0', id, method, params });
      return withDeadline(p, timeoutMs, signal, () => {
        waiting.delete(id);
        write({ jsonrpc: '2.0', method: 'notifications/cancelled', params: { requestId: id } });
      }, `${server}: ${method}`);
    },
    async notify(method, params) { write({ jsonrpc: '2.0', method, ...(params !== undefined ? { params } : {}) }); },
    async close() {
      if (closed) return;
      closed = new Error(`${server}: the connection is closed`);
      child.stdin.end();
      const timer = setTimeout(() => child.kill('SIGTERM'), 2000);
      const killer = setTimeout(() => child.kill('SIGKILL'), 5000);
      await gone;
      clearTimeout(timer);
      clearTimeout(killer);
    },
  };
}

function httpChannel(server: string, t: Extract<MCPTransport, { type: 'http' }>, timeoutMs: number): Channel {
  const doFetch = t.fetch ?? fetch;
  let nextId = 1;
  let sessionId: string | null = null;
  let version: string | null = null;

  const headers = (): Record<string, string> => ({
    ...t.headers,
    'content-type': 'application/json',
    accept: 'application/json, text/event-stream',
    ...(sessionId ? { 'mcp-session-id': sessionId } : {}),
    ...(version ? { 'mcp-protocol-version': version } : {}),
  });

  async function post(msg: RpcMessage, signal: AbortSignal): Promise<Response> {
    const res = await doFetch(t.url, { method: 'POST', headers: headers(), body: JSON.stringify(msg), signal });
    const sid = res.headers.get('mcp-session-id');
    if (sid) sessionId = sid;
    return res;
  }

  const channel: Channel = {
    async request(method, params, signal) {
      const id = nextId++;
      const ctl = new AbortController();
      const work = (async () => {
        let res = await post({ jsonrpc: '2.0', id, method, params }, ctl.signal);
        if (res.status === 404 && sessionId && method !== 'initialize') {
          // The server dropped our session: start a new one and try once more.
          await res.body?.cancel();
          sessionId = null;
          version = null;
          await channel.request('initialize', INIT_PARAMS, ctl.signal);
          await channel.notify('notifications/initialized');
          res = await post({ jsonrpc: '2.0', id, method, params }, ctl.signal);
        }
        if (!res.ok) throw new Error(`HTTP ${res.status} ${(await res.text()).slice(0, 200)}`);
        const type = res.headers.get('content-type') ?? '';
        const msg = type.includes('text/event-stream') ? await readSse(res, id) : ((await res.json()) as RpcMessage);
        if (msg.error) throw new Error(msg.error.message);
        if (method === 'initialize') version = ((msg.result as { protocolVersion?: string })?.protocolVersion) ?? MCP_PROTOCOL_VERSION;
        return msg.result;
      })();
      return withDeadline(work, timeoutMs, signal, () => ctl.abort(), `${server}: ${method}`);
    },
    async notify(method, params) {
      const ctl = new AbortController();
      const timer = setTimeout(() => ctl.abort(), timeoutMs);
      try {
        const res = await post({ jsonrpc: '2.0', method, ...(params !== undefined ? { params } : {}) }, ctl.signal);
        await res.body?.cancel();
      } finally { clearTimeout(timer); }
    },
    async close() {
      if (!sessionId) return;
      const ctl = new AbortController();
      const timer = setTimeout(() => ctl.abort(), 5000);
      try { await doFetch(t.url, { method: 'DELETE', headers: headers(), signal: ctl.signal }); } catch { /* the server may not allow it */ }
      finally { clearTimeout(timer); sessionId = null; }
    },
  };
  return channel;
}

/** Reads an SSE answer until the message for `id` comes. Other messages on
 *  the stream (progress, logs) are skipped. */
async function readSse(res: Response, id: number): Promise<RpcMessage> {
  if (!res.body) throw new Error('empty event stream');
  const reader = res.body.pipeThrough(new TextDecoderStream()).getReader();
  let buf = '';
  let data: string[] = [];
  try {
    for (;;) {
      const { value, done } = await reader.read();
      if (done) break;
      buf += value;
      let nl: number;
      while ((nl = buf.indexOf('\n')) >= 0) {
        const line = buf.slice(0, nl).replace(/\r$/, '');
        buf = buf.slice(nl + 1);
        if (line === '') {
          const text = data.join('\n');
          data = [];
          if (!text) continue;
          let msg: RpcMessage;
          try { msg = JSON.parse(text); } catch { continue; }
          if (msg.id === id && msg.method === undefined) return msg;
        } else if (line.startsWith('data:')) {
          data.push(line.slice(5).replace(/^ /, ''));
        }
      }
    }
  } finally {
    reader.cancel().catch(() => {});
  }
  throw new Error('the event stream ended with no answer');
}
