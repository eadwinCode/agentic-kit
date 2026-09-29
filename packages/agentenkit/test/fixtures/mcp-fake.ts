// A tiny MCP server for the tests: the same tools over stdio (run this file)
// and over HTTP (mcpHttpHandler). The Go tests run their own copy.

type Msg = { jsonrpc: '2.0'; id?: number | string; method?: string; params?: any; result?: unknown; error?: unknown };

const PAGES = [
  [
    { name: 'echo', description: 'Says the text back', inputSchema: { type: 'object', properties: { text: { type: 'string' } }, required: ['text'] } },
    { name: 'fail', description: 'Always fails', inputSchema: { type: 'object', properties: {} } },
  ],
  [
    { name: 'add', description: 'Adds two numbers', inputSchema: { type: 'object', properties: { a: { type: 'number' }, b: { type: 'number' } } } },
    { name: 'weird.name', description: 'A name with a dot' },
    { name: 'slow', description: 'Never answers in time', inputSchema: { type: 'object', properties: {} } },
  ],
];

/** The answer to one message, or null for a notification. */
export function answer(msg: Msg): Msg | null {
  if (msg.id === undefined) return null;
  const ok = (result: unknown): Msg => ({ jsonrpc: '2.0', id: msg.id, result });
  switch (msg.method) {
    case 'initialize':
      return ok({ protocolVersion: '2025-06-18', capabilities: { tools: {} }, serverInfo: { name: 'fake', version: '1' } });
    case 'tools/list':
      return msg.params?.cursor === 'p2' ? ok({ tools: PAGES[1] }) : ok({ tools: PAGES[0], nextCursor: 'p2' });
    case 'tools/call': {
      const { name, arguments: args } = msg.params;
      if (name === 'echo') return ok({ content: [{ type: 'text', text: `echo: ${args.text}` }] });
      if (name === 'fail') return ok({ content: [{ type: 'text', text: 'it broke' }], isError: true });
      if (name === 'add') return ok({ content: [], structuredContent: { sum: args.a + args.b } });
      if (name === 'weird.name') return ok({ content: [{ type: 'text', text: 'hi' }, { type: 'image', mimeType: 'image/png', data: 'AA==' }] });
      if (name === 'slow') return null;
      return { jsonrpc: '2.0', id: msg.id, error: { code: -32602, message: `no tool ${name}` } };
    }
    default:
      return { jsonrpc: '2.0', id: msg.id, error: { code: -32601, message: 'method not found' } };
  }
}

export const sessions = { opened: 0, deleted: 0 };

/** Streamable HTTP: answers `tools/call` as an event stream (with a progress
 *  note first), everything else as plain JSON. */
export async function mcpHttpHandler(req: Request): Promise<Response> {
  if (req.method === 'DELETE') { sessions.deleted++; return new Response(null, { status: 200 }); }
  const msg = (await req.json()) as Msg;
  if (msg.method !== 'initialize' && req.headers.get('mcp-session-id') !== 'sess-1') return new Response('no session', { status: 400 });
  const out = answer(msg);
  if (msg.method === 'initialize') {
    sessions.opened++;
    return Response.json(out, { headers: { 'mcp-session-id': 'sess-1' } });
  }
  if (!out) return new Response(null, { status: 202 });
  if (msg.method === 'tools/call') {
    const body = `event: message\ndata: ${JSON.stringify({ jsonrpc: '2.0', method: 'notifications/progress', params: {} })}\n\n`
      + `event: message\ndata: ${JSON.stringify(out)}\n\n`;
    return new Response(body, { headers: { 'content-type': 'text/event-stream' } });
  }
  return Response.json(out);
}

if (import.meta.main) {
  console.log('fake mcp server starting'); // not JSON: the client must skip it
  let buf = '';
  process.stdin.setEncoding('utf8');
  process.stdin.on('data', (chunk: string) => {
    buf += chunk;
    let nl: number;
    while ((nl = buf.indexOf('\n')) >= 0) {
      const line = buf.slice(0, nl);
      buf = buf.slice(nl + 1);
      if (!line.trim()) continue;
      const out = answer(JSON.parse(line));
      if (out) process.stdout.write(JSON.stringify(out) + '\n');
    }
  });
  process.stdin.on('end', () => process.exit(0));
}
