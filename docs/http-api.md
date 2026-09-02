# HTTP API

The runtime is a library, not a server. You expose it. This page is the contract
that [`use-agentenkit`](./react.md) expects by default — change the paths freely
and tell the hook where they moved.

Every handler is a few lines over the runtime. The examples are Next.js App
Router; the shape is the same anywhere.

## The endpoints

| Purpose | Method + path | Runtime call |
| :--- | :--- | :--- |
| Start a run | `POST /api/agent/run` | `agent.run(...)` |
| Stop a run | `POST /api/agent/control` | `agent.stop(threadId)` |
| Answer an approval | `POST /api/agent/respond` | `runtime.hitl.respond(...)` |
| Hydrate a client | `GET /api/agent/history` | `runtime.getThreadSnapshot(...)` |
| Live stream | `GET /api/agent/stream` | `runtime.events.*` |
| Token + context usage | `GET /api/agent/usage` | `runtime.getThreadUsage(...)` |
| List / delete threads | `GET`, `DELETE /api/threads` | `runtime.listThreads()`, `deleteThread(...)` |
| Queue consumer | `POST /api/queue/agent-run` | `runtime.worker.handleJob(job)` |

## Start a run

```ts
export async function POST(req: NextRequest) {
  const { threadId, prompt, model, editMessageId } = await req.json();

  const result = await chat.run({ threadId, prompt, model, editMessageId });
  if (!result.accepted) return NextResponse.json(result, { status: 409 });

  return NextResponse.json(result, { status: 202 });
}
```

`202`, not `200`: the run has been accepted, not completed. A `409` means the
thread already has an active run — stop it first, or wait.

`editMessageId` replaces that user turn and drops everything after it, then
answers again. Only a user turn may be edited: cutting from anywhere else can
strip a tool result off the assistant tool-call that produced it, and a dangling
call is a conversation no provider accepts.

## Stop a run

```ts
export async function POST(req: NextRequest) {
  const { threadId } = await req.json();
  const result = await chat.stop(threadId);
  return NextResponse.json(result, { status: result.accepted ? 200 : 409 });
}
```

One durable write. The running worker notices within `stopPollMs`.

## Answer an approval

```ts
export async function POST(req: NextRequest) {
  const body = await req.json();       // { threadId, toolCallId, approved, payload? }
  const result = await runtime.hitl.respond(body);
  return NextResponse.json(result, { status: result.delivered ? 200 : 409 });
}
```

See [Human in the loop](./human-in-the-loop.md).

## Hydrate a client

```ts
export async function GET(req: NextRequest) {
  const threadId = req.nextUrl.searchParams.get('threadId');
  if (!threadId) return NextResponse.json({ error: 'threadId is required' }, { status: 400 });

  const snapshot = await runtime.getThreadSnapshot(threadId);
  if (!snapshot) return NextResponse.json({ error: 'Thread not found' }, { status: 404 });

  return NextResponse.json(snapshot);
}
```

The snapshot carries `thread`, `messages`, `runs`, `lastEventSeq` and
`activeEvents`. A client renders the messages, applies `activeEvents` to restore
whatever the in-flight run has produced but not yet committed, then opens the
stream at `lastEventSeq`.

`activeEvents` deliberately excludes stream chunks from steps that are already
durable. Replaying those on top of the messages would render each finished step
twice.

## Live stream

The one endpoint with real logic — so the logic is in the runtime, not in your
handler.

```ts
const { stream, headers } = runtime.events.sse(threadId, { since, signal });
```

You get back a `ReadableStream<Uint8Array>` of SSE frames and the headers to
serve it with. Not a `Response`, because half the ecosystem has none.

### What it does for you

Three rules, each of which is a real bug when a handler gets it wrong:

1. **Subscribe before replaying.** An event published between the replay
   finishing and the tail starting is otherwise lost for ever.
2. **Never emit at or below the cursor.** The client would render it twice.
3. **A bus-only notice (`seq === 0`) is forwarded but never moves the cursor** —
   and is sent *without* an `id:` line. EventSource stores any id it sees and
   returns it as `Last-Event-ID`, so stamping `id: 0` on a heartbeat would
   rewind a reconnecting client to the start of the thread.

It also unsubscribes when the client hangs up, when the signal aborts, or when
the consumer stops iterating — a subscription that outlives its reader leaks one
per reconnect.

### Options

| Option | Meaning |
| :--- | :--- |
| `since` | Resume after this seq. `-1` (default) replays from the start. |
| `signal` | Abort to stop the stream and unsubscribe. **Pass it.** |
| `retryMs` | Emitted once up front: how long a browser waits before reconnecting. |
| `state` | Run state, if your storage is tenant-scoped. |

### Reading the cursor

The same three lines in every framework:

```ts
const raw = headerOrQuery('last-event-id') ?? query('since');
const parsed = raw === null ? -1 : Number(raw);
const since = Number.isFinite(parsed) ? parsed : -1;   // a bad cursor replays
```

---

### Next.js, Hono, Bun, Deno, Cloudflare Workers

Anywhere `Response` is native, serve the stream directly.

```ts
// Next.js App Router
export async function GET(req: NextRequest) {
  const threadId = req.nextUrl.searchParams.get('threadId')!;
  const raw = req.headers.get('last-event-id') ?? req.nextUrl.searchParams.get('since');
  const parsed = raw === null ? -1 : Number(raw);

  const { stream, headers } = runtime.events.sse(threadId, {
    since: Number.isFinite(parsed) ? parsed : -1,
    signal: req.signal,
  });

  return new Response(stream, { headers });
}
```

```ts
// Hono
app.get('/api/agent/stream', (c) => {
  const raw = c.req.header('last-event-id') ?? c.req.query('since');
  const parsed = raw === undefined ? -1 : Number(raw);

  const { stream, headers } = runtime.events.sse(c.req.query('threadId')!, {
    since: Number.isFinite(parsed) ? parsed : -1,
    signal: c.req.raw.signal,
  });

  return new Response(stream, { headers });
});
```

### Express and Fastify

Node has no `Response`, so use the event iterator and write frames yourself.
`sseFrame` is the same encoder the stream uses.

```ts
import { SSE_HEADERS, sseFrame } from 'agentenkit';

app.get('/api/agent/stream', async (req, res) => {
  const raw = (req.headers['last-event-id'] as string) ?? (req.query.since as string);
  const parsed = raw === undefined ? -1 : Number(raw);

  // Node gives you no AbortSignal — make one and tie it to the socket, or the
  // subscription outlives the client.
  const abort = new AbortController();
  res.on('close', () => abort.abort());

  res.writeHead(200, SSE_HEADERS);
  res.flushHeaders?.();

  try {
    for await (const event of runtime.events.follow(String(req.query.threadId), {
      since: Number.isFinite(parsed) ? parsed : -1,
      signal: abort.signal,
    })) {
      res.write(sseFrame(event));
    }
  } finally {
    res.end();
  }
});
```

Prefer to pipe? Node ≥ 18 can adapt the web stream:

```ts
import { Readable } from 'node:stream';

const { stream, headers } = runtime.events.sse(threadId, { since, signal: abort.signal });
res.writeHead(200, headers);
Readable.fromWeb(stream as any).pipe(res);
```

### NestJS

Nest's `@Sse()` wants an `Observable<MessageEvent>`. RxJS 7 takes an async
iterable directly, so the iterator drops straight in:

```ts
import { Controller, Query, Req, Sse, type MessageEvent } from '@nestjs/common';
import { from, map, type Observable } from 'rxjs';

@Controller('api/agent')
export class AgentStreamController {
  @Sse('stream')
  stream(
    @Query('threadId') threadId: string,
    @Query('since') since: string | undefined,
    @Req() req: any,
  ): Observable<MessageEvent> {
    const abort = new AbortController();
    req.on('close', () => abort.abort());
    const parsed = since === undefined ? -1 : Number(since);

    return from(
      runtime.events.follow(threadId, {
        since: Number.isFinite(parsed) ? parsed : -1,
        signal: abort.signal,
      }),
    ).pipe(
      // Carry the seq as the SSE id so a reconnect resumes — but NOT for a
      // seq-0 notice, which would rewind the cursor.
      map((event) => ({
        data: event as unknown as Record<string, unknown>,
        ...(event.seq !== 0 ? { id: String(event.seq) } : {}),
      })),
    );
  }
}
```

Nest reads `Last-Event-ID` as a plain header if you prefer it to the query
parameter — inject it with `@Headers('last-event-id')`.

### Something else entirely

`follow` is just an async iterable of events. A WebSocket, a long-poll, a log
shipper, a test — all the same shape:

```ts
for await (const event of runtime.events.follow(threadId, { signal })) {
  socket.send(JSON.stringify(event));
}
```

### Heal a parked approval on connect

Expiry rides the queue, so this is only a fallback — it catches threads parked
before the timer existed, and any queue adapter that ignores delays. One call
per connection rather than a poll per viewer:

```ts
void runtime.hitl.reclaimIfOrphaned(threadId);
```

### Behind a proxy

`SSE_HEADERS` already sets `Cache-Control: no-cache, no-transform` and
`X-Accel-Buffering: no`. If a stream still looks frozen, the proxy is buffering
— nginx needs `proxy_buffering off;` for the location.

## Queue consumer

```ts
async function handler(req: NextRequest) {
  const job = await req.json();
  waitUntil(runtime.worker.handleJob(job));
  return NextResponse.json({ accepted: true });   // ack immediately
}

// Verification wraps the handler: only genuine deliveries reach the runtime.
export const POST = verifySignatureAppRouter(handler);
```

Acknowledge immediately and let the work continue in the background. The message
is a dispatch ticket, not an execution leash — a run, including a parked
approval, outlives this HTTP response.

Delivery is at-least-once, so double dispatch is possible; the per-thread run
lock makes it a no-op.

> **This endpoint must be authenticated.** It executes agents. See
> [Production](./production.md#security).

## Local development without a cloud queue

Run the engine in-process, using the same dispatch ticket the queue would have
delivered, so the inline path and the worker path stay identical:

```ts
if (process.env.INLINE_WORKER === '1') {
  waitUntil(runtime.worker.handleJob({
    threadId: result.threadId,
    runId: result.runId,        // the run id run() enqueued — required
    model: model ?? 'gpt-4o',
    agent: chat.name,
  }));
}
```

Carry `result.runId`. A worker without the dispatch's identity cannot be stopped
or replaced.
