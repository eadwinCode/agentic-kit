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
  const { threadId, prompt, model, editMessageId, clientMessageId } = await req.json();

  const result = await chat.run({ threadId, prompt, model, editMessageId, clientMessageId });
  if (!result.accepted) return NextResponse.json(result, { status: 409 });

  return NextResponse.json(result, { status: 202 });
}
```

`202`, not `200`: the run has been accepted, not completed. A `409` means the
thread already has an active run — stop it first, or wait.

`run()` also accepts `runId` (name the run yourself; a reused id is a
`409`), `maxSteps` (cap the run below the config's ceiling) and `attachments`
(`[{url, mediaType}]`, images on the user turn).

`clientMessageId` is the sending client's own name for the user turn. It comes
back on the turn's `MESSAGE_APPENDED`, which is how `use-agentenkit` swaps its
optimistic copy for the real one. Pass it through, or the hook falls back to
matching the turn by its text. `model` may be absent: the hook sends none
unless told to, and the agent's own model is used.

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

The snapshot carries `thread`, `messages`, `runs`, `lastEventSeq`,
`activeEvents` and `stream`:

| Field | What it is |
| :--- | :--- |
| `messages` | Every saved message. A finished step's text is here. |
| `runs` | Nested runs, each with its `startedAt` and, once it ended, its `endedAt`. |
| `lastEventSeq` | The thread record's last `seq`. |
| `activeEvents` | The unfinished run's record entries: its open park, a refusal. |
| `stream` | `{ streamId, runId, items, end, offset }` for the latest segment's run stream, while it is open or ended less than a minute ago. `null` otherwise. |

A client renders the messages, applies `activeEvents`, shows `stream.items`
after the messages, then opens the live stream from there.

`stream.items` leaves out each agent's content up to its last finished step,
because that part is already in the messages. Showing it again would render
each finished step twice.

## Live stream

The one endpoint with real logic — so the logic is in the runtime, not in your
handler.

```ts
const { stream, headers } = runtime.events.sse(threadId, { cursor, lastMessageId, signal });
```

You get back a `ReadableStream<Uint8Array>` of SSE frames and the headers to
serve it with. Not a `Response`, because half the ecosystem has none.

### What it sends

Each SSE message is one frame, as JSON:

| Frame | What it is |
| :--- | :--- |
| `{ kind: 'thread', event }` | A thread record entry, or a live notice (`seq: 0`) such as `STATE_CHANGE` |
| `{ kind: 'stream', streamId, item }` | One item from a run stream: a typed event with its `offset` |
| `{ kind: 'snapshot', snapshot }` | The stream the client was reading is gone; here is a fresh snapshot |

The follow reads the thread record from the cursor's `seq`, and the run
stream the cursor names from its offset. When a new segment starts, its
`RUN_STARTED` entry moves the read to the new stream. When the stream the
client was reading is gone (past its grace window), the follow sends one
`snapshot` frame on the same connection — the messages after
`lastMessageId`, the record state, and the open stream — and carries on from
there. No second request, no reload.

See [Run streams](./run-streams.md) for the events a stream carries.

### The cursor

The cursor is one string: `<seq> <streamId> <offset>`, with `-` for a part the
client does not have. For example `42 run_abc:2 1718-0`.

Every frame that moves the cursor carries it as its SSE `id:`. EventSource
keeps the last id it saw and sends it back as `Last-Event-ID` when it
reconnects, so a reconnect picks up exactly where it stopped. A notice
(`seq: 0`) carries no `id:`, so it never moves the cursor.

A bare number is read as a record `seq` alone, which is what older clients
send.

### What it does for you

The rules, each of which is a real bug when a handler gets it wrong:

1. **Subscribe before replaying.** An event published between the replay
   finishing and the tail starting is otherwise lost for ever.
2. **Never emit at or below the cursor.** The client would render it twice.
3. **Never skip a seq.** A record entry that arrives out of order has the gap
   read back from storage first.
4. **A notice is forwarded but never moves the cursor**, and is sent without
   an `id:` line.
5. **A gone stream becomes one snapshot**, not an error.

It also stops reading when the client hangs up, when the signal aborts, or when
the consumer stops iterating — a subscription that outlives its reader leaks one
per reconnect.

### Options

| Option | Meaning |
| :--- | :--- |
| `cursor` | Where the client is: the `<seq> <streamId> <offset>` string (or a parsed `ThreadCursor`). Absent, it follows from now: the record from the start, and the run stream in flight from what the messages lack. |
| `since` | A bare record seq, for an older client. Used when there is no `cursor`. |
| `lastMessageId` | The last message the client has, so a `snapshot` frame carries only newer ones. |
| `signal` | Abort to stop the stream and unsubscribe. **Pass it.** |
| `retryMs` | Emitted once up front: how long a browser waits before reconnecting. |
| `wireFormat` | `'ag-ui'` sends [AG-UI](https://docs.ag-ui.com) events instead of the frames above. Opt-in; the default is the native frames. |
| `state` | Run state, if your storage is tenant-scoped. |

### Reading the cursor

The same line in every framework: the header first, then the query.

```ts
const cursor = header('last-event-id') ?? query('cursor') ?? query('since');   // a bad cursor is no cursor
const lastMessageId = query('lastMessageId') ?? undefined;
```

`runtime.events.sse` and `follow` take the string as it is. There is no need
to parse it.

---

### Next.js, Hono, Bun, Deno, Cloudflare Workers

Anywhere `Response` is native, serve the stream directly.

```ts
// Next.js App Router
export async function GET(req: NextRequest) {
  const threadId = req.nextUrl.searchParams.get('threadId')!;
  const cursor =
    req.headers.get('last-event-id') ??
    req.nextUrl.searchParams.get('cursor') ??
    req.nextUrl.searchParams.get('since');
  const lastMessageId = req.nextUrl.searchParams.get('lastMessageId') ?? undefined;

  const { stream, headers } = runtime.events.sse(threadId, {
    cursor,
    lastMessageId,
    signal: req.signal,
  });

  return new Response(stream, { headers });
}
```

```ts
// Hono
app.get('/api/agent/stream', (c) => {
  const cursor = c.req.header('last-event-id') ?? c.req.query('cursor') ?? c.req.query('since');

  const { stream, headers } = runtime.events.sse(c.req.query('threadId')!, {
    cursor,
    lastMessageId: c.req.query('lastMessageId'),
    signal: c.req.raw.signal,
  });

  return new Response(stream, { headers });
});
```

### Express and Fastify

Node has no `Response`. Node 18 and later can adapt the web stream:

```ts
import { Readable } from 'node:stream';

app.get('/api/agent/stream', (req, res) => {
  const cursor =
    (req.headers['last-event-id'] as string | undefined) ??
    (req.query.cursor as string | undefined) ??
    (req.query.since as string | undefined);

  // Node gives you no AbortSignal — make one and tie it to the socket, or the
  // follow outlives the client.
  const abort = new AbortController();
  res.on('close', () => abort.abort());

  const { stream, headers } = runtime.events.sse(String(req.query.threadId), {
    cursor,
    lastMessageId: req.query.lastMessageId as string | undefined,
    signal: abort.signal,
  });
  res.writeHead(200, headers);
  res.flushHeaders?.();
  Readable.fromWeb(stream as any).pipe(res);
});
```

Prefer to write frames yourself? `follow` yields the frames, and `followFrame`
is the same encoder the stream uses. It moves the cursor you give it:

```ts
import { SSE_HEADERS, followFrame, parseCursor } from 'agentenkit';

const at = parseCursor(cursor) ?? { seq: -1 };
res.writeHead(200, SSE_HEADERS);
for await (const frame of runtime.events.follow(threadId, { cursor, lastMessageId, signal: abort.signal })) {
  res.write(followFrame(frame, at));
}
res.end();
```

### NestJS

The simplest route in Nest is a plain `@Get` that pipes the stream, the same
as Express:

```ts
import { Controller, Get, Headers, Query, Req, Res } from '@nestjs/common';
import { Readable } from 'node:stream';

@Controller('api/agent')
export class AgentStreamController {
  @Get('stream')
  stream(
    @Query('threadId') threadId: string,
    @Query('cursor') cursor: string | undefined,
    @Query('lastMessageId') lastMessageId: string | undefined,
    @Headers('last-event-id') lastEventId: string | undefined,
    @Req() req: any,
    @Res() res: any,
  ) {
    const abort = new AbortController();
    req.on('close', () => abort.abort());

    const { stream, headers } = runtime.events.sse(threadId, {
      cursor: lastEventId ?? cursor,
      lastMessageId,
      signal: abort.signal,
    });
    res.writeHead(200, headers);
    Readable.fromWeb(stream as any).pipe(res);
  }
}
```

`@Sse()` works too, but then you set each message's `id` yourself: use
`followFrame` as above, or the cursor goes missing and a reconnect starts over.

### Something else entirely

`follow` is just an async iterable of frames. A WebSocket, a long-poll, a log
shipper, a test — all the same shape:

```ts
for await (const frame of runtime.events.follow(threadId, { cursor, signal })) {
  socket.send(JSON.stringify(frame));
}
```

Only need one run? `runtime.streams.read(streamId, after, signal)` reads one
run stream on its own, live until it ends. It throws `StreamGoneError` once
the stream is past its grace window.

Want the thread record alone, as before run streams? `runtime.events.followRecord(threadId, { since, signal })`
yields plain events, with no stream frames.

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

`handleJob` throws `UnknownAgentError` for a job naming an agent this process
does not have, so a queue that retries on failure keeps the job rather than
losing it. A long-lived worker passes `{ signal }` and aborts it on shutdown:
the segment stops at once and the job goes back on the queue without spending
an attempt.

When your queue gives up on a job (its attempts are spent), call
`runtime.worker.handleDeadJob(job, attempts, cause)`: the run behind it is
failed with the reason and settled, so its thread does not read `QUEUED` or
`RUNNING` for ever. A job whose run has already moved on is left alone.

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
    model: model ?? 'gpt-4o',   // the agent's own model when the client sent none
    agent: chat.name,
  }));
}
```

Carry `result.runId`. A worker without the dispatch's identity cannot be stopped
or replaced.
