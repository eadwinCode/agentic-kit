# Run streams

A run says a lot while it works: every text delta, every bit of reasoning,
every tool call. Almost none of it matters once the step is saved, because the
saved messages hold the final text. So the platform keeps those live events
in a short-lived **run stream**, and keeps only a small **thread record** for
what must outlive the run.

## Two places, two jobs

| | Run stream | Thread record |
| :--- | :--- | :--- |
| What | text, reasoning, tool calls and results, step ends, nested run activity, your live events | parks and answers, refusals, budget stops, compaction, each segment's start and end, your durable events |
| Where | the `RunStreams` port | `Storage.events` |
| Lives | until `streamGraceMs` after its segment ends | until the thread is deleted |
| Cursor | an opaque `offset` per stream | a monotonic `seq` per thread |
| Grows with | tokens | runs |

Before run streams, every chunk went into the events table and stayed there.
A long thread could hold hundreds of thousands of rows that nobody would read
again. Now the table grows with runs, not with tokens.

## One stream per segment

A **segment** is one pickup of a run by a worker. The first pickup is segment
1. A resume after a park, or a retry after a failure, is a new pickup, so it is
a new segment with a new stream. The stream id is `<runId>:<n>`:

```
run_abc:1   the first pickup; parks on an approval
run_abc:2   the resume after the answer; finishes
```

A closed stream never opens again. Each segment's `RUN_STARTED` and `RUN_ENDED`
go in the thread record, with the stream id, so a reader can always find the
latest stream from the record.

## The life of a stream

1. **Open.** The worker opens the stream when it picks the run up and writes
   `RUN_STARTED` to it.
2. **Append.** Events are grouped into appends: they wait up to
   `streamFlushMs` (50 ms), or until `streamFlushEvents` (32) are waiting. A
   step end, a tool result, a park and a close go out at once. A failed append
   is logged, never thrown: the stream is delivery, and the messages are saved
   either way.
3. **Close.** When the segment ends, the worker writes one last item:
   `RUN_FINISHED` (status `finished`, `parked` or `stopped`) or `RUN_ERROR`
   (status `error`). A reader that sees it stops. If the worker died, the
   stuck-run sweep closes the stream with `RUN_ERROR` status `lost`.
4. **Grace.** The stream is kept for `streamGraceMs` (10 minutes) after it
   closes, so a tab that reconnects in that time picks up inside it.
5. **Delete.** Then it is removed. A stream nothing ever closed is removed
   after `streamTtlMs` (24 hours). Deleting a thread deletes its run streams
   at once.

## What a stream carries

The events are one typed union (`StreamEvent` in TypeScript, the types in
`ports/streams.go` in Go). The shapes follow [AG-UI](https://docs.ag-ui.com).
Each item also has the `offset` the store gave it.

| Event | Fields |
| :--- | :--- |
| `RUN_STARTED` | `threadId`, `runId`, `streamId`, `segment` |
| `TEXT_MESSAGE_START` / `TEXT_MESSAGE_CONTENT` / `TEXT_MESSAGE_END` | `messageId`, and `delta` on content |
| `REASONING_START` / `REASONING_CONTENT` / `REASONING_END` | `messageId`, and `delta` on content |
| `TOOL_CALL_START` | `toolCallId`, `toolName` |
| `TOOL_CALL_ARGS` | `toolCallId`, `delta` |
| `TOOL_CALL_END` | `toolCallId`, `toolName`, `args` (the whole parsed object) |
| `TOOL_CALL_RESULT` | `toolCallId`, `toolName`, `result` |
| `SOURCE` | `source` — a page or document the model cited |
| `STEP_FINISHED` | `step`, `agentId`, `finishReason`, `usage` — the step's messages are saved |
| `MESSAGE_APPENDED` | `message`, `clientMessageId?` |
| `INPUT_REQUIRED` | the park: `toolCallId`, `toolName`, `arguments`, `reason`, `expiresAt?`, … |
| `SUBAGENT_STARTED` | `subagentId`, `name`, `depth` |
| `SUBAGENT_EVENT` | `subagentId`, `event` — one of the child's own events, wrapped |
| `SUBAGENT_FINISHED` | `subagentId`, `status` (`completed`, `failed`, `cancelled`), `error?` |
| `CUSTOM` | `name`, `value` — your own live event, from `publishEvent` |
| `RUN_FINISHED` | `status` (`finished`, `parked`, `stopped`), `usage?`, `costs?`, `finishReason?`, `text?` |
| `RUN_ERROR` | `status` (`error`, `lost`), `error` |

`RUN_FINISHED` or `RUN_ERROR` is always the last item. `text` on
`RUN_FINISHED` is the answer of a generate-text agent, which streams nothing
else.

## The thread record

`Storage.events` now keeps only these platform types: `INPUT_REQUIRED`,
`INPUT_EXPIRED`, `HITL_RESPONSE`, `RUN_REFUSED`, `TOKEN_BUDGET_EXHAUSTED`,
`COST_BUDGET_EXHAUSTED`, `CONTEXT_COMPACTED`, `MESSAGES_DROPPED`,
`RUN_STARTED`, `RUN_ENDED` — plus your own events published with
`{ durable: true }`.

Everything else the platform used to store (`CHUNK`, `STEP_COMMITTED`,
`STATE_CHANGE`, `MESSAGE_APPENDED`, …) is live only: a notice on the bus, and
an event on the run stream while one is open. `STATE_CHANGE` still reaches
every client live; it is just not stored.

The store mints each entry's `seq`, and each entry carries the `runId` it
belongs to. See [Ports and adapters](./ports-and-adapters.md#events-the-thread-record).

## Reading it

A client never has to join the two by hand. `runtime.events.follow` reads the
thread record and the run streams as one sequence of frames, and
`runtime.events.sse` encodes the same thing as Server-Sent Events.

```ts
for await (const frame of runtime.events.follow(threadId, { cursor, lastMessageId, signal })) {
  switch (frame.kind) {
    case 'thread':   /* frame.event: a record entry, or a notice (seq 0) */ break;
    case 'stream':   /* frame.streamId, frame.item: one run stream item */ break;
    case 'snapshot': /* frame.snapshot: the stream you were reading is gone */ break;
  }
}
```

```go
frames, err := rt.Events.Follow(ctx, threadID, agentenkit.FollowStateOptions{
	Cursor: cursor, LastMessageID: lastMessageID,
})
for f := range frames.Frames() {
	switch f.Kind { // "thread" (f.Event), "stream" (f.StreamID, f.Item), "snapshot" (f.Snapshot)
	}
}
```

### The cursor

The cursor is one string: `<seq> <streamId> <offset>`, with `-` for a part
the client does not have. Every SSE frame that moves it carries it as its
`id:`, so a browser's `EventSource` sends it back as `Last-Event-ID` when it
reconnects. A route passes it straight through:

```ts
const cursor = req.headers.get('last-event-id') ?? query.cursor ?? query.since;
runtime.events.sse(threadId, { cursor, lastMessageId: query.lastMessageId, signal });
```

A bare number is read as a record `seq` alone, which is what an older client
sends. Offsets are opaque: only the store compares them.

### When the stream is gone

A tab that comes back after the grace window names a stream that no longer
exists. The follow does not fail. It sends one `snapshot` frame on the same
connection — the messages after `lastMessageId`, the record state, and the
open stream if there is one — and carries on from there. The final text of the
gone stream is in those messages.

### The snapshot

`runtime.getThreadSnapshot(threadId)` carries the run stream too:

```ts
stream: { streamId, runId, items, end, offset } | null
```

It is the latest segment's stream while it is open, or ended less than a
minute ago. `items` leaves out each agent's content up to its last finished
step, because that part is in the messages already. `offset` is where a live
read picks up. `activeEvents` is the unfinished run's record entries, and
`lastEventSeq` is the record's last seq.

### One run on its own

For a caller that only cares about one run:

```ts
for await (const item of runtime.streams.read(streamId, null, signal)) { … }
```

```go
for item, err := range rt.Streams.Read(ctx, streamID, "") { … }
```

It reads from the start (or after an offset), live until the end item. Past
the grace window it throws `StreamGoneError` (Go: `ErrStreamGone`).

## Choosing an adapter

Pass one as `streams` to `setupAgentCore` (Go: `Streams` in
`agentenkit.RuntimeOptions`).

| Store | TypeScript | Go | Grace |
| :--- | :--- | :--- | :--- |
| Redis | `RedisRunStreams` | `redis.NewRunStreams` | keep it short: it is memory |
| Upstash | `UpstashRunStreams` | `upstash.NewRunStreams` | keep it short: it is memory |
| Postgres | `PrismaRunStreams` | `postgres.NewRunStreams` | can be longer: rows on disk |
| SQLite | `SqliteRunStreams` | `sqlite.NewRunStreams` | can be longer: rows on disk |
| Memory | `MemoryRunStreams` | `memory.NewRunStreams` | tests |

`PrismaRunStreams` needs the `RunStream` and `RunStreamEvent` models (see
[`examples/nextjs-app/prisma/schema.prisma`](https://github.com/eadwinCode/agentic-kit/tree/main/examples/nextjs-app/prisma/schema.prisma)).
Go's `postgres.NewRunStreams` takes an optional `Listener`, so readers in
other processes wake on `NOTIFY` instead of polling. See
[Ports and adapters](./ports-and-adapters.md#runstreams) for the details of
each.

**Leave it out** and the runtime keeps streams in memory and logs a warning
once. Only that process can read them. That is fine for one process, and wrong
for a deployment where web servers and workers are separate processes: the web
server would never see what the worker wrote.

## Settings

| Setting | Go | Default | Meaning |
| :--- | :--- | ---: | :--- |
| `streamGraceMs` | `StreamGrace` | 10 min | How long a stream is kept after its segment ends |
| `streamTtlMs` | `StreamTTL` | 24 h | How long a stream lives if nothing ever closes it |
| `streamFlushMs` | `StreamFlush` | 50 ms | How long events wait to be appended together; `0` sends each at once |
| `streamFlushEvents` | `StreamFlushEvents` | 32 | Append at once when this many are waiting |

The grace is a trade. Longer means a tab that was away longer can pick up
inside the stream, but the stream holds its space for longer. On Redis that
space is memory, so keep it short. On Postgres or SQLite it can be higher.
Past the grace, nothing is lost: the tab gets a snapshot instead.

## AG-UI on the wire

The SSE route sends the native frames above by default. Opt in to
[AG-UI](https://docs.ag-ui.com) events instead with `wireFormat: 'ag-ui'` on
`runtime.events.sse` (Go: `WireFormat: "ag-ui"` on `SSEStateOptions`). The
cursor still rides on each message's `id:`, so reconnects work the same way.

## Upgrading

- **Pass a `streams` port** wherever web servers and workers run apart.
- **Migrate your storage.** Entries now carry a `runId`, and the store mints
  the `seq`. A custom `Storage` must take `append` entries without a seq,
  return them with one, and add `list`. With Prisma, add the `runId` column
  to `AgentEvent` and the `RunStream` and `RunStreamEvent` models.
- **Check `publishEvent` calls.** They are now live only by default. Add
  `{ durable: true }` (Go: `PublishOptions{Durable: true}`) to any event a
  client must still see after a reload. See [Custom events](./custom-events.md).
- **Update the stream route** to pass `cursor` and `lastMessageId` — see
  [HTTP API](./http-api.md#live-stream). The React hook sends both.
- **Clear old rows, when it suits you.** Older releases left chunks, step
  markers and state changes in the events table. Nothing reads them now.

  ```ts
  await runtime.pruneEvents({ dryRun: true });   // count first
  await runtime.pruneEvents();                   // then delete, 10,000 rows a batch
  ```

  ```go
  report, err := rt.PruneEvents(ctx, agentenkit.PruneOptions{DryRun: true})
  ```

  It deletes only the platform's old stream-only types. Your own types and the
  thread record are kept. Each batch is its own short delete, so it is safe to
  stop and run again. It needs a storage whose `events` has `prune`; the
  reference ones do.
- **Admin `getRun`** now returns the run's record entries as `events`, not
  the whole log with chunks stripped.
