# Core concepts

Read this once and the rest of the documentation stops being surprising.

## The one idea

**A run outlives the request that started it.**

Everything else follows. If a run outlives its request, it cannot be a promise
the caller awaits, which means it needs somewhere durable to live, a way to be
stopped, a way to be resumed after a pause, and a way for several people to
watch it at once. That is what this library is.

## Thread, run, step

| | What it is | Lifetime |
| :--- | :--- | :--- |
| **Thread** | A conversation. Holds messages, its record and usage. | Until deleted |
| **Run** | One dispatched attempt to advance a thread. | Minutes |
| **Segment** | One pickup of a run by a worker. A resume or a retry is a new one. It has its own run stream. | Minutes, plus `streamGraceMs` for its stream |
| **Step** | One model round trip inside a run. | Seconds |

A thread has many runs. A run has many steps. `Thread.state` only ever describes
the **latest** run, which is why the operational store keeps a row per run —
without it you cannot answer "what happened last Tuesday".

## The loop is the platform's, not the SDK's

The AI SDK can loop on its own, taking tool results and calling the model again.
This library does not let it: each step runs with `maxSteps: 1`, and the
platform decides what happens next.

```
  dispatch → [ step → persist → decide ] → … → finalize
                       ↑          │
                       └──────────┘
```

Every continuation decision — tool results ready, budget spent, step ceiling
reached, approval needed, user pressed stop — is made **between** steps, where
the decision can be written down. A loop inside the SDK is a loop whose state
lives in a process that can die.

Because each step's messages are persisted before the next begins, a worker that
dies mid-run resumes from the last completed step rather than restarting.

## The shape of a run

```
run()                    worker                        finalize
  │                        │                              │
  ├─ persist user message  ├─ claim the run lock          ├─ state → COMPLETED
  ├─ state → RUNNING       ├─ compact history             │   or FAILED
  ├─ enqueue a job    ───► ├─ loop: step, persist, decide │   or CANCELLED
  │                        │                              │
  └─ return 202            └─ park for approval ──────────┘  (no lock held)
```

`run()` accepts no execution responsibility whatsoever. It returns as soon as
the job is on the queue.

## Run identity

Every run gets an id, held in a key the platform owns. A worker checks that the
id it holds is still the current one; if a newer run has replaced it, the old
worker retires without writing state on the live run's behalf.

This is what makes "stop, then immediately send another message" safe. Without
it, the stopping worker and the new run race to write the thread's state, and
which one wins depends on timing.

## Ports

The engine imports no database driver. Five interfaces stand between it and your
stack:

| Port | Role | Delivery |
| :--- | :--- | :--- |
| `Storage` | threads, messages, the thread record, usage | your database |
| `Queue` | durable run dispatch | at-least-once |
| `EventBus` | live fan-out to watching clients | at-most-once |
| `Kv` | hot state, handoff keys, counters | fast, expendable |
| `RunStreams` | one short-lived stream per run segment | kept for a grace window |

The split between the queue and the bus matters. The queue must not lose a job,
so it is at-least-once and the engine is idempotent under redelivery. The bus may
lose a frame, because a client that misses one recovers by reading the thread
record and the run stream again from its cursor.

See [Ports and adapters](./ports-and-adapters.md).

## The thread record and run streams

What a run says goes to two places.

- **The run stream.** Each run segment gets its own short-lived stream: text
  deltas, reasoning, tool calls and results, step ends, nested run activity,
  your own live events. A segment is one pickup by a worker; a resume after a
  park, or a retry, starts a new segment and a new stream. The stream is
  closed when the segment ends and deleted a little later (`streamGraceMs`).
- **The thread record.** A small, per-thread log with a monotonic `seq`. It
  keeps only what must outlive a run: parks and their answers, refusals,
  budget stops, compaction, each segment's start and end, and your own events
  published with `durable: true`.

The finished text is not lost when a stream goes: it is in the messages. So a
client loads the snapshot (messages, the record, and the stream in flight),
then follows the record and the stream from its cursor. A reload, a reconnect,
or a second tab all end up with the same conversation.

`runtime.events.follow()` and `runtime.events.sse()` do that for you, in any
framework — see [HTTP API](./http-api.md#live-stream) and
[Run streams](./run-streams.md).

Common stream events (the shapes follow [AG-UI](https://docs.ag-ui.com)):

| Type | Meaning |
| :--- | :--- |
| `RUN_STARTED` | A segment began |
| `TEXT_MESSAGE_START` / `_CONTENT` / `_END` | Model text, as deltas |
| `REASONING_START` / `_CONTENT` / `_END` | Model reasoning, as deltas |
| `TOOL_CALL_START` / `_ARGS` / `_END`, `TOOL_CALL_RESULT` | Tool activity |
| `STEP_FINISHED` | A step's messages are saved |
| `MESSAGE_APPENDED` | A message was saved |
| `INPUT_REQUIRED` | A tool is waiting for a human |
| `SUBAGENT_STARTED` / `SUBAGENT_EVENT` / `SUBAGENT_FINISHED` | Nested run activity |
| `CUSTOM` | Your own event, `{ name, value }` |
| `RUN_FINISHED` / `RUN_ERROR` | The segment ended; always the last item |

Common record entries: `INPUT_REQUIRED`, `INPUT_EXPIRED`, `HITL_RESPONSE`,
`RUN_REFUSED`, `TOKEN_BUDGET_EXHAUSTED`, `COST_BUDGET_EXHAUSTED`,
`CONTEXT_COMPACTED`, `MESSAGES_DROPPED`, `RUN_STARTED`, `RUN_ENDED`.
`STATE_CHANGE` is still sent live on the bus, but it is not stored.

### Your own events

The pipe is not only the platform's. A tool, or any server code, can
[publish an event](./custom-events.md) on a thread, and a client reads it
through the same follow. Live only by default; `durable: true` keeps it in the
thread record.

## Thread states

```
IDLE ──► QUEUED ──► RUNNING ──► COMPLETED
                      │  ▲
                      │  └── WAITING_FOR_INPUT   (parked; holds no process)
                      ├────► CANCELLED           (stopped)
                      └────► FAILED              (attempts exhausted, timed out, refused)
```

`QUEUED` is a run the server accepted and put on the queue: no worker has it
yet. It becomes `RUNNING` the moment a worker picks the job up,
and goes back to `QUEUED` while a failed run waits for its retry. A client can
tell a run that is waiting in line from one that is working, and so can an
operator: the run record keeps `enqueuedAt` and the queue-wait percentiles
count the runs still waiting.

Every change between these states is one compare-and-set on the thread row,
on the state **and** the run that owns the thread. So a stop is never
overwritten by a run that finishes a moment later, and a run that was stopped
or replaced can never move the thread again: its change simply loses, and it
publishes nothing.

`WAITING_FOR_INPUT` is the interesting one: it is a durable state, not a blocked
promise. No worker, no lock, no memory is held while a thread waits for a human.
The expiry is a delayed queue message, so it fires whether or not anyone is
watching.

## What this library does not do

- It does not own your prompts, models or tools. The AI SDK does.
- It does not own your database schema. You implement `Storage`.
- It does not render anything. `use-agentenkit` is one option; the follow's frames
  are a public contract you can build any client over.

## Next

- [HTTP API](./http-api.md) — turning the runtime into endpoints.
- [Agents and tools](./agents-and-tools.md) — registering what runs.
