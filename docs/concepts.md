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
| **Thread** | A conversation. Holds messages, events and usage. | Until deleted |
| **Run** | One dispatched attempt to advance a thread. | Minutes |
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

The engine imports no database driver. Four interfaces stand between it and your
stack:

| Port | Role | Delivery |
| :--- | :--- | :--- |
| `Storage` | threads, messages, events, usage | your database |
| `Queue` | durable run dispatch | at-least-once |
| `EventBus` | live fan-out to watching clients | at-most-once |
| `Kv` | hot state, handoff keys, counters | fast, expendable |

The split between the queue and the bus matters. The queue must not lose a job,
so it is at-least-once and the engine is idempotent under redelivery. The bus may
lose a frame, because a client that misses one recovers by replaying the durable
event log from its cursor.

See [Ports and adapters](./ports-and-adapters.md).

## The event log

Everything a run does is appended to a per-thread event log with a monotonic
`seq`, then fanned out on the bus. A client hydrates from the durable log and
then tails the bus from its cursor, so a reload, a reconnect, or a second tab all
converge on the same conversation.

`runtime.events.follow()` and `runtime.events.sse()` do that sequencing for you,
in any framework — see [HTTP API](./http-api.md#live-stream).

Common event types:

| Type | Meaning |
| :--- | :--- |
| `STATE_CHANGE` | The thread moved to a new state |
| `MESSAGE_APPENDED` | A user turn was persisted |
| `MESSAGES_DROPPED` | An edit removed a turn and everything after it |
| `CHUNK` | A piece of model output — text, reasoning, tool activity |
| `STEP_COMMITTED` | A step's messages are now durable |
| `INPUT_REQUIRED` | A tool is waiting for a human |
| `INPUT_EXPIRED` | That wait timed out |
| `SUBAGENT_STARTED` / `_CHUNK` / `_COMPLETED` / `_FAILED` | Nested run activity |
| `CONTEXT_COMPACTED` | History was summarized |

## Thread states

```
IDLE ──► RUNNING ──► COMPLETED
           │  ▲          
           │  └── WAITING_FOR_INPUT   (parked; holds no process)
           ├────► CANCELLED           (stopped)
           └────► FAILED              (attempts exhausted)
```

`WAITING_FOR_INPUT` is the interesting one: it is a durable state, not a blocked
promise. No worker, no lock, no memory is held while a thread waits for a human.
The expiry is a delayed queue message, so it fires whether or not anyone is
watching.

## What this library does not do

- It does not own your prompts, models or tools. The AI SDK does.
- It does not own your database schema. You implement `Storage`.
- It does not render anything. `use-agentkit` is one option; the event log is a
  public contract you can build any client over.

## Next

- [HTTP API](./http-api.md) — turning the runtime into endpoints.
- [Agents and tools](./agents-and-tools.md) — registering what runs.
