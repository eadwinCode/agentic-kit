# Observability

## Two stores, on purpose

| | Yours | The platform's |
| :--- | :--- | :--- |
| Holds | threads, messages, events, usage | run records, step timings, a thread index |
| You | implement `Storage` | read it back |
| Tables | yours | prefixed `agentic_` |

You do **not** implement `AdminStore`. Run records and step timings are the
platform's own data, in the platform's own tables.

Keeping them apart is what lets a dashboard answer "what is running right now"
without touching your database at all — and it means operational history
survives you reshaping your schema.

## Configuring it

Nothing to configure and it is SQLite on disk:

```bash
AGENTIC_KIT_ADMIN_DB=./agentic-kit-admin.sqlite   # optional; moves the file
```

Set a Postgres URL and it is Postgres — its own database, or the one you already
have. The `agentic_` prefix keeps them apart.

```bash
AGENTIC_KIT_ADMIN_DATABASE_URL=postgresql://user:pass@host/db
```

`pg` is an optional peer dependency, needed only on that path.

Pass `admin:` to `setupAgentCore` to override entirely — `MemoryAdminStore` is
the right choice in tests, so a suite does not write to a file on disk.

## The reads

```ts
// Threads and runs by state, plus what is in flight right now.
await runtime.admin.overview({ since });

// Runs, filtered.
await runtime.admin.listRuns({ state: ['FAILED'], since, limit: 50 });

// p50/p95 duration and queue wait, tokens, failure rate.
await runtime.admin.stats({ since, until });

// One run: its steps, its nested runs, its timeline.
await runtime.admin.getRun(runId);

// Threads with their runs rolled up — the top level of an operational view,
// since a thread is what a person recognises.
await runtime.admin.listThreads({ state: ['RUNNING'], since, limit: 50 });
await runtime.admin.getThread(threadId);

// Drilling down.
await runtime.admin.listRunsByThread(threadId);
await runtime.admin.listSteps(runId);
```

Percentiles are computed in the library, not in SQL, so every backing store
reports them the same way.

## What started a thread

A thread's first dispatched run is recorded on the thread itself, once, as
`startedWith`, and a later run never overwrites it. It is what a listing shows
when it has to say who asked for what without opening the thread:

```ts
const threads = await runtime.admin.listThreads({ limit: 50 });
threads[0].startedWith;
// {
//   runId: '…', agent: 'chat', model: 'gpt-4o', at: Date,
//   prompt: 'send the quarterly report',          // recordPayloads only
//   tokenBudget: 50_000,                          // recordPayloads only
//   state: { orgId: 'acme', userId: 'u1' },       // recordPayloads only
//   providerOptions: { openai: { reasoningEffort: 'low' } }, // recordPayloads only
// }
```

`runId`, `agent`, `model` and `at` are always there. The rest follows
`recordPayloads`, like every other payload in this store. Every run record
carries the same `providerOptions` it was dispatched with, merged across the
three levels (config, spec, input), next to its `prompt`, `tokenBudget` and
`runState`.

A thread recorded before this existed has `startedWith: null`; the roll-up
then falls back to the earliest dispatched run inside the query window.

## What a step record holds

Timings and counts always; payloads when you allow them:

```ts
{
  runId, threadId, agentId, index,
  durationMs, finishReason,
  inputTokens, cachedInputTokens, outputTokens, totalTokens,
  tools: ['sendEmail'],
  // only when recordPayloads is on:
  text: 'the model output, truncated',
  toolCalls: [{ toolName, args, result }],
}
```

## Payloads and privacy

```ts
config: {
  recordPayloads: true,   // default
  payloadCapChars: 2_000, // default
}
```

On by default, because a dashboard that cannot show *what happened* is worth
little. **Turn it off when prompts or tool payloads carry anything that should
not sit in an operational database** — health data, credentials, personal
details. With it off you still get timings, token counts and tool names.

The cap stops one large prompt or tool result from bloating the store.

## Building a dashboard

The library gives you the reads; the UI is yours. A structure that works, from
the sample in `examples/nextjs-app/app/admin`:

**Threads → runs → steps → one step in detail.** A person recognises a thread,
remembers roughly when something went wrong, and wants the step that did it.

Two things worth putting on screen early:

- **Token split per step** — input, cached, output. Cache hits are invisible in
  a total, and they are usually most of the bill.
- **Step durations against the run's wall clock.** Time inside steps that does
  not add up to the run's duration is queue wait or provider latency, and the
  gap is where the problem usually is.

> A real case: a chart showed "12s of 9.9s in steps", which is impossible. The
> cause was nested-run steps being attributed to the parent's run id. In a table
> it was invisible; in a chart it was obvious.

## Events versus the admin store

They answer different questions:

| Question | Look at |
| :--- | :--- |
| What is this conversation showing right now? | the event log |
| How long did runs take last week? | the admin store |
| What did step 3 of that failed run produce? | the admin store |
| What should this client render next? | the event log |

The event log is per-thread, replayable and client-facing. The admin store is
cross-thread, aggregated and operator-facing.
