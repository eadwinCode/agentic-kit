# Run state

Whatever you attach to a run reaches **every** storage call it makes, every
tool, and every nested run — including in a worker that picks the job up hours
later, in another process, after an approval.

The platform never reads it.

## Attaching it

```ts
await chat.run({
  prompt: 'hi',
  state: { orgId: 'acme', userId: 'u1' },
});
```

## Reading it in storage

Every `Storage` method takes a trailing context:

```ts
async list(threadId: string, opts, ctx) {
  return this.db.message.findMany({
    where: { threadId, orgId: ctx.state.orgId },
  });
}
```

This is the multi-tenancy story. Scoping happens in your storage layer, where
your database's rules already live, rather than being threaded through prompts
or re-derived per query.

## Typing it

Augment the interface rather than making everything generic — a `<TState>`
parameter would ripple through `Storage`, `AgentCore`, tools and subagents
before reaching the one place it is read:

```ts
declare module 'agentenkit' {
  interface AgentRunState {
    orgId: string;
    userId: string;
  }
}
```

Now `ctx.state.orgId` is typed everywhere, and a typo is a compile error.

## Reads take it directly

A run carries its state on the dispatch ticket. A read has no ticket, so pass it:

```ts
await runtime.listThreads({ orgId });
await runtime.getThreadSnapshot(threadId, { orgId });
await runtime.getThreadUsage(threadId, { orgId });
await runtime.deleteThread(threadId, { orgId });
await runtime.hitl.respond({ threadId, toolCallId, approved, state: { orgId } });
await chat.stop(threadId, { orgId });
```

Omit it and that one call reaches your storage with an empty context. Make your
storage throw rather than fall back to an unscoped query — see
[Multi-tenancy](./multi-tenancy.md).

## Where it shows up

| Place | How |
| :--- | :--- |
| Storage methods | trailing `ctx` argument |
| Tools | second argument of `execute` — use `agentTool` to type it |
| Nested runs | inherited by every child |
| Resumed runs | rebuilt from the durable job, not from memory |
| After an approval | persisted with the park, restored on resume |

The last two rows are the ones that matter. The state travels **with the
ticket**, so a run resumed by a different process days later still scopes its
queries correctly — anything held only in a closure would be gone by then.

## What not to put in it

- **Secrets.** The state is persisted with the run record.
- **Large objects.** It rides on every dispatch.
- **Anything the platform should act on.** It is opaque by design; behaviour
  belongs in config or in your tools.

## Stopping with state

`stop` takes it too, for storage implementations that need scoping even to write
a cancellation:

```ts
await chat.stop(threadId, { orgId: 'acme' });
```

An accepted stop also closes the current run's operational record with
`state: 'CANCELLED'`, `stopReason: 'cancelled'`, `endedAt`, and `durationMs`.
This includes runs still queued or waiting for input, which may never reach a
worker again. Read the record through `runtime.admin.getRun(runId)` or list
the thread's runs through `runtime.admin.listRunsByThread(threadId)`.

The durable `STATE_CHANGE` event emitted by `stop` includes the stopped
`runId`, `stopReason: 'cancelled'`, and `endedAt`. This timestamp records the
accepted stop; a running worker can still add usage during teardown without
changing it. Existing step and token counters are preserved by `stop`.
