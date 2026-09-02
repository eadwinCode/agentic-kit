# Human in the loop

A tool that should not fire without a person's say-so is marked, and the run
parks when the model calls it.

## Marking a tool

```ts
import { markRequiresConfirmation } from 'agentenkit';

const sendEmail = markRequiresConfirmation(
  tool({
    description: 'Sends an email (destructive — requires approval)',
    parameters: z.object({ to: z.string().email(), subject: z.string(), body: z.string() }),
    execute: async (args) => send(args),
  }),
);
```

## What a park actually is

**A durable state transition that holds no process.**

When the model calls a marked tool, the platform:

1. writes an `INPUT_REQUIRED` event carrying the tool name and arguments,
2. moves the thread to `WAITING_FOR_INPUT`,
3. schedules the expiry as a *delayed queue message*,
4. ends the run segment and **releases the run lock**.

No worker is blocked. No promise is pending. No memory is held. A thread can
wait for a week, across deploys and restarts, and cost nothing.

Point 3 is the one that is easy to get wrong. If expiry depended on a live
connection or a running worker, an approval nobody is watching would hang for
ever. It rides the queue, so it fires with nobody there.

## Answering

```ts
await runtime.hitl.respond({
  threadId,
  toolCallId,
  approved: true,
  payload,          // optional — becomes the tool result when approved
});
// → { delivered: true } | { delivered: false, error }
```

`delivered: false` means the wait is gone: it expired, it was already answered,
or the thread moved on. Show it as "too late", not as an error.

On approval the tool runs and the run resumes **where it stopped** — the step is
not replayed. On denial the tool returns a denial result and the model continues
from there. The model decides what to do about being refused; that is a
conversation, not an error.

## Expiry

An unanswered request resolves as a timeout denial — literally "user had no
response, action cancelled" — and the run continues. The window is
`hitlTtlMs` (default 15 minutes).

```ts
config: { hitlTtlMs: 5 * 60_000 }
```

There is a second, slower safety net: `reclaimIfOrphaned` heals a thread parked
before the expiry timer existed, or one whose queue adapter ignored the delay.
It is cheap to call on connect and costs one call per connection rather than a
poll per viewer.

## Several approvals at once

A single step can park more than one request — most often when a step spawns two
subagents and both park. The thread stays `WAITING_FOR_INPUT` until **every**
open approval is settled, and the pending set is derived from durable state on
every read rather than cached.

Practical consequences:

- Answering one request does not resume the run while a sibling is still open.
- Requests may be answered in any order. There is no "latest" to answer.
- One request expiring does not disturb its siblings.

## Nested approvals

A subagent can park too. The `INPUT_REQUIRED` event carries the asking run's
name and depth, so a card can say "mailer" rather than an opaque id, and the
frame stack unwinds the answer back through the nesting levels.

## Building the UI

With [`use-agentenkit`](./react.md) it is two fields and a call:

```tsx
{pendingInputs.map((input) => (
  <div key={input.toolCallId}>
    <code>{input.toolName}</code>
    <pre>{JSON.stringify(input.arguments, null, 2)}</pre>
    <button onClick={() => respondToInput(input.toolCallId, true)}>Approve</button>
    <button onClick={() => respondToInput(input.toolCallId, false)}>Deny</button>
  </div>
))}
```

The hook drops the card immediately and lets the server confirm, so a slow
network does not leave a button looking unpressed.

## Testing it

Nothing here needs a real model. Script a mock that calls the marked tool, run
the worker, and assert the thread is `WAITING_FOR_INPUT` with an
`INPUT_REQUIRED` event. Then call `respond` and run the worker again.
