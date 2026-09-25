# Custom events

A thread's follow is the one channel between a run and everyone watching it.
The platform writes to it (text deltas, tool calls, `STATE_CHANGE`,
`INPUT_REQUIRED`, …) and every client reads it, live and on reconnect.
`publishEvent` lets **your** code write to it too, so anything a run learns
along the way reaches the UI through the same pipe: a progress label, a preview
URL, a credit warning, a set of questions to show.

> **Breaking change: live only by default.** `publishEvent` used to keep every
> event in the thread's history unless you passed `{ durable: false }`. Now it
> is the other way round: an event is **live only** unless you pass
> `{ durable: true }`. If a client must still see an event after a reload, or a
> week later, add `{ durable: true }` to that call. In Go,
> `PublishOptions{Notice: true}` is gone; pass `PublishOptions{Durable: true}`
> for a kept event, and nothing for a live one.

## From a tool

Every tool receives `publishEvent` beside `state` and `toolCallId`, already
bound to the thread the tool is acting on. It works in the main agent, in a
nested run, and in a segment resumed after an approval.

```ts
import { agentTool } from 'agentenkit';

const renderDesign = agentTool({
  description: 'Render a design preview',
  parameters: z.object({ brief: z.string() }),
  execute: async ({ brief }, { publishEvent, state }) => {
    await publishEvent('PROGRESS', { label: 'Rendering…' });   // live only
    const url = await render(brief, state.orgId);
    await publishEvent('DESIGN_PREVIEW', { url }, { durable: true });   // kept
    return { url };
  },
});
```

`publishEvent(type, payload, options?)` returns the published event. A durable
one has its record `seq`; a live one has `seq: 0`.

## From anywhere else

The runtime exposes the same thing for code that is not a tool: a webhook, a
cron job, a route that learned something about the thread.

```ts
await runtime.events.publishEvent(threadId, 'CREDIT_LIMIT', { kind: 'monthly' }, { state, durable: true });
```

`state` scopes the storage write the same way a run's state does, so a
tenant-scoped `Storage` sees who caused it.

## Live or durable

| | live (default) | `durable: true` |
| :--- | :--- | :--- |
| From a tool, during a run | goes to the run stream, as `CUSTOM { name, value }` | kept in the thread record |
| Anywhere else (no run stream open in this process) | a notice on the bus | kept in the thread record |
| Gets a `seq` | `0` | yes |
| A tab that reconnects within `streamGraceMs` | sees it (it is in the stream) | sees it |
| A tab that opens the thread next week | does not see it | it is still in the record (see below) |
| Use for | ticks: progress, typing, "still working" | facts: a preview URL, questions, a result |

A durable event is part of the thread's history. It is delivered once, as the
record entry: it does not also go to the run stream, so a tab never shows it
twice. A live event is gone once its stream is gone, which is exactly right
for something nobody needs to see twice.

## Events the platform publishes for you

Three platform events exist because a client needs them and only the engine
knows the moment:

| Event | When | Payload |
| :--- | :--- | :--- |
| `RUN_REFUSED` | `billingPreCheck` said no, at dispatch or at pickup, or the queue is at `maxQueueDepth` | `{ reason: 'billing' \| 'queue_full', error, runId? }` |
| `TOKEN_BUDGET_EXHAUSTED` | the run's spend crossed `tokenBudget` between steps, just before it stops | `{ agentId, tokensUsed, tokenBudget }` |
| `COST_BUDGET_EXHAUSTED` | the run's spend crossed `costBudgetMicros` between steps, just before it stops | `{ agentId, costMicros, costBudgetMicros, currency }` |

All three are kept in the thread record. The pre-check also receives
`publishEvent`, so it can say it in your own terms first: a reset date, a plan
name, a link. Pass `{ durable: true }` if that message must still show after a
reload.

## Types are yours, except the platform's

Any string is a valid type. The platform's own types are refused, because a
client's reducer trusts them to mean what the engine meant:

```ts
await publishEvent('STATE_CHANGE', …);   // throws: platform event type
```

The full list is exported as `RESERVED_EVENT_TYPES`. Use something that reads
as yours: `DESIGN_PREVIEW`, `CREDIT_LIMIT`, `app:progress`.

## On the client

The React hook hands your events to `onCustom(name, value)`, once each, whether
they came from the run stream or, when durable, from the record:

```tsx
const thread = useAgentThread({
  onCustom: (name, value) => {
    if (name === 'DESIGN_PREVIEW') setPreview((value as { url: string }).url);
  },
});
```

It also hands every event to `onEvent` before its own reducer, replayed and
live alike. A `CUSTOM` stream item reaches `onEvent` as an event whose `type`
is your name and whose `payload` is your value, so the same reducer handles
live and durable events. Keep your own state next to the hook's:

```tsx
const [ui, dispatch] = useReducer(reduce, { progress: null, preview: null, error: null });

const thread = useAgentThread({
  onEvent: (e) => {
    switch (e.type) {
      case 'MESSAGE_APPENDED':
        if (e.payload.role === 'user') dispatch({ type: 'reset' });   // a new turn clears the last
        break;
      case 'STATE_CHANGE':
        if (e.payload.state === 'FAILED') dispatch({ type: 'error', error: e.payload.error });
        break;
      case 'PROGRESS':
        dispatch({ type: 'progress', label: e.payload.label });
        return true;                                                  // yours: skip the built-in reducer
      case 'DESIGN_PREVIEW':
        dispatch({ type: 'preview', url: e.payload.url });
        return true;
    }
  },
});
```

Returning `true` tells the hook the event is handled. Returning nothing lets
the hook keep interpreting the built-in ones, so the two reducers compose.

### What replays, and what does not

A reconnecting client gets the **active run's** record entries and the open
run stream from the snapshot, then the live tail. A live event from a stream
that is gone does not come back. A durable event from an earlier, finished run
is still in the record and comes back from `runtime.events.since(threadId, -1)`,
but it is not in the snapshot. If a value must show forever, a preview URL for
instance, store it on your own side too and load it with the thread.

## Use case: a design agent

The [`examples/go-app`](https://github.com/eadwinCode/agentic-kit/tree/main/examples/go-app)
example is this scenario, running: a Go server, a React SPA, and every event
below on screen. It works offline on a built-in mock model.

Say an agent renders designs, asks the user questions along the way, and can
run out of credits. Each piece of UI state has a home:

| State | Where it comes from |
| :--- | :--- |
| "Rendering…" progress | `publishEvent('PROGRESS', …)` from the tool (live only), or the hook's `activity` |
| The preview URL | `publishEvent('DESIGN_PREVIEW', { url }, { durable: true })` from the tool |
| Questions for the user | mark the tool with `markRequiresConfirmation`: the run parks, the questions land in `pendingInputs`, the answers come back through `respondToInput(toolCallId, true, answers)` |
| Out of credits | `billingPreCheck` refuses the run when the user sends; it publishes your `CREDIT_LIMIT` (with the reset date) and the platform publishes `RUN_REFUSED`, so the chat shows the refusal in place |
| Budget spent mid-run | pass the remaining credit as the run's `tokenBudget` — or as `costBudgetMicros` with a pricer configured; the platform publishes `TOKEN_BUDGET_EXHAUSTED` / `COST_BUDGET_EXHAUSTED` between steps, then finalizes |
| An error | `STATE_CHANGE` with `state: 'FAILED'` carries the reason |
| Context usage | the hook's `usage.context`, loaded after every run |

The questions deserve the approval mechanism rather than a plain event: the run
is genuinely waiting, it survives a reload and a worker restart, and the answer
resumes it exactly where it stopped. See [Human in the loop](./human-in-the-loop.md).

## In Go

Same shape. A tool built with `AgentTool` receives a `ToolContext`; one built
with `goai.NewTool` reads it from the context.

```go
render := agentenkit.AgentTool("renderDesign", "Render a design preview",
	func(ctx context.Context, in struct{ Brief string `json:"brief"` }, tc agentenkit.ToolContext) (string, error) {
		tc.PublishEvent(ctx, "PROGRESS", map[string]any{"label": "Rendering…"}, agentenkit.PublishOptions{}) // live only
		url, err := render(ctx, in.Brief, tc.State["orgId"])
		if err != nil {
			return "", err
		}
		_, _ = tc.PublishEvent(ctx, "DESIGN_PREVIEW", map[string]any{"url": url}, agentenkit.PublishOptions{Durable: true})
		return url, nil
	})

// Anywhere else on the server:
rt.Events.PublishEvent(ctx, threadID, "CREDIT_LIMIT", map[string]any{"kind": "monthly"},
	agentenkit.PublishStateOptions{State: state, PublishOptions: agentenkit.PublishOptions{Durable: true}})
```

`PublishOptions{Durable: true}` is the Go spelling of `{ durable: true }`. The
old `Notice` field is gone: live only is now the default.
