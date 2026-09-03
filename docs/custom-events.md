# Custom events

The event log is the one channel between a run and everyone watching it. The
platform writes to it (`CHUNK`, `STATE_CHANGE`, `INPUT_REQUIRED`, …) and every
client reads it, live and on reconnect. `publishEvent` lets **your** code write
to it too, so anything a run learns along the way reaches the UI through the
same pipe: a progress label, a preview URL, a credit warning, a set of
questions to show.

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
    await publishEvent('PROGRESS', { label: 'Rendering…' }, { durable: false });
    const url = await render(brief, state.orgId);
    await publishEvent('DESIGN_PREVIEW', { url });
    return { url };
  },
});
```

`publishEvent(type, payload, options?)` returns the published event, with its
`seq`.

## From anywhere else

The runtime exposes the same thing for code that is not a tool: a webhook, a
cron job, a route that learned something about the thread.

```ts
await runtime.events.publishEvent(threadId, 'CREDIT_LIMIT', { kind: 'monthly' }, { state });
```

`state` scopes the storage write the same way a run's state does, so a
tenant-scoped `Storage` sees who caused it.

## Durable or notice

| | `durable: true` (default) | `durable: false` |
| :--- | :--- | :--- |
| Written to `storage.events` | yes | no |
| Gets a `seq` | yes | `0` |
| Replayed to a reconnecting client | yes | no |
| Fanned out live over the bus | yes | yes |
| Use for | facts: a preview URL, questions, a result | ticks: progress, typing, "still working" |

A durable event is part of the thread's history. A notice is gone the moment it
is delivered, which is exactly right for something nobody needs to see twice.

## Events the platform publishes for you

Two platform events exist because a client needs them and only the engine
knows the moment:

| Event | When | Payload |
| :--- | :--- | :--- |
| `RUN_REFUSED` | `billingPreCheck` said no | `{ reason: 'billing', error }` |
| `TOKEN_BUDGET_EXHAUSTED` | the run's spend crossed `tokenBudget` between steps, just before it stops | `{ agentId, tokensUsed, tokenBudget }` |
| `COST_BUDGET_EXHAUSTED` | the run's spend crossed `costBudgetMicros` between steps, just before it stops | `{ agentId, costMicros, costBudgetMicros, currency }` |

Both are durable. The pre-check also receives `publishEvent`, so it can say
it in your own terms first: a reset date, a plan name, a link.

## Types are yours, except the platform's

Any string is a valid type. The platform's own types are refused, because a
client's reducer trusts them to mean what the engine meant:

```ts
await publishEvent('STATE_CHANGE', …);   // throws: platform event type
```

The full list is exported as `RESERVED_EVENT_TYPES`. Use something that reads
as yours: `DESIGN_PREVIEW`, `CREDIT_LIMIT`, `app:progress`.

## On the client

The React hook hands every event to `onEvent` before its own reducer, replayed
and live alike. Keep your own state next to the hook's:

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

A reconnecting client gets the **active run's** events from the snapshot, then
the live tail. A durable event from an earlier, finished run is still in the
log and comes back from `runtime.events.since(threadId, -1)`, but it is not in
the snapshot. If a value must show forever, a preview URL for instance, store it
on your own side too and load it with the thread.

## Use case: a design agent

The [`examples/go-app`](https://github.com/eadwinCode/agentic-kit/tree/main/examples/go-app)
example is this scenario, running: a Go server, a React SPA, and every event
below on screen. It works offline on a built-in mock model.

Say an agent renders designs, asks the user questions along the way, and can
run out of credits. Each piece of UI state has a home:

| State | Where it comes from |
| :--- | :--- |
| "Rendering…" progress | `publishEvent('PROGRESS', …, { durable: false })` from the tool, or the hook's `activity` |
| The preview URL | `publishEvent('DESIGN_PREVIEW', { url })` from the tool |
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
		tc.PublishEvent(ctx, "PROGRESS", map[string]any{"label": "Rendering…"}, agentenkit.PublishOptions{Notice: true})
		url, err := render(ctx, in.Brief, tc.State["orgId"])
		if err != nil {
			return "", err
		}
		_, _ = tc.PublishEvent(ctx, "DESIGN_PREVIEW", map[string]any{"url": url}, agentenkit.PublishOptions{})
		return url, nil
	})

// Anywhere else on the server:
rt.Events.PublishEvent(ctx, threadID, "CREDIT_LIMIT", map[string]any{"kind": "monthly"},
	agentenkit.PublishStateOptions{State: state})
```

`PublishOptions{Notice: true}` is the Go spelling of `{ durable: false }`.
