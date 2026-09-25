# React: use-agentenkit

```bash
bun add use-agentenkit
```

A hook that owns the client state machine — hydrate from the durable snapshot,
replay the active run, then tail the event stream — and leaves every endpoint,
string and transport to you.

## The smallest thing that works

```tsx
'use client';
import { useAgentThread } from 'use-agentenkit';

export function Chat() {
  const { entries, run, stop, agentState, pendingInputs, respondToInput } = useAgentThread();
  const running = agentState === 'QUEUED' || agentState === 'RUNNING' || agentState === 'WAITING_FOR_INPUT';

  return (
    <>
      {entries.map((e) => (
        <p key={e.id}>{e.role}: {e.text}</p>
      ))}

      {pendingInputs.map((input) => (
        <button key={input.toolCallId} onClick={() => respondToInput(input.toolCallId, true)}>
          Approve {input.toolName}
        </button>
      ))}

      <button onClick={() => (running ? stop() : run('hello'))}>
        {running ? 'Stop' : 'Send'}
      </button>
    </>
  );
}
```

With no configuration it calls the [default routes](./http-api.md). Nothing else
is assumed.

## What it returns

**State**

| | |
| :--- | :--- |
| `threadId` | the open thread, or `undefined` |
| `entries` | the conversation — see below |
| `agentState` | `IDLE` · `QUEUED` · `RUNNING` · `WAITING_FOR_INPUT` · `COMPLETED` · `CANCELLED` · `FAILED` — `QUEUED` is accepted and waiting for a worker |
| `activity` | `{ phase, label, detail }` — a live status line |
| `historyLoading` | hydration in progress |
| `pendingInputs` | requests the run is parked on: `reason: 'approval'` waits on a human (show a card); any other reason is a tool waiting on work it started (show a status) |
| `subagents` | nested runs, with status and text |
| `threads`, `threadsLoading` | the thread list, for a sidebar |
| `usage` | tokens spent, money spent, and context load |
| `currentRun` | the thread's latest run: `{ id, startedAt?, endedAt? }`, or `null` before the first |
| `connection` | the live stream: `connecting` · `open` · `reconnecting` · `closed` |
| `error` | why the last send did not go through, or why the server refused the run (`RUN_REFUSED`); `null` otherwise |

`currentRun` is what a "running for 1:32" timer counts from. It is hydrated
from the snapshot's `runs` and then kept from `STATE_CHANGE` alone: every one
names its run and carries `startedAt` while the run is running or waiting, or
`endedAt` once it ended. A server that leaves a clock off still gets a timer:
the event's own time stands in. A client never refetches history for timing.

`connection` says where the stream stands. A dropped connection is
`reconnecting` while the transport retries on its own. One the transport gave
up on (a 401, a 404) is `closed`: the hook then reads the thread again and
opens a new stream from where it stands, waiting longer each time (1 s,
doubling, up to 30 s). An event the hook already has — a replay, or a
transport that resent after reconnecting — is dropped before anything sees it,
so text is never shown twice.

**Actions**

`run`, `stop`, `respondToInput`, `newThread`, `selectThread`, `deleteThread`,
`loadThreads`, `loadUsage`.

`run()` is refused while a run is queued, running or waiting on an approval,
and while an earlier send is still on its way: a second send would wipe the
first run's approval cards, and two fast sends on a new thread would make two
threads. A send that fails — refused by the server, or lost in transit — puts
the conversation back exactly as it was and sets `error`; it does not mark the
thread `FAILED`. The next accepted send clears `error`. An edit of a turn the
server has not confirmed yet is refused too: it has no id the server knows.

The turn a send adds right away is swapped for the real one by an id the hook
sends with it (`clientMessageId`, echoed on `MESSAGE_APPENDED`), never by
matching its text.

`stop()` and `respondToInput()` return `true` when the server confirms the
action, and `false` on refusal or a transport failure. Failures appear in
`activity.label` and `activity.detail`; `agentState` still reflects the run.
Approval cards stay visible until delivery is confirmed. Customize the error
labels with `labels.stopFailed` and `labels.responseFailed`.

## Entries

```ts
{ id, kind: 'text' | 'tool' | 'reasoning', role, text, agentId, parts }
```

`text` is the flat rendering. `parts` is the structure behind it, for a UI
that draws tool cards, thumbnails, or a foldable thought:

```ts
type EntryPart =
  | { type: 'text'; text }
  | { type: 'reasoning'; text }
  | { type: 'image'; image; mimeType? }
  | { type: 'tool-call'; toolCallId; toolName; args; state: 'running' | 'done' | 'error'; result? }
  | { type: 'tool-result'; toolCallId; toolName?; result };
```

A tool call's `state` flips to `done` (or `error`) in place when its result
arrives, live or on reload, so a card can settle without a second lookup.
Live, the runtime publishes a `tool-result` chunk for every tool it ran,
before the step commits. On reload the state is derived from the durable tool
messages, which is how a denied approval, a stop, or an approval that ran the
tool later gets its state: those never stream a result. `error` means the
result reports a failure (`{ error }`, or the `error: …` text a failed tool's
result carries); a denial or a cancellation is an answer, so the call is
`done`. Parts are filled the same way from the durable snapshot and from the
stream: a reload renders what the live run did.

`kind` is what a UI branches on:

```tsx
{entries.map((e) =>
  e.kind === 'reasoning' ? <Thought key={e.id} text={e.text} /> :
  e.kind === 'tool'      ? <ToolLine key={e.id} text={e.text} /> :
                           <Bubble key={e.id} {...e} />
)}
```

## Streaming thought

Models that expose their reasoning stream it separately from the answer. The
hook keeps the two apart, so you can show thinking live and fold it away once
the answer starts.

It reads the same on a reload as it did live: a persisted message's reasoning
parts are lifted into their own entry rather than folded into the answer text.

Whether you see anything depends on the model. Providers that do not expose
reasoning never send it, and no reasoning entries appear — OpenAI's `gpt-4o` is
one of those, and the o-series reports reasoning *token counts* without the
text. Anthropic extended thinking and DeepSeek R1 do send it.

## Sending

```ts
await run('what is the weather?');
await run('…', { model: 'gpt-4o-mini' });
await run('corrected text', { editMessageId: someUserMessageId });
await run('…', { providerOptions: { openai: { serviceTier: 'flex' } } });
await run('…', { orgId: 'acme' });   // extra fields merge into the request body
await run('what is this?', { attachments: [{ url: 'https://cdn/cat.png', mediaType: 'image/png' }] });
await run('…', { runId: myRunId, maxSteps: 8 });
```

`attachments` show on the optimistic turn as image parts and reach the model
natively. `runId` names the run so your own records can be keyed by it before
the server answers; `maxSteps` caps the run below the server's ceiling. Your
run route must pass them through to `run()`; both runtimes accept them.

The hook adds the user's turn locally before the request goes out, then replaces
it with the durable one when the server confirms — so the id is real and the
message can be edited.

## Routes

| Route | Method | Default |
| :--- | :--- | :--- |
| `run` | POST | `/api/agent/run` |
| `stop` | POST | `/api/agent/control` |
| `respond` | POST | `/api/agent/respond` |
| `stream` | GET (SSE) | `/api/agent/stream` |
| `history` | GET | `/api/agent/history` |
| `usage` | GET | `/api/agent/usage` |
| `threads` | GET | `/api/threads` |
| `deleteThread` | DELETE | `/api/threads` |

Override any subset. A route is either a path the hook appends its own query to,
or a function that builds the whole URL — the function form covers path
parameters, a different query vocabulary, or another host:

```ts
useAgentThread({
  baseUrl: 'https://api.example.com',
  routes: {
    run: '/v2/agent/start',
    history: ({ threadId }) => `/v2/threads/${threadId}`,
  },
});
```

## Configuring once

```tsx
'use client';
import { AgentRunProvider } from 'use-agentenkit';

export function Providers({ children }: { children: React.ReactNode }) {
  return (
    <AgentRunProvider config={{ routes: { run: '/v2/agent/start' } }}>
      {children}
    </AgentRunProvider>
  );
}
```

A hook's own options still win, section by section — a component can replace one
route without restating the rest.

## Everything else you can change

| Option | Default | For |
| :--- | :--- | :--- |
| `fetch` | global `fetch` | auth wrappers, retries, a test double |
| `headers` | none | sent with every request; a function is called per request, so a rotating token stays fresh |
| `openStream` | `browserEventStream` | how the stream is opened — see below |
| `defaultModel` | none | the model `run()` sends when the call names none; unset sends no model, and the server uses the agent's own |
| `persistence` | `browserPersistence()` | where the open thread id is remembered; `false` keeps it in memory |
| `labels` | English | every user-facing string the hook produces |
| `format` | plain text | how tool calls, results and subagent notices render |
| `onEvent` | none | see every event first; return `true` to claim it |
| `threadsRefreshMs` | `30000` | background thread-list refresh; `false` disables |
| `loadThreadsOnMount` | `true` | off for a single-thread embed |

### Authentication on the stream

`headers` does **not** reach the event stream. The browser's `EventSource`
cannot send headers — an API limitation, not a package one. Use a cookie, or
supply your own opener:

```ts
useAgentThread({
  openStream: (url, { onMessage, onError, onOpen, onClose, getCursor }) => {
    const source = new MyAuthedEventSource(url, { token });
    source.onopen = () => onOpen?.();
    source.onmessage = (e) => onMessage(e.data);
    source.onerror = onError; // retrying on its own
    source.onclose = () => onClose?.(); // gave up: the hook re-reads and reopens
    return { close: () => source.close() };
  },
});
```

The same hook takes a WebSocket or fetch-streaming transport. One that
reconnects on its own should resume after `getCursor()`, the seq of the last
event the hook applied; the hook drops anything at or below it anyway.

The default persistence keeps the thread id in the URL with
`history.replaceState`, passing the page's own history state through, so it
does not disturb the Next.js router.

### Your own event types

```ts
useAgentThread({
  onEvent: (event) => {
    if (event.type === 'MY_APP_EVENT') {
      handle(event.payload);
      return true;   // handled — skip the built-in reducer
    }
  },
});
```

Returning `true` also lets you override a built-in type. Your own events come
from `publishEvent` on the server; [Custom events](./custom-events.md) walks
through a full reducer.

### Translating

```ts
useAgentThread({
  labels: { thinking: 'Denkt na', waitingApproval: 'Wacht op goedkeuring' },
});
```

Unlisted labels keep their defaults.

## Multiple tabs

Two tabs on the same thread stay in sync: a message sent in one appears in the
other, an edit truncates both, an approval answered in one drops its card in
the other (`HITL_RESPONSE`), a thread deleted in one resets the other
(`THREAD_DELETED`), and a tab opened mid-run rebuilds what has happened so far
and then follows along. You do not have to do anything for this
— it falls out of hydrate-then-tail.

## Notes

- The server is the only source of truth.
- Switching threads clears the old thread's messages, cards and run at once,
  and a slow answer for a thread you already left never lands.
- Streamed text is shown once per animation frame, not re-rendered per token.
- A generate-text agent streams nothing; its whole answer arrives as one
  `TEXT_RESULT` and is shown as one entry.
- Config is read fresh on every request, but an open stream keeps the URL it was
  opened with. Change routes at mount, not mid-run.
- The hook is client-side. In Next.js App Router, the component using it needs
  `'use client'`, and so does any provider above it.
