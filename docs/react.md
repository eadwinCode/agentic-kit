# React: use-agentrun

```bash
bun add use-agentrun
```

A hook that owns the client state machine — hydrate from the durable snapshot,
replay the active run, then tail the event stream — and leaves every endpoint,
string and transport to you.

## The smallest thing that works

```tsx
'use client';
import { useAgentThread } from 'use-agentrun';

export function Chat() {
  const { entries, run, stop, agentState, pendingInputs, respondToInput } = useAgentThread();
  const running = agentState === 'RUNNING' || agentState === 'WAITING_FOR_INPUT';

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
| `agentState` | `IDLE` · `RUNNING` · `WAITING_FOR_INPUT` · `COMPLETED` · `CANCELLED` · `FAILED` |
| `activity` | `{ phase, label, detail }` — a live status line |
| `historyLoading` | hydration in progress |
| `pendingInputs` | approvals waiting on a human |
| `subagents` | nested runs, with status and text |
| `threads`, `threadsLoading` | the thread list, for a sidebar |
| `usage` | tokens spent and context load |

**Actions**

`run`, `stop`, `respondToInput`, `newThread`, `selectThread`, `deleteThread`,
`loadThreads`, `loadUsage`.

## Entries

```ts
{ id, kind: 'text' | 'tool' | 'reasoning', role, text, agentId }
```

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
```

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
import { AgentRunProvider } from 'use-agentrun';

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
| `defaultModel` | `'gpt-4o'` | when `run()` names no model |
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
  openStream: (url, { onMessage, onError }) => {
    const source = new MyAuthedEventSource(url, { token });
    source.onmessage = (e) => onMessage(e.data);
    source.onerror = onError;
    return { close: () => source.close() };
  },
});
```

The same hook takes a WebSocket or fetch-streaming transport.

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

Returning `true` also lets you override a built-in type.

### Translating

```ts
useAgentThread({
  labels: { thinking: 'Denkt na', waitingApproval: 'Wacht op goedkeuring' },
});
```

Unlisted labels keep their defaults.

## Multiple tabs

Two tabs on the same thread stay in sync: a message sent in one appears in the
other, an edit truncates both, and a tab opened mid-run rebuilds what has
happened so far and then follows along. You do not have to do anything for this
— it falls out of hydrate-then-tail.

## Notes

- The server is the only source of truth.
- Config is read fresh on every request, but an open stream keeps the URL it was
  opened with. Change routes at mount, not mid-run.
- The hook is client-side. In Next.js App Router, the component using it needs
  `'use client'`, and so does any provider above it.
