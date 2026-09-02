# use-agentkit

A React hook for an [`@agentic-kit/core`](../agentic-kit) backend: durable
threads, live streaming, human-in-the-loop approvals and nested runs.

It owns the client state machine — hydrate from the durable snapshot, replay
the active run, then tail the event stream — and leaves every endpoint, string
and transport to you.

```bash
bun add use-agentkit
```

## Use it

```tsx
'use client';
import { useAgentThread } from 'use-agentkit';

export function Chat() {
  const { entries, run, stop, agentState, pendingInputs, respondToInput } =
    useAgentThread();

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
      <button onClick={() => (agentState === 'RUNNING' ? stop() : run('hello'))}>
        {agentState === 'RUNNING' ? 'Stop' : 'Send'}
      </button>
    </>
  );
}
```

With no config it calls the default routes below. Nothing else is assumed.

## Routes

| Route | Method | Default | Carries |
| --- | --- | --- | --- |
| `run` | POST | `/api/agent/run` | `{ threadId?, prompt, model, editMessageId? }` |
| `stop` | POST | `/api/agent/control` | `{ threadId }` |
| `respond` | POST | `/api/agent/respond` | `{ threadId, toolCallId, approved, payload? }` |
| `stream` | GET (SSE) | `/api/agent/stream` | `?threadId=&since=` |
| `history` | GET | `/api/agent/history` | `?threadId=` |
| `usage` | GET | `/api/agent/usage` | `?threadId=` |
| `threads` | GET | `/api/threads` | — |
| `deleteThread` | DELETE | `/api/threads` | `?threadId=` |

Override any subset. A route is either a path the hook appends its own query
to, or a function that builds the whole URL — the function form covers path
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

## Configure once

`AgentKitProvider` sets the config for everything below it. A hook's own
options still win, section by section, so a component can replace one route
without restating the rest.

```tsx
'use client';
import { AgentKitProvider } from 'use-agentkit';

export function Providers({ children }) {
  return (
    <AgentKitProvider config={{ routes: { run: '/v2/agent/start' } }}>
      {children}
    </AgentKitProvider>
  );
}
```

## Everything else you can change

| Option | Default | What it is for |
| --- | --- | --- |
| `fetch` | global `fetch` | auth wrappers, retries, a test double |
| `headers` | none | sent with every request; a function is called per request, so a rotating token stays fresh |
| `openStream` | `browserEventStream` | how the event stream is opened — see below |
| `defaultModel` | `'gpt-4o'` | used when `run()` names no model |
| `persistence` | `browserPersistence()` | where the open thread id is remembered; `false` keeps it in memory only |
| `labels` | English | every user-facing string the hook produces |
| `format` | plain text | how tool calls, results and subagent notices render |
| `onEvent` | none | see every event first; return `true` to handle it yourself |
| `threadsRefreshMs` | `30000` | background refresh of the thread list; `false` disables it |
| `loadThreadsOnMount` | `true` | off for a single-thread embed |

### Auth on the stream

`headers` does **not** reach the event stream. The browser's `EventSource`
cannot send headers — that is a limitation of the API, not of this package. Use
a cookie, or supply your own `openStream`:

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

### Your own events

`onEvent` runs before the built-in reducer and can claim an event:

```ts
useAgentThread({
  onEvent: (event) => {
    if (event.type === 'MY_APP_EVENT') {
      handle(event.payload);
      return true; // handled — skip the built-in reducer
    }
  },
});
```

## Streaming thought

Models that expose their reasoning stream it separately from the answer. The
hook keeps the two apart: thinking arrives as entries with `kind: 'reasoning'`,
in order, so a UI can show it live and fold it away once the answer starts.

```tsx
{entries.map((e) =>
  e.kind === 'reasoning' ? <Thought key={e.id} text={e.text} /> : <Bubble key={e.id} {...e} />,
)}
```

It reads the same on a reload as it did live: a persisted message's reasoning
parts are lifted into their own entry rather than folded into the answer text.

Whether you see anything depends on the model. Providers that do not expose
reasoning simply never send it, and no reasoning entries appear — OpenAI's
`gpt-4o` is one of those, and the o-series reports reasoning *token counts*
without the text. Anthropic extended thinking and DeepSeek R1 do send it.

## What the hook returns

State: `threadId`, `entries`, `agentState`, `activity`, `historyLoading`,
`pendingInputs`, `subagents`, `threads`, `threadsLoading`, `usage`.

Actions: `run`, `stop`, `respondToInput`, `newThread`, `selectThread`,
`deleteThread`, `loadThreads`, `loadUsage`.

An entry is `{ id, kind, role, text, agentId }`, where `kind` is `'text'`,
`'tool'` or `'reasoning'`.

## Notes

- The server is the only source of truth. A reload, or a second tab, rebuilds
  from the durable snapshot and then resumes the stream at its cursor.
- `run()` adds the user's turn locally before the request goes out, then
  replaces it with the durable one when the server confirms it, so the id is
  real and the message can be edited.
- Config is read fresh on every request, but an open stream keeps the URL it
  was opened with. Change routes at mount, not mid-run.
