# Agents and tools

## Registering an agent

```ts
export const chat = runtime.createStreamTextAgent({
  name: 'chat',            // the dispatch key — must be unique and stable
  model: 'gpt-4o',         // a key your resolveModel understands
  system: 'You are terse.',
  tools: { sendEmail },
});
```

`createGenerateTextAgent` is the same, without streaming.

`name` is how a queue job finds its way back to this handle. Renaming an agent
with jobs in flight strands them.

### Register where you build the runtime

The registry lives inside the runtime's closure. In frameworks that give each
route its own module instance — Next.js does — a worker route that imports the
runtime from a file which does **not** also register the agents will resolve
every job to an unknown agent.

Put `setupAgentCore` and every `create*Agent` call in one module, and import that
module everywhere.

### A system prompt built per step

`system` is a string. When the persona depends on what the run is acting on
— a project, a page, a user's settings — give the spec a `systemFn` (Go:
`SystemFn`) instead. It is called once per step with the thread id and the
run's [state](./run-state.md), and wins over `system`. A throw (Go: an error)
fails the step, like a model error:

```ts
runtime.createStreamTextAgent({
  name: 'designer',
  systemFn: async (threadId, state) => {
    const project = await projects.load(state.projectId as string);
    return `${stablePersona}\n\n${project.brief()}`;
  },
});
```

```go
rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{
	Name: "designer",
	SystemFn: func(ctx context.Context, threadID string, state agentenkit.AgentRunState) (string, error) {
		project, err := projects.Load(ctx, state["projectId"].(string))
		if err != nil {
			return "", err // fails the run rather than prompting blind
		}
		return stablePersona + "\n\n" + project.Brief(), nil
	},
})
```

Keep the stable part first. Prompt caching stamps the system message as a
cached prefix, and a prefix that moves every step is a prefix that never
hits.

### Context for one step only

`prepareStep` (Go: `PrepareStep`) edits the prompt just before each step is
sent. It gets the thread id, the run's state and the messages the platform
assembled, and what it returns is what the model sees. It is the place for
context that must not be saved — a screenshot to look at once, an editor
snapshot — because nothing added here reaches the stored history:

```ts
prepareStep: (threadId, state, messages) => [
  ...messages,
  { role: 'user', content: [{ type: 'image', image: currentScreenshot() }] },
],
```

## Models

`resolveModel` turns your registry key into the two things the platform needs:

```ts
resolveModel: (name) => {
  const model = registry[name];
  if (!model) throw new Error(`Unknown model: ${name}`);
  return { instance: () => model, contextWindow: 128_000 };
}
```

`contextWindow` feeds compaction. A window declared here wins over the
`nativeWindows` config table.

Throw for an unknown key. A model name can reach this function from a run
request or from a subagent the model itself named, and a wrong name should fail
loudly rather than silently fall back to something else.

## Tools

Tools are AI SDK tools. The platform does not wrap them unless you ask:

```ts
import { tool } from 'ai';
import { z } from 'zod';

const lookup = tool({
  description: 'Look something up',
  parameters: z.object({ query: z.string() }),
  execute: async ({ query }) => search(query),
});
```

### Tools that need a human first

```ts
import { markRequiresConfirmation } from 'agentenkit';

const sendEmail = markRequiresConfirmation(
  tool({
    description: 'Sends an email (destructive)',
    parameters: z.object({ to: z.string().email(), subject: z.string(), body: z.string() }),
    execute: async (args) => send(args),
  }),
);
```

The run parks instead of executing, and resumes where it stopped once a human
answers. See [Human in the loop](./human-in-the-loop.md).

A tool that throws (TS) or returns an error (Go) does not fail the run: the
model gets `error: <message>` as the call's result and decides what to do next,
in both runtimes. A user stop is the exception: it ends the run.

### Tools see the run's state

Every tool receives the run's [state](./run-state.md) as part of its second
argument — the same object the caller attached to `run()`, present for nested
runs and for segments resumed after an approval.

Use `agentTool` to get it **typed**:

```ts
import { agentTool } from 'agentenkit';

const lookupInvoice = agentTool({
  description: 'Find one invoice',
  parameters: z.object({ invoiceId: z.string() }),
  execute: async ({ invoiceId }, { state, toolCallId }) =>
    db.invoice.findFirst({ where: { id: invoiceId, orgId: state.orgId } }),
});
```

It returns an ordinary AI SDK tool, so it goes anywhere `tool()` does — and
composes with `markRequiresConfirmation`:

```ts
const sendEmail = markRequiresConfirmation(
  agentTool({
    parameters: z.object({ to: z.string().email() }),
    execute: async ({ to }, { state }) => send(to, state.orgId),
  }),
);
```

**Why a helper rather than plain `tool()`?** The platform injects `state` at
call time, but the AI SDK's own `ToolExecutionOptions` has no field for it, and
narrowing that parameter yourself is rejected by TypeScript as unsound:

```ts
// ✗ Property 'state' does not exist on type 'ToolExecutionOptions'
tool({ parameters, execute: async (args, { state }) => … })
```

With a plain `tool()` the state still arrives — you just have to reach for it
yourself:

```ts
import type { ToolContext } from 'agentenkit';

tool({
  parameters,
  execute: async (args, options) => {
    const { state } = options as ToolContext;
    …
  },
});
```

The model cannot reach outside the tenant even if it asks to, because the tool
— not the prompt — decides what it can see.

### Tools can publish events

The same second argument carries `publishEvent`, bound to the thread the tool
runs on. Anything the tool learns can reach the UI through the event log, live
and on reconnect:

```ts
execute: async ({ brief }, { publishEvent }) => {
  const url = await render(brief);
  await publishEvent('DESIGN_PREVIEW', { url });
  return { url };
},
```

See [Custom events](./custom-events.md) for the durable/notice choice and the
client side.

## Budgets and ceilings

Three different limits, easy to confuse:

| Limit | Set by | What it stops |
| :--- | :--- | :--- |
| `tokenBudget` | run input, agent spec, or config | Spend, in tokens, across the whole run including nested runs |
| `maxSteps` | config (default 25) | Runaway loops — model round trips per run |
| `contextCeilingTokens` | config | The prompt growing past what a model can take |

```ts
await chat.run({ prompt: 'hi', tokenBudget: 50_000 });
```

The budget counts a child's spend against its parent the moment it happens — a
budget that ignores delegated work is not a budget.

## Provider-specific options

```ts
runtime.createStreamTextAgent({
  name: 'researcher',
  model: 'claude-sonnet-4',
  providerOptions: { anthropic: { thinking: { type: 'enabled', budgetTokens: 8_000 } } },
});
```

Settable at three levels — runtime config, agent spec, run input — each
overriding the one before it per provider namespace. See
[Provider options](./provider-options.md).

## Starting a run

```ts
const result = await chat.run({
  threadId,               // omit to create a thread
  prompt: 'hi',
  model: 'gpt-4o',        // optional — falls back to the spec, then 'gpt-4o'
  tokenBudget: 50_000,
  state: { orgId: 'acme' },
  editMessageId,          // replace this user turn and resend
  providerOptions: { openai: { serviceTier: 'flex' } },
});
// → { accepted, threadId, runId }  |  { accepted: false, error }
```

Model resolution order: run input → agent spec → `'gpt-4o'`.

`accepted: false` means the thread already has an active run, or your
`billingPreCheck` rejected it. Nothing was written.

Four more fields:

| Field | What it does |
| :--- | :--- |
| `runId` | Name the run yourself. Your own records (a workspace, a billing line) can be keyed by it *before* dispatch, and the worker sees the same id. A reused id is refused with `accepted: false`. |
| `maxSteps` | Cap this run's round trips below the config's `maxSteps`. 0 keeps the config value; more is clamped to it; a negative one is refused. |
| `attachments` | Images sent with the prompt (`{ url, mediaType }`). Stored as image parts on the user turn and handed to the model natively. |
| `partitionKey` | Your tenant, written on the dispatch ticket so a queue that spreads its claims across partitions keeps one tenant's backlog from starving the others. |

A refusal carries a `reason` a host can act on: `active_run`, `queue_full`
(try again shortly) or `billing`.

A budget (`tokenBudget`, `costBudgetMicros`) of `0` means "no cap from this
level": the agent spec's applies, then the config's. A negative one is refused.

```ts
await chat.run({
  prompt: 'what is in this picture?', runId, maxSteps: 8,
  attachments: [{ url: 'https://cdn.example/cat.png', mediaType: 'image/png' }],
});
```

```go
chat.Run(ctx, agentenkit.RunInput{
	Prompt: "what is in this picture?", RunID: runID, MaxSteps: 8,
	Attachments: []agentenkit.Attachment{{URL: "https://cdn.example/cat.png", MediaType: "image/png"}},
})
```

## Settling a run

`onFinish` fires after the terminal state is written, which is too late for
work every client must see as done the moment the run ends: committing the
files a run edited, charging for it. `onSettle` (Go: `OnSettle`) runs after
the last step and **before** the terminal `STATE_CHANGE`:

```ts
runtime.createStreamTextAgent({
  name: 'designer',
  onSettle: async (info) => {
    if (info.cancelled) return repo.discard(info.runId!);
    await repo.commit(info.runId!); // a throw finalizes the run FAILED, with this reason
    await billing.charge(info.runId!, info.usage);
  },
});
```

```go
rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{
	Name: "designer",
	OnSettle: func(ctx context.Context, info agentenkit.RunFinishInfo) error {
		if info.Cancelled {
			return repo.Discard(ctx, info.RunID)
		}
		if err := repo.Commit(ctx, info.RunID); err != nil {
			return err // the run finalizes FAILED, with this reason
		}
		return billing.Charge(ctx, info.RunID, info.Usage)
	},
})
```

It runs once per run, **whatever way the run ends**:

| End | Who settles |
| :--- | :--- |
| Completed, or stopped mid-step | The worker, before the terminal state |
| Stopped while queued or parked | The stop itself, with `cancelled` set and the usage of the steps it did make |
| Failed (attempts spent, lock never cleared) | Whoever fails it, with `error` set |
| The settler died, or the hook failed | The late-settle sweep, `reclaimStuckRuns` (Go: `ReclaimStuckRuns`) |

Rules:

- An error fails a run that was going to complete: the reason lands on the
  terminal event and the run record. A stop reaches the hook with `cancelled`
  set, and its error is ignored.
- An error also leaves the run **unsettled**, so the sweep runs the hook
  again. Call `reclaimStuckRuns` from a periodic job to get that retry.
- In Go the hook's `ctx` is never cancelled, not by a stop and not by a
  shutdown, so its writes land. Tell a stop apart by `Cancelled`, not by
  `ctx.Err()`.
- `onFinish` fires once too, on every end, after the terminal state.

**Once** is kept by a claim on the run record: before the hook runs, one
conditional write claims the run, and only the settler that wins calls the
hook. A stop and a worker that arrive together cannot both bill. Success marks
the run settled (`settledAt`); a claim left by a settler that died is taken
over after 10 minutes. So the hook can still, rarely, see one run twice — a
hook slower than 10 minutes, or a settle mark that could not be written — and
**must be idempotent on `runId`**. Pass it as the idempotency key to whatever
you charge.

## Stopping

```ts
await chat.stop(threadId);
```

Works regardless of which agent's run is active — stop belongs to the thread,
not the handle. One durable write; the worker notices within `stopPollMs`.

## Streaming callbacks

`onChunk`, `onFinish` and `onStepFinish` from the AI SDK still fire. The
platform chains its own handlers around yours rather than replacing them, so
your callback runs *and* the event still reaches the log and the bus.

Platform-owned keys — `model`, `messages`, `tools`, `maxSteps`, `abortSignal` —
are set by the engine and cannot be overridden from the spec.
