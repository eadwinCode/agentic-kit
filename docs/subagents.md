# Subagents

## The idea

**A subagent is a run.** Not a special object, not a second engine — the same
loop, the same table, the same persistence, with `depth > 0` and a
`parentRunId`.

That one decision buys everything else: a subagent can use tools, can park for a
human, can be resumed after an approval, and shows up in the operational store
next to every other run. Nothing had to be built twice.

## Turning it on

Delegation is opt-in:

```ts
export const chat = runtime.createStreamTextAgent({
  name: 'chat',
  model: 'gpt-4o',
  subagents: true,
});
```

The platform injects a scoped `spawnSubagent` tool. The model calls it with a
name and instructions.

## Giving children tools

```ts
runtime.createStreamTextAgent({
  name: 'chat',
  model: 'gpt-4o',
  subagents: { tools: { sendEmail } },
  tools: { sendEmail },
});
```

Tools listed under `subagents` are merged into every child and wrapped exactly
as the parent's are — so a child that calls a marked tool parks for approval,
and is resumed where it stopped rather than restarted.

## Named profiles

> Go runtime.

The default child is a generalist with one persona and the shared tools.
When delegation should go to specialists — a page manager, a copywriter, an
analyst — name them:

```go
Subagents: &agentenkit.SubagentsConfig{
	Profiles: map[string]agentenkit.SubagentProfile{
		"page-manager": {
			Description: "edits and reorders the pages of the site",
			SystemFn:    pageManagerPrompt,
			Model:       "gpt-4o-mini",
			Tools:       pageTools,
			MaxSteps:    20,
		},
		"copywriter": {Description: "writes headings and body copy", System: "You write clean copy."},
	},
},
```

With profiles set, `spawnSubagent`'s description lists the names and what
each one does, and `name` must be one of them; an unknown name comes back to
the model as a tool error, not a crash. The child takes the profile's
persona (`SystemFn` wins over `System`), model, tools and step cap. Everything
else is unchanged: it is still a run, it still parks for approval, and it is
re-entered under the same profile after one.

## What a child gets

A child receives its **brief**, not the parent's transcript. Its turns are
persisted in the same thread under its own `agentId`, so they are its
transcript, not the conversation's. A UI that renders the main thread filters to
`agentId === null`.

The result handed back to the parent is capped at
`subagentResultCapChars` (default 8000).

## Limits

| Setting | Default | Meaning |
| :--- | :--- | :--- |
| `subagentMaxDepth` | 2 | How deep nesting may go |
| `subagentMaxConcurrent` | 3 | Children running at once per run |
| `subagentMaxSteps` | 10 | Model round trips per child |
| `subagentResultCapChars` | 8000 | Characters returned to the parent |

Depth 2 means a child may spawn a grandchild, and there it stops. Exceeding the
cap is reported to the caller as a tool result, not raised as a crash.

## Failure is a result, not a crash

A child that fails reports back to its parent instead of killing the run. The
parent's model sees a failed tool result and decides what to do — retry
differently, work around it, or tell the user. That is usually what you want; an
agent whose helper failed is not an agent that should stop existing.

The failure carries the reason, which matters more than it sounds: a model that
invents a model name gets a clear "unknown model" rather than a silent
disappearance.

## The token ledger

A child's spend counts against the run's budget the moment it happens, through a
ledger shared by reference. Attribution still records which agent spent what, so
you can see the split — but the cap is enforced across the whole tree.

## Watching them

Four event types describe a child's life: `SUBAGENT_STARTED`, `SUBAGENT_CHUNK`,
`SUBAGENT_COMPLETED`, `SUBAGENT_FAILED`. `use-agentenkit` turns them into a
`subagents` array:

```tsx
{subagents.map((s) => (
  <div key={s.agentId} data-depth={s.depth}>
    <strong>{s.name}</strong> — {s.status}
    {s.error && <span>{s.error}</span>}
    <pre>{s.text}</pre>
  </div>
))}
```

Those events only replay while a run is unfinished. On a completed thread the
hook rebuilds each card from the durable run rows and the child's persisted
turns instead, so a reload does not lose a subagent's output.
