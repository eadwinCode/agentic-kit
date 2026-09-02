# Provider options

Provider-specific settings — a reasoning budget, a service tier, a safety
identifier, a cache hint — passed straight through to the provider by the AI
SDK. The platform does not interpret them.

They are keyed by **provider namespace**, so options for a provider you are not
using are simply ignored:

```ts
{
  openai: { serviceTier: 'flex' },
  anthropic: { thinking: { type: 'enabled', budgetTokens: 8_000 } },
}
```

## Three places to set them

Widest first. Each level overrides the one before it, **per provider
namespace**.

### 1. Runtime-wide, at setup

Applies to every run of every agent.

```ts
const runtime = await setupAgentCore({
  // …ports…
  config: {
    providerOptions: {
      openai: { serviceTier: 'flex' },
    },
  },
});
```

Use this for something that is true of your whole deployment — a service tier,
a safety identifier, a compliance flag.

### 2. Per agent, at registration

```ts
export const researcher = runtime.createStreamTextAgent({
  name: 'researcher',
  model: 'claude-sonnet-4',
  providerOptions: {
    anthropic: { thinking: { type: 'enabled', budgetTokens: 8_000 } },
  },
});
```

Use this for something true of that agent's job — a research agent that should
think hard, a classifier that should not.

### 3. Per run

```ts
await chat.run({
  prompt: 'take your time on this one',
  providerOptions: {
    anthropic: { thinking: { type: 'enabled', budgetTokens: 32_000 } },
  },
});
```

Use this for something true of the request — a user on a plan that buys more
reasoning, a retry that should try harder.

## How the levels merge

Merging is **shallow, at the namespace level**. A later level replaces a
namespace wholesale; it does not merge field by field inside it.

```ts
config:  { openai: { serviceTier: 'flex' },  anthropic: { thinking: { type: 'enabled' } } }
spec:    { openai: { serviceTier: 'priority' } }
run:     { openai: { serviceTier: 'auto' } }

// what the provider receives:
{ openai: { serviceTier: 'auto' },  anthropic: { thinking: { type: 'enabled' } } }
```

The `anthropic` namespace survives because no later level mentioned it. The
`openai` namespace was replaced twice.

The practical consequence: if a level sets `{ openai: { serviceTier, user } }`
and a later one sets `{ openai: { serviceTier } }`, **`user` is gone**. Restate
the whole namespace at the level you are overriding from.

## They survive the whole run

Provider options are carried on the dispatch ticket, so they apply to every step
of the run — not just the first — and they are restored when a run resumes after
an approval or a redrive. A run that started with a thinking budget keeps it
after a human says yes.

Nested runs inherit the parent's options.

## From a browser

The hook's `run()` merges anything beyond its known options into the request
body:

```ts
await run('think hard about this', {
  providerOptions: { anthropic: { thinking: { type: 'enabled', budgetTokens: 16_000 } } },
});
```

Your run route has to forward it:

```ts
const { threadId, prompt, model, providerOptions } = await req.json();
await chat.run({ threadId, prompt, model, providerOptions });
```

> Think before you forward this from an untrusted client. Provider options can
> raise cost — a reasoning budget most obviously. Either validate the shape
> against an allowlist, or set the option server-side from the user's plan
> rather than from their request.

## What not to put here

Anything the platform owns: `model`, `messages`, `tools`, `maxSteps`,
`abortSignal`. Those are set by the engine and cannot be overridden.

Cache breakpoints are also handled for you — see
[Context and tokens](./context-and-tokens.md#prompt-caching). Setting
`cacheControl` by hand here is not the supported path.

## Compatibility note

The engine forwards the options under both the SDK v5 name (`providerOptions`)
and the v4 alias (`experimental_providerMetadata`), so the same configuration
works across that version boundary.
