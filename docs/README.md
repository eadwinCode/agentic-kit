# agentic-kit documentation

> Reading on GitHub. The same pages are published with search at
> **[https://eadwincode.github.io/agentic-kit/](https://eadwincode.github.io/agentic-kit/)**.

A durable runtime for AI agent runs, and the React hook that talks to it.

Two packages:

| Package | What it is |
| :--- | :--- |
| [`agentrun`](https://github.com/eadwinCode/agentic-kit/tree/main/packages/agentrun) | The server-side runtime. Owns the lifecycle of a run. |
| [`use-agentrun`](https://github.com/eadwinCode/agentic-kit/tree/main/packages/use-agentrun) | The React hook. Owns the client state machine. |

You can use the core on its own — the hook is one way to build a UI over it, not
a requirement.

## Start here

1. **[Getting started](./getting-started.md)** — install, a runtime you can run
   with nothing to stand up, and your first run.
2. **[Core concepts](./concepts.md)** — threads, runs, the loop, and why a run
   outlives the request that started it. Read this before the rest.
3. **[HTTP API](./http-api.md)** — the endpoints you expose. This is the
   contract between the two packages.

## Setting it up

- **[setupAgentCore](./setup.md)** — every option, every adapter choice, and
  how to assemble a runtime for development, production and tests.

## Building with it

- **[Agents and tools](./agents-and-tools.md)** — registering agents, models,
  tools, and per-run budgets.
- **[Human in the loop](./human-in-the-loop.md)** — pausing a run for approval
  and resuming it exactly where it stopped.
- **[Subagents](./subagents.md)** — delegation as nested runs.
- **[Context and tokens](./context-and-tokens.md)** — compaction, prompt
  caching, and how spend is attributed.
- **[Provider options](./provider-options.md)** — passing provider-specific
  settings at setup, per agent, or per run.
- **[Run state](./run-state.md)** — carrying tenant or user context into every
  storage call, tool and nested run.
- **[Multi-tenancy](./multi-tenancy.md)** — a worked isolation story, end to
  end: schema, storage, edges, tools, tests.

## Wiring it to your stack

- **[Ports and adapters](./ports-and-adapters.md)** — the four interfaces you
  implement, their invariants, and the reference adapters.
- **[Observability](./observability.md)** — the operational store the platform
  keeps, and the reads you build a dashboard from.

## Client

- **[React: use-agentrun](./react.md)** — the hook, its routes, and its options.

## Reference

- **[Configuration](./configuration.md)** — every setting in both packages, with
  defaults, plus environment variables.
- **[Production](./production.md)** — deployment shapes, scaling, security, and
  a checklist.
- **[Troubleshooting](./troubleshooting.md)** — symptoms, causes, fixes.

## Deeper background

The source carries `§` references — §2.5 for the human-in-the-loop rules, §2.7
for nested runs, and so on. They point at a behavioural specification that is
no longer kept in the repository; these pages are now the written description
of that behaviour, and the test suite is its executable one.

A full integration lives in
[`examples/nextjs-app`](https://github.com/eadwinCode/agentic-kit/tree/main/examples/nextjs-app).
It is an example, not the product.
