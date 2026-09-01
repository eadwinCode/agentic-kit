# agentic-kit

A durable runtime for AI agent runs.

Not an agent framework — it does not own your prompts, models, or tools; the
[AI SDK](https://sdk.vercel.ai) does. It owns the **lifecycle of a run**: that a run
outlives the request that started it, survives a worker dying mid-step, can be stopped,
parked for a human, resumed exactly where it stopped, nested, metered, and watched by
several people at once.

Everything vendor-specific lives behind four small ports — storage, queue, event bus,
key-value — so the engine never imports a database driver.

```
packages/
  agentic-kit/        @agentic-kit/core — the library
    src/core/         the engine: loop, HITL, subagents, compaction, run identity
    src/ports/        the four interfaces you implement
    src/adapters/     reference adapters (Prisma, Redis, QStash, Upstash, memory)
    test/
examples/
  nextjs-app/         a full integration — an example, not the product
```

- [`packages/agentic-kit/README.md`](./packages/agentic-kit/README.md) — install and usage
- [`agent-platform-technical-spec.md`](./agent-platform-technical-spec.md) — the behavioral
  source of truth; the `§` references throughout the code point here
- [`agent-runtime-abstraction.md`](./agent-runtime-abstraction.md) — the ports rationale

## Development

```bash
bun install
bun test         # the package's suite
bun run typecheck
```

The example app expects Postgres, Redis and a local QStash; see
[`examples/nextjs-app/README.md`](./examples/nextjs-app/README.md).
