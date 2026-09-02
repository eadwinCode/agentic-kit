# go-app: a complete agent service in Go

A Go server on [`go-agentenkit`](../../packages/go-agentenkit) serving a React
SPA that uses [`use-agentenkit`](../../packages/use-agentenkit). Everything the
platform does is on screen: streaming, tools, approvals, questions answered by
the user, subagents, token usage, thread history, and custom events published
from tools and from outside a run.

It runs with nothing to stand up. Without `OPENAI_API_KEY` it uses a built-in
mock model that answers from keywords, so every path can be tried offline.

## Run it

```bash
# 1. build the SPA (once, from the repo root so the workspace links resolve)
bun install
bun run --cwd examples/go-app/web build

# 2. start the server
cd examples/go-app
go run .                       # http://localhost:8080, mock model
OPENAI_API_KEY=sk-… go run .   # gpt-4o-mini (MODEL=… to change)
```

For frontend work, run Vite with a proxy to the Go server instead of rebuilding:

```bash
go run .                                   # terminal 1
bun run --cwd examples/go-app/web dev      # terminal 2, http://localhost:5173
```

Flags and env: `-addr` / `ADDR`, `-db` / `DB_FILE` (SQLite file, default
`go-app.sqlite`), `-static` / `STATIC_DIR` (built SPA, default `web/dist`).

## Try these

| Prompt | What it shows |
| :--- | :--- |
| `weather in Paris and Rome` | two tool calls in one step, progress notices |
| `render a logo for a coffee brand` | progress notices, then a durable `DESIGN_PREVIEW` event with an image |
| `ask me questions about my design` | the run parks, a form appears, the answers resume the tool and it renders |
| `send an email to the client` | approval card; approve or deny, or let it expire |
| `show my orders` | a tool reading the run state (`orgId`) the client attached |
| `research goroutines` | a subagent with its own stream and card |
| `what is (12*7)+3` | a plain tool |
| `think about what 6*7 is` | a reasoning stream first: shown live as a "Thinking" block, folded to one line once the answer starts, restored from the durable message on reload |
| **Simulate credit limit** button | sets the thread's allowance to zero, like a billing webhook. The composer stays open: the next message meets `BillingPreCheck`, which publishes `CREDIT_LIMIT` and refuses, and the chat shows *credit limit reached. resets … - clear it to continue*. **Clear limit** restores the allowance with `CREDIT_RESTORED` |
| a long multi-tool prompt with a small `tokenBudget` (send `{"tokenBudget": 900}` on the run body) | the platform publishes `TOKEN_BUDGET_EXHAUSTED` between steps and finalizes with `stopReason: token_budget`; the chat shows it |

Open the same thread in a second tab: it stays in sync. Reload mid-run: the
snapshot plus the replayed events rebuild the view, preview included.

## Where things are

| File | |
| :--- | :--- |
| [`main.go`](./main.go) | assembles the runtime: SQLite storage, admin and kv in one file, memory bus, inline queue |
| [`agent.go`](./agent.go) | the tools and the agent; every tool gets a `ToolContext` with `State`, `PublishEvent`, `Approval` |
| [`mock_model.go`](./mock_model.go) | the keyword-driven stand-in for a provider |
| [`handlers.go`](./handlers.go) | the HTTP contract the hook expects, admin reads, SSE, the SPA |
| [`web/src/App.tsx`](./web/src/App.tsx) | the SPA: the hook plus an `onEvent` reducer for the custom events |
| [`web/src/events.ts`](./web/src/events.ts) | that reducer |

## Custom events in this app

| Event | Durable | Published by |
| :--- | :--- | :--- |
| `PROGRESS` | no (notice) | `getWeather`, `renderDesign` |
| `DESIGN_PREVIEW` | yes | `renderDesign` |
| `QUESTIONS_ANSWERED` | yes | `askDesignQuestions`, after the approval |
| `EMAIL_SENT` | yes | `sendEmail`, after the approval |
| `CREDIT_LIMIT` | yes | `creditCheck`, the `BillingPreCheck`, as it refuses a run (with the reset date) |
| `CREDIT_RESTORED` | yes | `DELETE /api/demo/credit-limit?threadId=…` |
| `RUN_REFUSED` | yes | the platform, whenever the pre-check says no |
| `TOKEN_BUDGET_EXHAUSTED` | yes | the platform, when a run spends its `tokenBudget` (the handler passes the thread's remaining allowance) |

See the [Custom events](../../docs/custom-events.md) guide.
