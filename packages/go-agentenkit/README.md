# go-agentenkit

The Go version of [`agentenkit`](../agentenkit): a durable runtime for AI agent runs, as a
**library**. Where the TypeScript package builds on the Vercel AI SDK, this one builds on
[goai](https://github.com/zendev-sh/goai), the Go SDK with the same shape.

It does not own your prompts, models, or tools; goai does. It owns the **lifecycle of a
run**: that a run outlives the request that started it, survives a worker dying mid-step,
can be stopped, parked for a human, resumed exactly where it stopped, nested, metered, and
watched by several people at once.

The behaviour is the TypeScript package's, port by port and file by file. The
[full documentation](https://eadwincode.github.io/agentic-kit/) describes it; this README
shows the Go shape of each piece.

`BillingPreCheck` receives a `BillingCheck` with the thread, the run state
and `PublishEvent`, so a refusal can carry its reason to every client; the
platform publishes `RUN_REFUSED` as well, and `TOKEN_BUDGET_EXHAUSTED` or
`COST_BUDGET_EXHAUSTED` when a run spends its budget between steps.

A complete, runnable example, a Go server serving a React SPA with tools,
approvals, subagents and custom events, lives in
[`examples/go-app`](../../examples/go-app).

## Install

```bash
go get github.com/eadwinCode/agentic-kit/packages/go-agentenkit
```

Requires Go 1.25+. Adapters live in their own packages, so a program only compiles the
drivers it imports.

## Running locally, with nothing to stand up

Assembling the platform is four adapters and one wire, and seeing them is the point:
swapping any of them for the durable equivalent later is then obvious rather than magic.

```go
package main

import (
	"context"
	"log"

	"github.com/zendev-sh/goai/provider"
	"github.com/zendev-sh/goai/provider/openai"
	_ "modernc.org/sqlite" // any database/sql SQLite driver

	agentenkit "github.com/eadwinCode/agentic-kit/packages/go-agentenkit"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/adapters/inline"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/adapters/memory"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/adapters/sqlite"
	sqliteadmin "github.com/eadwinCode/agentic-kit/packages/go-agentenkit/admin/sqlite"
)

func main() {
	ctx := context.Background()

	// One file holds both: your tables, and, prefixed agentic_, the platform's
	// own operational history.
	db, err := sqlite.Open("agentic-kit.sqlite")
	if err != nil {
		log.Fatal(err)
	}
	storage, _ := sqlite.New(db)
	admin, _ := sqliteadmin.New(db)
	kv, _ := sqlite.NewKv(db) // the seq counters live here: keep them as durable as the log
	queue := inline.New(ctx)

	rt, err := agentenkit.SetupAgentCore(ctx, agentenkit.RuntimeOptions{
		Storage: storage,           // later: adapters/postgres, or your own
		Admin:   admin,             // later: admin/postgres
		Bus:     memory.NewBus(),   // later: adapters/redis
		Kv:      kv,                // later: adapters/redis
		Queue:   queue,             // later: adapters/qstash
		ResolveModel: func(name string) (agentenkit.ResolvedModel, error) {
			return agentenkit.ResolvedModel{
				Instance:      func() provider.LanguageModel { return openai.Chat(name) },
				ContextWindow: 128_000,
			}, nil
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer rt.Close()

	// The queue and the worker each need the other, so the queue is attached
	// once the core exists. Nothing dispatches until this line runs.
	queue.Bind(rt.Worker.Handler())

	chat := rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{Name: "chat", Model: "gpt-4o"})
	res, err := chat.Run(ctx, agentenkit.RunInput{Prompt: "hi"}) // persisted, dispatched, running
	log.Println(res, err)
}
```

The inline queue dispatches on its own goroutine, so `Run` returns at once and a run still
outlives the request that started it. It honours delays, so parked approvals expire and
blocked jobs redrive exactly as they do against a durable queue. What it does not do is
survive a restart.

`sqlite.Open` uses whichever `database/sql` SQLite driver the process has registered
(`modernc.org/sqlite` or `mattn/go-sqlite3`). Pass `":memory:"` for a database that dies
with the process.

## Going to production

Same shape, durable pieces. Each line above swaps for one here.

```go
rdb := goredis.NewClient(&goredis.Options{Addr: os.Getenv("REDIS_ADDR")})
pg, _ := sql.Open("pgx", os.Getenv("DATABASE_URL"))
storage, _ := postgres.New(ctx, pg)

rt, err := agentenkit.SetupAgentCore(ctx, agentenkit.RuntimeOptions{
	Storage: storage,                       // or Mongo / Dynamo / your DB
	Bus:     redis.NewBus(rdb, 0),          // Ably / Kafka / Postgres LISTEN…
	Kv:      redis.NewKv(rdb),
	Queue: qstash.New(qstash.Client{Token: os.Getenv("QSTASH_TOKEN")},
		qstash.Options{URL: "https://app.example.com/api/queue/agent-run"}),
	// Admin omitted on purpose: with AGENTIC_KIT_ADMIN_DATABASE_URL set, the
	// platform opens Postgres for its own history. Pass one to override.
	ResolveModel: resolve,
})
```

Or one Postgres for all four, no Redis and no queue service:

```go
pg, _ := sql.Open("pgx", url)
storage, _ := postgres.New(ctx, pg)
kv, _ := postgres.NewKv(ctx, pg)
queue, _ := postgres.NewQueue(ctx, pg, postgres.QueueOptions{})
bus := postgres.NewBus(pg, pgxlisten.New(url), storage.Events(), kv, postgres.BusOptions{})
rt, err := agentenkit.SetupAgentCore(ctx, agentenkit.RuntimeOptions{Storage: storage, Kv: kv, Bus: bus, Queue: queue, ResolveModel: resolve})
queue.Bind(rt.Worker.Handler())
```

The bus rides LISTEN/NOTIFY and routes an event past the 8000-byte cap by
reference; the queue claims with `SKIP LOCKED` and renews its lease while a job
runs. Both are in `adapters/postgres`; the listener is `adapters/postgres/pgxlisten`.

## Operations

Runs belong to an agent handle; reads belong to the runtime.

```go
chat.Run(ctx, agentenkit.RunInput{Prompt: "hi", State: agentenkit.AgentRunState{"orgId": orgID}})
chat.Stop(ctx, threadID, nil)                                              // one write: state → CANCELLED
rt.HITL.Respond(ctx, agentenkit.RespondInput{ThreadID: threadID, ToolCallID: id, Approved: true})
rt.GetThreadSnapshot(ctx, threadID, nil)                                   // hydrate a client
stream, _ := rt.Events.SSE(r.Context(), threadID, agentenkit.SSEStateOptions{}) // then stream.ServeHTTP(w, r)
events, _ := rt.Events.Follow(ctx, threadID, agentenkit.FollowStateOptions{})   // or range events.Events()
rt.Events.Since(ctx, threadID, lastSeq, nil)                               // raw replay
rt.Worker.HandleJob(ctx, job)                                              // queue consumer
rt.Events.PublishEvent(ctx, threadID, "MY_EVENT", payload, agentenkit.PublishStateOptions{}) // your own event
```

An SSE route is a few lines:

```go
http.HandleFunc("/api/agent/events", func(w http.ResponseWriter, r *http.Request) {
	stream, err := rt.Events.SSE(r.Context(), r.URL.Query().Get("threadId"), agentenkit.SSEStateOptions{
		SSEOptions: agentenkit.SSEOptions{FollowOptions: agentenkit.FollowOptions{Since: lastEventID(r)}},
	})
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	stream.ServeHTTP(w, r) // returns when the client hangs up, and unsubscribes
})
```

The `CHUNK` and `SUBAGENT_CHUNK` payloads use the same part shapes the TypeScript package
publishes, so the [`use-agentenkit`](../use-agentenkit) React hook works against either
runtime.

## Tools and approvals

Tools are goai tools. Wrap them, or build them with the run state in hand:

```go
lookup := agentenkit.AgentTool("lookupInvoice", "Find one invoice",
	func(ctx context.Context, in struct{ InvoiceID string `json:"invoiceId"` }, tc agentenkit.ToolContext) (string, error) {
		// tc.State is the run state; tc.PublishEvent publishes on this thread.
		_, _ = tc.PublishEvent(ctx, "LOOKUP", map[string]any{"id": in.InvoiceID}, agentenkit.PublishOptions{Notice: true})
		return db.FindInvoice(ctx, in.InvoiceID, tc.State["orgId"].(string))
	})

// Parked behind a human approval instead of executing.
wipe := agentenkit.RequireConfirmation(goai.NewTool("wipe", "Wipe an account", wipeFn))

chat := rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{
	Name:   "chat",
	Model:  "gpt-4o",
	System: "You are terse.",
	Tools:  []agentenkit.Tool{lookup, wipe},
	Options: []goai.Option{goai.WithTemperature(0.2)}, // any goai option; the platform's own win
	Subagents: &agentenkit.SubagentsConfig{},         // opt in to delegation
})
```

The platform owns the model, the messages, the step ceiling and the stop handling. Every
other goai option is yours, passed through `Options`.

## The spec's hooks

```go
rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{
	Name: "designer",
	// Built per step with the run's state; wins over System.
	SystemFn: func(ctx context.Context, threadID string, state agentenkit.AgentRunState) (string, error) {
		return persona + projectBrief(ctx, state), nil
	},
	// Runs after the last step and BEFORE the terminal state is written: commit
	// what the run produced, bill it. An error fails the run; a stop arrives
	// with Cancelled set on a cancelled ctx. Idempotent on RunID, please.
	OnSettle: func(ctx context.Context, info agentenkit.RunFinishInfo) error {
		return repo.Commit(context.WithoutCancel(ctx), info.RunID)
	},
	OnFinish: func(info agentenkit.RunFinishInfo) { log.Println("done", info.RunID, info.State) },
	Subagents: &agentenkit.SubagentsConfig{Profiles: map[string]agentenkit.SubagentProfile{
		"page-manager": {Description: "edits pages", SystemFn: pagePrompt, Tools: pageTools, MaxSteps: 20},
	}},
})

// A run can name itself, cap its own steps, and carry images.
chat.Run(ctx, agentenkit.RunInput{
	Prompt: "make the hero bolder", RunID: myRunID, MaxSteps: 8,
	Attachments: []agentenkit.Attachment{{URL: "https://cdn.example/ref.png", MediaType: "image/png"}},
})
```

## Run state

Whatever you attach to a run reaches **every** storage call it makes, every tool, and every
nested run, including in a worker that picks the job up hours later, in another process,
after an approval. The platform never reads it.

```go
chat.Run(ctx, agentenkit.RunInput{Prompt: "hi", State: agentenkit.AgentRunState{"orgId": "acme"}})

// Your Storage sees it on every method, as a trailing argument.
func (m messages) List(ctx context.Context, threadID string, scope *ports.MessageScope, sc ports.StorageContext) ([]ports.MessageDTO, error) {
	org := sc.State["orgId"]
	// ...
}

// A tool reads it from its context, or takes it as an argument via AgentTool.
state := agentenkit.RunStateFromContext(ctx)
tc := agentenkit.ToolContextFrom(ctx) // state, tool call id, and PublishEvent
```

## Ports: your data

| Port | Role | Reference adapters |
| :--- | :--- | :--- |
| `Storage` | threads / messages / events / usage, incl. atomic `ClaimState` | `adapters/postgres`, `adapters/sqlite`, `adapters/memory` |
| `EventBus` | live fan-out + HITL death notices (at-most-once) | `adapters/redis`, `adapters/upstash`, `adapters/memory` |
| `Queue` | durable run dispatch (at-least-once) | `adapters/qstash`, `adapters/inline` (dev), `adapters/memory` |
| `Kv` | hot state cache, HITL handoff keys, seq/attempt counters, run locks | `adapters/redis`, `adapters/upstash`, `adapters/sqlite`, `adapters/memory` |

Implement any of them for your own stack; `core/` imports nothing else. The
[memory adapters](./adapters/memory/memory.go) are a complete implementation used by the
test suite and double as a template.

## Cost

Give the runtime a `Pricer` and every usage row carries the money as well as the
tokens, so spend is read from the store the engine already fills:

```go
import "github.com/eadwinCode/agentic-kit/packages/go-agentenkit/pricing"

rt, err := agentenkit.SetupAgentCore(ctx, agentenkit.RuntimeOptions{
    // ... ports ...
    Pricer: pricing.Table{
        // dollars per MILLION tokens, typed off the provider's pricing page
        "gpt-4o": {InputPerMillion: 2.5, CacheReadPerMillion: 1.25, OutputPerMillion: 10},
    },
})
```

One row per model call: main run, nested run or compaction, streamed or not,
finished or cut short. Reading it back:

```go
rt.GetThreadUsage(ctx, threadID, nil)                        // the thread header
storage.Usage().Total(ctx, threadID, ports.UsageFilter{RunID: runID}, sc) // one run's bill
```

Every total carries `CostMicros`, `Currency`, `Unpriced` and `Lines` — the same
spend grouped by agent and model, which is what a credit system charges for. A
spec's `OnFinish` receives it ready-made as `info.Usage`, and
`RunInput.CostBudgetMicros` caps a run by money the way `TokenBudget` caps it by
tokens.

`pricing` also ships `Receipt`, for the figure a gateway already computed, and
`Chain`, to try one then the other. See
[Cost and pricing](https://eadwincode.github.io/agentic-kit/cost-and-pricing).

## The admin store: not yours

Run records, step timings and a thread index are the **platform's** data, in the platform's
own tables. You do not implement `AdminStore`; you read it back:

```go
rt.Admin.Overview(ctx, nil)                     // threads and runs by state, plus what's in flight
rt.Admin.ListRuns(ctx, agentenkit.RunFilter{State: []agentenkit.ExecutionState{agentenkit.StateFailed}})
rt.Admin.Stats(ctx, agentenkit.StatsRange{})   // p50/p95 duration and queue wait, tokens, failures
rt.Admin.GetRun(ctx, runID)                     // one run: steps, nested runs, timeline, spend
```

Its schema migrates itself, from numbered `.sql` files embedded in the binary
(`admin/migrate/sql`). `SetupAgentCore` opens the connection and returns; the
schema is brought up to date on a goroutine behind it, and every admin call
waits for that before its first query. Several workers starting at once queue on
a Postgres advisory lock rather than racing. An existing database needs nothing:
migration `0001` is the schema as it stood before the migrator. Your own
`Storage` is never touched by any of this.

Configure nothing and it is SQLite on disk (`AGENTIC_KIT_ADMIN_DB` moves the file). Set
`AGENTIC_KIT_ADMIN_DATABASE_URL` and it is Postgres. Either way the platform uses whichever
`database/sql` driver the process has registered, like the TypeScript package uses whichever
SQLite driver the runtime has. Import one:

```go
import _ "modernc.org/sqlite"             // or github.com/mattn/go-sqlite3
import _ "github.com/jackc/pgx/v5/stdlib" // or github.com/lib/pq
```

## File map

Every TypeScript file has a Go twin. The one structural change: Go cannot have an import
cycle between `core` and `ports`, so the shared types (`core/types.ts`, the type half of
`core/state.ts`) live in `ports/`.

| TypeScript | Go |
| :--- | :--- |
| `src/index.ts` | `agentenkit.go` (re-exports) |
| `src/runtime.ts` | `runtime.go` (`SetupAgentCore`, `AgentCore`) |
| `src/ports/*.ts`, `src/core/types.ts`, `src/core/state.ts` (types) | `ports/*.go` |
| `src/core/*.ts` | `core/*.go` (`core/messages.go` is new: JSON ⇄ goai messages) |
| `src/adapters/memory.ts`, `inline.ts`, `sqlite.ts`, `redis.ts`, `qstash.ts`, `upstash.ts` | `adapters/<name>/<name>.go` |
| `src/adapters/prisma.ts` | `adapters/postgres/postgres.go` |
| `src/admin/memory.ts`, `sqlite.ts`, `postgres.ts`, `default.ts` | `admin/<name>/<name>.go`, `admin/default.go` |
| `test/*.test.ts` | `*_test.go` |

The stored message and event JSON is the same as the TypeScript package's, and the SQLite
and Postgres admin schemas are identical, so a Go worker and a TypeScript worker can share
one database.

## Differences worth knowing

- **Durations, not milliseconds.** `AgentConfig` uses `time.Duration` (`HITLTTL`,
  `StopPoll`, `RunLockLease`, …). `TokenBudget` zero means unbounded.
- **Results and errors are separate.** A refusal (`Accepted: false`) comes back in the
  result; an infrastructure failure comes back as an `error`.
- **Config is a value.** `cfg := agentenkit.DefaultConfig()`, change what you need, pass
  `&cfg`. `CompactionModel` names the cheap model used for context summaries.
- **A mixed step keeps its results.** When one tool parks and another runs in the same
  step, the executed result is persisted. The TypeScript package drops the whole message.
- **Total tokens** are always input + cached + output, for every provider.

## Development

```bash
go test ./...                               # memory + SQLite; Postgres tests skip
TEST_ADMIN_PG=postgres://... go test ./...  # also runs the Postgres store tests
```
