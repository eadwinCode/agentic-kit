# agentenkit — Next.js example (spec §5)

Thin reference integration: every route handler is a few lines over the runtime; all behavior lives in the package. Runs on [Bun](https://bun.sh).

- `POST /api/agent/run` — persist + enqueue (`202`), heals orphaned HITL waits first
- `POST /api/agent/control` — the one-stop write (`state → CANCELLED`)
- `POST /api/agent/respond` — HITL approval / denial delivery
- `GET  /api/agent/stream` — SSE replay + live tail; doubles as the §2.5 orphan watchdog
- `POST /api/queue/agent-run` — QStash-signed worker (`executeWithPolicy`)

The UI (`app/page.tsx` + `hooks/useAgentThread.ts`) demonstrates streaming bubbles, tool/subagent activity, the stop button, and the HITL approval banner. Open two tabs on the same thread — they stay in sync via the §2.2 event log.

## Setup

```bash
bun install
cp .env.example .env        # fill in the values
bunx prisma migrate dev --name init
bun dev
```

**Web tools.** Set `BRAVE_API_KEY` (or `JINA_API_KEY`) in `.env` and the agent
and its subagents can search the web (`web_search`) and read pages
(`web_fetch`). Without a key the agent can still read pages but not search.
The keys are passed to the adapters in `lib/runtime.ts`. Try: *"What is the
latest version of the MCP spec? Cite the page."* See
[Web tools](../../docs/web-tools.md).

**Sandbox tools.** Set `SANDBOX=docker` (needs a Docker daemon),
`SANDBOX=e2b` (with `E2B_API_KEY`) or `SANDBOX=local` and the agent and its
subagents get `bash`, `code_execution` and `text_editor`, in one sandbox per
thread that is kept between messages. `local` runs the model's commands on
your machine with no isolation: use it only to try things out. `bash` and
file changes wait for your approval, and a command's output shows under it as
it runs. Try: *"Plot y = x² for x from -5 to 5 and save it as chart.png."* See
[Sandboxes](../../docs/sandboxes.md).

**Admin view.** `/admin` lists threads and runs. Open a thread to see its
steps, what each tool call was sent and returned, and its spend: a line per
agent and model, and per tool, so a search or a sandbox command sits next to
the model calls that asked for it.

For local queue testing without QStash cloud, the [`@upstash/qstash` dev CLI](https://docs.upstash.com/qstash/how-tos/local-development) replays signed requests to `localhost`.
