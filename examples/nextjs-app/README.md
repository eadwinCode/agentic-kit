# agentrun — Next.js example (spec §5)

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

For local queue testing without QStash cloud, the [`@upstash/qstash` dev CLI](https://docs.upstash.com/qstash/how-tos/local-development) replays signed requests to `localhost`.
