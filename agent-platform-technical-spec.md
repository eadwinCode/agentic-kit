# Technical Specification: Multi-Tenant Real-Time AI Agent Platform

## 1. Executive Summary & Architecture Overview

This specification outlines the architecture for a real-time, multi-model AI Agent platform built using the **Vercel AI SDK (v3.4+)**, **Next.js (App Router)**, **Vercel Data Cache**, and **Server-Sent Events (SSE)**.

The core requirements—an immediate stop button, disconnect-safe multi-user synchronous viewing, human-in-the-loop (HITL) confirmation, subagent delegation, bounded context windows with automatic compaction (265k ceiling), LLM model abstraction, thread persistence, and granular token-based billing—are resolved using an event-driven control plane coupled with queue-dispatched background execution and standard stream handlers.

The platform ships as a **headless TypeScript library** (working name `@agent/core`): the engine depends only on a small set of **ports** — storage, event bus, queue, key-value (§3) — with reference adapters for PostgreSQL/Prisma, Upstash Redis, and QStash. Users with different databases or team architectures implement the same ports against their own stack. The Next.js routes in §5 are an example integration, not the product.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                            CLIENT LAYER (Next.js)                           │
│                                                                             │
│   ┌─────────────────────┐   ┌──────────────────────┐   ┌────────────────┐   │
│   │ Execution Controls  │   │ Synchronized Stream  │   │ Billing Meter  │   │
│   │ (Stop / HITL Reply) │   │ (SSE Hook)           │   │ (UI/Usage)     │   │
│   └─────────────────────┘   └──────────────────────┘   └────────────────┘   │
└──────────────┼──────────────────────────┼───────────────────────┼───────────┘
│              │   HTTP POST              │   EventStream         │   Sync    │
│              ▼                          ▲                       ▲           │
┌─────────────────────────────────────────────────────────────────────────────┐
│                      API LAYER (Next.js Route Handlers)                     │
│                                                                             │
│   ┌─────────────────────┐   ┌──────────────────────┐   ┌────────────────┐   │
│   │ Run API             │   │ Event Distributor    │   │ Stop + Respond │   │
│   │ (enqueue, §2.8)     │   │ (/api/agent/stream)  │   │ (§5.2 / §5.4)  │   │
│   │                     │   │ + HITL watchdog §2.5 │   │                │   │
│   └─────────────────────┘   └──────────────────────┘   └────────────────┘   │
└──────────────┼──────────────────────────────────────────────────────────────┘
│              │   Enqueue                │                       │           │
│              ▼                                                              │
┌─────────────────────────────────────────────────────────────────────────────┐
│                             MESSAGE QUEUE (§2.8)                            │
│                                                                             │
│   ┌─────────────────────────────────────────────────────────────────────┐   │
│   │   `agent-runs` (QStash)  ──►  /api/queue/agent-run worker  (§5.6)   │   │
│   └─────────────────────────────────────────────────────────────────────┘   │
└──────────────┼──────────────────────────────────────────────────────────────┘
│              │   Run                    │                       │           │
│              ▼                                                              │
┌─────────────────────────────────────────────────────────────────────────────┐
│                           STATE & EXECUTION LAYER                           │
│                                                                             │
│   ┌─────────────────────────────────────────────────────────────────────┐   │
│   │ Upstash Redis                                                       │   │
│   │   - State: `agent:state:{threadId}`  (RUNNING/CANCELLED/...)        │   │
│   │   - Run identity: `agent:run:{threadId}`  (current run; retires older) │   │
│   │   - HITL Handoff: `agent:hitl:{toolCallId}`  (answer handoff, TTL-guarded)│   │
│   │   - Pub/Sub: `thread:{threadId}:events`  (fan-out + death notices)  │   │
│   └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│   ┌───────────────────────────────┐   ┌─────────────────────────────────┐   │
│   │ Engine (`ai` SDK)             │   │ PostgreSQL / Prisma             │   │
│   │ - streamText + tools          │   │ - AgentEvent log (replay)       │   │
│   │ - Subagents in-proc (§2.7)    │   │ - Message · TokenUsage          │   │
│   │                               │   │   · SubagentRun                 │   │
│   └───────────────────────────────┘   └─────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Core System Modules

### 2.1 Detached Execution & Stop

Execution is fully detached from client connections. The HTTP request that starts a run does not carry the stream: `/api/agent/run` persists the user message, starts the run in a background context, and returns `202 Accepted` immediately.

- **Queue-dispatched:** every run is a durable job on the `agent-runs` message queue (§2.8). The HTTP request returns `202 Accepted` the moment the job is enqueued; a signature-verified worker consumes it and executes the engine. Dispatch survives deploys, transient failures are retried with backoff, and exhausted attempts finalize the thread `FAILED`.

All output follows one canonical path regardless of who is connected. Every chunk is (1) appended to the durable `AgentEvent` log in PostgreSQL and (2) published to the Redis Pub/Sub channel `thread:{threadId}:events`. No client holds a private stream — even the initiating client consumes the run through the §2.2 SSE endpoint. Because execution holds no reference to any connection, **every client can disconnect without affecting the run**; on reconnect, clients replay the missed events from the event log (§2.2).

#### Stop & Termination

A run ends in one of three ways. Every path finalizes state in `onFinish`, persisting partial output and token usage:

| Signal | Trigger | In-flight work | Final state | `stopReason` |
| :--- | :--- | :--- | :--- | :--- |
| **Stop** | `POST /api/agent/control` — a single write: `await redis.set(\`agent:state:${threadId}\`, 'CANCELLED')` | Aborted immediately via `abortSignal` — generation, tools, and subagents | `CANCELLED` | `cancelled` |
| **Task finished** | Model returns `finishReason: 'stop'` with no pending tool calls | — | `COMPLETED` | `completed` |
| **Safety cap** | `maxSteps` (25) or per-run token budget exceeded | — | `COMPLETED` | `max_steps` |

Stop is stop: one button, one behavior — everything stops immediately. The engine polls the state key (500 ms); the moment it reads `CANCELLED` it fires the abort signal, which tears down generation, tools, and subagents alike (§2.7). A run parked on a HITL wait (§2.5) aborts the same way instead of waiting out its TTL.

### 2.2 Synchronous Multi-User Real-Time Stream

To ensure all users watching a thread see the exact same output at the same time, and that disconnecting never interrupts or loses anything:

1. Every generated event is appended to the durable **`AgentEvent` log** in PostgreSQL with a per-thread monotonic `seq`, **and** published to the **Upstash Redis Pub/Sub channel** (`thread:{threadId}:events`) for live fan-out.
2. Client connections subscribe to the SSE endpoint (`/api/agent/stream?threadId=XYZ`). Each SSE message carries `id: seq`, and `EventSource` reconnects automatically.
3. On (re)connect, the route first replays from PostgreSQL every event after the client's `Last-Event-ID` (or `?since=` cursor), then tails Redis Pub/Sub for live events. A reconnecting client resumes exactly where it left off, and a client that never comes back has zero effect on the run.

While subscribed, the distributor also heals HITL orphans once on connect (§2.5) — a fallback beside the park's own scheduled expiry, not a poll.

### 2.3 Model Abstraction Layer

Using Vercel AI SDK (`ai`), models are instantiated using a central registry map.

```typescript
// lib/ai/models.ts
import { createOpenAI } from '@ai-sdk/openai';
import { createAnthropic } from '@ai-sdk/anthropic';
import { createGoogleGenerativeAI } from '@ai-sdk/google';

export const modelRegistry = {
  'gpt-4o': createOpenAI({ apiKey: process.env.OPENAI_API_KEY })('gpt-4o'),
  'gpt-4o-mini': createOpenAI({ apiKey: process.env.OPENAI_API_KEY })('gpt-4o-mini'), // compaction & summarization (§2.6)
  'claude-3-5-sonnet': createAnthropic({ apiKey: process.env.ANTHROPIC_API_KEY })('claude-3-5-sonnet-20240620'),
  'gemini-1.5-pro': createGoogleGenerativeAI({ apiKey: process.env.GOOGLE_GENERATIVE_AI_API_KEY })('gemini-1.5-pro'),
};

export type SupportedModels = keyof typeof modelRegistry;
```

### 2.4 Thread History & Persistence Schema

The reference `PrismaStorage` adapter (§3) is backed by this schema — thread execution state, message histories, the durable event log behind multi-user sync and reconnect replay, and subagent runs. Users of other databases implement the §3.2 `Storage` port against their own schema instead:

```prisma
datasource db {
  provider = "postgresql"
  url      = env("DATABASE_URL")
}

generator client {
  provider = "prisma-client-js"
}

enum ExecutionState {
  IDLE
  RUNNING
  WAITING_FOR_INPUT
  CANCELLED
  COMPLETED
  FAILED
}

model Thread {
  id        String         @id @default(cuid())
  title     String         @default("New Thread")
  createdAt DateTime       @default(now())
  updatedAt DateTime       @updatedAt
  state     ExecutionState @default(IDLE)
  model     String         @default("gpt-4o")

  messages     Message[]
  usage        TokenUsage[]
  events       AgentEvent[]
  subagentRuns SubagentRun[]
}

model Message {
  id        String   @id @default(cuid())
  threadId  String
  agentId   String?  // producing agent; null = main agent (§2.7)
  role      String   // 'user' | 'assistant' | 'system' | 'tool'
  content   Json     // AI SDK message state, standard text, or CONTEXT_SUMMARY (§2.6)
  createdAt DateTime @default(now())

  thread    Thread   @relation(fields: [threadId], references: [id], onDelete: Cascade)
}

// Append-only event log: the replay source for SSE (re)connects and the
// durable record of INPUT_REQUIRED (HITL) requests.
model AgentEvent {
  id        String   @id @default(cuid())
  threadId  String
  seq       Int      // per-thread monotonic sequence (INCR agent:seq:{threadId})
  type      String   // 'CHUNK' | 'TOOL_CALL' | 'STATE_CHANGE' | 'INPUT_REQUIRED'
                     // | 'INPUT_EXPIRED' | 'HITL_ORPHANED' | 'CONTEXT_COMPACTED' | 'SUBAGENT_*' | ...
  payload   Json
  createdAt DateTime @default(now())

  thread    Thread   @relation(fields: [threadId], references: [id], onDelete: Cascade)

  @@unique([threadId, seq])
}

model SubagentRun {
  id              String         @id @default(cuid())
  threadId        String
  name            String
  model           String
  depth           Int            @default(1)
  state           ExecutionState @default(RUNNING)
  result          Json?
  createdAt       DateTime       @default(now())
  updatedAt       DateTime       @updatedAt

  thread          Thread         @relation(fields: [threadId], references: [id], onDelete: Cascade)
}

model TokenUsage {
  id               String   @id @default(cuid())
  threadId         String
  agentId          String?  // subagent attribution; null = main agent (§2.7)
  model            String
  promptTokens     Int
  completionTokens Int
  costUSD          Decimal  @db.Decimal(10, 6)
  createdAt        DateTime @default(now())

  thread           Thread   @relation(fields: [threadId], references: [id], onDelete: Cascade)
}
```

### 2.5 Human-in-the-Loop (HITL) Confirmation

Destructive tool calls (e.g., sending an email, executing code) require explicit user approval before they run. HITL is a first-class run suspension handled server-side — not a client-side concern:

1. **Declare:** tools that need approval are flagged in their definition (`requiresConfirmation: true`, optionally with an `inputSchema` for extra form fields).
2. **Park:** when the model emits such a tool call, the engine does not execute it. It publishes `INPUT_REQUIRED` (tool call id, tool name, arguments, input schema, and a **resume ticket**), flips the thread to `WAITING_FOR_INPUT` on both homes, and **ends the run segment right there** — no process stays blocked or alive waiting. The suspension is a durable state transition, not an in-process wait.
3. **Prompt:** all connected clients receive `INPUT_REQUIRED` over SSE and render an approval form.
4. **Resolve:** any authorized viewer POSTs `/api/agent/respond` with `{ threadId, toolCallId, approved, payload? }`. The route writes the answer to `agent:hitl:{toolCallId}` (with a remaining-TTL expiry) and **enqueues a resume job** — the same dispatch ticket persisted inside the `INPUT_REQUIRED` payload, so the identical worker path picks it up (§2.8). The resumed segment consumes the answer, executes the real tool on approval (or appends `{ denied: true }`), flips the thread back to `RUNNING`, and continues the loop with a fresh step.
5. **Timeout:** if the TTL elapses with no response, the pending request is resolved as `{ responded: false, cancelled: true, reason: 'timeout' }` — the user had no response, the action is cancelled — the thread flips back to `RUNNING`, and the run continues, letting the AI decide what to do next. Enforcement details in *Expiry Enforcement* below.

Example `INPUT_REQUIRED` event as delivered over SSE:

```json
{
  "type": "INPUT_REQUIRED",
  "seq": 42,
  "payload": {
    "toolCallId": "call_9f2a",
    "toolName": "sendEmail",
    "arguments": { "to": "team@example.com", "subject": "Q3 report" },
    "inputSchema": { "note": "string (optional)" },
    "resume": { "agent": "chat", "model": "gpt-4o", "tokenBudget": 12000 }
  }
}
```

The `resume` ticket is the key to durability: everything a worker needs to continue the parked run is persisted in the event log itself. `/respond` and TTL reclamation both rebuild the dispatch from it — there is no hidden in-memory state to lose.

#### The Park: a Run-Segment State Transition

Suspension does **not** keep an invocation alive. A `requiresConfirmation` tool's wrapper persists the request and returns a sentinel; the engine loop sees the sentinel in the step's tool results and ends the segment. The user's verdict is appended as the tool result by whichever segment resumes the run:

```typescript
// src/core/hitl.ts — ships in the package; `deps` are the §3.2 ports
import type { RuntimePorts } from '../ports/runtime';
import type { ResumeInfo } from './types';

export const HITL_TTL_MS = 15 * 60 * 1000;
export const hitlKey = (toolCallId: string) => `agent:hitl:${toolCallId}`;

// Marker returned by a parked tool's wrapper — the engine scans a step's
// tool results for it and ends the segment. Never persisted as a result.
export const HITL_PARKED = '__hitl_parked__';

// The §2.5 suspension as a durable state transition — NO process waits.
export async function parkForApproval(deps: RuntimePorts, i: {
  threadId: string; toolCallId: string; toolName: string; args: unknown;
  resume: ResumeInfo;                       // dispatch ticket persisted in the payload
}) {
  await deps.kv.set(`agent:state:${i.threadId}`, 'WAITING_FOR_INPUT');
  await deps.storage.threads.setState(i.threadId, 'WAITING_FOR_INPUT'); // durable truth (§3.4)
  await publish(deps, i.threadId, 'INPUT_REQUIRED', {
    toolCallId: i.toolCallId, toolName: i.toolName, agentId: i.agentId ?? null,
    arguments: i.args, inputSchema: null, resume: i.resume,
  });
  await publish(deps, i.threadId, 'STATE_CHANGE', { state: 'WAITING_FOR_INPUT' });
}
```

Resolution happens at the START of a resumed segment (`src/core/engine.ts`): the thread enters `execute` in `WAITING_FOR_INPUT`, the pending request is hydrated from the event log, and the outcome is appended as the tool result before the loop runs its next step:

```typescript
async function resumePendingHitl(deps: RuntimePorts, threadId: string, pending: PendingHitl,
                                 tools: Record<string, any>, signal: AbortSignal) {
  const raw = await deps.kv.get(hitlKey(pending.toolCallId));
  const expired = Date.now() - pending.requestedAt >= deps.config.hitlTtlMs;
  if (!raw && !expired) return false; // still parked — at-least-once redelivery is a no-op (§2.8)

  await deps.kv.del(hitlKey(pending.toolCallId));

  let result: unknown;
  if (raw) {
    const answer = JSON.parse(raw) as { approved: boolean; payload?: unknown };
    if (answer.approved) {
      try {
        // The verdict arrives from a different process than the one that ran
        // the model — a tool failure is surfaced TO THE MODEL as the tool
        // result, so the conversation always stays executable
        result = await tools[pending.toolName].execute(pending.arguments, {
          toolCallId: pending.toolCallId, abortSignal: signal,
        });
      } catch (err) {
        result = { error: err instanceof Error ? err.message : String(err) };
      }
    } else {
      result = { denied: true };
    }
  } else {
    result = { responded: false, cancelled: true, reason: 'timeout' }; // TTL expiry (§2.5)
  }

  await deps.storage.messages.append(threadId, {
    role: 'tool',
    content: [{ type: 'tool-result', toolCallId: pending.toolCallId, toolName: pending.toolName, result }],
  });
  await deps.kv.set(`agent:state:${threadId}`, 'RUNNING');
  await deps.storage.threads.setState(threadId, 'RUNNING');
  await publish(deps, threadId, 'STATE_CHANGE', { state: 'RUNNING' });
  if (!raw) await publish(deps, threadId, 'INPUT_EXPIRED', { toolCallId: pending.toolCallId });
  return true;
}
```

**Why a segment park instead of an in-process wait:** the execution environment's wall-clock limits stop mattering — there is no long-lived parked invocation to kill, no `maxDuration` to tune, and the run lock is released while parked (a parked thread holds no worker). The TTL becomes purely an *answer-validity* window instead of a timer a process must survive.

#### Expiry Enforcement

The TTL is enforced at **resolution time, not by a timer**: when a segment resumes a parked request, an unanswered request past `hitlTtlMs` becomes the timeout denial — `{ responded: false, cancelled: true, reason: 'timeout' }` — the user had no response, the action is cancelled, and the AI decides how to proceed. The answer handoff key carries a remaining-TTL expiry too, so a response can never outlive its request. No external timer, no cron.

**The park schedules its own expiry.** `parkForApproval` enqueues one delayed dispatch of the same run, timed for `hitlTtlMs + reclaimGraceMs`. When it lands, the ordinary resume path runs: `resumePendingHitl` finds no answer past the TTL, writes the timeout denial, and the loop continues. This is still not a timer holding a run — nothing is pinned, no process waits, no cron sweeps. The queue holds the deadline exactly as it holds the original dispatch (§2.8), so the TTL means the same thing whether or not anyone is watching.

The delayed job carries the **parked run's id**, never a fresh one. The answer dispatch from `/api/agent/respond` reuses that id too, so the two are deliveries of one run rather than rival runs: whichever reaches the run lock first resolves the park, and the other is a no-op (§2.1). Minting a new id on respond would let both run a segment, the second replying to a conversation that already ended.

Two fallbacks remain, for what a delayed dispatch cannot cover — threads parked before the timer existed, and queue adapters that deliver without honoring a delay:

1. **Heal on connect:** every SSE connection in `/api/agent/stream` (§2.2) runs `reclaimIfOrphaned(threadId)` once when it opens, which also makes an already-expired approval resolve live in front of the user. It no longer polls: the deadline is the queue's job, not the viewer's.
2. **Lazy checks (floor guarantee):** `/api/agent/respond` and the run route's active-run guard call the same `reclaimIfOrphaned(threadId)` before acting, so a thread heals on first touch. A late response after reclamation gets `409` — an expired approval has already become a denial and the run continued.

**Races are safe by construction:** the engine owns every state transition around the park; the respond route only delivers. Reclamation's conditional `UPDATE ... WHERE state = 'WAITING_FOR_INPUT'` claims each orphan exactly once, so a human response and any number of concurrent listeners can never double-append a tool result — the loser gets `409` or skips.

**Interplay with stop:** the user's stop flips the thread to `CANCELLED` instantly while a request is parked — nothing is running to abort. A late resume dispatch is a no-op (terminal-state guard, §2.8), `respond` on a cancelled thread is rejected, and cancelled threads never match reclamation's query.

Reference implementation — the reclamation helper:

```typescript
// src/core/reclaim.ts — ships in the package; invoked by listeners, never a scheduler
import type { RuntimePorts } from '../ports/runtime';

// Small grace so an in-flight /respond delivery always lands first —
// reclamation only ever sees true orphans. Concurrent callers are safe:
// threads.claimState is a compare-and-set (§3.4) — exactly one caller wins.
const graceAfterMs = (deps: RuntimePorts) => deps.config.hitlTtlMs + deps.config.reclaimGraceMs;

export async function reclaimIfOrphaned(deps: RuntimePorts, threadId: string): Promise<boolean> {
  const stale = await deps.storage.events.latest(threadId, 'INPUT_REQUIRED');
  const age = stale ? Date.now() - stale.createdAt.getTime() : 0;
  if (!stale || age < graceAfterMs(deps)) return false;

  // Atomic claim — exactly one caller wins
  const claimed = await deps.storage.threads.claimState(threadId, 'WAITING_FOR_INPUT', 'RUNNING');
  if (!claimed) return false;

  const { toolCallId, toolName, resume } = stale.payload as { toolCallId: string; toolName?: string; resume?: ResumeInfo };

  // The same tool result the resumed segment would have produced on timeout (§2.5)
  await deps.storage.messages.append(threadId, {
    role: 'tool',
    content: [{ type: 'tool-result', toolCallId, toolName: toolName ?? 'unknown',
                result: { responded: false, cancelled: true, reason: 'timeout' } }],
  });
  await publish(deps, threadId, 'INPUT_EXPIRED', { toolCallId });
  await publish(deps, threadId, 'STATE_CHANGE', { state: 'RUNNING' });
  await deps.kv.set(`agent:state:${threadId}`, 'RUNNING');

  const thread = await deps.storage.threads.get(threadId);
  if (thread) {
    // Re-enter via the queue (§2.8) — the ticket from the event payload
    // rebuilds the original dispatch
    await deps.queue.enqueue({ threadId, model: resume?.model ?? thread.model, ...(resume ? { agent: resume.agent } : {}) });
  }
  return true;
}
```

```typescript
// Wiring point 1: the death notice — a worker torn down mid-segment (deploy,
// crash, infra kill) notifies the channel itself, best-effort. A thread parked
// on HITL outlives its worker by design; this notice just accelerates healing.
process.once('SIGTERM', () => {
  void deps.bus.publish(threadId, {
    type: 'HITL_ORPHANED', threadId, toolCallId,
  } as AgentEvent);
});

// Wiring point 2: /api/agent/stream — the SSE distributor is already subscribed
// to the thread channel, so it doubles as the watchdog while anyone is watching.
// runtime.events.subscribe includes the §2.5 heartbeat internally.
export async function GET(req: NextRequest) {
  const threadId = req.nextUrl.searchParams.get('threadId')!;
  const since = Number(req.nextUrl.searchParams.get('since') ?? -1);
  const encoder = new TextEncoder();

  const stream = new ReadableStream({
    async start(controller) {
      // 1. Replay the durable log after the client's cursor (§2.2) ...
      for (const e of await runtime.events.since(threadId, since)) {
        controller.enqueue(encoder.encode(`id: ${e.seq}\ndata: ${JSON.stringify(e)}\n\n`));
      }

      // 2. ...heal the thread if its parked request expired untouched ...
      void runtime.hitl.reclaimIfOrphaned(threadId);

      // 3. ...then tail live events; death notices trigger reclamation instantly
      const unsubscribe = await runtime.events.subscribe(threadId, (event) => {
        controller.enqueue(encoder.encode(`id: ${event.seq}\ndata: ${JSON.stringify(event)}\n\n`));
        if (event.type === 'HITL_ORPHANED') void runtime.hitl.reclaimIfOrphaned(threadId);
      });

      req.signal.addEventListener('abort', () => {
        unsubscribe();
        controller.close();
      });
    },
  });

  return new Response(stream, {
    headers: { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache' },
  });
}
```

### 2.6 Context Window Management & Compaction

All models share a **hard context ceiling of 265,000 tokens**. A model's effective budget is the smaller of its native window and the ceiling — models below 265k simply keep their native window:

| Model | Native Window | Effective Budget |
| :--- | :--- | :--- |
| GPT-4o | 128k | 128k (native, below ceiling) |
| Claude 3.5 Sonnet | 200k | 200k (native, below ceiling) |
| Gemini 1.5 Pro | 1M | **265k (ceiling applies)** |

#### Compaction Strategy

1. **Accounting:** token counts are estimated per message (~JSON chars ÷ 4, or a provider tokenizer). The budget for history is `effectiveBudget − outputReserve` (16k reserved for the completion) .
2. **Trigger:** before every run (and between steps in long tool loops), if the history estimate exceeds **80% of budget**, compaction runs.
3. **Compaction:** the most recent tail (≤ 25% of budget) is kept verbatim; everything older is summarized into a single `CONTEXT_SUMMARY` system message by `gpt-4o-mini`.
4. **Durability:** the summary is persisted as a real `Message`, so all clients and every reconnect replay (§2.2) reconstruct the identical context — compaction is never a per-request transform.
5. **Oversized inputs:** a single tool result larger than 10% of budget is stored in summarized form; a single user message larger than 50% of budget is rejected with `400`.
6. **Billing:** compaction is an LLM call, so its usage is recorded in `TokenUsage` (model `gpt-4o-mini`) and billed like any other (§4), announced via a `CONTEXT_COMPACTED` event.

Reference implementation:

```typescript
// src/core/context.ts — ships in the package; `deps` are the §3.2 ports
import { generateText } from 'ai';
import type { RuntimePorts } from '../ports/runtime';
import { publish } from './engine';

export const CONTEXT_TOKEN_CEILING = 265_000; // hard ceiling across all models

const NATIVE_WINDOWS: Record<string, number> = {
  'gpt-4o': 128_000,
  'claude-3-5-sonnet': 200_000,
  'gemini-1.5-pro': 1_000_000,
};

// Models below the ceiling keep their native window
export function contextBudget(model: string): number {
  return Math.min(NATIVE_WINDOWS[model] ?? CONTEXT_TOKEN_CEILING, CONTEXT_TOKEN_CEILING);
}

const OUTPUT_RESERVE = 16_000;  // headroom for the completion itself
const COMPACTION_TRIGGER = 0.8; // compact at 80% of budget
const TAIL_SHARE = 0.25;        // recent history kept verbatim

const estimateTokens = (content: unknown) => Math.ceil(JSON.stringify(content).length / 4);

// Returns a history array guaranteed to fit the model's budget. Compaction is
// durable: the summary is persisted as a Message, so every client and every
// reconnect replay (§2.2) reconstructs the exact same context.
export async function compactContext(deps: RuntimePorts, threadId: string, model: string) {
  const budget = contextBudget(model) - OUTPUT_RESERVE;
  const history = await deps.storage.messages.list(threadId);

  const total = history.reduce((sum, m) => sum + estimateTokens(m.content), 0);
  if (total <= budget * COMPACTION_TRIGGER) return history;

  // Keep the most recent tail verbatim ...
  const tail: typeof history = [];
  let tailTokens = 0;
  for (const m of [...history].reverse()) {
    if (tailTokens + estimateTokens(m.content) > budget * TAIL_SHARE) break;
    tail.unshift(m);
    tailTokens += estimateTokens(m.content);
  }
  const older = history.slice(0, history.length - tail.length);
  if (older.length === 0) return history; // single oversized turn — blocked by the input guards above

  // ... and summarize everything before it with a cheap model
  const { text, usage } = await generateText({
    model: deps.models['gpt-4o-mini'],
    prompt:
      'Summarize the following conversation history into a dense context brief ' +
      '(decisions, open threads, key facts) for an AI agent:\n\n' +
      older.map((m) => `${m.role}: ${JSON.stringify(m.content)}`).join('\n'),
  });

  const summary = await deps.storage.messages.append(threadId, {
    role: 'system',
    content: { type: 'CONTEXT_SUMMARY', text },
  });

  // Compaction is an LLM call, so it is billed like any other (§4)
  const costUSD = calculateCost(deps, 'gpt-4o-mini', usage.promptTokens ?? 0, usage.completionTokens ?? 0);
  await deps.storage.usage.record(threadId, {
    model: 'gpt-4o-mini',
    promptTokens: usage.promptTokens ?? 0,
    completionTokens: usage.completionTokens ?? 0,
    costUSD: costUSD ?? 0,
  });
  await publish(deps, threadId, 'CONTEXT_COMPACTED', { summarizedMessages: older.length });

  return [summary, ...tail];
}
```

### 2.7 Subagents (Delegated Execution)

The main agent can delegate self-contained sub-tasks to subagents. A subagent is a full agent run with its own isolated context, executed through the same detached pipeline as §2.1. The full reference implementation lives in `src/core/subagent.ts` (§5.5); this section walks through each concern.

#### Delegation Tool & Contract

The parent model sees exactly one tool. Its `instructions` parameter is the entire contract: the subagent never sees the parent's history, so the brief must be self-contained (goal, relevant facts, expected output shape).

```typescript
// What the parent model calls:
spawnSubagent({
  name: 'research-pricing', // short label — shown in the client's subagent tree
  instructions:
    'Find current on-demand GPU pricing ($/hr, H100) on AWS, GCP and Azure. ' +
    'Return a markdown table with source URLs.',
  model: 'gpt-4o-mini', // optional — narrow tasks can run on cheap models
});
```

```typescript
// src/core/subagent.ts — the tool factory (a run-scoped context keeps nesting honest)
export interface SubagentCtx {
  threadId: string;
  depth: number;  // 0 = called from the main agent
  sem: Semaphore; // per-run concurrency cap (below)
  ports: RuntimePorts; // the §3.2 ports bundle
}

export function spawnSubagentTool(ctx: SubagentCtx) {
  return tool({
    description: 'Delegates a self-contained task to a subagent with an isolated context',
    parameters: z.object({
      name: z.string().describe('Short name for the sub-task'),
      instructions: z.string().describe('Complete, self-contained brief: goal, constraints, expected output format'),
      model: z.enum(['gpt-4o', 'gpt-4o-mini', 'claude-3-5-sonnet', 'gemini-1.5-pro']).optional(),
    }),
    execute: async ({ name, instructions, model }, { abortSignal }) => {
      if (ctx.depth >= MAX_SUBAGENT_DEPTH) {
        return { error: `Max subagent depth (${MAX_SUBAGENT_DEPTH}) reached` };
      }
      const release = await ctx.sem.acquire(); // never more than 3 concurrent per run
      try {
        return await delegate({ ...ctx, name, instructions, model, abortSignal });
      } finally {
        release(); // sibling subagents queue instead of running away
      }
    }),
  });
}
```

#### Context Isolation

The subagent's prompt is the brief — nothing else is forwarded, which is what bounds the parent's context growth (§2.6) and lets cheap models handle narrow tasks:

```typescript
const result = streamText({
  model: modelRegistry[model] || modelRegistry['gpt-4o'],
  system: `You are the "${name}" subagent. Complete the task, then stop.`,
  prompt: instructions, // ← the only input; parent history is never passed
  abortSignal,          // cancellation propagates from the parent (below)
  maxSteps: 10,
  tools: {
    // Nesting allowed up to MAX_SUBAGENT_DEPTH. The default toolset is
    // restricted: destructive tools (sendEmail & co) are NOT included (§2.5).
    spawnSubagent: spawnSubagentTool({ ...ctx, depth: depth + 1 }),
  },
  ...
});

// The parent receives a capped result, keeping its own context small (§2.6)
return { agentId: run.id, result: result.slice(0, PARENT_RESULT_CAP) }; // 8k chars
```

#### Observability: Namespaced Events

Subagent events go to the same `AgentEvent` log with `agentId` in the payload, so the §2.2 pipeline (durable log + Pub/Sub fan-out) works unchanged and every connected client sees subagent activity live:

```typescript
await publish(deps, threadId, 'SUBAGENT_STARTED',   { agentId: run.id, name, depth });
await publish(deps, threadId, 'SUBAGENT_CHUNK',     { agentId, chunk }); // every token, attributed
await publish(deps, threadId, 'SUBAGENT_COMPLETED', { agentId, summary });
await publish(deps, threadId, 'SUBAGENT_FAILED',    { agentId, state });
```

UIs filter on the `SUBAGENT_*` prefix and nest chunks under the matching `agentId`; the SSE replay path (`Last-Event-ID`) rebuilds the tree identically after a reconnect.

#### Billing Attribution

Each subagent records its own `TokenUsage` row with `agentId` (§2.4), so costs roll up per agent and per thread for the §4 credit meter:

```typescript
onFinish: async ({ usage }) => {
  await deps.storage.usage.record(threadId, {
    agentId, // ← attribution; null = main agent
    model,
    promptTokens: usage.promptTokens,
    completionTokens: usage.completionTokens,
    costUSD: calculateCost(model, usage.promptTokens, usage.completionTokens),
  });
},
```

```typescript
// Roll-up for the credit meter (§4): usage and cost per agent within a thread.
// User-side, adapter-specific — a SQL groupBy for the Prisma adapter.
const perAgent = await prisma.tokenUsage.groupBy({
  by: ['agentId'],
  where: { threadId },
  _sum: { promptTokens: true, completionTokens: true, costUSD: true },
});
```

#### Limits: Depth & Concurrency

```typescript
export const MAX_SUBAGENT_DEPTH = 2;        // main (0) → sub (1) → sub-sub (2)
export const MAX_CONCURRENT_SUBAGENTS = 3;  // per run, enforced by the Semaphore in §5.5
const PARENT_RESULT_CAP = 8_000;            // chars handed back to the parent (§2.6)
```

The concurrency cap uses a run-scoped semaphore: a subagent task acquires a slot before doing work and releases it in a `finally`, so a fourth concurrent delegation waits instead of starting. There is **no wall-clock timeout** on subagents — they are bounded by `maxSteps` (10) at the AI level, by the HITL TTL while parked (§2.5), and by the user's stop button. That is deliberate: a subagent may sit in a HITL wait for as long as the TTL allows without an outer timer killing it mid-wait. A failed subagent (provider error, nested failure) is recorded `FAILED` in `SubagentRun` and the error propagates to the parent step (§5.5).

#### HITL from a Subagent

Subagents share the thread's HITL path (§2.5) — `parkForApproval` works identically, with `agentId` added to the `INPUT_REQUIRED` payload so clients know which agent is asking:

```typescript
await publishEvent(threadId, 'INPUT_REQUIRED', { toolCallId, toolName, agentId, arguments: args, resume });
```

Because a subagent runs inside the parent's `spawnSubagent` tool call, a subagent park suspends the **whole run segment**: the parent's step cannot complete without the subagent's result, so `WAITING_FOR_INPUT` is truthful for the entire thread, and no extra machinery is needed — the state key, the event channel, and the handoff keys are all per-thread. Resolution is the identical §2.5 flow: respond → `agent:hitl:{toolCallId}` + resume job → the resumed segment resolves the pending request → the subagent executes or records the denial → finishes → its result returns to the parent as the `spawnSubagent` tool result.

With no subagent timeout, **the HITL TTL is the only bound on a parked request**: the thread waits for as long as the TTL allows (15 min default), and on expiry the model receives `{ responded: false, cancelled: true, reason: 'timeout' }` and decides what to do — there is no outer timer killing anything mid-wait, because no process is held waiting at all (§2.5). Stop and orphan reclamation are unchanged: both are thread-keyed and operate on the durable state regardless of which agent parked.

One deliberate policy: **concurrent parks are answer-latest**. Up to 3 subagents may park at once, but respond answers only the *latest* pending `INPUT_REQUIRED` — earlier ones time out. One prompt at a time is the intended UX; answering any pending request would be a small change to the respond validation.

By default subagents carry no destructive tools at all; grant them (and thereby HITL prompts) only when a workflow genuinely needs an approved side effect.

#### Stop Cascade

Subagents inherit termination from the parent run: the parent's abort signal is threaded into the subagent's `streamText` (`abortSignal`), so the moment the user presses stop, generation and nested tools abort immediately everywhere. The catch block records `SubagentRun.state = 'CANCELLED'` (§2.4) for each subagent that was running.

```typescript
// The catch block in spawnSubagentTool (§5.5) — records why the subagent died
catch (err) {
  const state = (await ctx.ports.kv.get(`agent:state:${threadId}`)) === 'CANCELLED' ? 'CANCELLED' : 'FAILED';
  await ctx.ports.storage.runs.update(run.id, { state });
  await publish(ctx.ports, threadId, 'SUBAGENT_FAILED', { agentId: run.id, state });
  throw err; // propagate: the parent step sees the failure / abort
}
```

### 2.8 Message Queue (Run Dispatch)

Every top-level run is dispatched through a durable message queue (QStash-style HTTP queues; Inngest works equally well). The queue is the backbone between the API and the engine:

- **Dispatch:** `/api/agent/run` never executes anything — it validates, persists the user message, marks the thread `RUNNING`, enqueues `{ threadId, model }` on the `agent-runs` queue, and returns `202 Accepted`. A deploy can never strand an accepted request: the job is already durable.
- **Consumer:** a signature-verified worker route (`/api/queue/agent-run`, §5.6) consumes the queue. The message is a **dispatch ticket, not an execution leash** — the worker acknowledges immediately and runs the engine via `waitUntil`, so runs (including parked HITL waits, §2.5) outlive any single HTTP response inside the long-running worker runtime. Before any work, the engine acquires the **per-thread run lock** (`agent:lock:{threadId}`, `SET NX` + lease via `kv.set`): a redelivered job or second worker finds the lock held and exits, and a crashed worker's lock expires instead of blocking forever.
- **At-least-once delivery:** queues redeliver, so consumers must be idempotent. Double dispatch is already a no-op: the state guard plus the §6.1 Redlock mean a second worker finds the thread busy and exits.
- **Failure policy:** transient failures (provider 5xx, network) are re-enqueued with exponential backoff, tracked by an attempt counter in Redis (max 3). Exhausted attempts finalize the thread `FAILED` with a `STATE_CHANGE` event instead of retrying forever. A user stop (`CANCELLED`) is never retried.
- **Flow control:** queue-level concurrency limits (e.g., QStash flow control) cap total concurrent runs platform-wide — in addition to the per-thread lock (§6.1) and the per-run subagent semaphore (§2.7). HITL reclamation (§2.5) re-enqueues the same message, so healed threads re-enter through the identical path.
- **Why subagents are not queued:** subagents stay in-process under the parent's abort signal, because the stop cascade (§2.7) and the shared concurrency semaphore depend on it. The queue is for top-level runs; queue-backed subagents can be layered on later without changing the event model.

---

## 3. Package Architecture: Ports & Adapters

The platform ships as a headless TypeScript library (working name `@agent/core`). Everything in §2 — engine, HITL, subagents, compaction, reclamation — lives in the package's `core/` and depends **only on ports** (interfaces). Vendor code exists exclusively in **adapters**: the package ships reference adapters for PostgreSQL (Prisma), Upstash Redis, and QStash, while users with MongoDB, MySQL, DynamoDB, SQS, Kafka, Ably — or in-house services shaped around their own team architecture — implement the same small interfaces and the entire platform works unchanged.

### 3.1 Package Layout

```
@agent/core
├── src/
│   ├── core/                  # pure business logic — imports ports, never vendors
│   │   ├── engine.ts          # execute / executeWithPolicy / publish (§5.6)
│   │   ├── hitl.ts            # parkForApproval, respond, TTL keys (§2.5)
│   │   ├── reclaim.ts         # reclaimIfOrphaned (§2.5)
│   │   ├── subagent.ts        # spawnSubagentTool, Semaphore (§2.7)
│   │   ├── context.ts         # contextBudget, compactContext (§2.6)
│   │   └── types.ts           # ExecutionState, DTOs, AgentConfig
│   ├── ports/                 # the interfaces users implement (§3.2)
│   │   ├── storage.ts         # threads / messages / events / usage / runs
│   │   ├── bus.ts             # EventBus — live fan-out + death notices
│   │   ├── queue.ts           # durable run dispatch
│   │   └── kv.ts              # state cache, HITL handoff, seq/attempt counters
│   ├── runtime.ts             # createAgentRuntime — binds ports to core (§3.3)
│   ├── adapters/              # shipped reference adapters
│   │   ├── prisma/            # PrismaStorage (schema in §2.4)
│   │   ├── upstash/           # UpstashBus, UpstashKv
│   │   └── qstash/            # QStashQueue
│   └── index.ts               # public surface: createAgentRuntime + types
├── examples/
│   └── nextjs-app/            # the §5 routes — an example integration, not the library
└── package.json
```

### 3.2 Ports (the interfaces users implement)

```typescript
// ports/storage.ts — persistence. DTOs cross the boundary; ORM types never leak.
export interface Storage {
  threads: {
    get(threadId: string): Promise<ThreadDTO | null>;
    setState(threadId: string, state: ExecutionState): Promise<void>;
    /** Compare-and-set: returns true iff THIS caller performed the transition.
     *  Backs HITL reclamation + double-dispatch protection (§2.5, §2.8). */
    claimState(threadId: string, from: ExecutionState, to: ExecutionState): Promise<boolean>;
  };
  messages: {
    append(threadId: string, message: NewMessage): Promise<MessageDTO>;
    list(threadId: string): Promise<MessageDTO[]>;
  };
  events: {
    append(threadId: string, event: AgentEvent): Promise<void>;
    listSince(threadId: string, sinceSeq: number): Promise<AgentEvent[]>; // SSE replay
    latest(threadId: string, type: string): Promise<AgentEvent | null>;   // HITL pending check
  };
  usage: {
    record(threadId: string, usage: NewUsage): Promise<void>;             // billing (§4)
  };
  runs: {
    create(threadId: string, run: NewRun): Promise<RunDTO>;
    update(runId: string, patch: Partial<RunDTO>): Promise<void>;
  };
}

// ports/bus.ts — pub/sub: live fan-out + death notices. At-most-once (§3.4).
export interface EventBus {
  publish(threadId: string, event: AgentEvent): Promise<void>;
  /** The reference adapter runs the §2.5 heartbeat while subscribed */
  subscribe(threadId: string, handler: (event: AgentEvent) => void): Promise<() => void>;
}

// ports/queue.ts — durable dispatch. At-least-once; consumers idempotent (§2.8).
export interface Queue {
  enqueue(job: { threadId: string; model: string }): Promise<void>;
}

// ports/kv.ts — hot state cache, HITL handoff keys, seq & attempt counters
export interface Kv {
  get(key: string): Promise<string | null>;
  set(key: string, value: string, opts?: { exSeconds?: number }): Promise<void>;
  del(key: string): Promise<void>;
  incr(key: string): Promise<number>;
}
```

### 3.3 The Runtime Surface (what users call)

```typescript
// ports/runtime.ts
export function createAgentRuntime(opts: RuntimeOptions): AgentRuntime {
  const deps = { ...opts }; // the ports bundle — everything in core/ receives this
  return {
    run: (input) => run(deps, input),                          // §5.1 behavior
    stop: (threadId) => stop(deps, threadId),                  // §5.2 behavior
    hitl: {
      respond: (input) => respond(deps, input),                // §2.5 behavior
      reclaimIfOrphaned: (threadId) => reclaimIfOrphaned(deps, threadId),
    },
    events: {
      since: (threadId, sinceSeq) => deps.storage.events.listSince(threadId, sinceSeq),
      subscribe: (threadId, handler) => deps.bus.subscribe(threadId, handler),
    },
    engine: {
      execute: (input) => execute(deps, input),                // worker-side only (§5.6)
      executeWithPolicy: (input, policy) => executeWithPolicy(deps, input, policy),
    },
  };
}
```

Each behavior is a small core function over the same ports — e.g., the entire stop mechanism:

```typescript
// src/core/stop.ts
export async function stop(deps: RuntimePorts, threadId: string) {
  const thread = await deps.storage.threads.get(threadId);
  if (thread?.state !== 'RUNNING' && thread?.state !== 'WAITING_FOR_INPUT') {
    return { accepted: false, error: `Cannot stop thread in state ${thread?.state}` };
  }
  await deps.kv.set(`agent:state:${threadId}`, 'CANCELLED');   // hot cache — the engine polls this
  await deps.storage.threads.setState(threadId, 'CANCELLED');  // durable truth
  await publish(deps, threadId, 'STATE_CHANGE', { state: 'CANCELLED' });
  return { accepted: true };
}
```

### 3.4 Adapter Invariants

The engine's correctness depends on adapters honoring these contracts:

1. **Sequence numbers:** `events.append` receives `seq` from `kv.incr(\`agent:seq:{threadId}\`)` — monotonic per thread. SSE resume (§2.2) depends on ordering, not gaplessness.
2. **`claimState` must be atomic** (a single conditional `UPDATE` or equivalent). HITL reclamation and double-dispatch protection rely on exactly one caller winning.
3. **`queue.enqueue` is at-least-once; the engine is idempotent.** The state guard + `claimState` make double dispatch a no-op — adapters must not drop jobs to "help". An adapter that cannot honor `delaySeconds` may deliver immediately (both callers treat an early arrival as a no-op) but must never throw for it: the HITL expiry is scheduled from inside a parked tool call, and a throw there would fail the run the park belongs to. The reference adapter shows the shape — QStash accepts delays on publish and rejects them on queue enqueue, so `QStashQueue` publishes delayed jobs and queues everything else.
4. **`bus` is at-most-once.** The §2.5 death-notice/heartbeat/watchdog pattern exists because pub/sub drops; stronger buses may simplify the heartbeat, but reclamation stays.
5. **Two state homes, one truth:** durable thread state lives in `storage.threads`; the kv copy (`agent:state:{threadId}`) is a hot cache the engine polls (`stopPollMs`, 500 ms by default). Behavior functions write both — including terminal transitions like `FAILED` — and the durable copy decides recovery.
6. **No vendor on the core path:** `core/` imports nothing from `adapters/`; adapters never import each other's clients.
7. **Run lock:** `engine.execute` begins by acquiring `agent:lock:{threadId}` via `kv.set` with `onlyIfNotExists` (SET NX) and a lease (`runLockLeaseSeconds`) — queued→running is thus atomic, double dispatch is a no-op, and a crashed worker's lock self-expires. The lock is released in a `finally`. Its value is the holder's **run id**, which is what lets a blocked job tell a duplicate of itself apart from an older run still finishing (invariant 8).
8. **Run identity:** `run()` (and the HITL resume paths) mint a run id into `agent:run:{threadId}` **before** writing the state key, and every dispatch carries it. The state key alone cannot signal a stop — a user who stops and immediately sends another message puts `RUNNING` back over `CANCELLED` inside the poll window — so a worker also aborts when its run id is no longer current, and `finalize` writes state only while it still is. A job blocked by an older run's lock is re-dispatched (`runRedriveDelaySeconds`), never dropped: dropping it would strand the message the user just sent.
9. **A stream's error part is fatal:** `streamText` reports a provider failure (an aborted call included) as an `error` part and then ends the stream normally, while its `text`/`usage`/`response` promises never settle. `executeStep` carries that error out of the drain and throws it. Awaiting those promises instead hangs the worker forever holding the run lock.

---

## 4. Billing Architecture: Hybrid Credit Metering

To make billing intuitive and predictable for users, **avoid raw token billing in the UI. Instead, convert token usage into *Compute Credits***.

### Credit Calculation

$$\text{Credits Used} = (\text{Prompt Tokens} \times \text{Multiplier}_{\text{prompt}}) + (\text{Completion Tokens} \times \text{Multiplier}_{\text{completion}})$$

| Model | Prompt Cost / 1k Tokens | Completion Cost / 1k Tokens | Credit Ratio ($1 = 1000$ Credits) |
| :--- | :--- | :--- | :--- |
| **GPT-4o** | $0.0025 | $0.0100 | ~2.5 Credits / 1k Prompt |
| **Claude 3.5 Sonnet** | $0.0030 | $0.0150 | ~3.0 Credits / 1k Prompt |
| **Gemini 1.5 Pro** | $0.00125 | $0.0050 | ~1.25 Credits / 1k Prompt |

### Architectural Flow for Billing

1. **Pre-execution Check:** Ensure user balance >= Threshold (e.g., minimum 10 Credits).
2. **Streaming Counter:** Count incoming tokens during the Stream using `onFinish` hooks provided by `streamText`.
3. **Deduction:** Atomically decrement user credit balance in PostgreSQL/Stripe Metered Billing upon step or stream completion. Per-model rates come from `config.billingRates` merged over built-in defaults (GPT-4o, GPT-4o-mini, Claude 3.5 Sonnet, Gemini 1.5 Pro). A model with **no configured rate records cost 0 and publishes a `BILLING_UNPRICED` warning event** — it is never silently priced at another model's rate.

---

## 5. Reference Integration (Next.js App Router)

The package is headless — everything in §2 ships inside `@agent/core`. This section is the **example integration**: thin Next.js route handlers wiring the runtime to HTTP, demonstrating how the flows execute. Each handler is a few lines; all behavior lives in the runtime (§3.3). The only vendor-wiring file in the example is the runtime instantiation:

```typescript
// examples/nextjs-app/lib/runtime.ts
import { Client } from '@upstash/qstash';
import { Redis } from '@upstash/redis';
import { PrismaClient } from '@prisma/client';
import { createAgentRuntime } from '@agent/core';
import { PrismaStorage } from '@agent/adapters/prisma';
import { UpstashBus, UpstashKv } from '@agent/adapters/upstash';
import { QStashQueue } from '@agent/adapters/qstash';
import { modelRegistry } from './models'; // §2.3

const redis = new Redis({ url: process.env.UPSTASH_URL!, token: process.env.UPSTASH_TOKEN! });

export const runtime = createAgentRuntime({
  storage: new PrismaStorage(new PrismaClient()),   // ← swap for your own Storage adapter
  bus: new UpstashBus(redis),                       // ← Ably / Kafka / Postgres LISTEN…
  queue: new QStashQueue(new Client({ token: process.env.QSTASH_TOKEN! })), // ← SQS / BullMQ…
  kv: new UpstashKv(redis),
  models: modelRegistry,
});
```

### 5.1 Run API (`app/api/agent/run/route.ts`)

```typescript
import { NextRequest, NextResponse } from 'next/server';
import { runtime } from '@/lib/runtime';

export async function POST(req: NextRequest) {
  const { threadId, prompt, model } = await req.json();

  const result = await runtime.run({ threadId, prompt, model });
  // runtime.run: heal orphans (§2.5) → billing pre-check (§4) → persist user
  // message → state RUNNING → enqueue `agent-runs` (§2.8)
  return NextResponse.json(result, { status: result.accepted ? 202 : 409 });
}
```

### 5.2 Stop API (`app/api/agent/control/route.ts`)

```typescript
import { NextRequest, NextResponse } from 'next/server';
import { runtime } from '@/lib/runtime';

// One stop button: runtime.stop is the single `state → CANCELLED` write (§2.1)
export async function POST(req: NextRequest) {
  const { threadId } = await req.json();
  const result = await runtime.stop(threadId);
  return NextResponse.json(result, { status: result.accepted ? 200 : 409 });
}
```

### 5.3 Unified Client Hook (`hooks/useAgentThread.ts`)

Unchanged by the package split — the hook is client-side and talks to the example routes, not to the package:

```typescript
import { useState, useEffect } from 'react';

export function useAgentThread(threadId: string) {
  const [messages, setMessages] = useState<any[]>([]);
  const [agentState, setAgentState] = useState<
    'IDLE' | 'RUNNING' | 'WAITING_FOR_INPUT' | 'CANCELLED' | 'COMPLETED' | 'FAILED'
  >('IDLE');
  const [pendingInput, setPendingInput] = useState<{
    toolCallId: string;
    toolName: string;
    arguments: any;
  } | null>(null);

  useEffect(() => {
    // EventSource reconnects automatically; the server replays missed events
    // from the AgentEvent log, so a disconnect is invisible to the UI.
    const eventSource = new EventSource(`/api/agent/stream?threadId=${threadId}`);

    eventSource.onmessage = (event) => {
      const data = JSON.parse(event.data);

      if (data.type === 'STATE_CHANGE') {
        setAgentState(data.state);
        // Clears the approval prompt on resolve, expiry, or stop
        if (data.state !== 'WAITING_FOR_INPUT') setPendingInput(null);
      } else if (data.type === 'INPUT_REQUIRED') {
        setPendingInput(data.payload);
      } else if (data.type === 'CHUNK') {
        setMessages((prev) => [...prev, data.payload]);
      }
    };

    return () => {
      eventSource.close();
    };
  }, [threadId]);

  const post = (url: string, body: unknown) =>
    fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });

  return {
    messages,
    agentState,
    pendingInput,
    // One stop button: everything stops immediately (§2.1)
    stop: () => post('/api/agent/control', { threadId, action: 'STOP' }),
    // HITL: approve (with optional form payload) or deny the pending tool call
    respondToInput: (approved: boolean, payload?: unknown) =>
      post('/api/agent/respond', {
        threadId,
        toolCallId: pendingInput?.toolCallId,
        approved,
        payload,
      }),
  };
}
```

### 5.4 HITL Response API (`app/api/agent/respond/route.ts`)

```typescript
import { NextRequest, NextResponse } from 'next/server';
import { runtime } from '@/lib/runtime';

export async function POST(req: NextRequest) {
  const body = await req.json();
  const result = await runtime.hitl.respond(body);
  // runtime.hitl.respond: heal orphans (§2.5) → validate thread WAITING_FOR_INPUT
  // + latest pending INPUT_REQUIRED → write `agent:hitl:{toolCallId}` handoff key
  // (remaining-TTL) → enqueue the resume job (§2.8) so a worker appends the
  // tool result and continues the run
  return NextResponse.json(result, { status: result.delivered ? 200 : 409 });
}
```

### 5.5 Subagents (`src/core/subagent.ts` — ships in the package)

```typescript
// src/core/subagent.ts
import { streamText, tool } from 'ai';
import { z } from 'zod';
import type { RuntimePorts } from '../ports/runtime';
import { publish } from './engine';

export const MAX_SUBAGENT_DEPTH = 2;        // main (0) → sub (1) → sub-sub (2)
export const MAX_CONCURRENT_SUBAGENTS = 3;  // per run
const PARENT_RESULT_CAP = 8_000;            // chars handed back to the parent (§2.6)

export interface SubagentCtx {
  threadId: string;
  depth: number;       // 0 = called from the main agent
  sem: Semaphore;
  ports: RuntimePorts; // the §3.2 ports bundle
}

// Run-scoped semaphore: sibling subagents queue instead of running away
export class Semaphore {
  private active = 0;
  private waiters: (() => void)[] = [];
  constructor(private readonly limit: number) {}

  async acquire(): Promise<() => void> {
    await new Promise<void>((resolve) => {
      if (this.active < this.limit) { this.active++; resolve(); }
      else this.waiters.push(() => { this.active++; resolve(); });
    });
    let released = false;
    return () => {
      if (released) return;
      released = true;
      this.active--;
      this.waiters.shift()?.();
    };
  }
}

export function spawnSubagentTool(ctx: SubagentCtx) {
  return tool({
    description: 'Delegates a self-contained task to a subagent with an isolated context',
    parameters: z.object({
      name: z.string().describe('Short name for the sub-task'),
      instructions: z.string().describe('Complete, self-contained brief: goal, constraints, expected output format'),
      model: z.enum(['gpt-4o', 'gpt-4o-mini', 'claude-3-5-sonnet', 'gemini-1.5-pro']).optional(),
    }),
    execute: async ({ name, instructions, model }, { abortSignal }) => {
      if (ctx.depth >= MAX_SUBAGENT_DEPTH) {
        return { error: `Max subagent depth (${MAX_SUBAGENT_DEPTH}) reached` };
      }

      const release = await ctx.sem.acquire();
      try {
        const run = await ctx.ports.storage.runs.create(ctx.threadId, {
          name,
          model: model ?? 'gpt-4o',
          depth: ctx.depth + 1,
          state: 'RUNNING',
        });
        await publish(ctx.ports, ctx.threadId, 'SUBAGENT_STARTED', {
          agentId: run.id, name, depth: ctx.depth + 1,
        });

        try {
          const result = await runSubagent({
            threadId: ctx.threadId,
            depth: ctx.depth,
            sem: ctx.sem,
            ports: ctx.ports,
            agentId: run.id,
            name,
            instructions,
            model: (model ?? 'gpt-4o') as string,
            abortSignal, // cancellation propagates from the parent (§2.7)
          });

          await ctx.ports.storage.runs.update(run.id, {
            state: 'COMPLETED',
            result: { text: result },
          });
          await publish(ctx.ports, ctx.threadId, 'SUBAGENT_COMPLETED', { agentId: run.id });

          // The parent receives a capped result, keeping its own context small (§2.6)
          return { agentId: run.id, result: result.slice(0, PARENT_RESULT_CAP) };
        } catch (err) {
          const state = (await ctx.ports.kv.get(`agent:state:${ctx.threadId}`)) === 'CANCELLED'
            ? 'CANCELLED' : 'FAILED';
          await ctx.ports.storage.runs.update(run.id, { state });
          await publish(ctx.ports, ctx.threadId, 'SUBAGENT_FAILED', { agentId: run.id, state });
          throw err; // propagate: the parent step sees the failure / abort
        }
      } finally {
        release();
      }
    },
  });
}

async function runSubagent(opts: SubagentCtx & {
  agentId: string;
  name: string;
  instructions: string;
  model: string;
  abortSignal?: AbortSignal;
}) {
  const { threadId, depth, sem, ports, agentId, name, instructions, model, abortSignal } = opts;

  const result = streamText({
    model: ports.models[model] || ports.models['gpt-4o'],
    // Isolated context: seeded with the brief only — never the parent history
    system: `You are the "${name}" subagent. Complete the task, then stop.`,
    prompt: instructions,
    abortSignal, // stop tears this down immediately (§2.7)
    maxSteps: 10,
    tools: {
      // Nesting up to MAX_SUBAGENT_DEPTH. Default toolset is restricted:
      // destructive tools (requiresConfirmation) are not included (§2.5, §2.7)
      spawnSubagent: spawnSubagentTool({ threadId, depth: depth + 1, sem, ports }),
    },
    onChunk: async ({ chunk }) => {
      // Namespaced into the shared thread event log → same multi-user pipeline (§2.2)
      await publish(ports, threadId, 'SUBAGENT_CHUNK', { agentId, chunk });
    },
    onFinish: async ({ usage }) => {
      // Billing attribution per subagent (§4) — unpriced models are marked
      const costUSD = calculateCost(ports, model, usage.promptTokens, usage.completionTokens);
      if (costUSD === null) {
        await publish(ports, threadId, 'BILLING_UNPRICED', { agentId, model });
      }
      await ports.storage.usage.record(threadId, {
        agentId,
        model,
        promptTokens: usage.promptTokens,
        completionTokens: usage.completionTokens,
        costUSD: costUSD ?? 0,
      });
    },
  });

  return result.text;
}
```

### 5.6 Queue Consumer & Engine (`app/api/queue/agent-run/route.ts`, `src/core/engine.ts`)

```typescript
// examples/nextjs-app/app/api/queue/agent-run/route.ts — the `agent-runs` consumer
import { verifySignatureApprouter } from '@upstash/qstash/nextjs';
import { NextRequest, NextResponse } from 'next/server';
import { waitUntil } from '@vercel/functions';
import { runtime } from '@/lib/runtime';

async function handler(req: NextRequest) {
  const { threadId, model } = await req.json();

  // The message is a dispatch ticket, not an execution leash: runs — including
  // parked HITL waits (§2.5) — outlive this HTTP response inside the worker.
  // executeWithPolicy: redrive < maxAttempts, else finalize FAILED (§2.8)
  waitUntil(runtime.engine.executeWithPolicy({ threadId, model }));

  // Ack immediately. Delivery is at-least-once, so double dispatch is possible;
  // the per-thread run lock (§3.4) makes it a no-op.
  return NextResponse.json({ accepted: true });
}

// Signature verification WRAPS the handler — only genuine QStash deliveries
// ever reach the runtime (§2.8).
export const POST = verifySignatureApprouter(handler);
```

```typescript
// src/core/engine.ts — ships in the package
import { streamText, tool } from 'ai';
import { randomUUID } from 'node:crypto';
import { z } from 'zod';
import type { RuntimePorts } from '../ports/runtime';
import type { ExecutionState } from './types';
import { compactContext } from './context';
import { calculateCost } from './billing';
import { MAX_CONCURRENT_SUBAGENTS, Semaphore, spawnSubagentTool } from './subagent';

const runLockKey = (threadId: string) => `agent:lock:${threadId}`;

// Persist to the replayable event log, then fan out live to all subscribers
export async function publish(deps: RuntimePorts, threadId: string, type: string, payload: unknown) {
  const seq = await deps.kv.incr(`agent:seq:${threadId}`);
  const event = { threadId, seq, type, payload, createdAt: new Date() };
  await deps.storage.events.append(threadId, event);
  await deps.bus.publish(threadId, event);
}

export async function execute(deps: RuntimePorts, input: { threadId: string; model: string }) {
  const { threadId, model } = input;
  const abort = new AbortController();

  // Concurrency: acquire the per-thread run lock (SET NX + lease, §3.4) before
  // any work — two workers can never run one thread; a crashed worker's lock
  // self-expires instead of blocking forever.
  const locked = await deps.kv.set(runLockKey(threadId), randomUUID(), {
    onlyIfNotExists: true,
    exSeconds: deps.config.runLockLeaseSeconds,
  });
  if (!locked) return; // another worker owns this thread — at-least-once dispatch (§2.8)

  // One signal, one behavior: the moment the state key reads CANCELLED —
  // the user pressed stop (§5.2) — everything tears down immediately.
  const controlPoll = setInterval(async () => {
    if ((await deps.kv.get(`agent:state:${threadId}`)) === 'CANCELLED') {
      abort.abort();
    }
  }, 500);

  try {
    // Durable compaction pass — history always fits the model budget (§2.6)
    const history = await compactContext(deps, threadId, model);

    const result = streamText({
      model: deps.models[model] || deps.models['gpt-4o'],
      messages: history.map((m) => ({ role: m.role as any, content: m.content as string })),
      abortSignal: abort.signal,
      maxSteps: 25,
      tools: {
        executeTask: tool({
          description: 'Executes a long running task',
          parameters: z.object({ stepName: z.string() }),
          execute: async ({ stepName }, { abortSignal }) => {
            // Checkpoint: stop takes effect here even without the signal
            if (abortSignal?.aborted) throw new Error('EXECUTION_CANCELLED');
            return { status: 'SUCCESS', result: `Completed ${stepName}` };
          },
        }),
        sendEmail: tool({
          description: 'Sends an email (destructive — requires user approval)',
          parameters: z.object({ to: z.string().email(), subject: z.string(), body: z.string() }),
          requiresConfirmation: true, // spec convention: engine suspends with INPUT_REQUIRED instead of executing (§2.5)
          execute: async ({ to, subject, body }) => ({ status: 'SENT', to, subject }),
        }),
        spawnSubagent: spawnSubagentTool({ // subagent delegation (§2.7)
          threadId,
          depth: 0,
          sem: new Semaphore(MAX_CONCURRENT_SUBAGENTS),
          ports: deps,
        }),
      },
      onChunk: async ({ chunk }) => {
        // One canonical path for every client: durable log + live Pub/Sub (§2.1, §2.2)
        await publish(deps, threadId, 'CHUNK', chunk);
      },
      onFinish: async ({ usage, finishReason, response }) => {
        // Persist the completed assistant turn(s) — including tool calls and
        // tool results — BEFORE the state transition, so redrives, HITL
        // resumes, and replay always see a valid history (§2.2, §2.8)
        for (const message of response.messages) {
          await deps.storage.messages.append(threadId, {
            role: message.role,
            content: message.content,
          });
        }

        const finalState = abort.signal.aborted ? 'CANCELLED' : 'COMPLETED';
        const stopReason = abort.signal.aborted ? 'cancelled'
          : finishReason === 'tool-calls' ? 'max_steps' // safety cap hit (§2.1)
          : 'completed';

        // Unpriced models are marked, never silently mispriced (§4)
        const costUSD = calculateCost(deps, model, usage.promptTokens, usage.completionTokens);
        if (costUSD === null) {
          await publish(deps, threadId, 'BILLING_UNPRICED', { model });
        }
        await deps.storage.usage.record(threadId, {
          model,
          promptTokens: usage.promptTokens,
          completionTokens: usage.completionTokens,
          costUSD: costUSD ?? 0,
        });
        await deps.kv.set(`agent:state:${threadId}`, finalState);
        await deps.storage.threads.setState(threadId, finalState as ExecutionState);
        await publish(deps, threadId, 'STATE_CHANGE', { state: finalState, stopReason });
      },
    });

    await result.text; // keep the worker invocation alive until the stream drains
  } finally {
    clearInterval(controlPoll);
    await deps.kv.del(runLockKey(threadId)); // release — success, failure, or stop
  }
}

// §2.8 failure policy: transient errors redrive; exhausted attempts → FAILED
export async function executeWithPolicy(
  deps: RuntimePorts,
  input: { threadId: string; model: string },
  policy: { maxAttempts: number } = { maxAttempts: deps.config.runMaxAttempts },
): Promise<void> {
  try {
    await execute(deps, input);
    // Success resets the retry budget — past failures must not count forever
    await deps.kv.del(`agent:attempts:${input.threadId}`);
  } catch (err) {
    // A user stop already finalized the thread — never retry a stop
    if ((await deps.kv.get(`agent:state:${input.threadId}`)) === 'CANCELLED') return;

    const attempts = await deps.kv.incr(`agent:attempts:${input.threadId}`);
    if (attempts < policy.maxAttempts) {
      // Transient failure (provider 5xx, network): redrive with backoff
      return deps.queue.enqueue({ threadId: input.threadId, model: input.model });
    }

    // Attempts exhausted: finalize FAILED on BOTH the hot cache and durable
    // truth, or subsequent runs would still treat the thread as active (§2.1)
    await deps.kv.set(`agent:state:${input.threadId}`, 'FAILED');
    await deps.storage.threads.setState(input.threadId, 'FAILED');
    await publish(deps, input.threadId, 'STATE_CHANGE', { state: 'FAILED' });
    await deps.kv.del(`agent:attempts:${input.threadId}`);
  }
}
```

> `calculateCost` ships in the package (`src/core/billing.ts`): built-in rates for GPT-4o, GPT-4o-mini, Claude 3.5 Sonnet, and Gemini 1.5 Pro (§4), overridable via `config.billingRates`. It returns `null` for unpriced models instead of guessing a rate.

---

## 6. Architectural Additions (Missing Requirements Included)

### 6.1 Edge-Safe Distributed Locks

To prevent dual executions if multiple users click "Run" at the same time:

- **Implementation:** Implement a Redlock pattern via `@upstash/redis` at the start of the `/api/agent/run` route using key `lock:agent:${threadId}`.
