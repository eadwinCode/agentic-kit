# Design Doc: `setupAgentCore` — Splitting Agent Flavors from Platform Services

**Status:** Implemented (v1) — target surface for the next minor release of `@agent/core`.
**Builds on:** the [technical specification](./agent-platform-technical-spec.md) (§ references below point there).

> **Implementation status:** `setupAgentCore` and the registered-handle execution path live in `src/core/agent.ts` and `src/runtime.ts`. `spawnSubagent` is constructed by the platform **only when the spec enables subagents**, and model precedence is **run `input.model` → `spec.model` → `'gpt-4o'`**. The deprecated compatibility engine remains available through `createAgentRuntime` for the one-minor migration window.

---

## 1. Problem

`createAgentRuntime` currently hard-wires one generation flavor — `streamText` — into the engine. Everything else the runtime does (locks, event log, HITL, compaction, token attribution, redrive policy) is generation-agnostic, but the API gives users no way to say *"this agent streams"* vs *"this agent is a one-shot generate call"* without forking the engine.

The refactor, in one sentence: **the runtime keeps the platform services; agent flavors become first-class handles.**

- `setupAgentCore(...)` (replacing `createAgentRuntime`) returns platform-level services plus two factories.
- `createStreamTextAgent({...})` / `createGenerateTextAgent({...})` return an **agent handle** — an executor bound to a specific generation mode and to *your* generation arguments.
- `execute()` inside the engine receives those bound arguments and composes them with the platform-owned machinery (history + compaction, run lock, HITL wrapping, event log, token attribution, finalize).

---

## 2. Public Surface

```typescript
export interface AgentCore {
  /** Resolve a registry key to the stable identity and provider instance used
   *  by execution, compaction, usage attribution, and persisted run metadata. */
  resolveModel(modelName: string): ResolvedModel;

  /** One call for UIs / history routes: thread + messages + recent events. */
  getThreadSnapshot(threadId: string): Promise<ThreadSnapshot | null>;

  hitl: {
    respond(input: RespondInput): Promise<RespondResult>;
    reclaimIfOrphaned(threadId: string): Promise<boolean>;
  };

  events: {
    since(threadId: string, sinceSeq: number): Promise<AgentEvent[]>;
    subscribe(threadId: string, handler: (event: AgentEvent) => void): Promise<() => void>;
  };

  /** Agent factories — see §4. Each call registers a handle under `spec.name`. */
  createStreamTextAgent(spec: StreamTextAgentSpec): StreamTextAgent;
  createGenerateTextAgent(spec: GenerateTextAgentSpec): GenerateTextAgent;

  /** Worker-side resolution of a registered handle from the queue job. */
  getAgent(name: string): AgentHandle | null;

  /** The queue dispatch side of the platform (§2.8): resolves the handle,
   *  applies the failure policy, and is idempotent under at-least-once
   *  delivery (the per-thread run lock, §3.4). The HTTP layer only verifies
   *  signatures, parses JSON, and calls this. */
  worker: {
    handleJob(job: RunJob): Promise<{ accepted: boolean; reason?: string }>;
  };
}
```

```typescript
/** A registry entry after normalization. `instance` is a factory so the
 *  provider instance is created lazily per run; `contextWindow`, when the
 *  user declares it, feeds the §2.6 compaction budget math. */
export interface ResolvedModel {
  instance: () => LanguageModel;
  contextWindow?: number;
}
```

`createAgentRuntime` remains as a deprecated alias for `setupAgentCore` for one minor release. There is **no models registry** on the options — the consumer supplies `resolveModel(modelName)` and models can live in any shape on their side.

### 2.1 `ThreadSnapshot`

```typescript
export interface ThreadSnapshot {
  thread: ThreadDTO;        // id, state, model, timestamps
  messages: MessageDTO[];   // full history, ascending (incl. CONTEXT_SUMMARY, §2.6)
  events: AgentEvent[];     // last `snapshotEventLimit` (default 200), ascending
}
```

Deliberately **lean** — this is the payload the history route and the UI thread view reconstruct from (state header, bubbles, subagent tree from `SUBAGENT_*` events). It does not carry:

- **Usage rows** — an admin/billing concern, not a thread-view concern; raw rows would grow unboundedly and cost extra queries on every snapshot. Totals, when a UI needs them, belong in a dedicated accessor (e.g., `runtime.getUsage(threadId)` returning an aggregate) or a separate route.
- **Subagent runs** — the tree is already derivable from the snapshot's `SUBAGENT_*` events (§2.7 observability). Run-level detail stays available via the `runs` queries on the `Storage` port for admin views.

Backs the `/agent/history` route and any UI "thread view" — one storage round-trip set, no client-side stitching.

---

## 3. Agent Handles

```typescript
export interface AgentHandle {
  readonly name: string;
  readonly kind: 'stream-text' | 'generate-text';

  /** Worker-side only (§5.6). Throws on failure — see executeWithPolicy.
   *  Returns 'lock-conflict' when another worker owns the thread's run lock
   *  (at-least-once duplicate delivery, §2.8) and nothing was executed.
   *
   *  tokenBudget: max cumulative tokens (input + output) for the whole run.
   *  Tracked on every onStepFinish; the generation loop breaks BEFORE the
   *  next step once spent, and the run finalizes COMPLETED with
   *  stopReason 'token_budget' (§2.1 safety cap). */
  execute(input: {
    threadId: string;
    model: string;
    tokenBudget?: number;
  }): Promise<'executed' | 'lock-conflict'>;

  /** execute + §2.8 failure policy: redrive < maxAttempts, else finalize FAILED */
  executeWithPolicy(
    input: { threadId: string; model: string; tokenBudget?: number },
    policy?: { maxAttempts?: number },
  ): Promise<void>;

  /** Persist user message → state RUNNING → enqueue a job dispatched back to
   *  THIS handle (the job carries `agent: this.name` and `tokenBudget`). */
  run(input: RunInput): Promise<RunResult>;

  /** Platform stop (§2.1) — works regardless of which agent's run is active. */
  stop(threadId: string): Promise<StopResult>;
}
```

`StreamTextAgent` and `GenerateTextAgent` are both `AgentHandle`; they differ only in what `execute()` invokes and in the accepted `spec.args`.

```typescript
// ports/runtime.ts
export interface RunInput {
  /** Omit to create a fresh thread first (threads.create, §3.2) */
  threadId?: string;
  prompt: string;
  model: string;
  /** Max cumulative tokens (input + output) for this run — overrides
   *  spec.tokenBudget / config (§2.1 safety cap). Flows to the worker
   *  via RunJob.tokenBudget. */
  tokenBudget?: number;
}
```

### 3.1 Spec types

The factory args are **the `streamText` / `generateText` call options, minus the platform-owned keys**:

```typescript
// Everything streamText accepts, except what the platform sets per-run.
// onChunk / onFinish ARE allowed — the platform chains them (§4).
export type StreamTextAgentSpec = {
  name: string;   // unique handle key — the queue dispatch key (§5)

  /** Registry key for this agent's model — resolved via
   *  `AgentCore.resolveModel` (instance + contextWindow). */
  model?: string;

  /** Opt-in subagent delegation (§2.7). The platform constructs the
   *  run-scoped spawnSubagent tool (semaphore, depth, ports) — users cannot
   *  build it themselves. `true` = defaults; an object configures the
   *  spawned subagents (flavor, default model, extra tools). Default: off. */
  subagents?: boolean | SubagentsConfig;

  /** Default per-run token budget (input + output) for runs dispatched to
   *  this handle. Per-run `input.tokenBudget` wins; `undefined` = unbounded
   *  apart from `maxSteps`. */
  tokenBudget?: number;
} & Omit<Parameters<typeof streamText>[0],
    'model' | 'messages' | 'prompt' | 'system' | 'abortSignal'
    | 'maxSteps' | 'onStepFinish' | 'onError'> & {
  /** `system` is allowed here (static persona); per-run system is not. */
  system?: string;
  tools?: ToolSet;               // user tools — platform wraps HITL (§2.5). Nothing is injected unless subagents is enabled.
  onChunk?: Parameters<typeof streamText>[0]['onChunk'];   // chained after platform persistence
  onFinish?: Parameters<typeof streamText>[0]['onFinish']; // chained after platform finalize
};

export type GenerateTextAgentSpec = {
  name: string;
  model?: string;   // registry key — resolved via resolveModel (§3.3)
  subagents?: boolean | SubagentsConfig;
  tokenBudget?: number;
} & Omit<Parameters<typeof generateText>[0],
    'model' | 'messages' | 'prompt' | 'abortSignal' | 'onFinish'> & {
  tools?: ToolSet;
  onFinish?: Parameters<typeof generateText>[0]['onFinish']; // chained after platform finalize
};
```

```typescript
// ports/runtime.ts — delegation config (§2.7)
export interface SubagentsConfig {
  /** Generation flavor for spawned subagents — picks the nested loop the
   *  platform runs for them (§4). Default: 'stream-text'. A 'generate-text'
   *  child publishes only SUBAGENT_STARTED / SUBAGENT_COMPLETED /
   *  SUBAGENT_FAILED — no chunk stream. */
  kind?: 'stream-text' | 'generate-text';

  /** Registry key used when a delegation call omits `model`. */
  model?: string;

  /** Extra tools merged into every spawned subagent's toolset
   *  (HITL-wrapped identically to the parent's tools). */
  tools?: ToolSet;
}
```

**Ownership rule (the whole point of the abstraction):**

| Key | Owner | Why |
| :--- | :--- | :--- |
| `model` | **registry key on spec/run; resolved via `AgentCore.resolveModel`** → `{ instance, contextWindow }` — the instance executes, the context window feeds §2.6 |
| `messages` / `prompt` | **platform** | built from thread history + compaction (§2.6); subagents get the brief only (§2.7) |
| `abortSignal` | **platform** | wired to the stop poller (§2.1) |
| `maxSteps` | **config** (`maxSteps`, §2.1 safety cap) | |
| `tools` | **user + platform** | user-supplied set; platform wraps `requiresConfirmation` tools. `spawnSubagent` is **opt-in** (`subagents` config) — never injected silently |
| `onChunk` / `onFinish` | **platform + user** | platform persists, bills, and publishes first, **then chains the user's callback** — replacing it would silently drop user code |
| `system`, `temperature`, `toolChoice`, … | **user (spec)** | the agent's identity and behavior |

Platform keys win: user args are spread first, platform assignments last. A user cannot opt out of persistence, token attribution, or the stop signal.

---

## 4. What `execute()` Does With the Bound Args

```typescript
export async function execute(
  deps: RuntimePorts,
  agent: RegisteredAgent,                 // { name, kind, spec, args } resolved from the registry
  input: { threadId: string; model: string; tokenBudget?: number },
): Promise<'executed' | 'lock-conflict'> {
  const abort = new AbortController();

  // 1. Per-thread run lock (§2.8, §3.4) — SET NX + lease
  const locked = await deps.kv.set(runLockKey(input.threadId), randomUUID(), {
    onlyIfNotExists: true,
    exSeconds: deps.config.runLockLeaseSeconds,
  });
  if (!locked) return 'lock-conflict';    // duplicate delivery: nothing executed

  // 2. Token budget (§2.1 safety cap) — precedence: execute input → spec →
  //    config. Tracked cumulatively on every onStepFinish; the loop breaks
  //    BEFORE the next step once spent. This is NOT a user stop.
  const tokenBudget = input.tokenBudget
    ?? agent.spec.tokenBudget
    ?? deps.config.tokenBudget;          // undefined = unbounded apart from maxSteps
  let tokensUsed = 0;
  let budgetExceeded = false;

  // Delegation config (§3.2): false/undefined = off; true = defaults
  const subCfg: SubagentsConfig =
    agent.spec.subagents === true ? {} : (agent.spec.subagents ?? {});

  const controlPoll = setInterval(/* stop poller (§2.1) — aborts on CANCELLED */);

  try {
    // 3. Durable history + compaction (§2.6) — the budget uses the resolved
    //    model's contextWindow (resolveModel, §3.3) over config.nativeWindows
    const history = await compactContext(deps, input.threadId, input.model);

    // 4. Dispatch on the bound flavor — user args spread FIRST,
    //    platform assignments LAST (ownership rule, §3.1)
    if (agent.kind === 'stream-text') {
      // Model resolution (§3.3): registry key → { instance, contextWindow }
      const model = deps.resolveModel(input.model);

      const result = streamText({
        ...agent.args,                                   // user: system, temperature, toolChoice…
        model: model.instance(),                         // platform: resolved provider instance
        messages: history.map(toCoreMessage),            // platform: durable history
        tools: withHitl(deps, input.threadId, {          // platform: HITL wrap (§2.5) over the user's set.
          ...(agent.args.tools ?? {}),                   // spawnSubagent is added ONLY when the
          ...(agent.spec.subagents ? {                   // spec opts in (§2.7) — never silently
            spawnSubagent: spawnSubagentTool({
              threadId, depth: 0, sem, ports: deps,
              kind: subCfg.kind ?? 'stream-text',        // streamText OR generateText nested loop
              defaultModel: subCfg.model,                // used when delegation omits model
              tools: subCfg.tools,                       // extra tools merged into children
            }),
          } : {}),
        }),
        abortSignal: abort.signal,                       // platform: stop signal
        maxSteps: deps.config.maxSteps,                  // platform: safety cap
        onStepFinish: (step) => {
          agent.args.onStepFinish?.(step);               // user callback still fires
          // 5. Budget tracking — break the loop BEFORE the next step
          tokensUsed += countTokens(step.usage);         // NaN-guarded input + output (§finalize)
          if (tokenBudget && tokensUsed >= tokenBudget) {
            budgetExceeded = true;
            abort.abort();                               // not a user stop — see finalize below
          }
        },
        onChunk: async (para) => {
          // One canonical path for every client: durable log + live Pub/Sub (§2.1, §2.2)
          await publish(deps, input.threadId, 'CHUNK', para.chunk);
          agent.args.onChunk?.(para);                    // user callback still fires
        },
        onFinish: async (finishParams) => {
          await finalize(deps, agent, input, finishParams, abort, {
            budgetExceeded, tokensUsed,
          });
          agent.args.onFinish?.(finishParams);           // user callback still fires
        },
      });
      await result.text;
    } else {
      const result = await generateText({
        ...agent.args,                                   // user: model instance, system, temperature, toolChoice…
        messages: history.map(toCoreMessage),            // platform: durable history
        tools: withHitl(deps, input.threadId, {          // platform: HITL wrap (§2.5) over the user's set.
          ...(agent.args.tools ?? {}),                   // spawnSubagent only when spec opts in (§2.7)
          ...(agent.spec.subagents ? {
            spawnSubagent: spawnSubagentTool({
              threadId, depth: 0, sem, ports: deps,
              kind: subCfg.kind ?? 'stream-text',
              defaultModel: subCfg.model,
              tools: subCfg.tools,
            }),
          } : {}),
        }),
        abortSignal: abort.signal,                       // platform: stop signal
        maxSteps: deps.config.maxSteps,                  // platform: safety cap
        onStepFinish: (step) => {
          agent.args.onStepFinish?.(step);               // user callback still fires
          tokensUsed += countTokens(step.usage);         // budget tracking — break BEFORE next step
          if (tokenBudget && tokensUsed >= tokenBudget) {
            budgetExceeded = true;
            abort.abort();
          }
        },
        onFinish: async (finishParams) => {
          await finalize(deps, agent, input, finishParams, abort, {
            budgetExceeded, tokensUsed,
          });
          agent.args.onFinish?.(finishParams);           // user callback still fires
        },
      });

      // One-shot flavor: no CHUNK stream — publish the final text as one event
      await publish(deps, input.threadId, 'TEXT_RESULT', { text: result.text });
    }

    return 'executed';
  } finally {
    clearInterval(controlPoll);
    await deps.kv.del(runLockKey(input.threadId));
  }
}
```

`finalize` (the shared `onFinish` body) is unchanged from §5.6 in structure, with three changes:

1. **Persist `response.messages` before the state transition** (assistant turns, tool calls, tool results) so redrives, HITL resumes, and replay always see a valid history (§2.2, §2.8).
2. **NaN-guard the usage counters.** Some OpenAI-compatible providers omit streaming usage; the AI SDK represents those as `NaN`. Clamp to `0` — optional metering must never keep a successfully completed run stuck in `RUNNING`.
3. **Token attribution, not pricing.** A model may be any provider instance (`createOpenAI(…)`, `createAnthropic(…)`, `createDeepSeek(…)`), so the platform records **input / cached-input / output tokens** and leaves USD pricing to a downstream concern computed over the recorded counters (spec §4 migration item).

```typescript
export async function finalize(
  deps: RuntimePorts,
  agent: RegisteredAgent,
  input: { threadId: string; model: string },
  finishParams: { usage: LanguageModelUsage; finishReason: FinishReason; response: ... },
  abort: AbortController,
  budget: { budgetExceeded: boolean; tokensUsed: number },
): Promise<void> {
  const { threadId } = input;
  const u = finishParams.usage;

  // Some providers omit streaming usage — the SDK reports NaN for those.
  // Never let optional metering keep a completed run stuck in RUNNING.
  // Token attribution is TOTAL TOKENS USED — input + cached + output.
  const totalTokens = countTokens(u);

  // Persist the completed assistant turn(s) — including tool calls and tool
  // results — BEFORE the state transition (§2.2, §2.8). On a budget-broken
  // run the recorded usage is partial by definition: the budget was spent.
  await deps.storage.messages.append(threadId, { /* response.messages, §5.6 */ });

  // Token attribution: total tokens used — that is all (§4 pricing is
  // downstream over these counters, if ever needed)
  await deps.storage.usage.record(threadId, {
    agentId: agent.name,
    totalTokens,
  });

  // Stop classification (§2.1). A budget break is NOT a user stop — the run
  // still completes successfully with the tokens it managed to spend.
  const finalState: ExecutionState = abort.signal.aborted && !budget.budgetExceeded
    ? 'CANCELLED' : 'COMPLETED';
  const stopReason = budget.budgetExceeded ? 'token_budget'
    : abort.signal.aborted ? 'cancelled'
    : finishParams.finishReason === 'tool-calls'
      ? 'max_steps' // safety cap hit (§2.1)
      : 'completed';

  await deps.kv.set(`agent:state:${threadId}`, finalState);
  await deps.storage.threads.setState(threadId, finalState);
  await publish(deps, threadId, 'STATE_CHANGE', {
    state: finalState, stopReason, tokensUsed: budget.tokensUsed,
  });
}

/** Total tokens used — input + cached + output, NaN-guarded. Providers that
 *  omit streaming usage report NaN; optional metering must never keep a
 *  completed run stuck in RUNNING. */
function countTokens(usage: LanguageModelUsage): number {
  const pick = (v: number | undefined) => (Number.isFinite(v) ? v : 0);
  return pick(usage.inputTokens ?? usage.promptTokens)
       + pick(usage.cachedInputTokens)
       + pick(usage.outputTokens ?? usage.completionTokens);
}
```

**Callback chaining order:** platform work (persist → attribute tokens → publish) runs first, then the user's `onChunk`/`onFinish` fires with the same parameters. A user callback that throws is its own problem — it must not prevent the state transition, so `finalize` completes before `args.onFinish` is invoked.

#### Token Budget Semantics

- **Precedence:** `execute input.tokenBudget` → `spec.tokenBudget` → `config.tokenBudget` (default `undefined` = unbounded apart from `maxSteps`).
- **Counted:** NaN-guarded `input + output` per step, accumulated across steps (`onStepFinish` fires once per step, including tool-call steps). `cachedInputTokens` are a subset of input on providers that report them — never double-counted.
- **Break:** the shared abort signal fires at a step boundary, so generation stops *before* the next step — the in-flight step completes and its tokens count. The run finalizes `COMPLETED` with `stopReason: 'token_budget'`; `STATE_CHANGE.tokensUsed` reports the actual spend.
- **Not a user stop:** the stop poller and the budget tracker are separate triggers of the same signal — `budgetExceeded` disambiguates them in `finalize`, so a budget break can never be misclassified as `cancelled` (or vice versa).
- **Partial is correct:** on a budget break the recorded usage is partial by definition — the budget was spent.
- **Queue path:** `run({ …, tokenBudget })` flows through `RunJob.tokenBudget` to the worker's `executeWithPolicy`.

`executeWithPolicy` wraps `execute` with the §2.8 policy exactly as today: redrive `< maxAttempts`, else finalize `FAILED` on both homes; a user stop is never retried; the attempt counter resets on success.

### 4.1 Flavor differences at a glance

| | `stream-text` | `generate-text` |
| :--- | :--- | :--- |
| SDK call | `streamText` | `generateText` |
| Live events | `CHUNK` per delta (§2.2 fan-out) | single `TEXT_RESULT` on completion |
| Typical UI | chat | summarization, extraction, batch |
| Stop | aborts mid-generation | aborts the awaited call |
| Token budget | tracked on `onStepFinish`, breaks before the next step | same |
| Everything else | identical: lock, history, HITL, token attribution, finalize, redrive | |

---

## 5. Queue Dispatch With Multiple Agents

`RunJob` gains the handle key:

```typescript
export interface RunJob {
  threadId: string;
  model: string;
  agent: string;       // registered handle name — the worker resolves it via runtime.getAgent()
  tokenBudget?: number; // flows from RunInput to the worker's executeWithPolicy (§2.1)
}
```

- `chatAgent.run({ prompt, model })` persists + enqueues `{ threadId, model, agent: 'chat' }`.
- The worker route resolves the handle once and executes with the policy. That behavior is **part of `AgentCore`** — the route is a transport shell:

```typescript
// app/api/queue/agent-run/route.ts (worker, §5.6) — transport shell only
export const POST = verifySignatureApprouter(async (req: NextRequest) => {
  const job = await req.json();
  waitUntil(runtime.worker.handleJob(job));   // resolve handle → executeWithPolicy
  return NextResponse.json({ accepted: true });
});
```

Everything the route used to do by hand lives in the package: handle resolution (unknown agent → `{ accepted: false, reason: 'unknown-agent' }`), `tokenBudget` threading, and idempotency under at-least-once delivery (the per-thread run lock, §3.4). Only two concerns stay at the HTTP edge, where they belong: **signature verification** (QStash-specific transport trust) and **JSON parsing** (framework-specific).

### 5.1 No-HTTP dispatch profile: Postgres

For long-lived deployments (self-hosted Bun server, docker-compose), dispatch doesn't need HTTP at all — the job is a **row in Postgres**, the same database as the event log. Same `Queue` port, same `worker.handleJob` consumption:

- **Dispatch = INSERT.** Durable by commit: a deploy or crash can never lose an accepted run.
- **Wake-up = `pg_notify`.** Postgres's pub/sub is the *doorbell only* — fire-and-forget. Durable delivery comes from the row; a lost chime costs seconds of latency, never the job.
- **Claim = `FOR UPDATE SKIP LOCKED`.** Exactly one worker per row, natively — multi-instance distributes rows with no coordination.
- **Crash recovery = stale predicate.** A worker that died mid-run leaves its claim to age out and become claimable again.

The job table (add to the reference schema, §2.4):

```prisma
model AgentJob {
  id        BigInt    @id @default(autoincrement())
  payload   Json                     // RunJob: { threadId, model, agent, tokenBudget }
  runAt     DateTime  @default(now()) // visibility time (redrive backoff, §2.8)
  pickedBy  String?                  // NULL = unclaimed
  pickedAt  DateTime?
  createdAt DateTime  @default(now())

  @@index([runAt])
}
```

Dispatch adapter (the `Queue` port):

```typescript
// src/adapters/postgres.ts
import { randomUUID } from 'node:crypto';
import type { RunJob } from '../core/types.js';
import type { Queue } from '../ports/queue.js';

const WORKER_ID = randomUUID();
const STALE_AFTER_MS = 40 * 60_000; // must exceed the longest possible run —
                                    // same rule as the run-lock lease (§2.8);
                                    // covers parked HITL waits (§2.5)

export class PostgresQueue implements Queue {
  constructor(private readonly prisma: PrismaLike) {}

  // Dispatch = INSERT (durable) + pg_notify (doorbell)
  async enqueue(job: RunJob): Promise<void> {
    await this.prisma.$executeRaw`
      INSERT INTO agent_jobs (payload) VALUES (${JSON.stringify(job)}::jsonb)`;
    await this.prisma.$executeRaw`SELECT pg_notify('agent_jobs', '')`;
  }

  /** Atomic claim — exactly one worker per row. A row claimed by a worker
   *  that died becomes claimable again once its pick ages past STALE_AFTER_MS. */
  async claim(): Promise<{ id: bigint; payload: RunJob } | null> {
    const rows = await this.prisma.$queryRaw<
      { id: bigint; payload: RunJob }[]
    >`
      UPDATE agent_jobs SET picked_by = ${WORKER_ID}, picked_at = now()
      WHERE id = (
        SELECT id FROM agent_jobs
        WHERE picked_by IS NULL AND run_at <= now()
        ORDER BY run_at
        LIMIT 1
        FOR UPDATE SKIP LOCKED
      )
      RETURNING id, payload`;
    return rows[0] ?? null;
  }

  async complete(id: bigint): Promise<void> {
    await this.prisma.agentJob.delete({ where: { id } });
  }

  /** Release a row the worker could not finish — visible again after backoff */
  async release(id: bigint, backoffMs: number): Promise<void> {
    await this.prisma.$executeRaw`
      UPDATE agent_jobs
      SET picked_by = NULL, run_at = now() + (${backoffMs} || ' milliseconds')::interval
      WHERE id = ${id}`;
  }
}
```

Consumption loop — claims a row, executes through `AgentCore`, no HTTP anywhere:

```typescript
// started once at boot (instrumentation.ts / lib/runtime.ts)
export function startPostgresWorker(deps: RuntimePorts, opts?: { pollMs?: number }) {
  const dispatch = new PostgresQueue(deps.storage['prisma']); // or a dedicated client
  let running = true;

  void (async () => {
    while (running) {
      const job = await dispatch.claim();
      if (!job) {
        await sleep(opts?.pollMs ?? 2_000); // fallback cadence; pg_notify wakes earlier
        continue;
      }
      try {
        // Engine: run lock + §2.8 policy + token budget (§5.6)
        await runtime.worker.handleJob(job.payload);
        await dispatch.complete(job.id);
      } catch (err) {
        // executeWithPolicy already applied the §2.8 policy — a throw here is
        // a transport problem: release the row for stale-reclaim
        await dispatch.release(job.id);
      }
    }
  })();

  return () => { running = false; };
}
```

Guarantees, compared to the QStash profile: both are **at-least-once with idempotent consumption** (the engine's run lock makes double dispatch a no-op, §3.4). Differences: the Postgres profile needs a live worker process (it *is* your app process on a long-lived deployment), redrive backoff is a row update instead of a queue-level delay, and crash recovery ages out through the stale predicate instead of QStash redelivery. The §2.8 failure policy itself — redrive, exhausted → `FAILED`, never retry a stop — is identical in both.

Backward compatibility: a job without `agent` dispatches to the **default handle** (`config.defaultAgent`, defaulting to the first registered `stream-text` agent).

---

## 6. Usage

```typescript
const agent = setupAgentCore({
  storage: new PrismaStorage(prisma),
  bus: new RedisBus(redis),
  queue: new QStashQueue(client, { url: `${APP_URL}/api/queue/agent-run` }),
  kv: new UpstashKv(redis),

  // Models can come in any shape — config files, a database, a hardcoded
  // map, provider SDKs. The platform only ever sees the resolved result:
  // { instance, contextWindow }.
  resolveModel: (modelName) => {
    const m = MY_MODELS[modelName];               // your shape, your lookup
    if (!m) throw new Error(`Unknown model: ${modelName}`);
    return {
      instance: () => m.create(),                 // the real model object, lazily
      contextWindow: m.contextWindow,             // feeds §2.6 compaction
    };
  },
});

// anywhere on your side — models are plain provider instances in your shape:
const MY_MODELS = {
  'gpt-4o': {
    provider: () => createOpenAI({ apiKey: OPENAI_KEY })('gpt-4o'),
    contextWindow: 128_000,
  },
  'deepseek-chat': {
    provider: () => createDeepSeek({ apiKey: DEEPSEEK_KEY })('deepseek-chat'),
    contextWindow: 128_000,
  },
};
```

// A streaming chat agent — model pinned at the spec level, overridable per run
const chat = agent.createStreamTextAgent({
  name: 'chat',
  model: 'gpt-4o',                       // spec default; run input.model wins if provided
  system: 'You are a concise product assistant.',
  temperature: 0.3,
  subagents: {                           // opt-in delegation with a configured flavor
    kind: 'stream-text',                 // nested loop: streamText (or generateText)
    model: 'gpt-4o-mini',                // default model for delegations
  },
});
await chat.run({ prompt: 'Say hello' });                    // uses the spec default model
// worker: await chat.executeWithPolicy({ threadId, model: 'gpt-4o' });

// A one-shot summarizer — same platform, different flavor, cheap model pinned
const summarizer = agent.createGenerateTextAgent({
  name: 'summarizer',
  model: 'gpt-4o-mini',
  system: 'Summarize threads into a dense brief.',
});
await summarizer.execute({ threadId, model: 'gpt-4o-mini' });

// Platform services, unchanged
const snap = await agent.getThreadSnapshot(threadId);
await agent.hitl.respond({ threadId, toolCallId, approved: true, payload });
const unsub = await agent.events.subscribe(threadId, handler);
await chat.stop(threadId);
```

---

## 7. Migration From `createAgentRuntime`

| Today (§3.3) | After |
| :--- | :--- |
| `runtime.run(input)` | `handle.run(input)` — same behavior, job now tagged with the handle name |
| `runtime.stop(threadId)` | `handle.stop(threadId)` (or a future `agent.stop` on the runtime) |
| `runtime.engine.execute(input)` | `handle.execute(input)` → returns `'executed' \| 'lock-conflict'` |
| `runtime.engine.executeWithPolicy(input)` | `handle.executeWithPolicy(input, policy?)` |
| `runtime.hitl` / `runtime.events` | unchanged on `AgentCore` |
| — new — | `AgentCore.resolveModel(key)` → `ResolvedModel { instance, contextWindow }` — **user-provided function**: models can live in any shape on the consumer side |
| — new — | `runtime.getThreadSnapshot(threadId)` |
| — new — | `runtime.createStreamTextAgent` / `createGenerateTextAgent` / `getAgent` |
| `RunJob { threadId, model }` | `RunJob { threadId, model, agent, tokenBudget }` (missing `agent` → default handle) |
| worker route body (resolve handle → policy) | `runtime.worker.handleJob(job)` — the route becomes a transport shell (verify + parse + call) |
| `TokenUsage { model, promptTokens, completionTokens, costUSD }` | `{ agentId, totalTokens }` — total tokens used; USD/credit pricing (spec §4) becomes a downstream concern computed over the recorded counters |

Non-breaking rollout: ship `setupAgentCore` alongside `createAgentRuntime` (alias) for one minor, migrate the example app, then drop the old factory.

### 7.1 Why `setupAgentCore` and not just a rename

The name change signals the split: the factory no longer *is* the agent — it **sets up the platform core** and hands out agent handles. "Setup" also leaves room for future registration-style config (`registerTool`, `registerBillingHook`) without widening the runtime interface.
