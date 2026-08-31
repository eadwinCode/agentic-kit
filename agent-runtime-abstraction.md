# Design Doc: `setupAgentCore` — Agent Flavors over Durable Platform Services

**Status:** Draft (v2) — corrected implementation target for the next compatible release of `@agent/core`.
**Builds on:** [agent-platform-technical-spec.md](./agent-platform-technical-spec.md).

> This document replaces the v1 draft. The existing `createAgentRuntime`, engine,
> queue job, blocking HITL flow, and storage schema remain legacy behavior until
> the complete migration described here lands. Do not ship this refactor as a
> piecemeal mix of old and new execution semantics.

---

## 1. Decision Summary

The runtime is split into three layers:

1. **`AgentCore`** owns shared platform services: threads, durable runs, storage,
   model resolution, queue dispatch, stop, HITL, events, compaction, usage, and
   snapshots.
2. **Agent handles** bind a stable name/version to one generation flavor and its
   user-owned arguments. Public callers normally use `handle.run()`.
3. **The worker executor** is an internal package surface. It resolves a queued
   job, atomically claims its durable run, executes the registered handle, and
   finalizes exactly once.

The two first-party flavors are:

- `stream-text`: calls `streamText` and emits live `CHUNK` events.
- `generate-text`: calls `generateText`, then explicitly persists and publishes
  its result. It uses a custom `onResult` hook because the installed AI SDK does
  not provide `generateText.onFinish`.

Every accepted request creates a durable top-level `Run`. Queue jobs carry its
`runId`, agent name, and agent version. A thread lock prevents simultaneous work;
the durable run claim prevents delayed at-least-once deliveries from executing a
completed run again.

---

## 2. Goals and Non-goals

### Goals

- Support multiple generation flavors without duplicating platform lifecycle code.
- Preserve type inference for user tools and callbacks.
- Make queue execution idempotent across concurrent and delayed redelivery.
- Resolve every model to a stable serializable key plus a provider instance.
- Make HITL resumable without keeping a serverless process parked.
- Keep user callbacks outside the durable correctness boundary.
- Return a compact, consistent thread view suitable for UI hydration and SSE resume.
- Provide a real compatibility facade for the legacy API until the next major release.

### Non-goals

- The core does not price provider usage in USD. It records normalized token usage.
- The core does not guarantee an exact hard cumulative token ceiling mid-step.
  Token budgets stop at verified step boundaries and may overshoot by one step.
- The core does not require a specific physical database layout. Relational,
  document, and custom adapters may store the same logical entities differently.

---

## 3. Public Surface

The returned value is called `core`, because it represents the shared platform,
not one particular agent.

```typescript
export interface AgentCore {
  /** Resolve a registry key to the stable identity and provider instance used
   * by execution, compaction, usage attribution, and persisted run metadata. */
  resolveModel(modelName: string): ResolvedModel;

  /** A single nested client view. Messages are paginated and internal context
   * envelopes are excluded unless an authorized internal caller opts in. */
  getThreadSnapshot(
    threadId: string,
    options?: ThreadSnapshotOptions,
  ): Promise<ThreadSnapshot | null>;

  /** Platform-wide cancellation. A handle-level stop method delegates here. */
  stop(threadId: string): Promise<StopResult>;

  hitl: {
    respond(input: RespondInput): Promise<RespondResult>;
    expire(input: { threadId: string; runId: string; toolCallId: string }): Promise<boolean>;
  };

  events: {
    since(threadId: string, sinceSeq: number): Promise<AgentEvent[]>;
    subscribe(threadId: string, handler: (event: AgentEvent) => void): Promise<() => void>;
  };

  /** Registration is deterministic and process-local. Duplicate name/version
   * pairs throw. Multiple versions may coexist while old jobs drain. */
  createStreamTextAgent<const TOOLS extends ToolSet = ToolSet>(
    spec: StreamTextAgentSpec<TOOLS>,
  ): StreamTextAgent<TOOLS>;

  createGenerateTextAgent<const TOOLS extends ToolSet = ToolSet>(
    spec: GenerateTextAgentSpec<TOOLS>,
  ): GenerateTextAgent<TOOLS>;

  /** Validate default/subagent references and freeze the registry. Worker
   * construction and run dispatch require a sealed core. */
  sealRegistrations(): void;

  /** Read-only lookup for diagnostics. Worker execution uses the internal
   * worker package and always resolves name + version from the queued job. */
  getAgent(ref: AgentRef): AgentHandle | null;
}

export function setupAgentCore(options: AgentCoreOptions): AgentCore;
```

`execute()` and `executeWithPolicy()` are deliberately absent. Worker code uses
the internal `@agent/core/worker` surface:

```typescript
import { createAgentWorker } from '@agent/core/worker';

core.sealRegistrations();
const worker = createAgentWorker(core);
await worker.execute(job);
```

The worker subpath is intended for authenticated queue consumers, not browser,
route-handler, or application-domain code.

### 3.1 Agent handles

```typescript
export interface AgentHandle {
  readonly name: string;
  readonly version: string;
  readonly kind: 'stream-text' | 'generate-text';

  /** Persist input and a durable run, then dispatch that run to the queue. */
  run(input: RunInput): Promise<RunResult>;

  /** Convenience delegate to core.stop(threadId). */
  stop(threadId: string): Promise<StopResult>;
}

export interface AgentRef {
  name: string;
  version: string;
}

export interface RunInput {
  threadId?: string;
  prompt: string;
  /** Serializable registry key. Omit to use the agent default. */
  model?: string;
  /** Per-request preference, clamped by the platform maximum. */
  tokenBudget?: number;
  /** Optional caller key for idempotent HTTP retries before a runId is returned. */
  idempotencyKey?: string;
}

export interface RunResult {
  accepted: boolean;
  threadId: string;
  runId?: string;
  state?: RunState;
  error?: string;
}
```

Only one top-level run may mutate a thread at a time. Different agent handles may
process the same thread sequentially, but every message, event, usage row, and
artifact remains attributed to its top-level `runId`, producing `agentName`,
and optional `subagentRunId`.

`core.stop(threadId)` atomically cancels the active root run and all unfinished
descendants, publishes attributed cancellation events, and signals any currently
executing worker. A later delivery for any cancelled run is a durable no-op.

---

## 4. Model Registry and Resolution

Provider instances never cross the queue or persistence boundary. They are
registered under stable keys during core setup.

```typescript
export interface ModelDefinition {
  instance: LanguageModel;
  contextWindow?: number;
}

export type ModelRegistry = Record<string, LanguageModel | ModelDefinition>;

export interface ResolvedModel {
  key: string;
  instance: LanguageModel;
  contextWindow?: number;
}

export interface AgentCoreOptions extends RuntimeOptions {
  models: ModelRegistry;
  /** Required. Never infer this from registration order. */
  defaultAgent: AgentRef;
  tokenBudget?: {
    default?: number;
    /** Security/billing ceiling that no agent or request may exceed. */
    max?: number;
  };
  billingPreCheck?: (
    input: BillingPreCheckInput,
  ) => Promise<{ ok: boolean; error?: string }>;
}

export interface BillingPreCheckInput {
  threadId: string;
  agentName: string;
  agentVersion: string;
  modelKey: string;
  requestedTokenBudget?: number;
}
```

`core.resolveModel(modelName)`:

1. Looks up the exact key in `options.models`.
2. Normalizes a bare `LanguageModel` into `ResolvedModel`.
3. Throws `UnknownModelError` when the key does not exist. It never silently
   substitutes another provider or model.

Agent execution resolves model precedence as:

```text
allowed run input.model → agent spec.model
```

Every agent spec must define a default `model` key. A per-run override is accepted
only when it appears in the agent's `allowedModels`; otherwise `run()` rejects the
request before persistence or billing reservation. When `allowedModels` is
omitted, only the spec default is allowed.

Compaction receives the resolved model key and context window. Usage records the
same key. This removes the previous contradiction where a provider instance was
passed to generation while a separate string was used for compaction and billing.

---

## 5. Agent Specifications

The specs expose user-owned AI SDK arguments while excluding platform-owned
arguments. Generic `TOOLS` preserves tool names in `toolChoice`, step results,
and callbacks.

```typescript
type StreamArgs<TOOLS extends ToolSet> = Parameters<typeof streamText<TOOLS>>[0];
type GenerateArgs<TOOLS extends ToolSet> = Parameters<typeof generateText<TOOLS>>[0];

type PlatformStreamKeys =
  | 'model'
  | 'messages'
  | 'prompt'
  | 'abortSignal'
  | 'maxSteps'
  | 'tools'
  | 'onChunk'
  | 'onFinish'
  | 'onStepFinish'
  | 'onError';

type PlatformGenerateKeys =
  | 'model'
  | 'messages'
  | 'prompt'
  | 'abortSignal'
  | 'maxSteps'
  | 'tools'
  | 'onStepFinish';

export interface SubagentPolicy {
  /** Registered agent used for delegated work. */
  agent: AgentRef;
  /** Explicit model allow-list for model-selected delegation. */
  allowedModels?: readonly string[];
  /**
   * User-defined tools from the referenced agent that delegated runs may use.
   * Omission grants no user-defined tools; platform-reserved tools are governed
   * separately by the referenced agent's own policies.
   */
  allowedTools?: readonly string[];
  maxDepth?: number;
  maxConcurrent?: number;
  resultCapChars?: number;
}

export interface CompletedAgentResult {
  runId: string;
  text: string;
  finishReason: string;
  stopReason: StopReason;
  usage: NormalizedUsage;
  artifactId?: string;
}

export interface AgentHooks<CHUNK = unknown, RESULT = unknown> {
  /** Observational hooks. Failures are recorded but never fail or redrive a run. */
  onChunk?: (chunk: CHUNK) => void | Promise<void>;
  onResult?: (result: RESULT) => void | Promise<void>;
  onError?: (error: unknown) => void | Promise<void>;
}

export type StreamTextAgentSpec<TOOLS extends ToolSet> = {
  name: string;
  /** Queued jobs pin this version to prevent deployment drift. */
  version: string;
  model: string;
  allowedModels?: readonly string[];
  tokenBudget?: number;
  system?: StreamArgs<TOOLS>['system'];
  tools?: TOOLS;
  subagents?: SubagentPolicy;
  hooks?: AgentHooks<
    Parameters<NonNullable<StreamArgs<TOOLS>['onChunk']>>[0],
    CompletedAgentResult
  >;
} & Omit<StreamArgs<TOOLS>, PlatformStreamKeys | 'system'>;

export type GenerateTextAgentSpec<TOOLS extends ToolSet> = {
  name: string;
  version: string;
  model: string;
  allowedModels?: readonly string[];
  tokenBudget?: number;
  system?: GenerateArgs<TOOLS>['system'];
  tools?: TOOLS;
  subagents?: SubagentPolicy;
  /** Conversation appends response messages; artifact stores a separate result. */
  output?: 'conversation' | 'artifact';
  hooks?: AgentHooks<never, CompletedAgentResult>;
} & Omit<GenerateArgs<TOOLS>, PlatformGenerateKeys | 'system'>;
```

### 5.1 Ownership rules

| Key | Owner | Rule |
| :--- | :--- | :--- |
| `model` | core + spec | Specs and jobs store a key; `core.resolveModel()` supplies the provider instance. |
| `messages` / `prompt` | core | Built from the run input, public thread history, and compaction. |
| `abortSignal` | core | Used for user stop and worker shutdown, not token-budget classification. |
| `maxSteps` | core config | Global safety limit; an agent may request a lower value in a future policy field. |
| `tools` | user + core | Tool implementations belong to the versioned agent spec. A parent subagent policy may only narrow the child's tools by name; user tools are HITL-wrapped and delegation is injected only by policy. |
| SDK lifecycle callbacks | core | Core persists and finalizes. User hooks run through `safeInvokeHook`. |
| `system`, `temperature`, `toolChoice`, provider options | spec | Stable agent behavior. |

If `subagents` is configured, registration rejects a user tool named
`spawnSubagent`. Platform-owned reserved names never overwrite user tools
silently.

### 5.2 Registration rules

- Registration must happen at module initialization in every API and worker process.
- `(name, version)` is unique within a core instance. Registering the same pair
  twice throws; multiple versions may coexist while queued jobs drain.
- `version` is explicit and immutable for queued work.
- A deployment must retain handlers for still-queued versions or deliberately
  mark those runs `FAILED` with `stopReason: 'unknown_agent_version'`.
- `core.sealRegistrations()` validates `defaultAgent`, every subagent reference,
  model key, model allow-list, subagent tool allow-list, and reserved tool name,
  then freezes registration. Every `allowedTools` entry must exist on the exact
  referenced child agent version.
- Dispatch and worker construction fail fast when the core is not sealed.
- Registration order never changes dispatch behavior.

---

## 6. Durable Data Model

### 6.1 Thread view versus physical storage

The public API presents thread metadata and messages as one nested object. The
runtime abstraction does **not** require two physical tables, nor does it require
embedding all messages into one thread row.

Relational reference adapters should normally keep `Thread` and `Message`
normalized because message history is unbounded, append-heavy, and paginated.
Embedding it into one relational row creates write amplification, row contention,
and practical size limits. Document adapters may embed a bounded recent window
and archive older messages. Custom adapters may choose any layout that satisfies
the logical port and cursor guarantees.

### 6.2 Logical entities

```typescript
export type RunState =
  | 'QUEUED'
  | 'RUNNING'
  | 'WAITING_FOR_INPUT'
  | 'WAITING_FOR_SUBAGENT'
  | 'RETRYING'
  | 'COMPLETED'
  | 'FAILED'
  | 'CANCELLED';

export type StopReason =
  | 'completed'
  | 'user_cancelled'
  | 'token_budget'
  | 'max_steps'
  | 'input_expired'
  | 'unknown_agent'
  | 'unknown_agent_version'
  | 'enqueue_failed'
  | 'execution_failed';

export interface RunDTO {
  id: string;
  threadId: string;
  /** Self for a top-level run; inherited from the parent for delegated runs. */
  rootRunId: string;
  parentRunId: string | null;
  agentName: string;
  agentVersion: string;
  kind: 'stream-text' | 'generate-text';
  modelKey: string;
  depth: number;
  state: RunState;
  stopReason?: StopReason;
  tokenBudget?: number;
  tokensUsed: number;
  attempt: number;
  checkpointVersion: number;
  createdAt: Date;
  updatedAt: Date;
}

export interface MessageDTO {
  id: string;
  threadId: string;
  /** Root/top-level run that owns the conversation turn. */
  runId: string;
  /** Registered handle that produced this message. */
  agentName: string;
  agentVersion: string;
  /** Exact delegated RunDTO.id; null for the top-level agent. */
  subagentRunId: string | null;
  role: MessageRole;
  visibility: 'public' | 'internal';
  content: unknown;
  createdAt: Date;
}

export interface NormalizedUsage {
  inputTokens: number;
  cachedInputTokens: number;
  outputTokens: number;
}

export interface UsageDTO extends NormalizedUsage {
  id: string;
  threadId: string;
  /** Root/top-level run. */
  runId: string;
  agentName: string;
  agentVersion: string;
  subagentRunId: string | null;
  modelKey: string;
  createdAt: Date;
}

export interface ArtifactDTO {
  id: string;
  threadId: string;
  /** Root/top-level run. */
  runId: string;
  agentName: string;
  agentVersion: string;
  subagentRunId: string | null;
  type: 'text' | 'json';
  value: unknown;
  createdAt: Date;
}

export interface AgentEvent {
  threadId: string;
  /** Root/top-level run. */
  runId: string;
  agentName: string;
  agentVersion: string;
  subagentRunId?: string | null;
  seq: number;
  type: AgentEventType;
  payload: unknown;
  createdAt: Date;
}
```

`agentName`, root/top-level `runId`, and exact delegated `subagentRunId` are
separate concepts. `agentId` is not reused for all three.

Top-level and delegated execution use the same `RunDTO`; a subagent is simply a
run with `parentRunId` and `depth > 0`.

### 6.3 Required storage semantics

Production storage adapters must support:

- Atomic creation of a queued run plus its user message and outbox record.
- Atomic `claimRun(runId, allowedStates, nextState)` compare-and-set.
- Idempotent message, usage, artifact, and terminal-state writes keyed by `runId`.
- Paginated public-message reads.
- Active/recent run reads and usage aggregation.
- A consistent snapshot high-water cursor, or stable IDs that let clients dedupe
  any overlap between snapshot and event replay.

The queue outbox is part of correctness. If dispatch fails after persistence, an
outbox worker retries it. A thread must not remain permanently `RUNNING` because
an external enqueue call threw.

---

## 7. Thread Snapshot and SSE Hydration

Thread metadata and messages are merged in the public view:

```typescript
export interface Page<T> {
  items: T[];
  nextCursor: string | null;
}

export interface PublicMessageDTO
  extends Omit<MessageDTO, 'visibility' | 'content'> {
  content: PublicMessageContent;
}

export interface ThreadSnapshot extends ThreadDTO {
  messages: Page<PublicMessageDTO>;
  activeRun: RunDTO | null;
  /** Greatest event already represented by this snapshot. */
  lastEventSeq: number;
  /** Only unfinished-run events needed to restore thinking, tools, and HITL. */
  activeEvents: AgentEvent[];
  usageSummary?: UsageSummaryDTO;
  recentRuns?: Page<RunDTO>;
  recentArtifacts?: Page<ArtifactDTO>;
}

export interface ThreadSnapshotOptions {
  messageCursor?: string;
  messageLimit?: number; // bounded by config; default 50
  include?: readonly ('usageSummary' | 'recentRuns' | 'recentArtifacts')[];
  /** Internal server callers only. Never expose model context by default. */
  includeInternalMessages?: boolean;
}
```

`CONTEXT_SUMMARY`, hidden system prompts, and internal tool bookkeeping are not
returned to ordinary clients. The model-context view and the public thread view
are different projections over the same logical message history.

Hydration order:

1. Fetch `ThreadSnapshot`.
2. Render `messages` and reconstruct only `activeEvents`.
3. Open SSE with `since=lastEventSeq`.
4. Dedupe by stable event/message/run IDs if the storage adapter permits overlap.

Completed per-token `CHUNK` events are eligible for retention pruning after the
final messages are durable. Status and audit events may use a longer retention
policy. The durable transcript, not an unbounded token event log, is the source
of completed conversation history.

---

## 8. Run Creation and Queue Dispatch

`handle.run()` performs:

1. Validate the agent, version, model allow-list, token budget, and billing policy.
2. Create a thread when `threadId` is absent.
3. Reject or atomically serialize against an active top-level run.
4. In one storage transaction:
   - append the public user message,
   - create `RunDTO(state: 'QUEUED')`,
   - set `thread.activeRunId`,
   - append a queue outbox record.
5. Dispatch the outbox record and publish `RUN_QUEUED`.
6. Return `{ accepted, threadId, runId, state: 'QUEUED' }`.

The billing/reservation hook receives the actual request dimensions rather than
only a thread ID:

```typescript
billingPreCheck({
  threadId,
  agentName: handle.name,
  agentVersion: handle.version,
  modelKey: resolvedModel.key,
  requestedTokenBudget: effectiveBudget,
});
```

This lets applications reject expensive model overrides or reserve credits
before the durable run is accepted.

The job is a versioned dispatch ticket:

```typescript
export interface RunJobV1 {
  schemaVersion: 1;
  runId: string;
  threadId: string;
  agentName: string;
  agentVersion: string;
  modelKey: string;
  tokenBudget?: number;
  /** Incremented for each durable HITL resume checkpoint. */
  checkpointVersion: number;
}
```

Jobs never contain provider instances, tool closures, prompts, or message history.
Those are resolved from the registered handle and durable run state.

### 8.1 Idempotent worker claim

The worker:

1. Validates the versioned job schema.
2. Resolves exact `agentName + agentVersion`.
3. Acquires the per-thread lease.
4. Atomically claims the matching run/checkpoint:
   `QUEUED | RETRYING → RUNNING`.
5. Returns a no-op for completed, cancelled, stale-checkpoint, or already-running jobs.
6. Executes and finalizes with idempotent writes.
7. Releases the lease.

An unknown agent/version is a durable non-retryable failure: the worker marks the
run `FAILED`, publishes the attributed reason, and acknowledges it. Returning a
bare 400 without updating the run would leave the UI stuck and may cause a queue
retry storm.

The lease prevents concurrency. The durable claim prevents sequential duplicate
delivery. Both are required.

Attempt counters live on `RunDTO`, not in a thread-wide key. A failure in one
agent run cannot consume the retry budget of a later run on the same thread.

On a retryable failure, the worker atomically increments `run.attempt`, moves the
run to `RETRYING`, and returns a retryable HTTP outcome while attempts remain.
The queue redelivers the same `runId`; the worker does not create a second run or
enqueue an unrelated replacement job. Once the run-specific maximum is reached,
the worker durably finalizes `FAILED` and returns a non-retryable acknowledged
outcome.

### 8.2 Queue acknowledgement

The queue route must not acknowledge successful execution before the durable
worker outcome is known:

```typescript
export const POST = verifySignatureAppRouter(async (req: NextRequest) => {
  const job = RunJobV1Schema.parse(await req.json());
  const outcome = await worker.execute(job);

  if (outcome.kind === 'retryable-failure') {
    return NextResponse.json({ error: outcome.error }, { status: 503 });
  }

  // completed, suspended, duplicate, cancelled, and durable non-retryable
  // failures have all been recorded and are safe to acknowledge.
  return NextResponse.json({ accepted: true, outcome: outcome.kind });
});
```

Do not use `waitUntil(worker.execute(...))` followed by an immediate 200. A
process crash after that response would lose the queue's retry guarantee.

Runs that may exceed the hosting platform's request duration require a durable
workflow provider or smaller resumable checkpoints. QStash dispatch alone does
not make an arbitrarily long in-process function durable.

---

## 9. Shared Execution Pipeline

The internal worker composes flavor-specific generation with platform-owned
steps:

1. Load and validate the claimed `RunDTO`.
2. Resolve the exact agent version and `ResolvedModel`.
3. Load durable public/model context and compact it for the resolved window.
4. Merge user tools with the configured subagent tool; reject reserved collisions.
5. Wrap confirmation-required tools in durable HITL suspension.
6. Execute the selected generation flavor.
7. Persist messages or artifacts and normalized usage idempotently.
8. Publish final result/status events in causal order.
9. Commit the terminal run and thread state once.
10. Invoke observational user hooks through `safeInvokeHook`.

Platform assignments are applied after user-owned SDK arguments. Users cannot
replace durable messages, stop signals, tool wrapping, step limits, or platform
callbacks.

### 9.1 Core-owned step loop

Durable HITL and reliable step-boundary budgets require the core—not an opaque
SDK multi-step call—to decide whether another model step starts. Each SDK call
therefore runs with `maxSteps: 1`:

```typescript
let messages = await loadCompactedContext(run, resolvedModel);
const aggregate = createRunAccumulator(run);

while (aggregate.stepCount < config.maxSteps) {
  if (!budget.canStartNextStep()) {
    return finalizeCompleted(run, aggregate, 'token_budget');
  }

  const step = agent.kind === 'stream-text'
    ? await executeStreamStep({ run, agent, resolvedModel, messages })
    : await executeGenerateStep({ run, agent, resolvedModel, messages });

  budget.record(step.usage);
  aggregate.add(step);
  await persistStepIdempotently(run, step, agent.output);

  const confirmation = findPendingConfirmation(step.toolCalls);
  if (confirmation) {
    return suspendAtCheckpoint(run, aggregate, confirmation);
  }

  messages = [...messages, ...step.responseMessages];
  if (!step.requiresAnotherStep) {
    return finalizeCompleted(run, aggregate, 'completed');
  }
}

return finalizeCompleted(run, aggregate, 'max_steps');
```

Confirmation-required tools are advertised to the model without an SDK
`execute` function. The core sees the resulting tool call, persists a checkpoint,
and executes the tool only after approval. Ordinary tools may execute within the
single step.

### 9.2 Streaming step

```typescript
async function executeStreamStep(input: StepInput): Promise<NormalizedStep> {
  let finished: StreamFinishParams | undefined;

  const result = streamText({
    ...input.agent.specArgs,
    model: input.resolvedModel.instance,
    messages: input.messages,
    tools: prepareStepTools(input.agent.tools),
    abortSignal: input.userStop.signal,
    maxSteps: 1,
    onChunk: async (params) => {
      await publishRunEvent(input.run, 'CHUNK', params.chunk);
      await safeInvokeHook(input.run, 'onChunk', input.agent.hooks?.onChunk, params);
    },
    onFinish: async (params) => {
      // Capture one SDK step. Run-level finalization happens in the outer loop.
      finished = params;
    },
    onError: async ({ error }) => {
      await safeInvokeHook(input.run, 'onError', input.agent.hooks?.onError, error);
    },
  });

  // AI SDK streaming is lazy. Consumption is mandatory.
  for await (const _part of result.fullStream) {
    // onChunk records live output; onFinish captures the completed step
  }

  if (!finished) throw new Error('Stream ended without a finish result');
  return normalizeStreamStep(finished);
}
```

The SDK `onFinish` is a platform callback for one step. The user-facing
`hooks.onResult` fires exactly once, after durable run-level finalization.

### 9.3 Generate step

The installed AI SDK does not expose `generateText.onFinish`. The step result is
captured explicitly from the returned promise:

```typescript
async function executeGenerateStep(input: StepInput): Promise<NormalizedStep> {
  const result = await generateText({
    ...input.agent.specArgs,
    model: input.resolvedModel.instance,
    messages: input.messages,
    tools: prepareStepTools(input.agent.tools),
    abortSignal: input.userStop.signal,
    maxSteps: 1,
  });

  return normalizeGenerateStep(result);
}
```

When the outer loop finishes:

- `output: 'conversation'` persists the aggregate response messages publicly.
- `output: 'artifact'` keeps intermediate model/tool messages internal and stores
  the final text/JSON as an `ArtifactDTO`.
- `TEXT_RESULT` is published before terminal `STATE_CHANGE`.
- normalized usage and the terminal run/thread state are committed idempotently.
- `hooks.onResult` receives one `CompletedAgentResult` after the commit.

### 9.4 Idempotent finalization

Finalization writes use `(runId, logicalMessageId)` and `(runId, usageKind)`
uniqueness. A retry after a partial database failure may safely repeat the
operation without duplicating messages or usage.

Where supported, message/artifact persistence, usage, terminal run state, thread
state, and terminal event outbox are committed in one transaction. Other adapters
must provide equivalent idempotency and recovery semantics.

---

## 10. Safe User Hooks

User hooks are observational. They are never allowed to change durable execution
outcomes or trigger a generation retry.

```typescript
async function safeInvokeHook<T>(
  run: RunDTO,
  hookName: string,
  hook: ((value: T) => void | Promise<void>) | undefined,
  value: T,
): Promise<void> {
  if (!hook) return;
  try {
    await hook(value);
  } catch (error) {
    await publishRunEvent(run, 'USER_HOOK_FAILED', {
      hookName,
      error: serializeError(error),
    });
  }
}
```

Platform persistence/publication happens before the corresponding hook. Hook
failure is visible through an event and observability, but it does not throw back
into the worker retry policy.

---

## 11. Token Budget Semantics

The v1 approach of calling `abort.abort()` from `onStepFinish` is not used. With
the installed AI SDK, aborting can reject the next provider call and bypass normal
finish handling.

The target implementation uses the core-owned single-step loop in §9.1. Durable
HITL needs the same boundary, so an SDK upgrade may simplify the implementation
but does not remove the checkpoint requirement. Do not enforce cumulative budget
by aborting an opaque multi-step SDK call.

Budget rules:

- Effective budget is the requested or agent default value, clamped by
  `core.options.tokenBudget.max`.
- Count normalized input plus output tokens for every completed step.
- Cached input is a subset of input and is never counted twice.
- Check the cumulative total before beginning the next step.
- The budget is a step-boundary ceiling and may overshoot by the final completed
  step. Provider `maxTokens` remains the hard per-call output cap.
- A budget stop is successful: `COMPLETED` with `stopReason: 'token_budget'`.
- A user cancellation is `CANCELLED` with `stopReason: 'user_cancelled'`.
- Terminal reason is recorded with an atomic transition; one abort boolean is not
  used to infer competing causes.

Usage normalization is SDK-version-aware. The current SDK reports
`promptTokens/completionTokens`; newer providers may expose
`inputTokens/cachedInputTokens/outputTokens`. Missing or `NaN` counters become
zero, but the raw provider metadata may be retained for audit.

---

## 12. Durable HITL Suspension and Resume

HITL must not park a background process while polling for approval.

When a confirmation-required tool is reached:

1. Persist a `RunCheckpoint` containing the tool call, response messages needed
   to continue, accumulated usage, remaining steps, and `checkpointVersion`.
2. Atomically transition the run/thread to `WAITING_FOR_INPUT`.
3. Publish `INPUT_REQUIRED` with `runId`, `toolCallId`, agent attribution, args,
   schema, and expiry.
4. Release the thread lease and return worker outcome `suspended`.
5. The queue request acknowledges only after that checkpoint is durable.

`core.hitl.respond()`:

1. Atomically validates the latest unanswered tool call and checkpoint version.
2. Persists the approved/denied tool result exactly once.
3. Transitions `WAITING_FOR_INPUT → QUEUED`.
4. Adds a resume job to the durable outbox with the incremented checkpoint version.

A timeout sweeper performs the same flow with the standard timeout tool result.
Late or duplicate responses are rejected idempotently.

The resumed worker reconstructs the AI SDK messages from the checkpoint and tool
result, then starts the next model step. Process lifetime is no longer part of HITL
correctness.

```typescript
export interface RunCheckpoint {
  runId: string;
  checkpointVersion: number;
  toolCallId: string;
  toolName: string;
  toolArguments: unknown;
  responseMessages: CoreMessage[];
  accumulatedUsage: NormalizedUsage;
  completedSteps: number;
  expiresAt: Date;
}
```

---

## 13. Subagent Policy

Subagent delegation is explicit:

```typescript
const chat = core.createStreamTextAgent({
  name: 'chat',
  version: '2026-08-31',
  model: 'gpt-4o',
  subagents: {
    agent: { name: 'researcher', version: '2026-08-31' },
    allowedModels: ['gpt-4o-mini'],
    allowedTools: ['searchDocs', 'readArtifact'],
    maxDepth: 2,
    maxConcurrent: 3,
    resultCapChars: 8_000,
  },
});
```

The platform injects `spawnSubagent` only for agents with this policy. Delegated
work resolves a registered handle rather than invoking a hard-coded `streamText`
configuration. It creates a child `RunDTO` with `rootRunId` and `parentRunId`,
inherits cancellation from the root run, and obeys the declared model allow-list
and depth/concurrency limits. The child starts from the tool implementations on
the referenced, versioned agent handle, filtered by `allowedTools`. Omitting
`allowedTools` grants no user-defined tools. Unknown names fail during
`core.sealRegistrations()`, and a parent policy cannot inject or replace tool
implementations. Platform-reserved tools such as `spawnSubagent` are not valid
entries; they are controlled by the referenced child's own policies.

Delegated runs are durable queue jobs. The parent persists a subagent checkpoint,
transitions to `WAITING_FOR_SUBAGENT`, and releases its worker. Child completion
stores the capped tool result and enqueues a versioned parent-resume job. Durable
storage claims—not an in-memory semaphore—enforce `maxConcurrent` across worker
processes and restarts.

For child-produced messages/events/usage, `runId` remains the root run,
`subagentRunId` identifies the exact child `RunDTO`, and `agentName` identifies
the registered child handle. This avoids overloading the old nullable `agentId`.

---

## 14. Example Setup

```typescript
const core = setupAgentCore({
  storage: new PrismaStorage(prisma),
  bus: new RedisBus(redis),
  queue: new QStashQueue(client, {
    url: `${APP_URL}/api/queue/agent-run`,
  }),
  kv: new RedisKv(redis),
  models: {
    'gpt-4o': {
      instance: openai('gpt-4o'),
      contextWindow: 128_000,
    },
    'gpt-4o-mini': openai('gpt-4o-mini'),
  },
  defaultAgent: { name: 'chat', version: '2026-08-31' },
  tokenBudget: {
    default: 40_000,
    max: 100_000,
  },
});

const researcher = core.createGenerateTextAgent({
  name: 'researcher',
  version: '2026-08-31',
  model: 'gpt-4o-mini',
  allowedModels: ['gpt-4o-mini'],
  tools: researchTools,
  output: 'artifact',
  system: 'Produce a concise research brief.',
  hooks: {
    onResult: async (result) => {
      await analytics.track('research_complete', {
        length: result.text.length,
      });
    },
  },
});

const chat = core.createStreamTextAgent({
  name: 'chat',
  version: '2026-08-31',
  model: 'gpt-4o',
  allowedModels: ['gpt-4o', 'gpt-4o-mini'],
  system: 'You are a concise product assistant.',
  temperature: 0.3,
  tools: productTools,
  subagents: {
    agent: { name: researcher.name, version: researcher.version },
    allowedModels: ['gpt-4o-mini'],
    allowedTools: ['searchDocs', 'readArtifact'],
  },
});

core.sealRegistrations();

const resolved = core.resolveModel('gpt-4o');
await chat.run({ prompt: 'Say hello' });
await core.stop(threadId);
const snapshot = await core.getThreadSnapshot(threadId);
```

The summarizer/researcher uses artifact output, so it does not silently append a
summary assistant message to the chat transcript. Agents that deliberately
participate in a conversation set `output: 'conversation'`.

---

## 15. Compatibility and Migration

`createAgentRuntime` cannot be a simple alias because the old return type exposes
`run`, `stop`, and `engine`. The compatibility release provides a facade:

```typescript
/** @deprecated Migrate to setupAgentCore and an explicit handle. */
export function createAgentRuntime(options: LegacyRuntimeOptions): LegacyAgentRuntime {
  const core = setupAgentCore({
    ...adaptLegacyOptions(options),
    defaultAgent: { name: 'legacy-default', version: LEGACY_AGENT_VERSION },
  });

  const legacy = registerLegacyDefaultAgent(core, options);
  core.sealRegistrations();

  return {
    run: legacy.run,
    stop: core.stop,
    hitl: core.hitl,
    events: core.events,
    getThreadSnapshot: core.getThreadSnapshot,
    engine: createLegacyWorkerFacade(core, legacy),
  };
}
```

Migration phases:

1. Add durable top-level runs, run attribution fields, artifacts, outbox records,
   and snapshot/read methods without removing legacy fields.
2. Add `setupAgentCore`, model resolution, explicit agent registration, and the
   internal worker executor.
3. Migrate the example app and queue payload to `RunJobV1`.
4. Replace blocking HITL with checkpoint/resume.
5. Ship the legacy facade with deprecation warnings for the remainder of the
   current major version.
6. Remove the legacy factory and job decoder only in the next major version.

During migration, legacy jobs map only to an explicitly configured
`legacyDefaultAgent` and version. They never fall back to whichever stream agent
happened to register first.

The usage migration keeps old pricing columns readable while new runs write
normalized counters. Downstream pricing can backfill or calculate USD totals from
`modelKey` and the recorded token dimensions.

---

## 16. Required Test Matrix

The refactor is complete only when tests cover:

- Concurrent duplicate delivery while the lease is held.
- Delayed duplicate delivery after the lease is released.
- Duplicate HTTP `run()` calls with one `idempotencyKey`.
- Queue enqueue failure and outbox recovery.
- Worker crash before and after durable finalization.
- Unknown agent, removed agent version, and stale checkpoint jobs.
- Deterministic registration and duplicate name/version-pair rejection.
- Model override allow-list and unknown model rejection.
- Stream consumption, one SDK finish per step, and exactly-once run finalization.
- Generate-result persistence without relying on a nonexistent SDK callback.
- User hook rejection without state change or redrive.
- Token budget versus simultaneous user cancellation.
- HITL suspend, process restart, approval resume, denial, timeout, and duplicate response.
- Reserved `spawnSubagent` tool collision.
- Subagent depth, concurrency, model/tool allow-lists, missing tool validation,
  and cancellation propagation.
- Conversation versus artifact output behavior.
- Snapshot during finalization with no missing or duplicated visible messages.
- Internal context summaries excluded from public snapshots.
- SSE resume from `lastEventSeq` and event/message deduplication.
- Legacy facade behavior for the entire compatibility window.

---

## 17. Final API Direction

The abstraction boundary is:

```text
application → AgentHandle.run()
                  ↓
             durable Run + outbox
                  ↓
authenticated queue → internal AgentWorker
                  ↓
       shared platform execution pipeline
                  ↓
     streamText or generateText flavor
```

Agent handles describe behavior. Durable runs describe execution. `AgentCore`
owns platform correctness. Physical storage remains adapter-defined, while the
public thread snapshot is one nested, paginated client model.
