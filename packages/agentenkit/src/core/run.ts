import type { RuntimePorts, RunInput, RunResult } from '../ports/runtime.js';
import type { RegisteredAgent } from './agent.js';
import { reclaimIfOrphaned } from './reclaim.js';
import { runIdKey } from './keys.js';
import { recordStoppedRun } from './stop.js';
import { randomUUID } from 'node:crypto';
import { publish, setThreadState, publishEvent } from './publish.js';
import { mergeProviderOptions } from './types.js';

/** The §5.1 behavior: heal orphans → billing pre-check (§4) → persist the user
 *  message → state RUNNING (hot + durable) → enqueue on the dispatch queue
 *  (§2.8). Accepts no execution responsibility whatsoever — the queue does
 *  the rest, and the job dispatches back to THIS handle. */
export async function run(
  deps: RuntimePorts,
  agent: RegisteredAgent,
  input: RunInput,
): Promise<RunResult> {
  // Model resolution order (§3.1): run input → spec default → 'gpt-4o'
  const model = input.model ?? agent.spec.model ?? 'gpt-4o';

  let threadId = input.threadId;
  if (!threadId) {
    const created = await deps.storage.threads.create({ model });
    threadId = created.id;
  }

  // Heal an orphaned HITL wait first (§2.5)
  await reclaimIfOrphaned(deps, threadId);

  const state = await deps.kv.get(`agent:state:${threadId}`);
  const thread = await deps.storage.threads.get(threadId);
  if (!thread) return { accepted: false, threadId, error: 'Thread not found' };
  const initialState = thread.state;
  if (state === 'RUNNING' || state === 'WAITING_FOR_INPUT' ||
      thread.state === 'RUNNING' || thread.state === 'WAITING_FOR_INPUT') {
    return { accepted: false, threadId, error: 'Thread has an active run' };
  }

  // Billing pre-execution check (§4) — user-injected hook. A refusal is
  // published on the thread as well as returned, so every client on the
  // thread sees it, and a reload still shows it.
  if (deps.config.billingPreCheck) {
    const check = await deps.config.billingPreCheck({
      threadId,
      state: input.state ?? {},
      publishEvent: (type, payload, options) => publishEvent(deps, threadId, type, payload, options),
    });
    if (!check.ok) {
      const error = check.error ?? 'Billing check failed';
      await publish(deps, threadId, 'RUN_REFUSED', { reason: 'billing', error });
      return { accepted: false, threadId, error };
    }
  }

  // Edit + resend (§5.1): the edited turn and everything it led to are
  // dropped, then the new text is appended in its place — one thread, no
  // forking. Only a user turn may be edited: cutting from anywhere else can
  // strip a tool result off the assistant tool-call that produced it, and a
  // dangling call is a conversation no provider accepts.
  if (input.editMessageId) {
    // Only the main agent's turns are editable — a nested run's stream is
    // never addressed from the outside (§2.7)
    const history = await deps.storage.messages.list(threadId, { agentId: null });
    const target = history.find((m) => m.id === input.editMessageId);
    if (!target) return { accepted: false, threadId, error: 'Message not found' };
    if (target.role !== 'user') {
      return { accepted: false, threadId, error: 'Only a user message can be edited' };
    }
  }

  // The durable store already provides an atomic state claim. Do this before
  // editing or appending history, so only the winning send changes the thread.
  const runId = randomUUID();
  const previousRunId = await deps.kv.get(runIdKey(threadId));
  const admitted = await deps.storage.threads.claimState(threadId, initialState, 'RUNNING');
  if (!admitted) return { accepted: false, threadId, error: 'Thread has an active run' };

  let installed = false;
  try {
    await deps.kv.set(runIdKey(threadId), runId);
    installed = true;

    await deps.kv.set(`agent:state:${threadId}`, 'RUNNING');

    // The run's durable record opens here (§2.9): a thread accumulates many runs
    // and Thread.state only ever describes the latest, so this is the only place
    // "what happened, how long, what did it cost" can be answered from.
    await deps.admin.runs.start({
      id: runId, threadId, agent: agent.name, model,
      // What this run was asked to do (§2.9) — without it a dashboard can show
      // that a run was slow but not what it was slow at.
      ...(deps.config.recordPayloads
        ? {
            prompt:
              input.prompt.length > deps.config.payloadCapChars
                ? `${input.prompt.slice(0, deps.config.payloadCapChars)}…`
                : input.prompt,
            tokenBudget: input.tokenBudget ?? null,
            runState: input.state ?? null,
            providerOptions: providerOptionsFor(deps, agent, input),
          }
        : {}),
    });

    if (!await dispatchActive(deps, threadId, runId)) {
      return { accepted: false, threadId, runId, error: 'Run was stopped before dispatch' };
    }

    if (input.editMessageId) {
      await deps.storage.messages.deleteFrom(threadId, input.editMessageId);
      // Other clients are showing turns that no longer exist (§2.2). The tab
      // that made the edit truncated its own view; every other one needs telling.
      await publish(deps, threadId, 'MESSAGES_DROPPED', { fromMessageId: input.editMessageId });
    }

    const userMessage = await deps.storage.messages.append(threadId, {
      role: 'user',
      content: input.prompt,
    });

    // The user's turn goes on the bus like everything else (§2.2). Without it a
    // second client watching the same thread sees the reply stream in with no
    // question in front of it — the sending tab had only ever added the message
    // to its own local state.
    await publish(deps, threadId, 'MESSAGE_APPENDED', {
      id: userMessage.id,
      role: userMessage.role,
      content: userMessage.content,
      agentId: userMessage.agentId,
      createdAt: userMessage.createdAt,
    });

    // Admission already set durable RUNNING. Never overwrite a stop that
    // arrived while history was being persisted.
    if (!await dispatchActive(deps, threadId, runId)) {
      return { accepted: false, threadId, runId, error: 'Run was stopped before dispatch' };
    }
    // A durable run boundary lets reconnecting clients distinguish this turn's
    // in-flight chunks from earlier completed turns.
    await publish(deps, threadId, 'STATE_CHANGE', { state: 'RUNNING' });

    // What started the thread (§2.9), recorded once: the first dispatched
    // run's parameters. A later run never overwrites it. Observability must
    // never fail a run, so this is best-effort.
    await deps.admin.threads
      .upsert({
        id: threadId, state: 'RUNNING', model,
        startedWith: {
          runId, agent: agent.name, model, at: new Date(),
          ...(deps.config.recordPayloads
            ? {
                prompt: capText(input.prompt, deps.config.payloadCapChars),
                tokenBudget: input.tokenBudget ?? null,
                state: input.state ?? null,
                providerOptions: providerOptionsFor(deps, agent, input),
              }
            : {}),
        },
      })
      .catch(() => undefined);

    if (!await dispatchActive(deps, threadId, runId)) {
      return { accepted: false, threadId, runId, error: 'Run was stopped before dispatch' };
    }
    await deps.queue.enqueue({
      threadId, runId, model, agent: agent.name,
      enqueuedAt: Date.now(),
      // Persisted on the ticket so a worker — or a resume after an approval,
      // hours later, in another process — rehydrates the same state (§2.10).
      ...(input.state ? { state: input.state } : {}),
      tokenBudget: input.tokenBudget,
      costBudgetMicros: input.costBudgetMicros,
      providerOptions: input.providerOptions,
    });

    return { accepted: true, threadId, runId, state: 'RUNNING' };
  } catch (err) {
    const error = err instanceof Error ? err.message : String(err);
    await failDispatch(deps, threadId, runId, model, error, installed ? undefined : previousRunId);
    return { accepted: false, threadId, runId, error };
  }
}

/** Stop can arrive while a request is still preparing its dispatch. */
async function dispatchActive(deps: RuntimePorts, threadId: string, runId: string): Promise<boolean> {
  const current = await deps.kv.get(runIdKey(threadId));
  const thread = await deps.storage.threads.get(threadId);
  if (current === runId && thread?.state === 'RUNNING') return true;
  await recordStoppedRun(deps, runId, new Date());
  if (current === runId && thread?.state === 'CANCELLED') {
    await deps.kv.set(`agent:state:${threadId}`, 'CANCELLED');
  }
  return false;
}

/** A rejected dispatch must not leave an active thread with no worker.
 *  Conditional cleanup cannot overwrite a stop or a newer run. */
async function failDispatch(
  deps: RuntimePorts, threadId: string, runId: string,
  model: string, error: string,
  previousRunId?: string | null,
): Promise<void> {
  const key = `agent:state:${threadId}`;
  const current = await deps.kv.get(runIdKey(threadId));
  // If installing the identity failed, the reservation can still be ours
  // under the prior id. Never close or modify that prior run's record.
  if (current !== runId && (previousRunId === undefined || current !== previousRunId)) return;
  if (!await deps.storage.threads.claimState(threadId, 'RUNNING', 'FAILED')) return;
  await deps.kv.set(key, 'FAILED');
  await setThreadState(deps, threadId, 'FAILED', model);
  try {
    const prior = await deps.admin.runs.get(runId);
    if (prior) {
      const endedAt = new Date();
      await deps.admin.runs.patch(runId, {
        state: 'FAILED', stopReason: 'failed', error, endedAt,
        durationMs: endedAt.getTime() - new Date(prior.startedAt).getTime(),
      });
    }
  } catch { /* Operational history must not block dispatch recovery. */ }
  await publish(deps, threadId, 'STATE_CHANGE', { state: 'FAILED', stopReason: 'failed', runId, error });
}

/** The provider options a run is dispatched with (§3.1): config → spec →
 *  input, each winning over the one before, per provider namespace. Null when
 *  no level sets any, so the column stays empty rather than `{}`. */
function providerOptionsFor(
  deps: RuntimePorts,
  agent: RegisteredAgent,
  input: RunInput,
): Record<string, unknown> | null {
  const merged = mergeProviderOptions(
    mergeProviderOptions(deps.config.providerOptions, agent.spec.providerOptions),
    input.providerOptions,
  );
  return merged && Object.keys(merged).length > 0 ? merged : null;
}

function capText(text: string, limit: number): string {
  return text.length > limit ? `${text.slice(0, limit)}…` : text;
}
