import type { RuntimePorts, RunInput, RunResult } from '../ports/runtime.js';
import type { RegisteredAgent } from './agent.js';
import { reclaimIfOrphaned } from './reclaim.js';
import { claimRun } from './keys.js';
import { publish, setThreadState } from './publish.js';

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
  if (state === 'RUNNING' || state === 'WAITING_FOR_INPUT') {
    return { accepted: false, threadId, error: 'Thread has an active run' };
  }

  // Billing pre-execution check (§4) — user-injected hook
  if (deps.config.billingPreCheck) {
    const check = await deps.config.billingPreCheck(threadId);
    if (!check.ok) {
      return { accepted: false, threadId, error: check.error ?? 'Billing check failed' };
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
    await deps.storage.messages.deleteFrom(threadId, input.editMessageId);
  }

  await deps.storage.messages.append(threadId, { role: 'user', content: input.prompt });

  // Claim the thread for THIS run before the state key is touched (§2.1). The
  // next line overwrites whatever stop() may have just written, so the run id
  // — not the state key — is what retires a worker still running the previous
  // message.
  const runId = await claimRun(deps, threadId);

  await deps.kv.set(`agent:state:${threadId}`, 'RUNNING');
  await setThreadState(deps, threadId, 'RUNNING', model);
  // A durable run boundary lets reconnecting clients distinguish this turn's
  // in-flight chunks from earlier completed turns.
  await publish(deps, threadId, 'STATE_CHANGE', { state: 'RUNNING' });

  // The run's durable record opens here (§2.9): a thread accumulates many runs
  // and Thread.state only ever describes the latest, so this is the only place
  // "what happened, how long, what did it cost" can be answered from.
  await deps.admin.runs.start({ id: runId, threadId, agent: agent.name, model });

  await deps.queue.enqueue({
    threadId, runId, model, agent: agent.name,
    enqueuedAt: Date.now(),
    // Persisted on the ticket so a worker — or a resume after an approval,
    // hours later, in another process — rehydrates the same state (§2.10).
    ...(input.state ? { state: input.state } : {}),
    tokenBudget: input.tokenBudget,
    providerOptions: input.providerOptions,
  });

  return { accepted: true, threadId, runId, state: 'RUNNING' };
}
