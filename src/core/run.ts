import type { RuntimePorts, RunInput, RunResult } from '../ports/runtime.js';
import type { RegisteredAgent } from './agent.js';
import { reclaimIfOrphaned } from './reclaim.js';
import { publish } from './publish.js';

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
  console.log('threadId', threadId);
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

  await deps.storage.messages.append(threadId, { role: 'user', content: input.prompt });
  await deps.kv.set(`agent:state:${threadId}`, 'RUNNING');
  await deps.storage.threads.setState(threadId, 'RUNNING');
  // A durable run boundary lets reconnecting clients distinguish this turn's
  // in-flight chunks from earlier completed turns.
  await publish(deps, threadId, 'STATE_CHANGE', { state: 'RUNNING' });

  await deps.queue.enqueue({ threadId, model, agent: agent.name, tokenBudget: input.tokenBudget });

  return { accepted: true, threadId, state: 'RUNNING' };
}
