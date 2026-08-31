import type { RuntimePorts, RunInput, RunResult } from '../ports/runtime.js';
import { reclaimIfOrphaned } from './reclaim.js';

/** The §5.1 behavior: heal orphans → billing pre-check (§4) → persist the user
 *  message → state RUNNING (hot + durable) → enqueue on `agent-runs` (§2.8).
 *  Accepts no execution responsibility whatsoever — the queue does the rest. */
export async function run(deps: RuntimePorts, input: RunInput): Promise<RunResult> {
  let threadId = input.threadId;
  if (!threadId) {
    const created = await deps.storage.threads.create({ model: input.model });
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

  await deps.queue.enqueue({ threadId, model: input.model });

  return { accepted: true, threadId, state: 'RUNNING' };
}
