import type { RuntimePorts } from '../ports/runtime.js';
import { publish } from './publish.js';
import type { StopResult } from '../ports/runtime.js';

/** The whole stop mechanism (§2.1): one button, one behavior — everything
 *  stops immediately. The engine's poller sees CANCELLED on the hot cache and
 *  fires the abort; the durable state is the recovery truth (§3.4). */
export async function stop(deps: RuntimePorts, threadId: string): Promise<StopResult> {
  const thread = await deps.storage.threads.get(threadId);
  if (thread?.state !== 'RUNNING' && thread?.state !== 'WAITING_FOR_INPUT') {
    return { accepted: false, error: `Cannot stop thread in state ${thread?.state ?? 'unknown'}` };
  }

  await deps.kv.set(`agent:state:${threadId}`, 'CANCELLED');
  await deps.storage.threads.setState(threadId, 'CANCELLED');
  await publish(deps, threadId, 'STATE_CHANGE', { state: 'CANCELLED' });

  return { accepted: true };
}
