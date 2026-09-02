import type { RuntimePorts } from '../ports/runtime.js';
import { publish, setThreadState } from './publish.js';
import { loadOpenHitls } from './hitl.js';
import type { StopResult } from '../ports/runtime.js';

/** The whole stop mechanism (§2.1): one button, one behavior — everything
 *  stops immediately. The engine's poller sees CANCELLED on the hot cache and
 *  fires the abort; the durable state is the recovery truth (§3.4). */
export async function stop(deps: RuntimePorts, threadId: string): Promise<StopResult> {
  const thread = await deps.storage.threads.get(threadId);
  if (thread?.state !== 'RUNNING' && thread?.state !== 'WAITING_FOR_INPUT') {
    return { accepted: false, error: `Cannot stop thread in state ${thread?.state ?? 'unknown'}` };
  }

  // Read before the write: an adapter may hand back the very object the
  // state write mutates.
  const wasParked = thread.state === 'WAITING_FOR_INPUT';

  await deps.kv.set(`agent:state:${threadId}`, 'CANCELLED');
  await setThreadState(deps, threadId, 'CANCELLED', thread.model);
  await publish(deps, threadId, 'STATE_CHANGE', { state: 'CANCELLED' });

  if (wasParked) await closeOpenParks(deps, threadId);

  return { accepted: true };
}

/** Answer every open approval with a cancellation, so the history the next
 *  run sends is well-formed (§2.5). A park persists the assistant's tool call
 *  and defers its result to the resume; a stop means that resume never comes,
 *  and a dangling call is a prompt no strict provider accepts. Nested parks
 *  close their whole chain: the child's call and every spawnSubagent call
 *  waiting on it (§2.7), whose run records end CANCELLED too. */
async function closeOpenParks(deps: RuntimePorts, threadId: string): Promise<void> {
  const result = { cancelled: true, reason: 'stopped' };
  const toolResult = (toolCallId: string, toolName: string) => [
    { type: 'tool-result', toolCallId, toolName, result },
  ];
  for (const pending of await loadOpenHitls(deps, threadId)) {
    await deps.storage.messages.append(threadId, {
      role: 'tool',
      agentId: pending.agentId,
      content: toolResult(pending.toolCallId, pending.toolName),
    });
    if (pending.nested) {
      await deps.admin.runs
        .patch(pending.nested.agentId, { state: 'CANCELLED', stopReason: 'cancelled', endedAt: new Date() })
        .catch(() => undefined);
    }
    for (const frame of pending.frames) {
      await deps.storage.messages.append(threadId, {
        role: 'tool',
        agentId: frame.agentId,
        content: toolResult(frame.toolCallId, 'spawnSubagent'),
      });
      if (frame.nested) {
        await deps.admin.runs
          .patch(frame.nested.agentId, { state: 'CANCELLED', stopReason: 'cancelled', endedAt: new Date() })
          .catch(() => undefined);
      }
    }
  }
}
