import type { RuntimePorts } from '../ports/runtime.js';
import { ACTIVE_STATES, publish, transition } from './publish.js';
import { loadOpenHitls } from './hitl.js';
import type { StopResult } from '../ports/runtime.js';
import { currentRunId } from './keys.js';

/** The whole stop mechanism (§2.1): one button, one behavior — everything
 *  stops immediately. The engine's poller sees CANCELLED on the hot cache and
 *  fires the abort; the durable state is the recovery truth (§3.4). */
export async function stop(deps: RuntimePorts, threadId: string): Promise<StopResult> {
  const thread = await deps.storage.threads.get(threadId);
  if (!thread || !ACTIVE_STATES.includes(thread.state)) {
    return { accepted: false, error: `Cannot stop thread in state ${thread?.state ?? 'unknown'}` };
  }

  // Read before the write: an adapter may hand back the very object the
  // state write mutates.
  const wasParked = thread.state === 'WAITING_FOR_INPUT';
  const runId = await currentRunId(deps, threadId);
  const endedAt = new Date();

  // A compare-and-set on the state and the run (§3.4): a finish, a failure or
  // a newer run that got there first keeps its own ending, and this stop is
  // refused with the state it found.
  const won = await transition(deps, threadId, {
    from: ACTIVE_STATES, to: 'CANCELLED', runId, model: thread.model,
  });
  if (!won) {
    const now = await deps.storage.threads.get(threadId);
    return { accepted: false, error: `Cannot stop thread in state ${now?.state ?? 'unknown'}` };
  }
  // A queued or parked run may never execute again. Close its record here,
  // without touching usage that a running worker can still be accruing.
  if (runId) await recordStoppedRun(deps, runId, endedAt);
  await publish(deps, threadId, 'STATE_CHANGE', {
    state: 'CANCELLED', stopReason: 'cancelled', runId, endedAt,
  });

  if (wasParked) await closeOpenParks(deps, threadId);

  return { accepted: true };
}

export async function recordStoppedRun(deps: RuntimePorts, runId: string, endedAt: Date): Promise<void> {
  try {
    const prior = await deps.admin.runs.get(runId);
    if (!prior || prior.state === 'CANCELLED') return;
    await deps.admin.runs.patch(runId, {
      state: 'CANCELLED', stopReason: 'cancelled', endedAt,
      durationMs: endedAt.getTime() - new Date(prior.startedAt).getTime(),
    });
  } catch {
    // Operational history must not prevent cancellation.
  }
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
  const closed = new Set<string>(); // two parks in one child share its frames
  for (const pending of await loadOpenHitls(deps, threadId)) {
    let frames = pending.frames;
    if (pending.landed) {
      // Its own call has its result already; only the levels above wait.
      frames = frames.slice(pending.resumeFrame ?? 0);
    } else {
      await deps.storage.messages.append(threadId, {
        role: 'tool',
        agentId: pending.agentId,
        content: toolResult(pending.toolCallId, pending.toolName),
      });
      if (pending.nested) {
        await recordStoppedRun(deps, pending.nested.agentId, new Date());
      }
    }
    for (const frame of frames) {
      if (closed.has(frame.toolCallId)) continue;
      closed.add(frame.toolCallId);
      await deps.storage.messages.append(threadId, {
        role: 'tool',
        agentId: frame.agentId,
        content: toolResult(frame.toolCallId, 'spawnSubagent'),
      });
      if (frame.nested) {
        await recordStoppedRun(deps, frame.nested.agentId, new Date());
      }
    }
  }
}
