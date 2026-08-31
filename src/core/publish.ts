import type { RuntimePorts } from '../ports/runtime.js';
import type { AgentEvent } from './types.js';

/** Persist to the replayable event log, then fan out live to all subscribers
 *  (§2.2). Seq comes from Kv.incr — monotonic per thread (§3.4). */
export async function publish(
  deps: RuntimePorts,
  threadId: string,
  type: string,
  payload: unknown,
): Promise<AgentEvent> {
  const seq = await deps.kv.incr(`agent:seq:${threadId}`);
  const event: AgentEvent = { threadId, seq, type, payload, createdAt: new Date() };
  await deps.storage.events.append(threadId, event);
  await deps.bus.publish(threadId, event);
  return event;
}

/** Publish a bus-only notice (never persisted) — e.g. HITL death notices (§2.5). */
export async function publishNotice(
  deps: RuntimePorts,
  threadId: string,
  type: string,
  payload: unknown,
): Promise<void> {
  await deps.bus.publish(threadId, {
    threadId, seq: 0, type, payload, createdAt: new Date(),
  } as AgentEvent);
}
