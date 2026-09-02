import type { RuntimePorts } from '../ports/runtime.js';
import type { ExecutionState } from './types.js';
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

/** Move a thread to a new state on BOTH the caller's storage and the
 *  platform's own operational view (§2.9).
 *
 *  One choke point on purpose: the admin thread table is what lets a dashboard
 *  answer "what is running right now" without reading the caller's database,
 *  and it is only true if every transition passes through here. `model` is
 *  looked up when not supplied, so callers that already hold the thread can
 *  skip a read. */
export async function setThreadState(
  deps: RuntimePorts,
  threadId: string,
  state: ExecutionState,
  model?: string,
): Promise<void> {
  await deps.storage.threads.setState(threadId, state);
  try {
    const resolved = model ?? (await deps.storage.threads.get(threadId))?.model ?? 'unknown';
    await deps.admin.threads.upsert({ id: threadId, state, model: resolved });
  } catch {
    // Observability must never be able to fail a transition that succeeded.
  }
}
