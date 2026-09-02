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
): Promise<AgentEvent> {
  const event: AgentEvent = { threadId, seq: 0, type, payload, createdAt: new Date() };
  await deps.bus.publish(threadId, event);
  return event;
}

/** Event types the platform itself emits. An app cannot publish these: a
 *  client's reducer trusts them to mean what the engine meant. */
export const RESERVED_EVENT_TYPES: ReadonlySet<string> = new Set([
  'CHUNK',
  'STATE_CHANGE',
  'STEP_COMMITTED',
  'STEP_FINISHED',
  'INPUT_REQUIRED',
  'INPUT_EXPIRED',
  'HITL_RESPONSE',
  'MESSAGE_APPENDED',
  'MESSAGES_DROPPED',
  'CONTEXT_COMPACTED',
  'SUBAGENT_STARTED',
  'SUBAGENT_CHUNK',
  'SUBAGENT_COMPLETED',
  'SUBAGENT_FAILED',
  'TEXT_RESULT',
  'THREAD_DELETED',
  'HEARTBEAT',
  'RUN_REFUSED',
  'TOKEN_BUDGET_EXHAUSTED',
]);

export interface PublishEventOptions {
  /** `true` (the default) writes the event to the thread's log, so it is
   *  replayed to a client that reconnects. `false` sends it over the bus only:
   *  a progress tick, a typing indicator — anything nobody needs to see twice. */
  durable?: boolean;
}

/** Publish an event of your own on a thread, through the same pipeline the
 *  platform's events take: the durable log and the live bus (§2.2). A client
 *  sees it in `onEvent`, exactly like a built-in one. */
export async function publishEvent(
  deps: RuntimePorts,
  threadId: string,
  type: string,
  payload: unknown,
  options: PublishEventOptions = {},
): Promise<AgentEvent> {
  if (!type || typeof type !== 'string') {
    throw new Error('publishEvent: an event type is required');
  }
  if (RESERVED_EVENT_TYPES.has(type)) {
    throw new Error(`publishEvent: ${type} is a platform event type — pick your own`);
  }
  return options.durable === false
    ? publishNotice(deps, threadId, type, payload)
    : publish(deps, threadId, type, payload);
}

/** What a tool calls to publish: `publishEvent(type, payload, options?)`,
 *  already bound to the thread the tool is acting on. */
export type ToolPublishEvent = (
  type: string,
  payload: unknown,
  options?: PublishEventOptions,
) => Promise<AgentEvent>;

/** Give every tool `publishEvent` alongside the SDK's own options, bound to
 *  the thread it runs on — main agent, nested run, or a segment resumed after
 *  an approval alike. */
export function withPublishEvent(
  deps: RuntimePorts,
  threadId: string,
  tools: Record<string, any>,
): Record<string, any> {
  const publishHere: ToolPublishEvent = (type, payload, options) =>
    publishEvent(deps, threadId, type, payload, options);
  const out: Record<string, any> = {};
  for (const [name, t] of Object.entries(tools)) {
    out[name] =
      typeof t?.execute === 'function'
        ? {
            ...t,
            execute: (args: unknown, opts: object) =>
              t.execute(args, { ...opts, publishEvent: publishHere }),
          }
        : t;
  }
  return out;
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
