import type { RuntimePorts } from '../ports/runtime.js';
import { THREAD_KEY_TTL_SECONDS } from './keys.js';
import { activeSegment } from './segment.js';
import type { ExecutionState, ThreadTransition } from './types.js';
import type { AgentEvent } from './types.js';

/** Per-thread chains that serialise taking a seq and storing the event in
 *  this process, so the log is written in seq order and a counter reseed (see
 *  nextSeq) cannot race another publisher here. The bus send is left outside:
 *  a subscriber may publish from inside it, and a follower fills any gap the
 *  bus leaves from storage (see followEvents). */
const publishChains = new Map<string, Promise<unknown>>();

function serialise<T>(threadId: string, work: () => Promise<T>): Promise<T> {
  const before = publishChains.get(threadId) ?? Promise.resolve();
  const mine = before.then(work, work);
  const tail = mine.catch(() => undefined);
  publishChains.set(threadId, tail);
  // Dropped once nothing queues behind it, so the map does not grow per thread.
  void tail.then(() => {
    if (publishChains.get(threadId) === tail) publishChains.delete(threadId);
  });
  return mine;
}

/** The thread's next event seq. A counter that restarts at 1 on a thread that
 *  already has events means the kv lost the key (a flush, an eviction, a
 *  restart without persistence). Carrying on from 1 would repeat seqs the log
 *  already holds, and every client would drop the new events as already seen,
 *  so the counter is moved past the stored ones first. The move is a
 *  compare-and-set, so a publisher that took 2 meanwhile is not undone. */
async function nextSeq(deps: RuntimePorts, threadId: string): Promise<number> {
  const key = `agent:seq:${threadId}`;
  const seq = await deps.kv.incrWithExpiry(key, THREAD_KEY_TTL_SECONDS);
  if (seq !== 1) return seq;
  const stored = await deps.storage.events.listSince(threadId, 0);
  const top = stored.at(-1)?.seq ?? 0;
  if (top < 1) return seq;
  await deps.kv.setIfValue(key, '1', String(top));
  return deps.kv.incr(key);
}

/** Persist to the replayable event log, then fan out live to all subscribers
 *  (§2.2). Seq comes from Kv.incr — monotonic per thread (§3.4). */
export async function publish(
  deps: RuntimePorts,
  threadId: string,
  type: string,
  payload: unknown,
): Promise<AgentEvent> {
  const event = await serialise(threadId, async () => {
    const seq = await nextSeq(deps, threadId);
    const e: AgentEvent = { threadId, seq, type, payload, createdAt: new Date() };
    await deps.storage.events.append(threadId, e);
    return e;
  });
  await deps.bus.publish(threadId, event);
  await toSegment(deps, threadId, type, payload);
  return event;
}

/** Hands an event to the run stream this process has open on the thread,
 *  if any; the segment keeps what belongs in a stream (see SegmentStream). */
async function toSegment(deps: RuntimePorts, threadId: string, type: string, payload: unknown) {
  const seg = activeSegment(deps, threadId);
  if (seg) await seg.forward(type, payload, RESERVED_EVENT_TYPES.has(type));
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
  await toSegment(deps, threadId, type, payload);
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
  'COST_BUDGET_EXHAUSTED',
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

/** A non-terminal STATE_CHANGE: the state, the run it belongs to, and when
 *  that run started, read off its record. Every STATE_CHANGE names its run,
 *  so a client keeps one timer per run and never refetches history to find
 *  its start. */
export async function runStatePayload(
  deps: RuntimePorts,
  state: ExecutionState,
  runId?: string,
): Promise<Record<string, unknown>> {
  const p: Record<string, unknown> = { state };
  if (!runId) return p;
  p.runId = runId;
  try {
    const rec = await deps.admin.runs.get(runId);
    if (rec) p.startedAt = rec.startedAt;
  } catch {
    // The timer is a nicety; a missing record never fails a transition.
  }
  return p;
}

/** The states a run is still going in: the ones a stop or a failure can end. */
export const ACTIVE_STATES: ExecutionState[] = ['QUEUED', 'RUNNING', 'WAITING_FOR_INPUT'];

/** The one way a run moves its thread's state (§3.4). A compare-and-set on
 *  the durable row, on the state AND the run that owns the thread, and only
 *  the caller that wins it writes the hot cache and the admin view. So a stop
 *  is never overwritten by a finish, and a stopped or replaced run can never
 *  move the thread again: its change simply loses. Returns whether this
 *  caller made the change. Publishing the STATE_CHANGE is left to the caller,
 *  whose payload differs per change. */
export async function transition(
  deps: RuntimePorts,
  threadId: string,
  change: ThreadTransition & { model?: string },
): Promise<boolean> {
  const { model, ...t } = change;
  if (!(await deps.storage.threads.transition(threadId, t))) return false;
  await deps.kv.set(`agent:state:${threadId}`, t.to, { exSeconds: THREAD_KEY_TTL_SECONDS });
  await upsertAdminThread(deps, threadId, t.to, model);
  return true;
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
  await upsertAdminThread(deps, threadId, state, model);
}

async function upsertAdminThread(
  deps: RuntimePorts,
  threadId: string,
  state: ExecutionState,
  model?: string,
): Promise<void> {
  try {
    const resolved = model ?? (await deps.storage.threads.get(threadId))?.model ?? 'unknown';
    await deps.admin.threads.upsert({ id: threadId, state, model: resolved });
  } catch {
    // Observability must never be able to fail a transition that succeeded.
  }
}
