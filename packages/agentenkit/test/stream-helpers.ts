import type { RuntimePorts } from '../src/ports/runtime.js';
import type { StreamEvent, StreamItem } from '../src/core/stream-events.js';

/** Every item a run's streams hold, segment after segment. */
export async function runItems(ports: RuntimePorts, runId: string): Promise<StreamItem[]> {
  const out: StreamItem[] = [];
  for (let segment = 1; ; segment++) {
    const snap = await ports.streams!.snapshot(`${runId}:${segment}`);
    if (!snap) return out;
    out.push(...snap.items);
  }
}

/** Every item on a thread's run streams, in run order: the thread record
 *  names each segment it had. */
export async function threadItems(ports: RuntimePorts, threadId: string): Promise<StreamItem[]> {
  const out: StreamItem[] = [];
  for (const e of await ports.storage.events.list(threadId, { types: ['RUN_STARTED'] })) {
    const snap = await ports.streams!.snapshot((e.payload as { streamId: string }).streamId);
    if (snap) out.push(...snap.items);
  }
  return out;
}

/** The items of one type, nested ones unwrapped. */
export function ofType<T extends StreamEvent['type']>(items: StreamItem[], type: T): Array<Extract<StreamEvent, { type: T }>> {
  const flat = items.flatMap((i): StreamEvent[] => (i.type === 'SUBAGENT_EVENT' ? [i, i.event] : [i]));
  return flat.filter((e) => e.type === type) as Array<Extract<StreamEvent, { type: T }>>;
}

/** A thread's subagent starts and ends, from its run streams. */
export async function subagents(ports: RuntimePorts, threadId: string) {
  const items = await threadItems(ports, threadId);
  return {
    started: ofType(items, 'SUBAGENT_STARTED'),
    completed: ofType(items, 'SUBAGENT_FINISHED').filter((e) => e.status === 'completed'),
    failed: ofType(items, 'SUBAGENT_FINISHED').filter((e) => e.status !== 'completed'),
  };
}
