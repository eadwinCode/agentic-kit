import type { RuntimePorts, ThreadSnapshot } from '../ports/runtime.js';
import { currentRunId } from './keys.js';
import { ACTIVE_STATES } from './publish.js';
import type { StreamEnd, StreamItem } from './stream-events.js';
import type { AgentEvent } from './types.js';

/** The run stream a snapshot carries: the segment in flight, or one that
 *  ended a moment ago, so a tab that loads just as a run ends still sees
 *  its end. */
export interface SnapshotStream {
  streamId: string;
  runId: string;
  /** What the messages do not have yet: each agent's events after its last
   *  finished step. A finished step is in the messages already, and showing
   *  its deltas too would put its text on screen twice. */
  items: StreamItem[];
  /** The stream's end, when it has one. */
  end: StreamEnd | null;
  /** Where a live read picks up: the last offset the stream held, cut items
   *  included. Null for a stream with nothing in it. */
  offset: string | null;
}

/** How long after its end a stream still rides on the snapshot. */
export const RECENT_END_MS = 60_000;

/** Events that are part of a step's content, and so of its saved messages. */
const CONTENT = new Set([
  'TEXT_MESSAGE_START', 'TEXT_MESSAGE_CONTENT', 'TEXT_MESSAGE_END',
  'REASONING_START', 'REASONING_CONTENT', 'REASONING_END',
  'TOOL_CALL_START', 'TOOL_CALL_ARGS', 'TOOL_CALL_END', 'TOOL_CALL_RESULT', 'SOURCE',
]);

/** Whose step an item belongs to ('' the main agent), and whether it is
 *  content. */
function ownerOf(item: StreamItem): { agent: string; content: boolean } {
  if (item.type === 'SUBAGENT_EVENT') return { agent: item.subagentId, content: CONTENT.has(item.event.type) };
  return { agent: '', content: CONTENT.has(item.type) };
}

/** Drop each agent's content up to its last finished step (see
 *  SnapshotStream.items). */
export function cutCommitted(items: StreamItem[]): StreamItem[] {
  const lastStep = new Map<string, number>();
  items.forEach((item, index) => {
    if (item.type === 'STEP_FINISHED') lastStep.set(item.agentId ?? '', index);
  });
  return items.filter((item, index) => {
    const { agent, content } = ownerOf(item);
    return !content || index > (lastStep.get(agent) ?? -1);
  });
}

/** The thread's current run stream, from the thread record: the latest
 *  segment that started, while it is open or only just ended. Null when
 *  there is none, or it is gone. */
export async function snapshotStream(deps: RuntimePorts, threadId: string): Promise<SnapshotStream | null> {
  if (!deps.streams) return null;
  const started = await deps.storage.events.latest(threadId, 'RUN_STARTED');
  if (!started) return null;
  const { streamId, runId } = started.payload as { streamId: string; runId: string };
  const ended = await deps.storage.events.latest(threadId, 'RUN_ENDED');
  if (
    ended && ended.seq > started.seq &&
    (ended.payload as { streamId?: string }).streamId === streamId &&
    Date.now() - new Date(ended.createdAt).getTime() > RECENT_END_MS
  ) {
    return null;
  }
  const snap = await deps.streams.snapshot(streamId);
  if (!snap) return null;
  return {
    streamId,
    runId,
    items: cutCommitted(snap.items),
    end: snap.end,
    offset: snap.items.at(-1)?.offset ?? null,
  };
}

/** One read for a UI: the thread, its messages, its runs, the unfinished
 *  run's record entries and its run stream. Null when the thread is gone.
 *  `afterMessageId` keeps only the messages after that one: what a tab that
 *  already has it is missing (a SNAPSHOT frame). An id the thread does not
 *  have keeps them all. */
export async function threadSnapshot(
  deps: RuntimePorts,
  threadId: string,
  opts: { afterMessageId?: string } = {},
): Promise<ThreadSnapshot | null> {
  const thread = await deps.storage.threads.get(threadId);
  if (!thread) return null;

  let messages = await deps.storage.messages.list(threadId, undefined);
  if (opts.afterMessageId) {
    const at = messages.findIndex((m) => m.id === opts.afterMessageId);
    if (at >= 0) messages = messages.slice(at + 1);
  }
  const runs = await deps.admin.runs.listByThread(threadId);
  // The record is small (parks, refusals, each segment's start and end),
  // and the run's live events come from its stream, not from here.
  const events = await deps.storage.events.listSince(threadId, -1);
  const lastEventSeq = events.at(-1)?.seq ?? -1;
  // The unfinished run's record entries: its open park, a refusal.
  let activeEvents: AgentEvent[] = [];
  if (ACTIVE_STATES.includes(thread.state)) {
    const runId = await currentRunId(deps, threadId);
    activeEvents = runId ? events.filter((e) => e.runId === runId) : [];
  }
  const stream = await snapshotStream(deps, threadId);
  return { thread, messages, runs, lastEventSeq, activeEvents, stream };
}
