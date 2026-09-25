import type { StreamEvent, WireStreamItem } from './types.js';

/** Where the hook is, as the server reads it: the thread record's last seq
 *  and its place in a run stream, `<seq> <streamId> <offset>` with `-` for a
 *  part it does not have. */
export interface ThreadCursor {
  seq: number;
  streamId?: string;
  offset?: string;
}

export function formatCursor(c: ThreadCursor): string {
  return `${c.seq} ${c.streamId || '-'} ${c.offset || '-'}`;
}

/** A run stream event as one or more of the thread events the reducer knows:
 *  a delta as the CHUNK it replaces, a nested run's event as SUBAGENT_CHUNK,
 *  a one-shot answer as TEXT_RESULT. Events with nothing to show (a run's
 *  start and end, a step's end) become none; the thread's own notices say
 *  what state the run is in. Each comes out as a live event (seq 0), so it
 *  never moves the record cursor. */
export function streamItemEvents(item: WireStreamItem): StreamEvent[] {
  const live = (type: string, payload: unknown): StreamEvent => ({ seq: 0, type, payload });
  const chunk = chunkOf(item);
  if (chunk) return [live('CHUNK', chunk)];
  switch (item.type) {
    case 'SUBAGENT_STARTED':
      return [live('SUBAGENT_STARTED', { agentId: item.subagentId, name: item.name, depth: item.depth })];
    case 'SUBAGENT_EVENT': {
      const inner = chunkOf(item.event);
      return inner ? [live('SUBAGENT_CHUNK', { agentId: item.subagentId, chunk: inner })] : [];
    }
    case 'SUBAGENT_FINISHED':
      return item.status === 'completed'
        ? [live('SUBAGENT_COMPLETED', { agentId: item.subagentId })]
        : [live('SUBAGENT_FAILED', {
            agentId: item.subagentId,
            state: item.status === 'cancelled' ? 'CANCELLED' : 'FAILED',
            ...(item.error !== undefined ? { error: item.error } : {}),
          })];
    case 'INPUT_REQUIRED': {
      const { type: _type, offset: _offset, ...request } = item;
      return [live('INPUT_REQUIRED', request)];
    }
    case 'CUSTOM':
      return [live(item.name, item.value)];
    case 'RUN_FINISHED':
      return typeof item.text === 'string' ? [live('TEXT_RESULT', { text: item.text })] : [];
  }
  return [];
}

/** A content event as the SDK stream part it stands for, or null. */
function chunkOf(e: { type: string; [key: string]: any }): Record<string, unknown> | null {
  switch (e.type) {
    case 'TEXT_MESSAGE_CONTENT':
      return { type: 'text-delta', textDelta: e.delta };
    case 'REASONING_CONTENT':
      return { type: 'reasoning', textDelta: e.delta };
    case 'TOOL_CALL_START':
      return { type: 'tool-call-streaming-start', toolCallId: e.toolCallId, toolName: e.toolName };
    case 'TOOL_CALL_ARGS':
      return { type: 'tool-call-delta', toolCallId: e.toolCallId, argsTextDelta: e.delta };
    case 'TOOL_CALL_END':
      return { type: 'tool-call', toolCallId: e.toolCallId, toolName: e.toolName, args: e.args };
    case 'TOOL_CALL_RESULT':
      return { type: 'tool-result', toolCallId: e.toolCallId, toolName: e.toolName, result: e.result };
    case 'SOURCE':
      return { type: 'source', source: e.source };
  }
  return null;
}
