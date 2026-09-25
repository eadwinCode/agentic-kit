import { parseStreamId } from './stream-events.js';
import type { FollowFrame } from './follow.js';

/** The wire formats a follow can be sent in: our own frames (the default),
 *  or AG-UI events for a client built on that protocol. */
export type WireFormat = 'agentenkit' | 'ag-ui';

/** What the AG-UI encoder remembers across frames: the tool calls whose
 *  arguments already went out as deltas. */
export interface AgUiState {
  threadId: string;
  argsSent: Set<string>;
}

export function agUiState(threadId: string): AgUiState {
  return { threadId, argsSent: new Set() };
}

/** A follow frame as AG-UI events. Our stream events already follow AG-UI,
 *  so most keep their shape and only a few fields are renamed; what AG-UI
 *  has no event for (a park, a subagent, a thread notice, a snapshot) goes
 *  out as CUSTOM with our name. The TS and Go runtimes send the same. */
export function toAgUi(frame: FollowFrame, state: AgUiState): Record<string, unknown>[] {
  const custom = (name: string, value: unknown) => [{ type: 'CUSTOM', name, value }];
  if (frame.kind === 'thread') return custom(frame.event.type, frame.event.payload ?? null);
  if (frame.kind === 'snapshot') return custom('SNAPSHOT', frame.snapshot);

  const { offset: _offset, ...e } = frame.item as { offset: string } & Record<string, any>;
  const runId = parseStreamId(frame.streamId)?.runId ?? frame.streamId;
  switch (e.type) {
    case 'RUN_STARTED':
      return [{ type: 'RUN_STARTED', threadId: state.threadId, runId }];
    case 'TEXT_MESSAGE_START':
    case 'TEXT_MESSAGE_CONTENT':
    case 'TEXT_MESSAGE_END':
      return [e];
    case 'REASONING_START':
      return [{ type: 'THINKING_TEXT_MESSAGE_START' }];
    case 'REASONING_CONTENT':
      return [{ type: 'THINKING_TEXT_MESSAGE_CONTENT', delta: e.delta }];
    case 'REASONING_END':
      return [{ type: 'THINKING_TEXT_MESSAGE_END' }];
    case 'TOOL_CALL_START':
      return [{ type: 'TOOL_CALL_START', toolCallId: e.toolCallId, toolCallName: e.toolName }];
    case 'TOOL_CALL_ARGS':
      state.argsSent.add(e.toolCallId);
      return [{ type: 'TOOL_CALL_ARGS', toolCallId: e.toolCallId, delta: e.delta }];
    case 'TOOL_CALL_END': {
      // AG-UI builds the arguments from ARGS deltas alone: a call whose
      // arguments came whole sends them as one delta first.
      const out: Record<string, unknown>[] = [];
      if (!state.argsSent.has(e.toolCallId)) {
        out.push({ type: 'TOOL_CALL_ARGS', toolCallId: e.toolCallId, delta: JSON.stringify(e.args ?? {}) });
      }
      state.argsSent.delete(e.toolCallId);
      out.push({ type: 'TOOL_CALL_END', toolCallId: e.toolCallId });
      return out;
    }
    case 'TOOL_CALL_RESULT':
      return [{
        type: 'TOOL_CALL_RESULT',
        messageId: `${e.toolCallId}:result`,
        toolCallId: e.toolCallId,
        content: typeof e.result === 'string' ? e.result : JSON.stringify(e.result ?? null),
        role: 'tool',
      }];
    case 'STEP_FINISHED':
      return [{ type: 'STEP_FINISHED', stepName: `step ${e.step}` }];
    case 'CUSTOM':
      return custom(e.name, e.value ?? null);
    case 'RUN_FINISHED': {
      const { type: _t, ...result } = e;
      return [{ type: 'RUN_FINISHED', threadId: state.threadId, runId, result }];
    }
    case 'RUN_ERROR':
      return [{ type: 'RUN_ERROR', message: e.error, code: e.status }];
  }
  // A source, a saved message, a park, a subagent: ours, by name.
  const { type, ...value } = e;
  return custom(type, value);
}
