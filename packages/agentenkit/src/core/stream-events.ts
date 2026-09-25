/** The events a run stream carries (see RunStreams). One typed union: a
 *  reader checks `type` and the compiler knows the fields. The shapes follow
 *  AG-UI, so an AG-UI encoder only renames a few fields. The Go runtime has
 *  the same events, with the same names and the same JSON. */

/** Tokens a step or a run used. */
export interface StreamUsage {
  inputTokens: number;
  cachedInputTokens: number;
  outputTokens: number;
  totalTokens: number;
}

/** Money spent in one currency, in millionths of a unit. */
export interface StreamCost {
  currency: string;
  micros: number;
}

export interface RunStartedEvent {
  type: 'RUN_STARTED';
  threadId: string;
  runId: string;
  streamId: string;
  /** 1 for the first pickup, 2 for the resume after a park, and so on. */
  segment: number;
}

export interface TextMessageStartEvent {
  type: 'TEXT_MESSAGE_START';
  messageId: string;
  role: 'assistant';
}
export interface TextMessageContentEvent {
  type: 'TEXT_MESSAGE_CONTENT';
  messageId: string;
  delta: string;
}
export interface TextMessageEndEvent {
  type: 'TEXT_MESSAGE_END';
  messageId: string;
}

export interface ReasoningStartEvent {
  type: 'REASONING_START';
  messageId: string;
}
export interface ReasoningContentEvent {
  type: 'REASONING_CONTENT';
  messageId: string;
  delta: string;
}
export interface ReasoningEndEvent {
  type: 'REASONING_END';
  messageId: string;
}

export interface ToolCallStartEvent {
  type: 'TOOL_CALL_START';
  toolCallId: string;
  toolName: string;
}
export interface ToolCallArgsEvent {
  type: 'TOOL_CALL_ARGS';
  toolCallId: string;
  delta: string;
}
/** The call is complete. `args` is the whole, parsed argument object, so a
 *  reader that missed the ARGS deltas still has it. */
export interface ToolCallEndEvent {
  type: 'TOOL_CALL_END';
  toolCallId: string;
  toolName: string;
  args: unknown;
}
export interface ToolCallResultEvent {
  type: 'TOOL_CALL_RESULT';
  toolCallId: string;
  toolName: string;
  result: unknown;
}

/** A source the model cited (a web page, a document). Not in AG-UI: the
 *  encoder sends it as CUSTOM. */
export interface SourceEvent {
  type: 'SOURCE';
  source: unknown;
}

/** One model call is done and its messages are saved. */
export interface StepFinishedEvent {
  type: 'STEP_FINISHED';
  /** 1-based, per agent. */
  step: number;
  /** null for the main agent, the nested run's id otherwise. */
  agentId: string | null;
  finishReason: string;
  usage: StreamUsage;
}

export interface MessageAppendedEvent {
  type: 'MESSAGE_APPENDED';
  message: {
    id: string;
    role: string;
    content: unknown;
    agentId: string | null;
    createdAt: string;
  };
  /** The sender's own name for the message, echoed back. */
  clientMessageId?: string;
}

/** A tool is waiting for a person. Also kept in the thread record. */
export interface InputRequiredEvent {
  type: 'INPUT_REQUIRED';
  toolCallId: string;
  toolName: string;
  agentId: string | null;
  arguments: unknown;
  inputSchema: unknown;
  /** The unwind chain for a nested park; empty for the main agent. */
  frames: unknown[];
  nested?: unknown;
  /** The dispatch ticket the answer resumes with. */
  resume?: unknown;
  reason: string;
  expiresAt?: string;
}

export interface SubagentStartedEvent {
  type: 'SUBAGENT_STARTED';
  subagentId: string;
  name: string;
  depth: number;
}
/** One event from a nested run, wrapped. */
export interface SubagentEventEvent {
  type: 'SUBAGENT_EVENT';
  subagentId: string;
  event: StreamEvent;
}
export interface SubagentFinishedEvent {
  type: 'SUBAGENT_FINISHED';
  subagentId: string;
  /** `cancelled`: a user stop reached the child. */
  status: 'completed' | 'failed' | 'cancelled';
  error?: string;
}

/** An app's own event, sent with publishEvent. */
export interface CustomStreamEvent {
  type: 'CUSTOM';
  name: string;
  value: unknown;
}

/** The run's segment ended normally. Always the last item of a stream. */
export interface RunFinishedEvent {
  type: 'RUN_FINISHED';
  status: 'finished' | 'parked' | 'stopped';
  usage?: StreamUsage;
  costs?: StreamCost[];
  finishReason?: string;
  /** The final text of a one-shot agent. */
  text?: string;
}

/** The run's segment ended badly. Always the last item of a stream. */
export interface RunErrorEvent {
  type: 'RUN_ERROR';
  /** `lost`: the worker died and the sweep closed its stream. */
  status: 'error' | 'lost';
  error: string;
}

export type StreamEvent =
  | RunStartedEvent
  | TextMessageStartEvent
  | TextMessageContentEvent
  | TextMessageEndEvent
  | ReasoningStartEvent
  | ReasoningContentEvent
  | ReasoningEndEvent
  | ToolCallStartEvent
  | ToolCallArgsEvent
  | ToolCallEndEvent
  | ToolCallResultEvent
  | SourceEvent
  | StepFinishedEvent
  | MessageAppendedEvent
  | InputRequiredEvent
  | SubagentStartedEvent
  | SubagentEventEvent
  | SubagentFinishedEvent
  | CustomStreamEvent
  | RunFinishedEvent
  | RunErrorEvent;

export type StreamEventType = StreamEvent['type'];

/** How a stream ends: the last item it will ever hold. */
export type StreamEnd = RunFinishedEvent | RunErrorEvent;

/** An event as a stream holds it: with the offset the store gave it. */
export type StreamItem = StreamEvent & { offset: string };

/** True for the event that ends a stream. */
export function isStreamEnd(e: { type: string }): e is StreamEnd {
  return e.type === 'RUN_FINISHED' || e.type === 'RUN_ERROR';
}

/** A run segment's stream id: `<runId>:<segment>`. A park closes a
 *  segment's stream and the resume opens the next, so a closed stream never
 *  opens again. */
export function streamIdOf(runId: string, segment: number): string {
  return `${runId}:${segment}`;
}

/** The run id and segment in a stream id, or null for one not made by
 *  streamIdOf. */
export function parseStreamId(streamId: string): { runId: string; segment: number } | null {
  const at = streamId.lastIndexOf(':');
  if (at <= 0) return null;
  const segment = Number(streamId.slice(at + 1));
  if (!Number.isInteger(segment) || segment < 1) return null;
  return { runId: streamId.slice(0, at), segment };
}
