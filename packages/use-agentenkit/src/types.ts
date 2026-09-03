/** The shapes a UI renders. Nothing here knows how the data arrived — the
 *  transport lives in `config.ts`, so an app can move every endpoint without
 *  touching a component. */

export type AgentState =
  | 'IDLE'
  | 'RUNNING'
  | 'WAITING_FOR_INPUT'
  | 'CANCELLED'
  | 'COMPLETED'
  | 'FAILED';

export type MessageRole = 'user' | 'assistant' | 'system' | 'tool';

/** One structured piece of an entry, for a UI that renders more than text:
 *  a tool card with its state, an image the user attached, the thinking.
 *  Filled the same way from the durable snapshot and from the live stream,
 *  so a reload and a live run produce the same shape. */
export type EntryPart =
  | { type: 'text'; text: string }
  | { type: 'reasoning'; text: string }
  | { type: 'image'; image: string; mimeType?: string }
  | {
      type: 'tool-call';
      toolCallId: string;
      toolName: string;
      args: unknown;
      /** `running` until its result lands; `done` or `error` after. */
      state: 'streaming' | 'running' | 'done' | 'error';
      result?: unknown;
    }
  | { type: 'tool-result'; toolCallId: string; toolName?: string; result: unknown };

export interface ChatEntry {
  id: string;
  /** `reasoning` is the model's thinking, which arrives as its own stream and
   *  is kept apart from the answer so a UI can fold it away. */
  kind: 'text' | 'tool' | 'reasoning';
  role: MessageRole;
  text: string;
  agentId?: string | null;
  /** The entry's structured parts. `text` stays the flat rendering. */
  parts: EntryPart[];
}

/** An image sent with a prompt: a URL the provider can fetch, or a data: URL. */
export interface Attachment {
  url: string;
  mediaType?: string;
}

export type ActivityPhase =
  | 'idle'
  | 'loading'
  | 'thinking'
  | 'responding'
  | 'tool-call'
  | 'tool-result'
  | 'waiting-input'
  | 'completed'
  | 'stopped'
  | 'failed';

export interface AgentActivity {
  phase: ActivityPhase;
  label: string;
  detail?: string;
}

export type SubagentStatus =
  | 'RUNNING'
  | 'WAITING_FOR_INPUT'
  | 'COMPLETED'
  | 'FAILED'
  | 'CANCELLED';

export interface SubagentView {
  agentId: string;
  name: string;
  /** 1 = spawned by the main agent; deeper runs nest further. */
  depth: number;
  status: SubagentStatus;
  text: string;
  /** Why it died — SUBAGENT_FAILED carries the reason. */
  error?: string;
}

export interface UsageTotals {
  inputTokens: number;
  cachedInputTokens: number;
  outputTokens: number;
  totalTokens: number;
  /** What the thread cost so far, in millionths of `currency`: 1_000_000 is
   *  one dollar when the currency is USD. Zero when the server has no pricer
   *  configured — check `unpriced` to tell that apart from a free thread. */
  costMicros?: number;
  /** The unit `costMicros` is in, absent when nothing was priced. */
  currency?: string;
  /** How many model calls had no cost on them. Above zero, `costMicros` is a
   *  floor and not the whole bill. */
  unpriced?: number;
  /** The same spend grouped by agent and model — one line per pair, the shape
   *  a bill or a per-agent breakdown wants. */
  lines?: UsageLine[];
}

/** One agent's spend on one model, summed over its calls. */
export interface UsageLine {
  agentId?: string | null;
  agentName?: string | null;
  model?: string | null;
  modelId?: string | null;
  inputTokens: number;
  cacheReadInputTokens: number;
  cacheWriteInputTokens: number;
  outputTokens: number;
  reasoningTokens: number;
  calls: number;
  /** How many of those calls had estimated tokens, because they were cut off
   *  before the provider reported real ones. */
  estimated: number;
  costMicros: number;
}

export interface ContextUsage {
  usedTokens: number;
  budgetTokens: number;
  compactAtTokens: number;
  messages: number;
}

export interface ThreadUsage {
  tokens: UsageTotals;
  context: ContextUsage;
  model: string;
}

export interface PendingInput {
  toolCallId: string;
  toolName: string;
  /** `approval` when a person decides; anything else is a tool that parked
   *  itself on work it started (a job, a build) and is resumed by whoever
   *  finishes it. A UI shows a card for the first and a status for the rest. */
  reason: string;
  /** When the request stops being answerable, ISO time; absent on a park
   *  recorded before per-park deadlines. */
  expiresAt?: string;
  /** The stream that asked — null when the main agent did. */
  agentId?: string | null;
  /** The nested run's own name and depth, straight off the park, so a card can
   *  say "mailer" rather than an opaque id. */
  agentName?: string;
  depth?: number;
  arguments: unknown;
}

export interface ThreadListItem {
  id: string;
  title: string;
  state: AgentState;
  model: string;
  updatedAt: string;
}

export interface SnapshotMessage {
  id: string;
  role: MessageRole;
  content: unknown;
  agentId?: string | null;
}

export interface StreamEvent {
  seq: number;
  type: string;
  payload: any;
}

export interface SnapshotRun {
  id: string;
  agent: string;
  /** 0 is the dispatched run itself; nested runs are 1+. */
  depth: number;
  state: SubagentStatus;
}

export interface ThreadSnapshot {
  thread: { id: string; state: AgentState };
  messages: SnapshotMessage[];
  runs: SnapshotRun[];
  lastEventSeq: number;
  activeEvents: StreamEvent[];
}

export interface RunResult {
  accepted: boolean;
  threadId?: string;
  runId?: string;
  error?: string;
}
