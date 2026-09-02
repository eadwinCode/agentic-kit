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

export interface ChatEntry {
  id: string;
  /** `reasoning` is the model's thinking, which arrives as its own stream and
   *  is kept apart from the answer so a UI can fold it away. */
  kind: 'text' | 'tool' | 'reasoning';
  role: MessageRole;
  text: string;
  agentId?: string | null;
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
