import type { AgentEvent, AgentConfig, ExecutionState } from '../core/types.js';
import type { Storage } from './storage.js';
import type { EventBus } from './bus.js';
import type { Queue } from './queue.js';
import type { Kv } from './kv.js';

/** The ports bundle — everything in core/ receives this and nothing else. */
export interface RuntimePorts {
  storage: Storage;
  bus: EventBus;
  queue: Queue;
  kv: Kv;
  models: Record<string, unknown>;
  config: AgentConfig;
}

export interface RuntimeOptions {
  storage: Storage;
  bus: EventBus;
  queue: Queue;
  kv: Kv;
  /** Any `ai`-SDK models users register (§2.3). `gpt-4o` is the default key. */
  models: Record<string, unknown>;
  config?: Partial<AgentConfig>;
}

export interface RunInput {
  /** Omit to create a fresh thread first (threads.create, §3.2) */
  threadId?: string;
  prompt: string;
  model: string;
}

export interface RunResult {
  accepted: boolean;
  threadId: string;
  state?: ExecutionState;
  error?: string;
}

export interface StopResult {
  accepted: boolean;
  error?: string;
}

export interface RespondInput {
  threadId: string;
  toolCallId: string;
  approved: boolean;
  payload?: unknown;
}

export interface RespondResult {
  delivered: boolean;
  error?: string;
}

export interface AgentRuntime {
  run(input: RunInput): Promise<RunResult>;
  stop(threadId: string): Promise<StopResult>;
  hitl: {
    respond(input: RespondInput): Promise<RespondResult>;
    reclaimIfOrphaned(threadId: string): Promise<boolean>;
  };
  events: {
    since(threadId: string, sinceSeq: number): Promise<AgentEvent[]>;
    subscribe(threadId: string, handler: (event: AgentEvent) => void): Promise<() => void>;
  };
  engine: {
    /** Worker-side only (§5.6). Throws on failure — see executeWithPolicy.
     *  Returns 'lock-conflict' when another worker owns the thread's run lock
     *  (at-least-once duplicate delivery, §2.8) and nothing was executed. */
    execute(input: { threadId: string; model: string }): Promise<'executed' | 'lock-conflict'>;
    /** execute + §2.8 failure policy: redrive < maxAttempts, else finalize FAILED */
    executeWithPolicy(
      input: { threadId: string; model: string },
      policy?: { maxAttempts?: number },
    ): Promise<void>;
  };
}
