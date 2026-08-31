// ports — the interfaces users implement
export type { Storage } from './ports/storage.js';
export type { EventBus } from './ports/bus.js';
export type { Queue } from './ports/queue.js';
export type { Kv } from './ports/kv.js';
export type {
  AgentCore,
  AgentHandle,
  AgentKind,
  GenerateTextAgentSpec,
  RespondInput,
  RespondResult,
  ResolvedModel,
  RunInput,
  RunResult,
  RuntimeOptions,
  RuntimePorts,
  StopResult,
  StreamTextAgentSpec,
  SubagentsConfig,
  ThreadSnapshot,
} from './ports/runtime.js';

// runtime — the factory that binds ports to behaviors
export { setupAgentCore } from './runtime.js';
/** @deprecated use `setupAgentCore` — removed in the next minor (§7) */
export { createAgentRuntime } from './runtime.js';

// core — behaviors, all ports-only
export { execute, executeWithPolicy, finalize, markRequiresConfirmation } from './core/engine.js';
export { countTokens } from './core/usage.js';
export {
  waitForEvent,
  suspendForApproval,
  respond,
  HITL_TTL_MS,
  type HitlResponse,
  type SuspendInput,
  type TimeoutOutcome,
} from './core/hitl.js';
export { reclaimIfOrphaned } from './core/reclaim.js';
export { contextBudget, compactContext, CONTEXT_TOKEN_CEILING } from './core/context.js';
export { Semaphore, spawnSubagentTool, type SubagentCtx } from './core/subagent.js';
export { run } from './core/run.js';
export { stop } from './core/stop.js';

// types
export {
  DEFAULT_CONFIG,
  resolveConfig,
  type AgentConfig,
  type AgentEvent,
  type ExecutionState,
  type MessageDTO,
  type MessageRole,
  type NewMessage,
  type NewRun,
  type NewUsage,
  type ResolvedModel as ResolvedModelDTO,
  type RunDTO,
  type RunJob,
  type ThreadDTO,
} from './core/types.js';

// reference adapters
export { PrismaStorage, type PrismaLike } from './adapters/prisma.js';
export { UpstashBus, UpstashKv, THREAD_CHANNEL, type UpstashRedisLike, type UpstashSubscriberLike } from './adapters/upstash.js';
export { RedisBus, RedisKv, type RedisClientLike, type RedisSubscriberLike } from './adapters/redis.js';
export { QStashQueue, type QStashLike, type QStashQueueOptions } from './adapters/qstash.js';
export { MemoryStorage, MemoryBus, MemoryQueue, MemoryKv } from './adapters/memory.js';
