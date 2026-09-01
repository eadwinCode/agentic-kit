// ports — the interfaces users implement
export type { Storage } from './ports/storage.js';
export type { EventBus } from './ports/bus.js';
export type { EnqueueOptions, Queue } from './ports/queue.js';
export type { Kv } from './ports/kv.js';
export type {
  AgentCore,
  AgentHandle,
  AgentKind,
  DeleteThreadResult,
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
  ThreadUsage,
} from './ports/runtime.js';

// runtime — the factory that binds ports to behaviors
export { setupAgentCore } from './runtime.js';

// core — behaviors, all ports-only
export {
  execute,
  executeStep,
  executeWithPolicy,
  finalize,
  markRequiresConfirmation,
  type ExecuteInput,
  type ExecuteOutcome,
  type FinalizeInput,
  type StepResult,
} from './core/engine.js';
export { claimRun, redriveKey, runIdKey } from './core/keys.js';
export { countTokens } from './core/usage.js';
export {
  respond,
  parkForApproval,
  loadPendingHitl,
  HITL_PARKED,
  HITL_TTL_MS,
  hitlKey,
  type HitlResponse,
  type ParkInput,
  type PendingHitl,
} from './core/hitl.js';
export { reclaimIfOrphaned } from './core/reclaim.js';
export { contextBudget, contextUsage, compactContext, CONTEXT_TOKEN_CEILING } from './core/context.js';
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
  type ResumeInfo,
  type ContextUsage,
  type ThreadDTO,
  type UsageTotals,
} from './core/types.js';

// reference adapters
export { PrismaStorage, type PrismaLike } from './adapters/prisma.js';
export { UpstashBus, UpstashKv, THREAD_CHANNEL, type UpstashRedisLike, type UpstashSubscriberLike } from './adapters/upstash.js';
export { RedisBus, RedisKv, type RedisClientLike, type RedisSubscriberLike } from './adapters/redis.js';
export { QStashQueue, type QStashLike, type QStashQueueOptions } from './adapters/qstash.js';
export { MemoryStorage, MemoryBus, MemoryQueue, MemoryKv } from './adapters/memory.js';
