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
  Pricer,
  Logger,
  RunFinishInfo,
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
export { agentTool, type ToolContext } from './core/tools.js';
export {
  followEvents,
  toSseStream,
  sseFrame,
  SSE_HEADERS,
  type FollowOptions,
  type SseOptions,
  type SseStream,
} from './core/follow.js';
// The run-state types, named here so they are discoverable rather than
// reachable only by chance through another module's re-export.
export type { AgentRunState, BoundStorage, StorageContext } from './core/state.js';
export type { AdminThread, ThreadStart, StepRecord, RunFilter, AdminThreadFilter } from './ports/admin.js';
export type { BillingCheck } from './core/types.js';
export { claimRun, redriveKey, runIdKey } from './core/keys.js';
export { countTokens, sumUsage, emptyTotals } from './core/usage.js';

/** Pricing (§4): the pricers that ship with the platform. `pricing.table(...)`
 *  is the common case; see the module for `receipt` and `chain`. */
export * as pricing from './pricing.js';
export type { ModelPrice, PriceTable, ReceiptReader } from './pricing.js';
export {
  respond,
  parkForApproval,
  parkForInput,
  ToolParkedError,
  REASON_APPROVAL,
  hitlDeadline,
  isApprovalPark,
  type ParkRequest,
  loadPendingHitl,
  loadOpenHitls,
  withHitl,
  HITL_PARKED,
  HITL_TTL_MS,
  hitlKey,
  type HitlFrame,
  type HitlResponse,
  type ParkInput,
  type PendingHitl,
} from './core/hitl.js';
export { reclaimIfOrphaned } from './core/reclaim.js';
export { contextBudget, contextUsage, compactContext, CONTEXT_TOKEN_CEILING } from './core/context.js';
export { Semaphore, runNestedAgent, spawnSubagentTool, type SubagentCtx } from './core/subagent.js';
export { run } from './core/run.js';
export {
  publishEvent,
  withPublishEvent,
  RESERVED_EVENT_TYPES,
  type PublishEventOptions,
  type ToolPublishEvent,
} from './core/publish.js';
export { stop } from './core/stop.js';
export { repairDanglingToolCalls, DANGLING_CALL_RESULT } from './core/messages.js';

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
  type NewRunRecord,
  type NewUsage,
  type UsageLine,
  type UsageFilter,
  type UsageKind,
  type UsageOutcome,
  type Cost,
  type ResolvedModel as ResolvedModelDTO,
  type RunPatch,
  type RunRecord,
  type RunJob,
  type ResumeInfo,
  type ContextUsage,
  type NestedDescriptor,
  type ThreadDTO,
  type UsageTotals,
} from './core/types.js';

// reference adapters
export { PrismaStorage, type PrismaLike } from './adapters/prisma.js';
export { UpstashBus, UpstashKv, THREAD_CHANNEL, type UpstashRedisLike, type UpstashSubscriberLike } from './adapters/upstash.js';
export { RedisBus, RedisKv, type RedisClientLike, type RedisSubscriberLike } from './adapters/redis.js';
export { QStashQueue, type QStashLike, type QStashQueueOptions } from './adapters/qstash.js';
export { MemoryStorage, MemoryBus, MemoryQueue, MemoryKv } from './adapters/memory.js';
