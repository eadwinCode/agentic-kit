// ports — the interfaces users implement
export type { Storage } from './ports/storage.js';
export type { EventBus } from './ports/bus.js';
export {
  PRIORITY_LOW,
  QueueFullError,
  PayloadTooLargeError,
  DuplicateJobError,
  UnsupportedError,
  type EnqueueOptions,
  type Queue,
  type QueueStats,
  type QueuedJob,
} from './ports/queue.js';
export type { Kv } from './ports/kv.js';
export {
  StreamClosedError,
  StreamGoneError,
  type RunStreams,
  type StreamMeta,
  type StreamSnapshot,
} from './ports/streams.js';
export * from './core/stream-events.js';
export type { SnapshotStream } from './core/snapshot.js';
export { pruneEvents, STREAM_ONLY_TYPES, type PruneOptions, type PruneReport } from './core/prune.js';
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
  Attachment,
  RefusedReason,
  RuntimeOptions,
  RuntimePorts,
  StopResult,
  StreamTextAgentSpec,
  SubagentsConfig,
  ThreadSnapshot,
  FollowStartOptions,
  ThreadUsage,
  Pricer,
  Logger,
  RunFinishInfo,
  SettleFn,
  SystemFn,
  PrepareStepFn,
  ReclaimReport,
} from './ports/runtime.js';

// runtime — the factory that binds ports to behaviors
export { setupAgentCore, UnknownAgentError } from './runtime.js';

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
  followThread,
  followFrame,
  formatCursor,
  parseCursor,
  toFollowSse,
  toSseStream,
  sseFrame,
  SSE_HEADERS,
  type FollowFrame,
  type FollowOptions,
  type FollowThreadOptions,
  type SseOptions,
  type SseStream,
  type ThreadCursor,
} from './core/follow.js';
// The run-state types, named here so they are discoverable rather than
// reachable only by chance through another module's re-export.
export type { AgentRunState, BoundStorage, StorageContext } from './core/state.js';
export type { AdminThread, ThreadStart, StepRecord, RunFilter, AdminThreadFilter, RunCursor, RunDeltas, RunTotals } from './ports/admin.js';
export type { BillingCheck } from './core/types.js';
export { claimRun, redriveKey, runIdKey } from './core/keys.js';
export { countTokens, sumUsage, emptyTotals, UsageMerger, type UsageGroup } from './core/usage.js';

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
  type RunBudget,
  type AgentEvent,
  type ExecutionState,
  type MessageDTO,
  type MessageRole,
  type NewMessage,
  type NewRunRecord,
  type NewUsage,
  type UsageLine,
  type SubagentProfile,
  type CurrencyCost,
  type UsageFilter,
  type UsageKind,
  type UsageOutcome,
  type Cost,
  type ResolvedModel as ResolvedModelDTO,
  type RunPatch,
  type RunRecord,
  type RunJob,
  type JobKind,
  type ResumeInfo,
  type ContextUsage,
  type NestedDescriptor,
  type ThreadDTO,
  type UsageTotals,
} from './core/types.js';

// reference adapters
export { PrismaStorage, PrismaRunStreams, type PrismaLike, type PrismaStreamsLike } from './adapters/prisma.js';
export { UpstashBus, UpstashKv, UpstashRunStreams, THREAD_CHANNEL, type UpstashRedisLike, type UpstashSubscriberLike } from './adapters/upstash.js';
export { RedisBus, RedisKv, RedisRunStreams, type RedisClientLike, type RedisSubscriberLike } from './adapters/redis.js';
export { QStashQueue, type QStashLike, type QStashQueueOptions } from './adapters/qstash.js';
export { MemoryStorage, MemoryBus, MemoryQueue, MemoryKv, MemoryRunStreams } from './adapters/memory.js';
