// Package agentenkit is a durable runtime for AI agent runs, as a library.
//
// It does not own your prompts, models, or tools; goai does. It owns the
// lifecycle of a run: that a run outlives the request that started it,
// survives a worker dying mid-step, can be stopped, parked for a human,
// resumed exactly where it stopped, nested, metered, and watched by several
// people at once.
//
// This is the Go port of the TypeScript agentenkit package, file for file:
//
//	ports/     the four ports you implement, the admin store, every DTO
//	core/      the behaviors (engine, loop, HITL, subagents, follow, admin reads)
//	adapters/  memory, inline, sqlite, postgres, redis, qstash, upstash
//	admin/     memory, sqlite, postgres operational stores
//
// This root package re-exports the public surface, the way index.ts does,
// and holds SetupAgentCore (runtime.ts). Adapters are imported from their
// own packages so a program only compiles the drivers it uses.
package agentenkit

import (
	"context"

	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/core"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

// Ports: the interfaces users implement.
type (
	Storage      = ports.Storage
	ThreadStore  = ports.ThreadStore
	MessageStore = ports.MessageStore
	EventStore   = ports.EventStore
	UsageStore   = ports.UsageStore
	ThreadInit   = ports.ThreadInit
	MessageScope = ports.MessageScope

	EventBus       = ports.EventBus
	Queue          = ports.Queue
	EnqueueOptions = ports.EnqueueOptions
	QueueStats     = ports.QueueStats
	QueuedJob      = ports.QueuedJob
	JobKind        = ports.JobKind
	Kv             = ports.Kv
	RunStreams     = ports.RunStreams
	StreamItem     = ports.StreamItem
	StreamEvent    = ports.StreamEvent
	StreamEnd      = ports.StreamEnd
	StreamMeta     = ports.StreamMeta
	SnapshotStream = ports.SnapshotStream
	SetOptions     = ports.SetOptions

	AdminStore        = ports.AdminStore
	AdminThreadStore  = ports.AdminThreadStore
	RunStore          = ports.RunStore
	StepStore         = ports.StepStore
	AdminThread       = ports.AdminThread
	ThreadStart       = ports.ThreadStart
	NewAdminThread    = ports.NewAdminThread
	AdminThreadFilter = ports.AdminThreadFilter
	RunFilter         = ports.RunFilter
	StepRecord        = ports.StepRecord
	NewStepRecord     = ports.NewStepRecord
	StepToolCall      = ports.StepToolCall

	RuntimePorts          = ports.RuntimePorts
	RuntimeOptions        = ports.RuntimeOptions
	RunInput              = ports.RunInput
	RunResult             = ports.RunResult
	StopResult            = ports.StopResult
	DeleteThreadResult    = ports.DeleteThreadResult
	RespondInput          = ports.RespondInput
	RespondResult         = ports.RespondResult
	ThreadUsage           = ports.ThreadUsage
	ThreadSnapshot        = ports.ThreadSnapshot
	StreamTextAgentSpec   = ports.StreamTextAgentSpec
	GenerateTextAgentSpec = ports.GenerateTextAgentSpec
	RunFinishInfo         = ports.RunFinishInfo
	SubagentsConfig       = ports.SubagentsConfig
	SubagentProfile       = ports.SubagentProfile
	Attachment            = ports.Attachment
	SystemFunc            = ports.SystemFunc
	PrepareStepFunc       = ports.PrepareStepFunc
	SettleFunc            = ports.SettleFunc

	AgentRunState  = ports.AgentRunState
	StorageContext = ports.StorageContext
	BoundStorage   = ports.BoundStorage
)

// Types: the DTOs that cross the ports.
type (
	ExecutionState   = ports.ExecutionState
	MessageRole      = ports.MessageRole
	AgentKind        = ports.AgentKind
	ThreadDTO        = ports.ThreadDTO
	MessageDTO       = ports.MessageDTO
	NewMessage       = ports.NewMessage
	AgentEvent       = ports.AgentEvent
	RunRecord        = ports.RunRecord
	NewRunRecord     = ports.NewRunRecord
	RunPatch         = ports.RunPatch
	UsageTotals      = ports.UsageTotals
	ContextUsage     = ports.ContextUsage
	NewUsage         = ports.NewUsage
	UsageLine        = ports.UsageLine
	UsageFilter      = ports.UsageFilter
	UsageKind        = ports.UsageKind
	UsageOutcome     = ports.UsageOutcome
	Cost             = ports.Cost
	Pricer           = ports.Pricer
	PricerFunc       = ports.PricerFunc
	ProviderOptions  = ports.ProviderOptions
	RunJob           = ports.RunJob
	NestedDescriptor = ports.NestedDescriptor
	ResumeInfo       = ports.ResumeInfo
	ResolvedModel    = ports.ResolvedModel
	Tool             = ports.Tool
	AgentConfig      = ports.AgentConfig
	BillingCheck     = ports.BillingCheck
	BillingStage     = ports.BillingStage
	RunBudget        = ports.RunBudget

	Search           = ports.Search
	SearchOptions    = ports.SearchOptions
	SearchHit        = ports.SearchHit
	SearchRecency    = ports.SearchRecency
	Fetcher          = ports.Fetcher
	FetchOptions     = ports.FetchOptions
	FetchedPage      = ports.FetchedPage
	BuiltinToolPorts = ports.BuiltinToolPorts

	SandboxProvider      = ports.SandboxProvider
	Sandbox              = ports.Sandbox
	SandboxFileSystem    = ports.SandboxFileSystem
	SandboxInfo          = ports.SandboxInfo
	SandboxNetwork       = ports.SandboxNetwork
	SandboxMetadata      = ports.SandboxMetadata
	SandboxResources     = ports.SandboxResources
	CreateSandboxOptions = ports.CreateSandboxOptions
	RunCommandOptions    = ports.RunCommandOptions
	CommandResult        = ports.CommandResult
	FileEntry            = ports.FileEntry
)

const (
	KindStep       = ports.KindStep
	KindCompaction = ports.KindCompaction
	KindTool       = ports.KindTool

	UsageFinished = ports.UsageFinished
	UsageAborted  = ports.UsageAborted
	UsageErrored  = ports.UsageErrored

	StateIdle            = ports.StateIdle
	StateQueued          = ports.StateQueued
	StateRunning         = ports.StateRunning
	StateWaitingForInput = ports.StateWaitingForInput
	StateCancelled       = ports.StateCancelled
	StateCompleted       = ports.StateCompleted
	StateFailed          = ports.StateFailed

	RoleUser      = ports.RoleUser
	RoleAssistant = ports.RoleAssistant
	RoleSystem    = ports.RoleSystem
	RoleTool      = ports.RoleTool

	KindStreamText   = ports.KindStreamText
	KindGenerateText = ports.KindGenerateText

	JobDispatch = ports.JobDispatch
	JobRetry    = ports.JobRetry
	JobRedrive  = ports.JobRedrive
	JobResume   = ports.JobResume
	JobExpiry   = ports.JobExpiry
	JobReclaim  = ports.JobReclaim
	PriorityLow = ports.PriorityLow

	BillingAtDispatch = ports.BillingAtDispatch
	BillingAtPickup   = ports.BillingAtPickup

	RefusedActiveRun = ports.RefusedActiveRun
	RefusedQueueFull = ports.RefusedQueueFull
	RefusedBilling   = ports.RefusedBilling
)

// Errors a queue answers with, so a host can tell them apart.
var (
	ErrQueueFull       = ports.ErrQueueFull
	ErrPayloadTooLarge = ports.ErrPayloadTooLarge
	ErrDuplicateJob    = ports.ErrDuplicateJob
	ErrUnsupported     = ports.ErrUnsupported
)

var (
	// MainAgent scopes a message listing to the main agent's stream.
	MainAgent = ports.MainAgent
	// AgentScope scopes a message listing to one nested run's stream.
	AgentScope = ports.AgentScope
	// DefaultConfig returns the shipped defaults.
	DefaultConfig = ports.DefaultConfig
	// ResolveConfig validates a config.
	ResolveConfig = ports.ResolveConfig
	// WrapTool lifts a plain goai tool into the platform's Tool.
	WrapTool = ports.WrapTool
	// WrapTools lifts several goai tools.
	WrapTools = ports.WrapTools
	// BindStorage attaches a run's context to every storage method once.
	BindStorage = ports.BindStorage
	// MergeProviderOptions is a shallow per-provider merge.
	MergeProviderOptions = ports.MergeProviderOptions
)

// Ptr is a small helper for building a RunPatch.
func Ptr[T any](v T) *T { return ports.Ptr(v) }

// Core: behaviors, all ports-only.
type (
	ExecuteInput   = core.ExecuteInput
	ExecuteOutcome = core.ExecuteOutcome
	StopOptions    = core.StopOptions

	FinalizeInput = core.FinalizeInput
	Policy        = core.Policy
	ExecuteFunc   = core.ExecuteFunc
	StepResult    = core.StepResult
	StepCall      = core.StepCall
	LoopInput     = core.LoopInput
	LoopOutcome   = core.LoopOutcome
	RunLedger     = core.RunLedger

	AgentHandle     = core.Handle
	RegisteredAgent = core.RegisteredAgent

	FollowOptions = core.FollowOptions
	EventStream   = core.EventStream
	FrameStream   = core.FrameStream
	FollowFrame   = core.FollowFrame
	FollowSSE     = core.FollowSSE
	ThreadCursor  = core.ThreadCursor
	PruneOptions  = core.PruneOptions
	PruneReport   = core.PruneReport
	SSEOptions    = core.SSEOptions
	SSEStream     = core.SSEStream

	HitlFrame    = core.HitlFrame
	HitlResponse = core.HitlResponse
	HitlCtx      = core.HitlCtx
	ParkInput    = core.ParkInput
	PendingHitl  = core.PendingHitl

	SubagentCtx = core.SubagentCtx
	Semaphore   = core.Semaphore

	ContentPart      = core.ContentPart
	TokenAttribution = core.TokenAttribution

	ToolContext   = core.ToolContext
	ToolRun       = core.ToolRun
	ThreadSandbox = core.ThreadSandbox

	BuiltinToolOptions    = core.BuiltinToolOptions
	BuiltinToolDefinition = core.BuiltinToolDefinition
	WebSearchOptions      = core.WebSearchOptions
	WebFetchOptions       = core.WebFetchOptions
	BashOptions           = core.BashOptions
	CodeExecutionOptions  = core.CodeExecutionOptions
	TextEditorOptions     = core.TextEditorOptions
	ApprovalRule          = core.ApprovalRule
	Approval              = core.Approval
	ParkRequest           = core.ParkRequest
	PublishOptions        = core.PublishOptions
	EventPublisher        = core.EventPublisher

	AdminOverview = core.AdminOverview
	RunStats      = core.RunStats
	RunDetail     = core.RunDetail
	ThreadSummary = core.ThreadSummary
	ThreadDetail  = core.ThreadDetail
	Percentiles   = core.Percentiles
	StatsRange    = core.StatsRange
)

const (
	OutcomeExecuted     = core.OutcomeExecuted
	OutcomeLockConflict = core.OutcomeLockConflict
	OutcomeStale        = core.OutcomeStale
	OutcomeLockLost     = core.OutcomeLockLost

	// SettleClaimTTL is how long a settle claim holds (§5.6).
	SettleClaimTTL = core.SettleClaimTTL

	HITLParked          = core.HITLParked
	ReasonApproval      = core.ReasonApproval
	HITLTTL             = core.HITLTTL
	ContextTokenCeiling = core.ContextTokenCeiling
	ContextSummaryType  = core.ContextSummaryType
	DefaultModel        = core.DefaultModel
)

var (
	Execute                  = core.Execute
	ExecuteStep              = core.ExecuteStep
	ExecuteWithPolicy        = core.ExecuteWithPolicy
	Finalize                 = core.Finalize
	RunLoop                  = core.RunLoop
	IsParked                 = core.IsParked
	MarkRequiresConfirmation = core.MarkRequiresConfirmation
	RequireConfirmation      = core.RequireConfirmation

	FollowEvents = core.FollowEvents
	ToSSEStream  = core.ToSSEStream
	SSEFrame     = core.SSEFrame
	ParseCursor  = core.ParseCursor
	FormatCursor = core.FormatCursor
	SSEHeaders   = core.SSEHeaders

	ContextWithRunID = core.ContextWithRunID
	RunIDFromContext = core.RunIDFromContext
	ClaimRun         = core.ClaimRun
	ClaimRunAs       = core.ClaimRunAs
	UserContent      = core.UserContent
	CurrentRunID     = core.CurrentRunID
	RunIDKey         = core.RunIDKey
	RedriveKey       = core.RedriveKey
	AttemptsKey      = core.AttemptsKey
	IsActive         = core.IsActive
	StateKey         = core.StateKey
	SeqKey           = core.SeqKey

	RunLockKey     = core.RunLockKey
	ParseLockValue = core.ParseLockValue
	NewID          = core.NewID

	CountTokens     = core.CountTokens
	AttributeTokens = core.AttributeTokens

	Respond         = core.Respond
	ParkForApproval = core.ParkForApproval
	ParkForInput    = core.ParkForInput
	AsParkRequest   = core.AsParkRequest
	LoadPendingHitl = core.LoadPendingHitl
	LoadOpenHitls   = core.LoadOpenHitls
	WithHitl        = core.WithHitl
	HitlKey         = core.HitlKey

	ReclaimIfOrphaned = core.ReclaimIfOrphaned
	ContextBudget     = core.ContextBudget
	ContextUsageOf    = core.ContextUsage
	CompactContext    = core.CompactContext

	NewSemaphore      = core.NewSemaphore
	RunNestedAgent    = core.RunNestedAgent
	SpawnSubagentTool = core.SpawnSubagentTool

	Run          = core.Run
	Stop         = core.Stop
	StopRun      = core.StopRun
	DeleteThread = core.DeleteThread

	RunStateFromContext = core.RunStateFromContext
	ContextWithRunState = core.ContextWithRunState
	WithRunState        = core.WithRunState

	MarkPromptCaching  = core.MarkPromptCaching
	SystemCacheMessage = core.SystemCacheMessage
	ChunkPayload       = core.ChunkPayload

	ContentFromMessage      = core.ContentFromMessage
	MessageFromDTO          = core.MessageFromDTO
	MessagesFromDTOs        = core.MessagesFromDTOs
	RepairDanglingToolCalls = core.RepairDanglingToolCalls
	ParseContent            = core.ParseContent
	TextContent             = core.TextContent
	PartsContent            = core.PartsContent

	Publish              = core.Publish
	PublishNotice        = core.PublishNotice
	PublishEvent         = core.PublishEvent
	WithPublishEvent     = core.WithPublishEvent
	ToolContextFrom      = core.ToolContextFrom
	ToolRunFromContext   = core.ToolRunFromContext
	SandboxFor           = core.SandboxFor
	IsPermanentError     = core.IsPermanentError
	BuiltinToolNames     = core.BuiltinToolNames
	DecodeEntities       = core.DecodeEntities
	HTMLToText           = core.HTMLToText
	ApprovalFromContext  = core.ApprovalFromContext
	PublisherFromContext = core.PublisherFromContext
	ReservedEventTypes   = core.ReservedEventTypes
	SetThreadState       = core.SetThreadState
	Summarise            = core.Summarise
)

// AgentTool is goai.NewTool with the platform's ToolContext handed to the
// handler: the run state (§2.10) and PublishEvent.
func AgentTool[In any](name, description string, execute func(ctx context.Context, input In, tc ToolContext) (string, error)) Tool {
	return core.AgentTool(name, description, execute)
}

// The sandbox errors, for errors.Is.
var (
	ErrSandboxGone         = ports.ErrSandboxGone
	ErrSandboxFileNotFound = ports.ErrSandboxFileNotFound
	ErrSandboxUnsupported  = ports.ErrSandboxUnsupported
)

// WithSandbox runs fn on the thread's sandbox, from inside a tool. When the
// sandbox turns out to be gone part way (it ended on its own), a fresh one
// is made and fn runs once more, told Lost.
func WithSandbox[T any](ctx context.Context, fn func(ThreadSandbox) (T, error)) (T, error) {
	return core.WithSandbox(ctx, fn)
}

// AskAlways and AskNever are the two fixed approval rules for the sandbox
// tools.
var (
	AskAlways = core.AskAlways
	AskNever  = core.AskNever
)
