package agentenkit

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/admin"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/core"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

// AgentCore is the bound platform: ports plus behaviors (§3.3).
type AgentCore struct {
	opts   ports.RuntimeOptions
	admin  ports.AdminStore
	config ports.AgentConfig

	mu           sync.RWMutex
	registry     map[string]*core.Handle
	defaultAgent string

	// HITL answers approvals and heals orphans (§2.5).
	HITL *HITLAPI
	// Events replays and tails a thread (§2.2).
	Events *EventsAPI
	// Admin reads operational history (§2.9).
	Admin *AdminAPI
	// Worker is the queue dispatch side (§2.8).
	Worker *WorkerAPI
}

// SetupAgentCore binds the ports to the core behaviors (§3.3). This is the
// package's public entry point: the only place where anything is wired.
//
// Operational history is the platform's own (§2.9). Nothing configured means
// SQLite on disk, opened here, eagerly: a store that cannot be opened is a
// startup error you see immediately, rather than a surprise on the first run.
func SetupAgentCore(ctx context.Context, opts RuntimeOptions) (*AgentCore, error) {
	if opts.Storage == nil || opts.Bus == nil || opts.Queue == nil || opts.Kv == nil {
		return nil, errors.New("agentenkit: Storage, Bus, Queue and Kv are required")
	}
	if opts.ResolveModel == nil {
		return nil, errors.New("agentenkit: ResolveModel is required")
	}
	config, err := ResolveConfig(opts.Config)
	if err != nil {
		return nil, err
	}
	store := opts.Admin
	if store == nil {
		store, err = admin.OpenDefaultAdminStore(ctx)
		if err != nil {
			return nil, err
		}
	}
	c := &AgentCore{opts: opts, admin: store, config: config, registry: map[string]*core.Handle{}}
	c.HITL = &HITLAPI{c}
	c.Events = &EventsAPI{c}
	c.Admin = &AdminAPI{c}
	c.Worker = &WorkerAPI{c}
	return c, nil
}

// scope builds the ports for ONE call: the caller's storage with that call's
// state bound (§2.10). Called per entry point rather than once, because
// state belongs to a run and a runtime outlives many.
func (c *AgentCore) scope(state AgentRunState, runID string) ports.RuntimePorts {
	return ports.RuntimePorts{
		Storage:      BindStorage(c.opts.Storage, StorageContext{State: state, RunID: runID}),
		Admin:        c.admin,
		Bus:          c.opts.Bus,
		Queue:        c.opts.Queue,
		Kv:           c.opts.Kv,
		ResolveModel: c.opts.ResolveModel,
		Config:       c.config,
	}
}

// Ports returns the ports bundle for reads on behalf of no particular run,
// or for a run when state is given.
func (c *AgentCore) Ports(state AgentRunState) RuntimePorts { return c.scope(state, "") }

// Config is the resolved config.
func (c *AgentCore) Config() AgentConfig { return c.config }

// AdminStore is the operational store in use.
func (c *AgentCore) AdminStore() AdminStore { return c.admin }

// Close releases the admin store.
func (c *AgentCore) Close() error { return c.admin.Close() }

// ResolveModel resolves a registry key to the identity and provider instance
// used by execution, compaction and usage attribution.
func (c *AgentCore) ResolveModel(modelName string) (ResolvedModel, error) {
	return c.opts.ResolveModel(modelName)
}

// ListThreads lists threads most recent first. Takes the run state (§2.10)
// so a tenant-scoped Storage can filter; a read has no dispatch ticket.
func (c *AgentCore) ListThreads(ctx context.Context, state AgentRunState) ([]ThreadDTO, error) {
	return c.scope(state, "").Storage.Threads.List(ctx)
}

// DeleteThread deletes a thread and everything that follows it (§3.2).
// Refused while a run is active; Stop first.
func (c *AgentCore) DeleteThread(ctx context.Context, threadID string, state AgentRunState) (DeleteThreadResult, error) {
	return core.DeleteThread(ctx, c.scope(state, ""), threadID)
}

// GetThreadSnapshot is one call for UIs: thread + messages + runs + the
// unfinished run's events. Nil when the thread is gone.
func (c *AgentCore) GetThreadSnapshot(ctx context.Context, threadID string, state AgentRunState) (*ThreadSnapshot, error) {
	deps := c.scope(state, "")
	thread, err := deps.Storage.Threads.Get(ctx, threadID)
	if err != nil || thread == nil {
		return nil, err
	}
	messages, err := deps.Storage.Messages.List(ctx, threadID, nil)
	if err != nil {
		return nil, err
	}
	runs, err := deps.Admin.Runs().ListByThread(ctx, threadID)
	if err != nil {
		return nil, err
	}
	events, err := deps.Storage.Events.ListSince(ctx, threadID, -1)
	if err != nil {
		return nil, err
	}
	snap := &ThreadSnapshot{Thread: *thread, Messages: messages, Runs: runs, LastEventSeq: -1, ActiveEvents: []AgentEvent{}}
	if messages == nil {
		snap.Messages = []MessageDTO{}
	}
	if runs == nil {
		snap.Runs = []RunRecord{}
	}
	if len(events) > 0 {
		snap.LastEventSeq = events[len(events)-1].Seq
	}
	if thread.State == StateRunning || thread.State == StateWaitingForInput {
		boundary := 0
		for i := len(events) - 1; i >= 0; i-- {
			if events[i].Type != "STATE_CHANGE" {
				continue
			}
			var p struct {
				State string `json:"state"`
			}
			if events[i].PayloadInto(&p) == nil && p.State == string(StateRunning) {
				boundary = i
				break
			}
		}
		active := events[boundary:]
		// Everything up to the last committed step is ALREADY in messages
		// (§2.2). Only the in-flight step's chunks are missing from durable
		// history, so only those are transient. Chunks alone: a park is
		// published DURING the step, before its messages commit, so slicing
		// the whole window would drop the very approval a reconnecting client
		// needs.
		var lastCommitted int64 = -1
		for _, e := range active {
			if e.Type == "STEP_COMMITTED" {
				lastCommitted = e.Seq
			}
		}
		for _, e := range active {
			isStream := e.Type == "CHUNK" || e.Type == "SUBAGENT_CHUNK"
			if isStream && e.Seq != 0 && e.Seq <= lastCommitted {
				continue
			}
			snap.ActiveEvents = append(snap.ActiveEvents, e)
		}
	}
	return snap, nil
}

// GetThreadUsage is tokens spent so far and the §2.6 context load. Nil when
// the thread is gone.
func (c *AgentCore) GetThreadUsage(ctx context.Context, threadID string, state AgentRunState) (*ThreadUsage, error) {
	deps := c.scope(state, "")
	thread, err := deps.Storage.Threads.Get(ctx, threadID)
	if err != nil || thread == nil {
		return nil, err
	}
	tokens, err := deps.Storage.Usage.Total(ctx, threadID)
	if err != nil {
		return nil, err
	}
	usage, err := core.ContextUsage(ctx, deps, threadID, thread.Model)
	if err != nil {
		return nil, err
	}
	return &ThreadUsage{Tokens: tokens, Context: usage, Model: thread.Model}, nil
}

func (c *AgentCore) register(name string, handle *core.Handle, kind AgentKind) *core.Handle {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.registry[name] = handle
	// The first registered stream-text handle is the default for jobs that
	// omit `agent` (§5).
	if kind == KindStreamText && c.defaultAgent == "" {
		c.defaultAgent = name
	}
	return handle
}

// CreateStreamTextAgent registers a stream-text handle under spec.Name (§4).
func (c *AgentCore) CreateStreamTextAgent(spec StreamTextAgentSpec) *AgentHandle {
	return c.register(spec.Name, core.NewStreamTextAgent(c.scope, spec), KindStreamText)
}

// CreateGenerateTextAgent registers a generate-text handle under spec.Name.
func (c *AgentCore) CreateGenerateTextAgent(spec GenerateTextAgentSpec) *AgentHandle {
	return c.register(spec.Name, core.NewGenerateTextAgent(c.scope, spec), KindGenerateText)
}

// GetAgent resolves a registered handle by name, or nil.
func (c *AgentCore) GetAgent(name string) *AgentHandle {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.registry[name]
}

// HITLAPI answers approvals (§2.5).
type HITLAPI struct{ c *AgentCore }

// Respond records an answer and resumes the run through the queue. Scoped
// like the run it resumes (§2.10). An orphaned wait is healed first; if
// reclamation claims the thread, the response is rejected as late.
func (h *HITLAPI) Respond(ctx context.Context, input RespondInput) (RespondResult, error) {
	scoped := h.c.scope(input.State, "")
	if _, err := core.ReclaimIfOrphaned(ctx, scoped, input.ThreadID); err != nil {
		return RespondResult{}, err
	}
	return core.Respond(ctx, scoped, input)
}

// ReclaimIfOrphaned re-dispatches a thread whose approvals all expired.
func (h *HITLAPI) ReclaimIfOrphaned(ctx context.Context, threadID string, state AgentRunState) (bool, error) {
	return core.ReclaimIfOrphaned(ctx, h.c.scope(state, ""), threadID)
}

// EventsAPI replays and tails a thread (§2.2).
type EventsAPI struct{ c *AgentCore }

// Since is the raw replay: every event after the cursor.
func (e *EventsAPI) Since(ctx context.Context, threadID string, sinceSeq int64, state AgentRunState) ([]AgentEvent, error) {
	return e.c.scope(state, "").Storage.Events.ListSince(ctx, threadID, sinceSeq)
}

// Subscribe is the raw tail. Returns an unsubscribe function.
func (e *EventsAPI) Subscribe(ctx context.Context, threadID string, handler func(AgentEvent)) (func() error, error) {
	return e.c.opts.Bus.Subscribe(ctx, threadID, handler)
}

// FollowStateOptions carries the run state alongside the follow options.
type FollowStateOptions struct {
	FollowOptions
	State AgentRunState
}

// Follow is replay then live, as one sequence, with the cursor discipline
// already applied (§2.2). Cancel ctx, or the subscription outlives the client.
func (e *EventsAPI) Follow(ctx context.Context, threadID string, opts FollowStateOptions) (*EventStream, error) {
	return core.FollowEvents(ctx, e.c.scope(opts.State, ""), threadID, opts.FollowOptions)
}

// SSEStateOptions carries the run state alongside the SSE options.
type SSEStateOptions struct {
	SSEOptions
	State AgentRunState
}

// SSE is Follow, encoded as Server-Sent Events. Serve it with ServeHTTP or
// WriteTo.
func (e *EventsAPI) SSE(ctx context.Context, threadID string, opts SSEStateOptions) (*SSEStream, error) {
	stream, err := core.FollowEvents(ctx, e.c.scope(opts.State, ""), threadID, opts.FollowOptions)
	if err != nil {
		return nil, err
	}
	return core.ToSSEStream(stream, opts.SSEOptions), nil
}

// PublishStateOptions carries the run state alongside the publish options.
type PublishStateOptions struct {
	PublishOptions
	State AgentRunState
}

// PublishEvent publishes an event of your own on a thread, from anywhere on
// the server: a webhook, a cron job, a route. Tools get the same thing bound
// to their thread through ToolContext.PublishEvent. Durable by default;
// Notice sends a bus-only notice. Platform event types are refused.
func (e *EventsAPI) PublishEvent(ctx context.Context, threadID, typ string, payload any, opts PublishStateOptions) (AgentEvent, error) {
	return core.PublishEvent(ctx, e.c.scope(opts.State, ""), threadID, typ, payload, opts.PublishOptions)
}

// AdminAPI reads operational history (§2.9). Everything here comes from the
// platform's OWN store; it never reads the caller's database.
type AdminAPI struct{ c *AgentCore }

func (a *AdminAPI) deps() ports.RuntimePorts { return a.c.scope(nil, "") }

// Overview: threads and runs by state, plus what is in flight.
func (a *AdminAPI) Overview(ctx context.Context, since *time.Time) (AdminOverview, error) {
	return core.Overview(ctx, a.deps(), since)
}

// ListRuns lists runs, newest first, bounded.
func (a *AdminAPI) ListRuns(ctx context.Context, filter RunFilter) ([]RunRecord, error) {
	return core.ListRuns(ctx, a.deps(), filter)
}

// Stats: p50/p95 duration and queue wait, tokens, failures.
func (a *AdminAPI) Stats(ctx context.Context, r StatsRange) (RunStats, error) {
	return core.RunStatsFor(ctx, a.deps(), r)
}

// GetRun: one run with steps, nested runs and timeline. Nil when unknown.
func (a *AdminAPI) GetRun(ctx context.Context, runID string) (*RunDetail, error) {
	return core.GetRun(ctx, a.deps(), runID)
}

// ListRunsByThread: every run on a thread, newest first.
func (a *AdminAPI) ListRunsByThread(ctx context.Context, threadID string) ([]RunRecord, error) {
	return a.c.admin.Runs().ListByThread(ctx, threadID)
}

// ListSteps: a run's steps in order.
func (a *AdminAPI) ListSteps(ctx context.Context, runID string) ([]StepRecord, error) {
	return core.ListSteps(ctx, a.deps(), runID)
}

// ListThreads: threads with their runs rolled up.
func (a *AdminAPI) ListThreads(ctx context.Context, filter AdminThreadFilter) ([]ThreadSummary, error) {
	return core.ListThreads(ctx, a.deps(), filter)
}

// GetThread: one thread opened up. Nil when nothing was recorded for it.
func (a *AdminAPI) GetThread(ctx context.Context, threadID string) (*ThreadDetail, error) {
	return core.GetThread(ctx, a.deps(), threadID)
}

// WorkerAPI is the queue dispatch side of the platform (§2.8).
type WorkerAPI struct{ c *AgentCore }

// HandleJobResult says whether a job was accepted.
type HandleJobResult struct {
	Accepted bool   `json:"accepted"`
	Reason   string `json:"reason,omitempty"`
}

// HandleJob resolves the handle, applies the failure policy, and is
// idempotent under at-least-once delivery. The HTTP layer only verifies
// signatures, parses JSON, and calls this.
func (w *WorkerAPI) HandleJob(ctx context.Context, job RunJob) (HandleJobResult, error) {
	w.c.mu.RLock()
	agent := w.c.registry[job.Agent]
	if agent == nil && w.c.defaultAgent != "" {
		agent = w.c.registry[w.c.defaultAgent] // missing `agent` → the default handle
	}
	w.c.mu.RUnlock()
	if agent == nil {
		return HandleJobResult{Accepted: false, Reason: "unknown-agent"}, nil
	}
	// ExecuteWithPolicy: run lock (idempotent under at-least-once delivery,
	// §3.4) + §2.8 failure policy: redrive < maxAttempts, else finalize
	// FAILED; a user stop is never retried.
	err := agent.ExecuteWithPolicy(ctx, ExecuteInput{
		ThreadID: job.ThreadID,
		// The dispatch's identity (§2.1): without it the worker cannot tell it
		// has been replaced by a newer run, and a blocked job is dropped.
		RunID: job.RunID,
		// Carries the queue wait through to the run record (§2.9).
		EnqueuedAt: job.EnqueuedAt,
		// Rehydrated from the ticket: this worker never saw the caller (§2.10).
		State: job.State, Model: job.Model, TokenBudget: job.TokenBudget, ProviderOptions: job.ProviderOptions,
	}, nil)
	if err != nil {
		return HandleJobResult{Accepted: true}, err
	}
	return HandleJobResult{Accepted: true}, nil
}

// Handler adapts the worker to an inline queue's handler signature.
func (w *WorkerAPI) Handler() func(ctx context.Context, job RunJob) error {
	return func(ctx context.Context, job RunJob) error {
		_, err := w.HandleJob(ctx, job)
		return err
	}
}
