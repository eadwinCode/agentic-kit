package agentenkit

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"log/slog"
	"sync"
	"time"

	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/adapters/memory"
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
	// Streams reads run streams by id.
	Streams *StreamsAPI
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
	if opts.Streams == nil {
		opts.Streams = inMemoryStreams(opts.Log)
	}
	store := opts.Admin
	if store == nil {
		store, err = admin.OpenDefaultAdminStore(ctx, opts.Log)
		if err != nil {
			return nil, err
		}
	}
	c := &AgentCore{opts: opts, admin: store, config: config, registry: map[string]*core.Handle{}}
	c.HITL = &HITLAPI{c}
	c.Events = &EventsAPI{c}
	c.Admin = &AdminAPI{c}
	c.Worker = &WorkerAPI{c}
	c.Streams = &StreamsAPI{c}
	return c, nil
}

var warnedInMemory sync.Once

// inMemoryStreams is the run streams when none were passed: in memory,
// which only this process can read. Said once, loudly, since a web server
// and a worker in separate processes would each see only their own.
func inMemoryStreams(log *slog.Logger) ports.RunStreams {
	warnedInMemory.Do(func() {
		if log == nil {
			log = slog.Default()
		}
		log.Warn("agentenkit: no Streams port was passed, so run streams are kept in memory and only this " +
			"process can read them. Pass one (redis.NewRunStreams, a Postgres or SQLite store) when web " +
			"servers and workers run apart.")
	})
	return memory.NewRunStreams()
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
		Streams:      c.opts.Streams,
		ResolveModel: c.opts.ResolveModel,
		Pricer:       c.opts.Pricer,
		Log:          c.opts.Log,
		Config:       c.config,
	}
}

// Ports returns the ports bundle for reads on behalf of no particular run,
// or for a run when state is given.
func (c *AgentCore) Ports(state AgentRunState) RuntimePorts { return c.scope(state, "") }

// log is the platform's logger, defaulted.
func (c *AgentCore) log() *slog.Logger { return core.Logger(c.scope(nil, "")) }

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

// BuiltinTools returns the built-in tools, ready to put in an agent's
// Tools: one name and one input shape in every runtime, so any model that
// can call tools can use them. An error when a tool's adapter was not given
// in RuntimeOptions.Tools.
//
//	tools, err := rt.BuiltinTools([]string{"web_search", "web_fetch"}, agentenkit.BuiltinToolOptions{})
func (c *AgentCore) BuiltinTools(names []string, opts BuiltinToolOptions) ([]Tool, error) {
	return core.BuildBuiltinTools(c.opts.Tools, names, opts)
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
// unfinished run's record entries + its run stream. Nil when the thread is
// gone.
func (c *AgentCore) GetThreadSnapshot(ctx context.Context, threadID string, state AgentRunState) (*ThreadSnapshot, error) {
	return core.ThreadSnapshotOf(ctx, c.scope(state, ""), threadID, "")
}

// GetThreadUsage is tokens spent so far and the §2.6 context load. Nil when
// the thread is gone.
func (c *AgentCore) GetThreadUsage(ctx context.Context, threadID string, state AgentRunState) (*ThreadUsage, error) {
	deps := c.scope(state, "")
	thread, err := deps.Storage.Threads.Get(ctx, threadID)
	if err != nil || thread == nil {
		return nil, err
	}
	tokens, err := deps.Storage.Usage.Total(ctx, threadID, ports.UsageFilter{})
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
	// A stop ends whichever agent's run is active; that agent's settle hook
	// is found through the registry (§5.6).
	handle.Registry(func(name string) *core.RegisteredAgent {
		if h := c.GetAgent(name); h != nil {
			return h.Agent()
		}
		return nil
	})
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

// Subscribe is the raw tail. Returns an unsubscribe function. A bus that
// reads storage itself (the Postgres bus, after a reconnect) takes the run
// state from ctx: wrap it with ContextWithRunState on scoped storage.
func (e *EventsAPI) Subscribe(ctx context.Context, threadID string, handler func(AgentEvent)) (func() error, error) {
	return e.c.opts.Bus.Subscribe(ctx, threadID, handler)
}

// FollowStateOptions says where a follow starts, with the run state.
type FollowStateOptions struct {
	// Since is a bare record seq, as older clients send; used when Cursor
	// is empty.
	FollowOptions
	// Cursor is where the client is: the SSE id it last saw
	// (Last-Event-ID), "<seq> <streamId> <offset>".
	Cursor string
	// LastMessageID is the last message the client has, so a SNAPSHOT
	// carries only newer ones.
	LastMessageID string
	State         AgentRunState
}

// cursor is where the follow starts: Cursor, else a bare Since.
func (o FollowStateOptions) cursor() *core.ThreadCursor {
	if c, ok := core.ParseCursor(o.Cursor); ok {
		return &c
	}
	if o.Since != 0 {
		return &core.ThreadCursor{Seq: o.Since}
	}
	return nil
}

// Follow is a thread live, as one sequence of frames (§2.2): its record
// entries and notices, and its run streams — the stream the cursor names
// read on from its offset, the next segment's from its RUN_STARTED, and one
// SNAPSHOT frame when the stream the client was reading is gone. Cancel
// ctx, or the follow outlives the client.
func (e *EventsAPI) Follow(ctx context.Context, threadID string, opts FollowStateOptions) (*core.FrameStream, error) {
	// The state rides the context too: a bus that reads storage itself (the
	// Postgres bus replays after a reconnect) needs the same scope.
	ctx = core.ContextWithRunState(ctx, opts.State)
	return core.FollowThread(ctx, e.c.scope(opts.State, ""), threadID, core.FollowThreadOptions{
		Cursor: opts.cursor(), LastMessageID: opts.LastMessageID,
	})
}

// FollowRecord is the thread record and its notices alone, as before run
// streams: no stream frames.
func (e *EventsAPI) FollowRecord(ctx context.Context, threadID string, opts FollowStateOptions) (*EventStream, error) {
	ctx = core.ContextWithRunState(ctx, opts.State)
	return core.FollowEvents(ctx, e.c.scope(opts.State, ""), threadID, opts.FollowOptions)
}

// SSEStateOptions is FollowStateOptions with the SSE retry hint and wire
// format.
type SSEStateOptions struct {
	FollowStateOptions
	// RetryMs is emitted once, up front: how long a browser waits before
	// reconnecting. Zero omits it.
	RetryMs int
	// WireFormat is "agentenkit" (the default) or "ag-ui" to send AG-UI
	// events instead of our frames (opt-in).
	WireFormat string
}

// SSE is Follow, encoded as Server-Sent Events: each frame that moves the
// cursor carries it as its id:. Serve it with ServeHTTP or WriteTo.
func (e *EventsAPI) SSE(ctx context.Context, threadID string, opts SSEStateOptions) (*core.FollowSSE, error) {
	frames, err := e.Follow(ctx, threadID, opts.FollowStateOptions)
	if err != nil {
		return nil, err
	}
	sse := core.ToFollowSSE(frames, opts.cursor(), opts.RetryMs)
	if opts.WireFormat == core.WireAgUI {
		sse.AsAgUI(threadID)
	}
	return sse, nil
}

// PruneEvents deletes the stream-only rows (chunks, step markers, state
// changes…) releases before run streams left in the event table, a batch at
// a time. Nothing reads them any more; an app's own types are kept. Run it
// when it suits you, after upgrading: DryRun counts first.
func (c *AgentCore) PruneEvents(ctx context.Context, opts core.PruneOptions) (core.PruneReport, error) {
	return core.PruneEvents(ctx, c.scope(nil, ""), opts)
}

// StreamsAPI reads run streams by id, for a caller that only cares about
// one run.
type StreamsAPI struct{ c *AgentCore }

// Read yields a run stream's items after `after`, live until it closes. It
// yields ErrStreamGone once the stream is past its grace window.
func (s *StreamsAPI) Read(ctx context.Context, streamID, after string) iter.Seq2[ports.StreamItem, error] {
	return s.c.opts.Streams.Read(ctx, streamID, after)
}

// Snapshot is a run stream as it stands now; nil when it is gone.
func (s *StreamsAPI) Snapshot(ctx context.Context, streamID, after string) (*ports.StreamSnapshot, error) {
	return s.c.opts.Streams.Snapshot(ctx, streamID, after)
}

// PublishStateOptions carries the run state alongside the publish options.
type PublishStateOptions struct {
	PublishOptions
	State AgentRunState
}

// PublishEvent publishes an event of your own on a thread, from anywhere on
// the server: a webhook, a cron job, a route. Tools get the same thing bound
// to their thread through ToolContext.PublishEvent. Live only by default;
// Durable also keeps it in the thread record. Platform event types are
// refused.
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

// ErrUnknownAgent is a job for an agent this process has not registered,
// with no default handle to fall back on. It is an error rather than a
// quiet refusal: the queue keeps the job and retries it, so a process that
// registers its agents a moment after it starts consuming heals on its own,
// and one that never does leaves a dead row and a log line, not silence.
var ErrUnknownAgent = errors.New("agentenkit: no agent registered for this job")

// resolve finds the handle a job dispatches to: its own, or the default.
func (w *WorkerAPI) resolve(name string) *core.Handle {
	w.c.mu.RLock()
	defer w.c.mu.RUnlock()
	agent := w.c.registry[name]
	if agent == nil && w.c.defaultAgent != "" {
		agent = w.c.registry[w.c.defaultAgent] // missing `agent` → the default handle
	}
	return agent
}

// HandleJob resolves the handle, applies the failure policy, and is
// idempotent under at-least-once delivery. The HTTP layer only verifies
// signatures, parses JSON, and calls this.
func (w *WorkerAPI) HandleJob(ctx context.Context, job RunJob) (HandleJobResult, error) {
	agent := w.resolve(job.Agent)
	if agent == nil {
		return HandleJobResult{Accepted: false, Reason: "unknown-agent"}, fmt.Errorf("%w: %q", ErrUnknownAgent, job.Agent)
	}
	// ExecuteWithPolicy: run lock (idempotent under at-least-once delivery,
	// §3.4) + §2.8 failure policy: redrive < maxAttempts, else finalize
	// FAILED; a user stop is never retried.
	err := agent.ExecuteWithPolicy(ctx, ExecuteInput{
		ThreadID: job.ThreadID,
		// The dispatch's identity (§2.1): without it the worker cannot tell it
		// has been replaced by a newer run, and a blocked job is dropped.
		RunID: job.RunID, DispatchID: job.DispatchID,
		// Carries the queue wait through to the run record (§2.9), and the
		// run's place in line onto any retry (§2.8).
		EnqueuedAt: job.EnqueuedAt, DispatchedAt: job.DispatchedAt,
		Kind: job.Kind, PartitionKey: job.PartitionKey,
		// Rehydrated from the ticket: this worker never saw the caller (§2.10).
		State: job.State, Model: job.Model, TokenBudget: job.TokenBudget,
		CostBudgetMicros: job.CostBudgetMicros, ProviderOptions: job.ProviderOptions,
		MaxSteps: job.MaxSteps,
	}, nil)
	if err != nil {
		return HandleJobResult{Accepted: true}, err
	}
	return HandleJobResult{Accepted: true}, nil
}

// Handler adapts the worker to a queue's handler signature. A refused job
// is logged with its reason before the error goes back to the queue.
func (w *WorkerAPI) Handler() func(ctx context.Context, job RunJob) error {
	return func(ctx context.Context, job RunJob) error {
		res, err := w.HandleJob(ctx, job)
		if !res.Accepted {
			w.c.log().Error("job refused", "thread", job.ThreadID, "run", job.RunID, "agent", job.Agent, "reason", res.Reason, "err", err)
		}
		return err
	}
}

// HandleDeadJob is what a queue calls when it gives up on a job (§2.8): the
// run behind it is failed with the reason, so its thread does not read
// QUEUED or RUNNING for ever and its spend is settled. A job whose run has
// already moved on (a newer run, a stop, a finished run) is left alone.
func (w *WorkerAPI) HandleDeadJob(ctx context.Context, job RunJob, attempts int, cause error) {
	log := w.c.log().With("thread", job.ThreadID, "run", job.RunID, "kind", string(job.Kind), "attempts", attempts)
	agent := w.resolve(job.Agent)
	if agent == nil {
		log.Error("dead job for an unknown agent; its run cannot be failed", "err", cause)
		return
	}
	reason := fmt.Sprintf("the run's job was dropped by the queue after %d deliveries: %v", attempts, cause)
	failed, err := core.FailLostRun(ctx, w.c.scope(job.State, job.RunID), agent.Agent(), job.ThreadID, job.RunID, reason)
	switch {
	case err != nil:
		log.Error("dead job: run not failed", "err", err)
	case failed:
		log.Error("dead job: run failed", "err", cause)
	default:
		log.Warn("dead job: run had already moved on; nothing to fail", "err", cause)
	}
}

// ReclaimReport is what a stuck-run sweep did.
type ReclaimReport struct {
	// Checked is how many open run records the sweep looked at.
	Checked int `json:"checked"`
	// Redispatched is how many runs went back on the queue or were moved
	// to their record's end state.
	Redispatched int `json:"redispatched"`
	// Settled is how many ended runs had their settle run late.
	Settled int `json:"settled"`
	// Errors is how many runs the sweep could not act on.
	Errors int `json:"errors"`
}

// ReclaimStuckRuns is the backstop for a run that nothing is working on
// (§2.5, §2.8): a QUEUED or RUNNING record older than olderThan whose lock
// nobody holds and whose job the queue no longer has is re-dispatched, and
// an ended record older than olderThan whose settle never ran is settled.
// Call it from a periodic job. It pages through every such run, not only
// the first page. A record with no recorded state (RecordPayloads off) is
// read with an empty state, so a tenant-scoped storage sees no tenant.
func (c *AgentCore) ReclaimStuckRuns(ctx context.Context, olderThan time.Duration) (ReclaimReport, error) {
	var report ReclaimReport
	until := time.Now().Add(-olderThan)
	top := 0
	err := c.eachRun(ctx, ports.RunFilter{
		State: []ports.ExecutionState{StateQueued, StateRunning}, Until: &until, Depth: &top,
	}, func(rec ports.RunRecord) {
		if rec.EndedAt != nil {
			return
		}
		report.Checked++
		did, err := core.ReclaimIfOrphaned(ctx, c.scope(rec.RunState, rec.ID), rec.ThreadID)
		if err != nil {
			report.Errors++
			c.log().Error("stuck run not reclaimed", "thread", rec.ThreadID, "run", rec.ID, "err", err)
			return
		}
		if did {
			report.Redispatched++
		}
	})
	if err != nil {
		return report, err
	}
	err = c.eachRun(ctx, ports.RunFilter{Unsettled: true, Until: &until, Depth: &top}, func(rec ports.RunRecord) {
		if rec.EndedAt == nil || rec.EndedAt.After(until) {
			return
		}
		report.Checked++
		agent := w(c).resolve(rec.Agent)
		if agent == nil {
			return
		}
		settled, err := core.SettleLate(ctx, c.scope(rec.RunState, rec.ID), agent.Agent(), rec.ThreadID, rec.ID)
		if err != nil {
			report.Errors++
			c.log().Error("unsettled run not settled", "thread", rec.ThreadID, "run", rec.ID, "err", err)
			return
		}
		if settled {
			report.Settled++
		}
	})
	return report, err
}

// sweepPage is how many run records one sweep read brings back.
const sweepPage = 500

// eachRun calls fn for every run the filter matches, a page at a time.
func (c *AgentCore) eachRun(ctx context.Context, f ports.RunFilter, fn func(ports.RunRecord)) error {
	f.Limit = sweepPage
	for {
		page, err := c.admin.Runs().List(ctx, f)
		if err != nil {
			return err
		}
		for _, rec := range page {
			fn(rec)
		}
		if len(page) < sweepPage {
			return nil
		}
		f.Before = ports.CursorOf(page[len(page)-1])
	}
}

func w(c *AgentCore) *WorkerAPI { return c.Worker }
