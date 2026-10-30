// Package clevixflow holds the Queue adapter over Clevix Flow, and the Flow
// function that runs its jobs.
//
// Each job is one run of one Flow function, started with Client.Start. A key
// is the function's singleton key in "skip" mode, so at most one live run
// holds it, and it is free again as soon as that run ends. A delay is a sleep
// at the start of the run, so the key stays held while the job waits. Cancel
// cancels the run that holds a key. Find and Stats read Flow's runs index,
// which lags the runs by a few seconds.
//
// Queue is also a ports.QueueAdmin: Pause and Resume pause the function,
// and the dead jobs are its failed runs.
//
// Needs Flow SDK v0.8.0 or later and an engine with the run control routes.
package clevixflow

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	flow "github.com/eadwinCode/flow-sdk"
	"github.com/eadwinCode/flow-sdk/step"

	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

// FunctionID is the function the queue starts and the worker serves, unless
// both are given another.
const FunctionID = "agentenkit-run"

// JobEvent is the event the function lists as its trigger. Nothing sends it:
// runs come from Client.Start. Flow needs every function to have a trigger.
const JobEvent = "agentenkit/job"

// maxDataBytes is the most a run's data may be on Flow.
const maxDataBytes = 1_000_000

// Flow's priority.run is in seconds, from -600 to 600.
const (
	prioritySeconds = 60
	maxPriority     = 600
)

var active = []string{"queued", "running", "sleeping", "waiting", "waiting_for_app", "paused"}

// waiting are the statuses of a run that has not started its job, or waits
// between tries. Purge cancels only these.
var waiting = []string{"queued", "sleeping", "waiting", "paused"}

// pageSize is the most runs one list call gives.
const pageSize = 200

// JobData is what one run of the function carries.
type JobData struct {
	Job ports.RunJob `json:"job"`
	// Key is the singleton key: the enqueue's key, or the job's dispatch id.
	Key string `json:"key"`
	// Priority is in seconds, for priority.run.
	Priority int `json:"priority"`
	// NotBefore is epoch ms. The run sleeps until then before it runs the job.
	NotBefore int64 `json:"notBefore,omitempty"`
}

// Options tune the queue.
type Options struct {
	// FunctionID is the function to start. Defaults to FunctionID. Must match
	// the ID given to Functions.
	FunctionID string
	// Namespace lets two deployments share one Flow app: it goes in front of
	// the function ID, as "<namespace>-<id>". Singleton keys belong to one
	// function, so the keys are apart too. Must match Functions' Namespace.
	Namespace string
	// MaxDepth refuses a fresh dispatch (ports.ErrQueueFull) once this many
	// runs wait to start. Retries, redrives, resumes and expiries always go
	// in. The count comes from Flow's runs index, which lags a few seconds,
	// so a burst can go a little past it. Zero means no limit.
	MaxDepth int
}

// Queue is a Queue over Clevix Flow.
type Queue struct {
	client     *flow.Client
	functionID string
	maxDepth   int
}

var (
	_ ports.Queue      = (*Queue)(nil)
	_ ports.QueueAdmin = (*Queue)(nil)
)

// New makes a queue.
func New(client *flow.Client, opts Options) *Queue {
	return &Queue{client: client, functionID: functionName(opts.Namespace, opts.FunctionID), maxDepth: opts.MaxDepth}
}

// functionName is the function's ID: id, or FunctionID, after the namespace.
func functionName(namespace, id string) string {
	if id == "" {
		id = FunctionID
	}
	if namespace == "" {
		return id
	}
	return namespace + "-" + id
}

// Enqueue starts one run for the job.
func (q *Queue) Enqueue(ctx context.Context, job ports.RunJob, opts *ports.EnqueueOptions) error {
	if opts == nil {
		opts = &ports.EnqueueOptions{}
	}
	// An unkeyed job still needs a singleton key, or every unkeyed job would
	// share one. Its dispatch id is its own; a second start of the same
	// dispatch is the same job, so it is dropped without an error.
	key := opts.Key
	if key == "" {
		key = job.DispatchID
	}
	if key == "" {
		key = randomKey()
	}
	data := JobData{Job: job, Key: key, Priority: max(-maxPriority, min(maxPriority, opts.Priority*prioritySeconds))}
	if opts.Delay > 0 {
		// Rounded up, never down: a delay is a "not before".
		data.NotBefore = time.Now().UnixMilli() + int64((opts.Delay+time.Millisecond-1)/time.Millisecond)
	}
	body, err := json.Marshal(data)
	if err != nil {
		return err
	}
	if len(body) > maxDataBytes {
		return ports.ErrPayloadTooLarge
	}
	// Only new work is refused: a retry or a resume belongs to a run that is
	// already under way, and refusing it would strand that run.
	if q.maxDepth > 0 && job.Kind == ports.JobDispatch {
		counts, err := q.client.Runs().Counts(ctx, q.functionID)
		if err != nil {
			return err
		}
		if counts.Queued+counts.WaitingForApp+counts.Paused >= q.maxDepth {
			return ports.ErrQueueFull
		}
	}
	// Twice at most: a singleton answer with no active run means the run that
	// held the key ended in between, so the key is free now.
	for range 2 {
		// "hold": while the queue is paused, the job waits in it, as it
		// would in any queue, and starts on Resume.
		res, err := q.client.Start(ctx, q.functionID, flow.StartOpts{Data: json.RawMessage(body), WhenPaused: "hold"})
		if err != nil {
			return err
		}
		if res.Started {
			return nil
		}
		switch res.Reason {
		case "singleton":
			if res.ActiveRunID == "" {
				continue
			}
			if opts.Key != "" {
				return ports.ErrDuplicateJob
			}
			return nil
		case "paused":
			return fmt.Errorf("flow: function %s is paused", q.functionID)
		default:
			return fmt.Errorf("flow: no function %s; is the app served and synced?", q.functionID)
		}
	}
	// Both tries found the key held by a run that had just ended. No run
	// started, so the caller must hear it, or the job is lost.
	return fmt.Errorf("flow: no run started for key %s; the key was still being freed", key)
}

// Cancel cancels the run that holds key, if any.
func (q *Queue) Cancel(ctx context.Context, key string) error {
	_, err := q.client.Runs().Cancel(ctx, flow.CancelOpts{Function: q.functionID, SingletonKey: key})
	return err
}

// Find returns the earliest active run for runID, or nil.
func (q *Queue) Find(ctx context.Context, runID string) (*ports.QueuedJob, error) {
	page, err := q.client.Runs().List(ctx, flow.ListRunsOpts{
		Function: q.functionID, Tags: map[string]string{"runId": runID}, Status: active, Limit: 50,
	})
	if err != nil {
		return nil, err
	}
	if len(page.Items) == 0 {
		return nil, nil
	}
	// Newest first: the earliest is the last one.
	run := page.Items[len(page.Items)-1]
	return &ports.QueuedJob{
		ID: run.ID, RunID: run.Tags["runId"], ThreadID: run.Tags["threadId"], Kind: ports.JobKind(run.Tags["kind"]),
		RunAt: time.UnixMilli(run.CreatedAt), Position: -1,
	}, nil
}

// Stats counts the function's runs by status.
func (q *Queue) Stats(ctx context.Context) (ports.QueueStats, error) {
	counts, err := q.client.Runs().Counts(ctx, q.functionID)
	if err != nil {
		return ports.QueueStats{}, err
	}
	oldest, err := q.client.Runs().List(ctx, flow.ListRunsOpts{Function: q.functionID, Status: []string{"queued"}, Limit: 1, Order: "oldest"})
	if err != nil {
		return ports.QueueStats{}, err
	}
	var oldestMs int64
	if len(oldest.Items) > 0 {
		oldestMs = max(0, time.Now().UnixMilli()-oldest.Items[0].CreatedAt)
	}
	return ports.QueueStats{
		Ready:         counts.Queued + counts.WaitingForApp,
		Delayed:       counts.Sleeping + counts.Waiting,
		InFlight:      counts.Running,
		Dead:          counts.Failed,
		OldestReadyMs: oldestMs,
		Paused:        counts.Paused > 0,
	}, nil
}

// Pause pauses the function. Jobs enqueued meanwhile wait, paused, and jobs
// already started pause at their next step. Resume lets them all go.
func (q *Queue) Pause(ctx context.Context) error {
	_, err := q.client.Functions().Pause(ctx, q.functionID)
	return err
}

// Resume resumes the function and its paused runs.
func (q *Queue) Resume(ctx context.Context) error {
	_, err := q.client.Functions().Resume(ctx, q.functionID)
	return err
}

// Purge cancels the waiting runs that match. Running runs are left alone. With
// IncludeDead it deletes the failed runs that match too. It reads Flow's runs
// index, so a job enqueued in the last few seconds can be missed.
func (q *Queue) Purge(ctx context.Context, f ports.PurgeFilter) (int64, error) {
	if f.ThreadID == "" && f.RunID == "" && f.Kind == nil && f.OlderThan <= 0 {
		return 0, errors.New("flow queue: purge needs a filter")
	}
	tags := map[string]string{}
	if f.ThreadID != "" {
		tags["threadId"] = f.ThreadID
	}
	if f.RunID != "" {
		tags["runId"] = f.RunID
	}
	// The kind tag of a fresh dispatch is empty, which a tag filter cannot
	// ask for, so the kind is checked here.
	match := func(run flow.RunSummary) bool {
		if f.Kind != nil && run.Tags["kind"] != string(*f.Kind) {
			return false
		}
		return f.OlderThan <= 0 || time.Since(time.UnixMilli(run.CreatedAt)) > f.OlderThan
	}
	var n int64
	err := q.eachRun(ctx, waiting, tags, func(run flow.RunSummary) error {
		if !match(run) {
			return nil
		}
		res, err := q.client.Runs().Cancel(ctx, flow.CancelOpts{RunID: run.ID})
		n += int64(len(res.Cancelled))
		return err
	})
	if err != nil || !f.IncludeDead {
		return n, err
	}
	err = q.eachRun(ctx, []string{"failed"}, tags, func(run flow.RunSummary) error {
		if !match(run) {
			return nil
		}
		deleted, err := q.client.Runs().Delete(ctx, run.ID)
		if deleted {
			n++
		}
		return err
	})
	return n, err
}

// ListDead lists the failed runs, newest first, with the job each carried.
// It asks each run for its error, so it makes one call per job.
func (q *Queue) ListDead(ctx context.Context, limit int) ([]ports.DeadJob, error) {
	if limit <= 0 {
		limit = 100
	}
	out := []ports.DeadJob{}
	err := q.eachRun(ctx, []string{"failed"}, nil, func(run flow.RunSummary) error {
		if len(out) >= limit {
			return errStop
		}
		detail, err := q.client.Runs().Get(ctx, run.ID)
		if err != nil || detail == nil {
			// Gone since the list: deleted, or past its retention.
			return err
		}
		d := ports.DeadJob{ID: run.ID, Attempts: detail.Attempts, DeadAt: time.UnixMilli(run.EndedAt)}
		var data JobData
		if json.Unmarshal(detail.Event.Data, &data) == nil {
			d.Job = data.Job
		}
		if detail.Error != nil {
			d.LastError = detail.Error.Message
		}
		out = append(out, d)
		return nil
	})
	return out, err
}

// Redrive retries a failed run from where it stopped, under the same run ID.
// False when there is no such failed run, the queue is paused, or another
// run holds the job's key now.
func (q *Queue) Redrive(ctx context.Context, id string) (bool, error) {
	res, err := q.client.Runs().Retry(ctx, id)
	return res.Retried, err
}

// PurgeDead deletes the failed runs that ended longer ago than olderThan.
// Runs kept for debugging in the dashboard stay.
func (q *Queue) PurgeDead(ctx context.Context, olderThan time.Duration) (int64, error) {
	var n int64
	err := q.eachRun(ctx, []string{"failed"}, nil, func(run flow.RunSummary) error {
		if time.Since(time.UnixMilli(run.EndedAt)) <= olderThan {
			return nil
		}
		deleted, err := q.client.Runs().Delete(ctx, run.ID)
		if deleted {
			n++
		}
		return err
	})
	return n, err
}

// errStop ends eachRun early without an error.
var errStop = errors.New("stop")

// eachRun calls fn for each of the function's runs in these statuses, newest
// first, page by page.
func (q *Queue) eachRun(ctx context.Context, status []string, tags map[string]string, fn func(flow.RunSummary) error) error {
	opts := flow.ListRunsOpts{Function: q.functionID, Status: status, Limit: pageSize}
	if len(tags) > 0 {
		opts.Tags = tags
	}
	for {
		page, err := q.client.Runs().List(ctx, opts)
		if err != nil {
			return err
		}
		for _, run := range page.Items {
			if err := fn(run); err != nil {
				if errors.Is(err, errStop) {
					return nil
				}
				return err
			}
		}
		if page.Next == "" {
			return nil
		}
		opts.Before = page.Next
	}
}

func randomKey() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// Worker is the side of the platform the function calls. *agentenkit.WorkerAPI
// has both.
type Worker interface {
	Handler() func(ctx context.Context, job ports.RunJob) error
	HandleDeadJob(ctx context.Context, job ports.RunJob, attempts int, cause error)
}

// FunctionOpts tune the Flow function.
type FunctionOpts struct {
	// ID defaults to FunctionID. Must match the queue's FunctionID. At most 56
	// characters with the namespace, since the failure function adds
	// "-failure".
	ID string
	// Namespace goes in front of the ID. Must match the queue's Namespace.
	Namespace string
	// Concurrency is how many jobs of one PartitionKey run at once. Zero: no
	// limit. Job priorities only take effect under a limit.
	Concurrency int
	// GlobalConcurrency is how many jobs run at once in all, whatever their
	// PartitionKey. Zero: no limit. It can be set with Concurrency.
	GlobalConcurrency int
	// Retries is Flow's retries for a job that fails. Flow's default when nil.
	Retries *int
	// StepTimeout is how long Flow waits for one job. Defaults to 14 minutes,
	// Flow's most. When a job runs longer, Flow stops waiting and tries the
	// step again, but nothing stops the first call: it keeps running. The
	// retry finds the run lock held and does nothing, so keep segments
	// shorter than this (SegmentTimeout).
	StepTimeout time.Duration
	// SegmentTimeout is the longest SegmentTimeout of the agents this
	// function runs. When set, Functions refuses one that is not shorter than
	// StepTimeout, since a longer segment breaks Flow's retries.
	SegmentTimeout time.Duration
	// RetryBackoff is the wait before a failed job is tried again, doubled on
	// every further failure up to RetryBackoffMax, as on the Postgres queue.
	// Zero: Flow's own backoff. RetryBackoffMax zero means one minute.
	RetryBackoff    time.Duration
	RetryBackoffMax time.Duration
}

// backoff is the wait before try attempt+1: RetryBackoff doubled attempt
// times, up to RetryBackoffMax.
func (o FunctionOpts) backoff(attempt int) time.Duration {
	d := o.RetryBackoff
	for range attempt {
		if d >= o.RetryBackoffMax {
			break
		}
		d *= 2
	}
	return min(d, o.RetryBackoffMax)
}

// handled is the step's result.
type handled struct {
	Accepted bool `json:"accepted"`
}

// Functions makes the Flow function that runs the queue's jobs, and its
// failure function. Serve them with the app's other Flow functions.
//
// A job runs in one step. HandleJob only fails for what a queue should retry
// (an unknown agent, a store that is down), so Flow's retries cover delivery;
// agentic-kit retries a failed run itself. When Flow gives up, the failure
// function hands the job to HandleDeadJob, which fails the run.
func Functions(client *flow.Client, worker Worker, opts FunctionOpts) ([]flow.ServableFunction, error) {
	opts.ID = functionName(opts.Namespace, opts.ID)
	if opts.StepTimeout == 0 {
		opts.StepTimeout = 14 * time.Minute
	}
	if opts.SegmentTimeout > 0 && opts.SegmentTimeout >= opts.StepTimeout {
		return nil, fmt.Errorf("flow: SegmentTimeout %s must be shorter than StepTimeout %s, or a long segment is tried again while it still runs", opts.SegmentTimeout, opts.StepTimeout)
	}
	if opts.RetryBackoffMax <= 0 {
		opts.RetryBackoffMax = time.Minute
	}
	fnOpts := flow.FunctionOpts{
		ID:        opts.ID,
		Retries:   opts.Retries,
		Timeouts:  &flow.Timeouts{Step: opts.StepTimeout},
		Singleton: &flow.Singleton{Key: flow.StrPtr("event.data.key"), Mode: "skip"},
		Priority:  &flow.Priority{Run: flow.StrPtr("event.data.priority")},
		Tags: map[string]string{
			"runId": "event.data.job.runId", "threadId": "event.data.job.threadId", "kind": "event.data.job.kind",
		},
	}
	if opts.Concurrency > 0 {
		fnOpts.Concurrency = append(fnOpts.Concurrency, flow.Concurrency{Limit: opts.Concurrency, Key: flow.StrPtr("event.data.job.partitionKey")})
	}
	if opts.GlobalConcurrency > 0 {
		fnOpts.Concurrency = append(fnOpts.Concurrency, flow.Concurrency{Limit: opts.GlobalConcurrency})
	}
	handle := worker.Handler()
	run, err := flow.CreateFunction(client, fnOpts, flow.EventTrigger(JobEvent, nil),
		func(ctx context.Context, in flow.Input[JobData]) (any, error) {
			data := in.Event.Data
			if data.NotBefore > 0 {
				if err := step.SleepUntil(ctx, "delay", time.UnixMilli(data.NotBefore)); err != nil {
					return nil, err
				}
			}
			return step.Run(ctx, "handle", func(ctx context.Context) (handled, error) {
				if err := handle(ctx, data.Job); err != nil {
					if opts.RetryBackoff > 0 {
						return handled{}, flow.RetryAtError(err, time.Now().Add(opts.backoff(in.InputCtx.Attempt)))
					}
					return handled{}, err
				}
				return handled{Accepted: true}, nil
			})
		})
	if err != nil {
		return nil, err
	}
	failed, err := flow.CreateFunction(client, flow.FunctionOpts{ID: opts.ID + "-failure"},
		flow.EventTrigger("inngest/function.failed", flow.StrPtr(fmt.Sprintf("event.data.function_id == '%s'", run.FullyQualifiedID()))),
		func(ctx context.Context, in flow.Input[flow.FailedEventData]) (any, error) {
			d := in.Event.Data
			var trigger struct {
				Data *JobData `json:"data"`
			}
			if json.Unmarshal(d.Event, &trigger) != nil || trigger.Data == nil {
				return nil, nil
			}
			cause := errors.New("the Flow run failed")
			if d.Error != nil && d.Error.Message != "" {
				cause = errors.New(d.Error.Message)
			}
			worker.HandleDeadJob(ctx, trigger.Data.Job, d.Attempts, cause)
			return nil, nil
		})
	if err != nil {
		return nil, err
	}
	return []flow.ServableFunction{run, failed}, nil
}
