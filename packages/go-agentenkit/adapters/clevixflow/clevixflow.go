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
// Needs Flow SDK v0.7.0 or later and an engine with the run routes.
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
}

// Queue is a Queue over Clevix Flow.
type Queue struct {
	client     *flow.Client
	functionID string
}

// New makes a queue.
func New(client *flow.Client, opts Options) *Queue {
	if opts.FunctionID == "" {
		opts.FunctionID = FunctionID
	}
	return &Queue{client: client, functionID: opts.FunctionID}
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
	// Twice at most: a singleton answer with no active run means the run that
	// held the key ended in between, so the key is free now.
	for range 2 {
		res, err := q.client.Start(ctx, q.functionID, flow.StartOpts{Data: json.RawMessage(body)})
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
	if opts.Key != "" {
		return ports.ErrDuplicateJob
	}
	return nil
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
	queued, err := q.client.Runs().List(ctx, flow.ListRunsOpts{Function: q.functionID, Status: []string{"queued"}, Limit: 200})
	if err != nil {
		return ports.QueueStats{}, err
	}
	var oldestMs int64
	// Newest first. With more than one page this is the oldest of the newest
	// 200, so it can read low.
	if n := len(queued.Items); n > 0 {
		oldestMs = max(0, time.Now().UnixMilli()-queued.Items[n-1].CreatedAt)
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
	// characters, since the failure function adds "-failure".
	ID string
	// Concurrency is how many jobs of one PartitionKey run at once. Zero: no
	// limit. Job priorities only take effect under a limit.
	Concurrency int
	// Retries is Flow's retries for a job that fails. Flow's default when nil.
	Retries *int
	// StepTimeout is the longest one job may run. Defaults to 14 minutes,
	// Flow's most. A segment that runs longer is cut and retried.
	StepTimeout time.Duration
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
	if opts.ID == "" {
		opts.ID = FunctionID
	}
	if opts.StepTimeout == 0 {
		opts.StepTimeout = 14 * time.Minute
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
		fnOpts.Concurrency = []flow.Concurrency{{Limit: opts.Concurrency, Key: flow.StrPtr("event.data.job.partitionKey")}}
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
