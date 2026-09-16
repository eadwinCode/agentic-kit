package ports

import (
	"context"
	"errors"
	"time"
)

// JobKind says why a job was enqueued. A fresh user run is the default; the
// platform's own follow-ups name themselves so a queue can log them, order
// them and cap only the kind that brings new work in.
type JobKind string

const (
	// JobDispatch is a fresh run from Run: the only kind a depth cap refuses.
	JobDispatch JobKind = ""
	// JobRetry is the same run trying again after a failure (§2.8).
	JobRetry JobKind = "retry"
	// JobRedrive is a job that found the run lock held by an older run.
	JobRedrive JobKind = "redrive"
	// JobResume continues a parked run after an answer (§2.5).
	JobResume JobKind = "resume"
	// JobExpiry is a park's own deadline (§2.5).
	JobExpiry JobKind = "expiry"
	// JobReclaim re-dispatches an orphaned thread (§2.5).
	JobReclaim JobKind = "reclaim"
)

// PriorityLow is the priority of the platform's own housekeeping: park
// expiries, reclaims and lock redrives. A user's message is never queued
// behind them.
const PriorityLow = -10

var (
	// ErrQueueFull is what Enqueue returns when a depth cap is set and the
	// queue has reached it. Only a fresh dispatch is ever refused; retries,
	// redrives, resumes and expiries always go in, or a run already under way
	// would be stranded.
	ErrQueueFull = errors.New("agentenkit: queue is full")
	// ErrPayloadTooLarge is a job whose encoded payload is over the adapter's
	// cap.
	ErrPayloadTooLarge = errors.New("agentenkit: job payload too large")
	// ErrDuplicateJob is an enqueue whose Key is already waiting or running.
	ErrDuplicateJob = errors.New("agentenkit: a job with this key is already queued")
	// ErrUnsupported is an adapter's answer to an operation it cannot do,
	// such as counting on a queue with no read side.
	ErrUnsupported = errors.New("agentenkit: not supported by this adapter")
)

// EnqueueOptions tunes one dispatch.
type EnqueueOptions struct {
	// Delay holds the job before delivering it: the HITL expiry (§2.5), a
	// job blocked by an older run's lock (§2.8) and a failed run's retry
	// backoff.
	//
	// An adapter that cannot delay may deliver immediately (every caller
	// treats an early arrival as a no-op) but it must NOT fail, or a park
	// would fail the run that scheduled it.
	Delay time.Duration
	// Key dedupes. At most one live row per key: a second enqueue with the
	// same key is refused with ErrDuplicateJob, and Cancel drops the waiting
	// row for a key. Empty means no dedupe.
	Key string
	// Priority orders ready rows: higher first, then oldest first. Zero is a
	// normal dispatch; the platform's housekeeping uses PriorityLow.
	Priority int
}

// QueueStats is what a queue can say about itself. Every count is scoped to
// the consumer's own namespace.
type QueueStats struct {
	// Ready is jobs a worker could take right now.
	Ready int `json:"ready"`
	// Delayed is jobs held for a future run time: park expiries, retry
	// backoffs. Not backlog.
	Delayed int `json:"delayed"`
	// InFlight is jobs a worker holds a lease on.
	InFlight int `json:"inFlight"`
	// Dead is jobs the queue gave up on and kept for an operator.
	Dead int `json:"dead"`
	// OldestReadyMs is how long the oldest ready job has been waiting. The
	// number that says a queue is stuck rather than merely busy.
	OldestReadyMs int64 `json:"oldestReadyMs"`
	// Paused is true while the consumer is told not to claim.
	Paused bool `json:"paused"`
	// LastClaimAt is when this process last took a job; nil when it never has.
	LastClaimAt *time.Time `json:"lastClaimAt,omitempty"`
	// ClaimErrors is how many claims in a row have failed in this process.
	ClaimErrors int `json:"claimErrors"`
}

// QueuedJob is one row as the queue sees it, for a host that asks where a
// run's job is.
type QueuedJob struct {
	ID          string     `json:"id"`
	RunID       string     `json:"runId"`
	ThreadID    string     `json:"threadId"`
	Kind        JobKind    `json:"kind"`
	Attempts    int        `json:"attempts"`
	RunAt       time.Time  `json:"runAt"`
	LockedUntil *time.Time `json:"lockedUntil,omitempty"`
	// Position is how many ready jobs are ahead of it; -1 when the adapter
	// cannot say.
	Position int `json:"position"`
}

// Queue is the durable dispatch port (§2.8). Delivery is at-least-once; the
// engine's state guard and the run lock make double dispatch a no-op.
//
// Beside Enqueue the port carries the little an engine and a host need to
// see the queue: a way to withdraw a job by key, a way to find a run's job,
// and counts. An adapter that has no read side answers ErrUnsupported to
// Find and Stats; callers treat that as "unknown", never as a failure.
type Queue interface {
	Enqueue(ctx context.Context, job RunJob, opts *EnqueueOptions) error
	// Cancel drops every waiting job enqueued under key. A key nothing is
	// waiting under is not an error. Best-effort by contract: a delivered
	// row is a correct no-op anyway.
	Cancel(ctx context.Context, key string) error
	// Find returns the earliest waiting or running job for a run, or nil
	// when the queue holds none.
	Find(ctx context.Context, runID string) (*QueuedJob, error)
	// Stats counts what waits, runs and died.
	Stats(ctx context.Context) (QueueStats, error)
}
