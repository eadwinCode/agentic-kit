package ports

import (
	"context"
	"time"
)

// EnqueueOptions tunes one dispatch.
type EnqueueOptions struct {
	// Delay holds the job before delivering it: the HITL expiry (§2.5) and a
	// job blocked by an older run's lock (§2.8).
	//
	// An adapter that cannot delay may deliver immediately (both callers
	// treat an early arrival as a no-op) but it must NOT fail, or a park
	// would fail the run that scheduled it.
	Delay time.Duration
}

// Queue is the durable dispatch port (§2.8). Delivery is at-least-once; the
// engine's state guard and the run lock make double dispatch a no-op.
type Queue interface {
	Enqueue(ctx context.Context, job RunJob, opts *EnqueueOptions) error
}
