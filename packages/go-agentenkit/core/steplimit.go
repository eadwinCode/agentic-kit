package core

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

// stepLimit is the wall-time limit of one step (§2.8). It starts at
// Config.StepTimeout. A tool with its own Timeout moves the end out while it
// runs, so one slow tool gets the time it needs without raising StepTimeout
// for every step of every run.
//
// A context deadline cannot be moved, so the step's context is cancelled by
// a timer instead, and the loop asks the limit (not the context) whether the
// step ran out of time.
type stepLimit struct {
	mu     sync.Mutex
	cancel context.CancelCauseFunc
	timer  *time.Timer
	gen    int // a timer from an older end does nothing
	end    time.Time
	// limit is what set end, and what the error says the step ran past.
	limit   time.Duration
	expired bool
	done    bool
}

// startStepLimit starts a step's limit. base is Config.StepTimeout; zero
// is no limit for the step, though a tool's own Timeout still holds.
func startStepLimit(parent context.Context, base time.Duration) (context.Context, *stepLimit) {
	ctx, cancel := context.WithCancelCause(parent)
	s := &stepLimit{cancel: cancel}
	if base > 0 {
		s.arm(time.Now().Add(base), base)
	}
	return ctx, s
}

// arm sets the end and the timer for it. Called with mu held, or before the
// limit is shared.
func (s *stepLimit) arm(end time.Time, limit time.Duration) {
	s.end, s.limit = end, limit
	s.gen++
	gen := s.gen
	if s.timer != nil {
		s.timer.Stop()
	}
	s.timer = time.AfterFunc(time.Until(end), func() {
		s.mu.Lock()
		current := gen == s.gen
		s.mu.Unlock()
		if current {
			s.expire(limit)
		}
	})
}

// extend gives the step at least d from now. It never brings the end in,
// and a step with no limit keeps none: the tool's own timer still holds.
func (s *stepLimit) extend(d time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.done || s.expired || s.end.IsZero() {
		return
	}
	if end := time.Now().Add(d); end.After(s.end) {
		s.arm(end, d)
	}
}

// expire ends the step as timed out, once.
func (s *stepLimit) expire(limit time.Duration) {
	s.mu.Lock()
	if s.done || s.expired {
		s.mu.Unlock()
		return
	}
	s.expired, s.limit = true, limit
	s.mu.Unlock()
	s.cancel(context.DeadlineExceeded)
}

// stop ends the limit when the step is over, and says whether the step ran
// out of time and which limit it ran past.
func (s *stepLimit) stop() (expired bool, limit time.Duration) {
	s.mu.Lock()
	s.done = true
	if s.timer != nil {
		s.timer.Stop()
	}
	expired, limit = s.expired, s.limit
	s.mu.Unlock()
	s.cancel(context.Canceled)
	return expired, limit
}

// withStepLimit makes each tool with its own Timeout keep to it inside the
// step: while it runs the step has at least that long, and a call past it
// ends the step the way a step past StepTimeout ends.
func withStepLimit(tools []ports.Tool, s *stepLimit) []ports.Tool {
	out := make([]ports.Tool, 0, len(tools))
	for _, t := range tools {
		if t.Timeout <= 0 || t.Execute == nil {
			out = append(out, t)
			continue
		}
		inner, timeout := t.Execute, t.Timeout
		wrapped := t
		wrapped.Execute = func(ctx context.Context, input json.RawMessage) (string, error) {
			s.extend(timeout)
			callCtx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()
			stopTimer := context.AfterFunc(callCtx, func() {
				if callCtx.Err() == context.DeadlineExceeded && ctx.Err() == nil {
					s.expire(timeout)
				}
			})
			defer stopTimer()
			return inner(callCtx, input)
		}
		out = append(out, wrapped)
	}
	return out
}
