package core

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

// stepLimit is the wall-time limit of one step (§2.8). It starts at
// Config.StepTimeout. While a call to a tool with its own Timeout runs, the
// end moves out to cover it, so one slow tool gets the time it needs without
// raising StepTimeout for every step of every run. Once the call returns,
// the step is back to StepTimeout, counted from then.
//
// A context deadline cannot be moved, so when a step's tools include one
// with a Timeout, the step's context is cancelled by a timer instead, and
// the loop asks the limit (not the context) whether the step ran out of
// time. A step with no such tool keeps a plain context deadline.
type stepLimit struct {
	mu   sync.Mutex
	base time.Duration
	// baseEnd is when StepTimeout runs out: from the step's start, or from
	// the last timed call's return when that is later.
	baseEnd time.Time
	cancel  context.CancelCauseFunc
	// fixed is the plain deadline context of a step with no timed tool.
	fixed context.Context
	// calls are the timed calls running now, by id.
	calls  map[int]timedCall
	nextID int
	timer  *time.Timer
	gen    int // a timer from an older end does nothing
	end    time.Time
	// limit is what set end, and what the error says the step ran past.
	limit   time.Duration
	expired bool
	done    bool
}

type timedCall struct {
	end   time.Time
	limit time.Duration
}

// startStepLimit starts a step's limit. base is Config.StepTimeout; zero
// is no limit for the step, though a tool's own Timeout still holds.
func startStepLimit(parent context.Context, base time.Duration, tools []ports.Tool) (context.Context, *stepLimit) {
	s := &stepLimit{base: base, limit: base, calls: map[int]timedCall{}}
	if !hasTimedTool(tools) {
		ctx, cancel := parent, context.CancelFunc(func() {})
		if base > 0 {
			ctx, cancel = context.WithTimeout(parent, base)
		}
		s.fixed = ctx
		s.cancel = func(error) { cancel() }
		return ctx, s
	}
	ctx, cancel := context.WithCancelCause(parent)
	s.cancel = cancel
	if base > 0 {
		s.baseEnd = time.Now().Add(base)
		s.arm(s.baseEnd, base)
	}
	return ctx, s
}

// isTimed reports whether t has its own Timeout to keep to.
func isTimed(t ports.Tool) bool { return t.Timeout > 0 && t.Execute != nil }

func hasTimedTool(tools []ports.Tool) bool {
	for _, t := range tools {
		if isTimed(t) {
			return true
		}
	}
	return false
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
		fire := gen == s.gen && s.markExpired(limit)
		s.mu.Unlock()
		if fire {
			s.cancel(context.DeadlineExceeded)
		}
	})
}

// rearm sets the end to the latest of StepTimeout and the timed calls
// running now. A step with no StepTimeout keeps no end of its own: each
// timed call's own deadline still holds. Called with mu held.
func (s *stepLimit) rearm() {
	if s.done || s.expired || s.baseEnd.IsZero() {
		return
	}
	end, limit := s.baseEnd, s.base
	for _, c := range s.calls {
		if c.end.After(end) {
			end, limit = c.end, c.limit
		}
	}
	if !end.Equal(s.end) {
		s.arm(end, limit)
	}
}

// begin notes a timed call that may run for d, and returns its id.
func (s *stepLimit) begin(d time.Duration) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextID++
	s.calls[s.nextID] = timedCall{end: time.Now().Add(d), limit: d}
	s.rearm()
	return s.nextID
}

// finish notes the call has returned: the step has StepTimeout from now to
// finish, unless other timed calls still run longer.
func (s *stepLimit) finish(id int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.calls, id)
	if !s.baseEnd.IsZero() {
		if end := time.Now().Add(s.base); end.After(s.baseEnd) {
			s.baseEnd = end
		}
	}
	s.rearm()
}

// markExpired notes the step ran past limit, once. Called with mu held.
func (s *stepLimit) markExpired(limit time.Duration) bool {
	if s.done || s.expired {
		return false
	}
	s.expired, s.limit = true, limit
	return true
}

// expire ends the step as timed out, once.
func (s *stepLimit) expire(limit time.Duration) {
	s.mu.Lock()
	fire := s.markExpired(limit)
	s.mu.Unlock()
	if fire {
		s.cancel(context.DeadlineExceeded)
	}
}

// stop ends the limit when the step is over, and says whether the step ran
// out of time and which limit it ran past.
func (s *stepLimit) stop() (expired bool, limit time.Duration) {
	if s.fixed != nil {
		expired = s.base > 0 && errors.Is(s.fixed.Err(), context.DeadlineExceeded)
		s.cancel(context.Canceled)
		return expired, s.base
	}
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
	if s.fixed != nil {
		return tools // no tool here has a Timeout
	}
	out := make([]ports.Tool, 0, len(tools))
	for _, t := range tools {
		if !isTimed(t) {
			out = append(out, t)
			continue
		}
		inner, timeout := t.Execute, t.Timeout
		wrapped := t
		wrapped.Execute = func(ctx context.Context, input json.RawMessage) (string, error) {
			if err := ctx.Err(); err != nil {
				return "", err // the step is already over: do not start
			}
			id := s.begin(timeout)
			defer s.finish(id)
			callCtx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()
			// Ends the step as soon as the call runs out of time, so other
			// calls in the step stop too.
			stopEarly := context.AfterFunc(callCtx, func() {
				if callCtx.Err() == context.DeadlineExceeded && ctx.Err() == nil {
					s.expire(timeout)
				}
			})
			defer stopEarly()
			output, err := inner(callCtx, input)
			// Checked here as well, before the call hands back its result:
			// the early check runs on its own goroutine and may come too
			// late, after the step is already over.
			if errors.Is(callCtx.Err(), context.DeadlineExceeded) && ctx.Err() == nil {
				s.expire(timeout)
			}
			return output, err
		}
		out = append(out, wrapped)
	}
	return out
}
