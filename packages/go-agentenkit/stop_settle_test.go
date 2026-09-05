package agentenkit_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	agentenkit "github.com/eadwinCode/agentic-kit/packages/go-agentenkit"
)

// settleSpy counts settles and keeps what the last one saw.
type settleSpy struct {
	mu    sync.Mutex
	n     atomic.Int32
	seen  agentenkit.RunFinishInfo
	final atomic.Int32
}

func (s *settleSpy) settle(_ context.Context, info agentenkit.RunFinishInfo) error {
	s.mu.Lock()
	s.seen = info
	s.mu.Unlock()
	s.n.Add(1)
	return nil
}

func (s *settleSpy) last() agentenkit.RunFinishInfo {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.seen
}

// A stop that ends a run no worker holds settles it right there, once: the
// queued run that never reached a worker, and the parked run whose worker
// is long gone. The steps made before the park were priced as they ran, so
// their usage reaches the hook. A redelivered job afterwards settles nothing.
func TestStop_SettlesAQueuedOrParkedRunExactlyOnce(t *testing.T) {
	for _, parked := range []bool{false, true} {
		name := "queued"
		if parked {
			name = "parked"
		}
		t.Run(name, func(t *testing.T) {
			h := makeRuntime(t, scripted(step{calls: []call{{"c1", "wipe", `{}`}}, usage: &[2]int{20, 7}}),
				func(c *agentenkit.AgentConfig) { c.HITLTTL = time.Hour })
			wipe := tool("wipe", nil)
			wipe.RequiresConfirmation = true
			spy := &settleSpy{}
			chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{
				Name: "chat", Tools: []agentenkit.Tool{wipe},
				OnSettle: spy.settle,
				OnFinish: func(agentenkit.RunFinishInfo) { spy.final.Add(1) },
			})
			ran := h.run(t, chat, agentenkit.RunInput{Prompt: "go"})
			if parked {
				h.handleNext(t)
				mustEqual(t, h.thread(t, ran.ThreadID).State, agentenkit.StateWaitingForInput, "parked")
			}
			mustEqual(t, spy.n.Load(), int32(0), "nothing settled before the stop")

			res, err := chat.Stop(h.ctx, ran.ThreadID, nil)
			if err != nil || !res.Accepted {
				t.Fatalf("stop: %+v %v", res, err)
			}
			mustEqual(t, spy.n.Load(), int32(1), "the stop settled the run")
			mustEqual(t, spy.final.Load(), int32(1), "and finished it")
			seen := spy.last()
			mustEqual(t, seen.RunID, ran.RunID, "run")
			mustEqual(t, seen.Cancelled, true, "as a stop")
			mustEqual(t, seen.State, agentenkit.StateCancelled, "state")
			mustEqual(t, seen.StopReason, "cancelled", "reason")
			if seen.UsageErr != nil {
				t.Fatalf("bill not read: %v", seen.UsageErr)
			}
			if parked {
				mustEqual(t, seen.Usage.TotalTokens, 27, "the parked segment's usage is on the bill")
				mustEqual(t, seen.TokensUsed, 27, "and on the info")
			} else {
				mustEqual(t, seen.Usage.TotalTokens, 0, "a queued run spent nothing")
			}
			rec, _ := h.admin.Runs().Get(h.ctx, ran.RunID)
			if rec.SettledAt == nil {
				t.Fatal("the run does not record its settle")
			}
			mustEqual(t, h.kvGet(agentenkit.RunLockKey(ran.ThreadID)), "", "the lock is released after the settle")

			// The original job (queued) or the park's expiry job (parked) is
			// still in the queue: a worker that wakes up later settles nothing.
			h.drain(t)
			mustEqual(t, spy.n.Load(), int32(1), "a redelivered job settles nothing")
			mustEqual(t, spy.final.Load(), int32(1), "and finishes nothing")
		})
	}
}

// A stop during a live step leaves the settle to the worker that holds the
// run: it reaches the hook cancelled, once, with the cut-off call billed.
func TestStop_DuringALiveStepSettlesInTheWorkerOnly(t *testing.T) {
	h := makeRuntime(t, scripted(step{text: "slow", delay: 500 * time.Millisecond}))
	spy := &settleSpy{}
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{Name: "chat", OnSettle: spy.settle})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "hi"})
	job, _ := h.queue.Shift()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, _ = h.rt.Worker.HandleJob(h.ctx, job)
	}()
	time.Sleep(30 * time.Millisecond)
	if _, err := chat.Stop(h.ctx, ran.ThreadID, nil); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	mustEqual(t, spy.n.Load(), int32(1), "settled once, by the worker")
	seen := spy.last()
	mustEqual(t, seen.Cancelled, true, "as a stop")
	mustEqual(t, seen.RunID, ran.RunID, "run")
	rec, _ := h.admin.Runs().Get(h.ctx, ran.RunID)
	if rec.SettledAt == nil {
		t.Fatal("the worker's settle is not recorded")
	}
}

// A stop through a different agent's handle still settles with the hook of
// the agent whose run it was.
func TestStop_SettlesWithTheHookOfTheAgentWhoseRunItWas(t *testing.T) {
	h := makeRuntime(t, scripted(step{text: "never"}))
	spy := &settleSpy{}
	planner := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{Name: "planner", OnSettle: spy.settle})
	other := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{Name: "other"})
	ran := h.run(t, planner, agentenkit.RunInput{Prompt: "plan"})
	if _, err := other.Stop(h.ctx, ran.ThreadID, nil); err != nil {
		t.Fatal(err)
	}
	mustEqual(t, spy.n.Load(), int32(1), "the planner's hook ran")
	mustEqual(t, spy.last().RunID, ran.RunID, "for the planner's run")
}

// The finished run records its settle too, so nothing that arrives later
// can settle it again.
func TestSettle_AFinishedRunIsMarkedSettled(t *testing.T) {
	h := makeRuntime(t, scripted(step{text: "ok"}))
	spy := &settleSpy{}
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{Name: "chat", OnSettle: spy.settle})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "hi"})
	h.handleNext(t)
	rec, _ := h.admin.Runs().Get(h.ctx, ran.RunID)
	if rec.SettledAt == nil {
		t.Fatal("a completed run does not record its settle")
	}
	mustEqual(t, spy.n.Load(), int32(1), "once")
}
