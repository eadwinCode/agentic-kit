package agentenkit_test

import (
	"context"
	"testing"
	"time"

	agentenkit "github.com/eadwinCode/agentic-kit/packages/go-agentenkit"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

// A park hands the same run id to two later deliveries: the answer and the
// expiry (§2.5). Either can land while the parking segment is still winding
// down and holding the run lock. That used to look like a duplicate delivery
// of the running segment and was dropped, which left the thread waiting
// forever: nobody re-sends an expiry. It is now redriven, because the
// thread's durable state already says WAITING_FOR_INPUT when that happens.
func TestHitl_AnExpiryThatMeetsTheParkingSegmentsLockIsRedrivenNotDropped(t *testing.T) {
	model := scripted(step{calls: []call{{"c1", "danger", `{}`}}}, step{text: "all done"})
	h := makeRuntime(t, model, func(c *agentenkit.AgentConfig) {
		c.HITLTTL = 20 * time.Millisecond
		c.ReclaimGrace = 0
		c.RunRedriveDelay = time.Millisecond
	})
	ran := 0
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{
		Name: "chat", Model: "gpt-4o",
		Tools: []agentenkit.Tool{agentenkit.MarkRequiresConfirmation(tool("danger", func(context.Context, map[string]any) (string, error) {
			ran++
			return "ok", nil
		}))},
	})
	res := h.run(t, chat, agentenkit.RunInput{Prompt: "hi"})

	// The first segment runs, parks, and schedules its own expiry.
	h.handleNext(t)
	mustEqual(t, h.thread(t, res.ThreadID).State, agentenkit.StateWaitingForInput, "parked")
	mustEqual(t, h.queue.Len(), 1, "the park's expiry job is queued")

	// The parking segment is still holding the lock when the expiry arrives:
	// the lock carries the run's own id.
	if _, err := h.kv.Set(h.ctx, agentenkit.RunLockKey(res.ThreadID), res.RunID, ports.SetOptions{}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(25 * time.Millisecond) // past the TTL, so the expiry is due
	h.handleNext(t)
	mustEqual(t, h.queue.Len(), 1, "the expiry is redriven, not dropped as a duplicate")
	mustEqual(t, h.thread(t, res.ThreadID).State, agentenkit.StateWaitingForInput, "still parked while the lock is held")

	// The segment lets go; the redriven expiry now resolves the park and
	// the run finishes on the timeout denial.
	if err := h.kv.Del(h.ctx, agentenkit.RunLockKey(res.ThreadID)); err != nil {
		t.Fatal(err)
	}
	h.drain(t)
	mustEqual(t, h.thread(t, res.ThreadID).State, agentenkit.StateCompleted, "completed after the expiry")
	mustEqual(t, ran, 0, "the expired approval never ran the tool")

	// A duplicate delivery of a RUNNING segment is still a no-op.
	res2 := h.run(t, chat, agentenkit.RunInput{ThreadID: res.ThreadID, Prompt: "again"})
	if _, err := h.kv.Set(h.ctx, agentenkit.RunLockKey(res.ThreadID), res2.RunID, ports.SetOptions{}); err != nil {
		t.Fatal(err)
	}
	job, _ := h.queue.Shift()
	if _, err := h.rt.Worker.HandleJob(h.ctx, job); err != nil {
		t.Fatal(err)
	}
	mustEqual(t, h.queue.Len(), 0, "a duplicate of a running segment is dropped")
	_ = h.kv.Del(h.ctx, agentenkit.RunLockKey(res.ThreadID))
}
