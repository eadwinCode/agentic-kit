package agentenkit_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	agentenkit "github.com/eadwinCode/agentic-kit/packages/go-agentenkit"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

// These tests pin the queue and lifecycle hardening: what happens when too
// many runs are queued at once, and what happens to a run that is queued
// but not started. Each one names the property it holds.

// A queue at its depth cap refuses a NEW run before anything is written:
// the thread stays idle, no message, no record. Once the queue drains the
// same send goes through.
func TestOverload_AFullQueueRefusesANewRunBeforeWritingAnything(t *testing.T) {
	h := makeRuntime(t, scripted(step{text: "ok"}), func(c *agentenkit.AgentConfig) { c.MaxQueueDepth = 1 })
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{Name: "chat"})
	h.run(t, chat, agentenkit.RunInput{Prompt: "one"})

	thread, err := h.storage.Threads().Create(h.ctx, ports.ThreadInit{Model: "gpt-4o"}, ports.StorageContext{})
	if err != nil {
		t.Fatal(err)
	}
	res, err := chat.Run(h.ctx, agentenkit.RunInput{ThreadID: thread.ID, Prompt: "two"})
	if err != nil {
		t.Fatal(err)
	}
	mustEqual(t, res.Accepted, false, "refused")
	mustEqual(t, res.Reason, agentenkit.RefusedQueueFull, "the refusal names the cause")
	mustEqual(t, h.thread(t, thread.ID).State, agentenkit.StateIdle, "the thread is untouched")
	mustEqual(t, len(h.storage.MessageRows(thread.ID)), 0, "nothing was written")
	runs, _ := h.admin.Runs().ListByThread(h.ctx, thread.ID)
	mustEqual(t, len(runs), 0, "no run record was opened")
	mustEqual(t, h.queue.Len(), 1, "only the first run is queued")
	mustEqual(t, len(h.events(thread.ID, "RUN_REFUSED")), 1, "every client on the thread sees why")

	h.handleNext(t)
	h.run(t, chat, agentenkit.RunInput{ThreadID: thread.ID, Prompt: "two"})
}

// The adapter's own cap refuses a fresh dispatch too, and only that: a
// retry of a run already under way always goes in.
func TestOverload_TheAdapterCapSparesRetries(t *testing.T) {
	h := makeRuntime(t, scripted(step{err: errBoom}, step{text: "ok"}), func(c *agentenkit.AgentConfig) { c.RunRetryBackoff = 0 })
	h.queue.MaxDepth = 1
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{Name: "chat"})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "one"})
	h.handleNext(t) // fails once: the retry is queued although the cap is 1
	mustEqual(t, h.queue.Len(), 1, "the retry went in")
	mustEqual(t, h.queue.Items()[0].Kind, agentenkit.JobRetry, "as a retry")

	thread, _ := h.storage.Threads().Create(h.ctx, ports.ThreadInit{Model: "gpt-4o"}, ports.StorageContext{})
	res, err := chat.Run(h.ctx, agentenkit.RunInput{ThreadID: thread.ID, Prompt: "two"})
	if err == nil || res.Accepted {
		t.Fatalf("a fresh dispatch past the cap must be refused: %+v %v", res, err)
	}
	if !errors.Is(err, agentenkit.ErrQueueFull) {
		t.Fatalf("the error names the cause: %v", err)
	}
	mustEqual(t, res.Reason, agentenkit.RefusedQueueFull, "reason on the result")
	h.drain(t)
	mustEqual(t, h.lastTerminal(ran.ThreadID)["state"], "COMPLETED", "the retried run finished")
}

// The dispatch ticket carries the caller's tenant and when the run was
// first dispatched; the platform's own follow-ups queue behind every user
// message and keep the run's place in line.
func TestOverload_TheTicketCarriesTheTenantAndHousekeepingQueuesBehindUsers(t *testing.T) {
	h := hitlSetup(t, func(c *agentenkit.AgentConfig) { c.HITLTTL = time.Hour })
	h.run(t, h.chat, agentenkit.RunInput{Prompt: "delete", PartitionKey: "team-a"})
	job := h.queue.Items()[0]
	mustEqual(t, job.PartitionKey, "team-a", "the tenant rides the ticket")
	mustEqual(t, job.Kind, agentenkit.JobDispatch, "a fresh dispatch")
	if job.DispatchedAt == 0 || job.EnqueuedAt == 0 {
		t.Fatalf("both clocks are stamped: %+v", job)
	}
	mustEqual(t, h.queue.Priorities()[0], 0, "a user's message has normal priority")

	h.handleNext(t) // parks and schedules its expiry
	expiry := h.queue.Items()[0]
	mustEqual(t, expiry.Kind, agentenkit.JobExpiry, "the expiry says what it is")
	mustEqual(t, h.queue.Priorities()[0], agentenkit.PriorityLow, "the expiry queues behind every user message")
	mustEqual(t, h.queue.Keys()[0], "hitl-expiry:c1", "keyed so an answer can withdraw it")
	mustEqual(t, expiry.DispatchedAt, job.DispatchedAt, "a follow-up keeps the run's place in line")
}

// An answer withdraws the park's expiry row, and a second answer can
// neither rewrite the first nor queue a second resume.
func TestHITL_AnAnswerWithdrawsTheExpiryAndASecondAnswerCannotRewriteIt(t *testing.T) {
	h := hitlSetup(t)
	ran := h.run(t, h.chat, agentenkit.RunInput{Prompt: "delete"})
	h.handleNext(t)
	mustEqual(t, h.queue.Len(), 1, "the expiry is queued")

	res, err := h.rt.HITL.Respond(h.ctx, agentenkit.RespondInput{ThreadID: ran.ThreadID, ToolCallID: "c1", Approved: true})
	if err != nil || !res.Delivered {
		t.Fatalf("respond: %v %+v", err, res)
	}
	mustEqual(t, h.queue.Len(), 1, "the expiry is gone; the resume is queued")
	mustEqual(t, h.queue.Keys()[0], "hitl-resume:c1", "one resume per answer")
	mustEqual(t, h.queue.Items()[0].Kind, agentenkit.JobResume, "kind")

	again, err := h.rt.HITL.Respond(h.ctx, agentenkit.RespondInput{ThreadID: ran.ThreadID, ToolCallID: "c1", Approved: false})
	if err != nil {
		t.Fatal(err)
	}
	mustEqual(t, again.Delivered, false, "a second answer is refused")
	if !strings.Contains(again.Error, "already answered") {
		t.Fatalf("error: %q", again.Error)
	}
	mustEqual(t, h.queue.Len(), 1, "no second resume")
	h.drain(t)
	mustStrings(t, *h.executed, []string{"acc_1"}, "the first answer stood: the tool ran")
	mustEqual(t, h.thread(t, ran.ThreadID).State, agentenkit.StateCompleted, "completed")
}

// A step past StepTimeout fails like any failed step and the run is retried.
func TestDeadline_AStepPastStepTimeoutFailsAndIsRetried(t *testing.T) {
	h := makeRuntime(t, scripted(step{text: "slow", delay: 300 * time.Millisecond}, step{text: "fast"}),
		func(c *agentenkit.AgentConfig) { c.StepTimeout = 50 * time.Millisecond; c.RunRetryBackoff = 0 })
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{Name: "chat"})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "go"})
	h.handleNext(t)
	mustEqual(t, h.queue.Len(), 1, "the timed-out step put a retry on the queue")
	mustEqual(t, h.queue.Items()[0].Kind, agentenkit.JobRetry, "kind")
	mustEqual(t, h.thread(t, ran.ThreadID).State, agentenkit.StateQueued, "waiting to retry")
	h.handleNext(t)
	mustEqual(t, h.lastTerminal(ran.ThreadID)["state"], "COMPLETED", "the retry finished")
	mustEqual(t, h.model.Calls(), 2, "two model calls")
}

// A segment past SegmentTimeout settles the run FAILED with a reason, bills
// what it spent, and holds no worker: it is not retried.
func TestDeadline_ASegmentPastSegmentTimeoutSettlesFailed(t *testing.T) {
	var settled []agentenkit.RunFinishInfo
	h := makeRuntime(t, scripted(step{text: "slow", delay: 400 * time.Millisecond}),
		func(c *agentenkit.AgentConfig) { c.SegmentTimeout = 60 * time.Millisecond })
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{
		Name: "chat",
		OnSettle: func(_ context.Context, info agentenkit.RunFinishInfo) error {
			settled = append(settled, info)
			return nil
		},
	})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "go"})
	started := time.Now()
	h.handleNext(t)
	if time.Since(started) > 300*time.Millisecond {
		t.Fatal("the segment must end at the deadline, not when the model gives up")
	}
	term := h.lastTerminal(ran.ThreadID)
	mustEqual(t, term["state"], "FAILED", "state")
	mustEqual(t, term["stopReason"], "timeout", "stopReason")
	if !strings.Contains(term["error"].(string), "longer than") {
		t.Fatalf("error: %v", term["error"])
	}
	rec, _ := h.admin.Runs().Get(h.ctx, ran.RunID)
	mustEqual(t, rec.State, agentenkit.StateFailed, "record")
	if rec.SettledAt == nil {
		t.Fatal("the run settled")
	}
	mustEqual(t, len(settled), 1, "the settle hook ran once")
	mustEqual(t, settled[0].State, agentenkit.StateFailed, "with the failure")
	mustEqual(t, h.queue.Len(), 0, "not retried")
	mustEqual(t, h.kvGet(agentenkit.RunLockKey(ran.ThreadID)), "", "lock released")
}

// A run that fails waits before it tries again, and waits longer each time.
func TestRetry_WaitsLongerEachTime(t *testing.T) {
	h := makeRuntime(t, scripted(step{err: errBoom}), func(c *agentenkit.AgentConfig) {
		c.RunMaxAttempts = 4
		c.RunRetryBackoff = 100 * time.Millisecond
		c.RunRetryBackoffMax = time.Second
	})
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{Name: "chat"})
	h.run(t, chat, agentenkit.RunInput{Prompt: "go"})
	want := []time.Duration{100 * time.Millisecond, 200 * time.Millisecond, 400 * time.Millisecond}
	for i, base := range want {
		h.handleNext(t)
		mustEqual(t, h.queue.Len(), 1, "retry queued")
		got := h.queue.Delays()[0]
		if got < base || got > base+base/4 {
			t.Fatalf("retry %d waits %v, want %v plus up to a quarter of jitter", i+1, got, base)
		}
	}
}

// A large value in the run state is capped on the operational record; the
// small keys a listing needs survive, and the job itself keeps everything.
func TestAdmin_CapsALargeRunStateValueButKeepsTheSmallOnes(t *testing.T) {
	big := strings.Repeat("x", 5000)
	h := makeRuntime(t, scripted(step{text: "ok"}), func(c *agentenkit.AgentConfig) { c.PayloadCapChars = 100 })
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{Name: "chat"})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "go", State: agentenkit.AgentRunState{"tenant": "acme", "blob": big}})
	rec, _ := h.admin.Runs().Get(h.ctx, ran.RunID)
	mustEqual(t, rec.RunState["tenant"], "acme", "small keys survive")
	capped, _ := rec.RunState["blob"].(string)
	mustEqual(t, len([]rune(capped)), 101, "the large value is cut")
	mustEqual(t, h.queue.Items()[0].State["blob"], big, "the ticket carries the whole state")
}

// The run lock is renewed while the segment runs, so it outlives its own
// lease; a worker that is alive never loses it.
func TestLock_IsRenewedWhileTheSegmentRuns(t *testing.T) {
	h := makeRuntime(t, scripted(step{text: "slow", delay: 1400 * time.Millisecond}),
		func(c *agentenkit.AgentConfig) { c.RunLockLease = time.Second })
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{Name: "chat"})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "go"})
	job, _ := h.queue.Shift()
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = h.rt.Worker.HandleJob(h.ctx, job)
	}()
	time.Sleep(1200 * time.Millisecond) // past the lease: only a renewal keeps it
	holder, _ := agentenkit.ParseLockValue(h.kvGet(agentenkit.RunLockKey(ran.ThreadID)))
	mustEqual(t, holder, ran.RunID, "the lock is still this worker's")
	<-done
	mustEqual(t, h.kvGet(agentenkit.RunLockKey(ran.ThreadID)), "", "released at the end")
	mustEqual(t, h.lastTerminal(ran.ThreadID)["state"], "COMPLETED", "completed")
}

// A lock the worker can no longer hold ends the segment; the job comes back
// and resumes from the persisted steps instead of running blind.
func TestLock_ALostLockEndsTheSegmentAndTheJobComesBack(t *testing.T) {
	h := makeRuntime(t, scripted(step{text: "slow", delay: 900 * time.Millisecond}, step{text: "again"}),
		func(c *agentenkit.AgentConfig) { c.RunLockLease = time.Second; c.RunRedriveDelay = time.Millisecond })
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{Name: "chat"})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "go"})
	job, _ := h.queue.Shift()
	done := make(chan error, 1)
	go func() {
		_, err := h.rt.Worker.HandleJob(h.ctx, job)
		done <- err
	}()
	time.Sleep(100 * time.Millisecond)
	_ = h.kv.Del(h.ctx, agentenkit.RunLockKey(ran.ThreadID)) // the kv lost it
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	mustEqual(t, h.queue.Len(), 1, "the job came back")
	mustEqual(t, h.queue.Items()[0].Kind, agentenkit.JobRedrive, "as a redrive")
	mustEqual(t, h.queue.Items()[0].RunID, ran.RunID, "same run")
	h.drain(t)
	mustEqual(t, h.lastTerminal(ran.ThreadID)["state"], "COMPLETED", "the redriven job finished the run")
}

// A dead worker's lock is not a duplicate: once it lapses the redelivery
// takes the lock and resumes the run.
func TestLock_ADeadWorkersLockExpiresAndTheRedeliveryResumesTheRun(t *testing.T) {
	h := makeRuntime(t, scripted(step{text: "ok"}), func(c *agentenkit.AgentConfig) { c.RunLockLease = time.Second })
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{Name: "chat"})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "go"})
	// What a killed worker leaves behind: its own run id, with the lease it
	// last renewed to.
	if _, err := h.kv.Set(h.ctx, agentenkit.RunLockKey(ran.ThreadID), ran.RunID, ports.SetOptions{OnlyIfNotExists: true, Expiry: 50 * time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(80 * time.Millisecond)
	h.handleNext(t)
	mustEqual(t, h.lastTerminal(ran.ThreadID)["state"], "COMPLETED", "the redelivery ran the run")
	mustEqual(t, h.queue.Len(), 0, "nothing left")
}

// A run that ended while a worker held its lock is not dropped: its settle
// is retried once the lock has cleared, so its spend is charged.
func TestSettle_ARunThatEndedUnderAHeldLockIsSettledOnceTheLockClears(t *testing.T) {
	settled := 0
	h := makeRuntime(t, scripted(step{text: "ok"}), func(c *agentenkit.AgentConfig) { c.RunLockLease = time.Second })
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{
		Name:     "chat",
		OnSettle: func(context.Context, agentenkit.RunFinishInfo) error { settled++; return nil },
	})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "go"})
	if _, err := h.kv.Set(h.ctx, agentenkit.RunLockKey(ran.ThreadID), ran.RunID, ports.SetOptions{OnlyIfNotExists: true, Expiry: 250 * time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	if _, err := chat.Stop(h.ctx, ran.ThreadID, nil); err != nil {
		t.Fatal(err)
	}
	mustEqual(t, settled, 0, "the stop could not settle: a worker seemed to hold the run")
	h.handleNext(t) // the queued job meets the held lock
	mustEqual(t, h.queue.Len(), 1, "the settle is retried, not dropped")
	mustEqual(t, h.queue.Keys()[0], "settle:"+ran.RunID, "once per run")
	mustEqual(t, h.queue.Delays()[0], time.Second, "after the lock has surely cleared")
	time.Sleep(300 * time.Millisecond)
	h.handleNext(t)
	mustEqual(t, settled, 1, "settled late")
	rec, _ := h.admin.Runs().Get(h.ctx, ran.RunID)
	if rec.SettledAt == nil {
		t.Fatal("the record says so")
	}
}

// A queued or running thread whose job the queue lost is found by the
// sweep and re-dispatched.
func TestReclaim_AThreadWithNoLockAndNoJobIsRedispatched(t *testing.T) {
	h := makeRuntime(t, scripted(step{text: "ok"}), func(c *agentenkit.AgentConfig) { c.RunLockLease = time.Second })
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{Name: "chat"})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "go", State: agentenkit.AgentRunState{"tenant": "acme"}})
	h.queue.Shift() // the queue lost the job

	report, err := h.rt.ReclaimStuckRuns(h.ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	mustEqual(t, report.Redispatched, 0, "too soon: a run just accepted has no lock and no row for a moment")
	time.Sleep(1050 * time.Millisecond)
	report, err = h.rt.ReclaimStuckRuns(h.ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	mustEqual(t, report.Redispatched, 1, "re-dispatched")
	mustEqual(t, h.queue.Len(), 1, "back on the queue")
	mustEqual(t, h.queue.Items()[0].Kind, agentenkit.JobReclaim, "as a reclaim")
	mustEqual(t, h.queue.Keys()[0], "reclaim:"+ran.RunID, "once per run")
	mustEqual(t, h.queue.Items()[0].State["tenant"], "acme", "with the run's state")
	h.drain(t)
	mustEqual(t, h.lastTerminal(ran.ThreadID)["state"], "COMPLETED", "the run finished")
}

// A settle hook that fails leaves the run unsettled; the sweep runs it again.
func TestSettle_AFailedHookLeavesTheRunUnsettledAndTheSweepRetriesIt(t *testing.T) {
	calls := 0
	h := makeRuntime(t, scripted(step{text: "ok"}))
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{
		Name: "chat",
		OnSettle: func(context.Context, agentenkit.RunFinishInfo) error {
			calls++
			if calls == 1 {
				return errors.New("ledger down")
			}
			return nil
		},
	})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "go", State: agentenkit.AgentRunState{"tenant": "acme"}})
	h.handleNext(t)
	rec, _ := h.admin.Runs().Get(h.ctx, ran.RunID)
	mustEqual(t, rec.State, agentenkit.StateFailed, "a settle failure fails the run")
	if rec.SettledAt != nil {
		t.Fatal("a failed settle must not be marked settled")
	}
	report, err := h.rt.ReclaimStuckRuns(h.ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	mustEqual(t, report.Settled, 1, "settled by the sweep")
	mustEqual(t, calls, 2, "the hook ran again")
	rec, _ = h.admin.Runs().Get(h.ctx, ran.RunID)
	if rec.SettledAt == nil {
		t.Fatal("marked settled once the hook succeeded")
	}
}

// The billing check runs again at pickup, with the run id, and can lower
// the run's caps or refuse it.
func TestBilling_ThePickupCheckCanLowerTheCapOrRefuseTheRun(t *testing.T) {
	var stages []ports.BillingStage
	h := makeRuntime(t, scripted(
		step{calls: []call{{"c1", "probe", `{}`}}},
		step{calls: []call{{"c2", "probe", `{}`}}},
		step{text: "done"},
	), func(c *agentenkit.AgentConfig) {
		c.BillingPreCheck = func(_ context.Context, check ports.BillingCheck) error {
			stages = append(stages, check.Stage)
			if check.Stage == ports.BillingAtPickup {
				if check.RunID == "" || check.Budget == nil {
					t.Errorf("pickup check without a run or a budget: %+v", check)
				}
				check.Budget.MaxSteps = 1
			}
			return nil
		}
	})
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{
		Name:  "chat",
		Tools: []agentenkit.Tool{tool("probe", func(context.Context, map[string]any) (string, error) { return "ok", nil })},
	})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "go"})
	h.handleNext(t)
	mustEqual(t, len(stages), 2, "checked at dispatch and at pickup")
	mustEqual(t, stages[0], ports.BillingAtDispatch, "first at dispatch")
	mustEqual(t, stages[1], ports.BillingAtPickup, "then at pickup")
	mustEqual(t, h.lastTerminal(ran.ThreadID)["stopReason"], "max_steps", "the lowered cap applied")
	mustEqual(t, h.model.Calls(), 1, "one step")

	refused := makeRuntime(t, scripted(step{text: "never"}), func(c *agentenkit.AgentConfig) {
		c.BillingPreCheck = func(_ context.Context, check ports.BillingCheck) error {
			if check.Stage == ports.BillingAtPickup {
				return errors.New("out of credits")
			}
			return nil
		}
	})
	var settled []agentenkit.RunFinishInfo
	chat2 := refused.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{
		Name: "chat",
		OnSettle: func(_ context.Context, info agentenkit.RunFinishInfo) error {
			settled = append(settled, info)
			return nil
		},
	})
	ran2 := refused.run(t, chat2, agentenkit.RunInput{Prompt: "go"})
	refused.handleNext(t)
	term := refused.lastTerminal(ran2.ThreadID)
	mustEqual(t, term["state"], "FAILED", "refused at pickup fails the run")
	mustEqual(t, term["error"], "out of credits", "with the reason")
	mustEqual(t, refused.model.Calls(), 0, "no model call")
	mustEqual(t, len(refused.events(ran2.ThreadID, "RUN_REFUSED")), 1, "the refusal is on the thread")
	mustEqual(t, len(settled), 1, "the run settled")
	mustEqual(t, refused.queue.Len(), 0, "not retried")
}

// A job that waited past MaxQueueWait fails with the reason instead of
// running work nobody is waiting for.
func TestOverload_AJobThatWaitedPastMaxQueueWaitFailsInsteadOfRunning(t *testing.T) {
	h := makeRuntime(t, scripted(step{text: "never"}), func(c *agentenkit.AgentConfig) { c.MaxQueueWait = 20 * time.Millisecond })
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{Name: "chat"})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "go"})
	time.Sleep(40 * time.Millisecond)
	h.handleNext(t)
	term := h.lastTerminal(ran.ThreadID)
	mustEqual(t, term["state"], "FAILED", "failed")
	if !strings.Contains(term["error"].(string), "waited") {
		t.Fatalf("error: %v", term["error"])
	}
	mustEqual(t, h.model.Calls(), 0, "never ran")
	mustEqual(t, h.queue.Len(), 0, "not retried")
}

// A job the queue gives up on fails the run it belonged to, so the thread
// does not read QUEUED for ever; a run that already moved on is left alone.
func TestDeadJob_TheHandlerFailsTheRunItBelongedTo(t *testing.T) {
	var settled []agentenkit.RunFinishInfo
	h := makeRuntime(t, scripted(step{text: "ok"}))
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{
		Name: "chat",
		OnSettle: func(_ context.Context, info agentenkit.RunFinishInfo) error {
			settled = append(settled, info)
			return nil
		},
	})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "go"})
	job, _ := h.queue.Shift()
	h.rt.Worker.HandleDeadJob(h.ctx, job, 5, errBoom)
	mustEqual(t, h.thread(t, ran.ThreadID).State, agentenkit.StateFailed, "durable")
	mustEqual(t, h.kvGet(agentenkit.StateKey(ran.ThreadID)), "FAILED", "hot")
	rec, _ := h.admin.Runs().Get(h.ctx, ran.RunID)
	mustEqual(t, rec.State, agentenkit.StateFailed, "record")
	if !strings.Contains(rec.Error, "dropped by the queue") || !strings.Contains(rec.Error, "boom") {
		t.Fatalf("reason: %q", rec.Error)
	}
	mustEqual(t, len(settled), 1, "settled")
	mustEqual(t, h.kvGet(agentenkit.RunLockKey(ran.ThreadID)), "", "lock released")

	// The thread accepts a new run, and a stale dead job for the old run
	// cannot touch it.
	next := h.run(t, chat, agentenkit.RunInput{ThreadID: ran.ThreadID, Prompt: "again"})
	h.rt.Worker.HandleDeadJob(h.ctx, job, 5, errBoom)
	mustEqual(t, h.thread(t, ran.ThreadID).State, agentenkit.StateQueued, "the new run stands")
	h.handleNext(t)
	mustEqual(t, h.lastTerminal(ran.ThreadID)["state"], "COMPLETED", "and finishes")
	_ = next
}

// A job for an agent this process does not have is an error the queue
// keeps and retries, never a silent success that deletes the job.
func TestDispatch_AnUnknownAgentIsAnErrorNotASilentSuccess(t *testing.T) {
	bare := makeRuntime(t, scripted(step{text: "ok"}))
	bare.rt.CreateGenerateTextAgent(agentenkit.GenerateTextAgentSpec{Name: "one"})
	res, err := bare.rt.Worker.HandleJob(bare.ctx, agentenkit.RunJob{ThreadID: "x", Agent: "nope"})
	if !errors.Is(err, agentenkit.ErrUnknownAgent) {
		t.Fatalf("want ErrUnknownAgent, got %v", err)
	}
	mustEqual(t, res.Accepted, false, "refused")
	mustEqual(t, res.Reason, "unknown-agent", "reason")
	if err := bare.rt.Worker.Handler()(bare.ctx, agentenkit.RunJob{ThreadID: "x", Agent: "nope"}); err == nil {
		t.Fatal("the queue handler must see the error too")
	}
}

// Deleting a parked thread takes its operational history with it (§3.2):
// no run, step or thread row is left in the admin store, and the expiry that
// later finds no thread does nothing. The same case runs in the TS package
// (test/admin-store.test.ts).
func TestDelete_ADeletedThreadLeavesNothingInTheAdminStore(t *testing.T) {
	h := hitlSetup(t)
	ran := h.run(t, h.chat, agentenkit.RunInput{Prompt: "delete"})
	h.handleNext(t)
	res, err := h.rt.DeleteThread(h.ctx, ran.ThreadID, nil)
	if err != nil || !res.Accepted {
		t.Fatalf("delete: %v %+v", err, res)
	}
	if rec, _ := h.admin.Runs().Get(h.ctx, ran.RunID); rec != nil {
		t.Fatalf("the run record is gone with the thread: %+v", rec)
	}
	if th, _ := h.admin.Threads().Get(h.ctx, ran.ThreadID); th != nil {
		t.Fatal("the admin thread row is gone too")
	}
	if steps, _ := h.admin.Steps().ListByThread(h.ctx, ran.ThreadID); len(steps) != 0 {
		t.Fatalf("and its steps: %d left", len(steps))
	}
	h.drain(t) // the expiry finds no thread
	mustEqual(t, h.model.Calls(), 1, "nothing ran after the delete")
}

// A queued thread cannot be deleted from under its job: stop it first.
func TestDelete_AQueuedThreadIsRefused(t *testing.T) {
	h := makeRuntime(t, scripted(step{text: "ok"}))
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{Name: "chat"})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "go"})
	res, err := h.rt.DeleteThread(h.ctx, ran.ThreadID, nil)
	if err != nil {
		t.Fatal(err)
	}
	mustEqual(t, res.Accepted, false, "refused while queued")
}

// A worker cut off mid-step hands the run back at once: no attempt is
// spent, and the thread reads QUEUED until a worker has it again.
func TestShutdown_ACancelledWorkerRequeuesTheRunWithoutSpendingAnAttempt(t *testing.T) {
	h := makeRuntime(t, scripted(step{text: "slow", delay: 500 * time.Millisecond}, step{text: "done"}),
		func(c *agentenkit.AgentConfig) { c.RunMaxAttempts = 1 })
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{Name: "chat"})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "go"})
	job, _ := h.queue.Shift()
	ctx, cancel := context.WithCancel(h.ctx)
	done := make(chan error, 1)
	go func() {
		_, err := h.rt.Worker.HandleJob(ctx, job)
		done <- err
	}()
	time.Sleep(60 * time.Millisecond)
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("an interrupted worker hands the job back cleanly: %v", err)
	}
	mustEqual(t, h.queue.Len(), 1, "requeued")
	mustEqual(t, h.queue.Items()[0].Kind, agentenkit.JobRetry, "as a retry")
	mustEqual(t, h.queue.Delays()[0], time.Duration(0), "at once")
	mustEqual(t, h.kvGet(agentenkit.AttemptsKey(ran.RunID)), "", "no attempt spent")
	mustEqual(t, h.thread(t, ran.ThreadID).State, agentenkit.StateQueued, "waiting for a worker again")
	h.handleNext(t) // with RunMaxAttempts 1, a counted attempt would have failed it here
	mustEqual(t, h.lastTerminal(ran.ThreadID)["state"], "COMPLETED", "finished on the next worker")
}

// A queued thread is active from the moment it was accepted: a client that
// hydrates before pickup sees the run and its acceptance.
func TestSnapshot_AQueuedThreadIsActiveFromItsAcceptance(t *testing.T) {
	h := makeRuntime(t, scripted(step{text: "ok"}))
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{Name: "chat"})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "go"})
	snap, err := h.rt.GetThreadSnapshot(h.ctx, ran.ThreadID, nil)
	if err != nil || snap == nil {
		t.Fatalf("snapshot: %v", err)
	}
	mustEqual(t, snap.Thread.State, agentenkit.StateQueued, "state")
	mustEqual(t, len(snap.Runs), 1, "the run is on the snapshot")
	mustEqual(t, snap.Runs[0].State, agentenkit.StateQueued, "as queued")
	// Not picked up yet: no segment has started, so there is no stream.
	if snap.Stream != nil {
		t.Fatalf("a queued run has no stream yet: %+v", snap.Stream)
	}
}

// The queue-wait percentiles include a run still waiting, with the time it
// has waited so far, so the number rises while a backlog grows.
func TestAdmin_QueueWaitIncludesRunsStillWaiting(t *testing.T) {
	past := time.Now().Add(-5 * time.Second)
	stats := agentenkit.Summarise([]agentenkit.RunRecord{
		{ID: "waiting", State: agentenkit.StateQueued, EnqueuedAt: &past},
		{ID: "done", State: agentenkit.StateCompleted, QueuedMs: agentenkit.Ptr(int64(100))},
	})
	mustEqual(t, stats.Waiting, 1, "one waiting")
	if stats.Queued == nil || stats.Queued.Max < 4900 {
		t.Fatalf("the waiting run's wait so far is in the percentiles: %+v", stats.Queued)
	}
}

// Retry counters belong to the run: a new run on the thread starts with a
// full budget by construction.
func TestRetry_CountersBelongToTheRunNotTheThread(t *testing.T) {
	h := makeRuntime(t, scripted(step{err: errBoom}, step{text: "ok"}), func(c *agentenkit.AgentConfig) { c.RunRetryBackoff = 0 })
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{Name: "chat"})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "go"})
	h.handleNext(t)
	mustEqual(t, h.kvGet(agentenkit.AttemptsKey(ran.RunID)), "1", "counted against the run")
	mustEqual(t, h.kvGet(agentenkit.AttemptsKey(ran.ThreadID)), "", "never against the thread")
	h.handleNext(t)
	mustEqual(t, h.lastTerminal(ran.ThreadID)["state"], "COMPLETED", "recovered")
	mustEqual(t, h.kvGet(agentenkit.AttemptsKey(ran.RunID)), "", "cleared")
}

// A counter written with an expiry ages out on its own.
func TestKv_ACounterWithAnExpiryAgesOut(t *testing.T) {
	h := makeRuntime(t, scripted(step{text: "ok"}))
	n, err := h.kv.IncrWithExpiry(h.ctx, "agent:attempts:r1", 20*time.Millisecond)
	if err != nil || n != 1 {
		t.Fatalf("incr: %d %v", n, err)
	}
	n, _ = h.kv.IncrWithExpiry(h.ctx, "agent:attempts:r1", time.Hour)
	mustEqual(t, n, int64(2), "a live counter keeps its expiry and advances")
	time.Sleep(30 * time.Millisecond)
	mustEqual(t, h.kvGet("agent:attempts:r1"), "", "gone")
	ok, _ := h.kv.SetIfValue(h.ctx, "lock", "a", "a", time.Hour)
	mustEqual(t, ok, false, "cannot renew a lock nobody holds")
	_, _ = h.kv.Set(h.ctx, "lock", "a", ports.SetOptions{})
	ok, _ = h.kv.SetIfValue(h.ctx, "lock", "b", "b", time.Hour)
	mustEqual(t, ok, false, "cannot renew another worker's lock")
	ok, _ = h.kv.SetIfValue(h.ctx, "lock", "a", "a", time.Hour)
	mustEqual(t, ok, true, "the holder renews")
	ok, _ = h.kv.DelIfValue(h.ctx, "lock", "b")
	mustEqual(t, ok, false, "cannot free another worker's lock")
	ok, _ = h.kv.DelIfValue(h.ctx, "lock", "a")
	mustEqual(t, ok, true, "the holder frees it")
}
