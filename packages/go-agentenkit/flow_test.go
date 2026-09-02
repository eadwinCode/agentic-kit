package agentenkit_test

import (
	"context"
	"errors"
	"testing"
	"time"

	agentenkit "github.com/eadwinCode/agentic-kit/packages/go-agentenkit"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/core"
)

func TestRun_CreatesThreadPersistsMarksRunningEnqueues(t *testing.T) {
	h := makeRuntime(t, scripted(step{text: "ok"}))
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{Name: "chat", Model: "gpt-4o"})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "hello"})

	mustEqual(t, ran.State, agentenkit.StateRunning, "result state")
	if ran.RunID == "" {
		t.Fatal("no run id")
	}
	mustEqual(t, h.thread(t, ran.ThreadID).State, agentenkit.StateRunning, "durable state")
	mustEqual(t, h.kvGet(agentenkit.StateKey(ran.ThreadID)), "RUNNING", "hot state")
	mustEqual(t, h.kvGet(agentenkit.RunIDKey(ran.ThreadID)), ran.RunID, "run id key")
	mustStrings(t, h.roles(ran.ThreadID), []string{"user"}, "roles")
	mustEqual(t, string(h.storage.MessageRows(ran.ThreadID)[0].Content), `"hello"`, "user content")

	jobs := h.queue.Items()
	mustEqual(t, len(jobs), 1, "jobs")
	mustEqual(t, jobs[0].Agent, "chat", "job agent")
	mustEqual(t, jobs[0].RunID, ran.RunID, "job run id")
	mustEqual(t, jobs[0].Model, "gpt-4o", "job model")
	if jobs[0].EnqueuedAt == 0 {
		t.Fatal("enqueuedAt missing")
	}
	// The user's turn goes on the bus before the run starts (§2.2)
	evs := h.events(ran.ThreadID, "")
	mustEqual(t, evs[0].Type, "MESSAGE_APPENDED", "first event")
	mustEqual(t, evs[1].Type, "STATE_CHANGE", "second event")
	rec, _ := h.admin.Runs().Get(h.ctx, ran.RunID)
	mustEqual(t, rec.State, agentenkit.StateRunning, "run record")
	mustEqual(t, rec.Prompt, "hello", "recorded prompt")
}

func TestRun_RejectsASecondRunWhileOneIsActive(t *testing.T) {
	h := makeRuntime(t, scripted(step{text: "ok"}))
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{Name: "chat"})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "one"})
	second, err := chat.Run(h.ctx, agentenkit.RunInput{ThreadID: ran.ThreadID, Prompt: "two"})
	if err != nil {
		t.Fatal(err)
	}
	if second.Accepted {
		t.Fatal("second run must be refused")
	}
	mustEqual(t, second.Error, "Thread has an active run", "error")
}

func TestRun_RejectsWhenTheBillingPreCheckFails(t *testing.T) {
	h := makeRuntime(t, scripted(step{text: "ok"}), func(c *agentenkit.AgentConfig) {
		c.BillingPreCheck = func(ctx context.Context, check agentenkit.BillingCheck) error {
			// The check can tell every client why, before the refusal lands
			_, _ = check.PublishEvent(ctx, "CREDIT_LIMIT", map[string]any{"org": check.State["orgId"]}, false)
			return errors.New("no credits")
		}
	})
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{Name: "chat"})
	res, err := chat.Run(h.ctx, agentenkit.RunInput{Prompt: "x", State: agentenkit.AgentRunState{"orgId": "acme"}})
	if err != nil {
		t.Fatal(err)
	}
	if res.Accepted {
		t.Fatal("must be refused")
	}
	mustEqual(t, res.Error, "no credits", "error")
	mustEqual(t, h.queue.Len(), 0, "nothing queued")
	// Both the app's own event and the platform's refusal are on the log
	limit := h.events(res.ThreadID, "CREDIT_LIMIT")
	mustEqual(t, len(limit), 1, "the check published")
	mustEqual(t, payload(limit[0])["org"], "acme", "with the run state")
	refused := h.events(res.ThreadID, "RUN_REFUSED")
	mustEqual(t, len(refused), 1, "RUN_REFUSED")
	mustEqual(t, payload(refused[0])["error"], "no credits", "reason")
	if refused[0].Seq == 0 {
		t.Fatal("the refusal must be durable")
	}
}

func TestRun_ThreadsTokenBudgetThroughTheJob(t *testing.T) {
	h := makeRuntime(t, scripted(step{text: "ok"}))
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{Name: "chat"})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "x", TokenBudget: 500})
	mustEqual(t, h.queue.Items()[0].TokenBudget, 500, "job budget")
	rec, _ := h.admin.Runs().Get(h.ctx, ran.RunID)
	mustEqual(t, *rec.TokenBudget, 500, "recorded budget")
}

func TestRun_ListsThreadsMostRecentFirst(t *testing.T) {
	h := makeRuntime(t, scripted(step{text: "ok"}))
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{Name: "chat"})
	a := h.run(t, chat, agentenkit.RunInput{Prompt: "a"})
	time.Sleep(2 * time.Millisecond)
	b := h.run(t, chat, agentenkit.RunInput{Prompt: "b"})
	threads, err := h.rt.ListThreads(h.ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	mustEqual(t, len(threads), 2, "threads")
	mustEqual(t, threads[0].ID, b.ThreadID, "newest first")
	mustEqual(t, threads[1].ID, a.ThreadID, "oldest last")
}

func TestStop_WritesCancelledToBothHomes(t *testing.T) {
	h := makeRuntime(t, scripted(step{text: "ok"}))
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{Name: "chat"})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "x"})
	res, err := chat.Stop(h.ctx, ran.ThreadID, nil)
	if err != nil || !res.Accepted {
		t.Fatalf("stop: %v %+v", err, res)
	}
	mustEqual(t, h.kvGet(agentenkit.StateKey(ran.ThreadID)), "CANCELLED", "hot")
	mustEqual(t, h.thread(t, ran.ThreadID).State, agentenkit.StateCancelled, "durable")
	mustStrings(t, h.states(ran.ThreadID), []string{"RUNNING", "CANCELLED"}, "states")
	// The queued job is now a no-op
	h.handleNext(t)
	mustEqual(t, h.model.Calls(), 0, "model never called")
}

func TestStop_RejectsAnIdleThread(t *testing.T) {
	h := makeRuntime(t, scripted(step{text: "ok"}))
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{Name: "chat"})
	th, _ := h.storage.Threads().Create(h.ctx, agentenkit.ThreadInit{}, agentenkit.StorageContext{})
	res, err := chat.Stop(h.ctx, th.ID, nil)
	if err != nil {
		t.Fatal(err)
	}
	if res.Accepted {
		t.Fatal("must be refused")
	}
	mustEqual(t, res.Error, "Cannot stop thread in state IDLE", "error")
}

func TestRespond_RejectsUnknownToolCallAndNonWaitingThread(t *testing.T) {
	h := hitlSetup(t)
	ran := h.run(t, h.chat, agentenkit.RunInput{Prompt: "delete"})
	// Not waiting yet
	res, err := h.rt.HITL.Respond(h.ctx, agentenkit.RespondInput{ThreadID: ran.ThreadID, ToolCallID: "c1", Approved: true})
	if err != nil || res.Delivered {
		t.Fatalf("expected rejection, got %+v %v", res, err)
	}
	h.handleNext(t)
	res, err = h.rt.HITL.Respond(h.ctx, agentenkit.RespondInput{ThreadID: ran.ThreadID, ToolCallID: "nope", Approved: true})
	if err != nil || res.Delivered {
		t.Fatalf("expected rejection, got %+v %v", res, err)
	}
	mustEqual(t, res.Error, "No matching pending input request", "error")
}

func TestReclaim_DoesNothingForAYoungRequest(t *testing.T) {
	h := hitlSetup(t)
	ran := h.run(t, h.chat, agentenkit.RunInput{Prompt: "delete"})
	h.handleNext(t)
	before := h.queue.Len()
	ok, err := h.rt.HITL.ReclaimIfOrphaned(h.ctx, ran.ThreadID, nil)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("must not reclaim a young request")
	}
	mustEqual(t, h.queue.Len(), before, "nothing enqueued")
}

func TestReclaim_RedispatchesATrueOrphan(t *testing.T) {
	h := hitlSetup(t, func(c *agentenkit.AgentConfig) { c.HITLTTL = 5 * time.Millisecond; c.ReclaimGrace = 0 })
	ran := h.run(t, h.chat, agentenkit.RunInput{Prompt: "delete"})
	h.handleNext(t)
	h.queue.Shift() // drop the park's own expiry to prove reclamation alone works
	time.Sleep(20 * time.Millisecond)
	ok, err := h.rt.HITL.ReclaimIfOrphaned(h.ctx, ran.ThreadID, nil)
	if err != nil || !ok {
		t.Fatalf("reclaim: %v %v", ok, err)
	}
	job := h.queue.Items()[0]
	mustEqual(t, job.RunID, ran.RunID, "reuses the run id")
	mustEqual(t, job.Agent, "chat", "rebuilt from the ticket")
	// Nothing healed inline: the thread is still parked until the worker runs
	mustEqual(t, h.thread(t, ran.ThreadID).State, agentenkit.StateWaitingForInput, "state")
	h.handleNext(t)
	mustEqual(t, h.lastTerminal(ran.ThreadID)["state"], "COMPLETED", "resolved by the engine")
	mustEqual(t, len(*h.executed), 0, "expired: the tool never ran")
}

func TestDeleteThread_DeletesEverythingThatFollowsIt(t *testing.T) {
	h := makeRuntime(t, scripted(step{text: "ok"}))
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{Name: "chat"})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "x"})
	h.handleNext(t)
	res, err := h.rt.DeleteThread(h.ctx, ran.ThreadID, nil)
	if err != nil || !res.Accepted {
		t.Fatalf("delete: %v %+v", err, res)
	}
	th, _ := h.storage.Threads().Get(h.ctx, ran.ThreadID, agentenkit.StorageContext{})
	if th != nil {
		t.Fatal("thread still there")
	}
	mustEqual(t, len(h.storage.MessageRows(ran.ThreadID)), 0, "messages")
	evs, _ := h.storage.Events().ListSince(h.ctx, ran.ThreadID, -1, agentenkit.StorageContext{})
	mustEqual(t, len(evs), 0, "events")
	total, _ := h.storage.Usage().Total(h.ctx, ran.ThreadID, agentenkit.StorageContext{})
	mustEqual(t, total.TotalTokens, 0, "usage")
	for _, key := range []string{agentenkit.StateKey(ran.ThreadID), agentenkit.RunIDKey(ran.ThreadID), agentenkit.RunLockKey(ran.ThreadID)} {
		mustEqual(t, h.kvGet(key), "", "kv "+key)
	}
	notices := h.events(ran.ThreadID, "THREAD_DELETED")
	mustEqual(t, len(notices), 1, "THREAD_DELETED notice")
	mustEqual(t, notices[0].Seq, int64(0), "bus-only")
}

func TestDeleteThread_RefusesAnActiveRunButDeletesAParkedOne(t *testing.T) {
	h := hitlSetup(t)
	ran := h.run(t, h.chat, agentenkit.RunInput{Prompt: "delete"})
	res, _ := h.rt.DeleteThread(h.ctx, ran.ThreadID, nil)
	if res.Accepted {
		t.Fatal("must refuse a running thread")
	}
	h.handleNext(t) // parks
	res, _ = h.rt.DeleteThread(h.ctx, ran.ThreadID, nil)
	if !res.Accepted {
		t.Fatalf("parked thread must delete: %s", res.Error)
	}
	// A late resume dispatch after deletion is a no-op: no resurrection
	h.drain(t)
	th, _ := h.storage.Threads().Get(h.ctx, ran.ThreadID, agentenkit.StorageContext{})
	if th != nil {
		t.Fatal("thread resurrected")
	}
	unknown, _ := h.rt.DeleteThread(h.ctx, "nope", nil)
	mustEqual(t, unknown.Error, "Thread not found", "unknown thread")
}

func TestEvents_ReplaysSinceACursorAndSubscribesLive(t *testing.T) {
	h := makeRuntime(t, scripted(step{text: "ok"}))
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{Name: "chat"})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "x"})
	all, err := h.rt.Events.Since(h.ctx, ran.ThreadID, -1, nil)
	if err != nil {
		t.Fatal(err)
	}
	mustEqual(t, len(all), 2, "events so far")
	later, _ := h.rt.Events.Since(h.ctx, ran.ThreadID, all[0].Seq, nil)
	mustEqual(t, len(later), 1, "after cursor")

	var live []string
	unsub, err := h.rt.Events.Subscribe(h.ctx, ran.ThreadID, func(e agentenkit.AgentEvent) { live = append(live, e.Type) })
	if err != nil {
		t.Fatal(err)
	}
	h.handleNext(t)
	if len(live) == 0 || live[len(live)-1] != "STATE_CHANGE" {
		t.Fatalf("live tail missing, got %v", live)
	}
	_ = unsub()
}

func TestContextBudget_CapsAtTheCeilingButKeepsSmallerWindows(t *testing.T) {
	h := makeRuntime(t, scripted(step{text: "ok"}))
	deps := h.rt.Ports(nil)
	// The resolver declares 128k for everything, which is below the ceiling
	mustEqual(t, core.ContextBudget(deps, "gpt-4o"), 128_000, "declared window")
	deps.ResolveModel = func(string) (agentenkit.ResolvedModel, error) {
		return agentenkit.ResolvedModel{}, errors.New("unknown")
	}
	mustEqual(t, core.ContextBudget(deps, "gemini-1.5-pro"), 265_000, "ceiling")
	mustEqual(t, core.ContextBudget(deps, "gpt-4o-mini"), 128_000, "table")
	mustEqual(t, core.ContextBudget(deps, "mystery"), 265_000, "default")
	deps.Config.NativeWindows = map[string]int{"mystery": 32_000}
	mustEqual(t, core.ContextBudget(deps, "mystery"), 32_000, "config table")
	_ = context.Background
}
