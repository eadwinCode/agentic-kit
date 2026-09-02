package agentenkit_test

import (
	"context"
	"errors"
	"testing"
	"time"

	agentenkit "github.com/eadwinCode/agentic-kit/packages/go-agentenkit"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/core"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

// delayRefusingQueue cannot schedule delayed jobs, like a queue whose delay
// support is broken.
type delayRefusingQueue struct{ ports.Queue }

func (q delayRefusingQueue) Enqueue(ctx context.Context, job ports.RunJob, opts *ports.EnqueueOptions) error {
	if opts != nil && opts.Delay > 0 {
		return errors.New("no delays here")
	}
	return q.Queue.Enqueue(ctx, job, opts)
}

func TestDispatch_AQueueThatCannotScheduleTheExpiryStillParks(t *testing.T) {
	h := hitlSetup(t)
	deps := h.rt.Ports(nil)
	deps.Queue = delayRefusingQueue{h.queue}
	ran := h.run(t, h.chat, agentenkit.RunInput{Prompt: "delete"})
	job, _ := h.queue.Shift()
	outcome, err := core.Execute(h.ctx, deps, h.chat.Agent(), agentenkit.ExecuteInput{ThreadID: job.ThreadID, RunID: job.RunID, Model: job.Model})
	if err != nil {
		t.Fatalf("the failure must never reach the run: %v", err)
	}
	mustEqual(t, outcome, agentenkit.OutcomeExecuted, "outcome")
	mustEqual(t, h.thread(t, ran.ThreadID).State, agentenkit.StateWaitingForInput, "parked")
	mustEqual(t, h.queue.Len(), 0, "no expiry scheduled")
}

func TestDispatch_TheFailureRetryCarriesTheRunID(t *testing.T) {
	h := makeRuntime(t, scripted(step{err: errBoom}, step{text: "recovered"}))
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{Name: "chat"})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "hi", State: agentenkit.AgentRunState{"orgId": "acme"}})
	h.handleNext(t)
	mustEqual(t, h.queue.Len(), 1, "retry queued")
	retry := h.queue.Items()[0]
	mustEqual(t, retry.RunID, ran.RunID, "the retry is the same run")
	mustEqual(t, retry.State["orgId"], "acme", "the retry keeps the state")
	mustEqual(t, h.kvGet(agentenkit.StateKey(ran.ThreadID)), "RUNNING", "still running")
	h.handleNext(t)
	mustEqual(t, h.lastTerminal(ran.ThreadID)["state"], "COMPLETED", "recovered")
	mustEqual(t, h.kvGet("agent:attempts:"+ran.ThreadID), "", "attempt counter reset")
}

func TestDispatch_ExhaustedAttemptsFinalizeFailed(t *testing.T) {
	h := makeRuntime(t, scripted(step{err: errBoom}), func(c *agentenkit.AgentConfig) { c.RunMaxAttempts = 2 })
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{Name: "chat"})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "hi"})
	h.handleNext(t)
	mustEqual(t, h.queue.Len(), 1, "one retry")
	h.handleNext(t)
	mustEqual(t, h.queue.Len(), 0, "no more retries")
	mustEqual(t, h.thread(t, ran.ThreadID).State, agentenkit.StateFailed, "FAILED")
	mustEqual(t, h.model.Calls(), 2, "two attempts")
	// The thread accepts a new run afterwards
	h.run(t, chat, agentenkit.RunInput{ThreadID: ran.ThreadID, Prompt: "again"})
}

func TestDispatch_AJobBlockedByAnOlderRunsLockIsRedriven(t *testing.T) {
	h := makeRuntime(t, scripted(step{text: "ok"}))
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{Name: "chat"})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "hi"})
	// An older run still holds the lock
	_, _ = h.kv.Set(h.ctx, agentenkit.RunLockKey(ran.ThreadID), "older-run", agentenkit.SetOptions{})
	h.handleNext(t)
	mustEqual(t, h.model.Calls(), 0, "nothing ran")
	mustEqual(t, h.queue.Len(), 1, "redriven")
	mustEqual(t, h.queue.Delays()[0], 2*time.Second, "after the redrive delay")
	mustEqual(t, h.queue.Items()[0].RunID, ran.RunID, "same run")
	// The lock clears and the redriven job runs
	_ = h.kv.Del(h.ctx, agentenkit.RunLockKey(ran.ThreadID))
	h.handleNext(t)
	mustEqual(t, h.lastTerminal(ran.ThreadID)["state"], "COMPLETED", "state")
}

func TestDispatch_ADuplicateOfTheRunningJobIsDropped(t *testing.T) {
	h := makeRuntime(t, scripted(step{text: "ok"}))
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{Name: "chat"})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "hi"})
	_, _ = h.kv.Set(h.ctx, agentenkit.RunLockKey(ran.ThreadID), ran.RunID, agentenkit.SetOptions{})
	h.handleNext(t)
	mustEqual(t, h.queue.Len(), 0, "own duplicate: dropped")
}

func TestDispatch_ALockThatNeverClearsFailsTheRun(t *testing.T) {
	h := makeRuntime(t, scripted(step{text: "ok"}), func(c *agentenkit.AgentConfig) { c.RunMaxAttempts = 1 })
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{Name: "chat"})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "hi"})
	_, _ = h.kv.Set(h.ctx, agentenkit.RunLockKey(ran.ThreadID), "older-run", agentenkit.SetOptions{})
	h.handleNext(t) // redrive 1
	h.handleNext(t) // attempts exhausted
	mustEqual(t, h.thread(t, ran.ThreadID).State, agentenkit.StateFailed, "FAILED")
	rec, _ := h.admin.Runs().Get(h.ctx, ran.RunID)
	mustEqual(t, rec.Error, "the run lock never cleared", "reason")
}

func TestDispatch_AStaleJobIsANoOp(t *testing.T) {
	h := makeRuntime(t, scripted(step{text: "ok"}))
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{Name: "chat"})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "hi"})
	job, _ := h.queue.Shift()
	job.RunID = "replaced"
	outcome, err := chat.Execute(h.ctx, agentenkit.ExecuteInput{ThreadID: job.ThreadID, RunID: job.RunID, Model: job.Model})
	if err != nil {
		t.Fatal(err)
	}
	mustEqual(t, outcome, agentenkit.OutcomeStale, "stale")
	mustEqual(t, h.model.Calls(), 0, "nothing ran")
	mustEqual(t, h.thread(t, ran.ThreadID).State, agentenkit.StateRunning, "state untouched")
}

func TestDispatch_UnknownAgentIsRefusedAndTheDefaultIsTheFirstStreamHandle(t *testing.T) {
	h := makeRuntime(t, scripted(step{text: "ok"}))
	one := h.rt.CreateGenerateTextAgent(agentenkit.GenerateTextAgentSpec{Name: "one"})
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{Name: "chat"})
	_ = one
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "hi"})
	job, _ := h.queue.Shift()
	job.Agent = ""
	res, err := h.rt.Worker.HandleJob(h.ctx, job)
	if err != nil || !res.Accepted {
		t.Fatalf("default handle: %+v %v", res, err)
	}
	mustEqual(t, h.lastTerminal(ran.ThreadID)["state"], "COMPLETED", "ran on the default")
	// An unknown name falls back to the default too, like the TypeScript package
	res, _ = h.rt.Worker.HandleJob(h.ctx, agentenkit.RunJob{ThreadID: "x", Agent: "nope"})
	if !res.Accepted {
		t.Fatal("with a default handle registered, an unknown agent still dispatches")
	}
	mustEqual(t, h.rt.GetAgent("chat").Name(), "chat", "registry")
	if h.rt.GetAgent("nope") != nil {
		t.Fatal("unknown agent must be nil")
	}
	// Without a stream-text handle there is no default, and the job is refused
	bare := makeRuntime(t, scripted(step{text: "ok"}))
	bare.rt.CreateGenerateTextAgent(agentenkit.GenerateTextAgentSpec{Name: "one"})
	res, _ = bare.rt.Worker.HandleJob(bare.ctx, agentenkit.RunJob{ThreadID: "x", Agent: "nope"})
	mustEqual(t, res.Reason, "unknown-agent", "unknown")
}
