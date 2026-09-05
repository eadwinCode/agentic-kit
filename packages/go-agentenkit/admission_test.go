package agentenkit_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	agentenkit "github.com/eadwinCode/agentic-kit/packages/go-agentenkit"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/core"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

type admissionQueue func(context.Context, ports.RunJob, *ports.EnqueueOptions) error

func (q admissionQueue) Enqueue(ctx context.Context, job ports.RunJob, opts *ports.EnqueueOptions) error {
	return q(ctx, job, opts)
}

func TestAdmission_ConcurrentSendsOnlyPersistAndDispatchOneTurn(t *testing.T) {
	for _, cached := range []bool{false, true} {
		name := "missing-cache"
		if cached {
			name = "completed-cache"
		}
		t.Run(name, func(t *testing.T) {
			var arrivals atomic.Int32
			gate := make(chan struct{})
			h := makeRuntime(t, scripted(step{text: "done"}), func(c *agentenkit.AgentConfig) {
				c.BillingPreCheck = func(context.Context, ports.BillingCheck) error {
					if arrivals.Add(1) == 2 {
						close(gate)
					}
					<-gate
					return nil
				}
			})
			chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{Name: "chat"})
			thread, err := h.storage.Threads().Create(h.ctx, ports.ThreadInit{Model: "gpt-4o"}, ports.StorageContext{})
			if err != nil {
				t.Fatal(err)
			}
			if cached {
				if err := h.storage.Threads().SetState(h.ctx, thread.ID, ports.StateCompleted, ports.StorageContext{}); err != nil {
					t.Fatal(err)
				}
				if _, err := h.kv.Set(h.ctx, core.StateKey(thread.ID), "COMPLETED", ports.SetOptions{}); err != nil {
					t.Fatal(err)
				}
			}
			type answer struct {
				result ports.RunResult
				err    error
			}
			answers := make(chan answer, 2)
			for _, prompt := range []string{"a", "b"} {
				go func(prompt string) {
					res, err := chat.Run(h.ctx, ports.RunInput{ThreadID: thread.ID, Prompt: prompt})
					answers <- answer{res, err}
				}(prompt)
			}
			accepted := 0
			for range 2 {
				a := <-answers
				if a.err != nil {
					t.Fatal(a.err)
				}
				if a.result.Accepted {
					accepted++
				}
			}
			mustEqual(t, accepted, 1, "one accepted send")
			mustEqual(t, len(h.storage.MessageRows(thread.ID)), 1, "one user message")
			mustEqual(t, h.queue.Len(), 1, "one dispatch")
			runs, err := h.admin.Runs().ListByThread(h.ctx, thread.ID)
			if err != nil {
				t.Fatal(err)
			}
			mustEqual(t, len(runs), 1, "one run record")
		})
	}
}

func TestAdmission_QueueFailureClosesTheRunAndAllowsAnotherSend(t *testing.T) {
	h := makeRuntime(t, scripted(step{text: "done"}))
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{Name: "chat"})
	deps := h.rt.Ports(nil)
	deps.Queue = admissionQueue(func(ctx context.Context, job ports.RunJob, opts *ports.EnqueueOptions) error {
		// The backend accepted it, but its response was lost. Late delivery must
		// still observe the failed run's durable terminal state and do nothing.
		if err := h.queue.Enqueue(ctx, job, opts); err != nil {
			return err
		}
		return errors.New("queue unavailable")
	})
	failed, err := core.Run(h.ctx, deps, chat.Agent(), ports.RunInput{Prompt: "first"})
	if err == nil || failed.Accepted {
		t.Fatalf("dispatch: %+v %v", failed, err)
	}
	mustEqual(t, h.thread(t, failed.ThreadID).State, ports.StateFailed, "thread failed")
	mustEqual(t, h.kvGet(core.StateKey(failed.ThreadID)), "FAILED", "hot state")
	record, err := h.admin.Runs().Get(h.ctx, failed.RunID)
	if err != nil {
		t.Fatal(err)
	}
	mustEqual(t, record.State, ports.StateFailed, "run failed")
	mustEqual(t, record.Error, "queue unavailable", "failure reason")
	if record.EndedAt == nil || record.DurationMs == nil {
		t.Fatal("missing failure timing")
	}
	h.handleNext(t)
	mustEqual(t, h.model.Calls(), 0, "late delivery did not execute")
	h.run(t, chat, ports.RunInput{ThreadID: failed.ThreadID, Prompt: "again"})
	h.handleNext(t)
	mustEqual(t, h.thread(t, failed.ThreadID).State, ports.StateCompleted, "next run succeeds")
}

func TestAdmission_LateQueueFailureCannotFailANewerRun(t *testing.T) {
	h := makeRuntime(t, scripted(step{text: "done"}))
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{Name: "chat"})
	deps := h.rt.Ports(nil)
	ready, release := make(chan ports.RunJob, 1), make(chan struct{})
	deps.Queue = admissionQueue(func(_ context.Context, job ports.RunJob, _ *ports.EnqueueOptions) error {
		ready <- job
		<-release
		return errors.New("late failure")
	})
	done := make(chan error, 1)
	go func() { _, err := core.Run(h.ctx, deps, chat.Agent(), ports.RunInput{Prompt: "first"}); done <- err }()
	first := <-ready
	if _, err := chat.Stop(h.ctx, first.ThreadID, nil); err != nil {
		t.Fatal(err)
	}
	second := h.run(t, chat, ports.RunInput{ThreadID: first.ThreadID, Prompt: "second"})
	close(release)
	if err := <-done; err == nil {
		t.Fatal("expected queue failure")
	}
	mustEqual(t, h.thread(t, first.ThreadID).State, ports.StateRunning, "new run remains active")
	prior, _ := h.admin.Runs().Get(h.ctx, first.RunID)
	next, _ := h.admin.Runs().Get(h.ctx, second.RunID)
	mustEqual(t, prior.State, ports.StateCancelled, "stop retained")
	mustEqual(t, next.State, ports.StateRunning, "new record retained")
}
