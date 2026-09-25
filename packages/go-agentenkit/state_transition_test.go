package agentenkit_test

import (
	"context"
	"errors"
	"testing"
	"time"

	agentenkit "github.com/eadwinCode/agentic-kit/packages/go-agentenkit"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/adapters/memory"
	pgstorage "github.com/eadwinCode/agentic-kit/packages/go-agentenkit/adapters/postgres"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/core"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

// Workstream C: every run state change is one compare-and-set on the state
// AND the run that owns the thread. The same cases run in the TS package
// (test/state-transition.test.ts).

// transitionContract is what every Storage adapter's Transition must do.
func transitionContract(t *testing.T, s ports.Storage) {
	t.Helper()
	ctx := context.Background()
	sc := ports.StorageContext{}
	th, err := s.Threads().Create(ctx, ports.ThreadInit{}, sc)
	if err != nil {
		t.Fatal(err)
	}
	step := func(name string, tr ports.ThreadTransition, want bool, state ports.ExecutionState) {
		t.Helper()
		got, err := s.Threads().Transition(ctx, th.ID, tr, sc)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if got != want {
			t.Fatalf("%s: won=%v, want %v", name, got, want)
		}
		now, _ := s.Threads().Get(ctx, th.ID, sc)
		if now.State != state {
			t.Fatalf("%s: state %s, want %s", name, now.State, state)
		}
	}
	Q, R, W, C := ports.StateQueued, ports.StateRunning, ports.StateWaitingForInput, ports.StateCancelled
	step("admission records the run", ports.ThreadTransition{From: []ports.ExecutionState{ports.StateIdle}, To: Q, NewRunID: "r1"}, true, Q)
	step("wrong state loses", ports.ThreadTransition{From: []ports.ExecutionState{R}, To: ports.StateCompleted, RunID: "r1"}, false, Q)
	step("another run loses", ports.ThreadTransition{From: []ports.ExecutionState{Q}, To: R, RunID: "r2"}, false, Q)
	step("its own run wins", ports.ThreadTransition{From: []ports.ExecutionState{Q}, To: R, RunID: "r1"}, true, R)
	step("any of several states", ports.ThreadTransition{From: []ports.ExecutionState{Q, R, W}, To: C, RunID: "r1"}, true, C)
	step("a new run is admitted", ports.ThreadTransition{From: []ports.ExecutionState{C}, To: Q, NewRunID: "r2"}, true, Q)
	step("the replaced run can no longer move it", ports.ThreadTransition{From: []ports.ExecutionState{Q}, To: C, RunID: "r1"}, false, Q)
	step("no run named matches any run", ports.ThreadTransition{From: []ports.ExecutionState{Q}, To: R}, true, R)

	// A thread from before the run was recorded matches any run.
	legacy, _ := s.Threads().Create(ctx, ports.ThreadInit{}, sc)
	if won, err := s.Threads().Transition(ctx, legacy.ID, ports.ThreadTransition{From: []ports.ExecutionState{ports.StateIdle}, To: ports.StateFailed, RunID: "x"}, sc); err != nil || !won {
		t.Fatalf("a thread with no run recorded matches any run: %v %v", won, err)
	}
	if won, _ := s.Threads().Transition(ctx, "no-such-thread", ports.ThreadTransition{From: []ports.ExecutionState{ports.StateIdle}, To: Q}, sc); won {
		t.Fatal("an unknown thread never matches")
	}
}

func TestTransition_MemoryStorage(t *testing.T) { transitionContract(t, memory.NewStorage()) }
func TestTransition_SqliteStorage(t *testing.T) { transitionContract(t, openSqlite(t)) }
func TestTransition_PostgresStorage(t *testing.T) {
	db := openPostgres(t)
	ctx := context.Background()
	for _, tbl := range []string{"usage", "events", "messages", "threads"} {
		_, _ = db.ExecContext(ctx, "DROP TABLE IF EXISTS tr_"+tbl)
	}
	s, err := pgstorage.New(ctx, db, pgstorage.WithPrefix("tr_"))
	if err != nil {
		t.Fatal(err)
	}
	transitionContract(t, s)
}

func TestState_AFinishLosesToAStopThatLandedFirst(t *testing.T) {
	h := makeRuntime(t, scripted(step{text: "ok"}))
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{Name: "chat"})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "hi"})
	deps := h.rt.Ports(nil)
	// A worker picked the run up...
	if won, err := core.Transition(h.ctx, deps, ran.ThreadID, core.StateChange{
		From: []ports.ExecutionState{ports.StateQueued}, To: ports.StateRunning, RunID: ran.RunID,
	}); err != nil || !won {
		t.Fatalf("pickup: %v %v", won, err)
	}
	// ...the user stopped it...
	if res, err := chat.Stop(h.ctx, ran.ThreadID, nil); err != nil || !res.Accepted {
		t.Fatalf("stop: %+v %v", res, err)
	}
	// ...and then the worker finished, having read RUNNING before the stop.
	if err := agentenkit.Finalize(h.ctx, deps, nil, ran.ThreadID, agentenkit.FinalizeInput{
		State: ports.StateCompleted, StopReason: "completed", RunID: ran.RunID,
	}); err != nil {
		t.Fatal(err)
	}
	mustEqual(t, h.thread(t, ran.ThreadID).State, agentenkit.StateCancelled, "the stop stands")
	mustEqual(t, h.kvGet(agentenkit.StateKey(ran.ThreadID)), "CANCELLED", "on the hot cache too")
	mustStrings(t, h.states(ran.ThreadID), []string{"QUEUED", "CANCELLED"}, "no COMPLETED after the stop")
	rec, _ := h.admin.Runs().Get(h.ctx, ran.RunID)
	mustEqual(t, rec.State, agentenkit.StateCancelled, "the record keeps the stop")
}

func TestStop_AQueuedRunIsCancelledAndItsJobDoesNothing(t *testing.T) {
	h := makeRuntime(t, scripted(step{text: "ok"}))
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{Name: "chat"})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "hi"})
	res, err := chat.Stop(h.ctx, ran.ThreadID, nil)
	if err != nil || !res.Accepted {
		t.Fatalf("a queued run can be stopped: %+v %v", res, err)
	}
	h.handleNext(t)
	mustEqual(t, h.model.Calls(), 0, "the job does nothing")
	mustEqual(t, h.thread(t, ran.ThreadID).State, agentenkit.StateCancelled, "CANCELLED")
}

func TestPark_AStoppedRunParksNothing(t *testing.T) {
	h := makeRuntime(t, scripted(step{text: "ok"}))
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{Name: "chat"})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "hi"})
	deps := h.rt.Ports(nil)
	_, _ = core.Transition(h.ctx, deps, ran.ThreadID, core.StateChange{
		From: []ports.ExecutionState{ports.StateQueued}, To: ports.StateRunning, RunID: ran.RunID,
	})
	if _, err := chat.Stop(h.ctx, ran.ThreadID, nil); err != nil {
		t.Fatal(err)
	}
	// A tool that was mid-call when the stop landed now tries to park.
	if err := core.ParkForApproval(h.ctx, deps, core.ParkInput{
		ThreadID: ran.ThreadID, ToolCallID: "c1", ToolName: "send",
		Resume: ports.ResumeInfo{Agent: "chat", Model: "gpt-4o", RunID: ran.RunID},
	}); err != nil {
		t.Fatal(err)
	}
	mustEqual(t, h.thread(t, ran.ThreadID).State, agentenkit.StateCancelled, "still CANCELLED")
	mustEqual(t, len(h.events(ran.ThreadID, "INPUT_REQUIRED")), 0, "no approval request")
	mustEqual(t, h.queue.Len(), 1, "no expiry job (only the original dispatch)")
}

func TestRetry_AReplacedRunsFailureSpendsNoAttempt(t *testing.T) {
	h := makeRuntime(t, scripted(step{text: "ok"}))
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{Name: "chat"})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "hi"})
	job, _ := h.queue.Shift()
	_, _ = h.kv.Set(h.ctx, agentenkit.RunIDKey(ran.ThreadID), "a-newer-run", ports.SetOptions{})
	err := chat.ExecuteWithPolicy(h.ctx, agentenkit.ExecuteInput{ThreadID: job.ThreadID, RunID: job.RunID, Model: job.Model},
		&agentenkit.Policy{Exec: func(context.Context, ports.RuntimePorts, *agentenkit.RegisteredAgent, agentenkit.ExecuteInput) (agentenkit.ExecuteOutcome, error) {
			return "", errors.New("boom")
		}})
	if err != nil {
		t.Fatal(err)
	}
	mustEqual(t, h.kvGet(agentenkit.AttemptsKey(ran.RunID)), "", "no attempt spent")
	mustEqual(t, h.queue.Len(), 0, "nothing retried")
	mustEqual(t, h.thread(t, ran.ThreadID).State, agentenkit.StateQueued, "the thread is left to the newer run")
}

func TestRetry_AFailedRunWaitsQueued(t *testing.T) {
	h := makeRuntime(t, scripted(step{err: errBoom}, step{text: "ok"}), func(c *agentenkit.AgentConfig) {
		c.RunRetryBackoff = time.Millisecond
	})
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{Name: "chat"})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "hi"})
	h.handleNext(t)
	mustEqual(t, h.thread(t, ran.ThreadID).State, agentenkit.StateQueued, "QUEUED while it waits to retry")
	mustStrings(t, h.states(ran.ThreadID), []string{"QUEUED", "RUNNING", "QUEUED"}, "and says so")
	mustEqual(t, h.kvGet(agentenkit.AttemptsKey(ran.RunID)), "1", "one attempt, counted per run")
	h.handleNext(t)
	mustEqual(t, h.thread(t, ran.ThreadID).State, agentenkit.StateCompleted, "the retry completes it")
}
