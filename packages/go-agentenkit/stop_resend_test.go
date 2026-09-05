package agentenkit_test

import (
	"sync"
	"testing"
	"time"

	"github.com/zendev-sh/goai/provider"

	agentenkit "github.com/eadwinCode/agentic-kit/packages/go-agentenkit"
)

func TestStop_TearsARunningWorkerDown(t *testing.T) {
	h := makeRuntime(t, scripted(step{text: "slow", delay: 500 * time.Millisecond}))
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{Name: "chat"})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "hi"})
	job, _ := h.queue.Shift()
	var wg sync.WaitGroup
	wg.Add(1)
	started := time.Now()
	go func() {
		defer wg.Done()
		_, _ = h.rt.Worker.HandleJob(h.ctx, job)
	}()
	time.Sleep(30 * time.Millisecond)
	if _, err := chat.Stop(h.ctx, ran.ThreadID, nil); err != nil {
		t.Fatal(err)
	}
	stopped, _ := h.admin.Runs().Get(h.ctx, ran.RunID)
	if stopped.EndedAt == nil || stopped.DurationMs == nil {
		t.Fatal("stop did not record timing before worker teardown")
	}
	stoppedAt := *stopped.EndedAt
	wg.Wait()
	if time.Since(started) > 400*time.Millisecond {
		t.Fatal("the stop did not interrupt the model call")
	}
	mustEqual(t, h.thread(t, ran.ThreadID).State, agentenkit.StateCancelled, "state")
	term := h.lastTerminal(ran.ThreadID)
	mustEqual(t, term["state"], "CANCELLED", "published")
	mustEqual(t, term["stopReason"], "cancelled", "stopReason")
	mustEqual(t, h.kvGet(agentenkit.RunLockKey(ran.ThreadID)), "", "lock released")
	mustEqual(t, h.queue.Len(), 0, "a stop is never retried")
	rec, _ := h.admin.Runs().Get(h.ctx, ran.RunID)
	mustEqual(t, rec.State, agentenkit.StateCancelled, "run record")
	if rec.EndedAt == nil || !rec.EndedAt.Equal(stoppedAt) {
		t.Fatal("worker teardown changed the stop timestamp")
	}
}

func TestStop_TracksQueuedAndParkedRunsWithoutAnotherWorker(t *testing.T) {
	for _, parked := range []bool{false, true} {
		name := "queued"
		if parked {
			name = "parked"
		}
		t.Run(name, func(t *testing.T) {
			h := makeRuntime(t, scripted(step{calls: []call{{"c1", "wipe", `{}`}}}))
			wipe := tool("wipe", nil)
			wipe.RequiresConfirmation = true
			chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{Name: "chat", Tools: []agentenkit.Tool{wipe}})
			ran := h.run(t, chat, agentenkit.RunInput{Prompt: "go"})
			if parked {
				h.handleNext(t)
				mustEqual(t, h.thread(t, ran.ThreadID).State, agentenkit.StateWaitingForInput, "parked")
			}
			prior, _ := h.admin.Runs().Get(h.ctx, ran.RunID)
			res, err := chat.Stop(h.ctx, ran.ThreadID, nil)
			if err != nil || !res.Accepted {
				t.Fatalf("stop: %+v %v", res, err)
			}
			stopped, _ := h.admin.Runs().Get(h.ctx, ran.RunID)
			mustEqual(t, stopped.State, agentenkit.StateCancelled, "run state")
			mustEqual(t, stopped.StopReason, "cancelled", "reason")
			mustEqual(t, stopped.TotalTokens, prior.TotalTokens, "usage retained")
			mustEqual(t, stopped.Steps, prior.Steps, "steps retained")
			if stopped.EndedAt == nil || stopped.DurationMs == nil || *stopped.DurationMs < 0 {
				t.Fatal("missing stop timing")
			}
			term := h.lastTerminal(ran.ThreadID)
			mustEqual(t, term["runId"], ran.RunID, "event run identity")
			mustEqual(t, term["stopReason"], "cancelled", "event reason")
			if term["endedAt"] == nil {
				t.Fatal("missing event timestamp")
			}
			res, err = chat.Stop(h.ctx, ran.ThreadID, nil)
			if err != nil || res.Accepted {
				t.Fatalf("repeat stop: %+v %v", res, err)
			}
			h.drain(t)
			redelivered, _ := h.admin.Runs().Get(h.ctx, ran.RunID)
			mustEqual(t, redelivered.State, agentenkit.StateCancelled, "state after redelivery")
			if !redelivered.EndedAt.Equal(*stopped.EndedAt) {
				t.Fatal("redelivery changed the stop timestamp")
			}
		})
	}
}

// The user stops, then sends another message before the old worker's poll
// ever reads CANCELLED. The state key lies in that window; the run id never
// does: the old worker must notice it was replaced and stay silent.
func TestStop_ThenResend_TheReplacedWorkerStaysSilent(t *testing.T) {
	h := makeRuntime(t, scripted(
		step{text: "slow", delay: 300 * time.Millisecond},
		step{text: "fresh answer"},
	), func(c *agentenkit.AgentConfig) { c.StopPoll = 50 * time.Millisecond })
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{Name: "chat"})
	first := h.run(t, chat, agentenkit.RunInput{Prompt: "one"})
	job, _ := h.queue.Shift()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, _ = h.rt.Worker.HandleJob(h.ctx, job)
	}()
	time.Sleep(10 * time.Millisecond)
	if _, err := chat.Stop(h.ctx, first.ThreadID, nil); err != nil {
		t.Fatal(err)
	}
	// Immediately: a new message. RUNNING overwrites CANCELLED under the old worker.
	second := h.run(t, chat, agentenkit.RunInput{ThreadID: first.ThreadID, Prompt: "two"})
	if second.RunID == first.RunID {
		t.Fatal("a resend is a new run")
	}
	wg.Wait() // the old worker aborts on the run-id change and finalizes nothing
	mustEqual(t, h.thread(t, first.ThreadID).State, agentenkit.StateRunning, "the new run's RUNNING stands")
	mustEqual(t, h.kvGet(agentenkit.StateKey(first.ThreadID)), "RUNNING", "hot state stands")
	h.handleNext(t)
	mustEqual(t, h.lastTerminal(first.ThreadID)["state"], "COMPLETED", "the new run finishes")
	mustStrings(t, h.states(first.ThreadID), []string{"RUNNING", "CANCELLED", "RUNNING", "COMPLETED"}, "states")
	rows := h.storage.MessageRows(first.ThreadID)
	mustEqual(t, string(rows[len(rows)-1].Content), `[{"type":"text","text":"fresh answer"}]`, "answer")
}

// A park persists the assistant's tool call and defers its result to the
// resume. A stop means that resume never comes, so the stop must close the
// call itself, or the next run sends a prompt no strict provider accepts.
func TestStop_WhileParkedClosesTheDanglingToolCall(t *testing.T) {
	h := hitlSetup(t)
	ran := h.run(t, h.chat, agentenkit.RunInput{Prompt: "delete"})
	h.handleNext(t)
	mustStrings(t, h.roles(ran.ThreadID), []string{"user", "assistant"}, "parked: the call has no result yet")
	if _, err := h.chat.Stop(h.ctx, ran.ThreadID, nil); err != nil {
		t.Fatal(err)
	}
	mustStrings(t, h.roles(ran.ThreadID), []string{"user", "assistant", "tool"}, "the stop closed it")
	part := agentenkit.ParseContent(h.storage.MessageRows(ran.ThreadID)[2].Content)[0]
	mustEqual(t, part.ToolCallID, "c1", "answers the parked call")
	mustEqual(t, string(part.Result), `{"cancelled":true,"reason":"stopped"}`, "as a cancellation")
	mustEqual(t, len(*h.executed), 0, "the tool never ran")
	// The park is settled: it is no longer answerable, and the expiry job is a no-op
	res, _ := h.rt.HITL.Respond(h.ctx, agentenkit.RespondInput{ThreadID: ran.ThreadID, ToolCallID: "c1", Approved: true})
	mustEqual(t, res.Delivered, false, "respond after stop")
	h.drain(t)
	mustEqual(t, h.thread(t, ran.ThreadID).State, agentenkit.StateCancelled, "still cancelled")
	// And the next run on the thread sends a well-formed prompt
	h.run(t, h.chat, agentenkit.RunInput{ThreadID: ran.ThreadID, Prompt: "again"})
	h.handleNext(t)
	params := h.model.Params()
	last := params[len(params)-1]
	for i, m := range last.Messages {
		for _, p := range m.Content {
			if p.Type != provider.PartToolCall {
				continue
			}
			answered := false
			for _, later := range last.Messages[i+1:] {
				for _, q := range later.Content {
					if q.Type == provider.PartToolResult && q.ToolCallID == p.ToolCallID {
						answered = true
					}
				}
			}
			if !answered {
				t.Fatalf("tool call %s has no result in the prompt", p.ToolCallID)
			}
		}
	}
}

func TestStop_WhileANestedRunIsParkedClosesTheWholeChain(t *testing.T) {
	h := nestedParkSetup(t)
	ran := h.run(t, h.chat, agentenkit.RunInput{Prompt: "go"})
	h.handleNext(t)
	childID := payload(h.events(ran.ThreadID, "SUBAGENT_STARTED")[0])["agentId"].(string)
	if _, err := h.chat.Stop(h.ctx, ran.ThreadID, nil); err != nil {
		t.Fatal(err)
	}
	parent, _ := h.storage.Messages().List(h.ctx, ran.ThreadID, agentenkit.MainAgent, agentenkit.StorageContext{})
	child, _ := h.storage.Messages().List(h.ctx, ran.ThreadID, agentenkit.AgentScope(childID), agentenkit.StorageContext{})
	mustEqual(t, string(parent[len(parent)-1].Role), "tool", "the spawn call is closed")
	mustEqual(t, agentenkit.ParseContent(parent[len(parent)-1].Content)[0].ToolCallID, "s1", "spawn call id")
	mustEqual(t, string(child[len(child)-1].Role), "tool", "the child's call is closed")
	mustEqual(t, agentenkit.ParseContent(child[len(child)-1].Content)[0].ToolCallID, "d1", "child call id")
	rec, _ := h.admin.Runs().Get(h.ctx, childID)
	mustEqual(t, rec.State, agentenkit.StateCancelled, "child record cancelled")
}

func TestRepairDanglingToolCalls(t *testing.T) {
	msgs := []provider.Message{
		{Role: provider.RoleUser, Content: []provider.Part{{Type: provider.PartText, Text: "hi"}}},
		{Role: provider.RoleAssistant, Content: []provider.Part{
			{Type: provider.PartToolCall, ToolCallID: "a", ToolName: "x"},
			{Type: provider.PartToolCall, ToolCallID: "b", ToolName: "y"},
		}},
		{Role: provider.RoleTool, Content: []provider.Part{{Type: provider.PartToolResult, ToolCallID: "a", ToolName: "x", ToolOutput: "ok"}}},
		{Role: provider.RoleUser, Content: []provider.Part{{Type: provider.PartText, Text: "again"}}},
	}
	out := agentenkit.RepairDanglingToolCalls(msgs)
	mustEqual(t, len(out), 5, "one repair message inserted")
	mustEqual(t, out[3].Role, provider.RoleTool, "inserted before the next turn")
	mustEqual(t, out[3].Content[0].ToolCallID, "b", "for the unanswered call only")
	mustEqual(t, out[4].Content[0].Text, "again", "later turns untouched")
	mustEqual(t, len(agentenkit.RepairDanglingToolCalls(out)), 5, "idempotent")
}
