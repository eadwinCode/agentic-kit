package agentenkit_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/zendev-sh/goai/provider"

	agentenkit "github.com/eadwinCode/agentic-kit/packages/go-agentenkit"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/core"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

// Workstream D: a park is written only once its step is saved, an answer
// survives until its result is saved, and a failure on the way back up never
// leaves a thread stuck. The same cases run in the TS package
// (test/park-commit.test.ts).

// approvalSetup is a main agent with one approval tool ("send") and one plain
// tool ("lookup").
func approvalSetup(t *testing.T, steps ...step) (*harness, *agentenkit.AgentHandle, *int) {
	sent := 0
	h := makeRuntime(t, scripted(steps...), func(c *agentenkit.AgentConfig) { c.RunRetryBackoff = time.Millisecond })
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{
		Name: "chat", Model: "gpt-4o",
		Tools: []agentenkit.Tool{
			agentenkit.MarkRequiresConfirmation(tool("send", func(context.Context, map[string]any) (string, error) {
				sent++
				return `{"sent":true}`, nil
			})),
			tool("lookup", func(_ context.Context, args map[string]any) (string, error) {
				if args["bad"] == true {
					return "", errors.New("bad input")
				}
				return `{"found":42}`, nil
			}),
		},
	})
	return h, chat, &sent
}

func allEvents(t *testing.T, h *harness, threadID string) []agentenkit.AgentEvent {
	t.Helper()
	out, err := h.storage.Events().ListSince(h.ctx, threadID, -1, agentenkit.StorageContext{})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func seqOf(events []agentenkit.AgentEvent, typ string) int64 {
	for _, e := range events {
		if e.Type == typ {
			return e.Seq
		}
	}
	return -1
}

func TestPark_IsWrittenOnlyAfterItsStepIsSaved(t *testing.T) {
	h, chat, _ := approvalSetup(t, step{calls: []call{{"a1", "send", `{}`}}})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "go"})
	h.handleNext(t)
	events := allEvents(t, h, ran.ThreadID)
	committed, requested := seqOf(events, "STEP_COMMITTED"), seqOf(events, "INPUT_REQUIRED")
	if committed < 0 || requested < 0 || requested < committed {
		t.Fatalf("INPUT_REQUIRED (%d) must follow STEP_COMMITTED (%d)", requested, committed)
	}
	mustEqual(t, h.thread(t, ran.ThreadID).State, agentenkit.StateWaitingForInput, "parked")
	mustStrings(t, h.roles(ran.ThreadID), []string{"user", "assistant"}, "the call is saved with the park")
}

// failFirstAssistantSave is storage whose first save of an assistant turn
// fails: a step that ran its tools and then could not be saved.
type failFirstAssistantSave struct {
	ports.Storage
	failed *bool
}

type failingMessages struct {
	ports.MessageStore
	failed *bool
}

func (s failFirstAssistantSave) Messages() ports.MessageStore {
	return failingMessages{s.Storage.Messages(), s.failed}
}

func (m failingMessages) Append(ctx context.Context, threadID string, msg ports.NewMessage, sc ports.StorageContext) (*ports.MessageDTO, error) {
	if msg.Role == ports.RoleAssistant && !*m.failed {
		*m.failed = true
		return nil, errors.New("storage down")
	}
	return m.MessageStore.Append(ctx, threadID, msg, sc)
}

func TestPark_AStepThatFailsAfterAToolAskedWritesNoPark(t *testing.T) {
	sent := 0
	failed := false
	h := makeRuntimeOpts(t, scripted(step{calls: []call{{"a1", "send", `{}`}}}, step{text: "ok"}),
		func(o *agentenkit.RuntimeOptions) { o.Storage = failFirstAssistantSave{o.Storage, &failed} },
		func(c *agentenkit.AgentConfig) { c.RunRetryBackoff = time.Millisecond })
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{
		Name: "chat", Model: "gpt-4o",
		Tools: []agentenkit.Tool{agentenkit.MarkRequiresConfirmation(tool("send", func(context.Context, map[string]any) (string, error) {
			sent++
			return `{"sent":true}`, nil
		}))},
	})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "go"})
	h.handleNext(t) // the tool asks for approval, then the step cannot be saved
	mustEqual(t, len(h.events(ran.ThreadID, "INPUT_REQUIRED")), 0, "no approval request")
	mustEqual(t, h.thread(t, ran.ThreadID).State, agentenkit.StateQueued, "the step goes to the retry policy")
	h.handleNext(t)
	mustEqual(t, h.thread(t, ran.ThreadID).State, agentenkit.StateCompleted, "the retry completes it")
	mustEqual(t, sent, 0, "never sent")
}

func TestPark_TheOtherResultsOfAStepThatParksAreKept(t *testing.T) {
	h, chat, _ := approvalSetup(t, step{calls: []call{{"l1", "lookup", `{}`}, {"a1", "send", `{}`}}})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "go"})
	h.handleNext(t)
	msgs, _ := h.storage.Messages().List(h.ctx, ran.ThreadID, agentenkit.MainAgent, agentenkit.StorageContext{})
	found := false
	for _, m := range msgs {
		for _, p := range agentenkit.ParseContent(m.Content) {
			if p.ToolCallID == "l1" && strings.Contains(string(p.Result), "42") {
				found = true
			}
			if p.ToolCallID == "a1" && m.Role == ports.RoleTool {
				t.Fatal("the parked call has no result yet")
			}
		}
	}
	if !found {
		t.Fatal("the lookup that ran beside the park keeps its result")
	}
}

func TestHitl_AnApprovedToolThatRanBeforeACrashIsNotRunAgain(t *testing.T) {
	h, chat, sent := approvalSetup(t, step{calls: []call{{"a1", "send", `{}`}}}, step{text: "done"})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "go"})
	h.handleNext(t)
	h.queue.Shift() // the park's expiry job
	if res, err := h.rt.HITL.Respond(h.ctx, agentenkit.RespondInput{ThreadID: ran.ThreadID, ToolCallID: "a1", Approved: true}); err != nil || !res.Delivered {
		t.Fatalf("respond: %+v %v", res, err)
	}
	// The tool ran, then the worker died before its result was saved.
	_, _ = h.kv.Set(h.ctx, core.HitlDoneKey("a1"), `{"cached":true}`, ports.SetOptions{})
	h.handleNext(t)
	mustEqual(t, *sent, 0, "not run a second time")
	mustEqual(t, h.thread(t, ran.ThreadID).State, agentenkit.StateCompleted, "completed")
	msgs, _ := h.storage.Messages().List(h.ctx, ran.ThreadID, agentenkit.MainAgent, agentenkit.StorageContext{})
	result := agentenkit.ParseContent(msgs[2].Content)[0]
	mustEqual(t, result.ToolCallID, "a1", "the approved call")
	if !strings.Contains(string(result.Result), "cached") {
		t.Fatalf("the kept output lands: %s", result.Result)
	}
	mustEqual(t, h.kvGet(agentenkit.HitlKey("a1")), "", "answer cleared once landed")
	mustEqual(t, h.kvGet(core.HitlDoneKey("a1")), "", "kept output cleared once landed")
}

func TestHitl_ASecondAnswerIsRefused(t *testing.T) {
	h, chat, _ := approvalSetup(t, step{calls: []call{{"a1", "send", `{}`}}})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "go"})
	h.handleNext(t)
	first, _ := h.rt.HITL.Respond(h.ctx, agentenkit.RespondInput{ThreadID: ran.ThreadID, ToolCallID: "a1", Approved: true})
	second, _ := h.rt.HITL.Respond(h.ctx, agentenkit.RespondInput{ThreadID: ran.ThreadID, ToolCallID: "a1", Approved: false})
	mustEqual(t, first.Delivered, true, "first answer")
	mustEqual(t, second.Delivered, false, "second answer")
	mustEqual(t, second.Error, "This request was already answered", "why")
}

func TestHitl_AnEarlierRunsParkIsNotOpen(t *testing.T) {
	h, chat, _ := approvalSetup(t, step{calls: []call{{"a1", "send", `{}`}}})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "go"})
	h.handleNext(t)
	deps := h.rt.Ports(nil)
	open, _ := agentenkit.LoadOpenHitls(h.ctx, deps, ran.ThreadID)
	mustEqual(t, len(open), 1, "open for its own run")
	_, _ = h.kv.Set(h.ctx, agentenkit.RunIDKey(ran.ThreadID), "a-later-run", ports.SetOptions{})
	open, _ = agentenkit.LoadOpenHitls(h.ctx, deps, ran.ThreadID)
	mustEqual(t, len(open), 0, "not for a later run")
}

func TestHitl_AChildThatFailsWhileUnwindingIsReportedToItsParent(t *testing.T) {
	h := nestedParkSetup(t,
		step{err: errBoom},            // the child, re-entered after the verdict, fails
		step{text: "parent: handled"}, // the parent, handed the failure
	)
	ran := h.run(t, h.chat, agentenkit.RunInput{Prompt: "go"})
	h.handleNext(t)
	h.queue.Shift() // expiry job
	if _, err := h.rt.HITL.Respond(h.ctx, agentenkit.RespondInput{ThreadID: ran.ThreadID, ToolCallID: "d1", Approved: true}); err != nil {
		t.Fatal(err)
	}
	h.handleNext(t)
	mustStrings(t, *h.executed, []string{"prod"}, "the approved tool ran")
	mustEqual(t, h.thread(t, ran.ThreadID).State, agentenkit.StateCompleted, "the thread is not stuck")
	mustEqual(t, len(h.events(ran.ThreadID, "SUBAGENT_FAILED")), 1, "the child is recorded as failed")
	parent, _ := h.storage.Messages().List(h.ctx, ran.ThreadID, agentenkit.MainAgent, agentenkit.StorageContext{})
	spawn := agentenkit.ParseContent(parent[2].Content)[0]
	mustEqual(t, spawn.ToolCallID, "s1", "the waiting spawn call is answered")
	if !strings.Contains(string(spawn.Result), `"error"`) {
		t.Fatalf("with the failure: %s", spawn.Result)
	}
}

func TestHitl_AnUnwindCutShortCarriesOnFromTheLevelStillWaiting(t *testing.T) {
	h := nestedParkSetup(t, step{text: "child: done"}, step{text: "parent: done"})
	ran := h.run(t, h.chat, agentenkit.RunInput{Prompt: "go"})
	h.handleNext(t)
	childID := payload(h.events(ran.ThreadID, "SUBAGENT_STARTED")[0])["agentId"].(string)
	// A worker landed the child's verdict, then died before the level above.
	if _, err := h.storage.Messages().Append(h.ctx, ran.ThreadID, ports.NewMessage{
		Role: ports.RoleTool, AgentID: childID,
		Content: core.ToolResultContent("d1", "wipe", map[string]any{"denied": true}),
	}, agentenkit.StorageContext{}); err != nil {
		t.Fatal(err)
	}
	h.handleNext(t) // the park's expiry job comes due
	mustEqual(t, len(*h.executed), 0, "the landed verdict is not settled again")
	mustEqual(t, h.thread(t, ran.ThreadID).State, agentenkit.StateCompleted, "the unwind carried on to the end")
	parent, _ := h.storage.Messages().List(h.ctx, ran.ThreadID, agentenkit.MainAgent, agentenkit.StorageContext{})
	mustEqual(t, agentenkit.ParseContent(parent[2].Content)[0].ToolCallID, "s1", "the spawn call is answered")
}

func TestTools_AToolThatThrowsHandsTheErrorToTheModel(t *testing.T) {
	h, chat, _ := approvalSetup(t, step{calls: []call{{"l1", "lookup", `{"bad":true}`}}}, step{text: "sorry"})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "go"})
	h.handleNext(t)
	mustEqual(t, h.thread(t, ran.ThreadID).State, agentenkit.StateCompleted, "the run goes on")
	params := h.model.Params()
	seen := false
	for _, m := range params[len(params)-1].Messages {
		for _, p := range m.Content {
			if p.Type == provider.PartToolResult && p.ToolOutput == "error: bad input" {
				seen = true
			}
		}
	}
	if !seen {
		t.Fatal(`the model is told "error: bad input"`)
	}
}

func TestRepair_AnOrphanToolResultIsDropped(t *testing.T) {
	msgs := []provider.Message{
		{Role: provider.RoleUser, Content: []provider.Part{{Type: provider.PartText, Text: "hi"}}},
		{Role: provider.RoleTool, Content: []provider.Part{{Type: provider.PartToolResult, ToolCallID: "gone", ToolOutput: "{}"}}},
		{Role: provider.RoleAssistant, Content: []provider.Part{{Type: provider.PartText, Text: "hello"}}},
	}
	out := agentenkit.RepairDanglingToolCalls(msgs)
	mustEqual(t, len(out), 2, "the orphan result is gone")
	mustEqual(t, out[1].Role, provider.RoleAssistant, "the rest stays in order")
}
