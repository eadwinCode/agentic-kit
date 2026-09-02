package agentenkit_test

import (
	"context"
	"strings"
	"testing"

	"github.com/zendev-sh/goai"

	agentenkit "github.com/eadwinCode/agentic-kit/packages/go-agentenkit"
)

func TestCustomEvents_AToolPublishesADurableEvent(t *testing.T) {
	h := makeRuntime(t, scripted(step{calls: []call{{"c1", "render", `{}`}}}, step{text: "done"}))
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{
		Name: "chat", Model: "gpt-4o",
		Tools: []agentenkit.Tool{agentenkit.AgentTool("render", "r",
			func(ctx context.Context, _ struct{}, tc agentenkit.ToolContext) (string, error) {
				e, err := tc.PublishEvent(ctx, "DESIGN_PREVIEW", map[string]any{"url": "https://x/1.png", "org": tc.State["orgId"]}, agentenkit.PublishOptions{})
				if err != nil {
					return "", err
				}
				if e.Seq == 0 {
					t.Error("durable event must carry a seq")
				}
				return "ok", nil
			})},
	})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "hi", State: agentenkit.AgentRunState{"orgId": "acme"}})
	h.handleNext(t)
	live := h.events(ran.ThreadID, "DESIGN_PREVIEW")
	mustEqual(t, len(live), 1, "fanned out")
	mustEqual(t, payload(live[0])["url"], "https://x/1.png", "payload")
	mustEqual(t, payload(live[0])["org"], "acme", "state reached the tool")
	logged, _ := h.rt.Events.Since(h.ctx, ran.ThreadID, -1, nil)
	found := false
	for _, e := range logged {
		if e.Type == "DESIGN_PREVIEW" && e.Seq == live[0].Seq {
			found = true
		}
	}
	if !found {
		t.Fatal("durable event missing from the log")
	}
	mustEqual(t, h.lastTerminal(ran.ThreadID)["state"], "COMPLETED", "run completed")
}

func TestCustomEvents_ANoticeReachesTheBusOnly(t *testing.T) {
	h := makeRuntime(t, scripted(step{calls: []call{{"c1", "slow", `{}`}}}, step{text: "done"}))
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{
		Name: "chat",
		Tools: []agentenkit.Tool{agentenkit.WrapTool(goai.NewTool("slow", "s", func(ctx context.Context, _ struct{}) (string, error) {
			// A hand-built goai tool reads the context off ctx
			tc := agentenkit.ToolContextFrom(ctx)
			mustEqual(t, tc.ToolCallID, "c1", "tool call id")
			_, err := tc.PublishEvent(ctx, "PROGRESS", map[string]any{"label": "Rendering…"}, agentenkit.PublishOptions{Notice: true})
			return "ok", err
		}))},
	})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "hi"})
	h.handleNext(t)
	notices := h.events(ran.ThreadID, "PROGRESS")
	mustEqual(t, len(notices), 1, "one notice")
	mustEqual(t, notices[0].Seq, int64(0), "seq 0")
	logged, _ := h.rt.Events.Since(h.ctx, ran.ThreadID, -1, nil)
	for _, e := range logged {
		if e.Type == "PROGRESS" {
			t.Fatal("a notice must not be logged")
		}
	}
}

func TestCustomEvents_RefusesPlatformTypes(t *testing.T) {
	h := makeRuntime(t, scripted(step{text: "ok"}))
	for _, typ := range []string{"STATE_CHANGE", "CHUNK", "INPUT_REQUIRED"} {
		_, err := h.rt.Events.PublishEvent(h.ctx, "t", typ, nil, agentenkit.PublishStateOptions{})
		if err == nil || !strings.Contains(err.Error(), "platform event type") {
			t.Fatalf("%s: expected refusal, got %v", typ, err)
		}
	}
	if _, err := h.rt.Events.PublishEvent(h.ctx, "t", "", nil, agentenkit.PublishStateOptions{}); err == nil {
		t.Fatal("empty type must be refused")
	}
	if _, err := agentenkit.ToolContextFrom(context.Background()).PublishEvent(context.Background(), "X", nil, agentenkit.PublishOptions{}); err == nil {
		t.Fatal("outside a run there is no publisher")
	}
}

func TestCustomEvents_TheRuntimePublishesFromOutsideARun(t *testing.T) {
	h := makeRuntime(t, scripted(step{text: "ok"}))
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{Name: "chat"})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "hi"})
	e, err := h.rt.Events.PublishEvent(h.ctx, ran.ThreadID, "CREDIT_LIMIT", map[string]any{"kind": "monthly"},
		agentenkit.PublishStateOptions{State: agentenkit.AgentRunState{"orgId": "acme"}})
	if err != nil {
		t.Fatal(err)
	}
	mustEqual(t, e.Type, "CREDIT_LIMIT", "type")
	mustEqual(t, h.storage.LastContext.State["orgId"], "acme", "scoped write")
	logged, _ := h.rt.Events.Since(h.ctx, ran.ThreadID, -1, nil)
	mustEqual(t, logged[len(logged)-1].Type, "CREDIT_LIMIT", "logged last")
}

func TestCustomEvents_NestedAndResumedToolsPublishToo(t *testing.T) {
	h := makeRuntime(t, scripted(
		step{calls: []call{{"s1", "spawnSubagent", spawnArgs}}},
		step{calls: []call{{"d1", "wipe", `{}`}}}, // the child parks
		step{text: "child done"},
		step{text: "parent done"},
	))
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{
		Name: "chat",
		Subagents: &agentenkit.SubagentsConfig{
			Tools: []agentenkit.Tool{agentenkit.MarkRequiresConfirmation(agentenkit.AgentTool("wipe", "w",
				func(ctx context.Context, _ struct{}, tc agentenkit.ToolContext) (string, error) {
					_, err := tc.PublishEvent(ctx, "WIPED", map[string]any{"org": tc.State["orgId"]}, agentenkit.PublishOptions{})
					return "gone", err
				}))},
		},
	})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "hi", State: agentenkit.AgentRunState{"orgId": "acme"}})
	h.handleNext(t)
	if _, err := h.rt.HITL.Respond(h.ctx, agentenkit.RespondInput{ThreadID: ran.ThreadID, ToolCallID: "d1", Approved: true}); err != nil {
		t.Fatal(err)
	}
	h.drain(t)
	wiped := h.events(ran.ThreadID, "WIPED")
	mustEqual(t, len(wiped), 1, "the resumed nested tool published")
	mustEqual(t, payload(wiped[0])["org"], "acme", "with the run state")
	mustEqual(t, h.lastTerminal(ran.ThreadID)["state"], "COMPLETED", "completed")
}

func TestApproval_PayloadReachesTheApprovedTool(t *testing.T) {
	var seen *agentenkit.Approval
	var seenID string
	calls := 0
	h := makeRuntime(t, scripted(step{calls: []call{{"q1", "askQuestions", `{"questions":["Colour?"]}`}}}, step{text: "thanks"}))
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{
		Name: "chat",
		Tools: []agentenkit.Tool{agentenkit.MarkRequiresConfirmation(agentenkit.AgentTool("askQuestions", "q",
			func(_ context.Context, _ struct{ Questions []string }, tc agentenkit.ToolContext) (string, error) {
				calls++
				seen, seenID = tc.Approval, tc.ToolCallID
				return string(tc.Approval.Payload), nil
			}))},
	})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "hi"})
	h.handleNext(t)
	mustEqual(t, calls, 0, "parked: never executed")
	if _, err := h.rt.HITL.Respond(h.ctx, agentenkit.RespondInput{ThreadID: ran.ThreadID, ToolCallID: "q1", Approved: true, Payload: map[string]any{"Colour": "teal"}}); err != nil {
		t.Fatal(err)
	}
	h.drain(t)
	mustEqual(t, calls, 1, "ran once on resume")
	if seen == nil {
		t.Fatal("approval missing")
	}
	mustEqual(t, string(seen.Payload), `{"Colour":"teal"}`, "payload")
	mustEqual(t, seenID, "q1", "tool call id on the resumed call")
	part := agentenkit.ParseContent(h.storage.MessageRows(ran.ThreadID)[2].Content)[0]
	mustEqual(t, string(part.Result), `{"Colour":"teal"}`, "the answers became the tool result")
}
