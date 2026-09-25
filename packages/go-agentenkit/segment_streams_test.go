package agentenkit_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	agentenkit "github.com/eadwinCode/agentic-kit/packages/go-agentenkit"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/adapters/memory"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/core"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

// Workstream S4: the engine writes a run stream per segment. The TS package
// runs the same cases under the same names (test/segment-streams.test.ts).

func streamRuntime(t *testing.T, model *scriptedModel, tune ...func(*agentenkit.AgentConfig)) (*harness, *memory.RunStreams) {
	t.Helper()
	streams := memory.NewRunStreams()
	tune = append([]func(*agentenkit.AgentConfig){func(c *agentenkit.AgentConfig) {
		c.StreamFlush = 0
		c.RunMaxAttempts = 1
	}}, tune...)
	h := makeRuntimeOpts(t, model, func(o *agentenkit.RuntimeOptions) { o.Streams = streams }, tune...)
	return h, streams
}

func streamItems(t *testing.T, s *memory.RunStreams, streamID string) []ports.StreamItem {
	t.Helper()
	snap, err := s.Snapshot(context.Background(), streamID, "")
	if err != nil {
		t.Fatal(err)
	}
	if snap == nil {
		return nil
	}
	return snap.Items
}

func itemTypes(items []ports.StreamItem) []string {
	out := []string{}
	for _, i := range items {
		out = append(out, i.Event.StreamEventType())
	}
	return out
}

func TestSegmentStreams(t *testing.T) {
	t.Run("a run writes its stream from RUN_STARTED to RUN_FINISHED", func(t *testing.T) {
		h, streams := streamRuntime(t, scripted(step{reasoning: "hmm", deltas: []string{"Hel", "lo"}}))
		chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{Name: "chat", Model: "gpt-4o"})
		ran := h.run(t, chat, agentenkit.RunInput{Prompt: "hi"})
		h.handleNext(t)

		got := streamItems(t, streams, ran.RunID+":1")
		mustStrings(t, itemTypes(got), []string{
			"RUN_STARTED",
			"REASONING_START", "REASONING_CONTENT", "REASONING_END",
			"TEXT_MESSAGE_START", "TEXT_MESSAGE_CONTENT", "TEXT_MESSAGE_END",
			"STEP_FINISHED", "RUN_FINISHED",
		}, "stream")
		start := got[0].Event.(*ports.RunStartedEvent)
		mustEqual(t, start.ThreadID+"/"+start.RunID, ran.ThreadID+"/"+ran.RunID, "RUN_STARTED names the run")
		mustEqual(t, start.Segment, 1, "segment")
		var text string
		for _, i := range got {
			if c, ok := i.Event.(*ports.TextMessageContentEvent); ok {
				text += c.Delta
			}
		}
		mustEqual(t, text, "Hello", "text")
		step := got[len(got)-2].Event.(*ports.StepFinishedEvent)
		mustEqual(t, step.Step, 1, "step")
		mustEqual(t, step.AgentID == nil, true, "the main agent's step")
		mustEqual(t, step.FinishReason, "stop", "finishReason")
		end := got[len(got)-1].Event.(*ports.RunFinishedEvent)
		mustEqual(t, end.Status, "finished", "status")
		mustEqual(t, end.FinishReason, "stop", "finishReason")
		mustEqual(t, end.Usage.TotalTokens, int64(15), "usage")
		snap, _ := streams.Snapshot(context.Background(), ran.RunID+":1", "")
		mustEqual(t, snap.End.(*ports.RunFinishedEvent).Status, "finished", "the stream is closed")
	})

	t.Run("tool calls stream as TOOL_CALL events", func(t *testing.T) {
		h, streams := streamRuntime(t, scripted(
			step{calls: []call{{"c1", "lookup", `{"q":"x"}`}}},
			step{text: "found"},
		))
		chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{
			Name: "chat", Model: "gpt-4o",
			Tools: []agentenkit.Tool{tool("lookup", func(_ context.Context, a map[string]any) (string, error) {
				return "result for " + a["q"].(string), nil
			})},
		})
		ran := h.run(t, chat, agentenkit.RunInput{Prompt: "hi"})
		h.handleNext(t)

		got := streamItems(t, streams, ran.RunID+":1")
		mustStrings(t, itemTypes(got), []string{
			"RUN_STARTED",
			"TOOL_CALL_START", "TOOL_CALL_END", "TOOL_CALL_RESULT", "STEP_FINISHED",
			"TEXT_MESSAGE_START", "TEXT_MESSAGE_CONTENT", "TEXT_MESSAGE_END", "STEP_FINISHED",
			"RUN_FINISHED",
		}, "stream")
		end := got[2].Event.(*ports.ToolCallEndEvent)
		mustEqual(t, end.ToolCallID+"/"+end.ToolName+"/"+string(end.Args), `c1/lookup/{"q":"x"}`, "TOOL_CALL_END")
		res := got[3].Event.(*ports.ToolCallResultEvent)
		mustEqual(t, res.ToolCallID+"/"+string(res.Result), `c1/"result for x"`, "TOOL_CALL_RESULT")
	})

	t.Run("publishEvent arrives as CUSTOM with its name and value", func(t *testing.T) {
		h, streams := streamRuntime(t, scripted(
			step{calls: []call{{"c1", "render", `{}`}}},
			step{text: "done"},
		))
		chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{
			Name: "chat", Model: "gpt-4o",
			Tools: []agentenkit.Tool{agentenkit.AgentTool("render", "render", func(ctx context.Context, _ map[string]any, tc agentenkit.ToolContext) (string, error) {
				_, err := tc.PublishEvent(ctx, "SEARCH_PROGRESS", map[string]any{"done": 3, "of": 10}, agentenkit.PublishOptions{})
				return "ok", err
			})},
		})
		ran := h.run(t, chat, agentenkit.RunInput{Prompt: "hi"})
		h.handleNext(t)
		var custom []*ports.CustomEvent
		for _, i := range streamItems(t, streams, ran.RunID+":1") {
			if c, ok := i.Event.(*ports.CustomEvent); ok {
				custom = append(custom, c)
			}
		}
		mustEqual(t, len(custom), 1, "one CUSTOM")
		mustEqual(t, custom[0].Name+" "+string(custom[0].Value), `SEARCH_PROGRESS {"done":3,"of":10}`, "CUSTOM")
	})

	t.Run("a park closes the segment and the resume opens the next", func(t *testing.T) {
		h, streams := streamRuntime(t, scripted(
			step{calls: []call{{"c1", "wipe", `{}`}}},
			step{text: "wiped"},
		), func(c *agentenkit.AgentConfig) { c.HITLTTL = time.Hour })
		wipe := agentenkit.MarkRequiresConfirmation(tool("wipe", func(context.Context, map[string]any) (string, error) { return "gone", nil }))
		chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{Name: "chat", Model: "gpt-4o", Tools: []agentenkit.Tool{wipe}})
		ran := h.run(t, chat, agentenkit.RunInput{Prompt: "wipe it"})
		h.handleNext(t)

		first := streamItems(t, streams, ran.RunID+":1")
		asked := false
		for _, i := range first {
			if r, ok := i.Event.(*ports.InputRequiredEvent); ok && r.ToolCallID == "c1" {
				asked = true
			}
		}
		mustEqual(t, asked, true, "INPUT_REQUIRED on the first stream")
		mustEqual(t, first[len(first)-1].Event.(*ports.RunFinishedEvent).Status, "parked", "the park ends it")

		h.queue.Shift() // the expiry job
		if _, err := h.rt.HITL.Respond(h.ctx, agentenkit.RespondInput{ThreadID: ran.ThreadID, ToolCallID: "c1", Approved: true}); err != nil {
			t.Fatal(err)
		}
		h.handleNext(t)

		second := streamItems(t, streams, ran.RunID+":2")
		start := second[0].Event.(*ports.RunStartedEvent)
		mustEqual(t, start.Segment, 2, "segment 2")
		mustEqual(t, start.StreamID, ran.RunID+":2", "its own stream")
		result := false
		for _, i := range second {
			if r, ok := i.Event.(*ports.ToolCallResultEvent); ok && r.ToolCallID == "c1" {
				result = true
			}
		}
		mustEqual(t, result, true, "the verdict's result on the resume's stream")
		mustEqual(t, second[len(second)-1].Event.(*ports.RunFinishedEvent).Status, "finished", "finished")
	})

	t.Run("a failed run ends its stream with RUN_ERROR", func(t *testing.T) {
		h, streams := streamRuntime(t, scripted(step{err: errors.New("provider down")}))
		chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{Name: "chat", Model: "gpt-4o"})
		ran := h.run(t, chat, agentenkit.RunInput{Prompt: "hi"})
		job, _ := h.queue.Shift()
		_, _ = h.rt.Worker.HandleJob(h.ctx, job)
		got := streamItems(t, streams, ran.RunID+":1")
		end := got[len(got)-1].Event.(*ports.RunErrorEvent)
		mustEqual(t, end.Status, "error", "status")
		mustEqual(t, strings.Contains(end.Error, "provider down"), true, "the error: "+end.Error)
	})

	t.Run("a one-shot agent's text rides on RUN_FINISHED", func(t *testing.T) {
		h, streams := streamRuntime(t, scripted(step{text: "forty-two"}))
		oneShot := h.rt.CreateGenerateTextAgent(agentenkit.GenerateTextAgentSpec{Name: "answer", Model: "gpt-4o"})
		ran := h.run(t, oneShot, agentenkit.RunInput{Prompt: "q"})
		h.handleNext(t)
		got := streamItems(t, streams, ran.RunID+":1")
		end := got[len(got)-1].Event.(*ports.RunFinishedEvent)
		mustEqual(t, end.Status+" "+end.Text, "finished forty-two", "RUN_FINISHED")
	})

	t.Run("a subagent run is wrapped in SUBAGENT_EVENT", func(t *testing.T) {
		h, streams := streamRuntime(t, scripted(
			step{calls: []call{{"p1", "spawnSubagent", `{"name":"helper","instructions":"do it"}`}}},
			step{text: "did it"},
			step{text: "all set"},
		))
		chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{Name: "chat", Model: "gpt-4o", Subagents: &agentenkit.SubagentsConfig{}})
		ran := h.run(t, chat, agentenkit.RunInput{Prompt: "delegate"})
		h.handleNext(t)

		got := streamItems(t, streams, ran.RunID+":1")
		var started *ports.SubagentStartedEvent
		var nested []string
		var finished *ports.SubagentFinishedEvent
		childStep := false
		for _, i := range got {
			switch e := i.Event.(type) {
			case *ports.SubagentStartedEvent:
				started = e
			case *ports.SubagentEventEvent:
				mustEqual(t, e.SubagentID, started.SubagentID, "wrapped under the child")
				nested = append(nested, e.Event.StreamEventType())
			case *ports.StepFinishedEvent:
				if e.AgentID != nil && *e.AgentID == started.SubagentID {
					childStep = true
				}
			case *ports.SubagentFinishedEvent:
				finished = e
			}
		}
		mustEqual(t, started.Name+"/"+string(rune('0'+started.Depth)), "helper/1", "SUBAGENT_STARTED")
		mustStrings(t, nested, []string{"TEXT_MESSAGE_START", "TEXT_MESSAGE_CONTENT", "TEXT_MESSAGE_END"}, "the child's events")
		mustEqual(t, childStep, true, "the child's STEP_FINISHED")
		mustEqual(t, finished.SubagentID+"/"+finished.Status, started.SubagentID+"/completed", "SUBAGENT_FINISHED")
		mustEqual(t, got[len(got)-1].Event.(*ports.RunFinishedEvent).Status, "finished", "finished")
	})

	t.Run("a lost worker's stream is closed as lost by the sweep", func(t *testing.T) {
		h, streams := streamRuntime(t, scripted(step{text: "never"}))
		chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{Name: "chat", Model: "gpt-4o"})
		ran := h.run(t, chat, agentenkit.RunInput{Prompt: "hi"})
		job, _ := h.queue.Shift()
		// A worker picked the run up, opened its stream and died.
		core.OpenSegment(h.ctx, h.rt.Ports(nil), ran.ThreadID, ran.RunID)
		h.rt.Worker.HandleDeadJob(h.ctx, job, 3, errors.New("worker died"))
		snap, _ := streams.Snapshot(h.ctx, ran.RunID+":1", "")
		end, ok := snap.End.(*ports.RunErrorEvent)
		mustEqual(t, ok && end.Status == "lost", true, "closed as lost")
	})

	t.Run("events wait for the flush window, and a step end goes out at once", func(t *testing.T) {
		h, streams := streamRuntime(t, scripted(step{text: "a"}), func(c *agentenkit.AgentConfig) {
			c.StreamFlush = time.Minute
			c.StreamFlushEvents = 1000
		})
		seg := core.OpenSegment(h.ctx, h.rt.Ports(nil), "t1", "run1")
		seg.Forward(h.ctx, "CHUNK", json.RawMessage(`{"type":"text-delta","textDelta":"a"}`), true)
		mustStrings(t, itemTypes(streamItems(t, streams, "run1:1")), []string{"RUN_STARTED"}, "held")
		seg.Forward(h.ctx, "STEP_FINISHED", json.RawMessage(`{"agentId":null,"index":1,"finishReason":"stop","totalTokens":3}`), true)
		mustStrings(t, itemTypes(streamItems(t, streams, "run1:1")), []string{
			"RUN_STARTED", "TEXT_MESSAGE_START", "TEXT_MESSAGE_CONTENT", "TEXT_MESSAGE_END", "STEP_FINISHED",
		}, "flushed")
		seg.Close(h.ctx, &ports.RunFinishedEvent{Status: "finished"})
		mustEqual(t, seg.Closed(), true, "closed")
	})
}
