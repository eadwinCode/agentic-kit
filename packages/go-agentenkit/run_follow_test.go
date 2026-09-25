package agentenkit_test

import (
	"context"
	"strings"
	"testing"
	"time"

	agentenkit "github.com/eadwinCode/agentic-kit/packages/go-agentenkit"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/core"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

// Workstream S6: a follow reads the thread record and its run streams as
// one sequence. The TS package runs the same cases under the same names
// (test/run-follow.test.ts).

// readUntil reads frames until done holds, or fails after a second.
func readUntil(t *testing.T, stream *core.FrameStream, done func([]core.FollowFrame) bool) []core.FollowFrame {
	t.Helper()
	var frames []core.FollowFrame
	timeout := time.After(time.Second)
	for !done(frames) {
		select {
		case f, ok := <-stream.Frames():
			if !ok {
				t.Fatal("follow closed")
			}
			frames = append(frames, f)
		case <-timeout:
			t.Fatalf("timed out; got %d frames", len(frames))
		}
	}
	return frames
}

func frameTexts(frames []core.FollowFrame) []string {
	var out []string
	for _, f := range frames {
		if f.Kind == core.FrameKindStream {
			if c, ok := f.Item.Event.(*ports.TextMessageContentEvent); ok {
				out = append(out, c.Delta)
			}
		}
	}
	return out
}

func frameEnds(frames []core.FollowFrame) int {
	n := 0
	for _, f := range frames {
		if f.Kind == core.FrameKindStream {
			if _, ok := f.Item.Event.(*ports.RunFinishedEvent); ok {
				n++
			}
		}
	}
	return n
}

func TestRunFollow(t *testing.T) {
	t.Run("follow moves to the next run's stream on its start notice", func(t *testing.T) {
		h, _ := streamRuntime(t, scripted(step{text: "first"}, step{text: "second"}))
		chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{Name: "chat", Model: "gpt-4o"})
		ran := h.run(t, chat, agentenkit.RunInput{Prompt: "a"})
		ctx, cancel := context.WithCancel(h.ctx)
		defer cancel()
		stream, err := h.rt.Events.Follow(ctx, ran.ThreadID, agentenkit.FollowStateOptions{})
		must(t, err)
		time.Sleep(10 * time.Millisecond)
		h.handleNext(t)
		h.run(t, chat, agentenkit.RunInput{ThreadID: ran.ThreadID, Prompt: "b"})
		h.handleNext(t)
		frames := readUntil(t, stream, func(f []core.FollowFrame) bool { return frameEnds(f) == 2 })
		mustStrings(t, frameTexts(frames), []string{"first", "second"}, "both runs' text")
		streams := map[string]bool{}
		for _, f := range frames {
			if f.Kind == core.FrameKindStream {
				streams[f.StreamID] = true
			}
		}
		mustEqual(t, len(streams), 2, "one stream per run")
	})

	t.Run("a reconnect with Last-Event-ID resumes inside the stream", func(t *testing.T) {
		h, streams := streamRuntime(t, scripted(step{text: "x"}))
		seg := core.OpenSegment(h.ctx, h.rt.Ports(nil), "t1", "run1")
		seg.Push(h.ctx,
			&ports.TextMessageStartEvent{MessageID: "m1", Role: "assistant"},
			&ports.TextMessageContentEvent{MessageID: "m1", Delta: "seen "})
		seen := streamItems(t, streams, "run1:1")[2]
		seg.Push(h.ctx, &ports.TextMessageContentEvent{MessageID: "m1", Delta: "new"})
		seg.Close(h.ctx, &ports.RunFinishedEvent{Status: "finished"})

		// The browser sends back the id of the last frame it got.
		ctx, cancel := context.WithCancel(h.ctx)
		defer cancel()
		stream, err := h.rt.Events.Follow(ctx, "t1", agentenkit.FollowStateOptions{Cursor: "-1 run1:1 " + seen.Offset})
		must(t, err)
		frames := readUntil(t, stream, func(f []core.FollowFrame) bool { return frameEnds(f) == 1 })
		mustStrings(t, frameTexts(frames), []string{"new"}, "only what came after")
	})

	t.Run("a reconnect to a gone stream gets one SNAPSHOT with only the newer messages, on the same connection", func(t *testing.T) {
		h, streams := streamRuntime(t, scripted(step{text: "one"}, step{text: "two"}))
		chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{Name: "chat", Model: "gpt-4o"})
		ran := h.run(t, chat, agentenkit.RunInput{Prompt: "a"})
		h.handleNext(t)
		snap, err := h.rt.GetThreadSnapshot(h.ctx, ran.ThreadID, nil)
		must(t, err)
		had := snap.Messages[len(snap.Messages)-1].ID
		second := h.run(t, chat, agentenkit.RunInput{ThreadID: ran.ThreadID, Prompt: "b"})
		h.handleNext(t)
		must(t, streams.Delete(h.ctx, ran.RunID+":1")) // past its grace window

		ctx, cancel := context.WithCancel(h.ctx)
		defer cancel()
		stream, err := h.rt.Events.Follow(ctx, ran.ThreadID, agentenkit.FollowStateOptions{
			Cursor: "0 " + ran.RunID + ":1 1", LastMessageID: had,
		})
		must(t, err)
		frames := readUntil(t, stream, func(f []core.FollowFrame) bool {
			for _, x := range f {
				if x.Kind == core.FrameKindSnapshot {
					return true
				}
			}
			return false
		})
		var got *ports.ThreadSnapshot
		for _, f := range frames {
			if f.Kind == core.FrameKindSnapshot {
				got = f.Snapshot
			}
		}
		var roles []string
		for _, m := range got.Messages {
			roles = append(roles, string(m.Role))
		}
		mustStrings(t, roles, []string{"user", "assistant"}, "run b's messages only")
		mustEqual(t, got.Stream.StreamID, second.RunID+":1", "and run b's stream")
	})

	t.Run("the sse id carries the record seq and the stream position", func(t *testing.T) {
		at := core.ThreadCursor{Seq: 4}
		thread := core.FollowFrameSSE(core.FollowFrame{Kind: core.FrameKindThread, Event: &ports.AgentEvent{ThreadID: "t", Seq: 5, Type: "X"}}, &at)
		if !strings.HasPrefix(thread, "id: 5 - -\n") {
			t.Fatalf("thread frame: %q", thread)
		}
		item := core.FollowFrameSSE(core.FollowFrame{Kind: core.FrameKindStream, StreamID: "r1:2", Item: &ports.StreamItem{
			Offset: "9", Event: &ports.TextMessageContentEvent{MessageID: "m", Delta: "d"},
		}}, &at)
		if !strings.HasPrefix(item, "id: 5 r1:2 9\n") {
			t.Fatalf("stream frame: %q", item)
		}
		notice := core.FollowFrameSSE(core.FollowFrame{Kind: core.FrameKindThread, Event: &ports.AgentEvent{ThreadID: "t", Type: "HEARTBEAT"}}, &at)
		if strings.Contains(notice, "id:") {
			t.Fatalf("a notice leaves the browser's cursor as it was: %q", notice)
		}
	})

	t.Run("a cursor reads its wire form and a bare seq", func(t *testing.T) {
		c, ok := core.ParseCursor("5 r1:2 9")
		mustEqual(t, ok && c == core.ThreadCursor{Seq: 5, StreamID: "r1:2", Offset: "9"}, true, "full")
		c, ok = core.ParseCursor("5 - -")
		mustEqual(t, ok && c == core.ThreadCursor{Seq: 5}, true, "record only")
		c, ok = core.ParseCursor("12")
		mustEqual(t, ok && c == core.ThreadCursor{Seq: 12}, true, "bare seq")
		_, ok = core.ParseCursor("junk")
		mustEqual(t, ok, false, "junk")
		mustEqual(t, core.FormatCursor(core.ThreadCursor{Seq: 3, StreamID: "r:1", Offset: "1-0"}), "3 r:1 1-0", "format")
	})

	t.Run("stream content does not go on the bus while a segment is open", func(t *testing.T) {
		h, _ := streamRuntime(t, scripted(step{text: "hello"}))
		chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{Name: "chat", Model: "gpt-4o"})
		ran := h.run(t, chat, agentenkit.RunInput{Prompt: "a"})
		h.handleNext(t)
		var onBus []string
		for _, e := range h.events(ran.ThreadID, "") {
			onBus = append(onBus, e.Type)
		}
		joined := strings.Join(onBus, " ")
		if strings.Contains(joined, "CHUNK") || strings.Contains(joined, "STEP_COMMITTED") {
			t.Fatalf("stream content on the bus: %v", onBus)
		}
		if !strings.Contains(joined, "STATE_CHANGE") || !strings.Contains(joined, "RUN_STARTED") {
			t.Fatalf("thread notices missing: %v", onBus)
		}
	})
}
