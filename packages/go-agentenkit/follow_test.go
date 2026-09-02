package agentenkit_test

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	agentenkit "github.com/eadwinCode/agentic-kit/packages/go-agentenkit"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/adapters/memory"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/core"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

func publishN(t *testing.T, h *harness, threadID string, types ...string) []agentenkit.AgentEvent {
	t.Helper()
	var out []agentenkit.AgentEvent
	for _, typ := range types {
		e, err := core.Publish(h.ctx, h.rt.Ports(nil), threadID, typ, map[string]any{"t": typ})
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, e)
	}
	return out
}

func recv(t *testing.T, ch <-chan agentenkit.AgentEvent) agentenkit.AgentEvent {
	t.Helper()
	select {
	case e, ok := <-ch:
		if !ok {
			t.Fatal("stream closed")
		}
		return e
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for an event")
	}
	return agentenkit.AgentEvent{}
}

func TestFollow_ReplaysTheDurableLogThenGoesLiveInOrder(t *testing.T) {
	h := makeRuntime(t, scripted())
	publishN(t, h, "th", "A", "B")
	ctx, cancel := context.WithCancel(h.ctx)
	defer cancel()
	stream, err := h.rt.Events.Follow(ctx, "th", agentenkit.FollowStateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	mustEqual(t, recv(t, stream.Events()).Type, "A", "first")
	mustEqual(t, recv(t, stream.Events()).Type, "B", "second")
	publishN(t, h, "th", "C")
	mustEqual(t, recv(t, stream.Events()).Type, "C", "live")
}

func TestFollow_ResumesAfterACursorAndNeverResends(t *testing.T) {
	h := makeRuntime(t, scripted())
	evs := publishN(t, h, "th", "A", "B", "C")
	ctx, cancel := context.WithCancel(h.ctx)
	defer cancel()
	stream, _ := h.rt.Events.Follow(ctx, "th", agentenkit.FollowStateOptions{FollowOptions: agentenkit.FollowOptions{Since: evs[1].Seq}})
	mustEqual(t, recv(t, stream.Events()).Type, "C", "only what the client lacks")
	// A stale republish at or below the cursor is dropped
	_ = h.bus.Publish(h.ctx, "th", evs[0])
	publishN(t, h, "th", "D")
	mustEqual(t, recv(t, stream.Events()).Type, "D", "stale event skipped")
}

// replayHook publishes an event on the bus while the replay is still
// running: the window a route handler that subscribes late would lose.
type replayHook struct {
	agentenkit.Storage
	bus   *memory.Bus
	fired bool
}

type hookedEvents struct {
	ports.EventStore
	h *replayHook
}

func (r *replayHook) Events() ports.EventStore { return hookedEvents{r.Storage.Events(), r} }

func (e hookedEvents) ListSince(ctx context.Context, threadID string, since int64, sc ports.StorageContext) ([]ports.AgentEvent, error) {
	rows, err := e.EventStore.ListSince(ctx, threadID, since, sc)
	if !e.h.fired {
		e.h.fired = true
		_ = e.h.bus.Publish(ctx, threadID, ports.AgentEvent{ThreadID: threadID, Seq: 99, Type: "MID_REPLAY", CreatedAt: time.Now()})
	}
	return rows, err
}

func TestFollow_DoesNotDropAnEventPublishedDuringTheReplay(t *testing.T) {
	h := makeRuntime(t, scripted())
	publishN(t, h, "th", "A")
	hooked := &replayHook{Storage: h.storage, bus: h.bus}
	deps := h.rt.Ports(nil)
	deps.Storage = agentenkit.BindStorage(hooked, agentenkit.StorageContext{})
	ctx, cancel := context.WithCancel(h.ctx)
	defer cancel()
	stream, err := core.FollowEvents(ctx, deps, "th", agentenkit.FollowOptions{})
	if err != nil {
		t.Fatal(err)
	}
	mustEqual(t, recv(t, stream.Events()).Type, "A", "replay")
	mustEqual(t, recv(t, stream.Events()).Type, "MID_REPLAY", "the event behind the replay")
}

func TestFollow_ForwardsANoticeWithoutMovingTheCursor(t *testing.T) {
	h := makeRuntime(t, scripted())
	ctx, cancel := context.WithCancel(h.ctx)
	defer cancel()
	stream, _ := h.rt.Events.Follow(ctx, "th", agentenkit.FollowStateOptions{})
	evs := publishN(t, h, "th", "A")
	mustEqual(t, recv(t, stream.Events()).Type, "A", "A")
	_ = core.PublishNotice(h.ctx, h.rt.Ports(nil), "th", "HEARTBEAT", nil)
	mustEqual(t, recv(t, stream.Events()).Seq, int64(0), "notice forwarded")
	// The cursor still sits at A: a replay of A is dropped, B goes through
	_ = h.bus.Publish(h.ctx, "th", evs[0])
	publishN(t, h, "th", "B")
	mustEqual(t, recv(t, stream.Events()).Type, "B", "B")
}

func TestFollow_UnsubscribesWhenTheRequestIsCancelled(t *testing.T) {
	h := makeRuntime(t, scripted())
	ctx, cancel := context.WithCancel(h.ctx)
	stream, _ := h.rt.Events.Follow(ctx, "th", agentenkit.FollowStateOptions{})
	mustEqual(t, h.bus.Subscribers("th"), 1, "subscribed")
	cancel()
	for range stream.Events() {
	}
	mustEqual(t, h.bus.Subscribers("th"), 0, "unsubscribed")
	if stream.Err() != nil {
		t.Fatalf("cancellation is not an error: %v", stream.Err())
	}
}

func TestSSE_EncodesFramesWithTheSeqAsTheID(t *testing.T) {
	h := makeRuntime(t, scripted())
	evs := publishN(t, h, "th", "A")
	frame := agentenkit.SSEFrame(evs[0])
	if !strings.HasPrefix(frame, "id: 1\ndata: ") || !strings.HasSuffix(frame, "\n\n") {
		t.Fatalf("frame: %q", frame)
	}
	notice := agentenkit.SSEFrame(agentenkit.AgentEvent{ThreadID: "th", Seq: 0, Type: "HEARTBEAT"})
	if strings.Contains(notice, "id:") {
		t.Fatalf("a notice must carry no id: %q", notice)
	}
	mustEqual(t, agentenkit.SSEHeaders["Content-Type"], "text/event-stream; charset=utf-8", "content type")
	mustEqual(t, agentenkit.SSEHeaders["X-Accel-Buffering"], "no", "nginx")
}

func TestSSE_ServesRetryThenFramesAndUnsubscribesOnHangUp(t *testing.T) {
	h := makeRuntime(t, scripted())
	publishN(t, h, "th", "A")
	ctx, cancel := context.WithCancel(h.ctx)
	stream, err := h.rt.Events.SSE(ctx, "th", agentenkit.SSEStateOptions{SSEOptions: agentenkit.SSEOptions{RetryMs: 3000}})
	if err != nil {
		t.Fatal(err)
	}
	rec := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		stream.ServeHTTP(rec, httptest.NewRequest("GET", "/events", nil))
		close(done)
	}()
	time.Sleep(20 * time.Millisecond)
	cancel() // the client hung up
	<-done
	body := rec.Body.String()
	if !strings.HasPrefix(body, "retry: 3000\n\n") {
		t.Fatalf("body: %q", body)
	}
	if !strings.Contains(body, "id: 1\n") {
		t.Fatalf("frame missing: %q", body)
	}
	mustEqual(t, rec.Header().Get("Content-Type"), "text/event-stream; charset=utf-8", "header")
	mustEqual(t, h.bus.Subscribers("th"), 0, "unsubscribed on hang-up")
}
