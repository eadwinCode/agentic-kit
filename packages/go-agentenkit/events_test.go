package agentenkit_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	agentenkit "github.com/eadwinCode/agentic-kit/packages/go-agentenkit"
	pgstorage "github.com/eadwinCode/agentic-kit/packages/go-agentenkit/adapters/postgres"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/core"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

// Workstream G: a follower never skips a seq, a lost seq counter carries on
// from the log, token deltas go out merged, and the platform's own event
// types stay reserved. The same cases run in the TS package
// (test/events.test.ts).

func TestFollow_AnEventTheBusSkippedIsReadBackFromTheLog(t *testing.T) {
	h := makeRuntime(t, scripted(step{text: "ok"}))
	deps := h.rt.Ports(nil)
	th, _ := h.storage.Threads().Create(h.ctx, ports.ThreadInit{}, agentenkit.StorageContext{})
	store := func(seq int64) ports.AgentEvent {
		e := ports.AgentEvent{ThreadID: th.ID, Seq: seq, Type: "X", Payload: json.RawMessage("null"), CreatedAt: time.Now()}
		if err := h.storage.Events().Append(h.ctx, th.ID, e, agentenkit.StorageContext{}); err != nil {
			t.Fatal(err)
		}
		return e
	}
	store(1)
	ctx, cancel := context.WithCancel(h.ctx)
	defer cancel()
	stream, err := core.FollowEvents(ctx, deps, th.ID, core.FollowOptions{})
	if err != nil {
		t.Fatal(err)
	}
	next := func() int64 {
		select {
		case e := <-stream.Events():
			return e.Seq
		case <-time.After(2 * time.Second):
			t.Fatal("no event")
			return 0
		}
	}
	mustEqual(t, next(), int64(1), "the replay")
	// Seq 2 is stored but its bus message is lost; seq 3 arrives live.
	store(2)
	e3 := store(3)
	_ = h.bus.Publish(h.ctx, th.ID, e3)
	mustEqual(t, next(), int64(2), "the gap is read back first")
	mustEqual(t, next(), int64(3), "then the live event")
}

func TestPublish_ALostSeqCounterCarriesOnFromTheLog(t *testing.T) {
	h := makeRuntime(t, scripted(step{text: "ok"}))
	deps := h.rt.Ports(nil)
	th, _ := h.storage.Threads().Create(h.ctx, ports.ThreadInit{}, agentenkit.StorageContext{})
	for i := 0; i < 3; i++ {
		if _, err := core.Publish(h.ctx, deps, th.ID, "X", nil); err != nil {
			t.Fatal(err)
		}
	}
	_ = h.kv.Del(h.ctx, core.SeqKey(th.ID)) // flushed, evicted, restarted
	e, err := core.Publish(h.ctx, deps, th.ID, "X", nil)
	if err != nil {
		t.Fatal(err)
	}
	mustEqual(t, e.Seq, int64(4), "past what the log holds, never back at 1")
}

func TestChunks_ConsecutiveDeltasGoOutAsOneEvent(t *testing.T) {
	h, chat := routedRuntime(t, func(string, bool) step {
		return step{text: "one two three", deltas: []string{"one ", "two ", "three"}}
	})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "go"})
	h.handleNext(t)
	var deltas []string
	for _, e := range h.events(ran.ThreadID, "CHUNK") {
		if p := payload(e); p["type"] == "text-delta" {
			deltas = append(deltas, p["textDelta"].(string))
		}
	}
	mustEqual(t, strings.Join(deltas, ""), "one two three", "the text arrives whole")
	mustEqual(t, len(deltas), 1, "as one event")
}

func TestPublish_AnAppCannotPublishTheCostBudgetEvent(t *testing.T) {
	h := makeRuntime(t, scripted(step{text: "ok"}))
	th, _ := h.storage.Threads().Create(h.ctx, ports.ThreadInit{}, agentenkit.StorageContext{})
	if _, err := h.rt.Events.PublishEvent(h.ctx, th.ID, "COST_BUDGET_EXHAUSTED", nil, agentenkit.PublishStateOptions{}); err == nil {
		t.Fatal("a platform event type is reserved")
	}
}

func TestPostgresBus_ASlowHandlerHoldsUpNobody(t *testing.T) {
	p := openPgPlatform(t, "sb_", pgstorage.QueueOptions{})
	ctx := context.Background()
	release := make(chan struct{})
	defer close(release)
	stopSlow, _ := p.bus.Subscribe(ctx, "pg-slow", func(agentenkit.AgentEvent) { <-release })
	defer stopSlow()
	got := make(chan struct{}, 1)
	stopFast, _ := p.bus.Subscribe(ctx, "pg-slow", func(agentenkit.AgentEvent) {
		select {
		case got <- struct{}{}:
		default:
		}
	})
	defer stopFast()
	time.Sleep(200 * time.Millisecond) // LISTEN is up
	_ = p.bus.Publish(ctx, "pg-slow", agentenkit.AgentEvent{ThreadID: "pg-slow", Seq: 0, Type: "X", Payload: json.RawMessage("null")})
	select {
	case <-got:
	case <-time.After(3 * time.Second):
		t.Fatal("the fast subscriber waited on the slow one")
	}
}
