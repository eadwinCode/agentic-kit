package agentenkit_test

import (
	"encoding/json"
	"testing"

	agentenkit "github.com/eadwinCode/agentic-kit/packages/go-agentenkit"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

// runItems is every item a run's streams hold, segment after segment.
func runItems(t *testing.T, h *harness, runID string) []ports.StreamItem {
	t.Helper()
	streams := h.rt.Ports(nil).Streams
	var out []ports.StreamItem
	for segment := 1; ; segment++ {
		snap, err := streams.Snapshot(h.ctx, ports.StreamIDOf(runID, segment), "")
		if err != nil {
			t.Fatal(err)
		}
		if snap == nil {
			return out
		}
		out = append(out, snap.Items...)
	}
}

// threadItems is every item on a thread's run streams, in run order: the
// thread record names each segment it had.
func threadItems(t *testing.T, h *harness, threadID string) []ports.StreamItem {
	t.Helper()
	deps := h.rt.Ports(nil)
	started, err := deps.Storage.Events.List(h.ctx, threadID, ports.ThreadEventFilter{Types: []string{"RUN_STARTED"}})
	if err != nil {
		t.Fatal(err)
	}
	var out []ports.StreamItem
	for _, e := range started {
		var p struct {
			StreamID string `json:"streamId"`
		}
		_ = json.Unmarshal(e.Payload, &p)
		snap, err := deps.Streams.Snapshot(h.ctx, p.StreamID, "")
		if err != nil {
			t.Fatal(err)
		}
		if snap != nil {
			out = append(out, snap.Items...)
		}
	}
	return out
}

// flat is the items' events with nested ones unwrapped after their wrapper.
func flat(items []ports.StreamItem) []ports.StreamEvent {
	var out []ports.StreamEvent
	for _, i := range items {
		out = append(out, i.Event)
		if w, ok := i.Event.(*ports.SubagentEventEvent); ok {
			out = append(out, w.Event)
		}
	}
	return out
}

type subagentEvents struct {
	started           []*ports.SubagentStartedEvent
	completed, failed []*ports.SubagentFinishedEvent
}

// subagentsOf is a thread's subagent starts and ends, from its run streams.
func subagentsOf(t *testing.T, h *harness, threadID string) subagentEvents {
	t.Helper()
	var s subagentEvents
	for _, e := range flat(threadItems(t, h, threadID)) {
		switch e := e.(type) {
		case *ports.SubagentStartedEvent:
			s.started = append(s.started, e)
		case *ports.SubagentFinishedEvent:
			if e.Status == "completed" {
				s.completed = append(s.completed, e)
			} else {
				s.failed = append(s.failed, e)
			}
		}
	}
	return s
}

// customOf is the CUSTOM events of a name on a thread's run streams.
func customOf(t *testing.T, h *harness, threadID, name string) []*ports.CustomEvent {
	t.Helper()
	var out []*ports.CustomEvent
	for _, e := range flat(threadItems(t, h, threadID)) {
		if c, ok := e.(*ports.CustomEvent); ok && c.Name == name {
			out = append(out, c)
		}
	}
	return out
}

var _ = agentenkit.StorageContext{}
