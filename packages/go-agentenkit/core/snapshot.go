package core

import (
	"context"
	"encoding/json"
	"time"

	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

// RecentEnd is how long after its end a stream still rides on the snapshot.
const RecentEnd = time.Minute

// contentEvents are the events that are part of a step's content, and so
// of its saved messages.
var contentEvents = map[string]bool{
	ports.EventTextMessageStart: true, ports.EventTextMessageContent: true, ports.EventTextMessageEnd: true,
	ports.EventReasoningStart: true, ports.EventReasoningContent: true, ports.EventReasoningEnd: true,
	ports.EventToolCallStart: true, ports.EventToolCallArgs: true, ports.EventToolCallEnd: true,
	ports.EventToolCallResult: true, ports.EventSource: true,
}

// ownerOf is whose step an item belongs to ("" the main agent), and
// whether it is content.
func ownerOf(item ports.StreamItem) (string, bool) {
	if w, ok := item.Event.(*ports.SubagentEventEvent); ok {
		return w.SubagentID, contentEvents[w.Event.StreamEventType()]
	}
	return "", contentEvents[item.Event.StreamEventType()]
}

// CutCommitted drops each agent's content up to its last finished step
// (see ports.SnapshotStream.Items).
func CutCommitted(items []ports.StreamItem) []ports.StreamItem {
	lastStep := map[string]int{}
	for i, item := range items {
		if s, ok := item.Event.(*ports.StepFinishedEvent); ok {
			agent := ""
			if s.AgentID != nil {
				agent = *s.AgentID
			}
			lastStep[agent] = i
		}
	}
	out := []ports.StreamItem{}
	for i, item := range items {
		agent, content := ownerOf(item)
		last, ok := lastStep[agent]
		if !content || !ok || i > last {
			out = append(out, item)
		}
	}
	return out
}

// SnapshotStreamOf is the thread's current run stream, from the thread
// record: the latest segment that started, while it is open or only just
// ended. Nil when there is none, or it is gone.
func SnapshotStreamOf(ctx context.Context, deps ports.RuntimePorts, threadID string) (*ports.SnapshotStream, error) {
	if deps.Streams == nil {
		return nil, nil
	}
	started, err := deps.Storage.Events.Latest(ctx, threadID, "RUN_STARTED")
	if err != nil || started == nil {
		return nil, err
	}
	var at struct {
		StreamID string `json:"streamId"`
		RunID    string `json:"runId"`
	}
	_ = json.Unmarshal(started.Payload, &at)
	ended, err := deps.Storage.Events.Latest(ctx, threadID, "RUN_ENDED")
	if err != nil {
		return nil, err
	}
	if ended != nil && ended.Seq > started.Seq {
		var end struct {
			StreamID string `json:"streamId"`
		}
		_ = json.Unmarshal(ended.Payload, &end)
		if end.StreamID == at.StreamID && time.Since(ended.CreatedAt) > RecentEnd {
			return nil, nil
		}
	}
	snap, err := deps.Streams.Snapshot(ctx, at.StreamID, "")
	if err != nil || snap == nil {
		return nil, err
	}
	out := &ports.SnapshotStream{StreamID: at.StreamID, RunID: at.RunID, Items: CutCommitted(snap.Items), End: snap.End}
	if n := len(snap.Items); n > 0 {
		out.Offset = snap.Items[n-1].Offset
	}
	return out, nil
}
