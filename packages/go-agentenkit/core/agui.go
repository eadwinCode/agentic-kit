package core

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

// Wire formats a follow can be sent in: our own frames (the default), or
// AG-UI events for a client built on that protocol.
const (
	WireAgentenkit = "agentenkit"
	WireAgUI       = "ag-ui"
)

// AgUIState is what the AG-UI encoder remembers across frames: the tool
// calls whose arguments already went out as deltas.
type AgUIState struct {
	ThreadID string
	argsSent map[string]bool
}

// NewAgUIState starts the encoder's memory for one follow.
func NewAgUIState(threadID string) *AgUIState {
	return &AgUIState{ThreadID: threadID, argsSent: map[string]bool{}}
}

// ToAgUI is a follow frame as AG-UI events. Our stream events already follow
// AG-UI, so most keep their shape and only a few fields are renamed; what
// AG-UI has no event for (a park, a subagent, a thread notice, a snapshot)
// goes out as CUSTOM with our name. The TS runtime sends the same
// (src/core/agui.ts).
func ToAgUI(f FollowFrame, st *AgUIState) ([]map[string]any, error) {
	custom := func(name string, value any) []map[string]any {
		if value == nil {
			value = json.RawMessage("null")
		}
		return []map[string]any{{"type": "CUSTOM", "name": name, "value": value}}
	}
	switch f.Kind {
	case FrameKindThread:
		return custom(f.Event.Type, f.Event.Payload), nil
	case FrameKindSnapshot:
		return custom("SNAPSHOT", f.Snapshot), nil
	}
	raw, err := ports.EncodeStreamEvent(f.Item.Event)
	if err != nil {
		return nil, err
	}
	var e map[string]any
	if err := json.Unmarshal(raw, &e); err != nil {
		return nil, err
	}
	runID := f.StreamID
	if id, _, ok := ports.ParseStreamID(f.StreamID); ok {
		runID = id
	}
	typ, _ := e["type"].(string)
	switch typ {
	case ports.EventRunStarted:
		return []map[string]any{{"type": "RUN_STARTED", "threadId": st.ThreadID, "runId": runID}}, nil
	case ports.EventTextMessageStart, ports.EventTextMessageContent, ports.EventTextMessageEnd:
		return []map[string]any{e}, nil
	case ports.EventReasoningStart:
		return []map[string]any{{"type": "THINKING_TEXT_MESSAGE_START"}}, nil
	case ports.EventReasoningContent:
		return []map[string]any{{"type": "THINKING_TEXT_MESSAGE_CONTENT", "delta": e["delta"]}}, nil
	case ports.EventReasoningEnd:
		return []map[string]any{{"type": "THINKING_TEXT_MESSAGE_END"}}, nil
	case ports.EventToolCallStart:
		return []map[string]any{{"type": "TOOL_CALL_START", "toolCallId": e["toolCallId"], "toolCallName": e["toolName"]}}, nil
	case ports.EventToolCallArgs:
		st.argsSent[fmt.Sprint(e["toolCallId"])] = true
		return []map[string]any{{"type": "TOOL_CALL_ARGS", "toolCallId": e["toolCallId"], "delta": e["delta"]}}, nil
	case ports.EventToolCallEnd:
		// AG-UI builds the arguments from ARGS deltas alone: a call whose
		// arguments came whole sends them as one delta first.
		id := fmt.Sprint(e["toolCallId"])
		var out []map[string]any
		if !st.argsSent[id] {
			args := e["args"]
			if args == nil {
				args = map[string]any{}
			}
			b, _ := json.Marshal(args)
			out = append(out, map[string]any{"type": "TOOL_CALL_ARGS", "toolCallId": id, "delta": string(b)})
		}
		delete(st.argsSent, id)
		return append(out, map[string]any{"type": "TOOL_CALL_END", "toolCallId": id}), nil
	case ports.EventToolCallResult:
		content, ok := e["result"].(string)
		if !ok {
			b, _ := json.Marshal(e["result"])
			content = string(b)
		}
		return []map[string]any{{
			"type": "TOOL_CALL_RESULT", "messageId": fmt.Sprint(e["toolCallId"]) + ":result",
			"toolCallId": e["toolCallId"], "content": content, "role": "tool",
		}}, nil
	case ports.EventStepFinished:
		return []map[string]any{{"type": "STEP_FINISHED", "stepName": fmt.Sprintf("step %v", e["step"])}}, nil
	case ports.EventCustom:
		return custom(fmt.Sprint(e["name"]), e["value"]), nil
	case ports.EventRunFinished:
		delete(e, "type")
		return []map[string]any{{"type": "RUN_FINISHED", "threadId": st.ThreadID, "runId": runID, "result": e}}, nil
	case ports.EventRunError:
		return []map[string]any{{"type": "RUN_ERROR", "message": e["error"], "code": e["status"]}}, nil
	}
	// A source, a saved message, a park, a subagent: ours, by name.
	delete(e, "type")
	return custom(typ, e), nil
}

// AgUIFrameSSE is a frame as AG-UI events, one SSE message each; the
// frame's cursor rides on the first, so a reconnect resumes just as with
// our own frames.
func AgUIFrameSSE(f FollowFrame, cursor *ThreadCursor, st *AgUIState) (string, error) {
	ours := FollowFrameSSE(f, cursor)
	id := ""
	if strings.HasPrefix(ours, "id: ") {
		id = ours[:strings.Index(ours, "\n")+1]
	}
	events, err := ToAgUI(f, st)
	if err != nil {
		return "", err
	}
	var b strings.Builder
	for i, e := range events {
		data, err := json.Marshal(e)
		if err != nil {
			return "", err
		}
		if i == 0 {
			b.WriteString(id)
		}
		b.WriteString("data: " + string(data) + "\n\n")
	}
	return b.String(), nil
}

// UnmarshalJSON reads a frame written by MarshalJSON (or the TS runtime).
func (f *FollowFrame) UnmarshalJSON(data []byte) error {
	var head struct {
		Kind     string                `json:"kind"`
		Event    *ports.AgentEvent     `json:"event"`
		StreamID string                `json:"streamId"`
		Item     *ports.StreamItem     `json:"item"`
		Snapshot *ports.ThreadSnapshot `json:"snapshot"`
	}
	if err := json.Unmarshal(data, &head); err != nil {
		return err
	}
	*f = FollowFrame{Kind: head.Kind, Event: head.Event, StreamID: head.StreamID, Item: head.Item, Snapshot: head.Snapshot}
	return nil
}
