package ports

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"strconv"
	"strings"
	"time"
)

// RunStreams holds short-lived logs, one per run segment. Delivery lives
// here, not state: a stream is closed when its segment ends and deleted
// after a grace window. What must outlive the run is in the messages and
// the thread record.
//
// Every adapter keeps these rules:
//   - Offsets are opaque strings. Only the store compares them; a client
//     echoes the last one back and nothing else.
//   - Offsets strictly increase within a stream. One writer per stream: the
//     run lock already makes sure of that.
//   - Close writes the end as the last item, so a reader that sees it
//     stops, and one that comes late still gets it.
//   - A missed wake-up only delays a reader, never drops an event.
type RunStreams interface {
	// Open opens a stream. Opening one that exists is a no-op. ttl deletes
	// it even if it is never closed.
	Open(ctx context.Context, streamID string, meta StreamMeta, ttl time.Duration) error
	// Append appends in order and returns one offset per event. It fails
	// with ErrStreamClosed once closed and ErrStreamGone when the stream
	// does not exist.
	Append(ctx context.Context, streamID string, events []StreamEvent) ([]string, error)
	// Read yields the items after `after` ("" is from the start), then live
	// ones as they come. It ends after the end item, or when ctx is done
	// (quietly, with no error). It yields ErrStreamGone when the stream does
	// not exist, or goes while read.
	Read(ctx context.Context, streamID, after string) iter.Seq2[StreamItem, error]
	// Snapshot returns the items after `after` now, without waiting. Nil,
	// nil when the stream does not exist.
	Snapshot(ctx context.Context, streamID, after string) (*StreamSnapshot, error)
	// Close writes the end item and starts the grace window: the stream is
	// deleted grace later. Closing a closed stream is a no-op; closing one
	// that does not exist fails with ErrStreamGone.
	Close(ctx context.Context, streamID string, end StreamEnd, grace time.Duration) error
	// Delete removes a stream now (thread delete). A missing one is a no-op.
	Delete(ctx context.Context, streamID string) error
}

// ErrStreamGone reports a stream that does not exist: never opened, past its
// grace window, or deleted. A reader takes a fresh snapshot of the thread
// instead.
var ErrStreamGone = errors.New("run stream is gone")

// ErrStreamClosed reports an append to a stream that is already closed.
var ErrStreamClosed = errors.New("run stream is closed")

// StreamMeta is who a stream belongs to.
type StreamMeta struct {
	ThreadID string `json:"threadId"`
	RunID    string `json:"runId"`
}

// StreamSnapshot is a stream as it stands now.
type StreamSnapshot struct {
	Meta  StreamMeta   `json:"meta"`
	Items []StreamItem `json:"items"`
	// End is the end item when the stream is closed; it is also the last of
	// Items.
	End StreamEnd `json:"end"`
}

// StreamIDOf is a run segment's stream id: "<runID>:<segment>". A park
// closes a segment's stream and the resume opens the next, so a closed
// stream never opens again.
func StreamIDOf(runID string, segment int) string {
	return runID + ":" + strconv.Itoa(segment)
}

// ParseStreamID returns the run id and segment in a stream id, and false for
// one not made by StreamIDOf.
func ParseStreamID(streamID string) (runID string, segment int, ok bool) {
	at := strings.LastIndex(streamID, ":")
	if at <= 0 {
		return "", 0, false
	}
	n, err := strconv.Atoi(streamID[at+1:])
	if err != nil || n < 1 {
		return "", 0, false
	}
	return streamID[:at], n, true
}

// StreamEvent is one event a run stream carries. The concrete types below
// are the whole set; their JSON is the same as the TS runtime's, "type"
// first. The shapes follow AG-UI, so an AG-UI encoder only renames a few
// fields.
type StreamEvent interface {
	StreamEventType() string
}

// StreamEnd is how a stream ends: *RunFinishedEvent or *RunErrorEvent.
type StreamEnd interface {
	StreamEvent
	streamEnd()
}

// IsStreamEnd reports whether e ends a stream.
func IsStreamEnd(e StreamEvent) bool {
	_, ok := e.(StreamEnd)
	return ok
}

// StreamItem is an event as a stream holds it: with the offset the store
// gave it. Its JSON is the event's with "offset" added.
type StreamItem struct {
	Offset string
	Event  StreamEvent
}

// StreamUsage is the tokens a step or a run used.
type StreamUsage struct {
	InputTokens       int64 `json:"inputTokens"`
	CachedInputTokens int64 `json:"cachedInputTokens"`
	OutputTokens      int64 `json:"outputTokens"`
	TotalTokens       int64 `json:"totalTokens"`
}

// StreamCost is money spent in one currency, in millionths of a unit.
type StreamCost struct {
	Currency string `json:"currency"`
	Micros   int64  `json:"micros"`
}

// Event types.
const (
	EventRunStarted         = "RUN_STARTED"
	EventTextMessageStart   = "TEXT_MESSAGE_START"
	EventTextMessageContent = "TEXT_MESSAGE_CONTENT"
	EventTextMessageEnd     = "TEXT_MESSAGE_END"
	EventReasoningStart     = "REASONING_START"
	EventReasoningContent   = "REASONING_CONTENT"
	EventReasoningEnd       = "REASONING_END"
	EventToolCallStart      = "TOOL_CALL_START"
	EventToolCallArgs       = "TOOL_CALL_ARGS"
	EventToolCallEnd        = "TOOL_CALL_END"
	EventToolCallResult     = "TOOL_CALL_RESULT"
	EventSource             = "SOURCE"
	EventStepFinished       = "STEP_FINISHED"
	EventMessageAppended    = "MESSAGE_APPENDED"
	EventInputRequired      = "INPUT_REQUIRED"
	EventSubagentStarted    = "SUBAGENT_STARTED"
	EventSubagentEvent      = "SUBAGENT_EVENT"
	EventSubagentFinished   = "SUBAGENT_FINISHED"
	EventCustom             = "CUSTOM"
	EventRunFinished        = "RUN_FINISHED"
	EventRunError           = "RUN_ERROR"
)

// RunStartedEvent opens every stream.
type RunStartedEvent struct {
	ThreadID string `json:"threadId"`
	RunID    string `json:"runId"`
	StreamID string `json:"streamId"`
	// Segment is 1 for the first pickup, 2 for the resume after a park, and
	// so on.
	Segment int `json:"segment"`
}

type TextMessageStartEvent struct {
	MessageID string `json:"messageId"`
	Role      string `json:"role"`
}

type TextMessageContentEvent struct {
	MessageID string `json:"messageId"`
	Delta     string `json:"delta"`
}

type TextMessageEndEvent struct {
	MessageID string `json:"messageId"`
}

type ReasoningStartEvent struct {
	MessageID string `json:"messageId"`
}

type ReasoningContentEvent struct {
	MessageID string `json:"messageId"`
	Delta     string `json:"delta"`
}

type ReasoningEndEvent struct {
	MessageID string `json:"messageId"`
}

type ToolCallStartEvent struct {
	ToolCallID string `json:"toolCallId"`
	ToolName   string `json:"toolName"`
}

type ToolCallArgsEvent struct {
	ToolCallID string `json:"toolCallId"`
	Delta      string `json:"delta"`
}

// ToolCallEndEvent says the call is complete. Args is the whole, parsed
// argument object, so a reader that missed the ARGS deltas still has it.
type ToolCallEndEvent struct {
	ToolCallID string          `json:"toolCallId"`
	ToolName   string          `json:"toolName"`
	Args       json.RawMessage `json:"args"`
}

type ToolCallResultEvent struct {
	ToolCallID string          `json:"toolCallId"`
	ToolName   string          `json:"toolName"`
	Result     json.RawMessage `json:"result"`
}

// SourceEvent is a source the model cited. Not in AG-UI: the encoder sends
// it as CUSTOM.
type SourceEvent struct {
	Source json.RawMessage `json:"source"`
}

// StepFinishedEvent says one model call is done and its messages are saved.
type StepFinishedEvent struct {
	// Step is 1-based, per agent.
	Step int `json:"step"`
	// AgentID is nil for the main agent, the nested run's id otherwise.
	AgentID      *string     `json:"agentId"`
	FinishReason string      `json:"finishReason"`
	Usage        StreamUsage `json:"usage"`
}

// StreamMessage is a saved message as MESSAGE_APPENDED carries it.
type StreamMessage struct {
	ID        string          `json:"id"`
	Role      string          `json:"role"`
	Content   json.RawMessage `json:"content"`
	AgentID   *string         `json:"agentId"`
	CreatedAt string          `json:"createdAt"`
}

type MessageAppendedEvent struct {
	Message StreamMessage `json:"message"`
	// ClientMessageID is the sender's own name for the message, echoed back.
	ClientMessageID string `json:"clientMessageId,omitempty"`
}

// InputRequiredEvent says a tool is waiting for a person. Also kept in the
// thread record.
type InputRequiredEvent struct {
	ToolCallID  string          `json:"toolCallId"`
	ToolName    string          `json:"toolName"`
	AgentID     *string         `json:"agentId"`
	Arguments   json.RawMessage `json:"arguments"`
	InputSchema json.RawMessage `json:"inputSchema"`
	// Frames is the unwind chain for a nested park; empty for the main agent.
	Frames json.RawMessage `json:"frames"`
	Nested json.RawMessage `json:"nested,omitempty"`
	// Resume is the dispatch ticket the answer resumes with.
	Resume    json.RawMessage `json:"resume,omitempty"`
	Reason    string          `json:"reason"`
	ExpiresAt string          `json:"expiresAt,omitempty"`
}

type SubagentStartedEvent struct {
	SubagentID string `json:"subagentId"`
	Name       string `json:"name"`
	Depth      int    `json:"depth"`
}

// SubagentEventEvent is one event from a nested run, wrapped.
type SubagentEventEvent struct {
	SubagentID string
	Event      StreamEvent
}

type SubagentFinishedEvent struct {
	SubagentID string `json:"subagentId"`
	// Status is "completed" or "failed".
	Status string `json:"status"`
	Error  string `json:"error,omitempty"`
}

// CustomEvent is an app's own event, sent with PublishEvent.
type CustomEvent struct {
	Name  string          `json:"name"`
	Value json.RawMessage `json:"value"`
}

// RunFinishedEvent says the run's segment ended normally. Always the last
// item of a stream.
type RunFinishedEvent struct {
	// Status is "finished", "parked" or "stopped".
	Status       string       `json:"status"`
	Usage        *StreamUsage `json:"usage,omitempty"`
	Costs        []StreamCost `json:"costs,omitempty"`
	FinishReason string       `json:"finishReason,omitempty"`
	// Text is the final text of a one-shot agent.
	Text string `json:"text,omitempty"`
}

// RunErrorEvent says the run's segment ended badly. Always the last item of
// a stream.
type RunErrorEvent struct {
	// Status is "error", or "lost" when the worker died and the sweep closed
	// its stream.
	Status string `json:"status"`
	Error  string `json:"error"`
}

func (*RunStartedEvent) StreamEventType() string         { return EventRunStarted }
func (*TextMessageStartEvent) StreamEventType() string   { return EventTextMessageStart }
func (*TextMessageContentEvent) StreamEventType() string { return EventTextMessageContent }
func (*TextMessageEndEvent) StreamEventType() string     { return EventTextMessageEnd }
func (*ReasoningStartEvent) StreamEventType() string     { return EventReasoningStart }
func (*ReasoningContentEvent) StreamEventType() string   { return EventReasoningContent }
func (*ReasoningEndEvent) StreamEventType() string       { return EventReasoningEnd }
func (*ToolCallStartEvent) StreamEventType() string      { return EventToolCallStart }
func (*ToolCallArgsEvent) StreamEventType() string       { return EventToolCallArgs }
func (*ToolCallEndEvent) StreamEventType() string        { return EventToolCallEnd }
func (*ToolCallResultEvent) StreamEventType() string     { return EventToolCallResult }
func (*SourceEvent) StreamEventType() string             { return EventSource }
func (*StepFinishedEvent) StreamEventType() string       { return EventStepFinished }
func (*MessageAppendedEvent) StreamEventType() string    { return EventMessageAppended }
func (*InputRequiredEvent) StreamEventType() string      { return EventInputRequired }
func (*SubagentStartedEvent) StreamEventType() string    { return EventSubagentStarted }
func (*SubagentEventEvent) StreamEventType() string      { return EventSubagentEvent }
func (*SubagentFinishedEvent) StreamEventType() string   { return EventSubagentFinished }
func (*CustomEvent) StreamEventType() string             { return EventCustom }
func (*RunFinishedEvent) StreamEventType() string        { return EventRunFinished }
func (*RunErrorEvent) StreamEventType() string           { return EventRunError }

func (*RunFinishedEvent) streamEnd() {}
func (*RunErrorEvent) streamEnd()    {}

// newStreamEvent makes an empty event of a type, for decoding.
func newStreamEvent(typ string) (StreamEvent, bool) {
	switch typ {
	case EventRunStarted:
		return &RunStartedEvent{}, true
	case EventTextMessageStart:
		return &TextMessageStartEvent{}, true
	case EventTextMessageContent:
		return &TextMessageContentEvent{}, true
	case EventTextMessageEnd:
		return &TextMessageEndEvent{}, true
	case EventReasoningStart:
		return &ReasoningStartEvent{}, true
	case EventReasoningContent:
		return &ReasoningContentEvent{}, true
	case EventReasoningEnd:
		return &ReasoningEndEvent{}, true
	case EventToolCallStart:
		return &ToolCallStartEvent{}, true
	case EventToolCallArgs:
		return &ToolCallArgsEvent{}, true
	case EventToolCallEnd:
		return &ToolCallEndEvent{}, true
	case EventToolCallResult:
		return &ToolCallResultEvent{}, true
	case EventSource:
		return &SourceEvent{}, true
	case EventStepFinished:
		return &StepFinishedEvent{}, true
	case EventMessageAppended:
		return &MessageAppendedEvent{}, true
	case EventInputRequired:
		return &InputRequiredEvent{}, true
	case EventSubagentStarted:
		return &SubagentStartedEvent{}, true
	case EventSubagentEvent:
		return &SubagentEventEvent{}, true
	case EventSubagentFinished:
		return &SubagentFinishedEvent{}, true
	case EventCustom:
		return &CustomEvent{}, true
	case EventRunFinished:
		return &RunFinishedEvent{}, true
	case EventRunError:
		return &RunErrorEvent{}, true
	}
	return nil, false
}

// EncodeStreamEvent is an event's JSON: "type" first, then its fields, the
// same bytes the TS runtime writes for it.
func EncodeStreamEvent(e StreamEvent) ([]byte, error) {
	return encodeWith(e, "")
}

// encodeWith writes the event with "type" first and, when offset is set,
// "offset" last.
func encodeWith(e StreamEvent, offset string) ([]byte, error) {
	var body []byte
	var err error
	if w, ok := e.(*SubagentEventEvent); ok {
		inner, ierr := EncodeStreamEvent(w.Event)
		if ierr != nil {
			return nil, ierr
		}
		sid, _ := json.Marshal(w.SubagentID)
		body = []byte(`{"subagentId":` + string(sid) + `,"event":` + string(inner) + `}`)
	} else {
		body, err = json.Marshal(e)
		if err != nil {
			return nil, err
		}
	}
	typ, _ := json.Marshal(e.StreamEventType())
	var b strings.Builder
	b.WriteString(`{"type":`)
	b.Write(typ)
	if rest := strings.TrimSpace(string(body[1 : len(body)-1])); rest != "" {
		b.WriteString(",")
		b.WriteString(rest)
	}
	if offset != "" {
		o, _ := json.Marshal(offset)
		b.WriteString(`,"offset":`)
		b.Write(o)
	}
	b.WriteString("}")
	return []byte(b.String()), nil
}

// DecodeStreamEvent reads an event written by EncodeStreamEvent (or the TS
// runtime). An "offset" field, if any, is ignored.
func DecodeStreamEvent(data []byte) (StreamEvent, error) {
	var head struct {
		Type       string          `json:"type"`
		SubagentID string          `json:"subagentId"`
		Event      json.RawMessage `json:"event"`
	}
	if err := json.Unmarshal(data, &head); err != nil {
		return nil, err
	}
	e, ok := newStreamEvent(head.Type)
	if !ok {
		return nil, fmt.Errorf("unknown stream event type %q", head.Type)
	}
	if w, ok := e.(*SubagentEventEvent); ok {
		inner, err := DecodeStreamEvent(head.Event)
		if err != nil {
			return nil, err
		}
		w.SubagentID, w.Event = head.SubagentID, inner
		return w, nil
	}
	if err := json.Unmarshal(data, e); err != nil {
		return nil, err
	}
	return e, nil
}

// MarshalJSON writes the event's JSON with "offset" added.
func (i StreamItem) MarshalJSON() ([]byte, error) { return encodeWith(i.Event, i.Offset) }

// UnmarshalJSON reads an item written by MarshalJSON.
func (i *StreamItem) UnmarshalJSON(data []byte) error {
	var o struct {
		Offset string `json:"offset"`
	}
	if err := json.Unmarshal(data, &o); err != nil {
		return err
	}
	e, err := DecodeStreamEvent(data)
	if err != nil {
		return err
	}
	i.Offset, i.Event = o.Offset, e
	return nil
}

// MarshalJSON writes the wrapped event whole.
func (w *SubagentEventEvent) MarshalJSON() ([]byte, error) { return encodeWith(w, "") }

// MarshalJSON writes End as its event JSON, or null.
func (s StreamSnapshot) MarshalJSON() ([]byte, error) {
	var end json.RawMessage = []byte("null")
	if s.End != nil {
		b, err := EncodeStreamEvent(s.End)
		if err != nil {
			return nil, err
		}
		end = b
	}
	items := s.Items
	if items == nil {
		items = []StreamItem{}
	}
	return json.Marshal(struct {
		Meta  StreamMeta      `json:"meta"`
		Items []StreamItem    `json:"items"`
		End   json.RawMessage `json:"end"`
	}{s.Meta, items, end})
}
