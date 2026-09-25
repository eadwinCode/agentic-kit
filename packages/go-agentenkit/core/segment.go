package core

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

// A SegmentStream is the worker's side of a run stream: one per segment,
// open from pickup to the segment's end. Everything published on the
// thread while it is open is turned into typed stream events here (see
// Forward), so the engine keeps calling Publish and only opens and closes
// the segment.
//
// Events are appended together: they wait up to Config.StreamFlush, or
// until Config.StreamFlushEvents are waiting. A step end, a tool result, a
// park and a close go out at once. A failed append is logged, never
// returned: the stream is delivery, and the run's messages are saved either
// way. The TS runtime does the same (core/segment.ts).

// SegmentKey is the segment counter: each pickup of a run takes the next
// number, so a retry or a resume gets a stream of its own.
func SegmentKey(runID string) string { return "agent:segment:" + runID }

// openSegments holds the open segments by thread, per RunStreams, so two
// runtimes in one process never see each other's.
var openSegments = struct {
	sync.Mutex
	m map[ports.RunStreams]map[string]*SegmentStream
}{m: map[ports.RunStreams]map[string]*SegmentStream{}}

// ActiveSegment is the segment this process has open on a thread, or nil.
func ActiveSegment(deps ports.RuntimePorts, threadID string) *SegmentStream {
	if deps.Streams == nil {
		return nil
	}
	openSegments.Lock()
	defer openSegments.Unlock()
	return openSegments.m[deps.Streams][threadID]
}

var immediateEvents = map[string]bool{
	ports.EventRunStarted: true, ports.EventToolCallResult: true, ports.EventStepFinished: true,
	ports.EventInputRequired: true, ports.EventSubagentFinished: true,
}

func immediate(e ports.StreamEvent) bool {
	if w, ok := e.(*ports.SubagentEventEvent); ok {
		return immediate(w.Event)
	}
	return immediateEvents[e.StreamEventType()]
}

// blocks is the text, reasoning and tool calls one agent has open.
type blocks struct {
	text, reasoning string
	tools           map[string]bool
}

// OpenSegment opens the segment's stream and says RUN_STARTED on it. Nil
// when the runtime has no streams, or the open failed (logged): the run
// goes on without a stream.
func OpenSegment(ctx context.Context, deps ports.RuntimePorts, threadID, runID string) *SegmentStream {
	if deps.Streams == nil || runID == "" {
		return nil
	}
	n, err := deps.Kv.IncrWithExpiry(ctx, SegmentKey(runID), ThreadKeyTTL)
	if err == nil {
		streamID := ports.StreamIDOf(runID, int(n))
		err = deps.Streams.Open(ctx, streamID, ports.StreamMeta{ThreadID: threadID, RunID: runID}, deps.Config.StreamTTL)
		if err == nil {
			seg := &SegmentStream{deps: deps, streams: deps.Streams, ThreadID: threadID, StreamID: streamID, agents: map[string]*blocks{}}
			openSegments.Lock()
			if openSegments.m[deps.Streams] == nil {
				openSegments.m[deps.Streams] = map[string]*SegmentStream{}
			}
			openSegments.m[deps.Streams][threadID] = seg
			openSegments.Unlock()
			seg.Push(ctx, &ports.RunStartedEvent{ThreadID: threadID, RunID: runID, StreamID: streamID, Segment: int(n)})
			return seg
		}
	}
	Logger(deps).Error("run stream not opened", "thread", threadID, "run", runID, "err", err)
	return nil
}

// CloseLostSegment closes a segment's stream that its worker could not: the
// sweep's end for a run whose worker died. A stream already closed or gone
// is left as it is.
func CloseLostSegment(ctx context.Context, deps ports.RuntimePorts, runID, reason string) {
	if deps.Streams == nil {
		return
	}
	raw, ok, err := deps.Kv.Get(ctx, SegmentKey(runID))
	n, _ := strconv.Atoi(raw)
	if err != nil || !ok || n < 1 {
		return
	}
	_ = deps.Streams.Close(ctx, ports.StreamIDOf(runID, n), &ports.RunErrorEvent{Status: "lost", Error: reason}, deps.Config.StreamGrace)
}

type SegmentStream struct {
	deps     ports.RuntimePorts
	streams  ports.RunStreams
	ThreadID string
	StreamID string

	// appendMu keeps appends in the order their batches were taken.
	appendMu sync.Mutex

	mu     sync.Mutex
	buf    []ports.StreamEvent
	timer  *time.Timer
	ended  bool
	agents map[string]*blocks
	ids    int
	// oneShotText is a one-shot agent's final text, carried on RUN_FINISHED.
	oneShotText *string
}

// Closed reports whether Close ran.
func (s *SegmentStream) Closed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ended
}

// OneShotText is the one-shot text TEXT_RESULT carried, if any.
func (s *SegmentStream) OneShotText() (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.oneShotText == nil {
		return "", false
	}
	return *s.oneShotText, true
}

// Push queues events; it appends at once when any must go at once.
func (s *SegmentStream) Push(ctx context.Context, events ...ports.StreamEvent) {
	if len(events) == 0 {
		return
	}
	s.mu.Lock()
	if s.ended {
		s.mu.Unlock()
		return
	}
	s.buf = append(s.buf, events...)
	now := s.deps.Config.StreamFlush <= 0 || len(s.buf) >= s.deps.Config.StreamFlushEvents
	for _, e := range events {
		now = now || immediate(e)
	}
	if !now && s.timer == nil {
		s.timer = time.AfterFunc(s.deps.Config.StreamFlush, func() { s.Flush(context.Background()) })
	}
	s.mu.Unlock()
	if now {
		s.Flush(ctx)
	}
}

// Flush appends what waits.
func (s *SegmentStream) Flush(ctx context.Context) {
	s.appendMu.Lock()
	defer s.appendMu.Unlock()
	s.mu.Lock()
	if s.timer != nil {
		s.timer.Stop()
		s.timer = nil
	}
	batch := s.buf
	s.buf = nil
	s.mu.Unlock()
	if len(batch) == 0 {
		return
	}
	if _, err := s.streams.Append(context.WithoutCancel(ctx), s.StreamID, batch); err != nil {
		Logger(s.deps).Error("run stream events not appended", "stream", s.StreamID, "err", err)
	}
}

// Close ends the stream: what is open is ended, what waits is appended, and
// the end item goes last. A second close is a no-op.
func (s *SegmentStream) Close(ctx context.Context, end ports.StreamEnd) {
	s.mu.Lock()
	if s.ended {
		s.mu.Unlock()
		return
	}
	var closing []ports.StreamEvent
	for key := range s.agents {
		closing = append(closing, s.endBlocksLocked(key)...)
	}
	s.mu.Unlock()
	s.Push(ctx, closing...)
	s.Flush(ctx)
	s.mu.Lock()
	s.ended = true
	s.mu.Unlock()
	openSegments.Lock()
	if openSegments.m[s.streams][s.ThreadID] == s {
		delete(openSegments.m[s.streams], s.ThreadID)
	}
	openSegments.Unlock()
	if err := s.streams.Close(context.WithoutCancel(ctx), s.StreamID, end, s.deps.Config.StreamGrace); err != nil {
		Logger(s.deps).Error("run stream not closed", "stream", s.StreamID, "err", err)
	}
}

// Forward turns a published event into stream events. reserved is whether
// the type is one of the platform's; any other is an app's own, and goes
// out as CUSTOM.
func (s *SegmentStream) Forward(ctx context.Context, typ string, payload json.RawMessage, reserved bool) {
	var p map[string]any
	dec := json.NewDecoder(bytes.NewReader(payload))
	dec.UseNumber()
	_ = dec.Decode(&p)
	if p == nil {
		p = map[string]any{}
	}
	s.mu.Lock()
	events := s.mapLocked(typ, p, payload, reserved)
	s.mu.Unlock()
	s.Push(ctx, events...)
}

func (s *SegmentStream) mapLocked(typ string, p map[string]any, payload json.RawMessage, reserved bool) []ports.StreamEvent {
	switch typ {
	case "CHUNK":
		return s.mapChunkLocked("", p)
	case "SUBAGENT_CHUNK":
		agentID := str(p["agentId"])
		chunk, _ := p["chunk"].(map[string]any)
		if chunk == nil {
			chunk = map[string]any{}
		}
		return wrap(agentID, s.mapChunkLocked(agentID, chunk))
	case "STEP_FINISHED":
		var agentID *string
		var ending []ports.StreamEvent
		if a, ok := p["agentId"].(string); ok && a != "" {
			agentID = &a
			ending = wrap(a, s.endBlocksLocked(a))
		} else {
			ending = s.endBlocksLocked("")
		}
		return append(ending, &ports.StepFinishedEvent{
			Step: int(num(p["index"])), AgentID: agentID, FinishReason: str(p["finishReason"]),
			Usage: ports.StreamUsage{
				InputTokens: num(p["inputTokens"]), CachedInputTokens: num(p["cachedInputTokens"]),
				OutputTokens: num(p["outputTokens"]), TotalTokens: num(p["totalTokens"]),
			},
		})
	case "INPUT_REQUIRED":
		var e ports.InputRequiredEvent
		_ = json.Unmarshal(payload, &e)
		if len(e.Arguments) == 0 {
			e.Arguments = json.RawMessage("null")
		}
		if len(e.InputSchema) == 0 {
			e.InputSchema = json.RawMessage("null")
		}
		if len(e.Frames) == 0 {
			e.Frames = json.RawMessage("[]")
		}
		return []ports.StreamEvent{&e}
	case "SUBAGENT_STARTED":
		return []ports.StreamEvent{&ports.SubagentStartedEvent{
			SubagentID: str(p["agentId"]), Name: str(p["name"]), Depth: int(num(p["depth"])),
		}}
	case "SUBAGENT_COMPLETED", "SUBAGENT_FAILED":
		agentID := str(p["agentId"])
		done := &ports.SubagentFinishedEvent{SubagentID: agentID, Status: "completed"}
		if typ == "SUBAGENT_FAILED" {
			done.Status = "failed"
			if e, ok := p["error"]; ok && e != nil {
				done.Error = str(e)
			}
		}
		return append(wrap(agentID, s.endBlocksLocked(agentID)), done)
	case "TEXT_RESULT":
		if t, ok := p["text"].(string); ok {
			s.oneShotText = &t
		}
		return nil
	}
	if reserved {
		return nil // the thread record's, or a notice
	}
	value := payload
	if len(value) == 0 {
		value = json.RawMessage("null")
	}
	return []ports.StreamEvent{&ports.CustomEvent{Name: typ, Value: value}}
}

func (s *SegmentStream) blocksLocked(key string) *blocks {
	b := s.agents[key]
	if b == nil {
		b = &blocks{tools: map[string]bool{}}
		s.agents[key] = b
	}
	return b
}

func (s *SegmentStream) newIDLocked() string {
	s.ids++
	return fmt.Sprintf("%s:m%d", s.StreamID, s.ids)
}

func wrap(agentID string, events []ports.StreamEvent) []ports.StreamEvent {
	out := make([]ports.StreamEvent, len(events))
	for i, e := range events {
		out[i] = &ports.SubagentEventEvent{SubagentID: agentID, Event: e}
	}
	return out
}

// endBlocksLocked ends an agent's open text and reasoning.
func (s *SegmentStream) endBlocksLocked(key string) []ports.StreamEvent {
	b := s.agents[key]
	if b == nil {
		return nil
	}
	var out []ports.StreamEvent
	if b.reasoning != "" {
		out = append(out, &ports.ReasoningEndEvent{MessageID: b.reasoning})
	}
	if b.text != "" {
		out = append(out, &ports.TextMessageEndEvent{MessageID: b.text})
	}
	b.reasoning, b.text = "", ""
	return out
}

// mapChunkLocked is one SDK stream part as stream events, for one agent
// ("" the main one).
func (s *SegmentStream) mapChunkLocked(key string, c map[string]any) []ports.StreamEvent {
	b := s.blocksLocked(key)
	var out []ports.StreamEvent
	startTool := func(id, name string) {
		if b.tools[id] {
			return
		}
		b.tools[id] = true
		out = append(out, &ports.ToolCallStartEvent{ToolCallID: id, ToolName: name})
	}
	switch str(c["type"]) {
	case "text-delta":
		if b.reasoning != "" {
			out = append(out, &ports.ReasoningEndEvent{MessageID: b.reasoning})
			b.reasoning = ""
		}
		if b.text == "" {
			b.text = s.newIDLocked()
			out = append(out, &ports.TextMessageStartEvent{MessageID: b.text, Role: "assistant"})
		}
		out = append(out, &ports.TextMessageContentEvent{MessageID: b.text, Delta: str(c["textDelta"])})
	case "reasoning":
		if b.text != "" {
			out = append(out, &ports.TextMessageEndEvent{MessageID: b.text})
			b.text = ""
		}
		if b.reasoning == "" {
			b.reasoning = s.newIDLocked()
			out = append(out, &ports.ReasoningStartEvent{MessageID: b.reasoning})
		}
		out = append(out, &ports.ReasoningContentEvent{MessageID: b.reasoning, Delta: str(c["textDelta"])})
	case "tool-call-streaming-start":
		out = append(out, s.endBlocksLocked(key)...)
		startTool(str(c["toolCallId"]), str(c["toolName"]))
	case "tool-call-delta":
		startTool(str(c["toolCallId"]), str(c["toolName"]))
		out = append(out, &ports.ToolCallArgsEvent{ToolCallID: str(c["toolCallId"]), Delta: str(c["argsTextDelta"])})
	case "tool-call":
		out = append(out, s.endBlocksLocked(key)...)
		id := str(c["toolCallId"])
		startTool(id, str(c["toolName"]))
		delete(b.tools, id)
		args := c["args"]
		if text, ok := args.(string); ok && json.Valid([]byte(text)) {
			args = json.RawMessage(text)
		}
		out = append(out, &ports.ToolCallEndEvent{ToolCallID: id, ToolName: str(c["toolName"]), Args: MarshalPayload(args)})
	case "tool-result":
		out = append(out, &ports.ToolCallResultEvent{
			ToolCallID: str(c["toolCallId"]), ToolName: str(c["toolName"]), Result: MarshalPayload(c["result"]),
		})
	case "source":
		out = append(out, &ports.SourceEvent{Source: MarshalPayload(c["source"])})
	}
	return out
}

func str(v any) string {
	switch s := v.(type) {
	case string:
		return s
	case nil:
		return ""
	case json.Number:
		return s.String()
	}
	return fmt.Sprint(v)
}

func num(v any) int64 {
	switch n := v.(type) {
	case json.Number:
		i, err := n.Int64()
		if err != nil {
			f, _ := n.Float64()
			return int64(f)
		}
		return i
	case float64:
		return int64(n)
	case int:
		return int64(n)
	case int64:
		return n
	}
	return 0
}
