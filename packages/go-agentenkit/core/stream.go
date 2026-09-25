package core

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/zendev-sh/goai"
	"github.com/zendev-sh/goai/provider"
)

// usageJSON is the usage shape a CHUNK payload carries. Same field names the
// TypeScript package (AI SDK v4) publishes, so the React client reads both.
func usageJSON(u provider.Usage) map[string]any {
	return map[string]any{
		"promptTokens":      u.InputTokens,
		"completionTokens":  u.OutputTokens,
		"totalTokens":       u.TotalTokens,
		"cachedInputTokens": u.CacheReadTokens,
	}
}

// ChunkPayload renders a goai stream chunk in the AI SDK part shape the
// TypeScript package publishes as CHUNK. A client written against one
// runtime keeps working against the other.
func ChunkPayload(c provider.StreamChunk) map[string]any {
	switch c.Type {
	case provider.ChunkText:
		return map[string]any{"type": "text-delta", "textDelta": c.Text}
	case provider.ChunkReasoning:
		return map[string]any{"type": "reasoning", "textDelta": c.Text}
	case provider.ChunkToolCallStreamStart:
		return map[string]any{"type": "tool-call-streaming-start", "toolCallId": c.ToolCallID, "toolName": c.ToolName}
	case provider.ChunkToolCallDelta:
		return map[string]any{"type": "tool-call-delta", "toolCallId": c.ToolCallID, "toolName": c.ToolName, "argsTextDelta": c.ToolInput}
	case provider.ChunkToolCall:
		var args any
		if json.Valid([]byte(c.ToolInput)) {
			args = json.RawMessage(c.ToolInput)
		} else {
			args = c.ToolInput
		}
		return map[string]any{"type": "tool-call", "toolCallId": c.ToolCallID, "toolName": c.ToolName, "args": args}
	case provider.ChunkToolResult:
		result := jsonOrString(c.Text)
		if c.Error != nil {
			// A tool that failed: the result names the error, the way a
			// denied or unknown tool's result already does, so a client can
			// mark the call failed from the result alone.
			result = MarshalPayload(map[string]any{"error": c.Error.Error()})
		}
		return map[string]any{"type": "tool-result", "toolCallId": c.ToolCallID, "toolName": c.ToolName, "result": result}
	case provider.ChunkStepFinish:
		return map[string]any{"type": "step-finish", "finishReason": string(c.FinishReason), "usage": usageJSON(c.Usage)}
	case provider.ChunkFinish:
		return map[string]any{"type": "finish", "finishReason": string(c.FinishReason), "usage": usageJSON(c.Usage)}
	case provider.ChunkError:
		msg := ""
		if c.Error != nil {
			msg = c.Error.Error()
		}
		return map[string]any{"type": "error", "error": msg}
	}
	return map[string]any{"type": string(c.Type), "text": c.Text}
}

// drainStream reads a goai stream to the end, firing onChunk per part, and
// turns a provider failure into an error.
//
// goai reports a failure (an aborted call included) as an error chunk and
// then ends the stream; Err() is only meaningful once the stream is drained.
// Draining fully first also means onChunk fires for every part that did
// arrive before the failure.
func drainStream(stream *goai.TextStream, onChunk func(provider.StreamChunk)) error {
	for chunk := range stream.Stream() {
		if onChunk != nil {
			onChunk(chunk)
		}
	}
	return stream.Err()
}

// chunkWindow is how long a merged delta is held before it goes out.
const chunkWindow = 50 * time.Millisecond

// chunkBatcher merges token deltas before they go out as events (§2.2).
// Every published event costs a seq, a stored row and a bus send; one per
// token is thousands a reply. Consecutive text (or reasoning) deltas are
// joined into one delta of the same shape, so a client reads them exactly as
// before. A held delta goes out after chunkWindow, or at once when any other
// kind of chunk comes, or on flush: the loop flushes before a step is
// committed, so its text is never published after the step that produced
// it. Sends are made one at a time, in order.
type chunkBatcher struct {
	send  func(map[string]any)
	mu    sync.Mutex
	held  map[string]any
	timer *time.Timer
}

func newChunkBatcher(send func(map[string]any)) *chunkBatcher {
	return &chunkBatcher{send: send}
}

func isDelta(p map[string]any) bool {
	t, _ := p["type"].(string)
	_, text := p["textDelta"].(string)
	return text && (t == "text-delta" || t == "reasoning")
}

// push takes one chunk payload. A delta is held; anything else is sent,
// after what is held, before push returns.
func (b *chunkBatcher) push(p map[string]any) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if isDelta(p) && b.held != nil && b.held["type"] == p["type"] {
		b.held["textDelta"] = b.held["textDelta"].(string) + p["textDelta"].(string)
		return
	}
	b.flushLocked()
	if isDelta(p) {
		held := make(map[string]any, len(p))
		for k, v := range p {
			held[k] = v
		}
		b.held = held
		b.timer = time.AfterFunc(chunkWindow, b.flush)
		return
	}
	b.send(p)
}

// flush sends what is held.
func (b *chunkBatcher) flush() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.flushLocked()
}

func (b *chunkBatcher) flushLocked() {
	if b.timer != nil {
		b.timer.Stop()
		b.timer = nil
	}
	if b.held != nil {
		held := b.held
		b.held = nil
		b.send(held)
	}
}
