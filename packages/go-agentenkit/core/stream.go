package core

import (
	"encoding/json"

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
		return map[string]any{"type": "tool-result", "toolCallId": c.ToolCallID, "toolName": c.ToolName, "result": jsonOrString(c.Text)}
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
