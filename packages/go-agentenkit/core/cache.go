package core

import (
	"slices"

	"github.com/zendev-sh/goai/provider"
)

// Prompt caching (§2.6): providers like Anthropic cache a prompt prefix only
// when it carries explicit cache breakpoints; OpenAI-family models cache
// automatically for prompts of 1024+ tokens and ignore these markers.
//
// MarkPromptCaching stamps ephemeral cache breakpoints on the stable prefix
// of a prompt: the system message and the tail message of the conversation
// so far. The stamp is goai's per-part CacheControl field, which the
// Anthropic provider turns into cache_control and the others ignore.

const cacheControlEphemeral = "ephemeral"

func stampLastPart(m provider.Message) provider.Message {
	if len(m.Content) == 0 {
		return m
	}
	parts := slices.Clone(m.Content)
	parts[len(parts)-1].CacheControl = cacheControlEphemeral
	m.Content = parts
	return m
}

// SystemCacheMessage is a system prompt in the one shape that can carry a
// cache breakpoint: a message with a stamped part. As goai's WithSystem
// string it reaches the provider with no metadata channel and never caches,
// and it is usually the largest, most stable part of the prompt.
func SystemCacheMessage(system string) provider.Message {
	return provider.Message{
		Role:    provider.RoleSystem,
		Content: []provider.Part{{Type: provider.PartText, Text: system, CacheControl: cacheControlEphemeral}},
	}
}

// MarkPromptCaching marks the stable prefix of a prompt for provider-side
// caching: the system message (if any) plus the last message of the history.
func MarkPromptCaching(messages []provider.Message) []provider.Message {
	out := slices.Clone(messages)
	for i := range out {
		if out[i].Role == provider.RoleSystem {
			out[i] = stampLastPart(out[i])
			break
		}
	}
	if len(out) > 0 {
		out[len(out)-1] = stampLastPart(out[len(out)-1])
	}
	return out
}
