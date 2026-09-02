package core

import (
	"github.com/zendev-sh/goai/provider"

	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

// TokenAttribution is the four canonical counters (§4). Same shape as
// UsageTotals.
type TokenAttribution = ports.UsageTotals

// AttributeTokens turns one goai usage report into the four canonical
// counters.
//
// goai already normalises the providers' disagreements about what "input
// tokens" means: for OpenAI-family models it subtracts the cached tokens
// from InputTokens, and Anthropic reports cache reads beside the input count.
// So InputTokens is always the fresh count and CacheReadTokens the cached
// one. TotalTokens is computed as input + cached + output so it means the
// same thing for every provider.
func AttributeTokens(u provider.Usage) TokenAttribution {
	in := max(u.InputTokens, 0)
	cached := max(u.CacheReadTokens, 0)
	out := max(u.OutputTokens, 0)
	return TokenAttribution{
		InputTokens:       in,
		CachedInputTokens: cached,
		OutputTokens:      out,
		TotalTokens:       in + cached + out,
	}
}

// CountTokens is the total tokens used: input + cached + output.
func CountTokens(u provider.Usage) int { return AttributeTokens(u).TotalTokens }
