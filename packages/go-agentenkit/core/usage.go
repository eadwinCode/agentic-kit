package core

import (
	"context"
	"log/slog"

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

// FillTokens copies one goai usage report onto a usage row, cache writes and
// reasoning included. Those two are not part of TotalTokens (see
// ports.NewUsage.TotalTokens), but a pricer needs them: cache writes are a
// separate line on the provider's bill.
func FillTokens(n *ports.NewUsage, u provider.Usage) {
	n.InputTokens = max(u.InputTokens, 0)
	n.CacheReadInputTokens = max(u.CacheReadTokens, 0)
	n.CacheWriteInputTokens = max(u.CacheWriteTokens, 0)
	n.OutputTokens = max(u.OutputTokens, 0)
	n.ReasoningTokens = max(u.ReasoningTokens, 0)
}

// Logger is the platform's logger, defaulted.
func Logger(deps ports.RuntimePorts) *slog.Logger {
	if deps.Log != nil {
		return deps.Log
	}
	return slog.Default()
}

// RecordCall prices ONE model call and stores its usage row (§4). Called
// after every call the platform makes: a step of the main run, a step of a
// nested run, a compaction pass, streamed or not, finished or cut short.
//
// Pricing happens here, before the row is stored, so cost sits on the row
// beside the tokens and no reader has to work it out again. A pricer that
// fails, or has nothing to say, leaves the row unpriced rather than failing
// the run: a bill that is short a line is recoverable, a run that died over
// a price list is not.
//
// The write uses a context that a user stop cannot cancel, because the
// tokens of a stopped run were still spent. Storage failures are logged, not
// returned: the same reasoning.
func RecordCall(ctx context.Context, deps ports.RuntimePorts, threadID string, u ports.NewUsage) ports.NewUsage {
	if deps.Pricer != nil && u.Cost == nil {
		cost, err := deps.Pricer.Price(ctx, u)
		switch {
		case err != nil:
			Logger(deps).Error("usage not priced",
				"run", u.RunID, "model", u.Model, "err", err)
		default:
			u.Cost = cost
		}
	}
	if err := deps.Storage.Usage.Record(context.WithoutCancel(ctx), threadID, u); err != nil {
		Logger(deps).Error("usage not recorded",
			"run", u.RunID, "thread", threadID, "err", err)
	}
	return u
}
