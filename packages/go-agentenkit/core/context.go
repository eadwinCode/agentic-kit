package core

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/zendev-sh/goai"
	"github.com/zendev-sh/goai/provider"

	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

// ContextTokenCeiling is the universal context ceiling across all models (§2.6).
const ContextTokenCeiling = 265_000

var defaultNativeWindows = map[string]int{
	"gpt-4o":            128_000,
	"gpt-4o-mini":       128_000,
	"claude-3-5-sonnet": 200_000,
	"gemini-1.5-pro":    1_000_000,
}

// ContextBudget is min(native window, ceiling). The model's declared
// ContextWindow (via ResolveModel, §3.3) wins over the fallback tables.
func ContextBudget(deps ports.RuntimePorts, model string) int {
	native := 0
	if resolved, err := deps.ResolveModel(model); err == nil && resolved.ContextWindow > 0 {
		native = resolved.ContextWindow
	}
	if native == 0 {
		native = deps.Config.NativeWindows[model]
	}
	if native == 0 {
		native = defaultNativeWindows[model]
	}
	if native == 0 {
		native = deps.Config.ContextCeilingTokens
	}
	return min(native, deps.Config.ContextCeilingTokens)
}

// estimateTokens is the same rough estimate the TypeScript engine uses.
func estimateTokens(content []byte) int { return (len(content) + 3) / 4 }

// EstimateMessages is estimateTokens over a prompt. The platform uses it in
// the two places no real count exists: how full the context is (§2.6), and
// the prompt of a call that was cut off before the provider reported one
// (§4). One rule for both, so the two can never disagree.
func EstimateMessages(messages []provider.Message) int {
	raw, err := json.Marshal(messages)
	if err != nil {
		return 0
	}
	return estimateTokens(raw)
}

// ContextUsage is the read-only view of the §2.6 budget math: what
// CompactContext would see on the next run, without summarizing anything.
func ContextUsage(ctx context.Context, deps ports.RuntimePorts, threadID, model string) (ports.ContextUsage, error) {
	budget := ContextBudget(deps, model) - deps.Config.ContextOutputReserveTokens
	// The main agent's stream only: a nested run's turns are its own (§2.7)
	history, err := deps.Storage.Messages.List(ctx, threadID, ports.MainAgent)
	if err != nil {
		return ports.ContextUsage{}, err
	}
	used := 0
	for _, m := range history {
		used += estimateTokens(m.Content)
	}
	return ports.ContextUsage{
		UsedTokens: used, BudgetTokens: budget,
		CompactAtTokens: int(float64(budget) * deps.Config.CompactionTrigger),
		Messages:        len(history),
	}, nil
}

// CompactContext returns a history guaranteed to fit the model's budget.
// Compaction is durable: the summary is persisted as a message, so every
// client and every reconnect replay (§2.2) reconstructs the same context.
func CompactContext(ctx context.Context, deps ports.RuntimePorts, threadID, model string) ([]ports.MessageDTO, error) {
	budget := ContextBudget(deps, model) - deps.Config.ContextOutputReserveTokens
	// Scoped to the main agent: unscoped, delegated turns would be compacted
	// into, and then fed back through, the parent's prompt (§2.7)
	history, err := deps.Storage.Messages.List(ctx, threadID, ports.MainAgent)
	if err != nil {
		return nil, err
	}
	total := 0
	for _, m := range history {
		total += estimateTokens(m.Content)
	}
	if float64(total) <= float64(budget)*deps.Config.CompactionTrigger {
		return history, nil
	}

	// Keep the most recent tail verbatim ...
	tailBudget := float64(budget) * deps.Config.ContextTailShare
	tailStart := len(history)
	tailTokens := 0
	for i := len(history) - 1; i >= 0; i-- {
		t := estimateTokens(history[i].Content)
		if float64(tailTokens+t) > tailBudget {
			break
		}
		tailStart = i
		tailTokens += t
	}
	older := history[:tailStart]
	tail := history[tailStart:]
	if len(older) == 0 {
		return history, nil // single oversized turn
	}

	// ... and summarize everything before it with a cheap model
	var sb strings.Builder
	for _, m := range older {
		fmt.Fprintf(&sb, "%s: %s\n", m.Role, string(m.Content))
	}
	resolved, err := deps.ResolveModel(deps.Config.CompactionModel)
	if err != nil {
		return nil, fmt.Errorf("compaction model %q: %w", deps.Config.CompactionModel, err)
	}
	res, err := goai.GenerateText(ctx, resolved.Instance(), goai.WithPrompt(
		"Summarize the following conversation history into a dense context brief "+
			"(decisions, open threads, key facts) for an AI agent:\n\n"+sb.String()))
	if err != nil {
		return nil, err
	}
	summary, err := deps.Storage.Messages.Append(ctx, threadID, ports.NewMessage{
		Role: ports.RoleSystem, Content: ContextSummaryContent(res.Text),
	})
	if err != nil {
		return nil, err
	}
	// Compaction is a model call the platform made on its own account (§2.6),
	// so it gets its own priced row like any other (§4). Kind "compaction"
	// keeps it separable: nobody asked for this call, and it is worth being
	// able to see what the platform's own housekeeping costs.
	RecordCall(ctx, deps, threadID, ports.NewUsage{
		Kind: ports.KindCompaction, Model: deps.Config.CompactionModel,
		ModelID:               resolved.WireID(deps.Config.CompactionModel),
		Outcome:               ports.UsageFinished,
		ProviderMetadata:      providerMeta(res.ProviderMetadata, res.Response),
		InputTokens:           max(res.TotalUsage.InputTokens, 0),
		CacheReadInputTokens:  max(res.TotalUsage.CacheReadTokens, 0),
		CacheWriteInputTokens: max(res.TotalUsage.CacheWriteTokens, 0),
		OutputTokens:          max(res.TotalUsage.OutputTokens, 0),
		ReasoningTokens:       max(res.TotalUsage.ReasoningTokens, 0),
	})
	if _, err := Publish(ctx, deps, threadID, "CONTEXT_COMPACTED", map[string]any{"summarizedMessages": len(older)}); err != nil {
		return nil, err
	}
	out := make([]ports.MessageDTO, 0, 1+len(tail))
	out = append(out, *summary)
	out = append(out, tail...)
	return out, nil
}
