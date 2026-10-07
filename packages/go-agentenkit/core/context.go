package core

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
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

// estimateTokens is the same rough estimate the TypeScript engine uses: a
// quarter of the JSON's length in UTF-16 code units, which is what a
// JavaScript string's length counts. Counting bytes instead would make the
// two runtimes compact the same thread at different points.
func estimateTokens(content []byte) int {
	units := 0
	for _, r := range string(content) {
		if r >= 0x10000 {
			units += 2
		} else {
			units++
		}
	}
	return (units + 3) / 4
}

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
	history = PromptHistory(history)
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

// CompactOptions is what a compaction needs to know about the run it serves.
type CompactOptions struct {
	// RunID is the run the compaction call is billed to (§4), so it counts
	// in the run's bill and against its cost cap.
	RunID string
	// GenCtx is the context the summary call runs under: a stop cancels it.
	// Nil means ctx.
	GenCtx context.Context
	// Ledger is the run's ledger, so the summary call counts against the
	// run's caps like any other call. Nil records the row on its own.
	Ledger *RunLedger
	// Force compacts whatever the history's size: everything older than the
	// recent tail goes into the summary. Used when a provider has refused a
	// prompt as too long, and for a compaction someone asked for.
	Force bool
}

// CompactContext returns a history guaranteed to fit the model's budget.
// Compaction is durable: the summary is persisted as a message, so every
// client and every reconnect replay (§2.2) reconstructs the same context.
//
// The summary records the last message it covers, and the prompt carries
// the summary and only what came after (see PromptHistory), so a thread
// compacts once each time it grows past the trigger, not on every run.
func CompactContext(ctx context.Context, deps ports.RuntimePorts, threadID, model string, opts CompactOptions) ([]ports.MessageDTO, error) {
	budget := ContextBudget(deps, model) - deps.Config.ContextOutputReserveTokens
	// Scoped to the main agent: unscoped, delegated turns would be compacted
	// into, and then fed back through, the parent's prompt (§2.7)
	stored, err := deps.Storage.Messages.List(ctx, threadID, ports.MainAgent)
	if err != nil {
		return nil, err
	}
	history := PromptHistory(stored)
	total := 0
	for _, m := range history {
		total += estimateTokens(m.Content)
	}
	if !opts.Force && float64(total) <= float64(budget)*deps.Config.CompactionTrigger {
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
	// ... starting at a user turn. Cut anywhere else and the tail can open on
	// a tool result whose call went into the summary, which no provider
	// accepts. The first user turn inside the budget, or failing that the
	// last one before it.
	tailStart = userTurnAtOrAfter(history, tailStart)
	if opts.Force {
		// Forced (a refusal, or asked for): the tail share is what proved
		// too much, so only the latest user turn stays verbatim.
		tailStart = userTurnAtOrAfter(history, len(history))
	}
	older := history[:tailStart]
	tail := history[tailStart:]
	coversUpTo := ""
	for i := len(older) - 1; i >= 0; i-- {
		if _, isSummary := summaryOf(older[i]); !isSummary {
			coversUpTo = older[i].ID
			break
		}
	}
	if coversUpTo == "" {
		// Nothing new to summarize: one turn is larger than the tail's share.
		// It goes as it is; the estimate is rough, and the provider decides.
		warnOverBudget(deps, threadID, total, budget)
		return history, nil
	}

	// ... and summarize everything before it with a cheap model: the last
	// summary and the turns since, never the whole history again.
	lines := make([]string, 0, len(older))
	for _, m := range older {
		lines = append(lines, fmt.Sprintf("%s: %s\n", m.Role, string(m.Content)))
	}
	text, err := summarizeLines(ctx, deps, threadID, opts, lines)
	if err != nil {
		return nil, err
	}
	summary, err := deps.Storage.Messages.Append(ctx, threadID, ports.NewMessage{
		Role: ports.RoleSystem, Content: ContextSummaryContent(text, coversUpTo),
	})
	if err != nil {
		return nil, err
	}
	if _, err := Publish(ctx, deps, threadID, "CONTEXT_COMPACTED", map[string]any{"summarizedMessages": len(older)}); err != nil {
		return nil, err
	}
	out := make([]ports.MessageDTO, 0, 1+len(tail))
	out = append(out, *summary)
	out = append(out, tail...)
	// Still over the window: a tail turn alone is larger than it. The next
	// run compacts from this summary on, so this never loops; the call goes
	// as it is and the provider decides, but it is worth saying.
	used := 0
	for _, m := range out {
		used += estimateTokens(m.Content)
	}
	warnOverBudget(deps, threadID, used, budget)
	return out, nil
}

// warnOverBudget logs a prompt estimated past the whole window.
func warnOverBudget(deps ports.RuntimePorts, threadID string, used, budget int) {
	if used > budget {
		Logger(deps).Warn("prompt larger than the context window after compaction",
			"thread", threadID, "estimatedTokens", used, "budgetTokens", budget)
	}
}

// userTurnAtOrAfter moves a tail start onto a user turn: the first one at or
// after start, or failing that the last one before it. With no user turn at
// all, the tail is empty.
func userTurnAtOrAfter(history []ports.MessageDTO, start int) int {
	for i := start; i < len(history); i++ {
		if history[i].Role == ports.RoleUser {
			return i
		}
	}
	for i := min(start, len(history)) - 1; i >= 0; i-- {
		if history[i].Role == ports.RoleUser {
			return i
		}
	}
	return len(history)
}

// contextOverflow matches the "prompt too long" refusals providers send.
// Not "too many tokens": that is also how a throttle is worded (Bedrock).
var contextOverflow = regexp.MustCompile(`(?i)prompt is too long|context[_ ]length[_ ]exceeded|maximum context length|exceeds the context window|input is too long`)

// IsContextOverflow reports whether a model call was refused because the
// prompt did not fit the model's context window. A rate limit (429) never
// is, whatever its wording: compacting would not help it.
func IsContextOverflow(err error) bool {
	if err == nil {
		return false
	}
	var api *goai.APIError
	if errors.As(err, &api) && api.StatusCode == 429 {
		return false
	}
	return contextOverflow.MatchString(err.Error())
}

const (
	summaryPrompt = "Summarize the following conversation history into a dense context brief " +
		"(decisions, open threads, key facts) for an AI agent:\n\n"
	mergePrompt = "These are summaries of consecutive parts of one conversation, oldest first. " +
		"Merge them into one dense context brief (decisions, open threads, key facts) for an AI agent, " +
		"keeping later decisions over earlier ones:\n\n"
)

// summaryChunkTokens is how much history one summary call carries: half the
// compaction model's own window, leaving room for its answer and for the
// estimate being rough. The history can be larger than the summarizer's
// window, and a call past it would be refused.
func summaryChunkTokens(deps ports.RuntimePorts) int {
	return max(ContextBudget(deps, deps.Config.CompactionModel)/2, 1_000)
}

// summarizeLines summarizes the lines in as many calls as fit the
// compaction model, then merges the partial summaries the same way until
// one is left.
func summarizeLines(ctx context.Context, deps ports.RuntimePorts, threadID string, opts CompactOptions, lines []string) (string, error) {
	limit := summaryChunkTokens(deps)
	prompt := summaryPrompt
	for {
		chunks := chunkLines(lines, limit)
		parts := make([]string, 0, len(chunks))
		for _, chunk := range chunks {
			text, err := summaryCall(ctx, deps, threadID, opts, prompt+chunk)
			if err != nil {
				return "", err
			}
			parts = append(parts, text)
		}
		if len(parts) == 1 {
			return parts[0], nil
		}
		// A third of a chunk per part fits at least two per merge call, so
		// every round at least halves the parts and the loop ends.
		lines = make([]string, len(parts))
		for i, part := range parts {
			lines[i] = trimMiddle(fmt.Sprintf("Part %d:\n%s\n\n", i+1, part), limit/3)
		}
		prompt = mergePrompt
	}
}

// chunkLines packs lines in order into chunks of at most limit tokens. A
// line larger than a chunk keeps its start and end; its middle goes.
func chunkLines(lines []string, limit int) []string {
	var chunks []string
	var sb strings.Builder
	used := 0
	for _, line := range lines {
		line = trimMiddle(line, limit)
		t := estimateTokens([]byte(line))
		if used > 0 && used+t > limit {
			chunks = append(chunks, sb.String())
			sb.Reset()
			used = 0
		}
		sb.WriteString(line)
		used += t
	}
	if used > 0 || len(chunks) == 0 {
		chunks = append(chunks, sb.String())
	}
	return chunks
}

// trimMiddle cuts a line past limit tokens down to its start and end.
func trimMiddle(line string, limit int) string {
	if estimateTokens([]byte(line)) <= limit {
		return line
	}
	runes := []rune(line)
	keep := limit * 4 / 2 // the estimate is about four characters a token
	if keep*2 >= len(runes) {
		return line
	}
	return string(runes[:keep]) + "\n[... cut ...]\n" + string(runes[len(runes)-keep:])
}

// summaryCall is one summary call, recorded on its own priced row.
func summaryCall(ctx context.Context, deps ports.RuntimePorts, threadID string, opts CompactOptions, prompt string) (string, error) {
	resolved, err := deps.ResolveModel(deps.Config.CompactionModel)
	if err != nil {
		return "", fmt.Errorf("compaction model %q: %w", deps.Config.CompactionModel, err)
	}
	genCtx := opts.GenCtx
	if genCtx == nil {
		genCtx = ctx
	}
	res, err := goai.GenerateText(genCtx, resolved.Instance(), goai.WithPrompt(prompt))
	if err != nil {
		return "", err
	}
	// Compaction is a model call the platform made on its own account (§2.6),
	// so it gets its own priced row like any other (§4). Kind "compaction"
	// keeps it separable: nobody asked for this call, and it is worth being
	// able to see what the platform's own housekeeping costs. It is billed to
	// the run it served, so it is on that run's bill and under its cap.
	record := RecordCall
	if opts.Ledger != nil {
		record = opts.Ledger.Record
	}
	record(ctx, deps, threadID, ports.NewUsage{
		RunID: opts.RunID,
		Kind:  ports.KindCompaction, Model: deps.Config.CompactionModel,
		ModelID:               resolved.WireID(deps.Config.CompactionModel),
		Outcome:               ports.UsageFinished,
		ProviderMetadata:      providerMeta(res.ProviderMetadata, res.Response),
		InputTokens:           max(res.TotalUsage.InputTokens, 0),
		CacheReadInputTokens:  max(res.TotalUsage.CacheReadTokens, 0),
		CacheWriteInputTokens: max(res.TotalUsage.CacheWriteTokens, 0),
		OutputTokens:          max(res.TotalUsage.OutputTokens, 0),
		ReasoningTokens:       max(res.TotalUsage.ReasoningTokens, 0),
	})
	return res.Text, nil
}
