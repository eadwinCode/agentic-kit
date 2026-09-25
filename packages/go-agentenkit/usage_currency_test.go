package agentenkit_test

import (
	"testing"

	"github.com/zendev-sh/goai/provider"

	agentenkit "github.com/eadwinCode/agentic-kit/packages/go-agentenkit"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

// Workstream H: money is never converted. CostMicros is one currency (the
// first seen); a call priced in another is left out of it and counted as
// unpriced, and Costs keeps each currency's own total. The same cases run in
// the TS package (test/usage-currency.test.ts).

func TestUsageAggregatorKeepsOneTotalPerCurrency(t *testing.T) {
	var agg ports.UsageAggregator
	agg.Add(ports.NewUsage{AgentName: "a", Model: "m", InputTokens: 10, Cost: &ports.Cost{Micros: 100, Currency: "USD"}})
	agg.Add(ports.NewUsage{AgentName: "a", Model: "m", InputTokens: 10, Cost: &ports.Cost{Micros: 900, Currency: "EUR"}})
	agg.Add(ports.NewUsage{AgentName: "a", Model: "m", InputTokens: 10})
	total := agg.Totals()
	mustEqual(t, total.Currency, "USD", "the first currency seen")
	mustEqual(t, total.CostMicros, int64(100), "only that currency is summed")
	mustEqual(t, total.Unpriced, 2, "one unpriced, one in another currency")
	mustEqual(t, len(total.Costs), 2, "one total per currency")
	mustEqual(t, total.Costs[1], ports.CurrencyCost{Currency: "EUR", CostMicros: 900, Calls: 1}, "the euros are kept")
	mustEqual(t, len(total.Lines), 2, "a line per agent, model and currency")
	mustEqual(t, total.Lines[0].Calls, 2, "the unpriced call joins the first line")
	mustEqual(t, total.Lines[1].Currency, "EUR", "the euro line says so")
}

// The SQL adapters read GROUP BY agent, model and currency; the merger folds
// those groups back the same way.
func TestUsageLineMergerKeepsOneTotalPerCurrency(t *testing.T) {
	var m ports.UsageLineMerger
	line := func(cost int64, calls int) ports.UsageLine {
		return ports.UsageLine{AgentName: "a", Model: "m", InputTokens: 5, Calls: calls, CostMicros: cost}
	}
	m.Add(line(0, 1), "", 5, 1) // unpriced rows first: the line takes a currency later
	m.Add(line(100, 2), "USD", 5, 0)
	m.Add(line(700, 1), "EUR", 5, 0)
	total := m.Totals()
	mustEqual(t, total.Currency, "USD", "the first currency seen")
	mustEqual(t, total.CostMicros, int64(100), "only that currency is summed")
	mustEqual(t, total.Unpriced, 2, "one unpriced, one in another currency")
	mustEqual(t, total.TotalTokens, 15, "every call's tokens")
	mustEqual(t, len(total.Lines), 2, "a line per agent, model and currency")
	mustEqual(t, total.Lines[0].Currency, "USD", "the unpriced line took the first currency")
	mustEqual(t, total.Lines[0].Calls, 3, "three calls on it")
	mustEqual(t, total.Costs[1].CostMicros, int64(700), "the euros are kept")
}

// Total tokens is input + cached + output for every provider, never the
// provider's own total (Anthropic's leaves the cache reads out).
func TestUsage_TotalTokensIsAlwaysTheSum(t *testing.T) {
	got := agentenkit.AttributeTokens(provider.Usage{InputTokens: 10, CacheReadTokens: 30, OutputTokens: 5, TotalTokens: 15})
	mustEqual(t, got.TotalTokens, 45, "input + cached + output")
}
