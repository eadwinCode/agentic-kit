package pricing_test

import (
	"context"
	"errors"
	"testing"

	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/pricing"
)

var table = pricing.Table{
	// Anthropic's published Sonnet prices, per million tokens.
	"claude-sonnet-4": {
		InputPerMillion: 3, CacheReadPerMillion: 0.30,
		CacheWritePerMillion: 3.75, OutputPerMillion: 15,
	},
	"gpt-4o-2024-11-20": {InputPerMillion: 2.5, OutputPerMillion: 10},
}

func price(t *testing.T, p ports.Pricer, u ports.NewUsage) *ports.Cost {
	t.Helper()
	c, err := p.Price(context.Background(), u)
	if err != nil {
		t.Fatalf("price: %v", err)
	}
	return c
}

func TestTable_PricesPerMillionTokensIntoMicros(t *testing.T) {
	// 1M input at $3 is $3.00, which is 3_000_000 micros.
	got := price(t, table, ports.NewUsage{Model: "claude-sonnet-4", InputTokens: 1_000_000})
	if got.Micros != 3_000_000 {
		t.Fatalf("input: got %d micros, want 3000000", got.Micros)
	}
	// A realistic mixed call: 12k fresh input, 40k cache reads, 8k cache
	// writes, 900 output.
	//   12_000×3 + 40_000×0.30 + 8_000×3.75 + 900×15 = 36_000 + 12_000 + 30_000 + 13_500
	got = price(t, table, ports.NewUsage{
		Model: "claude-sonnet-4", InputTokens: 12_000, CacheReadInputTokens: 40_000,
		CacheWriteInputTokens: 8_000, OutputTokens: 900,
	})
	if got.Micros != 91_500 {
		t.Fatalf("mixed: got %d micros, want 91500", got.Micros)
	}
	if got.Currency != "USD" || got.Source != "table" {
		t.Fatalf("got %+v", got)
	}
}

func TestTable_FallsBackToTheWireIdThenTheBaseKey(t *testing.T) {
	// The registry key is unknown but the provider reported a wire id the
	// table does know.
	if c := price(t, table, ports.NewUsage{Model: "fast", ModelID: "gpt-4o-2024-11-20", OutputTokens: 1_000_000}); c == nil || c.Micros != 10_000_000 {
		t.Fatalf("wire id fallback: %+v", c)
	}
	// A "@variant" suffix on the registry key still finds the base model.
	if c := price(t, table, ports.NewUsage{Model: "claude-sonnet-4@high", InputTokens: 1_000_000}); c == nil || c.Micros != 3_000_000 {
		t.Fatalf("variant fallback: %+v", c)
	}
	// A model nobody knows is not priced, rather than priced at zero.
	if c := price(t, table, ports.NewUsage{Model: "who-knows", InputTokens: 1_000}); c != nil {
		t.Fatalf("unknown model priced: %+v", c)
	}
}

func TestReceipt_ReadsWhatTheProviderAlreadyComputed(t *testing.T) {
	p := pricing.Receipt(func(meta map[string]any) (int64, bool) {
		headers, ok := meta["responseHeaders"].(map[string]string)
		if !ok {
			return 0, false
		}
		if headers["x-cost-micros"] == "4200" {
			return 4200, true
		}
		return 0, false
	})
	got := price(t, p, ports.NewUsage{ProviderMetadata: map[string]any{
		"responseHeaders": map[string]string{"x-cost-micros": "4200"},
	}})
	if got.Micros != 4200 || got.Source != "receipt" {
		t.Fatalf("got %+v", got)
	}
	// No receipt on this call: say nothing, so the next pricer can try.
	if c := price(t, p, ports.NewUsage{ProviderMetadata: map[string]any{}}); c != nil {
		t.Fatalf("answered without a receipt: %+v", c)
	}
}

func TestChain_TakesTheFirstAnswerAndSkipsFailures(t *testing.T) {
	boom := ports.PricerFunc(func(context.Context, ports.NewUsage) (*ports.Cost, error) {
		return nil, errors.New("price service down")
	})
	silent := ports.PricerFunc(func(context.Context, ports.NewUsage) (*ports.Cost, error) {
		return nil, nil
	})
	// A pricer that errors is skipped rather than believed, and the table
	// behind it still answers.
	got := price(t, pricing.Chain(boom, silent, table),
		ports.NewUsage{Model: "claude-sonnet-4", InputTokens: 1_000_000})
	if got.Micros != 3_000_000 {
		t.Fatalf("got %+v", got)
	}
	// Nobody could price it: the error is what comes back, so a caller can
	// see that pricing is broken rather than that the call was free.
	if _, err := pricing.Chain(boom, silent).Price(context.Background(), ports.NewUsage{}); err == nil {
		t.Fatal("want the first error back when nothing answered")
	}
	// Nothing to say and nothing wrong is not an error.
	c, err := pricing.Chain(silent).Price(context.Background(), ports.NewUsage{})
	if c != nil || err != nil {
		t.Fatalf("got %+v, %v", c, err)
	}
}

func TestMicrosRoundTrip(t *testing.T) {
	if got := pricing.Micros(0.25); got != 250_000 {
		t.Fatalf("Micros(0.25) = %d", got)
	}
	if got := pricing.Amount(250_000); got != 0.25 {
		t.Fatalf("Amount(250000) = %v", got)
	}
	if got := pricing.Format(250_000, ""); got != "0.2500 USD" {
		t.Fatalf("Format = %q", got)
	}
}
