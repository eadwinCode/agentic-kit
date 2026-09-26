// Package brave is web search on the Brave Search API: its own index of the
// web, $5 per 1,000 searches with $5 free each month (checked 2026-09-26).
// The default search adapter for the web_search built-in tool.
package brave

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"time"

	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/adapters/internal/websearch"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

// WebSearch is a ports.Search on Brave.
type WebSearch struct {
	APIKey string
	// Country is the two-letter country the results are for, like "us".
	Country string
	// SearchLang is the language of the results, like "en".
	SearchLang string
	BaseURL    string
	HTTP       *http.Client
}

// New is a Brave search with an API key.
func New(apiKey string) (*WebSearch, error) {
	if apiKey == "" {
		return nil, errors.New("brave: an API key is required")
	}
	return &WebSearch{APIKey: apiKey}, nil
}

func (b *WebSearch) Name() string { return "brave" }

var freshness = map[ports.SearchRecency]string{"day": "pd", "week": "pw", "month": "pm", "year": "py"}

var reTags = regexp.MustCompile(`<[^>]*>`)

func (b *WebSearch) Search(ctx context.Context, query string, opts ports.SearchOptions) ([]ports.SearchHit, error) {
	params := url.Values{}
	params.Set("q", websearch.WithSiteFilters(query, opts))
	params.Set("count", strconv.Itoa(min(max(opts.MaxResults, 1), 20)))
	params.Set("text_decorations", "false")
	if f, ok := freshness[opts.Recency]; ok {
		params.Set("freshness", f)
	}
	if b.Country != "" {
		params.Set("country", b.Country)
	}
	if b.SearchLang != "" {
		params.Set("search_lang", b.SearchLang)
	}
	base := b.BaseURL
	if base == "" {
		base = "https://api.search.brave.com/res/v1/web/search"
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"?"+params.Encode(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("X-Subscription-Token", b.APIKey)
	client := b.HTTP
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	res, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("brave: %w", err)
	}
	defer res.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(res.Body, 4<<20))
	if err != nil {
		return nil, fmt.Errorf("brave: %w", err)
	}
	if res.StatusCode >= 300 {
		return nil, fmt.Errorf("brave: %d %s", res.StatusCode, clip(string(raw), 200))
	}
	var body struct {
		Web struct {
			Results []struct {
				Title       string `json:"title"`
				URL         string `json:"url"`
				Description string `json:"description"`
				PageAge     string `json:"page_age"`
			} `json:"results"`
		} `json:"web"`
	}
	if err := json.Unmarshal(raw, &body); err != nil {
		return nil, fmt.Errorf("brave: %w", err)
	}
	hits := []ports.SearchHit{}
	for _, r := range body.Web.Results {
		if r.URL == "" || !websearch.DomainAllowed(r.URL, opts) {
			continue
		}
		hits = append(hits, ports.SearchHit{
			Title: r.Title, URL: r.URL, Snippet: reTags.ReplaceAllString(r.Description, ""), PublishedAt: r.PageAge,
		})
		if len(hits) >= opts.MaxResults {
			break
		}
	}
	return hits, nil
}

func clip(s string, n int) string {
	if len(s) > n {
		return s[:n]
	}
	return s
}
