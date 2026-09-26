// Package jina is web search and page reading through Jina AI (checked
// 2026-09-26): s.jina.ai for search, about $0.50 per 1,000 searches ($0.05
// per million tokens, at least 10,000 a search, 100 requests a minute), and
// r.jina.ai for pages built with JavaScript, PDFs and sites that block plain
// fetches, about $0.25–$0.50 per 1,000 pages.
package jina

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/adapters/internal/websearch"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

const snippetChars = 300

// Client is what both Jina adapters share.
type Client struct {
	APIKey  string
	BaseURL string
	HTTP    *http.Client
}

func (c Client) get(ctx context.Context, base, target string, headers map[string]string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+target, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.APIKey)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	client := c.HTTP
	if client == nil {
		client = &http.Client{Timeout: 60 * time.Second}
	}
	res, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("jina: %w", err)
	}
	defer res.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(res.Body, 32<<20))
	if err != nil {
		return fmt.Errorf("jina: %w", err)
	}
	if res.StatusCode >= 300 {
		if len(raw) > 200 {
			raw = raw[:200]
		}
		return fmt.Errorf("jina: %d %s", res.StatusCode, raw)
	}
	if err := json.Unmarshal(raw, out); err != nil {
		return fmt.Errorf("jina: %w", err)
	}
	return nil
}

// WebSearch is a ports.Search on s.jina.ai. Cheaper than Brave, slower and
// more limited: for low traffic where cost matters most. Jina returns each
// result's page text; only a short snippet is kept, so the model still
// opens only the pages it needs. It has no recency filter, so Recency is
// ignored.
type WebSearch struct{ Client }

// NewWebSearch is a Jina search with an API key.
func NewWebSearch(apiKey string) (*WebSearch, error) {
	if apiKey == "" {
		return nil, errors.New("jina: an API key is required")
	}
	return &WebSearch{Client{APIKey: apiKey}}, nil
}

func (j *WebSearch) Name() string { return "jina-search" }

var reSpaces = regexp.MustCompile(`\s+`)

func (j *WebSearch) Search(ctx context.Context, query string, opts ports.SearchOptions) ([]ports.SearchHit, error) {
	base := j.BaseURL
	if base == "" {
		base = "https://s.jina.ai/"
	}
	var body struct {
		Data []struct {
			Title       string `json:"title"`
			URL         string `json:"url"`
			Description string `json:"description"`
			Content     string `json:"content"`
			Date        string `json:"date"`
		} `json:"data"`
	}
	params := url.Values{"q": {websearch.WithSiteFilters(query, opts)}}
	if err := j.get(ctx, base, "?"+params.Encode(), nil, &body); err != nil {
		return nil, err
	}
	hits := []ports.SearchHit{}
	for _, r := range body.Data {
		if r.URL == "" || !websearch.DomainAllowed(r.URL, opts) {
			continue
		}
		text := r.Description
		if text == "" {
			text = r.Content
		}
		snippet := strings.TrimSpace(reSpaces.ReplaceAllString(text, " "))
		if runes := []rune(snippet); len(runes) > snippetChars {
			snippet = string(runes[:snippetChars])
		}
		hits = append(hits, ports.SearchHit{Title: r.Title, URL: r.URL, Snippet: snippet, PublishedAt: r.Date})
		if len(hits) >= opts.MaxResults {
			break
		}
	}
	return hits, nil
}

// Reader is a ports.Fetcher on r.jina.ai. Jina fetches the page from its
// own servers, so your network is never reached. Use it where the page
// reader comes back empty or blocked.
type Reader struct{ Client }

// NewReader is a Jina reader with an API key.
func NewReader(apiKey string) (*Reader, error) {
	if apiKey == "" {
		return nil, errors.New("jina: an API key is required")
	}
	return &Reader{Client{APIKey: apiKey}}, nil
}

func (j *Reader) Name() string { return "jina-reader" }

func (j *Reader) Fetch(ctx context.Context, target string, opts ports.FetchOptions) (ports.FetchedPage, error) {
	u, err := url.Parse(target)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") {
		return ports.FetchedPage{}, fmt.Errorf("jina: only http and https pages can be read, not %s", target)
	}
	base := j.BaseURL
	if base == "" {
		base = "https://r.jina.ai/"
	}
	format := "markdown"
	if opts.Format == "text" {
		format = "text"
	}
	var body struct {
		Data struct {
			Title   string `json:"title"`
			URL     string `json:"url"`
			Content string `json:"content"`
		} `json:"data"`
	}
	if err := j.get(ctx, base, u.String(), map[string]string{"X-Return-Format": format}, &body); err != nil {
		return ports.FetchedPage{}, err
	}
	page := ports.FetchedPage{URL: body.Data.URL, Title: body.Data.Title, Content: body.Data.Content}
	if page.URL == "" {
		page.URL = u.String()
	}
	if len(page.Content) > opts.MaxBytes {
		cut := opts.MaxBytes
		for cut > 0 && !utf8Start(page.Content[cut]) {
			cut-- // never split a character
		}
		page.Content, page.Truncated = page.Content[:cut], true
	}
	return page, nil
}

func utf8Start(b byte) bool { return b&0xC0 != 0x80 }
