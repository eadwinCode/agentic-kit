package memory

import (
	"context"
	"fmt"
	"sync"

	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

// Search is a search engine for tests: it answers from a fixed list, or
// from a function of the query, and keeps every query it was asked.
type Search struct {
	name   string
	answer func(query string, opts ports.SearchOptions) ([]ports.SearchHit, error)

	mu      sync.Mutex
	queries []SearchQuery
}

// SearchQuery is one query a memory Search was asked.
type SearchQuery struct {
	Query   string
	Options ports.SearchOptions
}

// NewSearch answers every query with hits.
func NewSearch(hits []ports.SearchHit, name string) *Search {
	return NewSearchFunc(func(string, ports.SearchOptions) ([]ports.SearchHit, error) { return hits, nil }, name)
}

// NewSearchFunc answers with fn.
func NewSearchFunc(fn func(query string, opts ports.SearchOptions) ([]ports.SearchHit, error), name string) *Search {
	if name == "" {
		name = "memory-search"
	}
	return &Search{name: name, answer: fn}
}

func (s *Search) Name() string { return s.name }

func (s *Search) Search(_ context.Context, query string, opts ports.SearchOptions) ([]ports.SearchHit, error) {
	s.mu.Lock()
	s.queries = append(s.queries, SearchQuery{query, opts})
	s.mu.Unlock()
	hits, err := s.answer(query, opts)
	if err != nil {
		return nil, err
	}
	if len(hits) > opts.MaxResults {
		hits = hits[:opts.MaxResults]
	}
	return hits, nil
}

// Queries are the queries asked so far.
func (s *Search) Queries() []SearchQuery {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]SearchQuery(nil), s.queries...)
}

// Page is one page a memory Fetcher serves.
type Page struct {
	Title   string
	Content string
}

// Fetcher is a page reader for tests: it serves pages from a map by URL,
// and keeps every URL it was asked for. An unknown URL fails like a 404.
type Fetcher struct {
	name  string
	pages map[string]Page

	mu      sync.Mutex
	fetched []string
}

// NewFetcher serves pages.
func NewFetcher(pages map[string]Page, name string) *Fetcher {
	if name == "" {
		name = "memory-fetcher"
	}
	return &Fetcher{name: name, pages: pages}
}

func (f *Fetcher) Name() string { return f.name }

func (f *Fetcher) Fetch(_ context.Context, url string, opts ports.FetchOptions) (ports.FetchedPage, error) {
	f.mu.Lock()
	f.fetched = append(f.fetched, url)
	f.mu.Unlock()
	p, ok := f.pages[url]
	if !ok {
		return ports.FetchedPage{}, fmt.Errorf("memory-fetcher: %s answered 404", url)
	}
	content, truncated := p.Content, len(p.Content) > opts.MaxBytes
	if truncated {
		content = content[:opts.MaxBytes]
	}
	return ports.FetchedPage{URL: url, Title: p.Title, Content: content, Truncated: truncated}, nil
}

// Fetched are the URLs asked for so far.
func (f *Fetcher) Fetched() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.fetched...)
}
