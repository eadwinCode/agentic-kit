package ports

import "context"

// The ports behind the built-in tools. Each tool has one name and one input
// shape in every runtime; what does the work is an adapter you plug in
// here, like storage or the queue.

// SearchRecency is how far back a search may look: "day", "week", "month"
// or "year".
type SearchRecency string

// SearchOptions shapes one search.
type SearchOptions struct {
	// MaxResults is 1 to 10.
	MaxResults int
	// AllowedDomains keeps only results from these domains (and their
	// subdomains).
	AllowedDomains []string
	// BlockedDomains drops results from these domains (and their
	// subdomains).
	BlockedDomains []string
	Recency        SearchRecency
}

// SearchHit is one search result.
type SearchHit struct {
	Title string `json:"title"`
	URL   string `json:"url"`
	// Snippet is a short piece of the page that matched: what the model
	// reads to decide whether to open it.
	Snippet string `json:"snippet"`
	// PublishedAt is an ISO date, when the engine knows it.
	PublishedAt string `json:"publishedAt,omitempty"`
}

// Search is a web search engine: Brave, Jina, Tavily…
type Search interface {
	// Name is the adapter's name, as usage rows and prices know it: "brave".
	Name() string
	Search(ctx context.Context, query string, opts SearchOptions) ([]SearchHit, error)
}

// FetchOptions shapes one page read.
type FetchOptions struct {
	// MaxBytes stops reading the page after this many bytes.
	MaxBytes int
	// Format is "markdown" or "text".
	Format string
}

// FetchedPage is one page, read.
type FetchedPage struct {
	// URL is where the page ended up, after redirects.
	URL     string
	Title   string
	Content string
	// Truncated is true when the page was cut at MaxBytes.
	Truncated bool
}

// Fetcher reads one web page as clean text: our PageReader, Jina Reader…
type Fetcher interface {
	Name() string
	Fetch(ctx context.Context, url string, opts FetchOptions) (FetchedPage, error)
}

// BuiltinToolPorts are the adapters the built-in tools use. A tool whose
// port is missing cannot be asked for: BuiltinTools returns an error at
// startup rather than a run failing later.
type BuiltinToolPorts struct {
	Search  Search
	Fetcher Fetcher
}
