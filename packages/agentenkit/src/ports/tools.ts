/** The ports behind the built-in tools. Each tool has one name and one input
 *  shape in every runtime; what does the work is an adapter you plug in here,
 *  like storage or the queue. */

/** How far back a search may look. */
export type SearchRecency = 'day' | 'week' | 'month' | 'year';

export interface SearchOptions {
  /** 1 to 10. */
  maxResults: number;
  /** Only results from these domains (and their subdomains). */
  allowedDomains?: string[];
  /** Never results from these domains (and their subdomains). */
  blockedDomains?: string[];
  recency?: SearchRecency;
  signal?: AbortSignal;
}

export interface SearchHit {
  title: string;
  url: string;
  /** A short piece of the page that matched: what the model reads to decide
   *  whether to open it. */
  snippet: string;
  /** ISO date, when the engine knows it. */
  publishedAt?: string;
}

/** A web search engine: Brave, Jina, Tavily… */
export interface Search {
  /** The adapter's name, as usage rows and prices know it: 'brave'. */
  readonly name: string;
  search(query: string, options: SearchOptions): Promise<SearchHit[]>;
}

export interface FetchOptions {
  /** Stop reading the page after this many bytes. */
  maxBytes: number;
  format: 'markdown' | 'text';
  signal?: AbortSignal;
}

export interface FetchedPage {
  /** Where the page ended up, after redirects. */
  url: string;
  title: string;
  content: string;
  /** True when the page was cut at `maxBytes`. */
  truncated: boolean;
}

/** Reads one web page as clean text: our PageReader, Jina Reader… */
export interface Fetcher {
  readonly name: string;
  fetch(url: string, options: FetchOptions): Promise<FetchedPage>;
}

/** The adapters the built-in tools use. A tool whose port is missing cannot
 *  be asked for: `builtinTools` throws at startup rather than a run failing
 *  later. */
export interface BuiltinToolPorts {
  search?: Search;
  fetcher?: Fetcher;
}
