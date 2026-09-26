import type { FetchedPage, Fetcher, FetchOptions, Search, SearchHit, SearchOptions } from '../ports/tools.js';
import { domainAllowed, withSiteFilters } from './web-search-shared.js';

export interface JinaOptions {
  apiKey: string;
  baseUrl?: string;
  /** For tests. */
  fetch?: typeof fetch;
}

const SNIPPET_CHARS = 300;

/** Jina's result text is markdown; a snippet reads better as plain text.
 *  The Go adapter follows the same rules. */
export function plainSnippet(markdown: string): string {
  let s = markdown
    .replace(/!\[[^\]]*\]\([^)]*\)/g, '') // images
    .replace(/\[([^\]]*)\]\([^)]*\)/g, '$1') // links keep their text
    .replace(/\*\*|__|`/g, '') // bold and code marks
    .replace(/(^|[ \t\n\r\f\v])#{1,6}[ \t]/g, '$1') // heading marks
    .replace(/[ \t\n\r\f\v]+/g, ' ')
    .replace(/^ | $/g, '');
  if (s.length > SNIPPET_CHARS) {
    let end = SNIPPET_CHARS;
    const last = s.charCodeAt(end - 1);
    if (last >= 0xd800 && last <= 0xdbff) end--; // never split a character
    s = s.slice(0, end);
  }
  return s;
}

/** Web search on Jina's s.jina.ai: about $0.50 per 1,000 searches ($0.05 per
 *  million tokens, at least 10,000 a search), 100 requests a minute
 *  (checked 2026-09-26). Cheaper than Brave, slower and more limited: for
 *  low traffic where cost matters most. Jina returns each result's page
 *  text; only a short snippet is kept, so the model still opens only the
 *  pages it needs. It has no recency filter, so `recency` is ignored. */
export class JinaWebSearch implements Search {
  readonly name = 'jina-search';
  constructor(private readonly options: JinaOptions) {
    if (!options.apiKey) throw new Error('JinaWebSearch: apiKey is required');
  }

  async search(query: string, opts: SearchOptions): Promise<SearchHit[]> {
    const params = new URLSearchParams({ q: withSiteFilters(query, opts) });
    const doFetch = this.options.fetch ?? fetch;
    const res = await doFetch(`${this.options.baseUrl ?? 'https://s.jina.ai/'}?${params}`, {
      headers: { accept: 'application/json', authorization: `Bearer ${this.options.apiKey}` },
      ...(opts.signal ? { signal: opts.signal } : {}),
    });
    if (!res.ok) throw new Error(`jina: ${res.status} ${(await res.text()).slice(0, 200)}`);
    const body = (await res.json()) as {
      data?: Array<{ title?: string; url?: string; description?: string; content?: string; date?: string }>;
    };
    const hits: SearchHit[] = [];
    for (const r of body.data ?? []) {
      if (!r.url || !domainAllowed(r.url, opts)) continue;
      const snippet = plainSnippet(r.description || r.content || '');
      hits.push({ title: r.title ?? '', url: r.url, snippet, ...(r.date ? { publishedAt: r.date } : {}) });
      if (hits.length >= opts.maxResults) break;
    }
    return hits;
  }
}

/** Reads pages through Jina's r.jina.ai: pages built with JavaScript, PDFs,
 *  and sites that block plain fetches. About $0.25–$0.50 per 1,000 pages
 *  ($0.05 per million tokens returned). Jina fetches the page from its own
 *  servers, so your network is never reached. Use it where PageReader comes
 *  back empty or blocked. */
export class JinaReader implements Fetcher {
  readonly name = 'jina-reader';
  constructor(private readonly options: JinaOptions) {
    if (!options.apiKey) throw new Error('JinaReader: apiKey is required');
  }

  async fetch(url: string, opts: FetchOptions): Promise<FetchedPage> {
    const parsed = new URL(url);
    if (parsed.protocol !== 'http:' && parsed.protocol !== 'https:') {
      throw new Error(`jina: only http and https pages can be read, not ${url}`);
    }
    const doFetch = this.options.fetch ?? fetch;
    const res = await doFetch(`${this.options.baseUrl ?? 'https://r.jina.ai/'}${parsed.href}`, {
      headers: {
        accept: 'application/json',
        authorization: `Bearer ${this.options.apiKey}`,
        'x-return-format': opts.format === 'text' ? 'text' : 'markdown',
      },
      ...(opts.signal ? { signal: opts.signal } : {}),
    });
    if (!res.ok) throw new Error(`jina: ${res.status} ${(await res.text()).slice(0, 200)}`);
    const body = (await res.json()) as { data?: { title?: string; url?: string; content?: string } };
    const content = body.data?.content ?? '';
    const bytes = new TextEncoder().encode(content);
    const truncated = bytes.length > opts.maxBytes;
    return {
      url: body.data?.url || parsed.href,
      title: body.data?.title ?? '',
      content: truncated ? new TextDecoder().decode(bytes.subarray(0, opts.maxBytes)).replace(/�$/, '') : content,
      truncated,
    };
  }
}
