import type { Search, SearchHit, SearchOptions, SearchRecency } from '../ports/tools.js';
import { domainAllowed, withSiteFilters } from './web-search-shared.js';

export interface BraveWebSearchOptions {
  apiKey: string;
  /** Two-letter country the results are for, like 'us'. */
  country?: string;
  /** Language of the results, like 'en'. */
  searchLang?: string;
  baseUrl?: string;
  /** For tests. */
  fetch?: typeof fetch;
}

const FRESHNESS: Record<SearchRecency, string> = { day: 'pd', week: 'pw', month: 'pm', year: 'py' };

/** Web search on the Brave Search API: its own index of the web, $5 per
 *  1,000 searches with $5 free each month (checked 2026-09-26). The default
 *  search adapter. */
export class BraveWebSearch implements Search {
  readonly name = 'brave';
  constructor(private readonly options: BraveWebSearchOptions) {
    if (!options.apiKey) throw new Error('BraveWebSearch: apiKey is required');
  }

  async search(query: string, opts: SearchOptions): Promise<SearchHit[]> {
    const params = new URLSearchParams({
      q: withSiteFilters(query, opts),
      count: String(Math.min(Math.max(opts.maxResults, 1), 20)),
      text_decorations: 'false',
    });
    if (opts.recency) params.set('freshness', FRESHNESS[opts.recency]);
    if (this.options.country) params.set('country', this.options.country);
    if (this.options.searchLang) params.set('search_lang', this.options.searchLang);
    const doFetch = this.options.fetch ?? fetch;
    const res = await doFetch(`${this.options.baseUrl ?? 'https://api.search.brave.com/res/v1/web/search'}?${params}`, {
      headers: { accept: 'application/json', 'x-subscription-token': this.options.apiKey },
      ...(opts.signal ? { signal: opts.signal } : {}),
    });
    if (!res.ok) throw new Error(`brave: ${res.status} ${(await res.text()).slice(0, 200)}`);
    const body = (await res.json()) as {
      web?: { results?: Array<{ title?: string; url?: string; description?: string; page_age?: string }> };
    };
    const hits: SearchHit[] = [];
    for (const r of body.web?.results ?? []) {
      if (!r.url || !domainAllowed(r.url, opts)) continue;
      hits.push({
        title: r.title ?? '',
        url: r.url,
        snippet: (r.description ?? '').replace(/<[^>]*>/g, ''),
        ...(r.page_age ? { publishedAt: r.page_age } : {}),
      });
      if (hits.length >= opts.maxResults) break;
    }
    return hits;
  }
}
