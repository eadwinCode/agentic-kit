import type { SearchOptions } from '../ports/tools.js';

const hostOf = (url: string): string | null => {
  try {
    return new URL(url).hostname.toLowerCase();
  } catch {
    return null;
  }
};

/** A host is in a domain when it is the domain or one of its subdomains. */
export const inDomain = (host: string, domain: string): boolean => {
  const d = domain.toLowerCase().replace(/^\*?\./, '');
  return host === d || host.endsWith(`.${d}`);
};

/** Whether a result's URL passes the allowed and blocked domain lists. The
 *  engines are asked too (see withSiteFilters), but not all of them honour
 *  every operator, so each result is checked again here. */
export function domainAllowed(url: string, opts: Pick<SearchOptions, 'allowedDomains' | 'blockedDomains'>): boolean {
  const host = hostOf(url);
  if (!host) return false;
  if (opts.blockedDomains?.some((d) => inDomain(host, d))) return false;
  if (opts.allowedDomains?.length && !opts.allowedDomains.some((d) => inDomain(host, d))) return false;
  return true;
}

/** The query with `site:` operators for the domain lists, which most search
 *  engines understand. */
export function withSiteFilters(query: string, opts: Pick<SearchOptions, 'allowedDomains' | 'blockedDomains'>): string {
  let q = query.trim();
  const allowed = opts.allowedDomains ?? [];
  if (allowed.length === 1) q += ` site:${allowed[0]}`;
  else if (allowed.length > 1) q += ` (${allowed.map((d) => `site:${d}`).join(' OR ')})`;
  for (const d of opts.blockedDomains ?? []) q += ` -site:${d}`;
  return q;
}
