// Package websearch holds what the search adapters share: the domain lists.
package websearch

import (
	"net/url"
	"strings"

	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

// InDomain says whether a host is the domain or one of its subdomains.
func InDomain(host, domain string) bool {
	d := strings.TrimPrefix(strings.TrimPrefix(strings.ToLower(domain), "*"), ".")
	return host == d || strings.HasSuffix(host, "."+d)
}

// DomainAllowed says whether a result's URL passes the allowed and blocked
// domain lists. The engines are asked too (see WithSiteFilters), but not all
// of them honour every operator, so each result is checked again here.
func DomainAllowed(raw string, opts ports.SearchOptions) bool {
	u, err := url.Parse(raw)
	if err != nil || u.Hostname() == "" {
		return false
	}
	host := strings.ToLower(u.Hostname())
	for _, d := range opts.BlockedDomains {
		if InDomain(host, d) {
			return false
		}
	}
	if len(opts.AllowedDomains) == 0 {
		return true
	}
	for _, d := range opts.AllowedDomains {
		if InDomain(host, d) {
			return true
		}
	}
	return false
}

// WithSiteFilters is the query with site: operators for the domain lists,
// which most search engines understand.
func WithSiteFilters(query string, opts ports.SearchOptions) string {
	q := strings.TrimSpace(query)
	switch len(opts.AllowedDomains) {
	case 0:
	case 1:
		q += " site:" + opts.AllowedDomains[0]
	default:
		parts := make([]string, len(opts.AllowedDomains))
		for i, d := range opts.AllowedDomains {
			parts[i] = "site:" + d
		}
		q += " (" + strings.Join(parts, " OR ") + ")"
	}
	for _, d := range opts.BlockedDomains {
		q += " -site:" + d
	}
	return q
}
