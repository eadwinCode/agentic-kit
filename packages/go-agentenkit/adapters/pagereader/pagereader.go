// Package pagereader is our own page reader for the web_fetch built-in tool:
// it fetches the page itself and turns it into markdown, with no service in
// between and nothing to pay. It refuses private and local addresses,
// checked before every request and every redirect, and again on the address
// it actually connects to. A page built with JavaScript, a PDF, or a site
// that blocks plain fetches reads badly here; the jina reader is for those.
package pagereader

import (
	"context"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"syscall"
	"time"

	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/core"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

// Options shape a Reader. The zero value is the safe default.
type Options struct {
	// MaxRedirects is the most redirects followed. Default 5.
	MaxRedirects int
	// Timeout is per page. Default 30s.
	Timeout   time.Duration
	UserAgent string
	// AllowPrivate reaches private and local addresses (127.0.0.1, 10.x,
	// 169.254.169.254, localhost…). Off by default: a prompt must never make
	// the agent read your cloud metadata or admin pages. For tests and local
	// development.
	AllowPrivate bool
	// Lookup is for tests: the addresses a host name resolves to.
	Lookup func(ctx context.Context, host string) ([]string, error)
	// Transport is for tests.
	Transport http.RoundTripper
}

// Reader is a ports.Fetcher that reads pages itself.
type Reader struct {
	opts   Options
	client *http.Client
}

// ErrBlocked is returned, wrapped, for an address the reader must not reach.
var ErrBlocked = errors.New("page-reader: not allowed")

func blocked(u, why string) error { return fmt.Errorf("%w: %s: %s", ErrBlocked, u, why) }

// New is a page reader.
func New(opts Options) *Reader {
	if opts.MaxRedirects == 0 {
		opts.MaxRedirects = 5
	}
	if opts.Timeout == 0 {
		opts.Timeout = 30 * time.Second
	}
	if opts.UserAgent == "" {
		opts.UserAgent = "agentenkit-page-reader (+https://github.com/eadwinCode/agentic-kit)"
	}
	r := &Reader{opts: opts}
	transport := opts.Transport
	if transport == nil {
		dialer := &net.Dialer{Timeout: 10 * time.Second}
		if !opts.AllowPrivate {
			// The address actually connected to is checked too, so a host
			// that resolves differently between the check and the connect
			// (DNS rebinding) is still refused.
			dialer.Control = func(_, address string, _ syscall.RawConn) error {
				host, _, err := net.SplitHostPort(address)
				if err != nil {
					return err
				}
				if IsPrivateAddress(host) {
					return blocked(address, "it resolves to a private address ("+host+")")
				}
				return nil
			}
		}
		transport = &http.Transport{DialContext: dialer.DialContext, Proxy: nil, TLSHandshakeTimeout: 10 * time.Second}
	}
	r.client = &http.Client{
		Transport: transport,
		Timeout:   opts.Timeout,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) > opts.MaxRedirects {
				return fmt.Errorf("page-reader: more than %d redirects", opts.MaxRedirects)
			}
			return r.check(req.Context(), req.URL)
		},
	}
	return r
}

func (r *Reader) Name() string { return "page-reader" }

var reLocalName = regexp.MustCompile(`(?i)(^|\.)(localhost|local|internal|localdomain|home\.arpa)$`)

// IsPrivateAddress is true for an address the reader must never reach:
// loopback, private, link-local (cloud metadata), carrier NAT, multicast and
// reserved ranges, and an IPv6 address that maps onto one of them. Anything
// that is not an address at all is refused rather than guessed at.
func IsPrivateAddress(s string) bool {
	ip := net.ParseIP(s)
	if ip == nil {
		return true
	}
	if v4 := ip.To4(); v4 != nil {
		a, b := v4[0], v4[1]
		return a == 0 || a == 10 || a == 127 || a >= 224 ||
			(a == 100 && b >= 64 && b <= 127) ||
			(a == 169 && b == 254) ||
			(a == 172 && b >= 16 && b <= 31) ||
			(a == 192 && b == 168) ||
			(a == 192 && b == 0 && v4[2] == 0) ||
			(a == 198 && (b == 18 || b == 19))
	}
	return ip.IsUnspecified() || ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsMulticast()
}

func (r *Reader) check(ctx context.Context, u *url.URL) error {
	if u.Scheme != "http" && u.Scheme != "https" {
		return blocked(u.String(), "only http and https pages can be read")
	}
	if r.opts.AllowPrivate {
		return nil
	}
	host := u.Hostname()
	if reLocalName.MatchString(host) {
		return blocked(u.String(), "a local host name")
	}
	var addrs []string
	if net.ParseIP(host) != nil {
		addrs = []string{host}
	} else if r.opts.Lookup != nil {
		var err error
		if addrs, err = r.opts.Lookup(ctx, host); err != nil {
			return err
		}
	} else {
		var err error
		if addrs, err = net.DefaultResolver.LookupHost(ctx, host); err != nil {
			return err
		}
	}
	if len(addrs) == 0 {
		return blocked(u.String(), "the host does not resolve")
	}
	for _, a := range addrs {
		if IsPrivateAddress(a) {
			return blocked(u.String(), "it resolves to a private address ("+a+")")
		}
	}
	return nil
}

var reHTMLStart = regexp.MustCompile(`(?i)^\s*<(!doctype|html)`)

func (r *Reader) Fetch(ctx context.Context, target string, opts ports.FetchOptions) (ports.FetchedPage, error) {
	u, err := url.Parse(target)
	if err != nil {
		return ports.FetchedPage{}, fmt.Errorf("page-reader: %w", err)
	}
	if err := r.check(ctx, u); err != nil {
		return ports.FetchedPage{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return ports.FetchedPage{}, err
	}
	req.Header.Set("User-Agent", r.opts.UserAgent)
	req.Header.Set("Accept", "text/html,text/plain;q=0.9,*/*;q=0.5")
	res, err := r.client.Do(req)
	if err != nil {
		return ports.FetchedPage{}, err
	}
	defer res.Body.Close()
	final := u.String()
	if res.Request != nil && res.Request.URL != nil {
		final = res.Request.URL.String() // after redirects
	}
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return ports.FetchedPage{}, fmt.Errorf("page-reader: %s answered %d", final, res.StatusCode)
	}
	typ, _, _ := mime.ParseMediaType(res.Header.Get("Content-Type"))
	typ = strings.ToLower(typ)
	readable := typ == "" || strings.HasPrefix(typ, "text/") || typ == "application/json" || typ == "application/xhtml+xml" || typ == "application/xml"
	if !readable {
		return ports.FetchedPage{}, fmt.Errorf("page-reader: cannot read %s pages; use the jina reader for PDFs and other files", typ)
	}
	body, err := io.ReadAll(io.LimitReader(res.Body, int64(opts.MaxBytes)+1))
	if err != nil {
		return ports.FetchedPage{}, err
	}
	truncated := len(body) > opts.MaxBytes
	if truncated {
		body = body[:opts.MaxBytes]
	}
	text := strings.ToValidUTF8(string(body), "�")
	isHTML := typ == "" || typ == "text/html" || typ == "application/xhtml+xml" || reHTMLStart.MatchString(text)
	if !isHTML {
		return ports.FetchedPage{URL: final, Content: text, Truncated: truncated}, nil
	}
	page := core.HTMLToText(text, final, opts.Format)
	return ports.FetchedPage{URL: final, Title: page.Title, Content: page.Content, Truncated: truncated}, nil
}
