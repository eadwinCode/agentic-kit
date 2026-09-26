import type { FetchedPage, Fetcher, FetchOptions } from '../ports/tools.js';
import { htmlToText } from '../core/builtin/html.js';

export interface PageReaderOptions {
  /** Most redirects followed. Default 5. */
  maxRedirects?: number;
  /** Per page, in ms. Default 30,000. */
  timeoutMs?: number;
  userAgent?: string;
  /** Reach private and local addresses (127.0.0.1, 10.x, 169.254.169.254,
   *  localhost…). Off by default: a prompt must never make the agent read
   *  your cloud metadata or admin pages. For tests and local development. */
  allowPrivate?: boolean;
  /** For tests. */
  fetch?: typeof fetch;
  /** For tests: the addresses a host name resolves to. */
  lookup?: (host: string) => Promise<string[]>;
}

const IPV4 = /^(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)(\.(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)){3}$/;

/** 4 for an IPv4 address, 6 for IPv6, 0 for anything else (a host name). */
export function ipVersion(s: string): 0 | 4 | 6 {
  if (IPV4.test(s)) return 4;
  return s.includes(':') && /^[0-9a-fA-F:.]+$/.test(s) ? 6 : 0;
}

/** Refused before any request is made. */
export class BlockedUrlError extends Error {
  constructor(url: string, why: string) {
    super(`page-reader: ${url} is not allowed: ${why}`);
    this.name = 'BlockedUrlError';
  }
}

/** True for an address the reader must never reach: loopback, private,
 *  link-local (cloud metadata), carrier NAT, multicast and reserved ranges,
 *  and an IPv6 address that maps onto one of them. */
export function isPrivateAddress(ip: string): boolean {
  if (ipVersion(ip) === 4) {
    const [a, b] = ip.split('.').map(Number) as [number, number, number, number];
    return (
      a === 0 || a === 10 || a === 127 || a >= 224 ||
      (a === 100 && b >= 64 && b <= 127) ||
      (a === 169 && b === 254) ||
      (a === 172 && b >= 16 && b <= 31) ||
      (a === 192 && b === 168) ||
      (a === 192 && b === 0 && ip.split('.')[2] === '0') ||
      (a === 198 && (b === 18 || b === 19))
    );
  }
  if (ipVersion(ip) === 6) {
    const v = ip.toLowerCase();
    const mapped = /^::ffff:(\d+\.\d+\.\d+\.\d+)$/.exec(v);
    if (mapped) return isPrivateAddress(mapped[1]!);
    return (
      v === '::' || v === '::1' ||
      /^f[cd]/.test(v) || // fc00::/7, unique local
      /^fe[89ab]/.test(v) || // fe80::/10, link-local
      /^ff/.test(v) // multicast
    );
  }
  return true; // not an address at all: refuse rather than guess
}

const LOCAL_NAMES = /(^|\.)(localhost|local|internal|localdomain|home\.arpa)$/i;

/** Our own page reader: fetches the page itself and turns it into markdown,
 *  with no service in between and nothing to pay. Refuses private and local
 *  addresses, checked again after every redirect. Node and Bun only: it
 *  resolves host names itself. A page built with JavaScript, a PDF, or a
 *  site that blocks plain fetches reads badly here; JinaReader is for those. */
export class PageReader implements Fetcher {
  readonly name = 'page-reader';
  private readonly opts: Required<Omit<PageReaderOptions, 'fetch' | 'lookup'>> &
    Pick<PageReaderOptions, 'fetch' | 'lookup'>;

  constructor(options: PageReaderOptions = {}) {
    this.opts = {
      maxRedirects: options.maxRedirects ?? 5,
      timeoutMs: options.timeoutMs ?? 30_000,
      userAgent: options.userAgent ?? 'agentenkit-page-reader (+https://github.com/eadwinCode/agentic-kit)',
      allowPrivate: options.allowPrivate ?? false,
      ...(options.fetch ? { fetch: options.fetch } : {}),
      ...(options.lookup ? { lookup: options.lookup } : {}),
    };
  }

  private async check(url: URL): Promise<void> {
    if (url.protocol !== 'http:' && url.protocol !== 'https:') {
      throw new BlockedUrlError(url.href, 'only http and https pages can be read');
    }
    if (this.opts.allowPrivate) return;
    const host = url.hostname.replace(/^\[|\]$/g, '');
    if (LOCAL_NAMES.test(host)) throw new BlockedUrlError(url.href, 'a local host name');
    const addresses = ipVersion(host)
      ? [host]
      : this.opts.lookup
        ? await this.opts.lookup(host)
        : await resolveHost(host);
    if (addresses.length === 0) throw new BlockedUrlError(url.href, 'the host does not resolve');
    for (const a of addresses) {
      if (isPrivateAddress(a)) throw new BlockedUrlError(url.href, `it resolves to a private address (${a})`);
    }
  }

  async fetch(url: string, options: FetchOptions): Promise<FetchedPage> {
    const doFetch = this.opts.fetch ?? fetch;
    const timeout = AbortSignal.timeout(this.opts.timeoutMs);
    const signal = options.signal ? AbortSignal.any([options.signal, timeout]) : timeout;
    let current = new URL(url);
    let res: Response | undefined;
    for (let hop = 0; ; hop++) {
      await this.check(current);
      res = await doFetch(current.href, {
        redirect: 'manual',
        signal,
        headers: { 'user-agent': this.opts.userAgent, accept: 'text/html,text/plain;q=0.9,*/*;q=0.5' },
      });
      const location = res.headers.get('location');
      if (res.status >= 300 && res.status < 400 && location) {
        if (hop >= this.opts.maxRedirects) throw new Error(`page-reader: more than ${this.opts.maxRedirects} redirects`);
        await res.body?.cancel();
        current = new URL(location, current);
        continue;
      }
      break;
    }
    if (!res.ok) throw new Error(`page-reader: ${current.href} answered ${res.status}`);
    const type = (res.headers.get('content-type') ?? '').split(';')[0]!.trim().toLowerCase();
    const readable = type === '' || type.startsWith('text/') || type === 'application/json' || type === 'application/xhtml+xml' || type === 'application/xml';
    if (!readable) {
      await res.body?.cancel();
      throw new Error(`page-reader: cannot read ${type} pages; use JinaReader for PDFs and other files`);
    }
    const { text, truncated } = await readCapped(res, options.maxBytes);
    const isHtml = type === '' || type === 'text/html' || type === 'application/xhtml+xml' || /^\s*<(!doctype|html)/i.test(text);
    if (!isHtml) return { url: current.href, title: '', content: text, truncated };
    const page = htmlToText(text, current.href, options.format);
    return { url: current.href, title: page.title, content: page.content, truncated };
  }
}

/** The body as text, stopping at `maxBytes`. */
async function readCapped(res: Response, maxBytes: number): Promise<{ text: string; truncated: boolean }> {
  if (!res.body) return { text: '', truncated: false };
  const reader = res.body.getReader();
  const chunks: Uint8Array[] = [];
  let size = 0;
  let truncated = false;
  for (;;) {
    const { value, done } = await reader.read();
    if (done) break;
    if (size + value.length > maxBytes) {
      chunks.push(value.subarray(0, maxBytes - size));
      size = maxBytes;
      truncated = true;
      await reader.cancel();
      break;
    }
    chunks.push(value);
    size += value.length;
  }
  const all = new Uint8Array(size);
  let at = 0;
  for (const c of chunks) {
    all.set(c, at);
    at += c.length;
  }
  return { text: new TextDecoder().decode(all), truncated };
}

/** Every address a host name resolves to. Node's resolver is loaded only
 *  here, so the package still loads where it does not exist. */
async function resolveHost(host: string): Promise<string[]> {
  const { lookup } = await import('node:dns/promises');
  return (await lookup(host, { all: true })).map((a) => a.address);
}
