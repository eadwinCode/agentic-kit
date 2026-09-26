/** Our own HTML to markdown, for PageReader. Deliberately small: a fixed list
 *  of rules the Go runtime follows word for word, so the same page reads the
 *  same in both (`packages/parity/tools/page-reader/` holds the cases). It
 *  is not a full HTML parser; a page it reads badly is what JinaReader is
 *  for. */

/** Elements dropped with everything inside them: code, chrome and forms. */
const DROP = [
  'script', 'style', 'noscript', 'template', 'svg', 'head', 'nav', 'footer', 'header', 'aside', 'form',
  'iframe', 'button', 'select', 'canvas',
];

/** Elements that end a block of text. */
const BLOCK = [
  'p', 'div', 'section', 'article', 'main', 'ul', 'ol', 'table', 'tr', 'blockquote', 'pre', 'figure',
  'figcaption', 'dl', 'dt', 'dd', 'hr',
];

const ENTITIES: Record<string, string> = {
  amp: '&', lt: '<', gt: '>', quot: '"', apos: "'", nbsp: ' ', ndash: '–', mdash: '—',
  hellip: '…', lsquo: '‘', rsquo: '’', ldquo: '“', rdquo: '”', copy: '©',
};

// The spaces this reader knows, spelled out: JavaScript's \s and trim() and
// Go's differ, and the two runtimes must read a page the same.
const SPACE = '[ \\t\\r\\f\\v\\u00a0]'; // within a line
const WS = '[ \\t\\n\\r\\f\\v\\u00a0]'; // across lines
const SPACE_RUN = new RegExp(`${SPACE}+`, 'g');
const WS_RUN = new RegExp(`${WS}+`, 'g');
const WS_ENDS = new RegExp(`^${WS}+|${WS}+$`, 'g');
const TAG = /<[^>]*>/g;

const trimWs = (s: string): string => s.replace(WS_ENDS, '');
/** Text with its tags taken out, on one line. */
const oneLine = (s: string): string => trimWs(s.replace(TAG, '').replace(WS_RUN, ' '));

export function decodeEntities(s: string): string {
  return s.replace(/&(#[xX][0-9a-fA-F]+|#[0-9]+|[a-zA-Z]+);/g, (whole, name: string) => {
    if (name[0] === '#') {
      const code = name[1] === 'x' || name[1] === 'X' ? parseInt(name.slice(2), 16) : parseInt(name.slice(1), 10);
      const ok = code > 0 && code <= 0x10ffff && !(code >= 0xd800 && code <= 0xdfff);
      return ok ? String.fromCodePoint(code) : whole;
    }
    return ENTITIES[name.toLowerCase()] ?? whole;
  });
}

/** Collapse spaces on each line, drop empty runs of lines, trim. */
function tidy(s: string): string {
  const lines = s.split('\n').map((l) => trimWs(l.replace(SPACE_RUN, ' ')));
  return trimWs(lines.join('\n').replace(/\n{3,}/g, '\n\n'));
}

function inner(html: string, tag: string): string | null {
  const m = new RegExp(`<${tag}(?:\\s[^>]*)?>([\\s\\S]*?)</${tag}\\s*>`, 'i').exec(html);
  return m ? m[1]! : null;
}

function resolveHref(href: string, base: string): string | null {
  const h = trimWs(decodeEntities(href));
  if (!h || h.startsWith('#') || /^(javascript|mailto|tel|data):/i.test(h)) return null;
  try {
    const u = new URL(h, base);
    return u.protocol === 'http:' || u.protocol === 'https:' ? u.href : null;
  } catch {
    return null;
  }
}

export interface ReadPage {
  title: string;
  content: string;
}

/** A page's title and its main text, as markdown or plain text. */
export function htmlToText(html: string, baseUrl: string, format: 'markdown' | 'text'): ReadPage {
  const rawTitle = inner(html, 'title');
  const title = rawTitle === null ? '' : oneLine(decodeEntities(rawTitle));

  let s = html.replace(/<!--[\s\S]*?-->/g, '');
  // The main part of the page: the first article, else main, else body.
  s = inner(s, 'article') ?? inner(s, 'main') ?? inner(s, 'body') ?? s;
  for (const tag of DROP) {
    s = s.replace(new RegExp(`<${tag}(?:\\s[^>]*)?>[\\s\\S]*?</${tag}\\s*>`, 'gi'), '');
    s = s.replace(new RegExp(`<${tag}(?:\\s[^>]*)?/?>`, 'gi'), '');
  }

  if (format === 'markdown') {
    for (let n = 1; n <= 6; n++) {
      s = s.replace(
        new RegExp(`<h${n}(?:\\s[^>]*)?>([\\s\\S]*?)</h${n}\\s*>`, 'gi'),
        (_m, text: string) => `\n\n${'#'.repeat(n)} ${oneLine(text)}\n\n`,
      );
    }
    s = s.replace(/<a\s[^>]*?href\s*=\s*"([^"]*)"[^>]*>([\s\S]*?)<\/a\s*>/gi, (_m, href: string, text: string) => {
      const label = oneLine(text);
      const url = resolveHref(href, baseUrl);
      return url && label ? `[${label}](${url})` : label;
    });
    s = s.replace(/<li(?:\s[^>]*)?>/gi, '\n- ');
  } else {
    for (let n = 1; n <= 6; n++) s = s.replace(new RegExp(`</?h${n}(?:\\s[^>]*)?>`, 'gi'), '\n\n');
    s = s.replace(/<li(?:\s[^>]*)?>/gi, '\n');
  }
  s = s.replace(/<br\s*\/?>/gi, '\n');
  for (const tag of BLOCK) s = s.replace(new RegExp(`</?${tag}(?:\\s[^>]*)?/?>`, 'gi'), '\n\n');
  s = s.replace(/<\/?(?:td|th)(?:\s[^>]*)?>/gi, ' ');
  s = s.replace(TAG, '');
  return { title, content: tidy(decodeEntities(s)) };
}
