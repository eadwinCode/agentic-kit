# Web tools

Two built-in tools let an agent search the web and read pages: `web_search`
and `web_fetch`. They have one name and one input shape in TypeScript and Go,
and the model sees them as ordinary function tools, so **any model that can
call tools can use them**. Provider tools such as Anthropic's `web_search`
only work on one provider.

What does the work is an adapter you plug in, like storage or the queue: Brave
or Jina for search, our own page reader or Jina for pages.

## Quick start

```ts
import { setupAgentCore, BraveWebSearch, PageReader, pricing } from 'agentenkit';

const runtime = await setupAgentCore({
  storage, bus, queue, kv, resolveModel,
  // The adapters. Keys are passed in here, at setup; nothing reads them later.
  tools: {
    search: new BraveWebSearch({ apiKey: process.env.BRAVE_API_KEY! }),
    fetcher: new PageReader(),
  },
  // Price searches too, so they count against a run's money cap.
  pricer: pricing.chain(pricing.table(modelPrices), pricing.tools({ brave: { perUse: 0.005 } })),
});

const researcher = runtime.createStreamTextAgent({
  name: 'researcher',
  tools: { ...runtime.builtinTools(['web_search', 'web_fetch']), lookupInvoice },
});
```

```go
search, err := brave.New(os.Getenv("BRAVE_API_KEY"))
if err != nil {
	log.Fatal(err)
}
rt, err := agentenkit.SetupAgentCore(ctx, agentenkit.RuntimeOptions{
	Storage: storage, Bus: bus, Queue: queue, Kv: kv, ResolveModel: resolve,
	// The adapters. Keys are passed in here, at setup; nothing reads them later.
	Tools:  agentenkit.BuiltinToolPorts{Search: search, Fetcher: pagereader.New(pagereader.Options{})},
	Pricer: pricing.Chain(modelPrices, pricing.Tools{"brave": {PerUse: 0.005}}),
})

web, err := rt.BuiltinTools([]string{"web_search", "web_fetch"}, agentenkit.BuiltinToolOptions{})
researcher := rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{
	Name: "researcher", Tools: append(web, lookupInvoice),
})
```

Asking for a tool whose adapter was not set up is an error at startup, not a
failed run: `builtinTools(['web_search'])` throws (Go: returns an error) when
there is no `search`.

## The two tools

### `web_search`

The model sends a query; it gets back short results, each with an id.

| Input | |
| :--- | :--- |
| `query` | What to search for. |
| `maxResults` | 1 to 10. Default 5, or the app's `maxResults`. |

```json
{ "results": [
  { "id": "s1r1", "title": "Specification - Model Context Protocol",
    "url": "https://modelcontextprotocol.io/specification/2026-07-28",
    "snippet": "Model Context Protocol (MCP) is an open protocol…", "publishedAt": "2026-09-24T01:00:00" }
] }
```

**Every result has an id**, unique in the thread: `s2r3` is the third result
of the thread's second search. `web_fetch` takes the id in place of a URL, so
the model opens a result without copying a long URL, and cannot open a URL it
got wrong. The ids stay in the saved conversation, so a later message in the
same thread opens the same page by the same id.

The model cannot set domain filters or how recent results must be. Small
models fill in every field they are offered and narrow the search by mistake
(asked about the MCP spec, `gpt-4o-mini` limited it to `w3.org` and "the last
month"). So those are the app's to set, as in Anthropic's own web search:
see [Settings](#settings).

### `web_fetch`

Reads one page.

| Input | |
| :--- | :--- |
| `url` | The page, starting with `http://` or `https://`. |
| `id` | A `web_search` result's id, in place of `url`. |
| `prompt` | What the model wants to know from the page. Optional. |

Without a `prompt`, the page text comes back as markdown, cut at
`builtinToolResultCapChars` (default 20,000 characters):

```json
{ "url": "https://example.com/", "title": "Example Domain", "content": "# Example Domain\n\n…", "truncated": false }
```

With a `prompt`, the page never reaches the main model:

```json
{ "url": "https://modelcontextprotocol.io/specification/2026-07-28", "title": "Specification",
  "answer": "The latest version is 2026-07-28." }
```

## Reading a page with a question

When `web_fetch` gets a `prompt`, a small, cheap model reads the page and
answers, and only that answer goes back. This is what Claude Code's WebFetch
does, and it has three uses:

- **The context stays small.** A 50-page document costs the main model a few
  lines, not 30,000 tokens on every later step.
- **Hidden instructions reach a model with no tools.** Text on a page that
  tells the agent to do something goes to the small model, which can only
  answer the question.
- **It is cheap.** The reader runs on a small model at a fraction of the main
  model's price.

The reader is the agent's `webFetch.model` (a registry key), else the config's
`compactionModel`. A page longer than the reader's context window is cut to
fit (about 3 characters a token), and the answer then says `truncated: true`.
The reader's call is billed like any other model call: a usage row with
`kind: 'tool'`, on the run, under its caps.

## Adapters

| Adapter | TS | Go | Port | Price (checked 2026-09-26) | Use it for |
| :--- | :--- | :--- | :--- | :--- | :--- |
| Brave | `BraveWebSearch` | `brave.New` | search | $5 per 1,000 searches; $5 free each month | The default search: its own index, fast, zero data retention |
| Jina search | `JinaWebSearch` | `jina.NewWebSearch` | search | about $0.50 per 1,000 searches | Low traffic where cost matters most. Slower, 100 requests a minute; no recency filter |
| Page reader | `PageReader` | `pagereader.New` | fetcher | free | The default reader |
| Jina reader | `JinaReader` | `jina.NewReader` | fetcher | about $0.25–$0.50 per 1,000 pages | Pages built with JavaScript, PDFs, sites that block plain fetches |
| Memory | `MemorySearch`, `MemoryFetcher` | `memory.NewSearch`, `memory.NewFetcher` | both | free | Tests |

Every key is an argument to the adapter, given at setup. The adapters never
read the environment, a missing key fails at setup, and where the key comes
from (a secret store, an env var) is your app's choice.

- **Brave:** sign up at [api-dashboard.search.brave.com](https://api-dashboard.search.brave.com),
  subscribe to the Search plan (a card is asked for even for the free credit),
  and create a key under API Keys.
- **Jina:** get a key at [jina.ai](https://jina.ai/api-dashboard). The free
  tokens are for non-commercial use only.

### Our page reader

`PageReader` fetches the page itself and turns it into markdown with no
service in between. It keeps only the main part of the page (the first
`article`, else `main`, else `body`) and drops scripts, styles, navigation,
headers, footers and forms. The rules are the same, word for word, in both
runtimes, and a shared set of example pages checks it
(`packages/parity/tools/page-reader/`).

It **refuses private and local addresses**: `127.0.0.1`, `10.x`, `192.168.x`,
`169.254.169.254` (cloud metadata), `localhost`, `*.internal` and IPv6 forms of
the same. The check runs before every request and after every redirect; in Go
it also runs on the address actually connected to, so a host name that
resolves differently between the check and the connect is refused too.
Without this, a prompt could make the agent read your cloud credentials or
your admin pages. `allowPrivate` (Go: `AllowPrivate`) turns it off, for tests
and local development only.

In TypeScript it runs on Node and Bun: it resolves host names itself. It reads
text, HTML, JSON and XML; for a PDF or other files use the Jina reader.

## Settings

The second argument to `builtinTools` (Go: `BuiltinToolOptions`) is the app's:

| `webSearch` | Default | |
| :--- | :--- | :--- |
| `maxResults` | 5 | Results when the model does not say. Never more than 10. |
| `maxUses` | no limit | Searches allowed in one run. Past it the tool answers with an error the model can read, and the run goes on. |
| `allowedDomains` | — | Only results from these domains and their subdomains. |
| `blockedDomains` | — | Never results from these domains. |
| `recency` | — | `'day'`, `'week'`, `'month'` or `'year'`. |

| `webFetch` | Default | |
| :--- | :--- | :--- |
| `maxBytes` | 2,000,000 | Stop reading a page after this many bytes. |
| `maxUses` | no limit | Pages read in one run. |
| `model` | `compactionModel` | The small model that answers a `prompt`. |

```ts
runtime.builtinTools(['web_search', 'web_fetch'], {
  webSearch: { maxUses: 10, allowedDomains: ['docs.stripe.com'], recency: 'year' },
  webFetch: { maxUses: 20, model: 'gpt-4o-mini' },
});
```

## Subagents

Give the tools to children through `subagents.tools`, and a researcher
subagent searches and reads on its own:

```ts
const web = runtime.builtinTools(['web_search', 'web_fetch']);
runtime.createStreamTextAgent({
  name: 'chat',
  tools: { ...web },
  subagents: { tools: { ...web } },
});
```

A child's searches are billed to the parent's run, with the child's name on
the usage row, and count against the same money cap. Only the main agent's
sources go on the run stream.

## On the run stream

Search results and pages read go out as `SOURCE` events while the run works,
so a UI can list sources as they arrive:

```json
{ "type": "SOURCE", "source": { "sourceType": "url", "id": "s1r1", "url": "https://…", "title": "…" } }
```

## Cost

Each search and each page read writes a usage row, next to the model calls:

| Field | Value |
| :--- | :--- |
| `kind` | `'tool'` |
| `model` | `'tool:web_search'` or `'tool:web_fetch'` |
| `modelId` | the adapter: `'brave'`, `'jina-search'`, `'page-reader'`, `'jina-reader'` |
| `providerMetadata` | `{ tool, adapter, uses: 1 }` |

`pricing.tools` (Go: `pricing.Tools`) prices those rows per use, keyed by
adapter, or by tool name as a fallback. Chain it with the model table, and a
run that searches in a loop stops at its money cap like one that talks too
much. See [Cost and pricing](./cost-and-pricing.md).

## Writing your own adapter

A search engine is one method; a reader is one method:

```ts
import type { Search, Fetcher } from 'agentenkit';

class MySearch implements Search {
  readonly name = 'my-search'; // what usage rows and prices know it by
  async search(query, { maxResults, allowedDomains, blockedDomains, recency, signal }) {
    return [{ title, url, snippet, publishedAt }];
  }
}

class MyReader implements Fetcher {
  readonly name = 'my-reader';
  async fetch(url, { maxBytes, format, signal }) {
    return { url: finalUrl, title, content, truncated };
  }
}
```

```go
type MySearch struct{}

func (MySearch) Name() string { return "my-search" }
func (MySearch) Search(ctx context.Context, query string, o ports.SearchOptions) ([]ports.SearchHit, error) {
	return []ports.SearchHit{{Title: title, URL: url, Snippet: snippet}}, nil
}

type MyReader struct{}

func (MyReader) Name() string { return "my-reader" }
func (MyReader) Fetch(ctx context.Context, url string, o ports.FetchOptions) (ports.FetchedPage, error) {
	return ports.FetchedPage{URL: finalURL, Title: title, Content: content}, nil
}
```

An engine that cannot filter by domain or recency may ignore them: the shipped
adapters also add `site:` operators to the query and check each result's
domain again.

## Errors

A tool that cannot do its work answers the model with `{ "error": "…" }`
instead of failing the run: an unknown result id, a page that answered 404, a
refused address, `maxUses` used up. The model reads it and tries something
else.
