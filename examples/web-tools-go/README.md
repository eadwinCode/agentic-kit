# Web tools, in Go

The built-in `web_search` and `web_fetch` tools, and nothing else. The agent searches the web, opens results by their id, and has a small model read a page and answer a question about it, so a long page never fills the main context. Subagents search on their own, billed to the same run.

It is one small app with nothing else to stand up: SQLite on disk, a queue in
the same process, and the shared page from [`tools-ui`](../tools-ui). The
setup is in [`main.go`](./main.go) (the routes are in [`server.go`](./server.go)). The same app in TypeScript is
[`web-tools-ts`](../web-tools-ts), on the same page.

## Run it

You need:

- `OPENAI_API_KEY`
- `BRAVE_API_KEY` or `JINA_API_KEY` for search. Without one the agent can still read pages, but not search.

Put them in `.env` here (see [`.env.example`](./.env.example)), or in the
repository's root `.env`, which the app reads too.

```bash
bun install                               # once, at the repository root
bun run --cwd examples/tools-ui build     # once: the page
cd examples/web-tools-go && go run .
```

Then open http://localhost:3102.

## Try these

| Prompt | What it shows |
| :--- | :--- |
| *What is the latest version of the MCP spec? Cite the page.* | `web_search`, then `web_fetch` on a result id with a prompt |
| *Summarise https://en.wikipedia.org/wiki/Ada_Lovelace in three sentences.* | `web_fetch` on a URL |
| *Compare the three biggest vector databases, one subagent each.* | subagents that search and read on their own |

Open a tool call to see what it was sent and what came back. The header shows
what the thread has cost. See [Web tools](../../docs/web-tools.md).
