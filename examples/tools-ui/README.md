# tools-ui

The one page the four tool examples share:
[web-tools-ts](../web-tools-ts), [web-tools-go](../web-tools-go),
[sandbox-tools-ts](../sandbox-tools-ts) and [sandbox-tools-go](../sandbox-tools-go).

It is a chat over [`use-agentenkit`](../../packages/use-agentenkit): threads,
each tool call with its input and result, a command's output as it runs,
approval cards, and what the thread has cost. The server says what it is at
`/api/info` (a title, its tools, prompts to try, setup notes), so the same
build serves all four.

```bash
bun run --cwd examples/tools-ui build   # once; the servers serve dist/
```

To work on the page itself, run an example, then `bun run dev` here: Vite
serves the page on http://localhost:5174 and forwards `/api` to the example on
`API` (default `http://localhost:3101`, web-tools-ts).
