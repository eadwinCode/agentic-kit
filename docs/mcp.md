# MCP tools

An agent can use the tools of any MCP server. The adapter connects to the
server, lists its tools and turns each one into an ordinary tool. The agent
loop does not know they came from MCP, so approvals, stops and cost work as
they do for your own tools.

It speaks two transports:

- **stdio**: a local command. We start it and talk over stdin/stdout.
- **http**: a URL that speaks MCP over HTTP ("streamable HTTP"). Sessions and
  event-stream answers are handled for you.

It uses only `initialize`, `tools/list` and `tools/call`. There are no
resources, prompts or sampling yet.

## TS

```ts
import { connectMCPServers } from 'agentenkit/adapters/mcp';

const mcp = await connectMCPServers([
  { name: 'github', transport: { type: 'stdio', command: 'github-mcp-server', args: ['stdio'],
                                 env: { GITHUB_PERSONAL_ACCESS_TOKEN: process.env.GH_TOKEN! } },
    requiresConfirmation: ['create_issue'] },
  { name: 'docs', transport: { type: 'http', url: 'https://example.com/mcp',
                               headers: { authorization: `Bearer ${process.env.DOCS_KEY}` } } },
]);

runtime.createStreamTextAgent({ name: 'dev', model: 'claude-sonnet', tools: { ...mcp.tools, lookup } });

process.on('SIGTERM', () => mcp.close());
```

Use `connectMCP(server)` for a single server.

## Go

```go
import "github.com/eadwinCode/agentic-kit/packages/go-agentenkit/adapters/mcp"

conn, err := mcp.ConnectAll(ctx,
	mcp.Server{Name: "github", Stdio: &mcp.Stdio{Command: "github-mcp-server", Args: []string{"stdio"},
		Env: map[string]string{"GITHUB_PERSONAL_ACCESS_TOKEN": os.Getenv("GH_TOKEN")}},
		RequiresConfirmation: []string{"create_issue"}},
	mcp.Server{Name: "docs", HTTP: &mcp.HTTP{URL: "https://example.com/mcp",
		Headers: map[string]string{"Authorization": "Bearer " + os.Getenv("DOCS_KEY")}}},
)
if err != nil { return err }
defer conn.Close()

rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{Name: "dev", Tools: append(conn.Tools, lookup)})
```

Use `mcp.Connect(ctx, server)` for a single server.

## Options

| TS | Go | What it does |
|---|---|---|
| `name` | `Name` | Required. The default prefix of tool names. |
| `prefix` | `Prefix` | Put before each tool name. Default `<name>_`. Use `''` for none. |
| `tools` | `Tools` | Keep only these tools, by the server's names. A name the server lacks throws at connect. |
| `requiresConfirmation` | `RequiresConfirmation`, `ConfirmAll` | Park these tools behind an approval ([Human in the loop](./human-in-the-loop.md)). |
| `timeoutMs` | `Timeout` | Time limit for one request. Default 60s. |

## How it behaves

- **Names.** Tool names are `<prefix><tool>`, with anything outside
  `a-z A-Z 0-9 _ -` turned into `_`, cut to 64 characters. `connectMCPServers`
  and `ConnectAll` throw when two tools end up with the same name.
- **Answers.** The model gets the text parts joined by new lines. Anything that
  is not text shows as a short note like `[image image/png]`. With no content,
  the model gets the structured result as JSON.
- **Errors.** A tool error (`isError`), a timeout, or a server that is gone does
  not fail the run. The model gets `{"error": "..."}` and decides what to do.
  Connect errors do throw, so a bad setup shows up at startup.
- **Stops.** A stopped run cancels the call in flight and tells the server.
- **One connection per process.** Connect once at startup and share it with
  every run. Workers on other machines connect on their own.
