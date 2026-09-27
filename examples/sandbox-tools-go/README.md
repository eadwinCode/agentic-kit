# Sandbox tools, in Go

The built-in `bash`, `code_execution` and `text_editor` tools, and nothing else. The thread gets one sandbox, kept between messages: files made in one message are there in the next. `bash` and file changes wait for your approval; running code and viewing files do not. A command's output shows under it as it runs.

It is one small app with nothing else to stand up: SQLite on disk, a queue in
the same process, and the shared page from [`tools-ui`](../tools-ui). The
setup is in [`main.go`](./main.go) (the routes are in [`server.go`](./server.go)). The same app in TypeScript is
[`sandbox-tools-ts`](../sandbox-tools-ts), on the same page.

## Run it

You need:

- `OPENAI_API_KEY`
- A sandbox, set with `SANDBOX`:
  - `docker` (the default): a Docker daemon. The first message pulls `python:3.12-slim`.
  - `e2b`: `E2B_API_KEY`.
  - `local`: nothing, but it is a folder on your machine with **no isolation**, so every tool asks first. Only for trying things out.

Put them in `.env` here (see [`.env.example`](./.env.example)), or in the
repository's root `.env`, which the app reads too.

```bash
bun install                               # once, at the repository root
bun run --cwd examples/tools-ui build     # once: the page
cd examples/sandbox-tools-go && go run .
```

Then open http://localhost:3104.

It listens on this machine only (`127.0.0.1`). `ADDR=:3104` opens it to your
network, but there is no login: anyone who can reach it can spend your keys
and approve commands in the sandbox.

## Try these

| Prompt | What it shows |
| :--- | :--- |
| *Use Python to find the first 20 prime numbers and save them to primes.txt.* | `code_execution`, and the files it made |
| *Make a file notes.md with three short lines, change the second line, then show me the file.* | `text_editor` create, `str_replace` and `view`, with approvals |
| *Create app/main.py that prints hello, then run it with bash.* | `text_editor` and `bash`, both asking first |
| *Use code_execution to print the numbers 1 to 8, one per second.* | output shown live as the program prints |
| a second message in the same thread | the files from the first are still there |

Open a tool call to see what it was sent and what came back. The header shows
what the thread has cost. See [Sandboxes](../../docs/sandboxes.md).
