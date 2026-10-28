# Flow queue, in Go

agentic-kit with [Clevix Flow](https://eadwincode.github.io/flow-sdk/) as its
queue. Every job (a new message, a resume after an approval, a park's expiry)
is a Flow run that you can watch in the Flow dashboard. The agent reads pages
with `web_fetch` and saves notes with `saveNote`, which waits for your
approval.

It is one small app: SQLite on disk, `clevixflow` for the queue, and the shared
page from [`tools-ui`](../tools-ui). The setup is in [`main.go`](./main.go). The
same app in TypeScript is [`flow-queue-ts`](../flow-queue-ts), on the same page.

## Run it

You need:

- `OPENAI_API_KEY`
- A Flow engine, v2.1.1 or later, and an app in its dashboard. Copy the app's
  signing key into `FLOW_SIGNING_KEY`.

Put them in `.env` here (see [`.env.example`](./.env.example)), or in the
repository's root `.env`, which the app reads too.

```bash
bun install                               # once, at the repository root
bun run --cwd examples/tools-ui build     # once: the page
cd examples/flow-queue-go && go run .
```

Then open http://localhost:3108.

### Let the engine reach the app

Flow calls the app at `/api/flow` to run each job, so the engine must be able
to reach it.

- **A local engine** (see Flow's
  [local development](https://eadwincode.github.io/flow-sdk/local-dev)):
  set `FLOW_BASE_URL=http://127.0.0.1:8787`. In the dashboard, connect the app
  with `http://127.0.0.1:3108/api/flow`. If the engine runs in Docker, use
  `http://host.docker.internal:3108/api/flow`.
- **The hosted engine**: leave `FLOW_BASE_URL` empty. Open a tunnel,
  for example `cloudflared tunnel --url http://localhost:3108`, and connect
  the app with the tunnel's URL and `/api/flow`. Give the app an ID of your own
  (`FLOW_APP_ID`), so it never replaces another app's functions.

The app syncs its functions when it starts. After you change them, restart it,
or click **Resync** in the dashboard.

It listens on this machine only (`127.0.0.1`). `ADDR=:3108` opens it to your
network, but there is no login: anyone who can reach it can spend your keys.

## Try these

| Prompt | What to see in the Flow dashboard |
| :--- | :--- |
| *Summarise https://en.wikipedia.org/wiki/Ada_Lovelace in three sentences.* | One `agentenkit-run` run, with its `handle` step |
| *Read https://en.wikipedia.org/wiki/Grace_Hopper and save a note with her three biggest firsts.* | The run parks on `saveNote`. Its expiry is a run asleep in `delay`. Approve it: the expiry run is cancelled and a resume run starts |

Each run has the tags `runId`, `threadId` and `kind`, so you can find a
thread's runs in the dashboard. See [Setup: On Clevix Flow](../../docs/setup.md#on-clevix-flow).
