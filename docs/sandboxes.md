# Sandboxes

A sandbox is a place apart from your app where an agent's commands run and
its files live: a Docker container, a hosted machine on E2B, or a folder on
your machine in development. The built-in tools `bash`, `code_execution` and
`text_editor` work in it (see [below](#the-sandbox-tools)), and your own tools
can too.

Like storage or the queue, it is a port you plug an adapter into. The
interface has the same shape and names as [ComputeSDK](https://www.computesdk.com)'s,
in TypeScript and Go, so a new provider is one small adapter.

## Quick start

To try the sandbox tools first, run
[sandbox-tools-ts](https://github.com/eadwinCode/agentic-kit/tree/main/examples/sandbox-tools-ts) or
[sandbox-tools-go](https://github.com/eadwinCode/agentic-kit/tree/main/examples/sandbox-tools-go): one small app each, on a
Docker sandbox by default.

```ts
import { setupAgentCore, DockerSandbox, E2BSandbox, LocalSandbox } from 'agentenkit';

const runtime = await setupAgentCore({
  storage, bus, queue, kv, resolveModel,
  tools: {
    // Pick one. Keys are passed in here, at setup; nothing reads them later.
    sandbox: new E2BSandbox({ apiKey: process.env.E2B_API_KEY! }),
    // sandbox: new DockerSandbox({ image: 'python:3.12-slim' }),
    // sandbox: new LocalSandbox(), // development only
  },
});
```

```go
sandbox, err := e2b.New(os.Getenv("E2B_API_KEY"), e2b.Options{})
if err != nil {
	log.Fatal(err)
}
rt, err := agentenkit.SetupAgentCore(ctx, agentenkit.RuntimeOptions{
	Storage: storage, Bus: bus, Queue: queue, Kv: kv, ResolveModel: resolve,
	Tools: agentenkit.BuiltinToolPorts{Sandbox: sandbox},
	// or docker.New(docker.Options{}), or localsandbox.New(localsandbox.Options{})
})
```

## One sandbox per thread

A thread has one sandbox, kept between messages:

- The first sandbox call in a thread makes it. Its id is kept in the kv under
  the thread.
- Every later call finds the same one: the next message, a resume after an
  approval, a retry, another worker. So a coding agent builds on what it did
  before.
- One run at a time uses it, because the run lock allows one run per thread.
  A run's subagents share it.
- It ends when the thread is deleted, after it has sat unused for
  `sandboxIdleTtlMs` (default 30 minutes), or `sandboxMaxLifetimeMs` after it
  was made (default 24 hours).

Each use pushes the sandbox's own end back, so an idle sandbox shuts itself
down even when your app is not running to end it. You do not pay for a thread
someone walked away from.

When a thread's sandbox has ended, the next call makes a fresh one and says
the old files are lost (`lost: true`), so a tool can tell the model not to
expect them.

| Setting | TS | Go | Default |
| --- | --- | --- | --- |
| Idle time before it ends | `sandboxIdleTtlMs` | `SandboxIdleTTL` | 30 minutes |
| Longest it lives | `sandboxMaxLifetimeMs` | `SandboxMaxLifetime` | 24 hours |
| One per thread or per run | `sandboxScope` | `SandboxScope` | `'thread'` |
| Keep it while a run is parked | `sandboxKeepOnPark` | `SandboxKeepOnPark` | `false` |
| Options for every sandbox made | `sandboxDefaults` | `SandboxDefaults` | none |
| Where the tools keep their files | `sandboxStateDir` | `SandboxStateDir` | `.agentenkit` |

## One sandbox per run

When sandboxes come from a small shared pool, hold one only while the agent
works. With `sandboxScope: 'run'` (Go: `SandboxScope: agentenkit.SandboxScopeRun`):

- The first sandbox call in a run makes it. The run's later calls and its
  subagents share it.
- When the run ends (finished, failed or stopped), the runtime ends the
  sandbox. The next run in the thread starts with a fresh one, with no
  "lost" note: that is the normal case here.
- **When the run parks** (waiting for an approval, an answer, or a job), the
  sandbox is ended too, and the resumed run gets a fresh one. An approval can
  wait for hours; the sandbox does not wait with it. Set `sandboxKeepOnPark:
  true` to keep it through the park.
- A crash before the end is covered by the sandbox's own timeout, and the next
  run that finds an old sandbox ends it.

> [!WARNING]
> `bash` asks for approval before every command by default, and each approval
> is a park. With `sandboxScope: 'run'` and a provider that does not restore
> the work folder, every approved command starts in a fresh sandbox, without
> the files the earlier ones made. Use `sandboxKeepOnPark: true`, an approval
> rule that does not ask for every command, or a provider that restores the
> work folder.

## A provider that restores the work folder

Some apps keep the project somewhere else (a Git service, a bucket) and load
it into every new sandbox. Say so on the provider, and the tools tell the
model the files are back rather than gone:

```ts
const sandbox: SandboxProvider = { name: 'clevix', restoresWorkdir: true, create, connect };
```

```go
// A provider that implements agentenkit.WorkdirRestorer.
func (p *Provider) RestoresWorkdir() bool { return true }
```

The note then reads: "The sandbox restarted. The project files are back as
they were; anything outside the work folder (installed tools, temp files,
running processes) is gone." With `sandboxScope: 'run'`, a new run's fresh
sandbox gets no note at all.

## Options for every sandbox

`sandboxDefaults` (Go: `SandboxDefaults`) is merged into every sandbox the
runtime makes: `network`, `envs`, `template`, `image`, `resources`, `extra`.
The runtime still sets the timeout and the metadata itself.

```ts
config: { sandboxDefaults: { network: 'all', envs: { CI: '1' } } }
```

## A hook after each sandbox call

`afterSandboxCall` (Go: `BuiltinToolPorts.AfterSandboxCall`) is called once
after every tool call that used the sandbox: the built-in tools, and your own
tools that call `withSandbox` or `sandboxFor`. It gets the thread, the run,
the tool name, the call id, the sandbox, and the call's own error if it had
one. Save the work there, for example.

What the hook throws (Go: returns) becomes the call's error, so the model
knows the change was not saved. A call that already failed keeps its own
error.

```ts
tools: {
  sandbox,
  afterSandboxCall: async ({ sandbox, toolName }) => {
    const r = await sandbox.runCommand(`git add -A && git commit -qm ${JSON.stringify(toolName)} || true`);
    if (r.exitCode !== 0) throw new Error(`your change was not saved: ${r.stderr}`);
  },
},
```

## The adapters

| Adapter | TS | Go | For |
| --- | --- | --- | --- |
| E2B | `E2BSandbox` | `e2b.New` | Hosted; starts in under a second. Talks to E2B's HTTP API directly, so no SDK. |
| Docker | `DockerSandbox` | `docker.New` | Self-hosting: a container per thread, through the docker CLI. |
| Local | `LocalSandbox` | `localsandbox.New` | Development only: a folder on your machine, no isolation. Warns in production. |
| ComputeSDK | `ComputeSdkSandbox` | — | Any ComputeSDK provider: Daytona, Modal, Vercel, Cloudflare and more. |
| Memory | `MemorySandbox` | `memory.NewSandbox` | Tests. |

**Network is off by default.** E2B takes `network: 'all'` or
`{ allow: ['pypi.org'] }`; Docker takes `'none'` or `'all'` (it has no allow
list by domain). The local sandbox cannot limit the network at all.

**The local sandbox does not pass your environment on.** Commands see only
`PATH`, `HOME` (the sandbox folder) and `LANG`, so the API keys in your app's
environment do not reach what the model runs.

**ComputeSDK cannot push a sandbox's time back**, so a `ComputeSdkSandbox`
ends `timeoutMs` (default 1 hour) after it was made, used or not.

## The sandbox tools

Three built-in tools work in the thread's sandbox. Like the web tools, each
has one name and one input shape in TypeScript and Go, so any model that can
call tools can use them.

```ts
const coder = runtime.createStreamTextAgent({
  name: 'coder',
  tools: runtime.builtinTools(['bash', 'code_execution', 'text_editor'], {
    bash: { timeoutMs: 120_000 },
    codeExecution: { timeoutMs: 60_000 },
  }),
});
```

```go
tools, err := rt.BuiltinTools([]string{"bash", "code_execution", "text_editor"}, agentenkit.BuiltinToolOptions{
	Bash: agentenkit.BashOptions{Timeout: 2 * time.Minute},
})
```

| Tool | What the model can do | Result | Asks for approval by default |
| --- | --- | --- | --- |
| `bash` | Run a shell command | `{ stdout, stderr, exitCode }` | Yes |
| `code_execution` | Run a Python or JavaScript program | `{ stdout, stderr, exitCode, error?, files }` | No |
| `text_editor` | `view`, `create`, `str_replace`, `insert`, `undo_edit` | `view`: `{ path, content, totalLines }`; changes: `{ ok, message }` | For changes; not for `view` |

- **`bash`** runs each command in a new shell that starts in the folder the
  last one ended in, so `cd` carries over. Variables do not. `restart: true`
  goes back to the start folder.
- **`code_execution`** writes the program to a file and runs it with
  `python3` or `node`, in the start folder. `files` lists the files it made or
  changed (a chart saved as `chart.png` comes back as
  `{ path: 'chart.png', mediaType: 'image/png' }`). Showing images to the
  model comes in a later step.
- **`text_editor`** uses the input shape models know from Anthropic's editor.
  `str_replace` fails unless `old_str` matches exactly one place. Each change
  can be undone, up to 10 per file.

The tools keep their own files (the bash folder, the programs, the undo
history) in a hidden `.agentenkit` folder in the start folder. When the work
folder is a Git checkout, that folder shows up in `git status`; set
`sandboxStateDir` (Go: `SandboxStateDir`) to an absolute path, such as
`/home/app/.agentenkit`, to keep it out; each sandbox then gets its own
folder under it.

**Approval.** A call that asks waits for `respond` like any approval, then
runs when approved; a denied one tells the model it was denied. Pass
`approval` to change it: `true`, `false`, or a check that decides per call.

```ts
runtime.builtinTools(['bash'], {
  // Ask only for commands that delete something.
  bash: { approval: (input) => /\brm\b/.test(input.command ?? '') },
});
```

In Go, `Approval` takes `agentenkit.AskAlways`, `agentenkit.AskNever`, or a
`func(ctx, input) (bool, error)`.

**Limits.** Each command has a time limit (`bash` 120 s, `code_execution`
60 s by default); past it the result says `timedOut: true` and exit code 124.
Output past `builtinToolResultCapChars` keeps its start and its end, with the
middle cut. `maxUses` caps the calls in one run.

**Live output.** While a command runs, its output goes out as live-only
`tool.output` events (`{ toolCallId, tool, stream, text }`) on the run's
stream, so a UI can show it as it comes. They are not kept in the thread.

**Lost files.** When the thread's sandbox had ended and a fresh one was made,
the result carries a `note` saying the earlier files are gone, or, from a
provider that restores the work folder, that they are back.

**Cost.** Each call books a usage row (`kind: 'tool'`, `model: 'tool:bash'`,
the sandbox adapter as `modelId`) with the seconds its command ran. Price it
per second, per use, or both:

```ts
pricer: pricing.chain(pricing.table(prices), pricing.tools({ e2b: { perSecond: 0.000028 } })),
```

## Using it from your own tool

```ts
import { withSandbox } from 'agentenkit';

const runTests = tool({
  description: 'Run the test suite',
  inputSchema: z.object({}),
  execute: (_args, opts) =>
    withSandbox(opts, async ({ sandbox, lost }) => {
      const r = await sandbox.runCommand('pytest -q', { timeoutMs: 60_000 });
      return { lost, exitCode: r.exitCode, output: r.stdout.slice(-4_000) };
    }),
});
```

```go
runTests := agentenkit.AgentTool("run_tests", "Run the test suite",
	func(ctx context.Context, _ map[string]any, _ agentenkit.ToolContext) (string, error) {
		return agentenkit.WithSandbox(ctx, func(ts agentenkit.ThreadSandbox) (string, error) {
			r, err := ts.Sandbox.RunCommand(ctx, "pytest -q", agentenkit.RunCommandOptions{Timeout: time.Minute})
			if err != nil {
				return "", err
			}
			return fmt.Sprintf("exit %d\n%s", r.ExitCode, r.Stdout), nil
		})
	})
```

`withSandbox` gives your code the thread's sandbox. If the sandbox turns out
to have ended part way, it makes a fresh one and runs your code once more
with `lost: true`. `sandboxFor(opts)` (Go: `SandboxFor(ctx)`) just returns it.

## The interface

```ts
interface SandboxProvider {
  readonly name: string;
  create(options: CreateSandboxOptions): Promise<Sandbox>;
  connect(sandboxId: string): Promise<Sandbox>; // SandboxGoneError when it no longer exists
}

interface Sandbox {
  readonly sandboxId: string;
  readonly provider: string;
  readonly filesystem: SandboxFileSystem; // readFile, readFileBytes, writeFile, readdir, mkdir, exists, remove
  runCommand(command: string, options?: RunCommandOptions): Promise<CommandResult>;
  getUrl(options: { port: number; protocol?: 'http' | 'https' }): Promise<string>;
  getInfo(): Promise<SandboxInfo>;
  setTimeout(timeoutMs: number): Promise<void>; // not in ComputeSDK
  destroy(): Promise<void>;
}
```

A command runs in bash (sh where there is none), in the sandbox's work folder
unless you pass `cwd`. It has a time limit (`timeoutMs`, default 120 s): past
it the command is stopped, and the result has `timedOut: true` and exit code
124, with the output printed so far. Each output stream keeps at most 4 MiB;
past that the result says `truncated: true`.

Every adapter passes the same test suite in both runtimes (running commands,
exit codes, the time limit, streamed output, text and binary files, folders,
connecting again, and a destroyed sandbox being gone). An adapter you write
can run it too: see `test/sandbox-suite.ts` and `sandbox_test.go`.
