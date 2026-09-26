# Sandboxes

A sandbox is a place apart from your app where an agent's commands run and
its files live: a Docker container, a hosted machine on E2B, or a folder on
your machine in development. The built-in tools that run commands and edit
files (`bash`, `code_execution`, `text_editor`, coming next) use it, and your
own tools can too.

Like storage or the queue, it is a port you plug an adapter into. The
interface has the same shape and names as [ComputeSDK](https://www.computesdk.com)'s,
in TypeScript and Go, so a new provider is one small adapter.

## Quick start

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

## Using it from your own tool

```ts
import { withSandbox } from 'agentenkit';

const runTests = tool({
  description: 'Run the test suite',
  parameters: z.object({}),
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
