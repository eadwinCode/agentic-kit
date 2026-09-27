// bash, code_execution and text_editor, and nothing else: the smallest app
// that shows the built-in sandbox tools. Nothing else to stand up: SQLite on
// disk, a queue in this process, and the shared page from ../tools-ui. The
// sandbox is Docker by default.
//
// The same app in TypeScript is ../sandbox-tools-ts.
package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/zendev-sh/goai/provider"
	"github.com/zendev-sh/goai/provider/openai"
	_ "modernc.org/sqlite"

	agentenkit "github.com/eadwinCode/agentic-kit/packages/go-agentenkit"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/adapters/docker"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/adapters/e2b"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/adapters/inline"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/adapters/localsandbox"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/adapters/memory"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/adapters/sqlite"
	sqliteadmin "github.com/eadwinCode/agentic-kit/packages/go-agentenkit/admin/sqlite"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/pricing"
)

func main() {
	// Keys come from this folder's .env, then the repo's root .env.
	loadDotEnv(".env")
	loadDotEnv("../../.env")
	addr := envOr("ADDR", ":3104")
	apiKey := os.Getenv("OPENAI_API_KEY")
	if apiKey == "" {
		log.Fatal("Set OPENAI_API_KEY in examples/sandbox-tools-go/.env (or the repo root .env).")
	}
	model := envOr("MODEL", "gpt-4o-mini")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Where the commands run: one sandbox per thread, kept between messages
	// and ended after 30 idle minutes. SANDBOX picks it:
	//   docker (the default)  a container per thread; needs a Docker daemon
	//   e2b                   a hosted sandbox; needs E2B_API_KEY (the key is passed here, at setup)
	//   local                 a folder on this machine, with NO isolation: only for trying things out
	kind := envOr("SANDBOX", "docker")
	var sandbox agentenkit.SandboxProvider
	switch kind {
	case "docker":
		sandbox = docker.New(docker.Options{Image: envOr("SANDBOX_IMAGE", "python:3.12-slim")})
	case "e2b":
		s, err := e2b.New(os.Getenv("E2B_API_KEY"), e2b.Options{})
		if err != nil {
			log.Fatal("SANDBOX=e2b needs E2B_API_KEY.")
		}
		sandbox = s
	case "local":
		sandbox = localsandbox.New(localsandbox.Options{})
	default:
		log.Fatalf("SANDBOX must be docker, e2b or local, not %q.", kind)
	}
	// A local sandbox is a folder on this machine, and its paths reach the
	// rest of it: a program or a file view could read this app's .env. So
	// there every tool asks first. Elsewhere bash and file changes ask;
	// running code and viewing files do not.
	local := kind == "local"

	db, err := sqlite.Open(envOr("DB_FILE", "sandbox-tools.sqlite"))
	if err != nil {
		log.Fatal(err)
	}
	storage, err := sqlite.New(db)
	if err != nil {
		log.Fatal(err)
	}
	admin, err := sqliteadmin.New(db)
	if err != nil {
		log.Fatal(err)
	}
	kv, err := sqlite.NewKv(db)
	if err != nil {
		log.Fatal(err)
	}
	streams, err := sqlite.NewRunStreams(ctx, db, 0)
	if err != nil {
		log.Fatal(err)
	}
	queue := inline.New(ctx)

	cfg := agentenkit.DefaultConfig()
	rt, err := agentenkit.SetupAgentCore(ctx, agentenkit.RuntimeOptions{
		Storage: storage, Admin: admin, Bus: memory.NewBus(), Kv: kv, Streams: streams, Queue: queue,
		Config: &cfg,
		Tools:  agentenkit.BuiltinToolPorts{Sandbox: sandbox},
		// Money on every usage row: the model calls, and E2B time per second
		// (2 vCPUs). A Docker or local sandbox runs on your machine and costs
		// nothing.
		Pricer: pricing.Chain(
			pricing.Table{
				"gpt-4o-mini": {InputPerMillion: 0.15, CacheReadPerMillion: 0.075, OutputPerMillion: 0.6},
				"gpt-4o":      {InputPerMillion: 2.5, CacheReadPerMillion: 1.25, OutputPerMillion: 10},
			},
			pricing.Tools{"e2b": {PerSecond: 0.000028}, "docker": {}, "local": {}},
		),
		ResolveModel: func(name string) (agentenkit.ResolvedModel, error) {
			return agentenkit.ResolvedModel{
				Instance:      func() provider.LanguageModel { return openai.Chat(name, openai.WithAPIKey(apiKey)) },
				ContextWindow: 128_000,
			}, nil
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer rt.Close()
	queue.Bind(rt.Worker.Handler())

	// The built-in tools: one name and one input shape in every runtime, so
	// any model can use them. bash keeps its folder from one command to the
	// next; code_execution runs Python or JavaScript and lists the files it
	// made; text_editor views and changes files by exact replacement, with
	// undo.
	opts := agentenkit.BuiltinToolOptions{
		Bash:          agentenkit.BashOptions{MaxUses: 30},
		CodeExecution: agentenkit.CodeExecutionOptions{MaxUses: 20},
	}
	if local {
		opts.CodeExecution.Approval = agentenkit.AskAlways
		opts.TextEditor.Approval = agentenkit.AskAlways
	}
	tools, err := rt.BuiltinTools([]string{"bash", "code_execution", "text_editor"}, opts)
	if err != nil {
		log.Fatal(err)
	}
	chat := rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{
		Name:  "coder",
		Model: model,
		System: "You work in a sandbox: a separate machine kept for this conversation, with Python. Run Python with " +
			"code_execution to calculate things, use bash for shell commands, and text_editor to write and change " +
			"files. Say what you did and show the results.",
		Tools: tools,
		// Subagents get the same tools and share the thread's sandbox.
		Subagents: &agentenkit.SubagentsConfig{Tools: tools},
	})

	askNote := "bash and file changes ask first"
	if local {
		askNote = "local sandbox: no isolation, so every tool asks first"
	}
	s := &server{rt: rt, chat: chat, info: map[string]any{
		"title":    "Sandbox tools",
		"subtitle": "bash, code_execution and text_editor, in one sandbox per conversation.",
		"runtime":  "go",
		"tools":    []string{"bash", "code_execution", "text_editor"},
		"prompts": []string{
			"Use Python to find the first 20 prime numbers and save them to primes.txt.",
			"Make a file notes.md with three short lines, change the second line, then show me the file.",
			"Create app/main.py that prints hello, then run it with bash.",
			"What version of Python is in the sandbox, and what files are in the working folder?",
		},
		"notes": []string{"sandbox: " + kind, askNote, "model: " + model},
	}}

	ui := envOr("UI_DIR", "../tools-ui/dist")
	if _, err := os.Stat(ui + "/index.html"); err != nil {
		log.Fatal("Build the page first: bun run --cwd examples/tools-ui build")
	}
	srv := &http.Server{Addr: addr, Handler: s.routes(ui)}
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdown)
	}()
	log.Printf("Sandbox tools (Go) on http://localhost%s", addr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
