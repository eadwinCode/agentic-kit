// web_search and web_fetch, and nothing else: the smallest app that shows
// the built-in web tools. Nothing to stand up: SQLite on disk, a queue in
// this process, and the shared page from ../tools-ui.
//
// The same app in TypeScript is ../web-tools-ts.
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
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/adapters/brave"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/adapters/inline"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/adapters/jina"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/adapters/memory"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/adapters/pagereader"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/adapters/sqlite"
	sqliteadmin "github.com/eadwinCode/agentic-kit/packages/go-agentenkit/admin/sqlite"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/pricing"
)

func main() {
	// Keys come from this folder's .env, then the repo's root .env.
	loadDotEnv(".env")
	loadDotEnv("../../.env")
	addr := envOr("ADDR", ":3102")
	apiKey := os.Getenv("OPENAI_API_KEY")
	if apiKey == "" {
		log.Fatal("Set OPENAI_API_KEY in examples/web-tools-go/.env (or the repo root .env).")
	}
	model := envOr("MODEL", "gpt-4o-mini")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// The web tools' adapters. Each key is passed in here, at setup; nothing
	// reads it later. Brave when its key is set, else Jina. With neither,
	// the agent can still read pages (our page reader is free) but not
	// search.
	var tools agentenkit.BuiltinToolPorts
	braveKey, jinaKey := os.Getenv("BRAVE_API_KEY"), os.Getenv("JINA_API_KEY")
	if braveKey != "" {
		tools.Search, _ = brave.New(braveKey)
	} else if jinaKey != "" {
		tools.Search, _ = jina.NewWebSearch(jinaKey)
	}
	// Our own reader by default. WEB_READER=jina reads through Jina Reader,
	// which handles pages built with JavaScript, and PDFs.
	if os.Getenv("WEB_READER") == "jina" && jinaKey != "" {
		tools.Fetcher, _ = jina.NewReader(jinaKey)
	} else {
		tools.Fetcher = pagereader.New(pagereader.Options{})
	}

	db, err := sqlite.Open(envOr("DB_FILE", "web-tools.sqlite"))
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
	// web_fetch with a prompt has a small model read the page; this is it.
	cfg.CompactionModel = "gpt-4o-mini"
	rt, err := agentenkit.SetupAgentCore(ctx, agentenkit.RuntimeOptions{
		Storage: storage, Admin: admin, Bus: memory.NewBus(), Kv: kv, Streams: streams, Queue: queue,
		Config: &cfg,
		Tools:  tools,
		// Money on every usage row: the model calls, and each search.
		Pricer: pricing.Chain(
			pricing.Table{
				"gpt-4o-mini": {InputPerMillion: 0.15, CacheReadPerMillion: 0.075, OutputPerMillion: 0.6},
				"gpt-4o":      {InputPerMillion: 2.5, CacheReadPerMillion: 1.25, OutputPerMillion: 10},
			},
			pricing.Tools{"brave": {PerUse: 0.005}, "jina-search": {PerUse: 0.0005}},
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
	// any model can use them. A search result's id (s1r2) opens the page in
	// web_fetch, and a prompt has a small model read the page and answer, so
	// the page never fills the main context.
	names := []string{"web_fetch"}
	if tools.Search != nil {
		names = []string{"web_search", "web_fetch"}
	}
	web, err := rt.BuiltinTools(names, agentenkit.BuiltinToolOptions{
		WebSearch: agentenkit.WebSearchOptions{MaxUses: 10},
		WebFetch:  agentenkit.WebFetchOptions{MaxUses: 20},
	})
	if err != nil {
		log.Fatal(err)
	}
	chat := rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{
		Name:  "researcher",
		Model: model,
		System: "You answer questions from the web. Search with web_search, then open the most useful results with " +
			"web_fetch, passing the result id and a prompt that says what you need from the page. Cite the pages " +
			"you used. For a question with several parts, hand each part to a subagent with spawnSubagent.",
		Tools: web,
		// Subagents get the web tools too, and are billed to the same run.
		Subagents: &agentenkit.SubagentsConfig{Tools: web},
	})

	searchNote := "No BRAVE_API_KEY or JINA_API_KEY: web_fetch only, no search"
	if tools.Search != nil {
		searchNote = "search: " + tools.Search.Name()
	}
	s := &server{rt: rt, chat: chat, info: map[string]any{
		"title":    "Web tools",
		"subtitle": "web_search and web_fetch: search the web and read pages, on any model.",
		"runtime":  "go",
		"tools":    names,
		"prompts": []string{
			"What is the latest version of the MCP spec? Cite the page.",
			"Summarise https://en.wikipedia.org/wiki/Ada_Lovelace in three sentences.",
			"Compare the three biggest vector databases, one subagent each.",
		},
		"notes": []string{searchNote, "reader: " + tools.Fetcher.Name(), "model: " + model},
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
	log.Printf("Web tools (Go) on http://localhost%s", addr)
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
