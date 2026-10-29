// agentic-kit on Clevix Flow: Flow is the queue (every job is a Flow run you
// can watch in the Flow dashboard), the bus, and the run streams. The agent
// reads pages with web_fetch and saves notes with saveNote, which waits for
// your approval. Threads and the kv are in SQLite on disk, and the page is
// the shared one from ../tools-ui.
//
// The same app in TypeScript is ../flow-queue-ts.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	flow "github.com/eadwinCode/flow-sdk"
	"github.com/zendev-sh/goai/provider"
	"github.com/zendev-sh/goai/provider/openai"
	_ "modernc.org/sqlite"

	agentenkit "github.com/eadwinCode/agentic-kit/packages/go-agentenkit"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/adapters/clevixflow"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/adapters/pagereader"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/adapters/sqlite"
	sqliteadmin "github.com/eadwinCode/agentic-kit/packages/go-agentenkit/admin/sqlite"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/pricing"
)

func main() {
	// Keys come from this folder's .env, then the repo's root .env.
	loadDotEnv(".env")
	loadDotEnv("../../.env")
	// This machine only: the agent spends your keys. ADDR=:3108 opens it to
	// your network, for an engine on another machine.
	addr := envOr("ADDR", "127.0.0.1:3108")
	apiKey := os.Getenv("OPENAI_API_KEY")
	if apiKey == "" {
		log.Fatal("Set OPENAI_API_KEY in examples/flow-queue-go/.env (or the repo root .env).")
	}
	if os.Getenv("FLOW_SIGNING_KEY") == "" {
		log.Fatal("Set FLOW_SIGNING_KEY in examples/flow-queue-go/.env: make an app in the Flow dashboard and copy its signing key.")
	}
	model := envOr("MODEL", "gpt-4o-mini")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// The Flow client reads FLOW_SIGNING_KEY and FLOW_BASE_URL from the
	// environment. Empty FLOW_BASE_URL is the hosted engine.
	client, err := flow.NewClient(flow.ClientOpts{AppID: envOr("FLOW_APP_ID", "agentenkit-flow-go")})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	db, err := sqlite.Open(envOr("DB_FILE", "flow-queue.sqlite"))
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
	// Live events go over Flow realtime, one channel per thread. The kv
	// holds the few that are too big for one message.
	bus, err := clevixflow.NewBus(client, clevixflow.BusOptions{Kv: kv})
	if err != nil {
		log.Fatal(err)
	}
	// A run's output is kept in Flow streams, so a page that reloads picks
	// up where it left off.
	streams, err := clevixflow.NewStreams(client, clevixflow.StreamsOptions{Kv: kv})
	if err != nil {
		log.Fatal(err)
	}

	cfg := agentenkit.DefaultConfig()
	cfg.CompactionModel = "gpt-4o-mini"
	// Flow waits at most 14 minutes for one job, so a segment ends before
	// that. A longer run goes on in a new segment.
	cfg.SegmentTimeout = 13 * time.Minute
	rt, err := agentenkit.SetupAgentCore(ctx, agentenkit.RuntimeOptions{
		Storage: storage, Admin: admin, Kv: kv, Streams: streams, Bus: bus,
		// Each job starts a run of the Flow function below.
		Queue:  clevixflow.New(client, clevixflow.Options{}),
		Config: &cfg,
		Tools:  agentenkit.BuiltinToolPorts{Fetcher: pagereader.New(pagereader.Options{})},
		Pricer: pricing.Chain(modelPrices, pricing.Tools{"page-reader": {}}),
		ResolveModel: func(name string) (agentenkit.ResolvedModel, error) {
			if _, priced := modelPrices[name]; !priced && name != model {
				return agentenkit.ResolvedModel{}, fmt.Errorf("unknown model %q", name)
			}
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

	// The Flow function that runs the jobs, served at /api/flow. Up to 10
	// jobs run at once; priorities only count under a limit like this.
	if _, err := clevixflow.Functions(client, rt.Worker, clevixflow.FunctionOpts{Concurrency: 10}); err != nil {
		log.Fatal(err)
	}

	web, err := rt.BuiltinTools([]string{"web_fetch"}, agentenkit.BuiltinToolOptions{
		WebFetch: agentenkit.WebFetchOptions{MaxUses: 10},
	})
	if err != nil {
		log.Fatal(err)
	}
	chat := rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{
		Name:  "notes",
		Model: model,
		System: "You read web pages with web_fetch and answer from them. When the user asks you to keep something, " +
			"save it with saveNote: a short title and the text.",
		// saveNote is parked behind your approval. While it waits, its expiry
		// is a sleeping run in the Flow dashboard; your answer cancels it and
		// starts the resume.
		Tools: append(web, agentenkit.MarkRequiresConfirmation(saveNote())),
	})

	s := &server{rt: rt, chat: chat, info: map[string]any{
		"title":    "Flow queue",
		"subtitle": "agentic-kit on Clevix Flow: the queue, the bus and the run streams.",
		"runtime":  "go",
		"tools":    []string{"web_fetch", "saveNote"},
		"prompts": []string{
			"Summarise https://en.wikipedia.org/wiki/Ada_Lovelace in three sentences.",
			"Read https://en.wikipedia.org/wiki/Grace_Hopper and save a note with her three biggest firsts.",
		},
		"notes": []string{
			"Flow app: " + client.AppID(),
			"engine: " + envOr("FLOW_BASE_URL", flow.DefaultBaseURL),
			"model: " + model,
		},
	}}

	ui := envOr("UI_DIR", "../tools-ui/dist")
	if _, err := os.Stat(ui + "/index.html"); err != nil {
		log.Fatal("Build the page first: bun run --cwd examples/tools-ui build")
	}
	mux := http.NewServeMux()
	mux.Handle("/api/flow", client.Serve())
	mux.Handle("/", s.routes(ui))
	// No write timeout: the event stream stays open for as long as a tab
	// watches the thread, and Flow's calls can take minutes.
	srv := &http.Server{Addr: addr, Handler: mux}
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdown)
	}()
	log.Printf("Flow queue (Go) on http://%s; Flow calls /api/flow", addr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}

func saveNote() agentenkit.Tool {
	return agentenkit.AgentTool("saveNote", "Save a short note for the user (needs their approval).",
		func(_ context.Context, in struct {
			Title string `json:"title"`
			Text  string `json:"text"`
		}, _ agentenkit.ToolContext) (string, error) {
			// Only reached after you approved it.
			b, err := json.Marshal(map[string]any{"status": "SAVED", "title": in.Title, "text": in.Text})
			return string(b), err
		})
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// modelPrices are the models this app prices, per million tokens.
var modelPrices = pricing.Table{
	"gpt-4o-mini": {InputPerMillion: 0.15, CacheReadPerMillion: 0.075, OutputPerMillion: 0.6},
	"gpt-4o":      {InputPerMillion: 2.5, CacheReadPerMillion: 1.25, OutputPerMillion: 10},
}
