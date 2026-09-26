// A complete agent service in Go: go-agentenkit on the server, the React hook
// on the client, and nothing to stand up first.
//
//	cd examples/go-app/web && bun install && bun run build   # the SPA
//	cd .. && go run .                                         # http://localhost:8090
//
// Without OPENAI_API_KEY the app runs on a built-in mock model that answers
// with canned text and calls the tools on keywords, so every feature (tools,
// approvals, questions, subagents, custom events) can be tried offline.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/zendev-sh/goai/provider"
	"github.com/zendev-sh/goai/provider/openai"
	_ "modernc.org/sqlite"

	agentenkit "github.com/eadwinCode/agentic-kit/packages/go-agentenkit"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/adapters/brave"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/adapters/inline"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/adapters/jina"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/adapters/memory"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/adapters/pagereader"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/adapters/redis"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/adapters/sqlite"
	sqliteadmin "github.com/eadwinCode/agentic-kit/packages/go-agentenkit/admin/sqlite"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/pricing"
)

func main() {
	// A .env beside the binary, for the API key and the optional Redis URL.
	loadDotEnv(".env")
	addr := flag.String("addr", envOr("ADDR", ":8090"), "listen address")
	static := flag.String("static", envOr("STATIC_DIR", "web/dist"), "built SPA to serve (empty to serve none)")
	dbFile := flag.String("db", envOr("DB_FILE", "go-app.sqlite"), "SQLite file: your tables and the platform's own history")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// One file holds both: the app's threads, messages, events and usage, and,
	// prefixed agentic_, the platform's operational history.
	db, err := sqlite.Open(*dbFile)
	if err != nil {
		log.Fatal(err)
	}
	storage, err := sqlite.New(db)
	if err != nil {
		log.Fatal(err)
	}
	adminStore, err := sqliteadmin.New(db)
	if err != nil {
		log.Fatal(err)
	}
	// The Kv holds the event sequence counters, so it must live as long as
	// the log does: an in-memory Kv would reset them on restart and every
	// client's cursor would drop the new events. With REDIS_URL set, Redis
	// carries the kv AND the bus, so several server processes can share
	// threads; without it, the kv lives in the SQLite file and the bus in
	// memory, which is right for one process.
	var kv agentenkit.Kv
	var bus agentenkit.EventBus
	var streams agentenkit.RunStreams
	if url := os.Getenv("REDIS_URL"); url != "" {
		opts, err := goredis.ParseURL(url)
		if err != nil {
			log.Fatalf("REDIS_URL: %v", err)
		}
		client := goredis.NewClient(opts)
		if err := client.Ping(ctx).Err(); err != nil {
			log.Fatalf("redis at %s: %v", url, err)
		}
		kv, bus = redis.NewKv(client), redis.NewBus(client, 0)
		// A run's live events: one short-lived Redis Stream per run segment,
		// which any process can read.
		streams = redis.NewRunStreams(client, redis.StreamsOptions{})
		log.Printf("kv, bus and run streams on redis (%s)", opts.Addr)
	} else {
		kv, err = sqlite.NewKv(db)
		if err != nil {
			log.Fatal(err)
		}
		bus = memory.NewBus()
		// Run streams in the SQLite file, so a tab that reconnects mid-run,
		// even across a restart, picks up where it left off.
		streams, err = sqlite.NewRunStreams(ctx, db, 0)
		if err != nil {
			log.Fatal(err)
		}
	}

	// The queue and the worker each need the other, so the queue is bound
	// once the runtime exists. Swap for adapters/qstash in production.
	queue := inline.New(ctx)

	apiKey := os.Getenv("OPENAI_API_KEY")
	webTools := webToolPorts()
	cfg := agentenkit.DefaultConfig()
	cfg.StopPoll = 200 * time.Millisecond
	cfg.BillingPreCheck = creditCheck

	rt, err := agentenkit.SetupAgentCore(ctx, agentenkit.RuntimeOptions{
		Storage: storage,
		Admin:   adminStore,
		Bus:     bus,
		Kv:      kv,
		Streams: streams,
		Queue:   queue,
		Config:  &cfg,
		// Money (§4): every model call is priced before its usage row is
		// stored, so spend is read back from the same store the tokens come
		// from — no second table, no wrapper around the model. Swap this for
		// pricing.Chain(pricing.Receipt(...), modelPrices) if your gateway
		// sends the real figure back and you want that over a price list.
		// Tool use is priced too, per search, and counts against a run's
		// money cap.
		Pricer: pricing.Chain(modelPrices, toolPrices),
		// The adapters behind the built-in web tools (see newApp).
		Tools: webTools,
		// Models come in any shape; the platform only sees ResolvedModel.
		ResolveModel: func(name string) (agentenkit.ResolvedModel, error) {
			if apiKey == "" || name == "mock" {
				return agentenkit.ResolvedModel{
					Instance:      func() provider.LanguageModel { return &mockModel{id: name} },
					ContextWindow: 128_000,
					ModelID:       name,
				}, nil
			}
			// Only the models this app knows and prices. The model picks a
			// subagent's model itself; a name it made up (gpt-3.5-turbo, say)
			// is refused here, and the child falls back to the parent's
			// model, so no call goes out unpriced.
			if _, known := modelPrices[name]; !known && name != os.Getenv("MODEL") {
				return agentenkit.ResolvedModel{}, fmt.Errorf("unknown model %q", name)
			}
			return agentenkit.ResolvedModel{
				Instance:      func() provider.LanguageModel { return openai.Chat(name, openai.WithAPIKey(apiKey)) },
				ContextWindow: 128_000,
				// The wire id this key resolves to, recorded on every usage
				// row (§4). A key with no entry here is its own id.
				ModelID: modelIDs[name],
			}, nil
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer rt.Close()
	queue.Bind(rt.Worker.Handler())

	app := newApp(rt, defaultModel(apiKey), webTools.Search != nil)
	srv := &http.Server{Addr: *addr, Handler: app.routes(*static)}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	if apiKey == "" {
		log.Printf("OPENAI_API_KEY not set: using the built-in mock model")
	}
	log.Printf("agentenkit example listening on http://localhost%s (db %s)", *addr, *dbFile)
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

func defaultModel(apiKey string) string {
	if m := os.Getenv("MODEL"); m != "" {
		return m
	}
	if apiKey == "" {
		return "mock"
	}
	return "gpt-4o-mini"
}

// modelIDs is the wire id each registry key resolves to (§4). It goes onto
// every usage row, so a price list keyed by wire ids still matches when the
// key is an alias.
// toolPrices prices the built-in tools per use, keyed by adapter.
var toolPrices = pricing.Tools{"brave": {PerUse: 0.005}, "jina-search": {PerUse: 0.0005}}

// webToolPorts builds the web tools' adapters. Each key is passed in here,
// at setup; nothing reads it later. Brave when its key is set, else Jina;
// with neither, the agent can still read pages (our page reader is free) but
// not search.
func webToolPorts() agentenkit.BuiltinToolPorts {
	var ports agentenkit.BuiltinToolPorts
	braveKey, jinaKey := os.Getenv("BRAVE_API_KEY"), os.Getenv("JINA_API_KEY")
	if braveKey != "" {
		ports.Search, _ = brave.New(braveKey)
	} else if jinaKey != "" {
		ports.Search, _ = jina.NewWebSearch(jinaKey)
	}
	// Our own reader by default. The Jina reader reads pages built with
	// JavaScript and PDFs, for a small price: WEB_READER=jina uses it.
	if os.Getenv("WEB_READER") == "jina" && jinaKey != "" {
		ports.Fetcher, _ = jina.NewReader(jinaKey)
	} else {
		ports.Fetcher = pagereader.New(pagereader.Options{})
	}
	if ports.Search == nil {
		log.Printf("BRAVE_API_KEY and JINA_API_KEY not set: web_fetch only, no web_search")
	}
	return ports
}

var modelIDs = map[string]string{
	"gpt-4o":      "gpt-4o-2024-11-20",
	"gpt-4o-mini": "gpt-4o-mini-2024-07-18",
}

// modelPrices is the price list (§4), in dollars per MILLION tokens — typed
// straight off the provider's pricing page. The runtime prices every model
// call against it before the usage row is stored, so GetThreadUsage returns
// money as well as counters.
//
// Keys can be the registry key or the wire id; both are tried. A model that is
// not here is stored UNPRICED rather than priced at zero, so a missing price
// shows up as a gap in the bill rather than as free work — which is why the
// "mock" model below is priced too.
var modelPrices = pricing.Table{
	"gpt-4o":      {InputPerMillion: 2.5, CacheReadPerMillion: 1.25, OutputPerMillion: 10},
	"gpt-4o-mini": {InputPerMillion: 0.15, CacheReadPerMillion: 0.075, OutputPerMillion: 0.6},
	"mock":        {InputPerMillion: 1, OutputPerMillion: 2},
}
