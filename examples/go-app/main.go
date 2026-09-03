// A complete agent service in Go: go-agentenkit on the server, the React hook
// on the client, and nothing to stand up first.
//
//	cd examples/go-app/web && bun install && bun run build   # the SPA
//	cd .. && go run .                                         # http://localhost:8080
//
// Without OPENAI_API_KEY the app runs on a built-in mock model that answers
// with canned text and calls the tools on keywords, so every feature (tools,
// approvals, questions, subagents, custom events) can be tried offline.
package main

import (
	"context"
	"errors"
	"flag"
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
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/adapters/inline"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/adapters/memory"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/adapters/redis"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/adapters/sqlite"
	sqliteadmin "github.com/eadwinCode/agentic-kit/packages/go-agentenkit/admin/sqlite"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/pricing"
)

func main() {
	// A .env beside the binary, for the API key and the optional Redis URL.
	loadDotEnv(".env")
	addr := flag.String("addr", envOr("ADDR", ":8080"), "listen address")
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
		log.Printf("kv and bus on redis (%s)", opts.Addr)
	} else {
		kv, err = sqlite.NewKv(db)
		if err != nil {
			log.Fatal(err)
		}
		bus = memory.NewBus()
	}

	// The queue and the worker each need the other, so the queue is bound
	// once the runtime exists. Swap for adapters/qstash in production.
	queue := inline.New(ctx)

	apiKey := os.Getenv("OPENAI_API_KEY")
	cfg := agentenkit.DefaultConfig()
	cfg.StopPoll = 200 * time.Millisecond
	cfg.BillingPreCheck = creditCheck

	rt, err := agentenkit.SetupAgentCore(ctx, agentenkit.RuntimeOptions{
		Storage: storage,
		Admin:   adminStore,
		Bus:     bus,
		Kv:      kv,
		Queue:   queue,
		Config:  &cfg,
		// Money (§4): every model call is priced before its usage row is
		// stored, so spend is read back from the same store the tokens come
		// from — no second table, no wrapper around the model. Swap this for
		// pricing.Chain(pricing.Receipt(...), modelPrices) if your gateway
		// sends the real figure back and you want that over a price list.
		Pricer: modelPrices,
		// Models come in any shape; the platform only sees ResolvedModel.
		ResolveModel: func(name string) (agentenkit.ResolvedModel, error) {
			if apiKey == "" || name == "mock" {
				return agentenkit.ResolvedModel{
					Instance:      func() provider.LanguageModel { return &mockModel{id: name} },
					ContextWindow: 128_000,
					ModelID:       name,
				}, nil
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

	app := newApp(rt, defaultModel(apiKey))
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
