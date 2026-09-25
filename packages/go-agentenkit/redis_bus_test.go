package agentenkit_test

import (
	"context"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	agentenkit "github.com/eadwinCode/agentic-kit/packages/go-agentenkit"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/adapters/redis"
)

// Workstream G: the Redis bus shares one subscriber connection per process,
// and one slow handler holds up nobody. The same cases run in the TS package
// (test/redis-bus.test.ts). Needs TEST_REDIS_ADDR (host:port).
func openRedis(t *testing.T) *goredis.Client {
	t.Helper()
	addr := os.Getenv("TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("TEST_REDIS_ADDR not set")
	}
	// A name of its own, so a count of subscriber connections sees only
	// this test's, not those of tests running beside it.
	c := goredis.NewClient(&goredis.Options{Addr: addr, ClientName: "rb-" + t.Name()})
	t.Cleanup(func() { c.Close() })
	return c
}

func subscriberConnections(t *testing.T, c *goredis.Client) int {
	t.Helper()
	list, err := c.ClientList(context.Background()).Result()
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, line := range strings.Split(list, "\n") {
		if strings.Contains(line, " name=rb-"+t.Name()+" ") && strings.Contains(line, " sub=") && !strings.Contains(line, " sub=0") {
			n++
		}
	}
	return n
}

func TestRedisBus_ManySubscribersShareOneConnection(t *testing.T) {
	c := openRedis(t)
	ctx := context.Background()
	bus := redis.NewBus(c, time.Hour)
	var mu sync.Mutex
	got := map[string]int{}
	var stops []func() error
	for i := 0; i < 20; i++ {
		thread := "rb-thread-" + string(rune('a'+i%5))
		stop, err := bus.Subscribe(ctx, thread, func(e agentenkit.AgentEvent) {
			mu.Lock()
			got[e.ThreadID]++
			mu.Unlock()
		})
		if err != nil {
			t.Fatal(err)
		}
		stops = append(stops, stop)
	}
	mustEqual(t, subscriberConnections(t, c), 1, "one subscriber connection for twenty subscriptions")
	for i := 0; i < 5; i++ {
		thread := "rb-thread-" + string(rune('a'+i))
		_ = bus.Publish(ctx, thread, agentenkit.AgentEvent{ThreadID: thread, Seq: 1, Type: "X"})
	}
	waitFor(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		for i := 0; i < 5; i++ {
			if got["rb-thread-"+string(rune('a'+i))] != 4 {
				return false
			}
		}
		return true
	})
	for _, stop := range stops {
		_ = stop()
	}
	// UNSUBSCRIBE is sent without waiting for its reply, so give the server
	// a moment to act on it.
	waitFor(t, func() bool {
		n, _ := c.PubSubNumSub(ctx, redis.ThreadChannel("rb-thread-a")).Result()
		return n[redis.ThreadChannel("rb-thread-a")] == 0
	})
}

func TestRedisBus_ASlowHandlerHoldsUpNobody(t *testing.T) {
	c := openRedis(t)
	ctx := context.Background()
	bus := redis.NewBus(c, time.Hour)
	release := make(chan struct{})
	stopSlow, _ := bus.Subscribe(ctx, "rb-slow", func(agentenkit.AgentEvent) { <-release })
	defer stopSlow()
	got := make(chan struct{}, 1)
	stopFast, _ := bus.Subscribe(ctx, "rb-slow", func(agentenkit.AgentEvent) {
		select {
		case got <- struct{}{}:
		default:
		}
	})
	defer stopFast()
	_ = bus.Publish(ctx, "rb-slow", agentenkit.AgentEvent{ThreadID: "rb-slow", Seq: 1, Type: "X"})
	select {
	case <-got:
	case <-time.After(2 * time.Second):
		t.Fatal("the fast subscriber waited on the slow one")
	}
	close(release)
}
