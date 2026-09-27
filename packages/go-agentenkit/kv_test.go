package agentenkit_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/adapters/memory"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/adapters/sqlite"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

// The promise every Kv adapter keeps, run against each one. The TS package
// runs the same cases under the same names (test/kv.test.ts).

func runKvSuite(t *testing.T, name string, newKv func(t *testing.T) ports.Kv) {
	t.Run("kv ("+name+")", func(t *testing.T) {
		ctx := context.Background()
		get := func(t *testing.T, kv ports.Kv, key string) string {
			t.Helper()
			v, ok, err := kv.Get(ctx, key)
			if err != nil {
				t.Fatal(err)
			}
			if !ok {
				return "<missing>"
			}
			return v
		}
		ok := func(t *testing.T, got bool, err error, want bool, what string) {
			t.Helper()
			if err != nil {
				t.Fatal(err)
			}
			mustEqual(t, got, want, what)
		}
		count := func(t *testing.T, got int64, err error, want int64, what string) {
			t.Helper()
			if err != nil {
				t.Fatal(err)
			}
			mustEqual(t, got, want, what)
		}

		t.Run("get returns what set wrote, and null for a missing key", func(t *testing.T) {
			kv := newKv(t)
			w, err := kv.Set(ctx, "a", "1", ports.SetOptions{})
			ok(t, w, err, true, "set")
			mustEqual(t, get(t, kv, "a"), "1", "a")
			_, _ = kv.Set(ctx, "a", "2", ports.SetOptions{})
			mustEqual(t, get(t, kv, "a"), "2", "a again")
			mustEqual(t, get(t, kv, "missing"), "<missing>", "missing")
		})

		t.Run("set with onlyIfNotExists writes only when the key is absent", func(t *testing.T) {
			kv := newKv(t)
			w, err := kv.Set(ctx, "lock", "w1", ports.SetOptions{OnlyIfNotExists: true})
			ok(t, w, err, true, "first")
			w, err = kv.Set(ctx, "lock", "w2", ports.SetOptions{OnlyIfNotExists: true})
			ok(t, w, err, false, "second")
			mustEqual(t, get(t, kv, "lock"), "w1", "lock")
		})

		t.Run("an expired key reads as missing, and SET NX can take it", func(t *testing.T) {
			kv := newKv(t)
			_, _ = kv.Set(ctx, "lock", "w1", ports.SetOptions{Expiry: time.Second})
			mustEqual(t, get(t, kv, "lock"), "w1", "live")
			time.Sleep(1100 * time.Millisecond)
			mustEqual(t, get(t, kv, "lock"), "<missing>", "expired")
			w, err := kv.Set(ctx, "lock", "w2", ports.SetOptions{OnlyIfNotExists: true})
			ok(t, w, err, true, "SET NX over an expired key")
			mustEqual(t, get(t, kv, "lock"), "w2", "taken")
		})

		t.Run("del removes a key", func(t *testing.T) {
			kv := newKv(t)
			_, _ = kv.Set(ctx, "a", "1", ports.SetOptions{})
			if err := kv.Del(ctx, "a"); err != nil {
				t.Fatal(err)
			}
			mustEqual(t, get(t, kv, "a"), "<missing>", "deleted")
			if err := kv.Del(ctx, "a"); err != nil { // fine when it is not there
				t.Fatal(err)
			}
		})

		t.Run("incr counts up from 1", func(t *testing.T) {
			kv := newKv(t)
			n, err := kv.Incr(ctx, "n")
			count(t, n, err, 1, "first")
			n, err = kv.Incr(ctx, "n")
			count(t, n, err, 2, "second")
			mustEqual(t, get(t, kv, "n"), "2", "stored")
		})

		t.Run("incrWithExpiry stamps a new counter and keeps a live one's expiry", func(t *testing.T) {
			kv := newKv(t)
			n, err := kv.IncrWithExpiry(ctx, "n", time.Second)
			count(t, n, err, 1, "first")
			time.Sleep(600 * time.Millisecond)
			// Still the first expiry: a second count does not push it back.
			n, err = kv.IncrWithExpiry(ctx, "n", time.Minute)
			count(t, n, err, 2, "second")
			time.Sleep(600 * time.Millisecond)
			mustEqual(t, get(t, kv, "n"), "<missing>", "expired")
			// An expired counter starts again, with the new expiry.
			n, err = kv.IncrWithExpiry(ctx, "n", time.Minute)
			count(t, n, err, 1, "again")
		})

		t.Run("setIfValue writes only while the key holds the expected value", func(t *testing.T) {
			kv := newKv(t)
			_, _ = kv.Set(ctx, "lock", "w1", ports.SetOptions{})
			w, err := kv.SetIfValue(ctx, "lock", "w2", "w3", 0)
			ok(t, w, err, false, "wrong value")
			w, err = kv.SetIfValue(ctx, "lock", "w1", "w1b", time.Minute)
			ok(t, w, err, true, "right value")
			mustEqual(t, get(t, kv, "lock"), "w1b", "lock")
			w, err = kv.SetIfValue(ctx, "missing", "x", "y", 0)
			ok(t, w, err, false, "missing")
			mustEqual(t, get(t, kv, "missing"), "<missing>", "still missing")
		})

		t.Run("delIfValue deletes only while the key holds the expected value", func(t *testing.T) {
			kv := newKv(t)
			_, _ = kv.Set(ctx, "lock", "w1", ports.SetOptions{})
			w, err := kv.DelIfValue(ctx, "lock", "w2")
			ok(t, w, err, false, "wrong value")
			mustEqual(t, get(t, kv, "lock"), "w1", "kept")
			w, err = kv.DelIfValue(ctx, "lock", "w1")
			ok(t, w, err, true, "right value")
			mustEqual(t, get(t, kv, "lock"), "<missing>", "deleted")
		})
	})
}

func openKvDB(t *testing.T) string {
	return filepath.Join(t.TempDir(), "kv.sqlite")
}

func TestKv(t *testing.T) {
	runKvSuite(t, "memory", func(*testing.T) ports.Kv { return memory.NewKv() })
	runKvSuite(t, "sqlite", func(t *testing.T) ports.Kv {
		db, err := sqlite.Open(openKvDB(t))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Close() })
		kv, err := sqlite.NewKv(db)
		if err != nil {
			t.Fatal(err)
		}
		return kv
	})
}

func TestKv_WhatTheKvHoldsOutlivesTheHandle(t *testing.T) {
	ctx := context.Background()
	file := openKvDB(t)
	db, err := sqlite.Open(file)
	if err != nil {
		t.Fatal(err)
	}
	kv, _ := sqlite.NewKv(db)
	_, _ = kv.Set(ctx, "agent:tool:sandbox:t1", `{"id":"s1"}`, ports.SetOptions{})
	_ = db.Close()
	// A new handle on the same file, as the app after a restart.
	db, err = sqlite.Open(file)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	kv, _ = sqlite.NewKv(db)
	v, found, err := kv.Get(ctx, "agent:tool:sandbox:t1")
	if err != nil || !found || v != `{"id":"s1"}` {
		t.Fatalf("got %q %v %v", v, found, err)
	}
}
