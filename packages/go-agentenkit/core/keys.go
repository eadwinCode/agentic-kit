// Package core holds the platform's behaviors. Everything here works against
// the ports bundle and nothing else: no driver, no provider SDK beyond goai.
package core

import (
	"context"
	"crypto/rand"
	"encoding/hex"

	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

// StateKey is the hot copy of a thread's state (§2.1, §3.4).
func StateKey(threadID string) string { return "agent:state:" + threadID }

// RunLockKey is the per-thread run lock (§3.4).
func RunLockKey(threadID string) string { return "agent:lock:" + threadID }

// SeqKey is the per-thread event sequence counter (§3.4).
func SeqKey(threadID string) string { return "agent:seq:" + threadID }

// AttemptsKey counts §2.8 failure retries.
func AttemptsKey(threadID string) string { return "agent:attempts:" + threadID }

// RunIDKey holds the thread's CURRENT run id (§2.1).
//
// Stop and start-a-new-run both write the state key, so the state key alone
// can never tell a worker its run is over: a user who stops and then sends
// another message overwrites CANCELLED with RUNNING before the worker's poll
// ever reads it. This key only moves forward. A worker whose id no longer
// matches knows it has been replaced, whatever the state key says.
func RunIDKey(threadID string) string { return "agent:run:" + threadID }

// RedriveKey counts re-dispatches of a job that keeps finding the run lock
// held by an OLDER run (§2.8). Separate from the attempts key: a blocked job
// has not failed, it simply has not started yet.
func RedriveKey(threadID string) string { return "agent:redrive:" + threadID }

// CurrentRunID is the run that owns the thread right now, or "" on a thread
// that predates run ids. Resuming a parked run (§2.5) REUSES this: a resume
// is the same run continuing, so it must never bump the id.
func CurrentRunID(ctx context.Context, deps ports.RuntimePorts, threadID string) (string, error) {
	v, _, err := deps.Kv.Get(ctx, RunIDKey(threadID))
	return v, err
}

// ClaimRun claims the thread for a brand new run and returns its id (§2.1).
// Always called BEFORE the state key is written.
func ClaimRun(ctx context.Context, deps ports.RuntimePorts, threadID string) (string, error) {
	return ClaimRunAs(ctx, deps, threadID, NewID())
}

// ClaimRunAs is ClaimRun with a caller-chosen id (§2.1). The caller has
// already checked the id is unused.
func ClaimRunAs(ctx context.Context, deps ports.RuntimePorts, threadID, runID string) (string, error) {
	if _, err := deps.Kv.Set(ctx, RunIDKey(threadID), runID, ports.SetOptions{}); err != nil {
		return "", err
	}
	return runID, nil
}

// NewID mints a random UUID v4.
func NewID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("agentenkit: crypto/rand failed: " + err.Error())
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	var out [36]byte
	hex.Encode(out[0:8], b[0:4])
	out[8] = '-'
	hex.Encode(out[9:13], b[4:6])
	out[13] = '-'
	hex.Encode(out[14:18], b[6:8])
	out[18] = '-'
	hex.Encode(out[19:23], b[8:10])
	out[23] = '-'
	hex.Encode(out[24:36], b[10:16])
	return string(out[:])
}
