// Package qstash holds the reference Queue adapter over Upstash QStash HTTP
// queues, talking to the REST API directly.
package qstash

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

// DefaultBaseURL is the QStash API.
const DefaultBaseURL = "https://qstash.upstash.io"

// Client is the QStash REST client the queue uses.
type Client struct {
	// Token is the QSTASH_TOKEN.
	Token string
	// BaseURL defaults to DefaultBaseURL.
	BaseURL string
	// HTTP defaults to a client with a 30 second timeout. http.DefaultClient
	// has none, and an enqueue that hangs holds the run that made it.
	HTTP *http.Client
}

// Options tune the queue.
type Options struct {
	// URL is the fully-qualified consumer URL, e.g. https://app.example.com/api/queue/agent-run
	URL string
	// QueueName is the queue for flow control (§2.8). Defaults to agent-runs.
	QueueName string
}

// Queue is a Queue over QStash.
type Queue struct {
	client Client
	opts   Options
}

// New makes a queue.
func New(client Client, opts Options) *Queue {
	if client.BaseURL == "" {
		client.BaseURL = DefaultBaseURL
	}
	if client.HTTP == nil {
		client.HTTP = &http.Client{Timeout: 30 * time.Second}
	}
	if opts.QueueName == "" {
		opts.QueueName = "agent-runs"
	}
	return &Queue{client: client, opts: opts}
}

// Cancel is a no-op: QStash offers no way to withdraw a queued message by
// key, and every caller treats a delivered row as a correct no-op.
func (q *Queue) Cancel(context.Context, string) error { return nil }

// Find cannot look a message up; the caller treats this as unknown.
func (q *Queue) Find(context.Context, string) (*ports.QueuedJob, error) {
	return nil, ports.ErrUnsupported
}

// Stats cannot count; the caller treats this as unknown.
func (q *Queue) Stats(context.Context) (ports.QueueStats, error) {
	return ports.QueueStats{}, ports.ErrUnsupported
}

// Enqueue dispatches a job. A delayed job goes out as a published message
// rather than a queued one: QStash supports delays on publish only and
// rejects Upstash-Delay on enqueue. The trade is that this one message
// skips the queue's flow control, acceptable for the two things that ask
// for a delay (a HITL expiry and a blocked job's redrive), since both are
// single messages the run lock already serializes.
func (q *Queue) Enqueue(ctx context.Context, job ports.RunJob, opts *ports.EnqueueOptions) error {
	body, err := json.Marshal(job)
	if err != nil {
		return err
	}
	target := url.PathEscape(q.opts.URL)
	endpoint := q.client.BaseURL + "/v2/enqueue/" + url.PathEscape(q.opts.QueueName) + "/" + target
	var delay int64
	if opts != nil && opts.Delay > 0 {
		// Rounded up, never down: a delay is a "not before", and 1.9s cut
		// to 1s would deliver early.
		delay = max(int64(math.Ceil(opts.Delay.Seconds())), 1)
		endpoint = q.client.BaseURL + "/v2/publish/" + target
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+q.client.Token)
	req.Header.Set("Content-Type", "application/json")
	if delay > 0 {
		req.Header.Set("Upstash-Delay", strconv.FormatInt(delay, 10)+"s")
	}
	// The key dedupes on QStash's side. QStash remembers an id for ten
	// minutes, not for as long as the message waits, so a key reused after
	// that goes out again; every caller treats a second delivery as a no-op.
	if opts != nil && opts.Key != "" {
		req.Header.Set("Upstash-Deduplication-Id", opts.Key)
	}
	res, err := q.client.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	body, _ = io.ReadAll(io.LimitReader(res.Body, 4096))
	if res.StatusCode >= 300 {
		return fmt.Errorf("qstash: %s: %s", res.Status, string(body))
	}
	var out struct {
		Deduplicated bool `json:"deduplicated"`
	}
	if json.Unmarshal(body, &out) == nil && out.Deduplicated {
		return ports.ErrDuplicateJob
	}
	return nil
}
