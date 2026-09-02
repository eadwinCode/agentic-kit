// Package qstash holds the reference Queue adapter over Upstash QStash HTTP
// queues, talking to the REST API directly.
package qstash

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"

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
	// HTTP defaults to http.DefaultClient.
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
		client.HTTP = http.DefaultClient
	}
	if opts.QueueName == "" {
		opts.QueueName = "agent-runs"
	}
	return &Queue{client: client, opts: opts}
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
		delay = int64(opts.Delay.Seconds())
		if delay < 1 {
			delay = 1
		}
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
	res, err := q.client.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode >= 300 {
		msg, _ := io.ReadAll(io.LimitReader(res.Body, 4096))
		return fmt.Errorf("qstash: %s: %s", res.Status, string(msg))
	}
	return nil
}
