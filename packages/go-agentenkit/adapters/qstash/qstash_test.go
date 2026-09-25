package qstash

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

const consumer = "https://app.example.com/api/queue/agent-run"

// sign makes a token the way QStash does.
func sign(t *testing.T, key string, claims map[string]any) string {
	t.Helper()
	enc := base64.RawURLEncoding
	header, _ := json.Marshal(map[string]string{"alg": "HS256", "typ": "JWT"})
	payload, _ := json.Marshal(claims)
	unsigned := enc.EncodeToString(header) + "." + enc.EncodeToString(payload)
	mac := hmac.New(sha256.New, []byte(key))
	mac.Write([]byte(unsigned))
	return unsigned + "." + enc.EncodeToString(mac.Sum(nil))
}

func claimsFor(body []byte, now time.Time) map[string]any {
	sum := sha256.Sum256(body)
	return map[string]any{
		"iss": "Upstash", "sub": consumer,
		"exp": now.Add(5 * time.Minute).Unix(), "nbf": now.Add(-time.Second).Unix(),
		// QStash pads the body claim; the check must ignore the padding.
		"body": base64.URLEncoding.EncodeToString(sum[:]),
	}
}

func TestVerifySignature(t *testing.T) {
	keys := SigningKeys{Current: "current-key", Next: "next-key"}
	body := []byte(`{"threadId":"t1","runId":"r1"}`)
	now := time.Now()
	good := claimsFor(body, now)
	with := func(k string, v any) map[string]any {
		c := map[string]any{}
		for kk, vv := range good {
			c[kk] = vv
		}
		c[k] = v
		return c
	}
	cases := []struct {
		name  string
		token string
		body  []byte
		ok    bool
	}{
		{"signed with the current key", sign(t, "current-key", good), body, true},
		{"signed with the next key", sign(t, "next-key", good), body, true},
		{"signed with another key", sign(t, "someone-else", good), body, false},
		{"body changed", sign(t, "current-key", good), []byte(`{"threadId":"t2"}`), false},
		{"signed for another url", sign(t, "current-key", with("sub", "https://evil.example.com")), body, false},
		{"wrong issuer", sign(t, "current-key", with("iss", "Someone")), body, false},
		{"expired", sign(t, "current-key", with("exp", now.Add(-time.Minute).Unix())), body, false},
		{"not yet valid", sign(t, "current-key", with("nbf", now.Add(time.Minute).Unix())), body, false},
		{"not a token", "garbage", body, false},
	}
	for _, c := range cases {
		err := VerifySignature(c.token, c.body, keys, consumer, now)
		if c.ok && err != nil {
			t.Errorf("%s: want accepted, got %v", c.name, err)
		}
		if !c.ok && !errors.Is(err, ErrInvalidSignature) {
			t.Errorf("%s: want ErrInvalidSignature, got %v", c.name, err)
		}
	}
}

func TestMiddleware(t *testing.T) {
	keys := SigningKeys{Current: "current-key"}
	body := `{"threadId":"t1"}`
	var got string
	h := Middleware(keys, consumer, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		got = string(b)
	}))

	req := httptest.NewRequest(http.MethodPost, consumer, strings.NewReader(body))
	req.Header.Set("Upstash-Signature", sign(t, "current-key", claimsFor([]byte(body), time.Now())))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || got != body {
		t.Fatalf("a signed delivery reaches the handler with its body: %d %q", rec.Code, got)
	}

	got = ""
	req = httptest.NewRequest(http.MethodPost, consumer, strings.NewReader(body))
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized || got != "" {
		t.Fatalf("an unsigned request is refused before the handler: %d %q", rec.Code, got)
	}
}

func TestEnqueueSendsTheKeyAndReportsADuplicate(t *testing.T) {
	var dedupe string
	deduplicated := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		dedupe = r.Header.Get("Upstash-Deduplication-Id")
		_ = json.NewEncoder(w).Encode(map[string]any{"messageId": "m1", "deduplicated": deduplicated})
	}))
	defer srv.Close()
	q := New(Client{Token: "tok", BaseURL: srv.URL}, Options{URL: consumer})
	ctx := context.Background()

	if err := q.Enqueue(ctx, ports.RunJob{ThreadID: "t1"}, &ports.EnqueueOptions{Key: "hitl-expiry:c1"}); err != nil {
		t.Fatal(err)
	}
	if dedupe != "hitl-expiry:c1" {
		t.Fatalf("the key goes out as the deduplication id: %q", dedupe)
	}
	deduplicated = true
	err := q.Enqueue(ctx, ports.RunJob{ThreadID: "t1"}, &ports.EnqueueOptions{Key: "hitl-expiry:c1", Delay: time.Minute})
	if !errors.Is(err, ports.ErrDuplicateJob) {
		t.Fatalf("a message QStash deduplicated is ErrDuplicateJob: %v", err)
	}
	dedupe = "unset"
	if err := q.Enqueue(ctx, ports.RunJob{ThreadID: "t2"}, nil); err != nil && !errors.Is(err, ports.ErrDuplicateJob) {
		t.Fatal(err)
	}
	if dedupe != "" {
		t.Fatalf("no key, no deduplication id: %q", dedupe)
	}
}
