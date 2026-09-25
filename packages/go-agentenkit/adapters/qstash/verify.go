package qstash

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// SigningKeys are the two keys QStash signs with: the current one, and the
// next one it rotates to. A request signed with either is accepted.
type SigningKeys struct {
	Current string // QSTASH_CURRENT_SIGNING_KEY
	Next    string // QSTASH_NEXT_SIGNING_KEY
}

// ErrInvalidSignature is a request QStash did not sign, or signed for a
// different URL or body.
var ErrInvalidSignature = errors.New("qstash: invalid signature")

// MaxBodyBytes caps the body Verify reads. A job is a small JSON ticket.
const MaxBodyBytes = 1 << 20

// Verify checks the Upstash-Signature header of a delivery and returns the
// body it read. url is the consumer URL QStash was told to call, exactly as
// passed to Options.URL; it must match the token's subject. The request body
// is consumed; the returned bytes are the job.
//
// Without this, anyone who learns the consumer URL can post a job that runs
// any agent under any tenant's state, and bills it.
func Verify(r *http.Request, keys SigningKeys, url string) ([]byte, error) {
	sig := r.Header.Get("Upstash-Signature")
	if sig == "" {
		return nil, fmt.Errorf("%w: no Upstash-Signature header", ErrInvalidSignature)
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, MaxBodyBytes+1))
	if err != nil {
		return nil, err
	}
	if len(body) > MaxBodyBytes {
		return nil, fmt.Errorf("qstash: body larger than %d bytes", MaxBodyBytes)
	}
	if err := VerifySignature(sig, body, keys, url, time.Now()); err != nil {
		return nil, err
	}
	return body, nil
}

// Middleware verifies every request before next sees it, answering 401 to
// one that fails. next reads the verified body from r.Body as usual.
func Middleware(keys SigningKeys, url string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := Verify(r, keys, url)
		if err != nil {
			http.Error(w, "invalid signature", http.StatusUnauthorized)
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(body))
		next.ServeHTTP(w, r)
	})
}

// VerifySignature checks one signature against a body. It is Verify without
// the HTTP request, for a host that reads the body itself.
func VerifySignature(signature string, body []byte, keys SigningKeys, url string, now time.Time) error {
	var last error = fmt.Errorf("%w: no signing key configured", ErrInvalidSignature)
	for _, key := range []string{keys.Current, keys.Next} {
		if key == "" {
			continue
		}
		if last = verifyWithKey(signature, body, key, url, now); last == nil {
			return nil
		}
	}
	return last
}

// verifyWithKey is QStash's own check: an HS256 JWT signed with the key,
// issued by Upstash, for this URL, inside its time window, over a body whose
// SHA-256 is the token's body claim (base64url; padding ignored, as the
// official SDK does).
func verifyWithKey(token string, body []byte, key, url string, now time.Time) error {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return fmt.Errorf("%w: malformed token", ErrInvalidSignature)
	}
	var header struct {
		Alg string `json:"alg"`
	}
	if err := decodeSegment(parts[0], &header); err != nil || header.Alg != "HS256" {
		return fmt.Errorf("%w: unsupported token header", ErrInvalidSignature)
	}
	mac := hmac.New(sha256.New, []byte(key))
	mac.Write([]byte(parts[0] + "." + parts[1]))
	got, err := base64.RawURLEncoding.DecodeString(strings.TrimRight(parts[2], "="))
	if err != nil || !hmac.Equal(got, mac.Sum(nil)) {
		return fmt.Errorf("%w: bad signature", ErrInvalidSignature)
	}
	var claims struct {
		Iss  string `json:"iss"`
		Sub  string `json:"sub"`
		Exp  int64  `json:"exp"`
		Nbf  int64  `json:"nbf"`
		Body string `json:"body"`
	}
	if err := decodeSegment(parts[1], &claims); err != nil {
		return fmt.Errorf("%w: unreadable claims", ErrInvalidSignature)
	}
	switch {
	case claims.Iss != "Upstash":
		return fmt.Errorf("%w: issuer %q", ErrInvalidSignature, claims.Iss)
	case url != "" && claims.Sub != url:
		return fmt.Errorf("%w: signed for %q", ErrInvalidSignature, claims.Sub)
	case claims.Exp != 0 && now.Unix() > claims.Exp:
		return fmt.Errorf("%w: expired", ErrInvalidSignature)
	case claims.Nbf != 0 && now.Unix() < claims.Nbf:
		return fmt.Errorf("%w: not yet valid", ErrInvalidSignature)
	}
	sum := sha256.Sum256(body)
	if strings.TrimRight(claims.Body, "=") != base64.RawURLEncoding.EncodeToString(sum[:]) {
		return fmt.Errorf("%w: body does not match", ErrInvalidSignature)
	}
	return nil
}

func decodeSegment(seg string, v any) error {
	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimRight(seg, "="))
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, v)
}
