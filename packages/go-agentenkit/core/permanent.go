package core

import (
	"errors"
	"strings"

	"github.com/zendev-sh/goai"
)

// Errors that retrying cannot fix (§2.8). A run that fails with one is
// failed at once rather than tried again: a bad key, a model that does not
// exist, no credits left. Every retry would fail the same way, cost a model
// call, and keep the user waiting. The TS runtime applies the same rule
// (core/permanent.ts).

// permanentStatuses are what a provider answers when the request itself is
// wrong: bad input, a bad key, no access, an unknown model, too large.
var permanentStatuses = map[int]bool{400: true, 401: true, 403: true, 404: true, 413: true, 422: true}

// IsPermanentError is true when err, or an error it wraps, is a provider
// error that retrying cannot fix. OpenAI says "no credits" with 429, the
// same status as a rate limit that does pass; only the body
// ("insufficient_quota") tells them apart. Anything else, including a rate
// limit, a server error, a dropped connection or a stream cut short, is
// worth another try.
func IsPermanentError(err error) bool {
	var api *goai.APIError
	if !errors.As(err, &api) {
		return false
	}
	return permanentStatuses[api.StatusCode] || strings.Contains(api.ResponseBody, "insufficient_quota")
}
