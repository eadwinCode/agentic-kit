/** Errors that retrying cannot fix (§2.8). A run that fails with one is
 *  failed at once rather than tried again: a bad key, a model that does not
 *  exist, no credits left. Every retry would fail the same way, cost a model
 *  call, and keep the user waiting. The Go runtime applies the same rule
 *  (core/permanent.go). */

/** Statuses a provider answers when the request itself is wrong: bad input,
 *  a bad key, no access, an unknown model, too large. */
const PERMANENT_STATUSES = new Set([400, 401, 403, 404, 413, 422]);

/** OpenAI says "no credits" with 429, the same status as a rate limit that
 *  does pass; only the body tells them apart. */
const PERMANENT_BODY = /insufficient_quota/;

interface ApiErrorLike {
  statusCode?: unknown;
  responseBody?: unknown;
  lastError?: unknown;
  errors?: unknown;
  cause?: unknown;
}

/** True when `err`, or an error it wraps (the AI SDK's RetryError keeps the
 *  last one), is a provider error that retrying cannot fix. Anything else,
 *  including a rate limit, a server error, a dropped connection or a stream
 *  cut short, is worth another try. */
export function isPermanentError(err: unknown): boolean {
  const seen = new Set<unknown>();
  const queue: unknown[] = [err];
  while (queue.length > 0) {
    const e = queue.shift();
    if (!e || typeof e !== 'object' || seen.has(e)) continue;
    seen.add(e);
    const api = e as ApiErrorLike;
    const status = typeof api.statusCode === 'number' ? api.statusCode : undefined;
    const body = typeof api.responseBody === 'string' ? api.responseBody : '';
    if (status !== undefined && PERMANENT_STATUSES.has(status)) return true;
    if (PERMANENT_BODY.test(body)) return true;
    queue.push(api.lastError, api.cause);
    if (Array.isArray(api.errors)) queue.push(...api.errors);
  }
  return false;
}
