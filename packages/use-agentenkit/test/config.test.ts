import { describe, expect, it } from 'bun:test';
import {
  defaultRoutes,
  mergeConfig,
  resolveConfig,
  routeUrl,
  withQuery,
} from '../src/index.js';

describe('routes', () => {
  it('appends the hook query to a plain path', () => {
    expect(routeUrl(defaultRoutes.stream, { threadId: 't1', since: 7 }, '')).toBe(
      '/api/agent/stream?threadId=t1&since=7',
    );
  });

  it('prefixes baseUrl, and encodes', () => {
    expect(routeUrl(defaultRoutes.history, { threadId: 'a b/c' }, 'https://api.example.com')).toBe(
      'https://api.example.com/api/agent/history?threadId=a%20b%2Fc',
    );
  });

  // The escape hatch: a path cannot express a path parameter.
  it('leaves a function route to build its own URL, baseUrl included', () => {
    expect(
      routeUrl(({ threadId }) => `/v2/threads/${threadId}/history`, { threadId: 't1' }, '/ignored'),
    ).toBe('/v2/threads/t1/history');
  });

  it('keeps a query the route already carries', () => {
    expect(withQuery('/api/x?tenant=acme', { threadId: 't1' })).toBe(
      '/api/x?tenant=acme&threadId=t1',
    );
    expect(withQuery('/api/x', {})).toBe('/api/x');
    // undefined is dropped rather than sent as the string "undefined"
    expect(withQuery('/api/x', { a: undefined, b: 1 })).toBe('/api/x?b=1');
  });
});

describe('config', () => {
  it('overrides one route without disturbing the others', () => {
    const cfg = resolveConfig({ routes: { run: '/v2/start' } });
    expect(cfg.routes.run).toBe('/v2/start');
    expect(cfg.routes.stop).toBe(defaultRoutes.stop);
  });

  it('overrides one label without disturbing the others', () => {
    const cfg = resolveConfig({ labels: { thinking: 'Denkt na' } });
    expect(cfg.labels.thinking).toBe('Denkt na');
    expect(cfg.labels.completed).toBe('Completed');
  });

  it('takes headers as a value or a function, and always exposes a function', async () => {
    expect(await resolveConfig({ headers: { a: '1' } }).headers()).toEqual({ a: '1' });
    // A function is called per request, so a rotating token stays fresh
    let n = 0;
    const cfg = resolveConfig({ headers: () => ({ token: String(++n) }) });
    expect(await cfg.headers()).toEqual({ token: '1' });
    expect(await cfg.headers()).toEqual({ token: '2' });
  });

  it('turns persistence off when asked, and defaults it on', () => {
    expect(resolveConfig({ persistence: false }).persistence).toBeNull();
    expect(resolveConfig().persistence).not.toBeNull();
  });
});

describe('mergeConfig', () => {
  it('merges section by section, so a partial override keeps its siblings', () => {
    const merged = mergeConfig(
      { routes: { run: '/a', stop: '/b' }, labels: { thinking: 'T' }, defaultModel: 'm1' },
      { routes: { run: '/c' }, defaultModel: 'm2' },
    );
    expect(merged.routes).toEqual({ run: '/c', stop: '/b' });
    expect(merged.labels).toEqual({ thinking: 'T' });
    expect(merged.defaultModel).toBe('m2');
  });

  it('is a no-op when there is no provider above', () => {
    expect(mergeConfig(null, { defaultModel: 'm' })).toEqual({ defaultModel: 'm' });
  });
});
