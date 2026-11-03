import { describe, expect, it } from 'bun:test';
import { StepLimit, withStepLimit, withToolTimeout } from '../src/core/steplimit.js';

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

describe('step limit', () => {
  it('a timed tool that returns in time leaves the step time to finish', async () => {
    const abort = new AbortController();
    const limit = new StepLimit(abort, 290);
    const slow = withToolTimeout({ execute: async () => { await sleep(150); return 'ok'; } }, 300);
    await withStepLimit({ slow }, limit).slow.execute({}, { abortSignal: abort.signal });
    await sleep(160); // past the tool's own end, within the step limit of its return
    expect(limit.expired).toBe(false);
    limit.stop();
  });

  it('after a timed call the step is back to the step limit', async () => {
    const abort = new AbortController();
    const limit = new StepLimit(abort, 100);
    const quick = withToolTimeout({ execute: async () => 'ok' }, 60_000);
    await withStepLimit({ quick }, limit).quick.execute({}, { abortSignal: abort.signal });
    await new Promise((resolve) => abort.signal.addEventListener('abort', resolve, { once: true }));
    expect(limit.expired).toBe(true);
    expect(limit.limit).toBe(100); // the error names the step limit, not the tool's
    limit.stop();
  });

  it('withToolTimeout leaves the tool passed in alone', () => {
    const t = { execute: async () => 'ok' };
    const slow = withToolTimeout(t, 1_000);
    expect((slow as any).timeoutMs).toBe(1_000);
    expect((t as any).timeoutMs).toBeUndefined();
  });

  it('a step with no timed tool keeps its tools as they are', () => {
    const tools = { fast: { execute: async () => 'ok' } };
    const limit = new StepLimit(new AbortController(), 50);
    expect(withStepLimit(tools, limit)).toBe(tools);
    limit.stop();
  });
});
