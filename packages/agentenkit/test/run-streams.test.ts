import { describe, expect, it } from 'bun:test';
import { MemoryRunStreams } from '../src/adapters/memory.js';
import { parseStreamId, streamIdOf } from '../src/core/stream-events.js';
import { runStreamsSuite } from './run-streams-suite.js';

runStreamsSuite('memory', async () => new MemoryRunStreams());

describe('stream ids', () => {
  it('a stream id names its run and segment', () => {
    expect(streamIdOf('run_1', 2)).toBe('run_1:2');
    expect(parseStreamId('run_1:2')).toEqual({ runId: 'run_1', segment: 2 });
    expect(parseStreamId('run:with:colons:3')).toEqual({ runId: 'run:with:colons', segment: 3 });
    expect(parseStreamId('run_1')).toBeNull();
    expect(parseStreamId('run_1:0')).toBeNull();
  });
});
