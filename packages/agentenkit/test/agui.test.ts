import { describe, expect, it } from 'bun:test';
import { readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { agUiState, toAgUi } from '../src/core/agui.js';
import { agUiFrame, type FollowFrame } from '../src/core/follow.js';

// The AG-UI wire format (opt-in). The TS runtime writes each frame and what
// it becomes to parity/agui.jsonl; the Go package reads the same file
// (agui_test.go) and must turn each frame into the same events.
const FIXTURE = join(import.meta.dir, '../../parity/agui.jsonl');

const item = (type: string, fields: Record<string, unknown>, offset: string): FollowFrame =>
  ({ kind: 'stream', streamId: 'r1:1', item: { type, ...fields, offset } }) as FollowFrame;

const frames: FollowFrame[] = [
  item('RUN_STARTED', { threadId: 't1', runId: 'r1', streamId: 'r1:1', segment: 1 }, '1'),
  item('TEXT_MESSAGE_START', { messageId: 'm1', role: 'assistant' }, '2'),
  item('TEXT_MESSAGE_CONTENT', { messageId: 'm1', delta: 'Hi' }, '3'),
  item('TEXT_MESSAGE_END', { messageId: 'm1' }, '4'),
  item('REASONING_START', { messageId: 'm2' }, '5'),
  item('REASONING_CONTENT', { messageId: 'm2', delta: 'hmm' }, '6'),
  item('REASONING_END', { messageId: 'm2' }, '7'),
  item('TOOL_CALL_START', { toolCallId: 'c1', toolName: 'look' }, '8'),
  item('TOOL_CALL_ARGS', { toolCallId: 'c1', delta: '{"q":1}' }, '9'),
  item('TOOL_CALL_END', { toolCallId: 'c1', toolName: 'look', args: { q: 1 } }, '10'),
  item('TOOL_CALL_START', { toolCallId: 'c2', toolName: 'save' }, '11'),
  item('TOOL_CALL_END', { toolCallId: 'c2', toolName: 'save', args: { id: 7 } }, '12'),
  item('TOOL_CALL_RESULT', { toolCallId: 'c1', toolName: 'look', result: { found: true } }, '13'),
  item('TOOL_CALL_RESULT', { toolCallId: 'c2', toolName: 'save', result: 'ok' }, '14'),
  item('STEP_FINISHED', { step: 1, agentId: null, finishReason: 'stop', usage: { inputTokens: 1, cachedInputTokens: 0, outputTokens: 1, totalTokens: 2 } }, '15'),
  item('CUSTOM', { name: 'PROGRESS', value: { pct: 50 } }, '16'),
  item('SUBAGENT_STARTED', { subagentId: 's1', name: 'helper', depth: 1 }, '17'),
  item('RUN_FINISHED', { status: 'finished', finishReason: 'stop' }, '18'),
  item('RUN_ERROR', { status: 'lost', error: 'worker lost' }, '19'),
  { kind: 'thread', event: { threadId: 't1', seq: 3, type: 'STATE_CHANGE', payload: { state: 'COMPLETED' }, createdAt: new Date(0) } },
];

describe('ag-ui wire format', () => {
  it('every frame turns into the same AG-UI events in both runtimes', () => {
    const state = agUiState('t1');
    const lines = frames.map((frame) => JSON.stringify({ frame, events: toAgUi(frame, state) }));
    if (process.env.UPDATE_PARITY) writeFileSync(FIXTURE, lines.join('\n') + '\n');
    expect(readFileSync(FIXTURE, 'utf8').trimEnd().split('\n')).toEqual(lines);
  });

  it('the resume id rides on the first event of a frame', () => {
    const at = { seq: 2 };
    const sse = agUiFrame(frames[11]!, at, agUiState('t1')); // a call whose arguments came whole
    const messages = sse.split('\n\n').filter(Boolean);
    expect(messages).toHaveLength(2); // its arguments, then its end
    expect(messages[0]!.startsWith('id: 2 r1:1 12\ndata: {"type":"TOOL_CALL_ARGS"')).toBe(true);
    expect(messages[1]!.startsWith('data: {"type":"TOOL_CALL_END"')).toBe(true);
  });
});
