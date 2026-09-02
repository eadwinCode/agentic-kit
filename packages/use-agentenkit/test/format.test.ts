import { describe, expect, it } from 'bun:test';
import {
  contentToText,
  defaultFormat,
  messageToEntries,
  messageToEntry,
  reasoningText,
} from '../src/index.js';
import type { SnapshotMessage } from '../src/index.js';

const msg = (content: unknown, over: Partial<SnapshotMessage> = {}): SnapshotMessage => ({
  id: 'm1',
  role: 'assistant',
  content,
  ...over,
});

describe('reasoning (streamed thought)', () => {
  const withThought = msg([
    { type: 'reasoning', text: 'First I check the cursor. ' },
    { type: 'reasoning', text: 'Then the durable rows.' },
    { type: 'text', text: 'Here is the answer.' },
  ]);

  it('pulls the thinking out of a message', () => {
    expect(reasoningText(withThought.content)).toBe(
      'First I check the cursor. Then the durable rows.',
    );
    // Providers that expose no reasoning simply have no such parts
    expect(reasoningText([{ type: 'text', text: 'hi' }])).toBe('');
    expect(reasoningText('a plain string')).toBe('');
  });

  // The answer bubble must not contain the thinking: it streams separately and
  // a UI folds it away, so folding it into the text would make a reload look
  // different from the live run.
  it('keeps thinking out of the answer text', () => {
    const text = contentToText(withThought.content, defaultFormat);
    expect(text).toBe('Here is the answer.');
    expect(text).not.toContain('First I check');
  });

  it('renders as two entries, thinking first', () => {
    const entries = messageToEntries(withThought, defaultFormat);
    expect(entries.map((e) => e.kind)).toEqual(['reasoning', 'text']);
    expect(entries[0]!.text).toBe('First I check the cursor. Then the durable rows.');
    expect(entries[1]!.text).toBe('Here is the answer.');
    // Distinct, stable ids — React keys, and the answer keeps the message id
    expect(entries[0]!.id).toBe('m1:reasoning');
    expect(entries[1]!.id).toBe('m1');
  });

  it('yields one entry when there was no thinking', () => {
    const entries = messageToEntries(msg([{ type: 'text', text: 'just an answer' }]), defaultFormat);
    expect(entries).toHaveLength(1);
    expect(entries[0]!.kind).toBe('text');
  });

  // A model can think and then call a tool without ever writing prose.
  it('yields the thinking even when the answer is only a tool call', () => {
    const entries = messageToEntries(
      msg([
        { type: 'reasoning', text: 'I should look it up.' },
        { type: 'tool-call', toolName: 'search', args: { q: 'x' } },
      ]),
      defaultFormat,
    );
    expect(entries.map((e) => e.kind)).toEqual(['reasoning', 'tool']);
  });

  it('carries the agentId so a nested run keeps its own thinking', () => {
    const entries = messageToEntries(
      msg([{ type: 'reasoning', text: 'child thought' }], { agentId: 'sub_1' }),
      defaultFormat,
    );
    expect(entries[0]!.agentId).toBe('sub_1');
  });
});

describe('messageToEntry', () => {
  it('returns null for a message with nothing to show', () => {
    expect(messageToEntry(msg([]), defaultFormat)).toBeNull();
  });

  it('marks a tool-only message as tool, and a mixed one as text', () => {
    expect(messageToEntry(msg([{ type: 'tool-result', result: { ok: 1 } }]), defaultFormat)!.kind)
      .toBe('tool');
    expect(
      messageToEntry(
        msg([{ type: 'text', text: 'hi' }, { type: 'tool-call', toolName: 't', args: {} }]),
        defaultFormat,
      )!.kind,
    ).toBe('text');
  });
});

describe('structured parts', () => {
  it('mirrors the stored parts on the entry, with tool calls settled by their results', () => {
    const answered = new Set(['c1']);
    const entries = messageToEntries(
      msg([
        { type: 'text', text: 'Looking it up.' },
        { type: 'tool-call', toolCallId: 'c1', toolName: 'lookup', args: { q: 'x' } },
        { type: 'tool-call', toolCallId: 'c2', toolName: 'lookup', args: { q: 'y' } },
      ]),
      defaultFormat,
      answered,
    );
    expect(entries).toHaveLength(1);
    const parts = entries[0]!.parts;
    expect(parts.map((p) => p.type)).toEqual(['text', 'tool-call', 'tool-call']);
    expect(parts[1]).toMatchObject({ toolCallId: 'c1', state: 'done' });
    expect(parts[2]).toMatchObject({ toolCallId: 'c2', state: 'running' });
  });

  it('keeps an image-only user turn as an entry', () => {
    const entry = messageToEntry(
      msg([{ type: 'image', image: 'https://cdn/x.png', mimeType: 'image/png' }], { role: 'user' }),
      defaultFormat,
    );
    expect(entry).not.toBeNull();
    expect(entry!.text).toBe('');
    expect(entry!.parts).toEqual([{ type: 'image', image: 'https://cdn/x.png', mimeType: 'image/png' }]);
  });

  it('gives a plain string message one text part and thinking its own part', () => {
    expect(messageToEntry(msg('hello', { role: 'user' }), defaultFormat)!.parts).toEqual([
      { type: 'text', text: 'hello' },
    ]);
    const [thought] = messageToEntries(msg([{ type: 'reasoning', text: 'hm' }, { type: 'text', text: 'ok' }]), defaultFormat);
    expect(thought!.parts).toEqual([{ type: 'reasoning', text: 'hm' }]);
  });
});
