import {expect, it} from 'vitest';
import {OUTCOMES, settlement} from './settlement';
import type {Words} from './words';

const words = new Proxy({}, {get: (_, key) => String(key)}) as Words;

const drawn = settlement(words);
const kind = (id: string) => drawn.boxes.find((box) => box.id === id)?.kind;

it('draws a row per outcome, from the handler through the transport to the delivery', () => {
  expect(OUTCOMES).toEqual(['ack', 'retry', 'retryAfter', 'drop']);
  for (const outcome of OUTCOMES) {
    expect(drawn.links).toContainEqual({from: outcome, to: `${outcome}Wire`});
    expect(drawn.links).toContainEqual({from: `${outcome}Wire`, to: `${outcome}Effect`});
  }
});

it('shows that only a retry after a delay brings the message back', () => {
  expect(kind('retryAfterEffect')).toBe('core');
  expect(kind('retryEffect')).toBe('bad');
  expect(kind('dropEffect')).toBe('bad');
  expect(kind('ackEffect')).toBe('good');
});

it('heads the columns with who acts in each', () => {
  expect(drawn.lanes.map((lane) => lane.label)).toEqual(['handlerLane', 'wireLane', 'deliveryLane']);
});
