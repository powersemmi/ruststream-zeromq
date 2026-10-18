import {expect, it} from 'vitest';
import {sockets} from './sockets';
import type {Words} from './words';

const words = new Proxy({}, {get: (_, key) => String(key)}) as Words;

const drawn = sockets(words);
const to = (id: string): string[] =>
  drawn.links.filter((link) => link.from === id).map((link) => link.to);

it('draws peers that talk to each other, with no box between them standing for a server', () => {
  const kinds = new Set(drawn.boxes.map((box) => box.kind));
  expect(kinds.has('broker')).toBe(false);
});

it('deals a pipeline out to every worker, and every worker hands its result to the sink', () => {
  expect(to('ventilator')).toEqual(['worker1', 'worker2', 'worker3']);
  for (const worker of ['worker1', 'worker2', 'worker3']) {
    expect(to(worker)).toEqual(['sink']);
  }
});

it('sends a message only where a filter matches, and drops the one no filter matches', () => {
  expect(to('pub')).toEqual(['jobs', 'prices', 'unmatched']);
  const dropped = drawn.links.find((link) => link.to === 'unmatched');
  expect(dropped?.dashed).toBe(true);
  expect(drawn.boxes.find((box) => box.id === 'unmatched')?.kind).toBe('bad');
});

it('brings every requester to the one responder', () => {
  expect(to('dealerA')).toEqual(['responder']);
  expect(to('dealerB')).toEqual(['responder']);
});
