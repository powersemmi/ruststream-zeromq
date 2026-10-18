import type {Box, Link} from './flow';
import {lines, word, type Words} from './words';

const WORKERS = ['worker1', 'worker2', 'worker3'] as const;
const SUBSCRIBERS = ['jobs', 'prices', 'unmatched'] as const;
const DEALERS = ['dealerA', 'dealerB'] as const;

// Three cells across the drawing, the shape every pattern below fans out to.
const CELL = 104;
const STEP = 116;
const cellX = (index: number): number => 12 + index * STEP;
// Where along the narrower box above or below them each cell's link meets it, in cell order.
const SPREAD = [0.15, 0.5, 0.85] as const;

/*
 * The three socket patterns as ZeroMQ draws them, with no server anywhere: a pipeline that deals
 * work out in turn and gathers the results, a publisher that sends each message only to the
 * subscribers whose filter it matches, and a responder that tells its requesters apart by the
 * identity of each connection.
 */
export function sockets(words: Words): {boxes: Box[]; links: Link[]} {
  const box = (
    id: string,
    x: number,
    y: number,
    w: number,
    h: number,
    fields: Partial<Box> = {},
  ): Box => ({id, x, y, w, h, kind: 'service', lines: lines(words, id), ...fields});
  const boxes: Box[] = [
    box('pipeline', 0, 0, 360, 252, {kind: 'group'}),
    box('ventilator', 105, 26, 150, 44),
    ...WORKERS.map((id, index) => box(id, cellX(index), 104, CELL, 58)),
    box('sink', 105, 196, 150, 44),
    box('pubsub', 0, 276, 360, 180, {kind: 'group'}),
    box('pub', 105, 302, 150, 44),
    ...SUBSCRIBERS.map((id, index) =>
      box(id, cellX(index), 380, CELL, 58, index === 2 ? {kind: 'bad'} : {}),
    ),
    box('reqrep', 0, 480, 360, 168, {kind: 'group'}),
    box('router', 80, 506, 200, 44),
    ...DEALERS.map((id, index) => box(id, 28 + index * 164, 592, 140, 44)),
  ];
  const links: Link[] = [
    ...WORKERS.map((id, index) => ({
      from: 'ventilator',
      to: id,
      exitAt: SPREAD[index],
      ...(index === 0 ? {label: word(words, 'dealt')} : {}),
    })),
    ...WORKERS.map((id, index) => ({
      from: id,
      to: 'sink',
      enterAt: SPREAD[index],
    })),
    ...SUBSCRIBERS.map((id, index) => ({
      from: 'pub',
      to: id,
      exitAt: SPREAD[index],
      ...(index === 2 ? {dashed: true} : {label: word(words, index === 0 ? 'jobsDone' : 'pricesEur')}),
    })),
    ...DEALERS.map((id, index) => ({
      from: id,
      to: 'router',
      enterAt: index === 0 ? 0.25 : 0.75,
      label: word(words, index === 0 ? 'identityA' : 'identityB'),
    })),
  ];
  return {boxes, links};
}
