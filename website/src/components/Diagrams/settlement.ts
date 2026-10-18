import type {Box, Lane, Link} from './flow';
import {table} from './table';
import {lines, type Words} from './words';

/** The outcomes of a handler, in the order the page reads them. */
export const OUTCOMES = ['ack', 'retry', 'retryAfter', 'drop'] as const;

const EFFECT_KIND: Record<(typeof OUTCOMES)[number], Box['kind']> = {
  ack: 'good',
  retry: 'bad',
  retryAfter: 'core',
  drop: 'bad',
};

/*
 * The settlement table drawn as a timeline of three columns: what the handler returns, what the
 * delivery answers on a transport that settles nothing, and what becomes of the message.
 */
export function settlement(words: Words): {boxes: Box[]; links: Link[]; lanes: Lane[]} {
  const drawn = table(words, {
    lanes: ['handlerLane', 'wireLane', 'deliveryLane'],
    widths: [112, 104, 120],
    rows: OUTCOMES.map((outcome) => [
      {id: outcome, code: true, rows: 3},
      {id: `${outcome}Wire`, code: true, rows: 3},
      {id: `${outcome}Effect`, kind: EFFECT_KIND[outcome], rows: 3},
    ]),
  });
  const bottom = Math.max(...drawn.boxes.map((box) => box.y + box.h));
  drawn.boxes.push({
    id: 'atMostOnce',
    x: 0,
    y: bottom + 20,
    w: 360,
    h: 44,
    kind: 'note',
    lines: lines(words, 'atMostOnce'),
  });
  return drawn;
}
