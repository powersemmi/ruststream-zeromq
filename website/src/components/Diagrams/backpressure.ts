import type {Box, Link} from './flow';
import {lines, type Words} from './words';

// The way of a message down the left column, and the way the wait climbs back up the right one.
const FLOW = ['peer', 'kernel', 'driver', 'channel', 'handler'] as const;
const WAIT = ['full', 'stops', 'fills', 'waits'] as const;
const ROW = 76;

/*
 * A subscription's read-ahead: the driver task reads the socket into a channel read_ahead deep,
 * and the handler takes from the channel. When the handler falls behind the channel fills, the
 * driver stops reading, the socket buffers fill, and the peer's send waits.
 */
export function backpressure(words: Words): {boxes: Box[]; links: Link[]} {
  const boxes: Box[] = [
    ...FLOW.map(
      (id, index): Box => ({
        id,
        x: 12,
        y: index * ROW,
        w: 160,
        h: 58,
        kind: id === 'handler' || id === 'peer' ? 'service' : id === 'kernel' ? 'step' : 'broker',
        code: id === 'channel',
        lines: lines(words, id),
        href: id === 'channel' ? '/components/subscriptions/read-ahead/' : undefined,
      }),
    ),
    // The wait starts at the channel's row and climbs one row per step, up to the peer's.
    ...WAIT.map(
      (id, index): Box => ({
        id,
        x: 188,
        y: (3 - index) * ROW,
        w: 160,
        h: 58,
        kind: index === 0 || index === 3 ? 'bad' : 'core',
        lines: lines(words, id),
      }),
    ),
    {id: 'pub', x: 0, y: 5 * ROW, w: 360, h: 44, kind: 'note', lines: lines(words, 'pub')},
  ];
  const links: Link[] = [
    ...FLOW.slice(1).map((id, index) => ({from: FLOW[index]!, to: id})),
    {from: 'channel', to: 'full'},
    ...WAIT.slice(1).map((id, index) => ({from: WAIT[index]!, to: id})),
  ];
  return {boxes, links};
}
