import type {Box, Link} from './flow';
import {lines, word, type Words} from './words';

/*
 * Where the crate sits: the service on top, the core's runtime under it, this crate between the
 * runtime and the zeromq client, and the peers at the bottom, other processes with no server
 * between them. A subscription owns a socket of its own, read by a task of its own; a publisher
 * attaches a socket of its own on its first publish.
 */
export function design(words: Words): {boxes: Box[]; links: Link[]} {
  const box = (
    id: string,
    x: number,
    y: number,
    w: number,
    h: number,
    fields: Partial<Box> = {},
  ): Box => ({id, x, y, w, h, kind: 'step', lines: lines(words, id), ...fields});
  // The crate's columns leave a gutter at each side, where a link from a box to the client under
  // it runs past the boxes in between.
  const left = 28;
  const right = 182;
  const column = 150;
  const centre = (x: number): number => (x + column / 2 - 12) / 336;
  const boxes: Box[] = [
    box('service', 0, 0, 360, 80, {kind: 'group'}),
    box('handlers', 12, 26, 160, 42, {kind: 'service', code: true}),
    box('app', 188, 26, 160, 42, {kind: 'service', code: true}),
    box('core', 0, 104, 360, 86, {kind: 'group'}),
    box('runtime', 12, 128, 336, 50, {kind: 'core'}),
    box('crate', 0, 214, 360, 240, {kind: 'group'}),
    box('subscriptions', left, 240, column, 58, {kind: 'broker', href: '/architecture/backpressure/'}),
    box('publishers', right, 240, column, 58, {kind: 'broker', href: '/architecture/path/'}),
    box('brokers', left, 310, column, 58, {kind: 'broker', code: true}),
    box('lifecycle', right, 310, column, 58, {kind: 'broker', href: '/architecture/lifecycle/'}),
    box('wire', left, 380, 304, 58, {kind: 'broker', href: '/architecture/zeromq/frames/'}),
    box('client', 0, 478, 360, 80, {kind: 'group'}),
    box('receiving', left, 504, column, 44, {kind: 'broker', code: true}),
    box('sending', right, 504, column, 44, {kind: 'broker', code: true}),
    box('peers', 12, 582, 336, 44, {kind: 'service', href: '/architecture/zeromq/'}),
    box('testing', 0, 640, 360, 44, {kind: 'note'}),
  ];
  const links: Link[] = [
    {from: 'handlers', to: 'runtime', enterAt: 80 / 336, label: word(words, 'mounted')},
    {from: 'runtime', to: 'subscriptions', exitAt: centre(left), label: word(words, 'polls')},
    {from: 'subscriptions', to: 'receiving', exit: 'left', enter: 'left'},
    {from: 'publishers', to: 'sending', exit: 'right', enter: 'right'},
    {from: 'receiving', to: 'peers', enterAt: centre(left)},
    {from: 'sending', to: 'peers', enterAt: centre(right)},
  ];
  return {boxes, links};
}
