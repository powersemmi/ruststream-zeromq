import type {Box, Link} from './flow';
import {lines, word, type Words} from './words';

/*
 * The broker's three states as types on the left, and on the right what each transition does on
 * ZeroMQ: nothing at construction, a check of the address at connect, a socket per subscription
 * at startup, a socket per publisher on its first publish, and a flag at shutdown.
 */
export function lifecycle(words: Words): {boxes: Box[]; links: Link[]} {
  const left = (id: string, y: number, h = 58): Box => ({
    id,
    x: 12,
    y,
    w: 156,
    h,
    kind: 'broker',
    code: true,
    lines: lines(words, id),
  });
  const right = (id: string, y: number, h = 58): Box => ({
    id,
    x: 192,
    y,
    w: 156,
    h,
    kind: 'core',
    lines: lines(words, id),
  });
  const boxes: Box[] = [
    {id: 'states', x: 0, y: 0, w: 176, h: 410, kind: 'group', lines: lines(words, 'states')},
    {id: 'zeromq', x: 184, y: 0, w: 176, h: 410, kind: 'group', lines: lines(words, 'zeromq')},
    left('unconnected', 28),
    left('connected', 168),
    left('closed', 340),
    right('config', 28),
    right('check', 98),
    right('subscriptions', 168, 72),
    right('publishers', 252),
    right('flag', 340),
    {
      id: 'afterShutdown',
      x: 0,
      y: 432,
      w: 360,
      h: 44,
      kind: 'note',
      lines: lines(words, 'afterShutdown'),
    },
    {
      id: 'dropped',
      x: 0,
      y: 484,
      w: 360,
      h: 44,
      kind: 'note',
      lines: lines(words, 'dropped'),
      href: '/architecture/backpressure/',
    },
  ];
  const links: Link[] = [
    {from: 'unconnected', to: 'connected', label: word(words, 'connect')},
    {from: 'connected', to: 'closed', label: word(words, 'shutdown')},
    {from: 'config', to: 'check'},
    {from: 'check', to: 'subscriptions'},
    {from: 'subscriptions', to: 'publishers', dashed: true},
    {from: 'publishers', to: 'flag'},
  ];
  return {boxes, links};
}
