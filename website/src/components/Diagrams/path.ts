import type {Box, Link} from './flow';
import {chain} from './chain';
import type {Words} from './words';

/** The path of one message that is answered, from the peer's frames to the reply's frames. */
export function path(words: Words): {boxes: Box[]; links: Link[]} {
  return chain(words, [
    {key: 'peer', kind: 'service', href: '/architecture/zeromq/'},
    {key: 'driver', kind: 'broker'},
    {key: 'read', kind: 'broker', href: '/architecture/zeromq/frames/'},
    {key: 'channel', kind: 'broker', href: '/architecture/backpressure/'},
    {key: 'dispatch', kind: 'core', href: '/architecture/backpressure/'},
    {key: 'decode'},
    {key: 'body', kind: 'service'},
    {key: 'encode'},
    {key: 'frames', kind: 'broker', href: '/architecture/zeromq/frames/'},
    {key: 'publisher', kind: 'broker'},
    {key: 'settle', kind: 'core', href: '/architecture/settlement/'},
  ]);
}
