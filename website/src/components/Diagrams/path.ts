import type {Box, Link} from './flow';
import {chain} from './chain';
import type {Words} from './words';

/** The path of one message that is answered, from the peer's frames to the reply's frames. */
export function path(words: Words): {boxes: Box[]; links: Link[]} {
  return chain(words, [
    {key: 'peer', kind: 'service', href: '/architecture/zeromq/'},
    {key: 'driver', kind: 'broker'},
    {key: 'read', kind: 'broker', href: '/components/wire/'},
    {key: 'channel', kind: 'broker', href: '/components/subscriptions/read-ahead/'},
    {key: 'dispatch', kind: 'core', href: '/architecture/backpressure/'},
    {key: 'decode'},
    {key: 'body', kind: 'service', href: '/components/subscriptions/'},
    {key: 'encode'},
    {key: 'frames', kind: 'broker', href: '/components/wire/'},
    {key: 'publisher', kind: 'broker', href: '/components/publishing/'},
    {key: 'settle', kind: 'core', href: '/components/settlement/'},
  ]);
}
