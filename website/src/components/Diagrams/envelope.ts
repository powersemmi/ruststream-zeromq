import type {Box, Link} from './flow';
import {lines, word, type Words} from './words';

// The frames of a message at the ROUTER: the identity the ROUTER adds, then the crate's three.
const ROW = ['Identity', 'Name', 'Headers', 'Payload'] as const;
const CELL = 78;
const STEP = 86;

/*
 * A request and its reply at the responder's ROUTER: the identity frame in front of a request
 * becomes the request's reply-to header, and the reply travels behind that identity, which the
 * ROUTER takes off again before the requester's DEALER reads the other three frames.
 */
export function envelope(words: Words): {boxes: Box[]; links: Link[]} {
  const row = (prefix: 'request' | 'reply', y: number): Box[] =>
    ROW.map((frame, index) => ({
      id: `${prefix}${frame}`,
      x: 12 + index * STEP,
      y,
      w: CELL,
      h: 44,
      kind: index === 0 ? 'broker' : 'service',
      code: prefix === 'reply' && frame === 'Name',
      lines: lines(words, `${prefix}${frame}`),
    }));
  const boxes: Box[] = [
    {id: 'request', x: 0, y: 0, w: 360, h: 82, kind: 'group', lines: lines(words, 'request')},
    ...row('request', 26),
    {
      id: 'stamp',
      x: 12,
      y: 116,
      w: 336,
      h: 44,
      kind: 'core',
      lines: lines(words, 'stamp'),
      href: '/components/request-reply/responders/',
    },
    {id: 'reply', x: 0, y: 194, w: 360, h: 82, kind: 'group', lines: lines(words, 'reply')},
    ...row('reply', 220),
    {id: 'dealer', x: 0, y: 292, w: 360, h: 44, kind: 'note', lines: lines(words, 'dealer')},
  ];
  // The identity column, straight down from the request's identity to the reply's.
  const column = (CELL / 2) / 336;
  const links: Link[] = [
    {
      from: 'requestIdentity',
      to: 'stamp',
      enterAt: column,
      label: word(words, 'becomes'),
    },
    {from: 'stamp', to: 'replyIdentity', exitAt: column, label: word(words, 'addresses')},
  ];
  return {boxes, links};
}
