import type {Box, Link} from './flow';
import {lines, word, type Words} from './words';

// The way of one message down each column: the sending socket, the connection it writes, the
// kernel's buffers, the receiving socket.
const STEPS = ['send', 'write', 'kernel', 'recv'] as const;
const COLUMNS = {push: 12, pub: 188} as const;

/*
 * Where a message waits between two zeromq sockets. The client keeps no queue of messages: a
 * PUSH writes into the connection of the peer whose turn it is, a PUB into the connection of
 * every subscriber whose filter matches, one after the other, and a send ends when the frames
 * are written. What holds messages is the kernel's socket buffers at both ends.
 */
export function queues(words: Words): {boxes: Box[]; links: Link[]} {
  const boxes: Box[] = [
    {id: 'pushPull', x: 0, y: 0, w: 176, h: 330, kind: 'group', lines: lines(words, 'pushPull')},
    {id: 'pubSub', x: 184, y: 0, w: 176, h: 330, kind: 'group', lines: lines(words, 'pubSub')},
  ];
  const links: Link[] = [];
  for (const [column, x] of Object.entries(COLUMNS)) {
    STEPS.forEach((step, index) => {
      boxes.push({
        id: `${column}${step}`,
        x,
        y: 26 + index * 76,
        w: 160,
        h: 58,
        kind: step === 'kernel' ? 'broker' : step === 'write' ? 'core' : 'service',
        lines: lines(words, `${column}${step}`),
      });
      if (index > 0) {
        links.push({from: `${column}${STEPS[index - 1]}`, to: `${column}${step}`});
      }
    });
  }
  links[0] = {...links[0]!, label: word(words, 'inTurn')};
  boxes.push(
    {id: 'client', x: 0, y: 344, w: 360, h: 44, kind: 'note', lines: lines(words, 'client')},
    {id: 'libzmq', x: 0, y: 396, w: 360, h: 44, kind: 'note', lines: lines(words, 'libzmq')},
  );
  return {boxes, links};
}
