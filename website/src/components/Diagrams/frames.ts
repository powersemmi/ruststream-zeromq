import type {Box, Link} from './flow';
import {lines, word, type Words} from './words';

// The three frames of one message, side by side across the drawing.
export const FRAMES = ['name', 'headers', 'payload'] as const;

/*
 * A ZMTP connection as both peers open it, the greeting, then the READY command, then messages
 * made of frames, and under it the three frames this crate puts in every message.
 */
export function frames(words: Words): {boxes: Box[]; links: Link[]} {
  const step = (id: string, y: number): Box => ({
    id,
    x: 12,
    y,
    w: 336,
    h: 44,
    kind: 'broker',
    lines: lines(words, id),
  });
  const boxes: Box[] = [
    {id: 'connection', x: 0, y: 0, w: 360, h: 228, kind: 'group', lines: lines(words, 'connection')},
    step('greeting', 26),
    step('ready', 100),
    step('messages', 174),
    {id: 'message', x: 0, y: 264, w: 360, h: 82, kind: 'group', lines: lines(words, 'message')},
    ...FRAMES.map(
      (id, index): Box => ({
        id,
        x: 12 + index * 116,
        y: 290,
        w: 104,
        h: 44,
        kind: 'service',
        lines: lines(words, id),
      }),
    ),
    {id: 'rules', x: 0, y: 360, w: 360, h: 44, kind: 'note', lines: lines(words, 'rules')},
  ];
  const links: Link[] = [
    {from: 'greeting', to: 'ready', label: word(words, 'bothWays')},
    {from: 'ready', to: 'messages', label: word(words, 'bothWays')},
    {from: 'messages', to: 'message', label: word(words, 'oneMessage')},
  ];
  return {boxes, links};
}
