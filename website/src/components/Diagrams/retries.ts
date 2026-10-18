import type {Box, Link} from './flow';
import {lines, word, type Words} from './words';

const CELL = 84;
const STEP = 92;

/*
 * A retry on ZeroMQ, where nothing settles and every retry is a copy this service publishes: the
 * runtime reads the attempt off the framework's header and the registration's cap decides. Below
 * the cap the copy goes to the subscription's own listener when it binds, and to the destination
 * the mount site names when it dials or answers requests; at the cap it goes to the dead-letter
 * destination, or nowhere.
 */
export function retries(words: Words): {boxes: Box[]; links: Link[]} {
  const answers = ['own', 'named', 'deadLetter', 'dropped'] as const;
  const boxes: Box[] = [
    {id: 'delivery', x: 12, y: 0, w: 336, h: 44, kind: 'service', lines: lines(words, 'delivery')},
    {id: 'outcome', x: 12, y: 72, w: 336, h: 44, kind: 'service', lines: lines(words, 'outcome')},
    {id: 'cap', x: 12, y: 144, w: 336, h: 44, kind: 'core', lines: lines(words, 'cap')},
    ...answers.map(
      (id, index): Box => ({
        id,
        x: index * STEP,
        y: 220,
        w: CELL,
        h: 104,
        kind: index < 2 ? 'step' : 'bad',
        lines: lines(words, id),
      }),
    ),
    {
      id: 'next',
      x: 0,
      y: 352,
      w: STEP + CELL,
      h: 58,
      kind: 'service',
      lines: lines(words, 'next'),
    },
    {id: 'crash', x: 0, y: 432, w: 360, h: 44, kind: 'note', lines: lines(words, 'crash')},
  ];
  // Each answer is reached straight down from the cap, from above its own column.
  const along = (index: number): number => (index * STEP + CELL / 2 - 12) / 336;
  const links: Link[] = [
    {from: 'delivery', to: 'outcome', label: word(words, 'handled')},
    {from: 'outcome', to: 'cap', label: word(words, 'retries')},
    ...answers.map((id, index) => ({
      from: 'cap',
      to: id,
      exitAt: along(index),
      ...(index === 0 ? {label: word(words, 'below')} : {}),
      ...(index === 2 ? {label: word(words, 'atCap')} : {}),
    })),
    {from: 'own', to: 'next', enterAt: CELL / 2 / (STEP + CELL)},
    {from: 'named', to: 'next', enterAt: (STEP + CELL / 2) / (STEP + CELL)},
  ];
  return {boxes, links};
}
