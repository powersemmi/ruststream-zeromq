import {envelope} from './envelope';
import type {Box, Lane, Link} from './flow';
import {frames} from './frames';
import {queues} from './queues';
import {sockets} from './sockets';
import type {Words} from './words';

export interface Layout {
  boxes: Box[];
  links: Link[];
  lanes?: Lane[];
}

type Place = (words: Words) => Layout;

/** Every diagram a page can name: a layout places the boxes, the page supplies the words. */
export const LAYOUTS = {
  sockets,
  frames,
  envelope,
  queues,
} satisfies Record<string, Place>;

export type DiagramName = keyof typeof LAYOUTS;

/** The boxes and links of the diagram `name`, with the words of the page. */
export function layoutOf(name: DiagramName, words: Words): Layout {
  return LAYOUTS[name](words);
}
