import type {Box, Lane, Link} from './flow';
import type {Words} from './words';

export interface Layout {
  boxes: Box[];
  links: Link[];
  lanes?: Lane[];
}

type Place = (words: Words) => Layout;

/** Every diagram a page can name: a layout places the boxes, the page supplies the words. */
export const LAYOUTS = {} satisfies Record<string, Place>;

export type DiagramName = keyof typeof LAYOUTS;

// The registry seen through its names: a page still names only a registered diagram, and the
// lookup stays callable while the registry is empty.
const registry: Record<string, Place> = LAYOUTS;

/** The boxes and links of the diagram `name`, with the words of the page. */
export function layoutOf(name: DiagramName, words: Words): Layout {
  return registry[name]!(words);
}
