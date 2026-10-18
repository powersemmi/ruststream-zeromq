import {expect, it} from 'vitest';
import type {Box} from './flow';
import {LAYOUTS, layoutOf, type DiagramName} from './layouts';
import type {Words} from './words';

// Every word is its own key, so the test reads the layout and never a page's wording.
const words = new Proxy({}, {get: (_, key) => String(key)}) as Words;

// The drawing area every diagram is laid out on: a phone shows it at full width.
const WIDTH = 360;

const names = Object.keys(LAYOUTS) as DiagramName[];

// A frame holds other boxes and a note is text on the background; every other box is solid,
// and two solid boxes on one spot would hide each other's words.
const solid = (box: Box): boolean => box.kind !== 'group' && box.kind !== 'note';

const overlap = (a: Box, b: Box): boolean =>
  a.x < b.x + b.w && b.x < a.x + a.w && a.y < b.y + b.h && b.y < a.y + a.h;

it('registers a diagram for every Architecture page that leads with one', () => {
  expect(names.sort()).toEqual(
    [
      'backpressure',
      'design',
      'envelope',
      'frames',
      'lifecycle',
      'path',
      'queues',
      'retries',
      'settlement',
      'sockets',
    ].sort(),
  );
});

it.each(names)('%s: every box has an id of its own', (name) => {
  const ids = layoutOf(name, words).boxes.map((box) => box.id);
  expect(new Set(ids).size).toBe(ids.length);
});

it.each(names)('%s: every box lies on the drawing area', (name) => {
  for (const box of layoutOf(name, words).boxes) {
    expect(box.x, box.id).toBeGreaterThanOrEqual(0);
    expect(box.y, box.id).toBeGreaterThanOrEqual(0);
    expect(box.x + box.w, box.id).toBeLessThanOrEqual(WIDTH);
  }
});

it.each(names)('%s: no two solid boxes cover each other', (name) => {
  const boxes = layoutOf(name, words).boxes.filter(solid);
  const covered = boxes.flatMap((a, index) =>
    boxes.slice(index + 1).filter((b) => overlap(a, b)).map((b) => `${a.id} and ${b.id}`),
  );
  expect(covered).toEqual([]);
});

it.each(names)('%s: every link joins two boxes of the drawing', (name) => {
  const {boxes, links} = layoutOf(name, words);
  const ids = new Set(boxes.map((box) => box.id));
  for (const link of links) {
    expect(ids.has(link.from), link.from).toBe(true);
    expect(ids.has(link.to), link.to).toBe(true);
  }
});

it.each(names)('%s: every box is tall enough for the lines it holds', (name) => {
  for (const box of layoutOf(name, words).boxes.filter(solid)) {
    // A line is 15 units, with room above the first and below the last.
    expect(box.h, box.id).toBeGreaterThanOrEqual(13 + 15 * box.lines.length);
  }
});
