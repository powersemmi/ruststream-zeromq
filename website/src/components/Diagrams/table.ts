import type {Box, BoxKind, Lane, Link} from './flow';
import {lines, word, type Words} from './words';

export interface Cell {
  id: string;
  kind?: BoxKind;
  code?: boolean;
  href?: string;
  // Lines the cell takes, when it holds more than the default two.
  rows?: number;
}

export interface Table {
  // The word of each column's header, one per column.
  lanes: string[];
  // The width of each column; the columns spread over the drawing with even gaps between them.
  widths: number[];
  rows: Cell[][];
}

const WIDTH = 360;
// Room above the first row for the column headers.
const TOP = 28;
const GAP = 12;
const LINE = 15;

/*
 * A table drawn as a timeline of columns: each row reads left to right, a cell leading to the
 * next one, and each column is headed by who acts in it. The first column is the service's, the
 * others the broker's, unless a cell names its own kind.
 */
export function table(
  words: Words,
  {lanes, widths, rows}: Table,
): {boxes: Box[]; links: Link[]; lanes: Lane[]} {
  const total = widths.reduce((sum, width) => sum + width, 0);
  const gap = widths.length > 1 ? (WIDTH - total) / (widths.length - 1) : 0;
  const xs = widths.map((_, column) =>
    widths.slice(0, column).reduce((x, width) => x + width + gap, 0),
  );
  const boxes: Box[] = [];
  const links: Link[] = [];
  let y = TOP;
  for (const row of rows) {
    const h = 14 + LINE * Math.max(...row.map((cell) => cell.rows ?? 2));
    row.forEach((cell, column) => {
      boxes.push({
        id: cell.id,
        x: xs[column]!,
        y,
        w: widths[column]!,
        h,
        kind: cell.kind ?? (column === 0 ? 'service' : 'broker'),
        code: cell.code,
        href: cell.href,
        lines: lines(words, cell.id),
      });
      if (column > 0) {
        links.push({from: row[column - 1]!.id, to: cell.id});
      }
    });
    y += h + GAP;
  }
  return {
    boxes,
    links,
    lanes: lanes.map((key, column) => ({
      x: xs[column]! + widths[column]! / 2,
      label: word(words, key),
    })),
  };
}
