import {existsSync} from 'node:fs';
import {join} from 'node:path';
import {describe, expect, it} from 'vitest';
import {LAYOUTS, layoutOf, type DiagramName} from './layouts';
import type {Words} from './words';

// Every word is its own key, so the test reads the layout and never a page's wording.
const words = new Proxy({}, {get: (_, key) => String(key)}) as Words;

// The docs are served from the site root, so a route names the page's file under `docs/`.
const DOCS = join(__dirname, '..', '..', '..', '..', 'docs');

const hrefs = (name: DiagramName): Record<string, string> =>
  Object.fromEntries(
    layoutOf(name, words)
      .boxes.filter((box) => box.href !== undefined)
      .map((box) => [box.id, box.href!]),
  );

const page = (href: string): boolean => {
  const route = href.replace(/^\/|\/$/g, '');
  return ['.md', '.mdx', '/index.md', '/index.mdx'].some((ending) =>
    existsSync(join(DOCS, route + ending)),
  );
};

// Every box of every layout that links to a page. The check is a test of its own per box, outside
// a suite, so a registry with no diagram yet runs none of them.
const linked = (Object.keys(LAYOUTS) as DiagramName[]).flatMap((name) =>
  Object.entries(hrefs(name)).map(([box, href]) => ({name, box, href})),
);

it.each(linked)('a box that links to a page: $name, $box: $href is a page of the site', ({href}) => {
  // A root-relative route: the link prepends the language's base URL, so one route serves
  // every translation, and the site ends every route with a slash.
  expect(href).toMatch(/^\/[a-z0-9/-]+\/$/);
  expect(page(href), `no page under docs/ for ${href}`).toBe(true);
});

describe('a route', () => {
  it('resolves to its page under docs/, an index page included', () => {
    expect(page('/architecture/')).toBe(true);
    expect(page('/architecture/benchmarks/')).toBe(true);
  });

  it('resolves to nothing where docs/ has no page', () => {
    expect(page('/architecture/no-such-page/')).toBe(false);
  });
});
