import {expect, it} from 'vitest';
import {retries} from './retries';
import type {Words} from './words';

const words = new Proxy({}, {get: (_, key) => String(key)}) as Words;

const from = (id: string): string[] =>
  retries(words)
    .links.filter((link) => link.from === id)
    .map((link) => link.to);

it('decides on the attempt cap before a copy goes anywhere', () => {
  expect(from('outcome')).toEqual(['cap']);
  expect(from('cap')).toEqual(['own', 'named', 'deadLetter', 'dropped']);
});

it('makes a new message only of what is below the cap', () => {
  expect(from('own')).toEqual(['next']);
  expect(from('named')).toEqual(['next']);
  // At the cap the delivery leaves the retry loop: nothing links back.
  expect([...from('deadLetter'), ...from('dropped')]).toEqual([]);
});
