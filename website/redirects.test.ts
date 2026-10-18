import {expect, it} from 'vitest';
import {redirects} from './redirects';

it('moves no address yet', () => {
  expect(redirects('https://docs.rs/ruststream-zeromq/0.8/')).toEqual({});
});
