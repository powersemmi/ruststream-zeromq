import {describe, expect, it} from 'vitest';
import type {OnBrokenMarkdownLinksFunction} from '@docusaurus/types';
import {resolveMarkdownLinks} from './markdownLinks';

const hook = resolveMarkdownLinks() as OnBrokenMarkdownLinksFunction;
const resolve = (url: string, sourceFilePath: string) => hook({url, sourceFilePath} as never);

describe('resolveMarkdownLinks', () => {
  it('sends a translated page to an untranslated neighbour in the same language', () => {
    expect(
      resolve(
        '../components/broker/security.md',
        'i18n/ru/docusaurus-plugin-content-docs/current/guides/production.md',
      ),
    ).toBe('/components/broker/security/');
  });

  it('sends an untranslated page to a translated neighbour in the same language', () => {
    expect(resolve('../../guides/getting-started.md#run', '../docs/components/broker/index.md')).toBe(
      '/guides/getting-started/#run',
    );
  });

  it('fails the build on a link to a page that does not exist', () => {
    expect(() => resolve('../../guides/missing.md', '../docs/components/broker/index.md')).toThrow(
      "Markdown link '../../guides/missing.md' in '../docs/components/broker/index.md' does not resolve",
    );
  });

  it('fails the build on a link out of the docs', () => {
    expect(() => resolve('../../../README.md', '../docs/components/broker/index.md')).toThrow(
      'does not resolve',
    );
  });
});
