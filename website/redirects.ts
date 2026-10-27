// Pages of the earlier site, by their address within a version. Every language of a build gets
// the same set.

// The ZeroMQ page of the earlier site was a copy of the crate overview, which docs.rs holds.
const guides: Record<string, string> = {
  zeromq: 'index.html',
};

// Pages that moved within the site.
const pages: Record<string, string> = {
  // The benchmarks explain what the architecture costs.
  benchmarks: '/architecture/benchmarks/',
  // The identity frame of DEALER/ROUTER, once named after ZeroMQ's routing envelope; an envelope
  // is now the frame layout a broker chooses.
  'components/wire/envelope': '/components/wire/identity/',
};

/**
 * Old address to its target, relative to the build's base URL. `docsRs` is the crate's rustdoc
 * for the version built, with a trailing slash.
 */
export function redirects(docsRs: string): Record<string, string> {
  return {
    ...Object.fromEntries(
      Object.entries(guides).map(([from, to]) => [from, `${docsRs}ruststream_zeromq/${to}`]),
    ),
    ...pages,
  };
}
