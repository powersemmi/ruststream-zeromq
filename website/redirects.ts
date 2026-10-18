// Pages of the earlier site, by their address within a version. Every language of a build gets
// the same set.

// Pages of the earlier site that moved to the crate's rustdoc.
const guides: Record<string, string> = {};

// Pages that moved within the site.
const pages: Record<string, string> = {};

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
