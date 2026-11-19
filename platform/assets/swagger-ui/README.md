# Swagger UI, embedded

The platform serves these files to the Swagger UI page of every public document and of `_meta/docs`.
They are embedded in the platform binary (`platform/src/router/swagger_ui.rs`), so no page loads anything from a CDN.

- Package: `swagger-ui-dist` 5.17.14, licensed Apache-2.0 (`LICENSE`, `NOTICE`).
- Source: `https://registry.npmjs.org/swagger-ui-dist/-/swagger-ui-dist-5.17.14.tgz`, integrity
  `sha512-CVbSfaLpstV65OnSjbXfVd6Sta3q3F7Cj/yYuvHMp1P90LztOLs6PfUnKEVAeiIVQt9u2SaPwv0LiH/OyMjHRw==`.
- `index.html`, `swagger-ui.css`, `index.css`, `swagger-ui-bundle.js`, `swagger-ui-standalone-preset.js`,
  `favicon-32x32.png`, `favicon-16x16.png`, `LICENSE` and `NOTICE` are the package's files, unmodified.
- `swagger-initializer.js` is the platform's own file: the package's initializer with its configuration written as a
  JSON object and its URL fixed to `../openapi.json`, relative to the page.

To move to another version, run `./vendor.sh <version> <integrity>` with the version's published integrity. It
refuses a tarball whose sha512 differs. Then update the hashes and lengths in `swagger_ui.rs`'s pinning test, and
`swagger-initializer.js` if the package's initializer changed, in the same commit.
