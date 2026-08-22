# Issue #6563: OpenAPI: X-Request-Id is an undocumented response header on every endpoint

## Root cause

`AbstractServerHttpHandler` (server/src/main/java/com/arcadedb/server/http/handler/AbstractServerHttpHandler.java:305-308)
sets the `X-Request-Id` response header unconditionally on every response, generating a value
when the caller did not supply one. The generated OpenAPI document (`OpenApiSpecGenerator` and its
`OpenApiContributor` implementations) never declares this header on any response, so a generated
client in any language has no way to see it exists, even though it is the value needed to correlate
a failed request with a server log line.

## Affected components

- `server/src/main/java/com/arcadedb/server/http/handler/OpenApiSpecGenerator.java` - assembles the
  document-level parts of the spec (`Components`, then delegates to contributors).
- `server/src/test/java/com/arcadedb/server/http/OpenApiSpecGenerationIT.java` - the integration test
  that validates the served document end-to-end.

## Expected vs actual behavior

- Expected: every response the server can produce declares the `X-Request-Id` header in the OpenAPI
  spec, since `AbstractServerHttpHandler` sets it unconditionally.
- Actual: the header is declared nowhere in the spec.

## Fix approach

Because the header applies to every response of every operation regardless of which contributor owns
the path (a cross-cutting concern, like the root security declaration), it is applied centrally in
`OpenApiSpecGenerator.generateSpec()` after all contributors have run, rather than requiring every
contributor to remember to add it per-response. A reusable `Header` component
(`components.headers.RequestIdHeader`) is declared once in `createComponents()`, and every response of
every operation gets a `$ref` to it.

## Test plan

- `OpenApiSpecGeneratorTest#everyResponseDeclaresTheRequestIdHeader` (new, unit-level, runs locally):
  builds the spec directly via `new OpenApiSpecGenerator(null).generateSpec()` and asserts every
  response of every operation declares the `X-Request-Id` header. Confirmed RED before the fix
  (`Expecting actual not to be null` on the first operation encountered), GREEN after.
- `OpenApiSpecGenerationIT#everyResponseDeclaresTheRequestIdHeader` (new, end-to-end): same assertion
  against the served document over HTTP. Left to CI: this machine has a Homebrew ArcadeDB service
  permanently bound to `*:2480` (see `reference_local_port_2480_held_by_brew_arcadedb` in project
  memory), so `BaseGraphServerTest`-based ITs cannot bind their own test server here. This is the same
  constraint the existing `infoVersionIsTheBareReleaseVersionNotTheBuildStampedOne` unit test in
  `OpenApiSpecGeneratorTest` was written to work around.

## Verification performed

- `mvn -o -pl server -am test-compile`: BUILD SUCCESS.
- `mvn -o -pl server -am test -Dtest=OpenApiSpecGeneratorTest`: 1 failure (the new test) before the
  fix, 3/3 passing after.
- `mvn -o -pl server -am test` scoped to all 11 OpenAPI contributor test classes plus
  `OpenApiSpecGeneratorTest`: 132/132 passing after the fix (no regression in existing header/response
  assertions, e.g. `CoreApiSpecTest#beginDeclaresTheSessionIdResponseHeader`, which builds its spec
  from a single contributor directly and is unaffected since the new header is applied centrally in
  `OpenApiSpecGenerator.generateSpec()`, not per-contributor).
- `OpenApiSpecGenerationIT` and other `BaseGraphServerTest`-based ITs not run locally (port 2480
  contention, see above); left to CI.
