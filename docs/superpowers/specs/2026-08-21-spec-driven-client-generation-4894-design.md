# Spec-Driven Client Generation: Rescoped Design (Epic #4894)

**Status:** design approved, plan pending
**Date:** 2026-08-21
**Supersedes:** the original Epic #4894 body and child issues #4897 through #4903

## 1. Why this rescope

The original Epic proposed generating clients for six-plus languages, all hosted inside the
`ArcadeData/arcadedb` monorepo, with a `fetch`-native TypeScript client (#4897) as the priority
deliverable. Two things changed that plan.

First, repository topology. The `ArcadeData` org already runs a strongly polyrepo layout: nearly
everything that is not the Java server lives in its own repository, including things that are
effectively clients (`arcadedb-grafana-datasource`, `langchain-arcadedb`, `llama-index-arcadedb`,
`arcadedb-haystack`, and the TypeScript monorepo `arcadedb-claude`). There is also a direct, recent
precedent for extraction: `arcadedb-helm` was pulled out of `k8s/helm/` with a full written plan,
GitHub Pages serving, and a manual-dispatch release workflow. Hosting six language toolchains,
lockfiles, and Dependabot lanes inside the Java repository would run against that grain.

Second, the risk of splitting turned out to be smaller than it first appears. The obvious objection
is that a separate repository cannot block a server PR that changes the API. But #4896 already
shipped, and it asserts on every server PR that the OpenAPI spec matches the routes Undertow
actually registers. That gate is language-independent and stays in the server repository. What a
separate client repository can suffer is therefore *staleness* (generated from an older contract),
not *incorrectness* (generated from a lying contract). Staleness is bounded by versioned contract
artifacts, scheduled regeneration, and one smoke job.

The scope also narrows. Rather than fanning out to six languages at once, this rescope builds one
repository and one language (TypeScript, covering both HTTP and gRPC), proves the infrastructure,
and only then designs the remaining languages.

## 2. Decisions

| # | Decision | Choice |
|---|---|---|
| 1 | Client shape | Generated core plus a thin hand-written ergonomic facade; raw client exported as an escape hatch |
| 2 | Type generation | Generated types committed to git, freshness enforced by a CI drift check |
| 3 | Publish boundary | Build and CI-wire now; publishing behind a manual `workflow_dispatch` |
| 4 | Facade width | Data plane, discovery and lifecycle, time series, and the Grafana/PromQL read paths |
| 5 | Spec fidelity gaps | Fixed in Java in the server repository, so every future language inherits the fix |
| 6 | gRPC transport target | Node, Bun, and Deno. Browser gRPC deferred |
| 7 | Packaging | Two packages: `@arcadedb/client` and `@arcadedb/client-grpc` |
| 8 | Identity | Repo `ArcadeData/arcadedb-clients`; npm scope `@arcadedb` |
| 9 | Epic restructuring | Epic stays in `arcadedb` as a cross-repo umbrella |

## 3. Architecture and the contract boundary

Two repositories with one explicit contract between them.

**`ArcadeData/arcadedb` owns the contract.** It is the only place the API surface is defined, and
#4896's anti-drift test already proves that definition matches the registered routes on every PR.

**`ArcadeData/arcadedb-clients` owns the consumers.** It never guesses at the API. It consumes two
artifacts attached to the GitHub Release for a given server version:

- `arcadedb-openapi-<version>.json`, fetched from a running server of exactly that version
- `arcadedb-server-<version>.proto`, copied verbatim from `grpc/src/main/proto/`

This reuses infrastructure that already exists. Releases are created manually, and
`native-image.yml` already triggers on `release: [published]` and attaches assets with
`gh release upload`. Publishing the contract is one more workflow on the same trigger.

### Known limitation

A server PR that changes a *payload shape* without changing a *route* is invisible to #4896. The
mitigation is the M2 smoke job, which must exercise the facade surface rather than a token single
query. Section 9 records a concrete instance of this class of defect that was found while drafting
this design.

## 4. M0: work that stays in `arcadedb`

| Item | Description | Blocks M1? |
|---|---|---|
| M0-1 | Stamp the bare release version into `info.version` (not the build-stamped `Constants.getVersion()`, which `publish-contract.yml` cannot match against the release tag), currently hardcoded to `"1.0.0"` | No |
| M0-2 | Publish `openapi.json` and the proto as release assets, with `.sha256` files | Yes |
| M0-3 | Model PromQL `data.result` as an `anyOf` over vector/matrix/scalar (`oneOf` cannot work: an empty result array validates against every branch) | Yes, for the facade |
| M0-4 | Settle `ai/chat`: `mode=auto` returns `text/event-stream` while the spec documents JSON | No |
| M0-5 | Rewrite Epic #4894; close #4897 through #4903 as superseded | No |
| M0-6 | Model the `arcadedb-session-id` header on `begin`, `commit`, `rollback`, `query`, `command` | Yes |

M0-3 and M0-6 are small, independent, and coverable by the existing per-domain contributor unit
tests. M0-1 is not: `createApiInfo()` lives in `OpenApiSpecGenerator` itself, which has no
per-domain contributor class, so it is covered by `OpenApiSpecGeneratorTest` instead. M0-2 should
land last so the first published contract already carries the fixes. M0-4 carries design risk and
is deliberately off the critical path.

Plugin routes are declared statically in `PluginApiSpec`, so a default server yields the complete
63-operation spec without loading the Raft or metrics plugins.

## 5. The `arcadedb-clients` repository

```
arcadedb-clients/
├── package.json                 # npm workspaces root, private
├── contracts/
│   ├── arcadedb-openapi-<ver>.json       # committed, from the release asset
│   └── arcadedb-server-<ver>.proto       # committed, from the release asset
├── packages/
│   ├── client/                  # @arcadedb/client
│   │   └── src/generated/schema.d.ts     # committed, from openapi-typescript
│   └── client-grpc/             # @arcadedb/client-grpc
│       └── src/gen/                      # committed, from buf generate
├── e2e/                         # testcontainers suites for both packages
├── scripts/                     # fetch-contract, generate
└── .github/workflows/
```

**npm workspaces, not pnpm.** The org already runs npm everywhere, CI already caches it, and
Dependabot supports npm workspaces natively. Not worth a new tool for two packages.

**`tsc` only, ESM-only, no bundler.** A thin library ships unbundled ES modules and lets the
consumer's bundler tree-shake. Bundling would make tree-shaking harder, not easier. ESM-only avoids
the dual-package hazard, and every named target speaks ESM natively. Accepted cost: consumers on
CommonJS Node cannot `require()` these packages.

**Both the contract and the generated code are committed.** The contract makes generation
reproducible offline and turns a version bump into a reviewable diff. The generated code makes the
repository buildable and editable without Docker, which matters because the facade is hand-written
TypeScript that must typecheck against those types.

**Two independent checks.** A fast `npm run generate && git diff --exit-code` on every PR proves the
committed generated code matches the committed contract, with no network and no Docker. A separate
scheduled job fetches the newest release's contract and opens a PR when it differs. Splitting them
keeps the PR gate fast and delivers "the server moved" as a reviewable PR rather than a red build.

## 6. `@arcadedb/client` (HTTP)

One runtime dependency: `openapi-fetch`. Everything else is types, generated by `openapi-typescript`.

```ts
const db = createClient({
  baseUrl: "https://host:2480",
  auth: basicAuth("root", pw),        // or bearerAuth("at-...")
});
```

`createClient` returns the facade with the raw `openapi-fetch` client exposed as `.raw`, keeping the
other 50-odd operations reachable with full generated types.

Facade surface:

- **Data plane:** `query`, `command`, `transaction(fn)`
- **Discovery and lifecycle:** `listDatabases`, `exists`, `serverInfo`, `health`, `ready`
- **Time series:** `ts.write`, `ts.query`, the latter narrowing the existing `oneOf` between
  aggregated and raw responses into a discriminated union
- **Dashboards:** `grafana.query`, `promql.query`, `promql.queryRange`, `promql.labels`,
  `promql.series`, each returning its own envelope rather than one union of four

`transaction(fn)` is the piece that earns the facade. It calls `begin`, reads the session id from the
response header, threads it through every call made on the handle passed to `fn`, and commits on
return or rolls back on throw in a `finally`. It depends entirely on M0-6.

**Errors.** Any non-2xx becomes a thrown `ArcadeDBError` carrying `status` plus the server's `error`,
`exception`, `detail`, and `requestId`. One deliberate exception: `exists` returns
`200 {result:false}` both when a database is absent and when the caller is unauthorized. The facade
returns the boolean and documents that it cannot distinguish the two cases, rather than inventing a
certainty the server does not provide.

**Tree-shaking.** `sideEffects: false` plus per-module ESM output. Importing only `query` must not
pull the PromQL or Grafana modules into a bundle; this is an explicit size-check test.

## 7. `@arcadedb/client-grpc`

**Codegen.** `buf generate` over the committed proto using `protoc-gen-es`. Connect-ES v2 and
protobuf-es v2 generate message types and service descriptors from that single plugin, so there is
no second generator to keep in step. Output is committed under `src/gen/` and covered by the same
drift gate.

**Transport.** `createGrpcTransport` from `@connectrpc/connect-node`, speaking real gRPC over
HTTP/2, matching the `NettyServerBuilder` server. Node is the supported target and the one CI gates
on. Bun and Deno are exercised in e2e and documented as supported only if those legs actually pass.

**Why not the browser.** `GrpcServerPlugin` builds on `NettyServerBuilder` (and `XdsServerBuilder`
for xDS), with no gRPC-Web handler, no Connect protocol, and no servlet adapter anywhere in
`grpcw/`. A browser cannot speak plain gRPC over HTTP/2, so the original #4900 promise of browser
use "without a proxy" is not achievable against today's server. Making it true would require a
gRPC-Web translation layer on a security-sensitive path, to put the wrong transport in front of the
persona the HTTP client already serves. Deferred to M3+.

**Auth.** The server accepts `authorization: Bearer <token>` metadata, or a username/password pair
on `x-arcade-user` and `x-arcade-password` with an optional `x-arcade-database`. This differs from
HTTP's standard `Authorization: Basic`, which is why the two packages need separate auth helpers and
why a shared core package is not yet justified. `passwordAuth` refuses to attach credentials to an
insecure channel unless the caller explicitly opts in, because gRPC metadata is plaintext and
plaintext credential exposure was one of the findings in the closed hardening issue #5048.

**Surface.** Deliberately thinner than the HTTP facade. The generated Connect clients are already
ergonomic for unary calls, so the hand-written layer covers only streaming query results and
bidirectional bulk ingest, both exposed as async iterables. Both proto services are exposed, with
the admin service on its own import path so data-plane users never surface admin RPCs in
autocomplete.

## 8. CI, testing, release, and versioning

### CI

**`arcadedb-clients`, every PR:** install, typecheck, lint, unit tests, drift gate, tree-shaking
size check, then e2e via Testcontainers against a pinned published image. The e2e harness must
enable the gRPC plugin and map both ports.

**`arcadedb-clients`, scheduled:** fetch the newest release's contract assets; if they differ from
`contracts/`, open a PR with the new contract, regenerated code, and version bump.

**`arcadedb`, M2 smoke job:** after `build-and-package` produces the Docker image, check out
`arcadedb-clients` and run its e2e suite against that freshly built tag. This is the only check that
catches a payload-shape change on the PR that introduces it. **Non-blocking initially**, so a
clients-repo problem cannot wedge server development.

Test layering: unit tests for the facade against a mocked fetch and a mocked transport, e2e for
anything touching the wire, and the drift gate treated as a test rather than a build step.

### Versioning

Packages use **independent semver** and record the contract they were generated from in an
`arcadedb.serverVersion` field, exported as a `CONTRACT_VERSION` constant and stated in the README
with a compatibility table.

Strict lockstep with the server version was rejected: it deadlocks the first time a client needs a
fix while the server has not moved, since npm forbids republishing a version, prereleases sort
*before* the release, and npm will not accept two versions differing only in build metadata. A
machine-readable `CONTRACT_VERSION` serves the actual goal (knowing exactly which server release a
client was generated against) better than a version string that merely resembles the server's.

### Release

`workflow_dispatch` with a version input, gated on the contract in `contracts/` matching what the
packages claim. Publishing uses **npm Trusted Publishing over OIDC** rather than a long-lived
`NPM_TOKEN`: no stored credential to rotate or leak, and provenance attestation comes free. This
repository already uses `id-token: write` in `mvn-release.yml`.

## 9. Findings that shaped this design

Each was verified in source while drafting, and each changed the design.

**The session header is undocumented.** `createBeginPath()` documents only the `database` path
parameter and its responses. `PostBeginHandler` returns `arcadedb-session-id` as a response header
and `AbstractServerHttpHandler` reads it as a request header, but the spec models it nowhere. A
client generated from today's spec cannot do transactions in a typed way at all. This became M0-6,
and it is a precise instance of the payload-shape gap #4896 cannot see: the route exists and is
documented, only the header modelling is missing.

**`info.version` is hardcoded to `"1.0.0"`.** The spec does not identify which server produced it,
which undermines the entire "versioned to the server release" goal. This became M0-1.

**Browser gRPC is not achievable today.** See section 7.

**PromQL `data.result` is opaque.** The three real shapes live only in a prose description, so
generated entries are effectively untyped. This became M0-3, modelled with `anyOf` rather than
`oneOf` since an empty result array cannot be excluded from matching more than one branch.

**Grafana is already modelled correctly, and needs no work.** An earlier draft of this design listed
the Grafana DataFrame envelope alongside PromQL as a fidelity gap. That was wrong.
`GrafanaApiSpec.createQueryResponseSchema()` already models the per-`refId` map via
`results.setAdditionalProperties(perTarget)`, along with frames, `schema.fields`, column-major
`data.values`, and the per-target `error`. The only opaque leaf is the individual cell value, which
is genuinely heterogeneous and correctly left as-is. M0-3 is therefore PromQL only.

**The gRPC surface is hardened.** All twelve issues from the 2026-07 gRPC audit (#5039 through
#5050) are closed, including both critical security findings and the silent-data-loss items.
Publishing a public gRPC client no longer sits on top of an unaudited surface.

## 10. Sequencing

**M0, in `arcadedb`.** Rewrite the Epic first, since it frames everything. Then in parallel: M0-1,
M0-3, M0-6. Then M0-2 last, so the first published contract carries the fixes. M0-4 proceeds
independently and gates nothing.

*Scheduling constraint:* contract assets appear only when a release is published, so M1 would
otherwise idle until the next server release. `scripts/fetch-contract.sh` must therefore accept
either a release asset or a locally booted server image, letting M1 start against a dev-built
contract and switch to the published asset when one exists.

**M1, in `arcadedb-clients`.** Bootstrap the repository, claim the npm org, wire contracts and
codegen and the drift gate, then build `@arcadedb/client` and its e2e suite, then
`@arcadedb/client-grpc` and its e2e suite, then the release workflow, then READMEs and the
compatibility table. The two packages parallelize once the codegen scaffolding exists.

**M2, in `arcadedb`.** The non-blocking smoke job, once the clients repo has a working e2e suite.

**M3+.** Python, Go, C#, and Rust, plus browser gRPC if demand appears. Designed only after M1 and
M2 have proven the infrastructure.

The critical path to a usable TS client is M0-6 and M0-2, then M1's codegen scaffolding.

## 11. Prerequisites and open items

- **Claim the `arcadedb` npm org** and configure trusted publishing for both package names. Requires
  the maintainer's npm account; nothing blocks until the first `workflow_dispatch` publish. Neither
  `@arcadedb/client` nor `@arcadedata/client` is published today, and neither org resolves on the
  registry, so the scope appears unclaimed. That probe cannot distinguish unclaimed from private.
- **Confirm the gRPC plugin's port setting name** for the e2e harness.
- ~~Confirm whether `/api/v1/openapi.json` requires authentication.~~ **Resolved:**
  `GetOpenApiHandler.isRequireAuthentication()` returns `true`, so the contract-publishing workflow
  must authenticate. This is not a real obstacle: the workflow starts the container itself, so it
  sets the root password and uses it on the fetch.
- **M0-4 (`ai/chat`) needs its own design decision:** split the operation, or move to Accept-header
  content negotiation.

## 12. Non-goals

- Hand-writing per-language clients. The existing `grpc-client` Java module is grandfathered as a
  reference, not a template, and is not retrofitted here.
- Unifying the HTTP and gRPC specs. They stay two contracts serving two personas.
- Exposing the admin and observability surface over gRPC.
- Browser gRPC, and every non-TypeScript language, until M1 and M2 have proven the infrastructure.
- A shared `@arcadedb/client-core` package. The genuinely shared surface today is small, and the
  auth mechanisms differ between transports. Revisit once Python and Go reveal what is common.
