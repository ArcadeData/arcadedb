# ArcadeDB Native Image - Design

**Date:** 2026-07-18
**Status:** approved
**Base version:** 26.8.1-SNAPSHOT
**Supersedes:** the stale `poc/native_image` branch (v26.1.1-SNAPSHOT, GraalVM 23.1.1)

## Goal

Produce a GraalVM native-image build of the ArcadeDB server: a self-contained
native binary with fast cold start and low memory footprint, built for multiple
OS/architecture targets, and shipped as tiny static Docker images for
linux/amd64 and linux/arm64. This is delivered as an **experimental add-on** that
never gates or replaces the existing JVM distribution and Docker images.

## Scope

### Feature set baked into the native binary

**Included** (compile-scope deps of the `native` module):
`engine`, `network`, `server` (HTTP/REST + Studio), `ha-raft`, `metrics`,
`console`, `graphql`, and the wire protocols `postgresw`, `redisw`, `mongodbw`,
`bolt`, `grpcw`.

**Excluded:**
- **Gremlin** (TinkerPop + Groovy) - removed. Largest dynamic-reflection surface;
  not required for the native value prop.
- **OpenTelemetry `tracing` module** - excluded for now to cut reflection surface.
  Easy to add back once the binary is stable.

**Conditional - GraalVM polyglot JS** (JS user-defined SQL functions + the
polyglot query engine): decided by a **time-boxed Phase-1 spike**. If Truffle-JS
in native-image does not converge within the box, drop polyglot from the binary,
surface a clear "JS scripting unavailable in native build" error when invoked, and
document the limitation. **SQL and Cypher are unaffected either way.**

### Non-goals

- Replacing the JVM distribution or its Docker images.
- windows/arm64 (no free GitHub-hosted runner).
- Gating PRs or the release on native builds (add-on model only).

## Architecture

A single new **`native` Maven module** (revived from the POC, rebased onto
v26.8.1 / GraalVM CE 25 (JDK 25) Community). It aggregates the included modules and is
activated only by a `-Pnative` profile, so it never slows the normal
`mvn clean install` reactor. Main class: `com.arcadedb.server.ArcadeDBServer`.

Delivered in **three risk-gated phases**, each independently valuable and mergeable:

1. **Phase 1** - make one binary (linux/amd64) build and run. All technical risk here.
2. **Phase 2** - matrix workflow across 5 OS/arch targets, publishing binaries.
3. **Phase 3** - static-musl Docker images on `scratch`, multi-arch manifest.

## Phase 1 - Native binary build (the hard part)

### Reachability metadata strategy

Do **not** hand-maintain large reflect/resource JSON (the POC's ~1400-line
`reflect-config.json` rots quickly). Instead:

1. **GraalVM tracing agent** (`-agentlib:native-image-agent`) driving a scripted
   exercise: server boot, create DB, SQL + Cypher queries, one round-trip per
   included wire protocol, Studio asset fetch. Regenerates
   `reflect/resource/jni/serialization/proxy` config into
   `native/src/main/resources/META-INF/native-image/`.
2. **Library-supplied metadata** from the GraalVM reachability-metadata repository
   (Netty, Lucene, gRPC/protobuf, graphql-java ship known configs).
3. Hand-curate only ArcadeDB-specific deltas.

### Known blockers (each a plan step with its own smoke check)

- **Undertow + `wildfly-common`** - the POC's wall. Split build-time vs run-time
  init of `HostName`/logging via `--initialize-at-build-time` / `--initialize-at-run-time`.
  Expect the most iteration here.
- **Netty** - runtime-init flags (POC has a starting set). For the musl-static Linux
  build, disable the native DNS resolver / force the JDK resolver to sidestep
  `getaddrinfo` under static musl.
- **Lucene** full-text - codecs & SPI via service-loader metadata + `-H:IncludeResources`.
- **gRPC/protobuf + graphql-java** - reachability-repo metadata.
- **JLine JNI terminal** - jni-config + bundle the native lib, or force the
  dumb/exec terminal in server mode (preferred fallback).
- **JVector + `jdk.incubator.vector`** - pass `--add-modules jdk.incubator.vector`;
  accept scalar fallback if SIMD intrinsics do not compile (perf only, not a blocker).

### Polyglot-JS spike (time-boxed)

Attempt Truffle-JS-in-native-image with version-aligned `js-language`/Truffle
matching the native-image builder. On box expiry without convergence: drop polyglot,
register a clear unsupported-operation message on JS invocation, document it.

### Phase 1 exit criterion

A shell smoke-test script (reused by CI later) boots the native binary, waits for
HTTP readiness, and asserts:
- Studio index loads,
- a `CREATE`/`SELECT` SQL round-trips,
- a Cypher `MATCH` round-trips,
- a Postgres-wire `SELECT` succeeds.

## Phase 2 - Matrix workflow

New workflow `native-image.yml` (replaces the POC's Linux-only one).

| Job | Runner | Artifact | Required? |
|---|---|---|---|
| linux/amd64 | `ubuntu-latest` | `arcadedb-<ver>-linux-amd64` | required |
| linux/arm64 | `ubuntu-24.04-arm` | `arcadedb-<ver>-linux-arm64` | required |
| macos/arm64 | `macos-14` | `arcadedb-<ver>-macos-arm64` | best-effort |
| macos/amd64 | `macos-13` | `arcadedb-<ver>-macos-amd64` | best-effort |
| windows/amd64 | `windows-latest` | `arcadedb-<ver>-windows-amd64.exe` | best-effort |

- `matrix.include` with `continue-on-error` per target; Mac/Windows are best-effort,
  Linux is required (the contract). Native-image cannot cross-compile - each target
  builds on its own matching runner.
- `graalvm/setup-graalvm@v1`, `graalvm-community` / JDK 25, per-OS.
- Linux jobs additionally install the **musl toolchain** and build
  `--static --libc=musl`; Mac/Windows build dynamic.
- Each job runs the Phase-1 **smoke script** against its own binary before uploading
  (Windows smoke best-effort).
- Triggers: `workflow_dispatch` + `release` (published).
- On a release, binaries attach as **release assets** (`gh release upload`),
  compressed (`.tar.gz` / `.zip`) with a `SHA256SUMS` file.

## Phase 3 - Static-musl Docker images

Folds into the same workflow; `needs:` the two Linux jobs.

- Two `Dockerfile.native` builds `FROM scratch`, each `COPY`ing the matching
  musl-static binary + minimal runtime layout (config, Studio assets, empty
  `databases` volume) + a **CA-certificates bundle** (scratch has none; needed for
  HTTPS dataset import / MCP).
- `docker buildx` per-arch on the native runner, then one **multi-arch manifest** via
  `docker buildx imagetools` - **no QEMU** (each arch built natively, then stitched).
- Tags (additional, never touching JVM `latest`): `arcadedata/arcadedb:<ver>-native`
  fanning out to amd64/arm64 layers; optional moving `-native` tag.
- Same `EXPOSE`/`VOLUME`/non-root-user model as today's Dockerfile. JVM env vars
  (`JAVA_OPTS` etc.) are meaningless for a native binary and are replaced by native
  runtime flags (e.g. `-XX:MaximumHeapSizePercent` / substrate equivalents).

## Verification & testing

- The **smoke script is the single source of truth**: Phase 1 local, per-matrix-job
  in Phase 2, and against the built container in Phase 3 (`docker run` -> smoke).
- A short `docs/` page: how to build locally (`mvn -Pnative`), the JS-scripting
  limitation (pending spike outcome), and the SIMD-fallback perf note.

## Risks & mitigations

| Risk | Mitigation |
|---|---|
| Polyglot JS does not converge in native-image | Time-box the spike; drop JS with a clear error + docs. SQL/Cypher unaffected. |
| musl-static + Netty DNS (`getaddrinfo`) | Force the JDK DNS resolver; disable Netty native resolver on the static Linux build. |
| macOS/Windows native-image quirks | `continue-on-error`; Linux is the required contract, Mac/Windows best-effort. |
| Reachability metadata rot | Regenerate via tracing agent + reachability-repo metadata; hand-curate only deltas. |
| JVector SIMD intrinsics not compiled | Accept scalar fallback (perf only); note in docs, guard with benchmark tag. |

## Deliverables

- Revived `native/` module + `-Pnative` profile (rebased, GraalVM CE 25).
- Regenerated native-image metadata under `native/src/main/resources/META-INF/native-image/`.
- Reusable smoke-test script.
- `native-image.yml` 5-target matrix workflow with release-asset publishing.
- `Dockerfile.native` (x2 arch) + multi-arch `-native` manifest publishing.
- `docs/` page for the native build and its limitations.
