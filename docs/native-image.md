# Native Image (experimental)

ArcadeDB can be compiled into a native binary of the ArcadeDB server using
[GraalVM Native Image](https://www.graalvm.org/latest/reference-manual/native-image/). The native
binary starts in a fraction of a second and uses far less RAM than the equivalent JVM process,
which makes it attractive for containers, serverless-style deployments, and quick local testing.

This is an **experimental add-on**, built and published from a dedicated `native` Maven module and
a separate `-Pnative` profile / CI workflow. It does **not** replace the standard JVM distribution:
the JVM build remains the default, fully supported way to run ArcadeDB, carries the full module set
(including Gremlin), and is what every other doc on this site assumes. Reach for the native image
when fast startup and a small memory footprint matter more than having every module available, and
treat it as best-effort outside Linux.

## Prerequisites

- **GraalVM CE 25 (JDK 25).** The build uses the GraalVM Native Image Community Edition builder for
  JDK 25 (CI pins `graalvm-community-jdk-25.0.2` via
  [`graalvm/setup-graalvm`](https://github.com/graalvm/setup-graalvm)). `native/pom.xml` also pins
  the GraalVM polyglot/Truffle artifacts (`graal-sdk`, `polyglot`, `js-language`, `truffle-*`, etc.)
  to `25.0.2` to match the builder exactly; a Truffle version skew between the builder and those
  artifacts fails the build at feature registration (`NoSuchMethodError:
  OptimizedTruffleRuntime.getLoopNodeFactory()`).
- **`JAVA_HOME` (and `GRAALVM_HOME`) must point at the GraalVM home itself.** The
  `native-maven-plugin` resolves the native-image builder from `JAVA_HOME`/`GRAALVM_HOME`, not by
  searching `PATH` for a `native-image` executable. A version-manager shim (jenv, sdkman shell
  shims, etc.) that only puts `native-image` on `PATH` is **not sufficient** - if `JAVA_HOME` still
  points at a different JDK, the plugin fails with `native-image is not installed in your
  JAVA_HOME`. Export both explicitly before building locally, e.g. on macOS:

  ```bash
  export JAVA_HOME=/Library/Java/JavaVirtualMachines/graalvm-community-openjdk-25/Contents/Home
  export GRAALVM_HOME=$JAVA_HOME
  ```

  `graalvm/setup-graalvm` sets both variables automatically in CI.
- **`musl-tools`/`musl-dev`** are only needed to build the fully-static `linux/amd64` binary that
  feeds the `scratch`-based Docker image (`-Dnative.static=true`, see
  [Static and mostly-static Linux builds](#static-and-mostly-static-linux-builds) below). A plain
  dynamic-linked build - which is what you get on macOS, Windows, and any Linux build that doesn't
  pass `-Dnative.static=true` - needs no musl toolchain at all.

## Building locally

```bash
mvn -Pnative -pl native -am -DskipTests package
```

`native/pom.xml` binds the `native-maven-plugin`'s `compile-no-fork` goal to Maven's `package`
phase, so `package` is what actually invokes `native-image` and produces a binary.
`mvn -Pnative -pl native -am -DskipTests compile` only resolves and compiles the module (which has
no Java sources of its own) - the `package`-phase execution is never reached, so `compile` builds
nothing native and produces no binary. Use `compile` only as a fast dependency-resolution sanity
check (e.g. to confirm the module graph resolves before spending several minutes on an actual
build), never to validate that a native image builds successfully.

A successful build writes the executable to:

```
native/target/arcadedb-<version>-<os.detected.name>-<os.detected.arch>
```

using [os-maven-plugin](https://github.com/trustin/os-maven-plugin)'s detected OS/architecture
naming, e.g. `arcadedb-26.8.1-osx-aarch_64` on Apple Silicon or `arcadedb-26.8.1-linux-x86_64` on
Linux/amd64 (Windows appends `.exe`). Native Image cannot cross-compile: you get one binary for the
platform you build on, and building for another OS/architecture requires a runner or machine of
that platform (see [Target matrix](#target-matrix)).

### Static and mostly-static Linux builds

The two required Linux CI targets don't use a plain dynamic-linked build; they build binaries with
no (or almost no) runtime dependency on shared libraries, so they can run in a minimal container
base image with no libc/package manager of its own:

```bash
# linux/amd64: fully static, musl libc - runs on `FROM scratch`
mvn -Pnative -pl native -am -DskipTests -Dnative.static=true package

# linux/arm64: static except glibc itself - runs on a small glibc base (distroless)
mvn -Pnative -pl native -am -DskipTests -Dnative.mostlystatic=true package
```

`-Dnative.static=true` activates `native/pom.xml`'s `native-static` profile, which appends
`--static --libc=musl` to the native-image build args, producing a binary with **zero** dynamic
library dependencies (verifiable with `ldd` reporting "not a dynamic executable"). GraalVM Native
Image only supports `--static` against musl libc, not glibc - glibc's NSS mechanism requires
`dlopen` at runtime, which is incompatible with static linking.

Building `--static --libc=musl` requires a musl toolchain with a **musl-built static zlib** on its
library path (the JDK's zip/net code links `libz`), following GraalVM's own
["Static and Mostly-Static Images"](https://www.graalvm.org/latest/reference-manual/native-image/guides/build-static-executables/)
guide: install `musl-tools`/`musl-dev`, then build zlib from source with
`CC=<triplet>-linux-musl-gcc ./configure --static --includedir=/usr/include/<triplet>-linux-musl
--libdir=/usr/lib/<triplet>-linux-musl`. The include/lib directories matter: musl-gcc's generated
specs file restricts header/library search to that triplet-specific directory and does not fall
back to `/usr/include`/`/usr/lib`, so installing zlib to a plain `--prefix=/usr` leaves it invisible
to the musl toolchain and the native-image `-lz` link step fails. `.github/workflows/native-image.yml`'s
"Set up musl toolchain" step implements this end to end and only runs on the `linux/amd64` leg (see
below for why arm64 skips it).

**`linux/arm64` cannot use the same fully-static musl build.** GraalVM CE's `linux-aarch64` release
tarball ships `lib/static/linux-aarch64/glibc` but not `lib/static/linux-aarch64/musl` (the
`linux-amd64` tarball ships both), so `--static --libc=musl` fails at the link stage with "Missing
libraries: java, nio, net, zip" on arm64 regardless of how the musl toolchain is set up - a known,
long-standing GraalVM CE gap on aarch64 ([oracle/graal#4645](https://github.com/oracle/graal/issues/4645),
closed "not planned"). `-Dnative.mostlystatic=true` uses GraalVM's documented middle ground for this
case instead: `-H:+StaticExecutableWithDynamicLibC` links everything statically except glibc, which
stays dynamically loaded at runtime (matching glibc's own NSS/`dlopen` requirement). No musl
toolchain is needed for this build mode.

Neither of these flags affects Netty's DNS resolution: `io.netty.resolver.dns` is not on the
classpath of any module wired into `native/pom.xml` (the wire-protocol modules that do use Netty
only build server-side `Bootstrap`s to accept inbound connections, and `grpc-client`, the one module
that dials out, is not part of the native build). No `-Dio.netty.resolver.dns.*` runtime flag is
needed or recommended for this image.

## Feature set

Included and working in the native binary:

- SQL and OpenCypher query engines
- HTTP API and Studio
- Wire protocols: **PostgreSQL, Redis, MongoDB, Neo4j Bolt, gRPC**
- **GraalJS scripting** - embedded in the image and functional (see the
  [JS scripting caveat](#js-scripting-is-included-but-with-a-security-caveat) below for the one
  thing to be aware of)
- The `console` and `graphql` modules

Deliberately excluded from the native build (`native/pom.xml`'s dependency list omits them):

- **Gremlin** (Apache TinkerPop) - not on the native module's classpath
- **`arcadedb-tracing`** (OpenTelemetry) - not on the native module's classpath

If your deployment needs Gremlin or OpenTelemetry tracing, use the JVM distribution instead.

## Target matrix

`.github/workflows/native-image.yml` builds five targets. Native Image cannot cross-compile, so
each target is built on a runner of that same OS/architecture:

| Target | Runner | Status |
|---|---|---|
| linux/amd64 | `ubuntu-latest` | **Required** - fully static (musl) |
| linux/arm64 | `ubuntu-24.04-arm` | **Required** - mostly static (glibc) |
| macos/arm64 | `macos-15` | Best-effort |
| macos/amd64 | `macos-15-intel` | Best-effort |
| windows/amd64 | `windows-latest` | Best-effort |

The two Linux targets are `required: true` in the CI matrix and gate the workflow; the other three
are `required: false` (`continue-on-error`), so a failure there does not redden the run. There is no
`windows/arm64` leg: GitHub does not offer a free Windows-on-ARM hosted runner.

**`macos/amd64` is a likely dead leg.** GitHub fully retired the free `macos-13` (Intel) hosted
runner; the closest still-available x64 macOS label is `macos-15-intel`, but that label is part of
GitHub's paid "Larger Runners" tier and only schedules jobs if the repository/org has that
entitlement enabled. This repository has already hit the same wall elsewhere and disabled the
equivalent legs in `test-python-*.yml`. Until that entitlement exists (or GitHub reintroduces a free
x64 macOS runner), the `macos/amd64` native-image leg is expected to simply not run rather than fail
loudly.

## Docker images

Only the two required Linux targets are packaged into Docker images, because only those two produce
binaries with no runtime dependency on the host's shared libraries - a prerequisite for a minimal,
mostly-empty container base image. The two targets use **different** container bases, because they
reach "no shared libraries needed" by different build modes (see
[Static and mostly-static Linux builds](#static-and-mostly-static-linux-builds) above):

| Arch | Build mode | Dockerfile | Base image |
|---|---|---|---|
| linux/amd64 | fully static (musl) | `native/src/main/docker/Dockerfile.native.scratch` | `FROM scratch` |
| linux/arm64 | mostly static (glibc) | `native/src/main/docker/Dockerfile.native.distroless` | `FROM gcr.io/distroless/base-debian12:nonroot` |

Both Dockerfiles `COPY` in the binary, `config/` (staged from `package/src/main/config`), and expose
the same port/volume surface as the JVM image's Dockerfile, scoped to the modules the native build
actually bundles - notably **no port 8182** (Gremlin is excluded): `2480` (HTTP/Studio), `2424`
(binary protocol/replication), `5432` (Postgres), `6379` (Redis), `27017` (MongoDB), `7687` (Bolt),
and `50051` (gRPC). Volumes: `config`, `databases`, `backups`, `replication`, `log`.

A few things differ from the JVM Docker image because neither base has a shell:

- **No `JAVA_OPTS`-reading wrapper script.** The JVM image's `bin/server.sh` reads `$JAVA_OPTS`;
  these images have no shell to run such a script, so both `ENTRYPOINT`s are exec-form arrays that
  invoke the binary directly. Extra `-D` flags must be passed as trailing arguments to
  `docker run` instead of via `-e JAVA_OPTS=...` - see [Running the container](#running-the-container)
  below.
- **User model differs per base.** `scratch` has no `/etc/passwd` and no `adduser`, so that image
  runs as UID 0 unless the caller overrides it with `docker run --user <uid>:<gid>`. The distroless
  `:nonroot` tag bakes in a `nonroot:nonroot` (65532:65532) user, so the arm64 image already runs
  non-root by default with no override needed.
- **CA trust bundle.** `scratch` has no trust store of any kind, which breaks outbound HTTPS (import,
  replication, MCP-client code paths) unless one is supplied; the scratch Dockerfile `COPY`s in a
  `ca-certificates.crt` bundle for that. The distroless base already bundles glibc, libssl, and a CA
  bundle, so no separate `COPY` is needed there. Either way, this is a partial fix: GraalVM Native
  Image bakes a snapshot of the *build machine's* JDK cacerts trust store into the image at build
  time by default, and copying in an OS-level CA bundle does not change the JVM's baked-in default
  trust anchors - it only supplies a trust store for code paths that read the OS bundle explicitly.

### Tags

Images are published as `arcadedata/arcadedb:<version>-native-amd64` and
`arcadedata/arcadedb:<version>-native-arm64` (per-arch), stitched into a combined
`arcadedata/arcadedb:<version>-native` multi-arch manifest via `docker buildx imagetools create`.
`<version>` is the plain Maven `project.version` with no `v` prefix (e.g. `26.8.1`), matching this
repository's release-tag convention.

Publishing only happens on a **published GitHub release**: `native-image.yml` has no `push:`
trigger, only `workflow_dispatch` and `release: [published]`, so an ordinary branch push never
reaches the Docker jobs. A manual `workflow_dispatch` run still builds and smoke-tests both images
(`--load` into the runner's local Docker daemon) without logging into Docker Hub or pushing anything,
so the Dockerfiles can be validated end to end from any branch without publishing under
`arcadedata/arcadedb`.

## Running the container

```bash
docker run --rm -p 2480:2480 -p 5432:5432 \
  --user 1000:1000 \
  -v "$(pwd)/databases:/home/arcadedb/databases" \
  arcadedata/arcadedb:<version>-native \
  -Darcadedb.server.rootPassword=PlayWithData123!
```

- `--user 1000:1000` is only meaningful (and only necessary) for the `linux/amd64` (`scratch`)
  image; the `linux/arm64` (distroless) image already runs as its baked-in non-root UID by default.
  Whatever UID you choose should own the host-side `databases` bind mount (or `chown` the directory
  to match).
- Map whichever wire-protocol ports you actually enabled in addition to `2480` - `5432` (Postgres),
  `6379` (Redis), `27017` (MongoDB), `7687` (Bolt), `50051` (gRPC).
- Both `ENTRYPOINT`s already bake in two runtime defaults ahead of any arguments you pass:
  `-Dorg.jline.terminal.dumb=true` (the server runs non-interactively in a container, so it avoids
  JLine's JNI-backed interactive terminal provider) and
  `-Djava.util.logging.config.file=/home/arcadedb/config/arcadedb-log.properties` (JUL logging
  configuration, matching the JVM image's setup).
- **Setting the root password:** because neither base image has a shell, the JVM image's
  `-e JAVA_OPTS="-Darcadedb.server.rootPassword=..."` pattern does not work here - there is no shell
  to expand `$JAVA_OPTS` into the command line. Instead, pass `-Darcadedb.server.rootPassword=...`
  (and any other `-D` flags) as **trailing arguments** to `docker run`, as in the example above;
  Docker appends them after the `ENTRYPOINT` array, and the binary parses them exactly like JVM
  system properties.

## Known limitations and caveats

### JS scripting is included, but with a security caveat

GraalJS is embedded in the native binary and works correctly - there is no fallback error and no
missing language. It runs through the SVM-integrated Truffle runtime built into the image, not the
plain HotSpot Truffle runtime the JVM build uses.

Getting there required relaxing a Truffle safety check. By default, `native-image` refuses to embed
Truffle languages whose builtins reach call targets Truffle's partial-evaluation blocklist considers
unsafe to compile ahead of time. Six GraalJS builtins - `Atomics.*` and `TypedArray.prototype.set` -
reach `java.lang.invoke.MethodHandle.linkToStatic`, which trips that blocklist and aborts the build.
`native/pom.xml` passes `-H:-TruffleCheckBlockListMethods -H:-TruffleCheckBlackListedMethods` to let
the build proceed with JS embedded, at the cost of those specific builtins being unverified under
Truffle's own soundness check once their call targets get hot enough to runtime-compile.

This is a real, reachable residual risk, not a theoretical one. `GraalPolyglotEngine`'s sandboxing
(deny `Class`/`ClassLoader`/reflection, `IOAccess.NONE`, `allowNativeAccess(false)`,
`allowCreateProcess(false)`, `allowEnvironmentAccess(NONE)`, `allowCreateThread(false)`,
`allowPolyglotAccess(NONE)`) restricts Java/host interop, not the JS language surface - `Atomics`,
`SharedArrayBuffer`, and `TypedArray.prototype.set` are standard ECMAScript globals, so none of that
sandboxing touches them. Testing directly against the native binary confirmed all of them are
reachable through an ordinary `{"language":"js", ...}` command:

| Script | Result |
|---|---|
| `typeof Atomics` | `"object"` |
| `var a = new Int32Array(4); Atomics.add(a, 0, 5); a[0]` | `5` |
| `var b = new Uint8Array(4); b.set([1,2,3],0); b[1]` | `2` |
| `typeof SharedArrayBuffer` | `"function"` |
| `var sab = new SharedArrayBuffer(16); var ia = new Int32Array(sab); Atomics.store(ia, 0, 99); Atomics.load(ia, 0)` | `99` |
| `Atomics.wait(...)` | `TypeError: Unsupported operation` (blocked cleanly by `allowCreateThread(false)`; the server stayed up) |

So: any authenticated caller with query/command permissions can reach `Atomics.add`/`store`/`load`,
`SharedArrayBuffer` construction, and `TypedArray.prototype.set` through a plain
`{"language":"js"}` command or a polyglot SQL function. `Atomics.wait` is the one operation that is
not reachable in practice, because it requires real thread blocking that `allowCreateThread(false)`
denies. Functional testing here did not turn up a crash or unsound result from the other builtins,
but a functional smoke test cannot validate Truffle's own soundness concern under sustained or
adversarial load once those call targets get hot enough for runtime compilation. This is documented
here as an accepted, non-blocking risk for this experimental add-on, not something the JS regression
gate in `native/src/test/scripts/exercise.sh` can detect.

### Binary size

The native binary is currently large - around 732 MiB - mostly because `native/pom.xml` passes
`-H:IncludeResources=.*` to the build, which embeds every classpath resource (Studio's web assets,
config templates, Lucene codec files, etc.) into the image. This is deliberately broad for
correctness first; tightening `-H:IncludeResources` to the specific resource patterns each embedded
module actually needs at runtime is a known follow-up, not something this experimental add-on has
done yet.

### JVector and SIMD

The build passes `--add-modules=jdk.incubator.vector` so the JDK's Vector API module is available in
the image, and JVector (ArcadeDB's vector-index library) can load and run. Whether `native-image`
generates the same SIMD-intrinsic code paths the JIT produces on the JVM, or falls back to scalar
execution for some of those operations, has not been rigorously benchmarked here. Treat this as a
possible performance gap under the native image, not a correctness concern - vector search results
should be identical either way.

### CA trust nuance

See [CA trust bundle](#docker-images) above: GraalVM Native Image bakes a snapshot of the build
machine's JDK cacerts trust store into the image at build time, and neither Dockerfile's CA bundle
changes that baked-in default - it only supplies an OS-level trust store for code paths that read it
explicitly.

### Postgres wire protocol: verified in CI, not on every dev machine

The Postgres-wire smoke check in `native/src/test/scripts/exercise.sh` needs a `psql` client to run
as a hard assertion. CI installs `postgresql-client` on the `linux/amd64` leg specifically for this,
so a real Postgres-wire round trip against the native binary is verified there. It has not been
verified against a macOS development machine that lacks `psql` - on such a machine the check
WARN-skips instead of failing, so a missing local `psql` will not block a local build.

## Building the reachability metadata

The build's `native-maven-plugin` configuration enables GraalVM's
[reachability metadata repository](https://github.com/oracle/graalvm-reachability-metadata) (Netty,
gRPC, Undertow, Jackson, HdrHistogram, Apache HttpClient) alongside a hand-generated
`reachability-metadata.json` produced by running the assembled JVM server under the native-image
tracing agent (`native/src/test/scripts/trace.sh`) while exercising the smoke-test surface. This
combination has been sufficient so far - no manual edits to the generated JSON have been required
when adding new smoke-tested surface. Regenerate it with `trace.sh` if you add a code path that
reflects, serializes, or loads resources in a way the current metadata doesn't cover; the script
needs GraalVM's own `java` (point `JAVA_HOME` at it, same requirement as above) because it launches
the server with `-agentlib:native-image-agent`.
