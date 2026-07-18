# Native image build

Stub created by Task 5 of the native-image spike (see `docs/superpowers/plans/2026-07-18-native-image.md`).
Task 9 expands this into the full build/run/size guide (prerequisites, target matrix, JVector SIMD
fallback note, Docker usage). This page currently covers only what Task 5 needed to lock in: the
polyglot-JS outcome and the Truffle blocklist relaxation it depends on.

## `package` builds the image, `compile` does not

`native/pom.xml` binds the `native-maven-plugin`'s `compile-no-fork` goal to the Maven `package`
phase. `mvn -Pnative -pl native -am -DskipTests package` therefore runs the actual `native-image`
build. `mvn -Pnative -pl native -am -DskipTests compile` only resolves and compiles the module's
(nonexistent) sources - the `package`-phase execution is never reached, so no `native-image`
process runs and no binary is produced. Use `compile` only as a fast dependency-resolution sanity
check, never to validate a native build.

## JS scripting: included and working

GraalJS ships embedded in the native binary and runs correctly - no fallback error, no missing
language. Verified against `native/target/arcadedb-26.8.1-SNAPSHOT-osx-aarch_64`:

```
$ curl -u root:PlayWithData123! -H 'Content-Type: application/json' \
    -X POST http://127.0.0.1:2480/api/v1/command/jstest \
    -d '{"language":"js","command":"40 + 2"}'
{"user":"root","result":[{"value":42}]}
```

This is now a permanent regression gate: `native/src/test/scripts/exercise.sh` sends this exact
request as a **hard** assertion (fails the script on a wrong/absent result), in the same style as
the SQL and Cypher round-trip checks - not a best-effort WARN-skip like the optional wire-protocol
checks further down the script. `smoke.sh` runs it against both the native binary and, since
GraalJS is also on the JVM build's classpath, the JVM `server.sh` distribution.

Known JVM-side quirk (unrelated to the native image itself, but relevant to keeping the shared
`exercise.sh` green on both paths): running the plain classpath-assembled Truffle/GraalJS jars
under `graalvm-community-openjdk-25`'s bundled JDK can throw `NullPointerException` from
`HotSpotTruffleRuntimeAccess.getCompilerVersion` while creating the shared polyglot `Engine`
(`Engine.create()`), because that JDK's built-in Graal compiler modules and the classpath
`truffle-compiler`/`truffle-runtime` jars don't resolve a compiler version consistently outside a
native image. `GraalPolyglotEngine.getSupportedLanguages()` swallows this as a caught `Throwable`
and logs "GraalVM Polyglot Engine: no languages found"
(`engine/src/main/java/com/arcadedb/query/polyglot/GraalPolyglotEngine.java:140-147`), so `js`
silently drops out of `Available query languages` and any `{"language":"js"}` command then fails
with `Query engine 'js' was not found`. The workaround is `-Dtruffle.UseFallbackRuntime=true`
on the JVM launch, which switches Truffle to its interpreter-only fallback runtime and avoids the
buggy compiler-version probe entirely (verified: `smoke.sh <server.sh> -Dtruffle.UseFallbackRuntime=true`
passes, including the JS assertion). This does not apply to the native image, where Truffle runs
through the SVM-integrated optimizing runtime built by the native-image Truffle feature, not
`HotSpotTruffleRuntimeAccess`.

## Truffle blocklist relaxation

`native/pom.xml` passes `-H:-TruffleCheckBlockListMethods -H:-TruffleCheckBlackListedMethods` to
the native-image build. Without these, the build aborts: six GraalJS builtins (`Atomics.*` and
`TypedArray.prototype.set`) reach `java.lang.invoke.MethodHandle.linkToStatic`, which Truffle's
partial-evaluation blocklist rejects as unsafe to runtime-compile ahead-of-time. The blocklist
exists because Truffle cannot statically prove those call targets deoptimize/compile soundly when
AOT-compiled into the image; relaxing it lets the build proceed with JS embedded, at the cost of
those specific builtins being unverified under Truffle's own soundness check when their call
targets get hot enough to runtime-compile.

### Are the flagged builtins actually reachable through ArcadeDB's polyglot surface?

Yes. `GraalPolyglotEngine`'s `Context.Builder` (`engine/src/main/java/com/arcadedb/query/polyglot/GraalPolyglotEngine.java:83-98`)
sandboxes host interop (denies `Class`/`ClassLoader`/reflection access, `IOAccess.NONE`,
`allowNativeAccess(false)`, `allowCreateProcess(false)`, `allowEnvironmentAccess(NONE)`,
`allowCreateThread(false)`, `allowPolyglotAccess(NONE)`, and a package allowlist for
`Java.type(...)` lookups) but sets no context/engine option that disables or restricts the
ECMAScript builtins themselves. `Atomics`, `SharedArrayBuffer`, and
`TypedArray.prototype.set` are standard JS globals, not host-interop surface, so none of that
sandboxing touches them.

Confirmed directly against the running native binary (`arcadedb-26.8.1-SNAPSHOT-osx-aarch_64`),
via the same `{"language":"js", ...}` command path used above:

| Script | Result |
|---|---|
| `typeof Atomics` | `"object"` |
| `var a = new Int32Array(4); Atomics.add(a, 0, 5); a[0]` | `5` |
| `var b = new Uint8Array(4); b.set([1,2,3],0); b[1]` | `2` |
| `typeof SharedArrayBuffer` | `"function"` |
| `var sab = new SharedArrayBuffer(16); var ia = new Int32Array(sab); Atomics.store(ia, 0, 99); Atomics.load(ia, 0)` | `99` |
| `var sab2 = new SharedArrayBuffer(16); var ia2 = new Int32Array(sab2); Atomics.wait(ia2, 0, 1, 100)` | `TypeError: Unsupported operation` (rejected cleanly - `allowCreateThread(false)` blocks the blocking-wait path; the server stayed up) |

**Conclusion: the residual risk from relaxing the Truffle blocklist is real, not moot.** Any
authenticated caller with query/command permissions can reach `Atomics.add`/`store`/`load`,
`SharedArrayBuffer` construction, and `TypedArray.prototype.set` through a plain `{"language":"js"}`
command or a polyglot SQL function - the sandboxing in `GraalPolyglotEngine` restricts Java/host
interop, not the JS language surface. `Atomics.wait` is the one operation that is not reachable in
practice, because it requires real thread blocking and `allowCreateThread(false)` denies that; it
fails with a clean `TypeError` rather than crashing the process. Functional testing here did not
turn up a crash or unsound result from the other five builtins - the process survived
`/api/v1/ready` checks after every call - but a single functional smoke test does not validate
Truffle's own soundness concern (unverified behavior once these call targets get hot enough for
runtime compilation under sustained/adversarial load). This should stay documented as an accepted,
non-blocking risk for this experimental native-image add-on rather than something the JS regression
gate can detect.

## Static musl build for Linux (`scratch`-compatible binaries)

Task 7 of the native-image spike. The two required Linux jobs in `.github/workflows/native-image.yml`
(`linux/amd64`, `linux/arm64`) build a **fully statically-linked** executable - no dependency on the
runner's (or the target container's) shared libraries at all, verifiable with `ldd` reporting "not a
dynamic executable" - so the binary can run directly in a `scratch` (empty) container base image in
Task 8.

### Triggering it

```bash
mvn -B -ntp -Pnative -pl native -am -DskipTests -Dnative.static=true package
```

`-Dnative.static=true` activates `native/pom.xml`'s `native-static` profile, which appends
`--static --libc=musl` to the `native-maven-plugin`'s `buildArgs` (using
`<buildArgs combine.children="append">` so the two flags extend the base build's flags - see the
comment on that profile in `native/pom.xml` for why `combine.children` is required: Maven's default
profile/base plugin-configuration merge for a repeated element like `<buildArg>` is positional, not a
union, so an unmarked `<buildArgs>` in the profile would silently overwrite two of the base build's
flags instead of adding two new ones). Without the property, the profile still activates (visible via
`mvn -Dnative.static=true help:active-profiles -pl native`) but contributes no build args, matching
the default dynamic-linked build used on macOS/Windows.

GraalVM Native Image only supports `--static` against musl libc, not glibc (glibc's NSS mechanism
requires `dlopen` at runtime, which is incompatible with static linking); that's why `--libc=musl` is
mandatory alongside `--static`.

### musl + zlib toolchain requirement

A bare `musl-gcc` is not enough. `--static --libc=musl` also links `libz` (used by the JDK's
zip/net code), so a **musl-built static zlib** must be on the musl toolchain's library path before
the native-image build runs. `.github/workflows/native-image.yml`'s "Set up musl toolchain (static
Linux builds)" step implements the standard GraalVM recipe
(https://www.graalvm.org/latest/reference-manual/native-image/guides/build-static-executables/):

1. `apt-get install musl-tools musl-dev` - on Ubuntu 24.04 ("noble"), `musl-dev` installs the
   arch-prefixed compiler wrapper native-image's toolchain probe looks for by name
   (`x86_64-linux-musl-gcc` on amd64, `aarch64-linux-musl-gcc` on arm64 - see
   [oracle/graal#7103](https://github.com/oracle/graal/issues/7103), where native-image failed to
   find a plain `musl-gcc`), plus musl's own static `libc.a` and headers under
   `/usr/{include,lib}/<triplet>-linux-musl/`. The workflow derives `<triplet>` from `uname -m` so
   the same script works unchanged on both the amd64 and arm64 runners.
2. Download `zlib` source (pinned release tarball from zlib's GitHub releases, not the rotating
   `zlib.net` "latest" link) and build it with `CC=<triplet>-linux-musl-gcc ./configure --static
   --includedir=/usr/include/<triplet>-linux-musl --libdir=/usr/lib/<triplet>-linux-musl`, then
   `make && sudo make install`. The `--includedir`/`--libdir` placement matters: musl-gcc's
   generated specs file (`/usr/lib/<triplet>-linux-musl/musl-gcc.specs`) passes `-nostdinc
   -isystem <musl-triplet-dir>` and `-nostdlib -L<musl-triplet-dir>` (see musl's own
   `tools/musl-gcc.specs.sh`), so it does **not** fall back to the generic `/usr/include`/`/usr/lib`
   search path the way a normal system gcc would. Installing zlib to a plain `--prefix=/usr` (rather
   than the triplet-specific include/lib dirs) would silently leave `libz.a` invisible to the musl
   toolchain, and native-image's `-lz` link step would fail - `native/pom.xml` intentionally adds
   only `--static --libc=musl` to `buildArgs`, with no `-H:CLibraryPath`/`-H:CIncludePath` escape
   hatch, so the toolchain has to expose zlib on its own default search path.

### Known gap: `linux/arm64` static build (verified, not just suspected)

Confirmed directly against the `graalvm-community-jdk-25.0.2` release tarballs this workflow's
`graalvm/setup-graalvm` step installs (by listing archive contents, not by running a build): the
`linux-aarch64` distribution ships `lib/static/linux-aarch64/glibc` but **not**
`lib/static/linux-aarch64/musl`. `linux-amd64` ships both. `--static --libc=musl` needs the musl
variant of those static JDK libraries (`libjava.a`, `libnio.a`, `libnet.a`, `libzip.a`) to link the
image; without them the build fails at the native-image link stage with "Missing libraries: java,
nio, net, zip" - regardless of how correctly the musl/zlib toolchain above is set up. This is a
known, long-standing GraalVM CE gap on aarch64
([oracle/graal#4645](https://github.com/oracle/graal/issues/4645), closed "not planned" circa 2022)
that still reproduces against the JDK 25 release used here.

Practically: the `linux/amd64` static leg of `native-image.yml` is expected to build and pass
`ldd`'s "not a dynamic executable" check. The `linux/arm64` static leg is expected to **fail at the
native-image link step** until Oracle ships musl static JDK libraries for aarch64, or ArcadeDB
builds/vendors its own (a nontrivial undertaking - it means cross-compiling a musl-libc OpenJDK for
aarch64). This is called out here rather than silently worked around, since the workflow's toolchain
setup is correct and forward-compatible for when the upstream gap closes; there is no toolchain fix
on ArcadeDB's side that unblocks the arm64 leg today.

### Netty DNS / `getaddrinfo` under static musl: not applicable to ArcadeDB

The premise motivating this section (Netty's async DNS resolver, `io.netty.resolver.dns.*`, using
`getaddrinfo` in a way that's unreliable under a fully static musl binary) does not apply to
ArcadeDB's native image: `io.netty.resolver` is not on the classpath of any module wired into
`native/pom.xml`. Specifically:

- `mongodbw` (in the native build) only imports the `io.netty.channel.Channel` *type* and never
  constructs a `Bootstrap`/`ServerBootstrap` - no networking, hence no DNS, happens through Netty
  there.
- `grpcw`'s `GrpcServerPlugin` (in the native build) only builds a `NettyServerBuilder` to *accept*
  inbound connections - there is no outbound name resolution to perform.
- `grpc-client` (which does dial out, via `NettyChannelBuilder`) is only used by the `e2e`/`e2e-ha`/
  `load-tests` modules - it is not a dependency of `native/pom.xml` and is not compiled into the
  native binary at all. Even if it were, its name resolution goes through gRPC's own
  `io.grpc.internal.DnsNameResolver`, which is backed by plain `java.net.InetAddress`, not Netty's
  DNS resolver.
- A repo-wide `grep -r "io.netty.resolver"` across every module (engine, server, bolt, grpcw,
  redisw, mongodbw, postgresw, grpc-client, ha-raft) returns zero matches, and the `grpc-netty-shaded`
  jar ArcadeDB actually depends on (`grpc.version` in the root `pom.xml`) does not bundle any
  `io/netty/resolver/dns/*` classes at all.
- There is also no genuine, documented core-Netty system property that globally forces a
  "JDK resolver" (checked against the compiled classes of `netty-resolver-dns` directly: no
  `preferJdkResolver`-style string exists in that package). Netty's DNS-vs-JDK resolver choice is a
  code-level decision (which `AddressResolverGroup` a `Bootstrap` is built with), not a JVM flag - so
  there is nothing to "force" via a system property even in a hypothetical future module that does
  add `netty-resolver-dns` to the graph.

The actual native/musl risk area in this codebase is different: Netty's *native transport* JNI
libraries (`netty-transport-native-epoll`, `netty-tcnative`, bundled inside `grpc-netty-shaded`),
which `dlopen` a `.so` at runtime and fall back to plain NIO if that fails. `native/pom.xml` already
carries a defensive `--initialize-at-run-time=io.netty` build arg for exactly this kind of
JNI/native-state class. This should be re-verified functionally once the static Linux binary exists
(start the gRPC server, confirm it doesn't throw or hang), but it is a native-library-loading
concern, not a DNS-resolution one, and needs no runtime system property.

**Net effect for Task 8 (Docker `ENTRYPOINT`):** no `-Dio.netty.resolver.dns.*` flag is needed.
Nothing in this document should be read as recommending `-Dio.netty.resolver.dns.preferJdkResolver=
true` (or similar) as a real fix, since it targets a resolver ArcadeDB's native image never loads;
including it would be a harmless no-op at best and misleading documentation at worst. If a future
dependency change adds `netty-resolver-dns` to a module compiled into the native image, revisit this
section rather than assuming the belt-and-suspenders flag above covers it.

## Docker images: split build mode per Linux architecture

Task 8 of the native-image spike. `native-image.yml`'s `docker` job packages each required Linux
binary into a minimal container image and pushes per-arch tags; the `manifest` job stitches them
into one multi-arch tag. Both Linux targets are "static-ish" for exactly the reason explained
above (`scratch`/distroless bases have no shared libraries to satisfy a dynamically-linked
binary), but they get there by two **different** build modes, because GraalVM CE cannot build a
fully-static musl binary for `linux/arm64`:

| Arch | `-D` property | `native/pom.xml` profile | Build args added | Result | Docker base |
|---|---|---|---|---|---|
| linux/amd64 | `-Dnative.static=true` | `native-static` | `--static --libc=musl` | fully static, zero dynamic dependencies | `native/src/main/docker/Dockerfile.native.scratch` - `FROM scratch` |
| linux/arm64 | `-Dnative.mostlystatic=true` | `native-mostly-static` | `-H:+StaticExecutableWithDynamicLibC` | static except glibc itself (dynamically loaded at runtime) | `native/src/main/docker/Dockerfile.native.distroless` - `FROM gcr.io/distroless/base-debian12:nonroot` |

`linux/arm64` cannot use `native-static`'s `--static --libc=musl` combination: as documented
above, the GraalVM CE JDK 25 release ships no `lib/static/linux-aarch64/musl` (only `glibc`),
so that link fails with "Missing libraries: java, nio, net, zip" regardless of toolchain setup
(oracle/graal#4645, closed not-planned). `-H:+StaticExecutableWithDynamicLibC` is GraalVM's own
documented "mostly static" fallback for glibc targets that can't go fully static: everything
statically linked except glibc, which stays dynamically `dlopen`'d at runtime (this is also *why*
it can't go further - glibc's NSS mechanism needs `dlopen`, which is incompatible with static
linking). The two properties are activated independently and are mutually exclusive by CI
convention (each Linux leg of the matrix sets exactly one); neither defaults to `true`, so
neither affects the plain dynamic-linked builds used on macOS/Windows. Verify locally:

```bash
# Neither profile active by default
mvn -Pnative help:active-profiles -pl native | grep native-static      # no output
mvn -Pnative help:active-profiles -pl native | grep native-mostly-static  # no output

# Each property activates only its own profile
mvn -Pnative -Dnative.static=true       help:active-profiles -pl native | grep native-static
mvn -Pnative -Dnative.mostlystatic=true help:active-profiles -pl native | grep native-mostly-static
```

Because the CI matrix now distinguishes build mode per Linux leg (`linkmode: musl-static` for
amd64, `linkmode: mostly-static` for arm64), the "Set up musl toolchain" step in
`native-image.yml` only runs on the amd64 leg - it would be pure wasted CI time on arm64, since
that leg never attempts `--libc=musl` in the first place. One side effect: `exercise.sh`'s
Postgres-wire smoke check WARN-skips (rather than hard-failing) on the arm64 leg, because that
step is also what installs `postgresql-client`; this is an accepted, documented trade-off, not an
oversight.

### The two Dockerfiles

`native/src/main/docker/Dockerfile.native.scratch` (linux/amd64 only):
- `FROM scratch` - no libc, no shell, no package manager.
- `COPY`s in the static-musl binary, `config/` (staged from `package/src/main/config`), and a
  `ca-certificates.crt` bundle (scratch has no trust store at all - needed for HTTPS
  import/replication/MCP-client code paths). Note: GraalVM Native Image bakes a snapshot of the
  *build machine's* JDK cacerts trust store into the image at build time by default; this PEM
  bundle provides an OS-level trust store for anything that reads it explicitly, but does not
  itself change the JVM's default trust anchors. Documented as a known caveat, not resolved here.
- No `adduser`/`chown` is possible (no shell), so the image runs as UID 0 unless the caller
  overrides it with `docker run --user <uid>:<gid>` - see the run example below.

`native/src/main/docker/Dockerfile.native.distroless` (linux/arm64 only):
- `FROM gcr.io/distroless/base-debian12:nonroot` - glibc, libssl and a CA bundle already baked in
  (verified against distroless's published image contents: the `base` variant, unlike
  `static`, includes glibc + `ca-certificates`; no separate cert `COPY` needed here, unlike the
  scratch image above).
- The `:nonroot` tag bakes in a `nonroot:nonroot` (65532:65532) user/group, so this image runs
  non-root by default with no `--user` override required - unlike scratch, which has no
  `/etc/passwd` at all.

Both images preserve the same `EXPOSE`/`VOLUME` surface as `package/src/main/docker/Dockerfile`,
scoped to the protocol modules the native build actually bundles (`native/pom.xml`'s
dependencies: server, console, studio, graphql, postgresw, redisw, mongodbw, bolt, grpcw - no
Gremlin, hence no port 8182): `2480` (HTTP/Studio), `2424` (binary/replication), `5432`
(Postgres), `6379` (Redis), `27017` (MongoDB), `7687` (Bolt - `GlobalConfiguration.BOLT_PORT`'s
default, and easy to miss since it wasn't in the original task brief's port list), and `50051`
(gRPC). Volumes: `config`, `databases`, `backups`, `replication`, `log`, matching the JVM image.

Neither image has a shell, so neither can use a `JAVA_OPTS`-reading wrapper script the way
`package/src/main/docker/Dockerfile`'s `bin/server.sh` does (the README's documented
`docker run ... -e JAVA_OPTS="-Darcadedb.server.rootPassword=..."` pattern for the JVM image does
not carry over). Both `ENTRYPOINT`s are exec-form arrays invoking the static binary directly (the
binary is itself the "java" replacement); extra `-D` flags are passed as trailing `docker run`
arguments, which Docker appends after the `ENTRYPOINT` array:

```bash
docker run --rm -p 2480:2480 \
  -v "$(pwd)/databases:/home/arcadedb/databases" \
  --user 1000:1000 \
  arcadedata/arcadedb:<version>-native \
  -Darcadedb.server.rootPassword=PlayWithData123!
```

`--user 1000:1000` is only meaningful (and only necessary) for the `scratch`/amd64 image; the
`distroless`/arm64 image already runs as its baked-in non-root UID by default. Substitute a UID
that owns the host-side `databases` bind mount, or `chown` that directory to match. `<version>` is
the plain Maven `project.version` (e.g. `26.8.1`, matching this repo's release-tag convention - no
`v` prefix), and `-native` (no arch suffix) is the multi-arch manifest tag; `-native-amd64` /
`-native-arm64` are the per-arch tags if a specific architecture is needed directly.

### CI jobs: `docker` and `manifest`

`native-image.yml`'s `docker` job runs once per required Linux arch (`needs: build`, on that
arch's own native runner - no QEMU cross-emulation is involved anywhere in this workflow), and:
1. Downloads that arch's binary artifact and stages a build context (`ctx/`): the binary
   (`chmod`ped executable via `COPY --chmod=0755`), `config/` copied from
   `package/src/main/config`, and (amd64 only) `ca-certificates.crt`.
2. `docker buildx build`s with the arch-appropriate Dockerfile, tagging
   `arcadedata/arcadedb:<version>-native-<arch>`.
3. Smoke-tests the built image: runs it, polls `GET /api/v1/ready` for up to 60s, and fails the
   job if it never comes up.

The `manifest` job (`needs: docker`) stitches the two per-arch tags into
`arcadedata/arcadedb:<version>-native` via `docker buildx imagetools create`.

**Push gating (safe default, chosen and documented here):** `native-image.yml` has no `push:`
trigger at all - only `workflow_dispatch` and `release` (`types: [published]`) - so an ordinary
branch push can never reach the `docker`/`manifest` jobs in the first place. On top of that,
within the `docker` job itself, only a published release (`github.event_name == 'release'`) logs
into Docker Hub and passes `--push` to `buildx build`; a manual `workflow_dispatch` run instead
builds with `--load` (loads into the runner's local Docker daemon, never touches the registry)
and still runs the full smoke test against that locally-loaded image. This lets a maintainer
manually trigger the workflow from any branch to validate the Dockerfiles/build context/smoke
test end-to-end without publishing anything under `arcadedata/arcadedb`. The `manifest` job only
runs `if: github.event_name == 'release'`, since a `workflow_dispatch` run never pushes per-arch
tags for it to stitch.

### What is CI-only

This entire section describes CI-verified behavior. Author's environment for Task 8 was an arm64
macOS machine, which cannot build a Linux native binary (musl or mostly-static-glibc) or run a
Linux `docker buildx build` targeting either Dockerfile without the Linux binary the CI matrix
produces - so none of the following were built or run locally, and are verification gaps closed
only by the CI matrix + `docker`/`manifest` jobs: the `linux/amd64` static-musl and `linux/arm64`
mostly-static-glibc native-image builds themselves; either Dockerfile actually building an image;
the smoke test against a running container; and the multi-arch `imagetools create` manifest.
What *was* verified locally: `native/pom.xml`'s profile activation/mutual-exclusivity (the
`help:active-profiles` commands above), the workflow YAML's validity, `actionlint` (shellcheck
embedded) cleanliness, and `hadolint` cleanliness on both Dockerfiles.
