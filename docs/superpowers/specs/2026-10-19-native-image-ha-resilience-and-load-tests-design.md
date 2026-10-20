# Run the HA resilience and load tests against the native image too

Date: 2026-10-19

## Intent

The GraalVM native image is published every night (`arcadedata/arcadedb:latest-native`) and passes the e2e battery
(`e2e` module, see the `docker` job in `.github/workflows/native-image.yml`), but nobody runs HA failover,
partition, rolling restart or sustained load against it. The native image must behave like the JVM image, so the
same `e2e-ha` and `load-tests` suites should run against both, and a native failure is a failure, reported exactly
like a JVM one.

The `e2e` module already did this: its container template takes the image as its only parameter
(`e2e/src/test/java/com/arcadedb/e2e/ArcadeContainerTemplate.java`). This work applies the same idea to the shared
template used by `load-tests` and `e2e-ha`.

## Goals

- Every IT that runs today in `ha-resilience-tests.yml` (module `e2e-ha`) and `load-tests.yml` (module `load-tests`)
  runs twice per nightly: once against the JVM image built in the job, once against the pulled native image.
- One code path in the tests for both images; the image (and whether it is native) is the only difference.
- Native failures surface like JVM failures: the same test reporter, a failed check, reports and server logs uploaded.
- The JVM leg keeps behaving as it does today (same tests, same settings, same results).

## Non-goals

- No JUnit-level parametrization (no `@ParameterizedTest` over images). The double run is a workflow matrix.
- No change to `HaChaosIT` / `ha-chaos-tests.yml` beyond what the shared template change implies (it must keep
  working on the JVM image; running chaos on native is a later, separate step).
- No change to schedules. Moving the jobs after the 01:41 UTC native nightly so they test the freshest build is a
  follow-up the user will do later. For now the native leg pulls whatever `latest-native` is at run time (typically
  the previous night's build).
- Building the native image inside these workflows. It is pulled.

## Anti-goals

- A native leg that passes because it silently tests something else: server started without HA, wrong root password,
  JVM-only settings ignored, or the JVM image tested twice. The run log must name the image tested.
- A native leg marked report-only / `continue-on-error`. It must be as red as the JVM leg.
- Two legs overwriting each other's artifacts or test-report checks.

## Current state (analysis)

- `load-tests/src/test/java/com/arcadedb/test/support/ContainersTestTemplate.java` is the shared base; `e2e-ha`
  depends on the `arcadedb-load-tests` test-jar. `IMAGE` already reads `-Darcadedb.test.image`
  (default `arcadedata/arcadedb:latest`).
- All three container factories (around lines 491, 530, 590) pass server settings through `JAVA_OPTS` and memory through
  `ARCADEDB_OPTS_MEMORY` (plus `ARCADEDB_HEAP_DUMP_DIR` on the persistent one). Those are expanded by `bin/server.sh`
  in the JVM image only. The native images' entrypoint is the binary itself
  (`native/src/main/docker/Dockerfile.native.scratch` for amd64, `Dockerfile.native.distroless` for arm64), so on
  native every one of those settings is silently dropped: no HA, no root password, default heap.
- The cleanup container (around line 282) runs `rm -rf` as root using `IMAGE`. The native images have no `rm` and no
  shell (amd64 is `FROM scratch`), so it fails on native.
- The native binary includes `arcadedb-ha-raft`, `grpcw` and `metrics` (`native/pom.xml`), so
  `GrpcServerPlugin,PrometheusMetricsPlugin` and Raft HA are available. Gremlin is not bundled in native; these suites
  do not use it.
- `GlobalConfiguration` reads settings from environment variables named after the setting
  (`arcadedb.server.rootPassword=...`) in both images; the e2e template relies on this.
- Both workflows run `mvn verify ... --fail-never`; failure is signalled by `dorny/test-reporter` (check named
  "IT Tests Report").
- `SingleLocalhostServerSimpleLoadTestIT` does not start containers (it runs a server in-process), so it does not
  test any image.

## Decisions (agreed with the user)

1. The native image is pulled: `arcadedata/arcadedb:latest-native`. No build in these workflows.
2. A native failure is reported as a failure, same as JVM.
3. Schedules stay as they are for now.
4. Start small and lean. Done = template and two-leg workflows in place, both legs run, native failures are visible
   and get filed as issues. No native-only tuning in this work: the native image runs with its defaults (Serial GC,
   since `native/pom.xml` passes no `--gc`, versus ZGC in the JVM image), same heap and timeouts as the JVM leg. GC or
   other parameters are tuned later, after the first runs show what fails.

## Design

### Test template (`ContainersTestTemplate`)

- Add a `NATIVE` flag resolved like the e2e template: `-Darcadedb.test.native=true|false`, else derived from the image
  TAG containing `native` (reuse the `tagOf` logic: tag only, not repository name).
- Pass each server setting as its own environment variable (`withEnv("arcadedb.ha.enabled", "true")`, ...) instead of
  one `JAVA_OPTS` string. This is the single path for both images. The set of settings and their values must stay
  exactly as today in all three factories.
- Memory/heap:
  - JVM: keep `ARCADEDB_OPTS_MEMORY` as today.
  - Native: pass the same `-Xms/-Xmx` (and any extra opts the caller supplied) as container command arguments, which
    Docker appends after the native ENTRYPOINT's own arguments. Container memory limits (`withMemory`) stay as today.
  - `HaChaosIT` passes free-form `chaos.serverOpts` through the memory string; it must keep working on JVM. On native,
    forwarding those as command arguments is acceptable.
- Heap dumps: `ARCADEDB_HEAP_DUMP_DIR` stays for JVM. Native equivalent is the builder's call (see below).
- Cleanup container: run with a pinned `busybox` image (digest-pinned, as the native Dockerfiles do) instead of
  `IMAGE`, so it works regardless of the image under test.
- Log `"ArcadeDB test image: <image> (native)"` once, like the e2e template, so every run log names what it tested.

### Workflows

`ha-resilience-tests.yml` and `load-tests.yml` each get `strategy.matrix.image: [jvm, native]` with
`fail-fast: false`.

- `jvm` leg: unchanged steps (`-Pdocker` build, then tests against the freshly built image).
- `native` leg: skip the Docker image build; `docker pull arcadedata/arcadedb:latest-native` and print its digest;
  install the test module's dependencies from this tree (`./mvnw install -DskipTests -pl <module> -am`, as the native
  e2e job does); run the same `mvn verify` with `-Darcadedb.test.image=arcadedata/arcadedb:latest-native
  -Darcadedb.test.native=true`. The `it_test` dispatch input of ha-resilience applies to both legs.
- Naming (my call): the JVM leg keeps today's check and artifact names unchanged; only the native leg gets a suffix
  (`e2e-ha-failsafe-reports-native`, check "IT Tests Report (native)").
- Image version (my call): `latest-native` floats. The native leg logs the pulled digest and creation time (the native image has no version label);
  a mismatch with this tree's pom version is not an error (the schedule follow-up will narrow the skew).
- Server logs (my call): the server writes to `/home/arcadedb/log`, not the mounted `logs` folder, so today's
  `e2e-ha-server-logs` artifact carries almost no server log. Both workflows, both legs, upload
  `target/container-logs/` (container stdout, already written by the template), suffixed per leg as above. No server
  setting changes for this.
- `SingleLocalhostServerSimpleLoadTestIT` is excluded from the native leg (it would only retest the JVM).

## Verification

- Unit-level: a small test for the `NATIVE` resolution (tag parsing), mirroring the e2e one if it exists.
- Local: run one HA IT (e.g. `SimpleHaScenarioIT`) and one load IT against both `arcadedata/arcadedb:latest` (built
  with `-Pdocker`) and `arcadedata/arcadedb:latest-native`; check server logs show HA enabled and the right server
  name on native.
- A deliberate negative check: on native, confirm a node really joined the Raft cluster (the existing leader-election
  waits already assert this), so a dropped-settings regression goes red instead of green.
- CI: dispatch both workflows from the branch and confirm two legs, two report checks, separate artifacts.

## Left to the builder

- Exact native heap-dump handling (e.g. `-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=...` as command args if the
  native binary supports them, otherwise omit on native).
- How to exclude the localhost load test in the native leg (Failsafe exclude via property, or `assumeFalse(NATIVE)`
  in the test).
- Whether the native leg runs on amd64 only (default: `ubuntu-latest`, i.e. amd64, the `FROM scratch` image) or also
  arm64. My call: amd64 only, matching the current workflows' runner.
- Workflow step layout details.
