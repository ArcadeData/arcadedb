# Native image in HA resilience and load tests Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Run the `e2e-ha` and `load-tests` suites nightly against both the JVM image and the pulled
`arcadedata/arcadedb:latest-native`, with native failures reported like JVM ones.

**Architecture:** `ContainersTestTemplate` (load-tests, shared with e2e-ha through its test-jar) stops passing server
settings through `JAVA_OPTS` (expanded only by the JVM image's `server.sh`) and passes each setting as an environment
variable named after it, which `GlobalConfiguration.readConfiguration()` reads in both images. Heap options go to
`ARCADEDB_OPTS_MEMORY` on JVM and to container command arguments on native. The two workflows get a `[jvm, native]`
matrix.

**Tech Stack:** Java 21, JUnit 5, AssertJ, Testcontainers, Maven Failsafe, GitHub Actions.

**Spec:** `docs/superpowers/specs/2026-10-19-native-image-ha-resilience-and-load-tests-design.md`

## Global Constraints

- Native image: `arcadedata/arcadedb:latest-native`, pulled, never built in these workflows.
- No native-only tuning: same settings, heap, container memory, timeouts as the JVM leg. Native runs its default GC.
- Server settings per factory stay exactly as today (names and values), only the transport changes.
- JVM leg check and artifact names unchanged; native leg adds ` (native)` to the check name and `-native` to artifact names.
- Native leg runs on `ubuntu-latest` (amd64) only. Schedules unchanged (`0 0 * * *`).
- No `continue-on-error` on the native leg; `fail-fast: false` on the matrix.
- Cleanup container image: `busybox:1.37.0-musl@sha256:222ad6d973c0d198014546a65cd02c5fdedcc172123c5b4c2bf0af636550bd94`
  (the pin the native Dockerfiles already use).
- Project rules (CLAUDE.md): `final` where possible, no braces for single-statement `if`, AssertJ
  `assertThat(...).isTrue()` style, no fully qualified names, no Claude authorship, **do not commit** (the user commits
  after review), remove debug `System.out`.

## Review Focus

1. JVM image with `withCommand(...)` set: the JVM image's `CMD ["./bin/server.sh"]` would be replaced and the server
   never starts. The JVM path must never set a command. Pinned by `jvmImageGetsMemoryEnvAndNoCommand` (Task 1).
2. A `JAVA_OPTS` env left behind on either path: on JVM, system properties beat env vars in `readConfiguration()`, so a
   stale `JAVA_OPTS` copy would mask the env settings. Pinned by `noJavaOptsOnEitherImage` (Task 1).
3. Image reference whose repository (not tag) contains `native` (e.g. `myregistry/arcadedb-native:latest`) must not
   count as native; a registry with a port (`localhost:5000/arcadedata/arcadedb:latest-native`) must. Pinned by
   `nativeDetectionUsesTagOnly` (Task 1).
4. `HaChaosIT` free-form `chaos.serverOpts` (e.g. `-Xms4G -Xmx4G -Dfoo=bar`, multiple spaces) must reach the native
   binary as separate arguments with no empty ones. Pinned by `nativeImageSplitsMemoryOptsIntoCommandArgs` (Task 1).
5. Native leg silently testing the JVM image (no `-Darcadedb.test.image` passed, or pull failed): pinned by the
   workflow printing the image line from the template and failing the step if `docker pull` fails (Task 2, Step 3).

---

### Task 1: Image-agnostic server configuration in `ContainersTestTemplate`

**Files:**
- Modify: `load-tests/src/test/java/com/arcadedb/test/support/ContainersTestTemplate.java`
  (`IMAGE` ~line 77; cleanup ~lines 279-292; factories ~lines 488-620)
- Modify: `load-tests/src/test/java/com/arcadedb/test/load/SingleLocalhostServerSimpleLoadTestIT.java`
- Test: `load-tests/src/test/java/com/arcadedb/test/support/ContainersTestTemplateTest.java` (create)

**Interfaces:**
- Produces (all in `ContainersTestTemplate`):
  - `public static final boolean NATIVE` = `Boolean.parseBoolean(System.getProperty("arcadedb.test.native", String.valueOf(isNativeTag(IMAGE))))`
  - `static boolean isNativeTag(final String image)`: true when the tag (text after the last `:` that follows the last
    `/`, else `""`) contains `native`. Same rule as `tagOf` in `e2e/.../ArcadeContainerTemplate.java`.
  - `static GenericContainer<?> configureServer(final GenericContainer<?> container, final Map<String, String> settings, final String memoryOpts, final boolean nativeImage)`:
    applies every `settings` entry as `withEnv(key, value)`; if `nativeImage`, `withCommand(memoryOpts.trim().split("\\s+"))`
    (skip when blank), else `withEnv("ARCADEDB_OPTS_MEMORY", memoryOpts)`. Returns `container`.
  - `static final String CLEANUP_IMAGE` = the busybox pin from Global Constraints.

- [ ] **Step 1: Write the failing tests** in `ContainersTestTemplateTest` (plain JUnit, no Docker: a `GenericContainer`
  can be constructed and inspected without starting it)

```java
@Test
void nativeDetectionUsesTagOnly() {
  assertThat(ContainersTestTemplate.isNativeTag("arcadedata/arcadedb:latest-native")).isTrue();
  assertThat(ContainersTestTemplate.isNativeTag("arcadedata/arcadedb:26.11.1-native-amd64")).isTrue();
  assertThat(ContainersTestTemplate.isNativeTag("localhost:5000/arcadedata/arcadedb:latest-native")).isTrue();
  assertThat(ContainersTestTemplate.isNativeTag("arcadedata/arcadedb:latest")).isFalse();
  assertThat(ContainersTestTemplate.isNativeTag("myregistry/arcadedb-native:latest")).isFalse();
  assertThat(ContainersTestTemplate.isNativeTag("localhost:5000/arcadedb-native")).isFalse();
}

@Test
void jvmImageGetsMemoryEnvAndNoCommand() {
  final GenericContainer<?> c = ContainersTestTemplate.configureServer(new GenericContainer<>("arcadedata/arcadedb:latest"),
      Map.of("arcadedb.server.name", "arcadedb-0", "arcadedb.ha.enabled", "true"), "-Xms2G -Xmx2G", false);
  assertThat(c.getEnvMap()).containsEntry("arcadedb.server.name", "arcadedb-0").containsEntry("arcadedb.ha.enabled", "true")
      .containsEntry("ARCADEDB_OPTS_MEMORY", "-Xms2G -Xmx2G");
  assertThat(c.getCommandParts()).isEmpty();
}

@Test
void nativeImageSplitsMemoryOptsIntoCommandArgs() {
  final GenericContainer<?> c = ContainersTestTemplate.configureServer(new GenericContainer<>("arcadedata/arcadedb:latest-native"),
      Map.of("arcadedb.server.name", "arcadedb-0"), "  -Xms4G   -Xmx4G -Dfoo=bar ", true);
  assertThat(c.getEnvMap()).containsEntry("arcadedb.server.name", "arcadedb-0").doesNotContainKey("ARCADEDB_OPTS_MEMORY");
  assertThat(c.getCommandParts()).containsExactly("-Xms4G", "-Xmx4G", "-Dfoo=bar");
}

@Test
void noJavaOptsOnEitherImage() {
  for (final boolean nativeImage : new boolean[] { false, true })
    assertThat(ContainersTestTemplate.configureServer(new GenericContainer<>("x:y"), Map.of("arcadedb.server.name", "n"),
        "-Xmx1G", nativeImage).getEnvMap()).doesNotContainKey("JAVA_OPTS");
}
```

- [ ] **Step 2: Run to verify they fail**

Run: `./mvnw -o test -pl load-tests -Dtest=ContainersTestTemplateTest` (a single named class, so no `*IT` is pulled in)
Expected: compilation FAILURE, `isNativeTag` / `configureServer` not found.

- [ ] **Step 3: Implement `isNativeTag`, `NATIVE`, `configureServer`, `CLEANUP_IMAGE`** in `ContainersTestTemplate`.
  Add a static log line once (static initializer, `LoggerFactory.getLogger(ContainersTestTemplate.class)`):
  `"ArcadeDB test image: {}{}"` with `IMAGE` and `NATIVE ? " (native)" : ""`. Update the `IMAGE` Javadoc to mention
  `-Darcadedb.test.native` and that native is detected from the tag.

- [ ] **Step 4: Run to verify they pass**

Run: same as Step 2. Expected: `Tests run: 4, Failures: 0, Errors: 0`.

- [ ] **Step 5: Migrate the three factories to `configureServer`**
  - Replace each `.withEnv("JAVA_OPTS", ...)` + `.withEnv("ARCADEDB_OPTS_MEMORY", ...)` pair with
    `configureServer(container, settings, memoryOpts, NATIVE)` where `settings` is a `LinkedHashMap` holding exactly
    the same keys/values the `JAVA_OPTS` string held (drop the `-D`), in the same order:
    - standalone (`createArcadeContainer(name, network)`): `arcadedb.server.rootPassword=playwithdata`,
      `arcadedb.server.plugins=GrpcServerPlugin,PrometheusMetricsPlugin`, `arcadedb.server.name=<name>`,
      `arcadedb.backup.enabled=false`, `arcadedb.typeDefaultBuckets=10`; memory `-Xms2G -Xmx2G`.
    - HA (`createArcadeContainer(name, serverList, quorum, network)`) and persistent HA: the five above plus
      `arcadedb.ha.enabled=true`, `arcadedb.ha.quorum=<quorum>`, `arcadedb.server.readinessRequiresHA=true`,
      `arcadedb.ha.raft.port=2434`, `arcadedb.ha.serverList=<serverList>`,
      `arcadedb.server.restoreImportAllowLocalUrls=true`. Memory: `-Xms2G -Xmx2G` / `memoryOpts`.
    - Extract the shared five-entry base into one private helper (e.g. `baseSettings(name)`), the HA six on top
      (`haSettings(name, quorum, serverList)`), so the three factories don't repeat the list.
  - Keep `ARCADEDB_HEAP_DUMP_DIR` on the persistent factory for JVM only (`if (!NATIVE)`); on native it is omitted
    (spec: lean, no native-only tuning). Update its comment to say so.
  - Everything else (ports, mounts, `withMemory`, wait strategy, startup timeout) unchanged.
  - Fix the `HaChaosIT` comment at ~line 100 (`ARCADEDB_OPTS_MEMORY follows JAVA_OPTS on the server's command line`)
    to say the opts go through `ARCADEDB_OPTS_MEMORY` on JVM and as command arguments on native. No code change there.

- [ ] **Step 6: Cleanup container on busybox** - in `deleteContainersDirectoriesFromContainer()` replace
  `new GenericContainer<>(IMAGE)` with `new GenericContainer<>(CLEANUP_IMAGE)`; rest unchanged. Update its Javadoc to say
  why (the native images have no `rm`).

- [ ] **Step 7: Skip the in-process load test on native** - in `SingleLocalhostServerSimpleLoadTestIT` add a
  `@BeforeAll static void notAgainstNative()` with
  `assumeFalse(ContainersTestTemplate.NATIVE, "starts an in-process server, tests no image")`
  (merge into an existing `@BeforeAll` if the class has one).

- [ ] **Step 8: Compile both modules and rerun the unit tests**

Run: `./mvnw -o install -DskipTests -pl load-tests && ./mvnw -o test-compile -pl e2e-ha && ./mvnw -o test -pl load-tests,e2e-ha -DskipITs`
Expected: `BUILD SUCCESS`; e2e-ha's existing chaos unit tests (`ChaosConfigTest`, `LedgerTest`, ...) still pass.

- [ ] **Step 9: Stop.** Do not commit; the user commits after review.

---

### Task 2: `[jvm, native]` matrix in both workflows

**Files:**
- Modify: `.github/workflows/ha-resilience-tests.yml`
- Modify: `.github/workflows/load-tests.yml`

**Interfaces:**
- Consumes: `-Darcadedb.test.image`, `-Darcadedb.test.native` (Task 1); the template's `ArcadeDB test image: ...` log line;
  `target/container-logs/` (already written by the template).

- [ ] **Step 1: Matrix.** In each workflow's `setup` job add
  `strategy: { fail-fast: false, matrix: { image: [jvm, native] } }` and
  `name: setup (${{ matrix.image }})`. No `continue-on-error`.

- [ ] **Step 2: JVM-only steps.** Add `if: matrix.image == 'jvm'` to "Set up QEMU", "Set up Docker Buildx" and
  "Build and package with Maven Docker profile". All other existing steps (checkout, SHA-pin check, pre-commit, JDK,
  Maven cache) run on both legs.

- [ ] **Step 3: Native-only steps**, `if: matrix.image == 'native'`, placed where the Docker build was:
  - "Pull native image": `docker pull arcadedata/arcadedb:latest-native` then
    `docker image inspect --format '{{index .RepoDigests 0}} created {{.Created}}' arcadedata/arcadedb:latest-native`
    (the native image carries no version label; digest plus creation time names the build). `set -euo pipefail` so a
    failed pull fails the leg.
  - "Build test modules": `./mvnw -B -q install -DskipTests -pl e2e-ha -am` (ha-resilience) /
    `./mvnw -B -q install -DskipTests -pl load-tests -am` (load-tests).

- [ ] **Step 4: Test step passes the image.** Set a job-level env
  `TEST_IMAGE_ARGS: ${{ matrix.image == 'native' && '-Darcadedb.test.image=arcadedata/arcadedb:latest-native -Darcadedb.test.native=true' || '' }}`
  and append `${TEST_IMAGE_ARGS}` to every `./mvnw verify` line in the test step (both branches of the `IT_TEST` `if` in
  ha-resilience; the single line in load-tests). JVM leg command lines are otherwise unchanged.

- [ ] **Step 5: Names and artifacts.**
  - Artifact names: `name: e2e-ha-failsafe-reports${{ matrix.image == 'native' && '-native' || '' }}`, same for
    `e2e-ha-server-logs`.
  - New upload step in both workflows, `if: success() || failure()`: name
    `e2e-ha-container-logs${{ ...'-native'... }}` / `load-tests-container-logs${{ ...'-native'... }}`, path
    `e2e-ha/target/container-logs/` / `load-tests/target/container-logs/`, `retention-days: 7`, `if-no-files-found: warn`,
    same pinned `actions/upload-artifact` SHA as the existing steps.
  - Test reporter: `name: IT Tests Report${{ matrix.image == 'native' && ' (native)' || '' }}`.

- [ ] **Step 6: Lint.** Run: `pre-commit run --files .github/workflows/ha-resilience-tests.yml .github/workflows/load-tests.yml`
  and, if available, `actionlint .github/workflows/ha-resilience-tests.yml .github/workflows/load-tests.yml`.
  Expected: no errors.

- [ ] **Step 7: Stop.** Do not commit.

---

### Task 3: Verify against both images locally

No code. Proves the template drives both images and that native nodes really form a Raft cluster.

- [ ] **Step 1: Build the JVM image and pull native.**
  `./mvnw clean install -Pdocker -DskipTests` then `docker pull arcadedata/arcadedb:latest-native`.
  Check `lsof -nP -iTCP:2480-2489 -sTCP:LISTEN` is empty first.

- [ ] **Step 2: JVM regression.** `./mvnw verify -DskipTests -Pintegration -pl e2e-ha -Dit.test=SimpleHaScenarioIT`
  and `./mvnw verify -Pintegration -pl load-tests -Dit.test=SingleServerSimpleLoadTestIT`.
  Expected: both green; log shows `ArcadeDB test image: arcadedata/arcadedb:latest` (no `(native)`).

- [ ] **Step 3: Native.** Same two commands plus
  `-Darcadedb.test.image=arcadedata/arcadedb:latest-native -Darcadedb.test.native=true`.
  Expected: log shows `(native)`; `SimpleHaScenarioIT` passes its leader-election wait (proves `arcadedb.ha.*` reached
  the native binary - with dropped settings readiness never turns 204 and the test fails). A red native result here is
  an allowed outcome per the spec: record the failure and report it, do not tune.

- [ ] **Step 4: Localhost skip.** `./mvnw verify -Pintegration -pl load-tests -Dit.test=SingleLocalhostServerSimpleLoadTestIT -Darcadedb.test.image=arcadedata/arcadedb:latest-native`
  Expected: test reported as skipped.

- [ ] **Step 5: CI dispatch - ask the user first** (outward-facing). Proposed: push the branch and
  `gh workflow run ha-resilience-tests.yml --ref <branch> -f it_test=SimpleHaScenarioIT` and
  `gh workflow run load-tests.yml --ref <branch>`. Expected: two legs each, checks "IT Tests Report" and
  "IT Tests Report (native)", `-native` artifacts present.

- [ ] **Step 6: Report** results of Steps 2-5 to the user, including any native failure output verbatim. Remove any
  debug output added along the way.
