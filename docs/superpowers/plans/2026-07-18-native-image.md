# ArcadeDB Native Image Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship a GraalVM native-image build of the ArcadeDB server as an experimental add-on: a self-contained native binary for 5 OS/arch targets, plus tiny static Docker images for linux/amd64 and linux/arm64, without gating or replacing the JVM distribution.

**Architecture:** Revive the stale `poc/native_image` `native` Maven module, rebased onto v26.8.1 and GraalVM for JDK 21 Community. A `-Pnative` profile (outside the default reactor) drives the `native-maven-plugin`. Reachability metadata is regenerated with the GraalVM tracing agent plus library-supplied metadata, not hand-maintained. A single smoke-test script is the acceptance gate at every stage (local, per-matrix-job, per-container). A 5-target GitHub Actions matrix builds the binaries (native-image cannot cross-compile), and static-musl Linux binaries go into `FROM scratch` images stitched into a multi-arch manifest.

**Tech Stack:** GraalVM for JDK 21 Community, `org.graalvm.buildtools:native-maven-plugin`, `graalvm/setup-graalvm@v1`, Maven, GitHub Actions, `docker buildx` + `imagetools`, musl toolchain, existing ArcadeDB modules (engine, server, wire protocols).

## Global Constraints

- **Build/native JDK:** **GraalVM CE 25 (JDK 25)** (`distribution: graalvm-community`, `java-version: 25`), matching the project's `.java-version` pin (`graalvm64-25`) and the engine's polyglot `25.1.3`. The app compiles to `release 21` bytecode (`maven.compiler.release=21`, unchanged); the GraalVM 25 native-image builder consumes it. GraalVM CE 21.0.2 is also installed locally as a fallback.
- **Native module is out of the default reactor:** it must NOT be added to the root `pom.xml` `<modules>` default list; it builds only via `-Pnative`. Normal `mvn clean install` stays unchanged.
- **Feature set in the binary:** engine, network, server, ha-raft, metrics, console, graphql, postgresw, redisw, mongodbw, bolt, grpcw. **Excluded:** Gremlin, and the `tracing` (OpenTelemetry) module.
- **Polyglot JS:** conditionally included, decided by the Task 5 time-boxed spike; fall back to excluding it. SQL and Cypher must work regardless.
- **Docker images:** static (`--static --libc=musl`) Linux binaries only, `FROM scratch`, tags `arcadedata/arcadedb:<version>-native` and per-arch layers; never touch the JVM `latest`/default tags.
- **Dependencies:** Apache-2.0-compatible only (per CLAUDE.md allow-list). No new runtime dependency unless strictly necessary.
- **Code style:** import classes (no FQN), `final` on variables/params where practical, one-child `if` needs no braces, no `System.out` debug left behind, no Claude authorship in source.
- **Commits:** repo convention is that the maintainer reviews before committing. When executing, either commit per task as written below or stage and hand off per the maintainer's preference at execution time.
- **Copyright header:** every new source/XML/script file must carry the Apache-2.0 header block used across the repo (copy from any existing file of the same type).

---

### Task 1: Revive and rebase the `native` module skeleton

Bring back the POC `native` module on top of current `main`, updated to v26.8.1, the correct dependency set (Gremlin and tracing removed, all kept wire protocols added), and a modern `native-maven-plugin`. No native build yet - just a resolvable module and profile.

**Files:**
- Create: `native/pom.xml`
- Modify: `pom.xml` (add a `native` profile that registers the module; do NOT add it to the default `<modules>`)
- Create: `native/.gitignore` (ignore `target/`)

**Interfaces:**
- Produces: a Maven module `com.arcadedb:arcadedb-native` buildable via `mvn -Pnative -pl native -am ...`, main class `com.arcadedb.server.ArcadeDBServer`, native-image output name `arcadedb-<version>-<os>-<arch>` in `native/target/`.

- [ ] **Step 1: Add the module behind a profile in the root `pom.xml`**

Find the `<profiles>` section of the root `pom.xml` and add:

```xml
<profile>
    <id>native</id>
    <modules>
        <module>native</module>
    </modules>
</profile>
```

Leave the default `<modules>` list untouched so `mvn clean install` never builds `native`.

- [ ] **Step 2: Create `native/pom.xml`**

Include the Apache-2.0 header (copy from `engine/pom.xml`). Body:

```xml
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <parent>
        <groupId>com.arcadedb</groupId>
        <artifactId>arcadedb-parent</artifactId>
        <version>26.8.1-SNAPSHOT</version>
        <relativePath>../pom.xml</relativePath>
    </parent>

    <artifactId>arcadedb-native</artifactId>
    <packaging>jar</packaging>

    <properties>
        <mainClass>com.arcadedb.server.ArcadeDBServer</mainClass>
        <native-maven-plugin.version>0.10.6</native-maven-plugin.version>
        <imageName>arcadedb-${project.version}-${os.detected.name}-${os.detected.arch}</imageName>
        <maven.deploy.skip>true</maven.deploy.skip>
    </properties>

    <dependencies>
        <dependency>
            <groupId>com.arcadedb</groupId>
            <artifactId>arcadedb-server</artifactId>
            <version>${project.parent.version}</version>
        </dependency>
        <dependency>
            <groupId>com.arcadedb</groupId>
            <artifactId>arcadedb-console</artifactId>
            <version>${project.parent.version}</version>
        </dependency>
        <dependency>
            <groupId>com.arcadedb</groupId>
            <artifactId>arcadedb-studio</artifactId>
            <version>${project.parent.version}</version>
        </dependency>
        <dependency>
            <groupId>com.arcadedb</groupId>
            <artifactId>arcadedb-graphql</artifactId>
            <version>${project.parent.version}</version>
        </dependency>
        <dependency>
            <groupId>com.arcadedb</groupId>
            <artifactId>arcadedb-postgresw</artifactId>
            <version>${project.parent.version}</version>
        </dependency>
        <dependency>
            <groupId>com.arcadedb</groupId>
            <artifactId>arcadedb-redisw</artifactId>
            <version>${project.parent.version}</version>
        </dependency>
        <dependency>
            <groupId>com.arcadedb</groupId>
            <artifactId>arcadedb-mongodbw</artifactId>
            <version>${project.parent.version}</version>
        </dependency>
        <dependency>
            <groupId>com.arcadedb</groupId>
            <artifactId>arcadedb-bolt</artifactId>
            <version>${project.parent.version}</version>
        </dependency>
        <dependency>
            <groupId>com.arcadedb</groupId>
            <artifactId>arcadedb-grpcw</artifactId>
            <version>${project.parent.version}</version>
        </dependency>
    </dependencies>

    <build>
        <extensions>
            <extension>
                <groupId>kr.motd.maven</groupId>
                <artifactId>os-maven-plugin</artifactId>
                <version>1.7.1</version>
            </extension>
        </extensions>
    </build>

    <profiles>
        <profile>
            <id>native</id>
            <build>
                <plugins>
                    <plugin>
                        <groupId>org.graalvm.buildtools</groupId>
                        <artifactId>native-maven-plugin</artifactId>
                        <version>${native-maven-plugin.version}</version>
                        <extensions>true</extensions>
                        <executions>
                            <execution>
                                <id>build-native</id>
                                <goals><goal>compile-no-fork</goal></goals>
                                <phase>package</phase>
                            </execution>
                        </executions>
                        <configuration>
                            <mainClass>${mainClass}</mainClass>
                            <imageName>${imageName}</imageName>
                            <buildArgs>
                                <buildArg>--no-fallback</buildArg>
                                <buildArg>-H:+ReportExceptionStackTraces</buildArg>
                                <buildArg>--enable-url-protocols=http,https</buildArg>
                                <buildArg>--add-modules=jdk.incubator.vector</buildArg>
                            </buildArgs>
                        </configuration>
                    </plugin>
                </plugins>
            </build>
        </profile>
    </profiles>
</project>
```

Notes: the `os-maven-plugin` extension provides `${os.detected.name}`/`${os.detected.arch}` for the image name; the wire-protocol modules use `provided` scope for `arcadedb-server` per repo convention, so the `native` module pulls `arcadedb-server` explicitly (above) to supply it at runtime. Gremlin and `arcadedb-tracing` are deliberately absent.

- [ ] **Step 3: Create `native/.gitignore`**

```
target/
```

- [ ] **Step 4: Verify the default build is unaffected**

Run: `mvn -q -DskipTests -pl native help:evaluate -Dexpression=project.artifactId -DforceStdout 2>/dev/null; echo`
Expected: prints `arcadedb-native` only when the module is resolvable; then run `mvn -q help:evaluate -Dexpression=project.modules -DforceStdout -pl . 2>/dev/null | grep -c native` and expect `0` (native not in default modules).

- [ ] **Step 5: Verify the module resolves under the profile**

Run: `mvn -q -Pnative -pl native -am -DskipTests install -o 2>&1 | tail -5` (first run without `-o` to populate the local repo).
Expected: BUILD SUCCESS (this compiles/packages the plain jar; the native profile plugin only fires when the inner `-Pnative` is active in Task 4). If the reactor needs sibling artifacts, run `mvn -DskipTests -pl native -am install` once first.

- [ ] **Step 6: Commit**

```bash
git add native/pom.xml native/.gitignore pom.xml
git commit -m "build(native): revive native-image module skeleton behind -Pnative profile"
```

---

### Task 2: Reusable smoke-test script (the acceptance gate)

Write the acceptance test BEFORE the binary exists, and validate it against the existing JVM server so we know the assertions themselves are correct. Every later stage reuses this exact script.

**Files:**
- Create: `native/src/test/scripts/smoke.sh`

**Interfaces:**
- Consumes: a path to a runnable server (native binary OR `server.sh`) as `$1`, plus env `ARCADEDB_HOME` for config.
- Produces: exit 0 on success, non-zero with a diagnostic on first failed assertion. Asserts: HTTP ready on :2480, Studio index served, SQL `CREATE`/`SELECT` round-trip, Cypher `MATCH` round-trip, Postgres-wire `SELECT` round-trip.

- [ ] **Step 1: Write `native/src/test/scripts/smoke.sh`**

Include the Apache-2.0 shell header. Body:

```bash
#!/usr/bin/env bash
set -euo pipefail

# Usage: smoke.sh <path-to-server-executable> [extra server args...]
# Boots the server, waits for HTTP readiness, and asserts SQL, Cypher, Studio,
# and Postgres-wire round-trips. Reused for local, CI-matrix, and container smoke.

EXE="${1:?path to server executable required}"; shift || true
HOST=127.0.0.1
HTTP=2480
PG=5432
USER=root
PASS="${ARCADEDB_ROOT_PASSWORD:-PlayWithData123!}"

WORK="$(mktemp -d)"
trap 'kill "${SRV_PID:-0}" 2>/dev/null || true; rm -rf "$WORK"' EXIT

echo "[smoke] launching: $EXE"
ARCADEDB_ROOT_PASSWORD="$PASS" \
  "$EXE" -Darcadedb.server.rootPassword="$PASS" \
         -Darcadedb.server.databaseDirectory="$WORK/databases" "$@" \
  >"$WORK/server.log" 2>&1 &
SRV_PID=$!

echo "[smoke] waiting for HTTP :$HTTP"
for i in $(seq 1 60); do
  if curl -fsS "http://$HOST:$HTTP/api/v1/ready" >/dev/null 2>&1; then break; fi
  if ! kill -0 "$SRV_PID" 2>/dev/null; then
    echo "[smoke] FAIL: server exited early"; tail -50 "$WORK/server.log"; exit 1
  fi
  sleep 2
  if [ "$i" -eq 60 ]; then echo "[smoke] FAIL: HTTP never ready"; tail -50 "$WORK/server.log"; exit 1; fi
done

req() { curl -fsS -u "$USER:$PASS" -H 'Content-Type: application/json' "$@"; }

echo "[smoke] Studio index"
req "http://$HOST:$HTTP/" | grep -qi "arcadedb\|studio" || { echo "[smoke] FAIL: Studio index"; exit 1; }

echo "[smoke] create DB"
req -X POST "http://$HOST:$HTTP/api/v1/server" -d '{"command":"create database smoke"}' >/dev/null

echo "[smoke] SQL round-trip"
req -X POST "http://$HOST:$HTTP/api/v1/command/smoke" \
  -d '{"language":"sql","command":"CREATE DOCUMENT TYPE T; INSERT INTO T SET n = 42"}' >/dev/null
OUT="$(req -X POST "http://$HOST:$HTTP/api/v1/query/smoke" -d '{"language":"sql","command":"SELECT n FROM T"}')"
echo "$OUT" | grep -q '42' || { echo "[smoke] FAIL: SQL, got $OUT"; exit 1; }

echo "[smoke] Cypher round-trip"
OUT="$(req -X POST "http://$HOST:$HTTP/api/v1/command/smoke" \
  -d '{"language":"cypher","command":"CREATE (a:Person {name:\"Ada\"}) RETURN a.name AS n"}')"
echo "$OUT" | grep -q 'Ada' || { echo "[smoke] FAIL: Cypher, got $OUT"; exit 1; }

echo "[smoke] Postgres-wire round-trip"
if command -v psql >/dev/null 2>&1; then
  PGPASSWORD="$PASS" psql -h "$HOST" -p "$PG" -U "$USER" -d smoke -tAc 'SELECT 1' | grep -q '1' \
    || { echo "[smoke] FAIL: Postgres wire"; exit 1; }
else
  echo "[smoke] WARN: psql not installed, skipping Postgres-wire assertion"
fi

echo "[smoke] PASS"
```

- [ ] **Step 2: Make it executable**

Run: `chmod +x native/src/test/scripts/smoke.sh`

- [ ] **Step 3: Validate the script against the JVM server (control run)**

Build a normal distribution and point the script at `server.sh`:
Run:
```bash
mvn -q -DskipTests -pl package -am install
DIST=$(ls -d package/target/arcadedb-*.dir 2>/dev/null | head -1 || ls -d package/target/*/ | head -1)
ARCADEDB_HOME="$DIST" native/src/test/scripts/smoke.sh "$DIST/bin/server.sh"
```
Expected: ends with `[smoke] PASS`. If any assertion is wrong (endpoint path, ready URL, auth), FIX THE SCRIPT here against the known-good JVM server. Confirm the exact readiness endpoint (`/api/v1/ready`) and Postgres default port against `server/src/main/.../http` handlers and adjust if needed.

- [ ] **Step 4: Commit**

```bash
git add native/src/test/scripts/smoke.sh
git commit -m "test(native): add server smoke-test script validated against JVM build"
```

---

### Task 3: Generate reachability metadata with the tracing agent

Produce the reflection/resource/JNI/serialization/proxy config by observing a real run, rather than hand-writing it. Layer library metadata on top so we only curate ArcadeDB-specific deltas.

**Files:**
- Create: `native/src/test/scripts/trace.sh`
- Create: `native/src/main/resources/META-INF/native-image/com.arcadedb/arcadedb-native/` (agent output lands here)
- Modify: `native/pom.xml` (enable the reachability-metadata repository support)

**Interfaces:**
- Consumes: the distribution jars on the classpath and the smoke driver from Task 2.
- Produces: `reachability-metadata.json` (or the classic `reflect-config.json`/`resource-config.json`/`jni-config.json`/`serialization-config.json`/`proxy-config.json`) committed under `META-INF/native-image/...`.

- [ ] **Step 1: Enable library metadata repository in `native/pom.xml`**

Add to the `native-maven-plugin` `<configuration>`:

```xml
<metadataRepository>
    <enabled>true</enabled>
</metadataRepository>
```

This makes the plugin pull known configs for Netty, Lucene, gRPC/protobuf, graphql-java, etc. from the bundled GraalVM reachability-metadata repository.

- [ ] **Step 2: Write `native/src/test/scripts/trace.sh`**

Apache-2.0 header, then:

```bash
#!/usr/bin/env bash
set -euo pipefail
# Runs the assembled server under the GraalVM native-image tracing agent while the
# smoke script exercises SQL, Cypher, Studio, and every included wire protocol, so
# the agent records the reflection/resource/JNI surface actually used.

OUT=native/src/main/resources/META-INF/native-image/com.arcadedb/arcadedb-native
mkdir -p "$OUT"

DIST=$(ls -d package/target/arcadedb-*.dir 2>/dev/null | head -1)
WORK="$(mktemp -d)"; trap 'kill ${SRV_PID:-0} 2>/dev/null || true; rm -rf "$WORK"' EXIT

"$JAVA_HOME/bin/java" \
  -agentlib:native-image-agent=config-merge-dir="$OUT",config-write-period-secs=5 \
  --add-modules jdk.incubator.vector \
  -Darcadedb.server.rootPassword=PlayWithData123! \
  -Darcadedb.server.databaseDirectory="$WORK/databases" \
  -Darcadedb.server.plugins="Postgres:com.arcadedb.postgres.PostgresProtocolPlugin,Redis:com.arcadedb.redis.RedisProtocolPlugin,MongoDB:com.arcadedb.mongo.MongoDBProtocolPlugin" \
  -cp "$DIST/lib/*" com.arcadedb.server.ArcadeDBServer >"$WORK/agent.log" 2>&1 &
SRV_PID=$!

ARCADEDB_HOME="$DIST" native/src/test/scripts/smoke.sh /bin/true || true  # reuse assertions against running server
# (smoke.sh here only needs to hit HTTP; if it launches its own server, instead
#  inline the curl exercises against the agent-instrumented server on :2480.)

sleep 8
kill "$SRV_PID" 2>/dev/null || true
echo "[trace] metadata written to $OUT"
ls -la "$OUT"
```

Note for the implementer: the agent must observe the SAME JVM it instruments. If `smoke.sh` starts its own server, refactor the exercise portion of `smoke.sh` into a `native/src/test/scripts/exercise.sh` (the curl/psql assertions only, against an already-running `:2480`) and call `exercise.sh` from both `smoke.sh` and `trace.sh`. Do this refactor now if needed.

- [ ] **Step 3: Run the tracing agent**

Run: `export JAVA_HOME=$(dirname $(dirname $(which native-image))) ; chmod +x native/src/test/scripts/trace.sh ; native/src/test/scripts/trace.sh`
Expected: `$OUT` contains `reachability-metadata.json` (or the classic `*-config.json` set) and is non-empty. Exercise every included protocol so their reflection is captured (extend `exercise.sh` with a Redis PING, a Mongo `hello`, a Bolt handshake if drivers are available; otherwise note the gap for Task 4 iteration).

- [ ] **Step 4: Commit the generated metadata**

```bash
git add native/pom.xml native/src/test/scripts/trace.sh native/src/test/scripts/exercise.sh native/src/main/resources/META-INF/native-image
git commit -m "build(native): generate reachability metadata via tracing agent + metadata repo"
```

---

### Task 4: First native build on linux/amd64 and iterate the known blockers

Get the binary to build and pass `smoke.sh` on linux/amd64. This is the iterative heart of the project. The "test" is the native build succeeding AND `smoke.sh` passing - there is no JUnit here.

**Files:**
- Modify: `native/pom.xml` (grow `buildArgs` as blockers are resolved)
- Modify: `native/src/main/resources/META-INF/native-image/...` (hand-curated deltas)

**Interfaces:**
- Produces: `native/target/arcadedb-<version>-linux-amd64` that boots and passes `smoke.sh`.

- [ ] **Step 1: Attempt the first build**

Run: `mvn -q -Pnative -pl native -am -DskipTests package -o 2>&1 | tee /tmp/native-build.log | tail -40`
Expected initially: FAIL. Read the analysis errors; they name the classes needing build-time/run-time init or missing metadata.

- [ ] **Step 2: Resolve the Undertow / `wildfly-common` init blocker**

Add to `buildArgs` (this was the POC's wall):

```xml
<buildArg>--initialize-at-run-time=org.wildfly.common.net.HostName</buildArg>
<buildArg>--initialize-at-build-time=org.slf4j.LoggerFactory,org.slf4j.simple.SimpleLogger,org.slf4j.jul.JDK14LoggerAdapter</buildArg>
```

Rebuild (Step 1 command). If the analysis reports a different offending class, add it to the matching `--initialize-at-run-time`/`--initialize-at-build-time` list. Iterate until this class of error clears.

- [ ] **Step 3: Resolve Netty run-time init**

Add:

```xml
<buildArg>--initialize-at-run-time=io.netty</buildArg>
```

(Broad package init-at-run-time is the standard Netty recipe; narrow it only if it causes issues.) Rebuild.

- [ ] **Step 4: Resolve resources (Studio assets, config, Lucene)**

Ensure Studio's static resources and config templates are embedded. Add:

```xml
<buildArg>-H:IncludeResources=.*</buildArg>
```

Prefer tightening later to specific patterns (`.*\.html$`, `.*\.js$`, `.*\.properties$`, Lucene codec resources) once the build is green, to keep the binary small. Rebuild.

- [ ] **Step 5: Force the JLine dumb terminal in server mode**

The server does not need an interactive terminal. Avoid JLine JNI provider loading by ensuring the server path does not initialize a system terminal, or pass `-Dorg.jline.terminal.dumb=true` at runtime (add it to the smoke launch and document it as a native default). If console interactivity is needed later, add `jni-config.json` and bundle the JLine native lib. Rebuild.

- [ ] **Step 6: Iterate to green**

Repeat build → read error → add the minimal metadata/init flag → rebuild, until `mvn -Pnative ... package` succeeds. For "missing reflection registration" errors, prefer regenerating via Task 3's agent (exercise the newly-failing path) over hand-editing. Keep a running note of every added flag in the plan's Task 4 checklist comments.

- [ ] **Step 7: Run the smoke test against the native binary**

Run: `native/src/test/scripts/smoke.sh native/target/arcadedb-*-linux-amd64 -Dorg.jline.terminal.dumb=true`
Expected: `[smoke] PASS`. If a wire protocol fails only in native (works on JVM), it is almost always missing metadata - exercise it under the agent (Task 3) and rebuild.

- [ ] **Step 8: Capture the working build args and commit**

```bash
git add native/pom.xml native/src/main/resources/META-INF/native-image
git commit -m "build(native): linux/amd64 native image builds and passes smoke test"
```

---

### Task 5: Time-boxed polyglot-JS spike

Decide whether GraalVM-JS scripting ships in the binary. Fixed time box: **1 working day.** Outcome is recorded either way; the binary remains shippable.

**Files:**
- Modify: `native/pom.xml` (JS language dep + Truffle build args, OR an exclusion)
- Modify: `native/src/test/scripts/exercise.sh` (add a JS-function assertion, kept or removed per outcome)
- Create: `docs/native-image.md` (limitation note - stub created here, expanded in Task 9)

**Interfaces:**
- Produces: a definite state - either JS scripting works in `smoke.sh` (assertion added) OR JS invocation returns a clear "not supported in native build" error and the limitation is documented.

- [ ] **Step 1: Add a JS-scripting assertion to `exercise.sh`**

```bash
echo "[exercise] JS function"
OUT="$(curl -fsS -u root:$PASS -H 'Content-Type: application/json' \
  -X POST http://127.0.0.1:2480/api/v1/command/smoke \
  -d '{"language":"sql","command":"SELECT \"js\" AS lang"}')" # replace with a real JS UDF call
```
Use an actual JS user-defined-function or `polyglot` SQL invocation representative of the feature.

- [ ] **Step 2: Attempt to include Truffle JS (spike)**

Align the JS language artifact version with the native-image builder's Truffle version. Add to `native/pom.xml` dependencies (community, Apache-2.0/UPL compatible):

```xml
<dependency>
    <groupId>org.graalvm.polyglot</groupId>
    <artifactId>js-community</artifactId>
    <version>${graalvm.version}</version>
    <type>pom</type>
</dependency>
```

Rebuild and run `smoke.sh` with the JS assertion. Iterate within the time box on Truffle-specific build args.

- [ ] **Step 3: Decision gate (end of time box)**

If JS works and `smoke.sh` (with the JS assertion) passes: keep the dependency and the assertion. Record "JS: included" in `docs/native-image.md`.

If it does not converge: remove the JS dependency and the JS assertion from `exercise.sh`; ensure JS invocation surfaces a clear error (verify the engine already throws a comprehensible exception when the JS engine is absent, or add a guard message). Record "JS: excluded (native limitation)" in `docs/native-image.md`.

- [ ] **Step 4: Rebuild, smoke, commit**

Run: `mvn -q -Pnative -pl native -am -DskipTests package -o && native/src/test/scripts/smoke.sh native/target/arcadedb-*-linux-amd64 -Dorg.jline.terminal.dumb=true`
Expected: `[smoke] PASS`.

```bash
git add native/pom.xml native/src/test/scripts/exercise.sh docs/native-image.md
git commit -m "build(native): resolve polyglot-JS spike outcome for native image"
```

---

### Task 6: Five-target matrix workflow

Build the binary on all five runners, smoke each, and publish artifacts. Linux is required; macOS/Windows are best-effort.

**Files:**
- Create: `.github/workflows/native-image.yml`
- Delete: the stale `poc/native_image` workflow is not on `main`; nothing to delete.

**Interfaces:**
- Produces: per-target uploaded artifacts named `arcadedb-<version>-<os>-<arch>[.exe]`; on `release`, the same attached as release assets with `SHA256SUMS`.

- [ ] **Step 1: Write `.github/workflows/native-image.yml`**

No license header (repo convention for workflows). Content:

```yaml
name: Native Image

on:
  workflow_dispatch:
  release:
    types: [published]

permissions:
  contents: write

jobs:
  build:
    name: ${{ matrix.os_name }}-${{ matrix.arch }}
    runs-on: ${{ matrix.runner }}
    continue-on-error: ${{ matrix.required == false }}
    strategy:
      fail-fast: false
      matrix:
        include:
          - { runner: ubuntu-latest,     os_name: linux,   arch: amd64, required: true,  static: true  }
          - { runner: ubuntu-24.04-arm,  os_name: linux,   arch: arm64, required: true,  static: true  }
          - { runner: macos-14,          os_name: macos,   arch: arm64, required: false, static: false }
          - { runner: macos-13,          os_name: macos,   arch: amd64, required: false, static: false }
          - { runner: windows-latest,    os_name: windows, arch: amd64, required: false, static: false }
    steps:
      - uses: actions/checkout@v4

      - name: Set up GraalVM (Community, JDK 25)
        uses: graalvm/setup-graalvm@v1
        with:
          java-version: "25"
          distribution: "graalvm-community"
          github-token: ${{ secrets.GITHUB_TOKEN }}
          native-image-job-reports: "true"

      - name: Install musl toolchain (static Linux builds)
        if: ${{ matrix.static }}
        run: |
          sudo apt-get update
          sudo apt-get install -y musl-tools
          # zlib static for musl, per GraalVM static-image requirements
          echo "MUSL=1" >> "$GITHUB_ENV"

      - name: Build native image
        shell: bash
        run: |
          ARGS=""
          if [ "${{ matrix.static }}" = "true" ]; then
            ARGS="-Dnative.static=true"
          fi
          mvn -B -ntp -Pnative -pl native -am -DskipTests $ARGS package

      - name: Smoke test (Linux/macOS)
        if: ${{ matrix.os_name != 'windows' }}
        shell: bash
        run: |
          BIN=$(ls native/target/arcadedb-*-${{ matrix.os_name }}-${{ matrix.arch }}* | head -1)
          chmod +x "$BIN"
          native/src/test/scripts/smoke.sh "$BIN" -Dorg.jline.terminal.dumb=true

      - name: Package artifact
        shell: bash
        run: |
          cd native/target
          BIN=$(ls arcadedb-*-${{ matrix.os_name }}-${{ matrix.arch }}*)
          if [ "${{ matrix.os_name }}" = "windows" ]; then
            7z a "${BIN%.exe}.zip" "$BIN"
            sha256sum "${BIN%.exe}.zip" > "${BIN%.exe}.zip.sha256" || shasum -a 256 "${BIN%.exe}.zip" > "${BIN%.exe}.zip.sha256"
          else
            tar czf "$BIN.tar.gz" "$BIN"
            shasum -a 256 "$BIN.tar.gz" > "$BIN.tar.gz.sha256"
          fi

      - name: Upload workflow artifact
        uses: actions/upload-artifact@v4
        with:
          name: arcadedb-native-${{ matrix.os_name }}-${{ matrix.arch }}
          path: |
            native/target/arcadedb-*.tar.gz
            native/target/arcadedb-*.zip
            native/target/*.sha256

      - name: Attach to release
        if: ${{ github.event_name == 'release' }}
        env:
          GH_TOKEN: ${{ secrets.GITHUB_TOKEN }}
        shell: bash
        run: |
          gh release upload "${{ github.event.release.tag_name }}" \
            native/target/arcadedb-*.tar.gz native/target/arcadedb-*.zip native/target/*.sha256 \
            --clobber
```

- [ ] **Step 2: Wire the `-Dnative.static` property in `native/pom.xml`**

Add a property `<native.static>false</native.static>` and conditionally append static build args (Task 7 fills the actual flags). For now, add a nested profile activated by `-Dnative.static=true`.

- [ ] **Step 3: Validate the workflow YAML**

Run: `python3 -c "import yaml,sys; yaml.safe_load(open('.github/workflows/native-image.yml')); print('yaml ok')"`
Expected: `yaml ok`.

- [ ] **Step 4: Dry-run trigger (manual)**

Push the branch, then: `gh workflow run "Native Image" --ref <branch>` and watch `gh run watch`. Expected: the two Linux jobs succeed and smoke passes; macOS/Windows may fail without reddening the run. Iterate on per-OS quirks (e.g. Windows binary name, macOS codesign warnings).

- [ ] **Step 5: Commit**

```bash
git add .github/workflows/native-image.yml native/pom.xml
git commit -m "ci(native): add 5-target native-image matrix with smoke + release assets"
```

---

### Task 7: Static musl build for the Linux/Docker targets

Make the Linux binaries fully static against musl so they run on `scratch`. Mitigate the Netty DNS `getaddrinfo` issue by forcing the JDK resolver.

**Files:**
- Modify: `native/pom.xml` (static profile: `--static --libc=musl`, DNS mitigation)

**Interfaces:**
- Produces: statically-linked `arcadedb-<version>-linux-<arch>` binaries (verifiable with `ldd` reporting "not a dynamic executable") that pass `smoke.sh`.

- [ ] **Step 1: Add the static build args under the `-Dnative.static` profile**

```xml
<profile>
    <id>native-static</id>
    <activation><property><name>native.static</name><value>true</value></property></activation>
    <build>
        <plugins>
            <plugin>
                <groupId>org.graalvm.buildtools</groupId>
                <artifactId>native-maven-plugin</artifactId>
                <configuration>
                    <buildArgs combine.children="append">
                        <buildArg>--static</buildArg>
                        <buildArg>--libc=musl</buildArg>
                    </buildArgs>
                </configuration>
            </plugin>
        </plugins>
    </build>
</profile>
```

- [ ] **Step 2: Force the JDK DNS resolver at runtime (Netty + musl mitigation)**

Netty's native DNS resolver relies on `getaddrinfo`, which is unreliable under static musl. Ensure the server uses the JDK resolver. Add a documented default runtime flag `-Dio.netty.resolver.dns.preferJdkResolver=true` (or the equivalent Netty property in use) to `docs/native-image.md` and to the Docker `ENTRYPOINT` (Task 8). If Netty in ArcadeDB defaults to the JDK resolver already, verify and note it.

- [ ] **Step 3: Build static locally (on a musl-capable host or the Linux CI runner)**

Run: `mvn -B -ntp -Pnative -pl native -am -DskipTests -Dnative.static=true package`
Then: `ldd native/target/arcadedb-*-linux-amd64 || true`
Expected: `not a dynamic executable` (or `ldd` errors because it is static).

- [ ] **Step 4: Smoke the static binary**

Run: `native/src/test/scripts/smoke.sh native/target/arcadedb-*-linux-amd64 -Dorg.jline.terminal.dumb=true -Dio.netty.resolver.dns.preferJdkResolver=true`
Expected: `[smoke] PASS`. In particular confirm the Postgres-wire and HTTP bind work (the DNS-sensitive paths).

- [ ] **Step 5: Commit**

```bash
git add native/pom.xml docs/native-image.md
git commit -m "build(native): static musl profile for Linux binaries with JDK DNS resolver"
```

---

### Task 8: Static-musl Docker images + multi-arch manifest

Ship the two Linux binaries as `FROM scratch` images, build each arch on its native runner, and stitch a multi-arch manifest under `-native` tags.

**Files:**
- Create: `native/src/main/docker/Dockerfile.native`
- Modify: `.github/workflows/native-image.yml` (add a `docker` job that `needs` the two Linux builds)

**Interfaces:**
- Consumes: the static Linux binaries + `package` runtime layout (`config/`, empty `databases/`).
- Produces: `arcadedata/arcadedb:<version>-native` multi-arch manifest (amd64+arm64) plus per-arch tags.

- [ ] **Step 1: Write `native/src/main/docker/Dockerfile.native`**

Apache-2.0 header (comment form), then:

```dockerfile
# syntax=docker/dockerfile:1
FROM scratch

# Build-time inputs staged by the workflow into the build context:
#   ./arcadedb            -> the static musl binary for THIS arch
#   ./config              -> config templates (arcadedb-log.properties, server config)
#   ./ca-certificates.crt -> CA bundle (scratch has none; needed for HTTPS import/MCP)
COPY ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY config /home/arcadedb/config
COPY arcadedb /home/arcadedb/bin/arcadedb

WORKDIR /home/arcadedb

VOLUME [ "/home/arcadedb/databases" ]
VOLUME [ "/home/arcadedb/backups" ]
VOLUME [ "/home/arcadedb/replication" ]
VOLUME [ "/home/arcadedb/log" ]

EXPOSE 2480 2424 5432 6379 27017 50051

ENTRYPOINT ["/home/arcadedb/bin/arcadedb", \
  "-Dorg.jline.terminal.dumb=true", \
  "-Dio.netty.resolver.dns.preferJdkResolver=true", \
  "-Djava.util.logging.config.file=/home/arcadedb/config/arcadedb-log.properties"]
```

Note: `scratch` has no shell and no non-root user database; the static binary runs as the numeric UID set by the workflow (`--user`) at `docker run` time, documented in Task 9. If a non-root UID baked into the image is required, switch the base to `gcr.io/distroless/static-debian12:nonroot` instead of `scratch` (still tiny) - decide during Step 4 based on whether `scratch` is acceptable.

- [ ] **Step 2: Add a `docker` job to `native-image.yml`**

Append (after the `build` job):

```yaml
  docker:
    name: docker-${{ matrix.arch }}
    needs: build
    runs-on: ${{ matrix.runner }}
    strategy:
      fail-fast: false
      matrix:
        include:
          - { runner: ubuntu-latest,    arch: amd64 }
          - { runner: ubuntu-24.04-arm, arch: arm64 }
    steps:
      - uses: actions/checkout@v4
      - name: Download binary artifact
        uses: actions/download-artifact@v4
        with:
          name: arcadedb-native-linux-${{ matrix.arch }}
          path: dl
      - name: Stage build context
        shell: bash
        run: |
          mkdir -p ctx/config
          tar xzf dl/arcadedb-*-linux-${{ matrix.arch }}*.tar.gz -C ctx
          mv ctx/arcadedb-*-linux-${{ matrix.arch }}* ctx/arcadedb
          cp -r package/src/main/config/* ctx/config/
          cp /etc/ssl/certs/ca-certificates.crt ctx/ca-certificates.crt
      - name: Set up Docker Buildx
        uses: docker/setup-buildx-action@v3
      - name: Login to Docker Hub
        uses: docker/login-action@v3
        with:
          username: ${{ secrets.DOCKER_USERNAME }}
          password: ${{ secrets.DOCKER_PASSWORD }}
      - name: Build and push per-arch image
        run: |
          VERSION=$(mvn -q help:evaluate -Dexpression=project.version -DforceStdout)
          docker buildx build --platform linux/${{ matrix.arch }} \
            -f native/src/main/docker/Dockerfile.native \
            -t arcadedata/arcadedb:${VERSION}-native-${{ matrix.arch }} \
            --push ctx
      - name: Smoke the container
        run: |
          VERSION=$(mvn -q help:evaluate -Dexpression=project.version -DforceStdout)
          docker run -d --name a -p 2480:2480 -p 5432:5432 \
            -e ARCADEDB_ROOT_PASSWORD=PlayWithData123! \
            arcadedata/arcadedb:${VERSION}-native-${{ matrix.arch }}
          for i in $(seq 1 30); do curl -fsS http://127.0.0.1:2480/api/v1/ready && break; sleep 2; done

  manifest:
    needs: docker
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: docker/login-action@v3
        with:
          username: ${{ secrets.DOCKER_USERNAME }}
          password: ${{ secrets.DOCKER_PASSWORD }}
      - name: Create multi-arch manifest
        run: |
          VERSION=$(mvn -q help:evaluate -Dexpression=project.version -DforceStdout)
          docker buildx imagetools create -t arcadedata/arcadedb:${VERSION}-native \
            arcadedata/arcadedb:${VERSION}-native-amd64 \
            arcadedata/arcadedb:${VERSION}-native-arm64
```

- [ ] **Step 3: Validate YAML**

Run: `python3 -c "import yaml; yaml.safe_load(open('.github/workflows/native-image.yml')); print('yaml ok')"`
Expected: `yaml ok`.

- [ ] **Step 4: Dry-run against a throwaway tag/repo**

Temporarily push to a personal/test Docker namespace (or use `--load` instead of `--push` and smoke locally) to avoid publishing to the real `arcadedata/arcadedb` before review. Confirm both arch images boot and `curl /api/v1/ready` returns success, and `docker buildx imagetools inspect` shows a 2-platform manifest. Decide `scratch` vs `distroless/static:nonroot` here.

- [ ] **Step 5: Commit**

```bash
git add native/src/main/docker/Dockerfile.native .github/workflows/native-image.yml
git commit -m "ci(native): static scratch Docker images + multi-arch native manifest"
```

---

### Task 9: Documentation

Document how to build locally, the JS-scripting outcome, the SIMD-fallback note, and how to run the native Docker image.

**Files:**
- Modify: `docs/native-image.md` (expand the stub from Task 5)

**Interfaces:**
- Produces: a complete `docs/native-image.md`.

- [ ] **Step 1: Expand `docs/native-image.md`**

Include: prerequisites (GraalVM for JDK 21 Community, musl-tools for static); `mvn -Pnative -pl native -am -DskipTests package`; the target list and which are best-effort; the polyglot-JS outcome from Task 5; the JVector SIMD scalar-fallback perf note; running the container (`docker run --user 1000:1000 -p 2480:2480 -v ...:/home/arcadedb/databases arcadedata/arcadedb:<ver>-native`); and the two runtime defaults (`-Dorg.jline.terminal.dumb=true`, `-Dio.netty.resolver.dns.preferJdkResolver=true`).

- [ ] **Step 2: Link from the main docs index if one exists**

Run: `grep -rl "## Docker\|distribution" docs/ README.md 2>/dev/null | head`
Add a one-line pointer to `docs/native-image.md` from the most relevant existing index/README section. If none fits, skip.

- [ ] **Step 3: Commit**

```bash
git add docs/native-image.md README.md
git commit -m "docs(native): document native-image build, limitations, and container usage"
```

---

## Self-Review

**Spec coverage:**
- Experimental add-on, non-gating, `-native` tags → Task 1 (profile out of reactor), Task 6/8 (separate workflow, `-native` tags). ✓
- Feature set incl./excl. (Gremlin + tracing out; all wire protocols in) → Task 1 deps. ✓
- Polyglot-JS time-boxed spike + fallback → Task 5. ✓
- Reachability metadata via tracing agent + repo, not hand-maintained → Task 3. ✓
- Known blockers (Undertow/wildfly, Netty, Lucene, gRPC/graphql, JLine, JVector) → Task 4 steps 2-6. ✓
- Phase-1 smoke gate (Studio/SQL/Cypher/Postgres) → Task 2, reused Tasks 4-8. ✓
- 5-target matrix, cannot cross-compile, Linux required / others best-effort, release assets + SHA256 → Task 6. ✓
- Static musl + Netty DNS mitigation → Task 7. ✓
- `FROM scratch`, per-arch native build, imagetools manifest (no QEMU), CA bundle → Task 8. ✓
- Docs (build, JS limitation, SIMD note) → Task 9. ✓

**Placeholder scan:** the JS UDF exercise (Task 5 Step 1) and the exact readiness endpoint (Task 2 Step 3) are flagged for the implementer to confirm against real code - these are verification points, not unresolved design. All build args, YAML, Dockerfile, and script bodies are concrete. `scratch`-vs-`distroless` is an explicit decision gate (Task 8 Step 4), not a placeholder.

**Type/name consistency:** `smoke.sh`, `exercise.sh`, `trace.sh` referenced consistently; artifact naming `arcadedb-<version>-<os_name>-<arch>` consistent across Tasks 1/6/8; property `native.static` consistent Tasks 6/7; tag scheme `arcadedata/arcadedb:<version>-native[-<arch>]` consistent Tasks 8. ✓

**Known domain caveat:** native-image bring-up (Task 4) is inherently iterative; its "test" is the build succeeding plus `smoke.sh`, not unit tests. This is correct for the domain - the smoke script is the regression gate.
