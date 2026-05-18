# Test scaffolding consolidation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Collapse the duplicate test scaffolding (`TestServerHelper`, `StaticBaseServerTest`, `BaseGraphServerTest`, `WebSocketClientHelper`) into a single canonical home in `server/src/test/java/com/arcadedb/server/`, then delete the `arcadedb-test-utils` module entirely.

**Architecture:** Reconcile the server-package copies first (port the better behaviors from the test-utils copies into the server copies), then switch all 10 downstream modules from `arcadedb-test-utils` to `arcadedb-server:test-jar:test`, then delete `test-utils/`. Two-phase rollout (reconcile, then migrate-and-delete) keeps each commit independently reviewable.

**Tech Stack:** Java 21, Maven multi-module (parent pom + 30 modules), JUnit 5, AssertJ. The server module already produces a `tests` jar consumed by `ha-raft` and `grpcw` today.

---

## File Structure

**Reconciled canonical helpers (modified in place):**
- `server/src/test/java/com/arcadedb/server/BaseGraphServerTest.java` — gets try-with-resources for PrintWriter, structured async-callback logging, keeps existing error-body capture
- `server/src/test/java/com/arcadedb/server/ws/WebSocketClientHelper.java` — gets interrupt-flag restoration in InterruptedException catch
- `server/src/test/java/com/arcadedb/server/TestServerHelper.java` — improved `fail()` message, `final class` + private constructor
- `server/src/test/java/com/arcadedb/server/StaticBaseServerTest.java` — explicit `java.util.logging.Level` import (not wildcard)

**Pom files modified:**
- `pom.xml` (root) — remove `<module>test-utils</module>`
- `server/pom.xml` — remove the misleading `default-test-jar phase=none` override (test-jar IS being produced today despite this)
- 10 downstream poms: `bolt/pom.xml`, `console/pom.xml`, `gremlin/pom.xml`, `graphql/pom.xml`, `grpc-client/pom.xml`, `grpcw/pom.xml`, `metrics/pom.xml`, `mongodbw/pom.xml`, `postgresw/pom.xml`, `redisw/pom.xml`

**Files deleted (the entire test-utils module):**
- `test-utils/pom.xml`
- `test-utils/src/main/java/com/arcadedb/test/TestServerHelper.java`
- `test-utils/src/main/java/com/arcadedb/test/StaticBaseServerTest.java`
- `test-utils/src/main/java/com/arcadedb/test/BaseGraphServerTest.java`
- `test-utils/src/main/java/com/arcadedb/test/WebSocketClientHelper.java`
- `test-utils/` directory

**Java imports rewritten in ~50 files** across `bolt`, `console`, `gremlin`, `grpc-client`, `grpcw`, `metrics`, `mongodbw`, `postgresw`, `redisw` (graphql declares the dep but has zero imports — no Java change there).

---

### Task 1: Reconcile `BaseGraphServerTest` — adopt test-utils improvements

**Files:**
- Modify: `server/src/test/java/com/arcadedb/server/BaseGraphServerTest.java`

The current diff between the test-utils copy and the server copy is exactly three substantive hunks. Apply hunks 1 and 2 (improve the server copy); leave hunk 3 alone (server copy is already better with error-body capture).

- [ ] **Step 1: Update the stale Javadoc on the class**

In `server/src/test/java/com/arcadedb/server/BaseGraphServerTest.java`, around line 67, replace:

```java
/**
 * This class has been copied under Console project to avoid complex dependencies.
 */
```

with:

```java
/**
 * Base class for graph database multi-server and HA testing with graph schema population and verification utilities.
 */
```

- [ ] **Step 2: Wrap the PrintWriter in try-with-resources**

Around line 274-276, replace:

```java
    final PrintWriter output = new PrintWriter(new BufferedOutputStream(os));
    new Exception().printStackTrace(output);
    output.flush();
```

with:

```java
    try (final PrintWriter output = new PrintWriter(new BufferedOutputStream(os))) {
      new Exception().printStackTrace(output);
      output.flush();
    }
```

- [ ] **Step 3: Replace `e.printStackTrace()` in the async callback with structured logging**

Around line 501, replace:

```java
          e.printStackTrace();
```

with:

```java
          LogManager.instance().log(BaseGraphServerTest.this, Level.SEVERE, "Error in asynchronous callback", e);
```

- [ ] **Step 4: Compile the server module**

Run: `mvn -pl server -am test-compile -q`
Expected: BUILD SUCCESS, no compile errors.

- [ ] **Step 5: Run a sentinel test that exercises the modified paths**

Run: `mvn -pl server -am test -Dtest=AsyncInsertTest`
Expected: BUILD SUCCESS, tests pass.

- [ ] **Step 6: Commit**

```bash
git add server/src/test/java/com/arcadedb/server/BaseGraphServerTest.java
git commit -m "test: reconcile BaseGraphServerTest with test-utils improvements

Adopt try-with-resources for PrintWriter, structured async-callback
logging, and refreshed Javadoc from the test-utils copy. Server-copy
error-body capture in initialConnection retained."
```

---

### Task 2: Reconcile `WebSocketClientHelper` — restore interrupt flag

**Files:**
- Modify: `server/src/test/java/com/arcadedb/server/ws/WebSocketClientHelper.java`

The server copy swallows `InterruptedException` without restoring the interrupt flag. The test-utils copy does it correctly. Port the fix.

- [ ] **Step 1: Locate the InterruptedException catch and add interrupt restoration**

In `server/src/test/java/com/arcadedb/server/ws/WebSocketClientHelper.java`, around line 129, find:

```java
    } catch (final InterruptedException ignored) {
```

Replace with:

```java
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
```

- [ ] **Step 2: Compile the server module**

Run: `mvn -pl server -am test-compile -q`
Expected: BUILD SUCCESS.

- [ ] **Step 3: Run the WebSocket sentinel test**

Run: `mvn -pl server -am test -Dtest=WebSocketEventBusIT -DskipITs=false`
Expected: BUILD SUCCESS, test passes.

- [ ] **Step 4: Commit**

```bash
git add server/src/test/java/com/arcadedb/server/ws/WebSocketClientHelper.java
git commit -m "test: restore interrupt flag in WebSocketClientHelper

The InterruptedException catch was swallowing the interrupt status,
which can cause downstream blocking calls to hang. Mirror the
test-utils copy by re-asserting the interrupt flag."
```

---

### Task 3: Polish `TestServerHelper` and `StaticBaseServerTest`

**Files:**
- Modify: `server/src/test/java/com/arcadedb/server/TestServerHelper.java`
- Modify: `server/src/test/java/com/arcadedb/server/StaticBaseServerTest.java`

Adopt the test-utils versions' minor cleanups: informative `fail()` message, utility-class idiom, and explicit logging import.

- [ ] **Step 1: Update `TestServerHelper.expectException` fail message**

In `server/src/test/java/com/arcadedb/server/TestServerHelper.java`, around line 112, replace:

```java
    try {
      callback.call();
      fail("");
```

with:

```java
    try {
      callback.call();
      fail("Expected exception of type " + expectedException.getName() + " but none was thrown");
```

- [ ] **Step 2: Convert `TestServerHelper` to a utility class**

In `server/src/test/java/com/arcadedb/server/TestServerHelper.java`, around line 40-43, replace:

```java
/**
 * Executes all the tests while the server is up and running.
 */
public abstract class TestServerHelper {

  public static ArcadeDBServer[] startServers(final int totalServers,
```

with:

```java
/**
 * Static utility methods for test server lifecycle and database management.
 */
public final class TestServerHelper {

  private TestServerHelper() {
    // Utility class - no instances
  }

  public static ArcadeDBServer[] startServers(final int totalServers,
```

- [ ] **Step 3: Replace wildcard logging import in `StaticBaseServerTest`**

In `server/src/test/java/com/arcadedb/server/StaticBaseServerTest.java`, around line 26, replace:

```java
import java.util.logging.*;
```

with:

```java
import java.util.logging.Level;
```

- [ ] **Step 4: Compile the server module**

Run: `mvn -pl server -am test-compile -q`
Expected: BUILD SUCCESS.

- [ ] **Step 5: Run a sentinel test**

Run: `mvn -pl server -am test -Dtest=ServerSecurityIT -DskipITs=false`
Expected: BUILD SUCCESS, tests pass. This test uses `TestServerHelper.expectException` so the fail-message change is exercised in passing-path assertion text only (test still passes because no exception flow hits the assertion).

- [ ] **Step 6: Commit**

```bash
git add server/src/test/java/com/arcadedb/server/TestServerHelper.java server/src/test/java/com/arcadedb/server/StaticBaseServerTest.java
git commit -m "test: polish TestServerHelper and StaticBaseServerTest

- TestServerHelper: informative fail() message and utility-class form
  (final + private ctor)
- StaticBaseServerTest: explicit Level import instead of wildcard"
```

---

### Task 4: Update downstream module poms — switch to `arcadedb-server:test-jar:test`

**Files:**
- Modify: `bolt/pom.xml`
- Modify: `console/pom.xml`
- Modify: `gremlin/pom.xml`
- Modify: `graphql/pom.xml`
- Modify: `grpc-client/pom.xml`
- Modify: `grpcw/pom.xml`
- Modify: `metrics/pom.xml`
- Modify: `mongodbw/pom.xml`
- Modify: `postgresw/pom.xml`
- Modify: `redisw/pom.xml`

Two cases:

**Case A** — module does NOT already declare `arcadedb-server:test-jar:test`. Replace the `arcadedb-test-utils` block with a test-jar block.

**Case B** — module already declares `arcadedb-server:test-jar:test` (currently `grpcw` only). Remove the `arcadedb-test-utils` block entirely.

Verify per-module before editing: `grep -A4 "arcadedb-server" <module>/pom.xml | grep -B1 test-jar`. If present → Case B. If not → Case A.

- [ ] **Step 1: For each of the 9 Case-A modules — replace the test-utils block**

For each pom in `bolt`, `console`, `gremlin`, `graphql`, `grpc-client`, `metrics`, `mongodbw`, `postgresw`, `redisw`, find:

```xml
        <dependency>
            <groupId>com.arcadedb</groupId>
            <artifactId>arcadedb-test-utils</artifactId>
            <version>${project.parent.version}</version>
            <scope>test</scope>
        </dependency>
```

Replace with:

```xml
        <dependency>
            <groupId>com.arcadedb</groupId>
            <artifactId>arcadedb-server</artifactId>
            <version>${project.parent.version}</version>
            <scope>test</scope>
            <type>test-jar</type>
        </dependency>
```

Note: indentation in some poms uses 2 spaces, others 4. Preserve the existing indent in each file.

- [ ] **Step 2: For grpcw (Case B) — delete the test-utils block without replacement**

In `grpcw/pom.xml`, find and delete:

```xml
        <dependency>
            <groupId>com.arcadedb</groupId>
            <artifactId>arcadedb-test-utils</artifactId>
            <version>${project.parent.version}</version>
            <scope>test</scope>
        </dependency>
```

grpcw already has `arcadedb-server:test-jar:test` so no replacement is needed.

- [ ] **Step 3: Verify no pom still references test-utils**

Run: `grep -l "arcadedb-test-utils" bolt/pom.xml console/pom.xml gremlin/pom.xml graphql/pom.xml grpc-client/pom.xml grpcw/pom.xml metrics/pom.xml mongodbw/pom.xml postgresw/pom.xml redisw/pom.xml`
Expected: empty output. (test-utils/pom.xml itself still references it, that's fine — deleted in Task 6.)

- [ ] **Step 4: Build all downstream modules' main code (NOT tests yet — imports haven't been migrated)**

Run: `mvn -pl bolt,console,gremlin,graphql,grpc-client,grpcw,metrics,mongodbw,postgresw,redisw -am install -DskipTests -q`
Expected: BUILD SUCCESS. Main sources don't reference the helpers, so this should pass even before Task 5 import migration.

- [ ] **Step 5: Commit**

```bash
git add bolt/pom.xml console/pom.xml gremlin/pom.xml graphql/pom.xml grpc-client/pom.xml grpcw/pom.xml metrics/pom.xml mongodbw/pom.xml postgresw/pom.xml redisw/pom.xml
git commit -m "build: switch test deps from arcadedb-test-utils to server test-jar

Step 1 of 2 for the test-utils consolidation. Tests in these modules
will still fail to compile until imports are migrated in the next
commit."
```

---

### Task 5: Migrate Java imports — `com.arcadedb.test.*` → `com.arcadedb.server.*`

**Files:** ~50 test files across the 9 modules. Affected files identified by:

```
grep -rln 'com\.arcadedb\.test\.\(BaseGraphServerTest\|StaticBaseServerTest\|TestServerHelper\)' --include='*.java' bolt console gremlin grpc-client grpcw metrics mongodbw postgresw redisw
```

`WebSocketClientHelper` in the `com.arcadedb.test` package has zero consumers (verified by `grep -rln "com\.arcadedb\.test\.WebSocketClientHelper" --include="*.java"`) so no import migration is needed for it.

- [ ] **Step 1: Run a one-shot find+sed pass to rewrite the three import lines**

The sed command (mac/BSD-compatible — uses `-i ''`):

```bash
find bolt console gremlin grpc-client grpcw metrics mongodbw postgresw redisw \
  -name '*.java' -type f \
  -exec grep -l 'com\.arcadedb\.test\.\(BaseGraphServerTest\|StaticBaseServerTest\|TestServerHelper\)' {} \; \
  | xargs sed -i '' \
      -e 's|import com\.arcadedb\.test\.BaseGraphServerTest;|import com.arcadedb.server.BaseGraphServerTest;|' \
      -e 's|import com\.arcadedb\.test\.StaticBaseServerTest;|import com.arcadedb.server.StaticBaseServerTest;|' \
      -e 's|import com\.arcadedb\.test\.TestServerHelper;|import com.arcadedb.server.TestServerHelper;|'
```

(If using GNU sed, use `sed -i` without the `''`.)

- [ ] **Step 2: Verify the sed pass left no stragglers**

Run: `grep -rln 'com\.arcadedb\.test\.' --include='*.java' bolt console gremlin grpc-client grpcw metrics mongodbw postgresw redisw`
Expected: empty output. Anything found is a wildcard import, a `com.arcadedb.test.X.Y` nested ref, or a comment — must be handled manually before proceeding.

- [ ] **Step 3: Build all downstream modules WITH tests compiled**

Run: `mvn -pl bolt,console,gremlin,graphql,grpc-client,grpcw,metrics,mongodbw,postgresw,redisw -am test-compile -q`
Expected: BUILD SUCCESS.

If compile fails on missing transitive deps from test-utils (likely candidates: `awaitility`, `testcontainers`, `json-path`, `slf4j-jdk14`, `undertow-core`), add explicit test-scope deps to the failing module's pom. Re-run until compile passes.

- [ ] **Step 4: Run one sentinel test per migrated module (smoke check)**

Run each separately to confirm the helpers resolve at runtime:

```bash
mvn -pl bolt      -am test -Dtest=BoltProtocolIT          -DskipITs=false -DfailIfNoTests=false -q
mvn -pl console   -am test -Dtest=ConsoleTest             -DskipITs=false -DfailIfNoTests=false -q
mvn -pl gremlin   -am test -Dtest=GremlinServerTest       -DskipITs=false -DfailIfNoTests=false -q
mvn -pl metrics   -am test -Dtest=PrometheusMetricsPluginAuthenticatedTest -DskipITs=false -DfailIfNoTests=false -q
mvn -pl postgresw -am test -Dtest=PostgresProtocolIT      -DskipITs=false -DfailIfNoTests=false -q
mvn -pl mongodbw  -am test -Dtest=MongoDBServerTest       -DskipITs=false -DfailIfNoTests=false -q
mvn -pl redisw    -am test -Dtest=RedisWTest              -DskipITs=false -DfailIfNoTests=false -q
mvn -pl grpcw     -am test -Dtest=GrpcServerIT            -DskipITs=false -DfailIfNoTests=false -q
mvn -pl grpc-client -am test -Dtest=RemoteGrpcServerIT    -DskipITs=false -DfailIfNoTests=false -q
```

Expected: each runs and passes (or fails for reasons unrelated to imports — read the error before proceeding).

graphql has no Java references and skips this step.

- [ ] **Step 5: Commit**

```bash
git add bolt/ console/ gremlin/ grpc-client/ grpcw/ metrics/ mongodbw/ postgresw/ redisw/
git commit -m "test: migrate imports from com.arcadedb.test.* to com.arcadedb.server.*

Step 2 of 2 for the test-utils consolidation. All test classes now
import the canonical helpers from server's test-jar."
```

---

### Task 6: Delete the `arcadedb-test-utils` module and clean up

**Files:**
- Delete: `test-utils/` (entire directory)
- Modify: `pom.xml` (root)
- Modify: `server/pom.xml`

- [ ] **Step 1: Verify the module is no longer referenced anywhere**

Run: `grep -rn 'arcadedb-test-utils\|com\.arcadedb\.test\.' --include='*.java' --include='*.xml' --include='*.kt' --include='*.groovy' .`
Expected output: only matches inside `test-utils/` itself (which is about to be deleted), or the spec/plan docs.

If non-test-utils source files match, fix them before continuing.

- [ ] **Step 2: Delete the test-utils directory**

```bash
rm -rf test-utils/
```

- [ ] **Step 3: Remove `<module>test-utils</module>` from the root pom**

In `/Users/frank/projects/arcade/arcadedb/pom.xml`, find the `<modules>` block and remove the line `<module>test-utils</module>`.

Verify with: `grep test-utils pom.xml`
Expected: no matches.

- [ ] **Step 4: Remove the misleading test-jar override from `server/pom.xml`**

In `server/pom.xml`, remove the entire block:

```xml
  <build>
    <plugins>
      <!-- Disable test-jar packaging for server module -->
      <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-jar-plugin</artifactId>
        <executions>
          <execution>
            <id>default-test-jar</id>
            <phase>none</phase>
          </execution>
        </executions>
      </plugin>
    </plugins>
  </build>
```

This override does NOT actually disable test-jar emission today (the test-jar is produced via the parent pluginManagement) and is now actively misleading: the consolidation depends on the test-jar being produced. Removing the block restores intent.

If `<build>` had any other content, keep that and only delete the misleading plugin block.

- [ ] **Step 5: Full reactor build**

Run: `mvn clean install -DskipTests -B`
Expected: BUILD SUCCESS for all remaining modules. If any module fails for a missing dep that came transitively from `arcadedb-test-utils`, add the explicit dep to that module's pom and re-run.

- [ ] **Step 6: Confirm server's test-jar still attaches**

Run: `ls -la ~/.m2/repository/com/arcadedb/arcadedb-server/26.6.1-SNAPSHOT/ | grep tests.jar`
Expected: `arcadedb-server-26.6.1-SNAPSHOT-tests.jar` is present with a fresh timestamp.

- [ ] **Step 7: Commit**

```bash
git add -A
git commit -m "build: delete arcadedb-test-utils module after consolidation

The four helpers (TestServerHelper, StaticBaseServerTest,
BaseGraphServerTest, WebSocketClientHelper) now live solely in
server's test-jar. Removes the structural duplication that caused
the recent 403 test failures (test-utils' deleteDatabaseFolders had
target/config cleanup; server's did not).

Also drops the misleading maven-jar-plugin phase=none override in
server/pom.xml — it did not actually disable test-jar emission
and would break the test-jar consumers if it ever started working."
```

---

### Task 7: Final verification — full sentinel IT pass

**Files:** None modified.

Run the integration tests called out in the design's verification plan. These exercise both the consolidated helpers and the original 403 failure modes.

- [ ] **Step 1: Run the two 403 sentinel tests**

```bash
mvn -pl ha-raft -am test -Dtest=RaftHTTP2ServersCreateReplicatedDatabaseIT,FollowerSessionTokenQueryIT -DskipITs=false -Dsurefire.failIfNoSpecifiedTests=false
```

Expected: BUILD SUCCESS, both tests pass. These were the two tests that exposed the duplicated `target/config/` cleanup bug.

- [ ] **Step 2: Run the BaseGraphServerTest reconciliation sentinels**

```bash
mvn -pl ha-raft -am test -Dtest=RaftHARandomCrashIT -DskipITs=false -Dsurefire.failIfNoSpecifiedTests=false
mvn -pl bolt    -am test -Dtest=BoltTlsIT          -DskipITs=false -Dsurefire.failIfNoSpecifiedTests=false
```

Expected: BUILD SUCCESS for both. These tests exercise the async-callback error path and the server-connect error-body path that were touched in Task 1.

- [ ] **Step 3: Run one IT per downstream module group**

```bash
mvn -pl bolt,console,gremlin,grpc-client,grpcw,metrics,mongodbw,postgresw,redisw -am test \
    -Dtest=BoltProtocolIT,ConsoleTest,GremlinServerTest,PrometheusMetricsPluginAuthenticatedTest,PostgresProtocolIT,MongoDBServerTest,RedisWTest,GrpcServerIT,RemoteGrpcServerIT \
    -DskipITs=false -Dsurefire.failIfNoSpecifiedTests=false
```

Expected: BUILD SUCCESS, all listed tests pass.

- [ ] **Step 4: Report status to user**

Summarize: number of commits made, total files changed, sentinel IT results, any new explicit test-scope deps that had to be added in Task 5 step 3. No further commits.

---

## Notes for the implementer

- **Branch:** All work belongs on the existing `fix/failing-it-tests-2` branch unless a fresh feature branch is requested.
- **Test-jar reality:** The server module ALREADY emits a test-jar today (verified by inspecting `~/.m2/repository/com/arcadedb/arcadedb-server/26.6.1-SNAPSHOT/arcadedb-server-26.6.1-SNAPSHOT-tests.jar` timestamp). The `phase=none` plugin override in `server/pom.xml` is functionally inert. Removing it in Task 6 step 4 is cleanup, not enablement.
- **WebSocketClientHelper duplicate:** The `com.arcadedb.test.WebSocketClientHelper` copy is unreferenced (verified). It dies with the test-utils module in Task 6. The server copy (`com.arcadedb.server.ws.WebSocketClientHelper`) is the only one that matters — Task 2 fixes its interrupt handling.
- **graphql module:** Declares `arcadedb-test-utils` as a test dep but no Java code references the classes. Task 4 step 1 still updates its pom for consistency; Task 5 has no work in graphql.
- **If a downstream module needs a transitive dep** previously inherited from test-utils (awaitility, testcontainers, json-path, slf4j-jdk14, undertow-core), add it as `<scope>test</scope>` to that module's pom. These are likely candidates but may not all be needed — surface failures via `mvn test-compile`, fix as they appear.
- **Rollback:** Each task is one commit; revert in reverse order to undo partial work. A single full revert of all 6 commits restores the previous duplicate-helper state.
