# Test scaffolding consolidation — collapse `arcadedb-test-utils` into `arcadedb-server` test-jar

**Status:** drafting
**Date:** 2026-05-18
**Author:** robfrank

## Problem

Test scaffolding for server/HA tests is duplicated across two locations:

- `test-utils/src/main/java/com/arcadedb/test/` (package `com.arcadedb.test`) — the `arcadedb-test-utils` module
- `server/src/test/java/com/arcadedb/server/` (package `com.arcadedb.server`) — inside the server module's test sources

Four files are duplicated: `TestServerHelper`, `StaticBaseServerTest`, `BaseGraphServerTest`, `WebSocketClientHelper` (the last lives under `com.arcadedb.server.ws` in the server copy).

The duplication is structural, not accidental: `arcadedb-test-utils` declares a compile-scope dependency on `arcadedb-server`, so the server module cannot depend on `arcadedb-test-utils` without creating a Maven reactor cycle. A comment in `server/src/test/java/com/arcadedb/server/BaseGraphServerTest.java` documents the workaround verbatim:

> "This class has been copied under Console project to avoid complex dependencies."

The duplication has caused at least two production-of-test-suite bugs to date:

1. `FollowerSessionTokenQueryIT` failing with HTTP 403 because `target/config/server-users.jsonl` was not cleaned between runs. Fixed in `com.arcadedb.test.TestServerHelper` only.
2. `RaftHTTP2ServersCreateReplicatedDatabaseIT` failing with the same 403 today — same root cause, but in the un-fixed `com.arcadedb.server.TestServerHelper`. Fixed in 2026-05-18 commit by mirroring the cleanup.

The two `BaseGraphServerTest` copies have also drifted in non-trivial ways: try-with-resources usage, structured async error logging, and error-body capture on server-connect failures all exist in the `test-utils` version but not in the `server` copy.

## Goals

- Single source of truth for the four test helpers.
- No Maven reactor cycle.
- No behavioral regression in any existing test.
- Forward-port the better-quality `BaseGraphServerTest` improvements to all consumers.

## Non-goals

- Reorganizing test tags (`@Tag("benchmark")`, `@Tag("slow")`).
- Refactoring the helpers' APIs.
- Introducing new test dependencies.
- Changing any `src/main/java` (production) code.

## Approach

Delete the `arcadedb-test-utils` module. Keep the four helpers in `server/src/test/java/com/arcadedb/server/` (where most consumers already import them from). Have downstream modules depend on `arcadedb-server:test-jar:test` instead of `arcadedb-test-utils`.

The test-jar dependency is a test-scope dep on an artifact that is already a test artifact of the server module, so it does not introduce a compile cycle.

### Why this approach

| Option | Verdict |
|---|---|
| Consolidate into `arcadedb-server` test-jar | **Chosen.** No new module, breaks the cycle, helpers live next to the code they exercise. |
| Create a new `arcadedb-server-test-fixtures` module | Rejected. Adds a module that does the same job as today's test-utils, just with the cycle broken. More machinery for no extra benefit. |
| Keep `arcadedb-test-utils`, delete server copies | Rejected. Requires breaking the Maven cycle (e.g., splitting `ArcadeDBServer` into an api/impl pair). Multi-week refactor, touches public API. |

## Target layout

### Files kept (the four canonical helpers)

| Path | Source |
|---|---|
| `server/src/test/java/com/arcadedb/server/TestServerHelper.java` | merge of both copies; already has `target/config/` cleanup; adopt test-utils' improved `fail()` message and the `final class` + private-ctor utility-class form |
| `server/src/test/java/com/arcadedb/server/StaticBaseServerTest.java` | already canonical here; only diff is the wildcard `java.util.logging.*` import — keep the explicit import |
| `server/src/test/java/com/arcadedb/server/BaseGraphServerTest.java` | rewrite from the test-utils version; forward-port any server-only logic discovered during hunk-by-hunk diff |
| `server/src/test/java/com/arcadedb/server/ws/WebSocketClientHelper.java` | already canonical here |

### Files deleted

- `test-utils/src/main/java/com/arcadedb/test/TestServerHelper.java`
- `test-utils/src/main/java/com/arcadedb/test/StaticBaseServerTest.java`
- `test-utils/src/main/java/com/arcadedb/test/BaseGraphServerTest.java`
- `test-utils/src/main/java/com/arcadedb/test/WebSocketClientHelper.java`
- The entire `test-utils/` module directory.

### Package

`com.arcadedb.server` for `TestServerHelper`, `StaticBaseServerTest`, `BaseGraphServerTest`; `com.arcadedb.server.ws` for `WebSocketClientHelper`.

This minimizes downstream churn: the 60 files already on `com.arcadedb.server.*` are unchanged; the 50 files on `com.arcadedb.test.*` move to `com.arcadedb.server.*`.

## POM changes

### Root `pom.xml`

Remove the `<module>test-utils</module>` entry from `<modules>`.

### `server/pom.xml`

Verify the `maven-jar-plugin` test-jar goal is wired. The `ha-raft` module already consumes `arcadedb-server:test-jar:test` today, so the test-jar must already be produced — this is a sanity-check only. If absent, add:

```xml
<plugin>
  <artifactId>maven-jar-plugin</artifactId>
  <executions>
    <execution>
      <goals><goal>test-jar</goal></goals>
    </execution>
  </executions>
</plugin>
```

### Downstream module poms

Ten modules declare a test-scope dependency on `arcadedb-test-utils`: `bolt`, `console`, `gremlin`, `graphql`, `grpc-client`, `grpcw`, `metrics`, `mongodbw`, `postgresw`, `redisw`.

`graphql` declares the dep but no Java code references the classes — drop the dep, no import migration needed for that module. The other nine modules need both pom and import changes.

In each, replace:

```xml
<dependency>
  <groupId>com.arcadedb</groupId>
  <artifactId>arcadedb-test-utils</artifactId>
  <version>${project.parent.version}</version>
  <scope>test</scope>
</dependency>
```

with:

```xml
<dependency>
  <groupId>com.arcadedb</groupId>
  <artifactId>arcadedb-server</artifactId>
  <version>${project.parent.version}</version>
  <scope>test</scope>
  <type>test-jar</type>
</dependency>
```

If the module already declares `arcadedb-server:test-jar:test` for other reasons, the change is just to remove the `arcadedb-test-utils` block.

### Transitive dependency audit

`arcadedb-test-utils` declared `awaitility`, `testcontainers`, `slf4j-jdk14`, `undertow-core`, `json-path`. `arcadedb-server`'s test-jar does not redistribute these transitively. Any downstream module that used a `test-utils`-transitive dep directly will fail to compile. Mitigation: surface failures via `mvn clean install -DskipTests` and add explicit test-scope deps where needed.

## Java import migration

Across 50 files in 10 modules:

```java
- import com.arcadedb.test.BaseGraphServerTest;
- import com.arcadedb.test.StaticBaseServerTest;
- import com.arcadedb.test.TestServerHelper;
- import com.arcadedb.test.WebSocketClientHelper;
+ import com.arcadedb.server.BaseGraphServerTest;
+ import com.arcadedb.server.StaticBaseServerTest;
+ import com.arcadedb.server.TestServerHelper;
+ import com.arcadedb.server.ws.WebSocketClientHelper;
```

Note `WebSocketClientHelper` moves into the `ws` sub-package — its import is a different rewrite from the other three.

A scripted `sed` over the affected files handles the bulk; the `WebSocketClientHelper` import and any rare `com.arcadedb.test.*` star imports need manual review.

## Verification

In order:

1. `mvn clean install -DskipTests -B` from the repo root. Must succeed with `test-utils` removed and 10 downstream modules switched. Catches missing imports, pom misses, and reactor cycles.
2. `mvn -pl server,ha-raft -am test` and `mvn -pl bolt,console,gremlin,graphql,grpc-client,grpcw,metrics,mongodbw,postgresw,redisw -am test`. Catches test-only compile breakage and any missing transitive test deps.
3. Sentinel integration tests:
   - `RaftHTTP2ServersCreateReplicatedDatabaseIT` — the test fixed today.
   - `FollowerSessionTokenQueryIT` — the original 403 case.
   - One representative IT per downstream module: `BoltProtocolIT`, `GremlinServerTest`, `ConsoleTest`, `PrometheusMetricsPluginAuthenticatedTest`, `PostgresWJdbcIT`.
   Run with `-DskipITs=false`.
4. `RaftHARandomCrashIT` and `BoltTlsIT` to exercise the `BaseGraphServerTest` async error and connect-error-body paths after reconciliation.

## Risks

| Risk | Likelihood | Mitigation |
|---|---|---|
| BaseGraphServerTest reconciliation silently drops a server-only fix | Medium | Hunk-by-hunk diff before overwriting; commit the reconciled `BaseGraphServerTest` in a standalone commit for reviewability. |
| Server's test-jar emission not configured | Low | Already consumed by ha-raft; sanity-check the maven-jar-plugin block during plan execution. |
| Downstream module relied on a `test-utils`-transitive dep (awaitility, testcontainers, json-path, undertow-core, slf4j-jdk14) | Medium | Mvn compile failure surfaces these; add explicit test-scope deps as discovered. |
| Hidden third-party consumer of `com.arcadedb.test.*` | Low | These are test helpers, not API. Accept the break, document in PR description. |
| CI reactor-order differs from local | Low | CI runs the full reactor; pass criteria already covers full install. Run CI on the branch before merge. |

## Rollback

Single `git revert` of the consolidation commit restores `arcadedb-test-utils`, both sets of duplicates, and all downstream pom entries. No data, schema, or config impact.

## Out of scope (followups, if any)

- Auditing other test helpers under `engine/src/test/java` for similar duplication potential.
- Consolidating `gremlin/local-database-factory` test scaffolding (separate concern).
