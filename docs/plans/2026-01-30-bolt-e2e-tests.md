
# Bolt Protocol E2E Tests Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add end-to-end tests for the Bolt protocol in the e2e module, using TestContainers and the Neo4j Java driver.

**Architecture:** Extend `ArcadeContainerTemplate` to expose port 7687 and enable `BoltProtocolPlugin`, then create `RemoteBoltDatabaseTest` that uses the Neo4j Java driver to execute Cypher queries against the containerized ArcadeDB instance.

**Tech Stack:** Neo4j Java Driver 5.27.0, JUnit 5, TestContainers, AssertJ

**Issue:** https://github.com/ArcadeData/arcadedb/issues/3279

---

## Task 1: Add Neo4j Driver Dependency to E2E Module

**Files:**
- Modify: `e2e/pom.xml:37` (properties section)
- Modify: `e2e/pom.xml:108` (dependencies section)

**Step 1: Add version property**

Add to `<properties>` section after line 36:

```xml
<neo4j-driver.version>5.27.0</neo4j-driver.version>
```

**Step 2: Add dependency**

Add to `<dependencies>` section before the closing `</dependencies>` tag:

```xml
<dependency>
    <groupId>org.neo4j.driver</groupId>
    <artifactId>neo4j-java-driver</artifactId>
    <version>${neo4j-driver.version}</version>
    <scope>test</scope>
</dependency>
```

**Step 3: Verify dependency resolution**

Run: `cd /Users/frank/projects/arcade/worktrees/bolt-e2e && mvn dependency:resolve -pl e2e -q`
Expected: No errors

**Step 4: Commit**

```bash
git add e2e/pom.xml
git commit -m "feat(e2e): add Neo4j Java driver dependency for Bolt tests

Adds org.neo4j.driver:neo4j-java-driver:5.27.0 to e2e module for
testing Bolt protocol with TestContainers.

Refs #3279

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>"
```

---

## Task 2: Update ArcadeContainerTemplate for Bolt Protocol

**Files:**
- Modify: `e2e/src/test/java/com/arcadedb/e2e/ArcadeContainerTemplate.java`

**Step 1: Add port 7687 to exposed ports**

Change line 31 from:
```java
.withExposedPorts(2480, 6379, 5432, 8182, 50051)
```

To:
```java
.withExposedPorts(2480, 6379, 5432, 7687, 8182, 50051)
```

**Step 2: Add BoltProtocolPlugin to server plugins**

Change line 42 from:
```java
-Darcadedb.server.plugins=PostgresProtocolPlugin,GremlinServerPlugin,GrpcServerPlugin,PrometheusMetricsPlugin
```

To:
```java
-Darcadedb.server.plugins=PostgresProtocolPlugin,GremlinServerPlugin,GrpcServerPlugin,BoltProtocolPlugin,PrometheusMetricsPlugin
```

**Step 3: Add boltPort field**

Add after line 53 (after `grpcPort`):
```java
protected int    boltPort    = ARCADE.getMappedPort(7687);
```

**Step 4: Verify compilation**

Run: `cd /Users/frank/projects/arcade/worktrees/bolt-e2e && mvn compile -pl e2e -q`
Expected: BUILD SUCCESS

**Step 5: Commit**

```bash
git add e2e/src/test/java/com/arcadedb/e2e/ArcadeContainerTemplate.java
git commit -m "feat(e2e): expose Bolt port and enable BoltProtocolPlugin

- Expose port 7687 for Bolt protocol connections
- Add BoltProtocolPlugin to server plugins configuration
- Add boltPort field for mapped port access

Refs #3279

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>"
```

---

## Task 3: Create RemoteBoltDatabaseTest with Connection Test

**Files:**
- Create: `e2e/src/test/java/com/arcadedb/e2e/RemoteBoltDatabaseTest.java`

**Step 1: Write the failing test file**

Create `e2e/src/test/java/com/arcadedb/e2e/RemoteBoltDatabaseTest.java`:

```java
/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.e2e;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;

import static org.assertj.core.api.Assertions.assertThat;

class RemoteBoltDatabaseTest extends ArcadeContainerTemplate {

  private Driver driver;

  @BeforeEach
  void setUp() {
    driver = GraphDatabase.driver(
        "bolt://" + host + ":" + boltPort,
        AuthTokens.basic("root", "playwithdata"),
        Config.builder()
            .withoutEncryption()
            .build()
    );
  }

  @AfterEach
  void tearDown() {
    if (driver != null)
      driver.close();
  }

  @Test
  void testConnection() {
    driver.verifyConnectivity();
  }
}
```

**Step 2: Run test to verify it passes**

Run: `cd /Users/frank/projects/arcade/worktrees/bolt-e2e && mvn test -pl e2e -Dtest=RemoteBoltDatabaseTest#testConnection -q`
Expected: BUILD SUCCESS (1 test passed)

**Step 3: Commit**

```bash
git add e2e/src/test/java/com/arcadedb/e2e/RemoteBoltDatabaseTest.java
git commit -m "feat(e2e): add RemoteBoltDatabaseTest with connection test

Initial Bolt e2e test class using Neo4j Java driver to verify
connectivity to ArcadeDB via Bolt protocol on TestContainers.

Refs #3279

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>"
```

---

## Task 4: Add Simple Cypher Query Test

**Files:**
- Modify: `e2e/src/test/java/com/arcadedb/e2e/RemoteBoltDatabaseTest.java`

**Step 1: Add imports**

Add after line 27 (after `GraphDatabase` import):
```java
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
```

**Step 2: Write the failing test**

Add after the `testConnection` method:

```java
@Test
void simpleReturnQuery() {
  try (Session session = driver.session(SessionConfig.forDatabase("beer"))) {
    final Result result = session.run("RETURN 1 AS value");
    assertThat(result.hasNext()).isTrue();
    final org.neo4j.driver.Record record = result.next();
    assertThat(record.get("value").asLong()).isEqualTo(1L);
    assertThat(result.hasNext()).isFalse();
  }
}
```

**Step 3: Run test to verify it passes**

Run: `cd /Users/frank/projects/arcade/worktrees/bolt-e2e && mvn test -pl e2e -Dtest=RemoteBoltDatabaseTest#simpleReturnQuery -q`
Expected: BUILD SUCCESS

**Step 4: Commit**

```bash
git add e2e/src/test/java/com/arcadedb/e2e/RemoteBoltDatabaseTest.java
git commit -m "feat(e2e): add simple RETURN query test for Bolt

Tests basic Cypher query execution via Bolt protocol returning
a simple value.

Refs #3279

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>"
```

---

## Task 5: Add Query Against Beer Database Test

**Files:**
- Modify: `e2e/src/test/java/com/arcadedb/e2e/RemoteBoltDatabaseTest.java`

**Step 1: Write the failing test**

Add after the `simpleReturnQuery` method:

```java
@Test
void queryBeerDatabase() {
  try (Session session = driver.session(SessionConfig.forDatabase("beer"))) {
    final Result result = session.run("MATCH (b:Beer) RETURN b.name AS name LIMIT 1");
    assertThat(result.hasNext()).isTrue();
    final org.neo4j.driver.Record record = result.next();
    assertThat(record.get("name").asString()).isNotBlank();
    assertThat(result.hasNext()).isFalse();
  }
}
```

**Step 2: Run test to verify it passes**

Run: `cd /Users/frank/projects/arcade/worktrees/bolt-e2e && mvn test -pl e2e -Dtest=RemoteBoltDatabaseTest#queryBeerDatabase -q`
Expected: BUILD SUCCESS

**Step 3: Commit**

```bash
git add e2e/src/test/java/com/arcadedb/e2e/RemoteBoltDatabaseTest.java
git commit -m "feat(e2e): add Bolt query test against Beer database

Tests Cypher MATCH query against the pre-loaded Beer database
via Bolt protocol.

Refs #3279

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>"
```

---

## Task 6: Add Parameterized Query Test

**Files:**
- Modify: `e2e/src/test/java/com/arcadedb/e2e/RemoteBoltDatabaseTest.java`

**Step 1: Add import**

Add after the `SessionConfig` import:
```java
import java.util.Map;
```

**Step 2: Write the failing test**

Add after the `queryBeerDatabase` method:

```java
@Test
void parameterizedQuery() {
  try (Session session = driver.session(SessionConfig.forDatabase("beer"))) {
    final Result result = session.run(
        "RETURN $name AS name, $value AS value",
        Map.of("name", "test", "value", 42)
    );
    assertThat(result.hasNext()).isTrue();
    final org.neo4j.driver.Record record = result.next();
    assertThat(record.get("name").asString()).isEqualTo("test");
    assertThat(record.get("value").asLong()).isEqualTo(42L);
    assertThat(result.hasNext()).isFalse();
  }
}
```

**Step 3: Run test to verify it passes**

Run: `cd /Users/frank/projects/arcade/worktrees/bolt-e2e && mvn test -pl e2e -Dtest=RemoteBoltDatabaseTest#parameterizedQuery -q`
Expected: BUILD SUCCESS

**Step 4: Commit**

```bash
git add e2e/src/test/java/com/arcadedb/e2e/RemoteBoltDatabaseTest.java
git commit -m "feat(e2e): add parameterized Cypher query test for Bolt

Tests Bolt protocol parameter binding with named parameters.

Refs #3279

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>"
```

---

## Task 7: Run All E2E Tests and Verify

**Files:**
- None (verification only)

**Step 1: Run all RemoteBoltDatabaseTest tests**

Run: `cd /Users/frank/projects/arcade/worktrees/bolt-e2e && mvn test -pl e2e -Dtest=RemoteBoltDatabaseTest -q`
Expected: BUILD SUCCESS (4 tests passed)

**Step 2: Run all e2e tests to ensure nothing is broken**

Run: `cd /Users/frank/projects/arcade/worktrees/bolt-e2e && mvn test -pl e2e -q`
Expected: BUILD SUCCESS (all tests pass)

**Step 3: Final commit (squash if desired)**

If all tests pass, the implementation is complete. The branch is ready for PR creation.

---

## Summary

This plan implements issue #3279 with the following changes:

1. **e2e/pom.xml**: Add Neo4j Java driver dependency (5.27.0)
2. **ArcadeContainerTemplate.java**:
   - Expose port 7687 for Bolt
   - Add BoltProtocolPlugin to server plugins
   - Add `boltPort` field
3. **RemoteBoltDatabaseTest.java**: New test class with 4 tests:
   - `testConnection`: Verify Bolt connectivity
   - `simpleReturnQuery`: Basic RETURN query
   - `queryBeerDatabase`: MATCH query against Beer database
   - `parameterizedQuery`: Query with named parameters

Total: 3 files modified/created, 4 tests added.
