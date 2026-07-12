# Engine antlr 4.13.2 + Gremlin antlr relocation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move `arcadedb-engine` to a plain antlr 4.13.2 (fixing the Hibernate/Spring 6 `Could not deserialize ATN` clash) while keeping Gremlin on antlr 4.9.1 relocated into a private package, and prove the two coexist in one JVM.

**Architecture:** Engine stays an ordinary unshaded jar on antlr 4.13.2 (its parsers regenerate to v4 ATN). The gremlin shaded uber-jar relocates `org.antlr` -> `com.arcadedb.gremlin.shaded.org.antlr` so TinkerPop's precompiled v3 parser keeps its 4.9.1 runtime. A new `gremlin-it` reactor module runs a coexistence test - and the existing gremlin suite - against the shaded jar, where engine's real 4.13.2 and gremlin's relocated 4.9.1 live side by side.

**Tech Stack:** Java 21, Maven 3.9 multi-module reactor, maven-shade-plugin 3.6.2, antlr4-maven-plugin, JUnit 5 (Jupiter) + JUnit vintage, AssertJ.

**Spec:** `docs/superpowers/specs/2026-07-10-gremlin-antlr-shading-engine-bump-design.md`
**Issue:** #5208 (sub-issue of #4647). **Branch:** `feat/4647-antlr-engine-bump-gremlin-shading` (already exists).

## Global Constraints

- Engine antlr version: **4.13.2** (plain, unshaded jar).
- Gremlin/TinkerPop antlr version: **stays 4.9.1** - never bump (precompiled parser needs v3 ATN).
- Relocate **only** `org.antlr` in gremlin; do **not** relocate `org.apache.tinkerpop.gremlin.language.grammar`.
- Do **not** shade the engine.
- Relocation shaded pattern: `com.arcadedb.gremlin.shaded.org.antlr`.
- Use the `final` keyword on new variables/parameters; no `System.out` debug; import classes (no FQNs); prefer `assertThat(x).isTrue()` style.
- Do not add Claude as author. Do not commit unless the human asks (this plan's commit steps are for the human/executor to run).
- Tag slow/benchmark tests with `@Tag("slow")` / `@Tag("benchmark")` as per repo convention.

---

### Task 1: Bump engine to antlr 4.13.2

**Files:**
- Modify: `engine/pom.xml:46`

**Interfaces:**
- Produces: `arcadedb-engine` artifact depending on `org.antlr:antlr4-runtime:4.13.2`, with SQL/Cypher/Cypher25 parsers regenerated to v4 ATN. No API surface change (antlr types are not exposed by engine's public API).

- [ ] **Step 1: Change the antlr version property**

In `engine/pom.xml`, change line 46 from:

```xml
        <antlr4.version>4.9.1</antlr4.version>
```

to:

```xml
        <antlr4.version>4.13.2</antlr4.version>
```

- [ ] **Step 2: Clean-build the engine so parsers regenerate and hand-written code recompiles**

Run: `mvn -pl engine clean test-compile`
Expected: BUILD SUCCESS. The antlr4-maven-plugin regenerates `target/generated-sources/antlr4/**` with the 4.13.2 runtime; the 9 hand-written parser files compile unchanged (they use only stable API: `CharStreams`, `CommonTokenStream`, `BailErrorStrategy`, `BaseErrorListener`, `DefaultErrorStrategy`, `ParserRuleContext`, `Recognizer`, `Token`, `RecognitionException`, `PredictionMode`, `Interval`, `ParseCancellationException`, `ParseTree`, `TerminalNode`).

If compilation fails on an antlr symbol, fix the single call site to the 4.13.2 equivalent (most likely none). Do not introduce new dependencies.

- [ ] **Step 3: Run the engine SQL + Cypher parser test suites**

Run: `mvn -pl engine test -Dtest='SQL*,*Cypher*,*Antlr*,*Parser*'`
Expected: BUILD SUCCESS, all selected tests pass. This is the API-drift safety net: it proves the v4-ATN parsers deserialize and parse correctly under 4.13.2.

- [ ] **Step 4: Commit**

```bash
git add engine/pom.xml
git commit -m "fix(#5208): bump engine ANTLR runtime to 4.13.2

Regenerates SQL/Cypher parsers to v4 ATN so the engine converges with the
antlr-runtime 4.10+ pulled by Spring 6 / Hibernate 6, resolving the
'Could not deserialize ATN with version 3' clash for embedded engine users.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 2: Relocate `org.antlr` in the Gremlin shaded jar

**Files:**
- Modify: `gremlin/pom.xml:178-181` (the `maven-shade-plugin` declaration)

**Interfaces:**
- Produces: `arcadedb-gremlin-<version>-shaded.jar` in which antlr classes live at `com.arcadedb.gremlin.shaded.org.antlr.**` and no class references `org.antlr.**`. The plain `arcadedb-gremlin` jar and its `antlr4.version` are unchanged (4.9.1).

- [ ] **Step 1: Add the relocation to the shade plugin**

In `gremlin/pom.xml`, replace the plugin declaration at lines 178-181:

```xml
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-shade-plugin</artifactId>
            </plugin>
```

with:

```xml
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-shade-plugin</artifactId>
                <configuration>
                    <!-- Relocate ANTLR 4.9.1 (TinkerPop's precompiled parser needs the v3 ATN
                         format) into a private package so it never collides with the engine's
                         org.antlr 4.13.2 on a shared classpath. See issue #5208 / #4647. -->
                    <relocations>
                        <relocation>
                            <pattern>org.antlr</pattern>
                            <shadedPattern>com.arcadedb.gremlin.shaded.org.antlr</shadedPattern>
                        </relocation>
                    </relocations>
                </configuration>
            </plugin>
```

- [ ] **Step 2: Build the shaded jar (skip the now-divergent gremlin tests for this build)**

Run: `mvn -pl gremlin -am package -DskipTests`
Expected: BUILD SUCCESS. `-am` rebuilds the engine (4.13.2) first; gremlin compiles against engine 4.13.2 + antlr 4.9.1 (compilation does not deserialize ATN, so no clash) and produces the shaded jar. `-DskipTests` avoids running gremlin's SQL/Cypher-text tests, which cannot pass unshaded (Task 4 relocates their execution).

- [ ] **Step 3: Verify the relocation actually happened in the jar**

Run:
```bash
SJ=$(ls gremlin/target/arcadedb-gremlin-*-shaded.jar)
echo "relocated antlr classes: $(unzip -l "$SJ" | grep -c 'com/arcadedb/gremlin/shaded/org/antlr/')"
echo "leaked original antlr classes: $(unzip -l "$SJ" | grep -c 'org/antlr/')"
```
Expected: relocated count > 0 (around 229); leaked count == 0.

- [ ] **Step 4: Verify references inside the generated parser were rewritten**

Run:
```bash
SJ=$(ls gremlin/target/arcadedb-gremlin-*-shaded.jar)
cd /tmp && rm -rf antlrcheck && mkdir antlrcheck && cd antlrcheck
unzip -q "$OLDPWD/$SJ" 'org/apache/tinkerpop/gremlin/language/grammar/GremlinParser.class' 'org/apache/tinkerpop/gremlin/language/grammar/GremlinAntlrToJava.class'
javap -p -c org/apache/tinkerpop/gremlin/language/grammar/GremlinParser.class org/apache/tinkerpop/gremlin/language/grammar/GremlinAntlrToJava.class | grep -c 'org/antlr'          # expect 0
javap -p -c org/apache/tinkerpop/gremlin/language/grammar/GremlinParser.class org/apache/tinkerpop/gremlin/language/grammar/GremlinAntlrToJava.class | grep -c 'com/arcadedb/gremlin/shaded/org/antlr'   # expect > 0
cd "$OLDPWD"
```
Expected: first grep == 0, second grep > 0.

- [ ] **Step 5: Commit**

```bash
git add gremlin/pom.xml
git commit -m "fix(#5208): relocate ANTLR into a private package in the Gremlin shaded jar

Keeps TinkerPop's precompiled 4.9.1 parser on its required v3 ATN runtime while
letting the engine ship plain antlr 4.13.2, so both coexist on one classpath.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 3: Create the `gremlin-it` module with the coexistence test

This is the decisive proof for #4647: engine antlr 4.13.2 and gremlin antlr 4.9.1 running in one JVM. The module is kept dependency-minimal (engine + shaded gremlin + junit/assertj only - no `gremlin-test`), so it is deterministic and self-contained.

**Files:**
- Modify: `pom.xml` (parent `<modules>`, after line 139 `<module>gremlin</module>`)
- Create: `gremlin-it/pom.xml`
- Create: `gremlin-it/src/test/java/com/arcadedb/gremlin/antlr/AntlrCoexistenceIT.java`

**Interfaces:**
- Consumes: `arcadedb-gremlin:shaded` (relocated antlr 4.9.1 + bundled tinkerpop), `arcadedb-engine` (real org.antlr 4.13.2), the `ArcadeGraph.open(String)` / `ArcadeGraph.gremlin(String).execute()` / `BasicDatabase.query(String,String)` APIs.
- Produces: a passing `AntlrCoexistenceIT` that fails if either parser cannot load.

- [ ] **Step 1: Register the module in the reactor**

In `pom.xml`, after line 139 (`        <module>gremlin</module>`) add:

```xml
        <module>gremlin-it</module>
```

- [ ] **Step 2: Create `gremlin-it/pom.xml`**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!--
    Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.

    SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
    SPDX-License-Identifier: Apache-2.0
-->
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

    <artifactId>arcadedb-gremlin-it</artifactId>
    <packaging>jar</packaging>
    <name>ArcadeDB Gremlin Integration Tests</name>

    <description>
        Runs Gremlin tests against the RELOCATED (shaded) arcadedb-gremlin jar so that
        the engine's antlr 4.13.2 and Gremlin's relocated antlr 4.9.1 coexist in one JVM.
        This module produces no shipped artifact; it exists only to host coexistence tests.
    </description>

    <properties>
        <maven.deploy.skip>true</maven.deploy.skip>
    </properties>

    <dependencies>
        <!-- Engine on real org.antlr 4.13.2 (v4 ATN) for SQL/Cypher parsing. -->
        <dependency>
            <groupId>com.arcadedb</groupId>
            <artifactId>arcadedb-engine</artifactId>
            <version>${project.parent.version}</version>
            <scope>test</scope>
        </dependency>

        <!-- Self-contained shaded Gremlin uber-jar: bundles tinkerpop + groovy + netty +
             gremlin classes + the RELOCATED antlr 4.9.1. The wildcard exclusion prevents its
             non-reduced pom from re-pulling the original org.antlr / tinkerpop and
             reintroducing the version clash. -->
        <dependency>
            <groupId>com.arcadedb</groupId>
            <artifactId>arcadedb-gremlin</artifactId>
            <version>${project.parent.version}</version>
            <classifier>shaded</classifier>
            <scope>test</scope>
            <exclusions>
                <exclusion>
                    <groupId>*</groupId>
                    <artifactId>*</artifactId>
                </exclusion>
            </exclusions>
        </dependency>

        <dependency>
            <groupId>org.junit.jupiter</groupId>
            <artifactId>junit-jupiter</artifactId>
            <version>${junit.jupiter.version}</version>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.assertj</groupId>
            <artifactId>assertj-core</artifactId>
            <version>${assertj-core.version}</version>
            <scope>test</scope>
        </dependency>
    </dependencies>
</project>
```

- [ ] **Step 3: Write the failing coexistence test**

Create `gremlin-it/src/test/java/com/arcadedb/gremlin/antlr/AntlrCoexistenceIT.java`:

```java
/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.gremlin.antlr;

import com.arcadedb.database.BasicDatabase;
import com.arcadedb.gremlin.ArcadeGraph;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proves the engine's ANTLR 4.13.2 SQL/Cypher parsers (v4 ATN) and TinkerPop's relocated
 * ANTLR 4.9.1 Gremlin parser (v3 ATN) load and run in the same JVM without an
 * InvalidClassException "Could not deserialize ATN" error. Regression guard for #4647.
 */
class AntlrCoexistenceIT {

  @Test
  void engineAntlr413AndGremlinAntlr491Coexist() {
    final String directory = "./target/antlr-coexistence";
    FileUtils.deleteRecursively(new File(directory));

    final ArcadeGraph graph = ArcadeGraph.open(directory);
    try {
      final BasicDatabase database = graph.getDatabase();
      database.getSchema().createVertexType("Person");
      database.transaction(() ->
          database.newVertex("Person").set("name", "Jay").set("age", 25).save());

      // Engine ANTLR SQL parser (org.antlr 4.13.2, v4 ATN).
      final ResultSet sql = database.query("sql", "SELECT name, age FROM Person WHERE age = 25");
      assertThat(sql.hasNext()).isTrue();
      assertThat(sql.next().<String>getProperty("name")).isEqualTo("Jay");

      // Engine ANTLR Cypher parser (org.antlr 4.13.2, v4 ATN).
      final ResultSet cypher = database.query("cypher", "MATCH (p:Person) RETURN p.name AS name");
      assertThat(cypher.hasNext()).isTrue();

      // TinkerPop Gremlin parser (relocated com.arcadedb.gremlin.shaded.org.antlr 4.9.1, v3 ATN).
      final ResultSet gremlin = graph.gremlin("g.V().hasLabel('Person').count()").execute();
      assertThat((Long) gremlin.next().getProperty("result")).isEqualTo(1L);
    } finally {
      graph.drop();
    }
  }
}
```

- [ ] **Step 4: Run the test - it must pass against the shaded jar**

Run: `mvn -pl gremlin-it -am verify`
Expected: BUILD SUCCESS, `AntlrCoexistenceIT` passes. `-am` ensures engine + the gremlin shaded jar are freshly installed first.

To confirm the test is real (not a false pass), temporarily inspect the resolved test classpath and verify both antlr namespaces are present:
Run: `mvn -pl gremlin-it dependency:build-classpath -Dmdep.includeScope=test -q -Dmdep.outputFile=/dev/stdout | tr ':' '\n' | grep -E 'engine|gremlin.*shaded'`
Expected: both `arcadedb-engine` and `arcadedb-gremlin-*-shaded.jar` on the classpath.

- [ ] **Step 5: Commit**

```bash
git add pom.xml gremlin-it/pom.xml gremlin-it/src/test/java/com/arcadedb/gremlin/antlr/AntlrCoexistenceIT.java
git commit -m "test(#5208): gremlin-it module proving engine 4.13.2 + gremlin 4.9.1 coexistence

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 4: Run the existing Gremlin suite against the shaded jar via `dependenciesToScan`

Only tests that execute SQL/Cypher **text** break unshaded (pure-Gremlin/structure tests pass under the mediated 4.9.1 runtime). Rather than classify them by hand, run the whole gremlin test-jar against the shaded classpath in `gremlin-it`, and stop executing them in the `gremlin` module.

**Files:**
- Modify: `gremlin/pom.xml` (skip surefire/failsafe execution; keep test-jar)
- Modify: `gremlin-it/pom.xml` (add test-jar dependency, test deps mirrored from gremlin, `dependenciesToScan`)

**Interfaces:**
- Consumes: `arcadedb-gremlin:tests` (the compiled test classes + resources), plus the test-scoped dependencies those classes need (`gremlin-test`, `junit-vintage-engine`, `arcadedb-server` test-jar).
- Produces: the full gremlin functional suite executing green against the relocated jar.

- [ ] **Step 1: Stop executing tests in the gremlin module (still compile + build test-jar)**

In `gremlin/pom.xml`, inside `<build><plugins>`, add a surefire configuration that skips execution. Add this plugin next to the existing `maven-jar-plugin` / `maven-shade-plugin` entries:

```xml
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-surefire-plugin</artifactId>
                <configuration>
                    <!-- Gremlin's tests exercise BOTH the engine SQL/Cypher parser (antlr 4.13.2)
                         and TinkerPop's parser (antlr 4.9.1); those cannot coexist on the
                         unshaded test classpath. They run in the arcadedb-gremlin-it module
                         against the shaded jar instead. Test sources still compile here so the
                         test-jar is produced. See #5208. -->
                    <skipTests>true</skipTests>
                </configuration>
            </plugin>
```

Note: `skipTests` skips execution only; `test-compile` still runs and the `maven-jar-plugin` `test-jar` goal (bound to `package`) still produces `arcadedb-gremlin-<version>-tests.jar`.

- [ ] **Step 2: Add the test-jar and needed test dependencies to `gremlin-it`**

In `gremlin-it/pom.xml`, add these dependencies (after the `shaded` dependency). The `gremlin-test` and `junit-vintage-engine` deps carry the conformance-suite runner and JUnit4 bridge the migrated tests need; exclude their tinkerpop/antlr so the shaded jar's bundled copies win:

```xml
        <!-- Compiled gremlin test classes + resources, executed via dependenciesToScan. -->
        <dependency>
            <groupId>com.arcadedb</groupId>
            <artifactId>arcadedb-gremlin</artifactId>
            <version>${project.parent.version}</version>
            <classifier>tests</classifier>
            <scope>test</scope>
            <exclusions>
                <exclusion>
                    <groupId>*</groupId>
                    <artifactId>*</artifactId>
                </exclusion>
            </exclusions>
        </dependency>

        <!-- Server test scaffolding used by the com.arcadedb.server.gremlin.* tests. -->
        <dependency>
            <groupId>com.arcadedb</groupId>
            <artifactId>arcadedb-server</artifactId>
            <version>${project.parent.version}</version>
            <scope>test</scope>
            <type>test-jar</type>
        </dependency>

        <!-- TinkerPop conformance-suite runner; use the shaded jar's bundled tinkerpop + antlr. -->
        <dependency>
            <groupId>org.apache.tinkerpop</groupId>
            <artifactId>gremlin-test</artifactId>
            <version>3.8.1</version>
            <scope>test</scope>
            <exclusions>
                <exclusion>
                    <groupId>org.antlr</groupId>
                    <artifactId>antlr4-runtime</artifactId>
                </exclusion>
            </exclusions>
        </dependency>
        <dependency>
            <groupId>org.junit.vintage</groupId>
            <artifactId>junit-vintage-engine</artifactId>
            <version>${junit.jupiter.version}</version>
            <scope>test</scope>
        </dependency>
```

- [ ] **Step 3: Configure surefire and failsafe in `gremlin-it` to scan the test-jar**

In `gremlin-it/pom.xml`, add a `<build>` section:

```xml
    <build>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-surefire-plugin</artifactId>
                <configuration>
                    <dependenciesToScan>
                        <dependency>com.arcadedb:arcadedb-gremlin</dependency>
                    </dependenciesToScan>
                </configuration>
            </plugin>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-failsafe-plugin</artifactId>
                <configuration>
                    <dependenciesToScan>
                        <dependency>com.arcadedb:arcadedb-gremlin</dependency>
                    </dependenciesToScan>
                </configuration>
            </plugin>
        </plugins>
    </build>
```

- [ ] **Step 4: Run the migrated suite and iterate to green**

Run: `mvn -pl gremlin-it -am verify`
Expected end state: BUILD SUCCESS; the gremlin `*Test` (surefire) and `*IT` (failsafe) classes execute from the test-jar against the shaded jar, and no test throws `Could not deserialize ATN`.

Iteration guidance if the first run is red:
- **`NoClassDefFoundError` / `ClassNotFoundException` for a test-only type** -> that dependency was test-scoped in `gremlin/pom.xml` but is missing here. Add it to `gremlin-it/pom.xml` (mirror from `gremlin/pom.xml` lines 48-162), excluding `org.antlr:antlr4-runtime` and `org.apache.tinkerpop:*` where the shaded jar already bundles them.
- **A duplicate-class / `LinkageError` on `org.antlr` or `org.apache.tinkerpop`** -> a newly added dep re-pulled the originals. Add the same `<exclusion>` for `org.antlr:antlr4-runtime` (and tinkerpop artifacts) to that dependency.
- **Benchmark/slow tests dominate runtime** -> they are already `@Tag("benchmark")`/`@Tag("slow")` and excluded by the parent CI profile; leave them.
- **Server ITs needing ports/fixtures** -> they run under failsafe with the parent `pre-integration-test`/`post-integration-test` bindings; confirm they behaved the same before the change (they are environment-dependent, not antlr-dependent).

- [ ] **Step 5: Confirm the gremlin module itself still builds green (tests skipped, test-jar present)**

Run:
```bash
mvn -pl gremlin package
ls gremlin/target/arcadedb-gremlin-*-tests.jar   # test-jar exists
```
Expected: BUILD SUCCESS (no tests executed in this module) and the `-tests.jar` present.

- [ ] **Step 6: Commit**

```bash
git add gremlin/pom.xml gremlin-it/pom.xml
git commit -m "test(#5208): run gremlin suite against the shaded jar in gremlin-it

Skips test execution in the gremlin module (test-jar still built) and executes the
compiled tests in gremlin-it via dependenciesToScan, so engine 4.13.2 and gremlin's
relocated 4.9.1 coexist during tests.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 5: Attribution/NOTICE and full reactor verification

**Files:**
- Modify (if needed): `ATTRIBUTIONS.md`, `NOTICE`

**Interfaces:**
- Produces: a clean full-reactor `mvn clean install` and correct antlr attribution.

- [ ] **Step 1: Confirm antlr attribution and version**

Run: `grep -in "antlr" ATTRIBUTIONS.md NOTICE`
Expected: antlr (BSD-3-Clause) is already listed. If a version is stated, update it to note engine on 4.13.2 and the gremlin shaded jar bundling 4.9.1. If antlr is absent from `ATTRIBUTIONS.md`, add a line:

```
- ANTLR 4 Runtime (org.antlr:antlr4-runtime) - BSD-3-Clause - https://www.antlr.org
```

- [ ] **Step 2: Full reactor build**

Run: `mvn clean install -DskipTests`
Expected: BUILD SUCCESS across all modules (engine, gremlin, gremlin-it, package, ...). Confirms the new module and the skip/relocation changes do not break assembly.

- [ ] **Step 3: Targeted test pass on the changed areas**

Run: `mvn -pl engine,gremlin-it -am verify`
Expected: BUILD SUCCESS - engine parser suites and the gremlin-it coexistence + migrated suite all green.

- [ ] **Step 4: Confirm the shipped distribution still resolves the shaded gremlin jar**

Run: `grep -n "arcadedb-gremlin" -A3 package/pom.xml | grep -n "classifier"`
Expected: `package/pom.xml` still references `arcadedb-gremlin` with `<classifier>shaded</classifier>` (unchanged) - the distribution ships the relocated jar. No change needed; this is a verification step.

- [ ] **Step 5: Commit (if attribution changed)**

```bash
git add ATTRIBUTIONS.md NOTICE
git commit -m "docs(#5208): note ANTLR 4.13.2 (engine) / 4.9.1 (shaded gremlin) attribution

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Self-Review notes

- **Spec coverage:** engine bump (Task 1), gremlin relocation (Task 2), coexistence test (Task 3), gremlin suite against shaded jar + gremlin skip (Task 4), attribution + full build (Task 5), distribution consumption verified (Task 5 Step 4). Deferred item (Maven-consumer `<optional>`) intentionally out of scope.
- **Known risk hotspot:** Task 4's `dependenciesToScan` dependency reconciliation for the TinkerPop conformance suites. Mitigated with explicit iteration guidance; the decisive #4647 proof (Task 3) is independent and self-contained, so it lands even if Task 4 needs iteration.
- **Type consistency:** `com.arcadedb.gremlin.shaded.org.antlr` used identically in Task 2 and referenced in Task 3/Task 4 comments; `arcadedb-gremlin:shaded` and `arcadedb-gremlin:tests` classifiers consistent across Tasks 3-4.
