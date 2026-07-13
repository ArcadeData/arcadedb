# Gremlin antlr Maven-consumer isolation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stop `arcadedb-gremlin` from exporting `antlr4-runtime:4.9.1` transitively so a Maven-coordinate consumer of both `arcadedb-gremlin` and `arcadedb-engine` resolves antlr cleanly to the engine's `4.13.2`, guarded by a build-time enforcer check and documented in the README.

**Architecture:** Mark gremlin's `antlr4-runtime` dependency `<optional>true</optional>` (keeps it bundled+relocated in the shaded uber-jar but un-exports it). Add a check-only `gremlin-consumer-it` reactor module that depends on both coordinates and uses `maven-enforcer-plugin` `bannedDependencies` (`searchTransitive`) to fail the build if antlr `4.9.1` ever leaks. Document the `shaded`-classifier embedding path in the README.

**Tech Stack:** Maven multi-module (Maven 3.9.x), maven-shade-plugin 3.6.2, maven-enforcer-plugin 3.4.1, antlr 4.9.1 (gremlin) / 4.13.2 (engine).

## Global Constraints

- Java 21+; Maven reactor build (`mvn clean install`).
- All new files carry the Apache-2.0 license header comment block used by every other pom in the repo (copy verbatim from `gremlin-it/pom.xml` lines 2-19). Workflow files are the only exception; poms include it.
- No new third-party dependencies. `maven-enforcer-plugin` is already versioned in the parent (`${maven-enforcer-plugin.version}` = 3.4.1); reuse that property.
- Do NOT change `<antlr4.version>` in `gremlin/pom.xml` (stays `4.9.1`), do NOT touch the engine, the shipped distribution, `gremlin-it`, or `createDependencyReducedPom`.
- Do not add Claude as author. Do not use the em dash character in any file content. Do not `git commit` unless a step explicitly says to (project owner reviews first) - see the note under Execution.
- Commit messages: end with the repo's standard trailer only if the owner's convention requires it; these steps use plain messages (owner squashes/edits on merge).

---

## Task 1: Add the failing regression guard (`gremlin-consumer-it` module)

Create the check-only module FIRST, before the fix, so the enforcer guard fails on the current (leaky) dependency graph. This is the "red" state of the TDD cycle: the guard proves antlr 4.9.1 is currently exported.

**Files:**
- Create: `gremlin-consumer-it/pom.xml`
- Modify: `pom.xml` (parent) - add `<module>gremlin-consumer-it</module>` after line 141 (`<module>gremlin-it</module>`)

**Interfaces:**
- Consumes: the reactor's `com.arcadedb:arcadedb-engine` and `com.arcadedb:arcadedb-gremlin` (plain coordinate, `${project.parent.version}`), and the parent property `${maven-enforcer-plugin.version}`.
- Produces: a reactor module `arcadedb-gremlin-consumer-it` (packaging `pom`, ships nothing) whose `validate` phase runs `maven-enforcer-plugin:enforce` with a `bannedDependencies` rule banning `org.antlr:antlr4-runtime:4.9.1` transitively.

- [ ] **Step 1: Write the guard module pom (the failing test)**

Create `gremlin-consumer-it/pom.xml`:

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

    <artifactId>arcadedb-gremlin-consumer-it</artifactId>
    <packaging>pom</packaging>
    <name>ArcadeDB Gremlin Maven-Consumer Isolation Check</name>

    <description>
        Check-only module (ships nothing). Simulates a third-party app that depends on BOTH
        arcadedb-engine and the plain arcadedb-gremlin coordinate, and asserts via maven-enforcer
        that antlr4-runtime 4.9.1 is NOT exported transitively by arcadedb-gremlin (issue #5217).
        gremlin's antlr must be <optional>true</optional> so the only antlr edge left in a
        both-modules consumer graph is the engine's 4.13.2. Must build after arcadedb-gremlin in
        the reactor. Runs no code: the unshaded engine+gremlin combination is runtime-incompatible
        by design, so this only ever asserts on the resolved dependency graph.
    </description>

    <properties>
        <!-- This module ships nothing; do not deploy or install it. -->
        <maven.deploy.skip>true</maven.deploy.skip>
        <maven.install.skip>true</maven.install.skip>
    </properties>

    <dependencies>
        <!-- A both-modules consumer graph: engine (real org.antlr 4.13.2) + plain gremlin. -->
        <dependency>
            <groupId>com.arcadedb</groupId>
            <artifactId>arcadedb-engine</artifactId>
            <version>${project.parent.version}</version>
        </dependency>
        <dependency>
            <groupId>com.arcadedb</groupId>
            <artifactId>arcadedb-gremlin</artifactId>
            <version>${project.parent.version}</version>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-enforcer-plugin</artifactId>
                <version>${maven-enforcer-plugin.version}</version>
                <executions>
                    <execution>
                        <id>ban-gremlin-antlr-4.9.1-leak</id>
                        <phase>validate</phase>
                        <goals>
                            <goal>enforce</goal>
                        </goals>
                        <configuration>
                            <rules>
                                <bannedDependencies>
                                    <searchTransitive>true</searchTransitive>
                                    <excludes>
                                        <exclude>org.antlr:antlr4-runtime:4.9.1</exclude>
                                    </excludes>
                                    <message>
                                        arcadedb-gremlin must not export antlr4-runtime 4.9.1 transitively (issue #5217).
                                        A consumer of both arcadedb-engine and arcadedb-gremlin must resolve antlr to the
                                        engine's 4.13.2 only. Mark gremlin's antlr4-runtime dependency &lt;optional&gt;true&lt;/optional&gt;
                                        and consume the arcadedb-gremlin:shaded classifier at runtime.
                                    </message>
                                </bannedDependencies>
                            </rules>
                            <fail>true</fail>
                        </configuration>
                    </execution>
                </executions>
            </plugin>
        </plugins>
    </build>
</project>
```

- [ ] **Step 2: Register the module in the parent reactor**

In `pom.xml`, add the new module immediately after the `gremlin-it` line (currently line 141):

```xml
        <module>gremlin</module>
        <module>gremlin-it</module>
        <module>gremlin-consumer-it</module>
        <module>graphql</module>
```

- [ ] **Step 3: Ensure the gremlin/engine artifacts are installed, then run the guard - verify it FAILS**

The guard resolves `arcadedb-gremlin`/`arcadedb-engine` from the local repo, so install them first (current code, no fix yet):

```bash
mvn -q -pl engine,integration,network,server,gremlin -am install -DskipTests
mvn -pl gremlin-consumer-it validate
```

Expected: BUILD FAILURE. The enforcer output names the banned artifact, e.g.:
```
[WARNING] Rule 0: org.apache.maven.enforcer.rules.dependency.BannedDependencies failed with message:
... Found Banned Dependency: org.antlr:antlr4-runtime:jar:4.9.1
```
This is the red state: gremlin currently exports antlr 4.9.1. If it PASSES here, stop - the assumption behind the fix is wrong and must be re-investigated before proceeding.

- [ ] **Step 4: Commit the failing guard**

```bash
git add gremlin-consumer-it/pom.xml pom.xml
git commit -m "test(#5217): add gremlin-consumer-it enforcer guard against antlr 4.9.1 transitive leak"
```

---

## Task 2: Un-export gremlin's antlr (make the guard pass)

Apply the one-line fix and confirm the guard flips to green and the shaded jar is unchanged.

**Files:**
- Modify: `gremlin/pom.xml` - the `antlr4-runtime` dependency block AND the `gremlin-core` dependency's `<exclusions>`
- Modify: `gremlin-consumer-it/pom.xml` - correct the enforcer matcher to the exact range `[4.9.1]` (Task 1 committed a bare `4.9.1`, which enforcer treats as "≥4.9.1" and would also ban the legitimate 4.13.2, making the guard un-greenable)

**Interfaces:**
- Consumes: the guard from Task 1 (`gremlin-consumer-it`).
- Produces: `arcadedb-gremlin`'s pom declares `antlr4-runtime` `<optional>true</optional>` AND excludes `antlr4-runtime` from `gremlin-core` - so no antlr 4.9.1 is exported transitively, while the `shaded` classifier jar still bundles the relocated antlr.

**Why the gremlin-core exclusion is required (not just `optional`):** `gremlin-core` pulls `antlr4-runtime:4.9.1` transitively via `gremlin-language`, a separate non-optional edge that `optional` on gremlin's own duplicate declaration does not remove. Without the exclusion, the guard stays red. Verified with `dependency:tree`.

- [ ] **Step 1: Mark the antlr dependency optional**

In `gremlin/pom.xml`, change the existing block (lines 70-74):

```xml
        <!-- Explicit ANTLR runtime to ensure consistent version across legacy and native Cypher -->
        <dependency>
            <groupId>org.antlr</groupId>
            <artifactId>antlr4-runtime</artifactId>
            <version>${antlr4.version}</version>
        </dependency>
```

to (mark it `optional`):

```xml
        <!-- Explicit ANTLR runtime to ensure consistent version across legacy and native Cypher.
             optional=true: keeps antlr 4.9.1 on gremlin's own classpath and bundled+relocated in
             the shaded uber-jar (maven-shade includes optional deps), but does NOT export it to
             Maven-coordinate consumers. A both-modules consumer therefore resolves antlr to the
             engine's 4.13.2 only, and consumes the arcadedb-gremlin:shaded classifier at runtime
             (issue #5217). Must NOT be <scope>provided</scope>: shade drops provided deps, which
             would strip the relocated antlr from the shaded jar and regress #5216. -->
        <dependency>
            <groupId>org.antlr</groupId>
            <artifactId>antlr4-runtime</artifactId>
            <version>${antlr4.version}</version>
            <optional>true</optional>
        </dependency>
```

AND add an antlr exclusion to the existing `gremlin-core` dependency (which already excludes
`commons-beanutils`), because `gremlin-core` pulls antlr 4.9.1 transitively via `gremlin-language`
on a separate, non-optional edge:

```xml
        <dependency>
            <groupId>org.apache.tinkerpop</groupId>
            <artifactId>gremlin-core</artifactId>
            <version>${gremlin.version}</version>
            <exclusions>
                <exclusion>
                    <groupId>commons-beanutils</groupId>
                    <artifactId>commons-beanutils</artifactId>
                </exclusion>
                <!-- gremlin-core pulls antlr4-runtime 4.9.1 transitively via gremlin-language,
                     independently of the explicit (optional) declaration above. That edge is not
                     optional and still leaks 4.9.1 to Maven-coordinate consumers (issue #5217)
                     unless excluded here. -->
                <exclusion>
                    <groupId>org.antlr</groupId>
                    <artifactId>antlr4-runtime</artifactId>
                </exclusion>
            </exclusions>
        </dependency>
```

Also correct `gremlin-consumer-it/pom.xml`: change the enforcer exclude from
`org.antlr:antlr4-runtime:4.9.1` to `org.antlr:antlr4-runtime:[4.9.1]` (exact range) so it does
not also ban the legitimate 4.13.2.

- [ ] **Step 2: Rebuild gremlin and re-run the guard - verify it PASSES**

```bash
mvn -q -pl gremlin -am install -DskipTests
mvn -pl gremlin-consumer-it validate
```

Expected: BUILD SUCCESS. The `ban-gremlin-antlr-4.9.1-leak` execution reports no banned dependency.

- [ ] **Step 3: Confirm the consumer graph resolves antlr to 4.13.2 only**

```bash
mvn -pl gremlin-consumer-it dependency:tree -Dincludes=org.antlr:antlr4-runtime
```

Expected: the only `org.antlr:antlr4-runtime` node shown is `4.13.2`; no `4.9.1` appears.

- [ ] **Step 4: Confirm the shaded gremlin jar still bundles the relocated antlr (no regression of #5216)**

```bash
JAR=$(ls gremlin/target/arcadedb-gremlin-*-shaded.jar | head -1)
echo "relocated (expect matches):"; unzip -l "$JAR" | grep -c 'com/arcadedb/gremlin/shaded/org/antlr/'
echo "bare org/antlr (expect 0):";  unzip -l "$JAR" | grep -c '^.*[[:space:]]org/antlr/' || true
```

Expected: relocated count is > 0 (antlr still bundled), bare `org/antlr/` count is `0`.

- [ ] **Step 5: Commit the fix**

```bash
git add gremlin/pom.xml
git commit -m "fix(#5217): mark gremlin antlr4-runtime optional so it is not exported to Maven consumers"
```

---

## Task 3: Document the shaded-classifier embedding path in the README

**Files:**
- Modify: `README.md` (insert a new subsection after the existing Java-17 Maven-usage block, which ends at line 230 - after the `docker pull ghcr.io/...` fenced block and before `### Building and Testing` at line 232)

**Interfaces:**
- Consumes: nothing.
- Produces: a `### Embedding Gremlin alongside the engine` README subsection telling both-modules embedders to use the `arcadedb-gremlin:shaded` classifier.

- [ ] **Step 1: Insert the embedding subsection**

In `README.md`, between line 230 (blank line after the `docker pull ghcr.io/arcadedata/arcadedb:26.3.1-java17` code block) and line 232 (`### Building and Testing`), insert:

```markdown
### Embedding Gremlin alongside the engine

When you depend on both `arcadedb-engine` and `arcadedb-gremlin` by Maven coordinate, use the
`shaded` classifier for gremlin. Its ANTLR runtime is relocated into a private package, so it
never collides with the engine's ANTLR 4.13.2 on a shared classpath. The plain `arcadedb-gremlin`
jar references bare `org.antlr` and cannot share a classpath with the engine.

```xml
<dependency>
    <groupId>com.arcadedb</groupId>
    <artifactId>arcadedb-engine</artifactId>
    <version>26.8.1</version>
</dependency>
<dependency>
    <groupId>com.arcadedb</groupId>
    <artifactId>arcadedb-gremlin</artifactId>
    <version>26.8.1</version>
    <classifier>shaded</classifier>
</dependency>
```

```

Note: the inner ` ```xml ... ``` ` fence is part of the inserted README content. Keep the outer markdown intact; ensure exactly one blank line separates this subsection from the following `### Building and Testing` heading.

- [ ] **Step 2: Verify the README renders and the section reads correctly**

```bash
grep -n "Embedding Gremlin alongside the engine" README.md
sed -n '231,255p' README.md
```

Expected: the heading appears once; the fenced `xml` block shows engine (no classifier) + gremlin with `<classifier>shaded</classifier>`; a single blank line precedes `### Building and Testing`.

- [ ] **Step 3: Commit the docs**

```bash
git add README.md
git commit -m "docs(#5217): document arcadedb-gremlin:shaded classifier for both-modules embedders"
```

---

## Task 4: Full-reactor verification

Confirm the change is clean across the whole build.

**Files:** none (verification only).

- [ ] **Step 1: Full reactor build**

```bash
mvn clean install -DskipTests
```

Expected: BUILD SUCCESS across all modules, including `arcadedb-gremlin-consumer-it` (its `validate`-phase enforcer passes) and `arcadedb-gremlin-it` (coexistence suite unaffected).

- [ ] **Step 2: Regression-proof the guard (optional but recommended)**

Temporarily revert Task 2 to prove the guard bites, then restore:

```bash
git stash push -- gremlin/pom.xml   # remove the optional flag from the working tree
# If already committed, instead: git checkout HEAD~2 -- gremlin/pom.xml  (adjust as needed)
mvn -q -pl gremlin -am install -DskipTests
mvn -pl gremlin-consumer-it validate    # expect BUILD FAILURE: antlr 4.9.1 banned
git stash pop                            # restore the fix (or: git checkout HEAD -- gremlin/pom.xml)
mvn -q -pl gremlin -am install -DskipTests
mvn -pl gremlin-consumer-it validate    # expect BUILD SUCCESS
```

Expected: FAIL without the fix, PASS with it - the guard genuinely protects the invariant.

---

## Notes for the maintainer (not a code step)

- **Release notes (manual):** the 26.8.1 GitHub release notes (the version shipping #5216 + this follow-up) should call out that embedders depending on both modules must consume the `arcadedb-gremlin:shaded` classifier. GitHub Releases are published by the maintainer and are not editable from this change.
- **Version literals in the README** use `26.8.1` (the target release). If the release version changes, update both `<version>` literals in the embedding snippet.
