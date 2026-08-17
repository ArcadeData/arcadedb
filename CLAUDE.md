# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

ArcadeDB is a Multi-Model DBMS (Database Management System) built for extreme performance. It's a Java-based project that supports multiple data models (Graph, Document, Key/Value, Search Engine, Time Series, Vector Embedding) and query languages (SQL, Cypher, Gremlin, GraphQL, MongoDB Query Language).

## Response Formatting
- Never use the em dash character (`—`) in responses. Use a normal dash (`-`), a comma, or rephrase instead.

## Project Instructions

Before writing any code:
- State how you will verify this change works (e.g., unit tests, integration tests, manual testing)
- Write the tests first (TDD approach) whenever possible
- Ensure code adheres to existing coding standards and styles
- Then implement the code
- Run verification and iterate until it passes
- Run all the connected tests could be affected by the change to ensure nothing is broken (no need to run the whole suite, it would take too long)

General design principles:
- reuse existing components whenever is possible
- don't use fully qualified names if possible, always import the class and just use the name
- don't include a new dependency unless is strictly necessary, and they MUST be Apache 2.0 compatible:
  - ✅ ALLOWED: Apache 2.0, MIT, BSD (2/3-Clause), ISC, EPL 1.0/2.0, UPL 1.0, EDL 1.0, LGPL 2.1+ (for libraries only),
    MPL-2.0 (for libraries only, unmodified), CDDL 1.0/1.1 (for libraries only, unmodified), GPL-2.0 WITH the Classpath
    Exception specifically (never a bare GPL), CC0/Public Domain
  - ❌ FORBIDDEN: GPL, AGPL, proprietary licenses without explicit permission, SSPL, Commons Clause, BUSL-1.1, Elastic-2.0
  - This is an allow-list: a license in neither row above is not permitted by default. Get explicit maintainer sign-off
    and add it to the ALLOWED row before depending on it. "For libraries only" / "unmodified" means: depending on the
    unmodified jar as-is is fine, but do not vendor or patch its source under this project without revisiting the
    license implications. CI enforces this allow-list on the Maven graph
    (`.github/scripts/check-license-allowlist.py`, run from `license-compliance.yml`) and on the npm graph
    (`license-checker --onlyAllow` in `studio-security-audit.yml`)
  - When adding a dependency, you MUST update ATTRIBUTIONS.md and, if Apache-licensed with a NOTICE file, incorporate required notices into the main NOTICE file
- for Studio (webapp), limit to jquery and bootstrap 5. If necessary use 3rd party libs, but they must be Apache 2.0 compatible (see allowed licenses above)
- always bear in mind PERFORMANCE. It must be always your mantra: performance and lightweight on garbage collector. If you can, prefer using arrays of primitives to List of Objects
- if you need to use JSON, use the class com.arcadedb.serializer.json.JSONObject. Leverage the getter methods that accept the default value as 2nd argument, so you don't need to check if they present or not null = less boilerplate code
- same thing for JSON arrays: use com.arcadedb.serializer.json.JSONArray class
- code styles:
 - adhere to the existing code
 - if statements with only one child sub-statement don't require a curly brace open/close, keep it simple
 - use the final keyword when possible on variables and parameters
- all new server-side code must be tested with a test case. Check existing test case to see the framework and style to use
- write a regression test
- after every change in the backend (Java), compile the project and fix all the issues until the compilation passes
- test all the new and old components you've modified before considering the job finished. Please do not provide something untested
- always keep in mind speed and security with ArcadeDB, do not introduce security hazard or code that could slow down other parts unless requested/approved
- do not commit on git, I will do it after a review
- remove any System.out you used for debug when you have finished
- For test cases, prefer this syntax: `assertThat(property.isMandatory()).isTrue();`
- Annotate performance/benchmark tests so they're skipped from regular CI builds:
  - Use `@Tag("benchmark")` for pure microbenchmarks (e.g., JMH-style or comparison runs)
  - Use `@Tag("slow")` for functional regression tests that take noticeably long (large batches, multi-second elapsed time, big payloads)
  - Use `@Tag("vector")` for an LSM vector-index test that spends most of its time waiting on a background rebuild. `LSMVectorIndex.REBUILD_SEMAPHORE` holds one permit for the whole JVM, so these tests convoy against each other and the waits have to be sized for the worst case; the tag routes the class to the `vector-unit-tests` lane where the convoy costs nobody else anything
  - Apply at the class level when every method in the class is slow; at the method level when only some methods are slow
  - The tags are a partition, not a set of overlapping labels: CI selects each lane with `-Dgroups`/`-DexcludedGroups` such that every test runs in exactly one lane. Tagging a class `vector` and one of its methods `slow` is fine, but do not expect that method in the slow lane
  - Required imports: `import org.junit.jupiter.api.Tag;`
  - A JUnit tag on a Cucumber `@Suite` does not work: Surefire's `groups`/`excludedGroups` reach the scenarios inside the suite rather than the suite class. Route a suite to a lane with `-Dsurefire.includes` instead, as `.github/workflows/mvn-test.yml` does for the openCypher TCK
- Never assert on raw wall-clock elapsed time. A full-suite run shares one JVM, and a stop-the-world pause of tens of
  seconds late in a 12,000-test run turns any bound with less headroom than that into a coin flip on the JVM's mood
  (#6260). Measure with `com.arcadedb.utility.StallAwareStopwatch` (engine test-jar) instead: it discounts the JVM-wide
  stall observed inside the measured window, so the bound can stay tight AND stop flaking. Pick the assertion that says
  what the number is for, because the message is what stops the next person from "fixing" a red run by loosening it:
  - `assertGaveUpWithin(bound, whatItSeparates)` when the bound is a tripwire between a bounded operation and an
    unbounded one. Generous is free here: a wider bound cannot turn a passing run red
  - `assertStayedUnder(bound, claim)` when the bound IS the assertion - a complexity claim with no other practical
    expression (a regex that must not backtrack exponentially, one deadline shared across rows rather than charged per
    row). Loosening it deletes the test
  - a short wait expected to TIME OUT needs neither, and neither does a lower bound (`isGreaterThan`): a stall only
    makes those more true
  - `@Timeout` is plain wall clock and cannot be discounted, so size it as a hang detector, not as a latency bound
- don't add Claude as author of any source code

## Build and Development Commands

Standard Maven and npm invocations apply. The non-obvious ones, each of which silently produces a *meaningless
green or spuriously red* run rather than an error that tells you what you did wrong:

- **Run unit tests**: `mvn verify` (use `verify`/`install`, not bare `mvn test`, for a full reactor run: the `arcadedb-gremlin-it` module consumes `arcadedb-gremlin`'s package-phase artifacts (the `shaded` uber-jar and `tests` test-jar), which a `test`-phase build never produces)
- **Testing a subset of modules needs `-am`**: `mvn -o -pl server -am test`, not `mvn -o -pl server test`. Without
  `-am` the module is the whole reactor, so it resolves `arcadedb-engine` and friends from `~/.m2` instead of from
  your working tree, and can run against an artifact that predates your edits. If you added or renamed a method this
  surfaces as a `NoSuchMethodError`; **if you only changed behaviour it surfaces as nothing at all** and the run tells
  you precisely nothing. Installing first (`mvn install -DskipTests`) is not a reliable substitute - it has been
  observed still resolving a stale artifact
- **To skip a slow or flaky class, use `-DexcludedGroups`, never `-Dtest='!Foo'`**: the `@Tag` lanes (`benchmark`,
  `slow`, `vector`) exist for this, e.g. `-DexcludedGroups=benchmark,vector`. Passing `-Dtest` in any form *replaces*
  Surefire's default include patterns, which drags `*IT` classes into the `test` phase, where they run without the
  Failsafe setup their fixtures need and fail by the hundred
- **Server tests bind fixed ports** (2480 and up). Anything already listening - a server left running by an IDE, a
  previous run, another agent - takes the requests instead, and the failures read as authentication errors
  (`403`, "Too many failed authentication attempts") rather than as a port conflict. Check with
  `lsof -nP -iTCP:2480 -sTCP:LISTEN` before believing a wall of red in the `server` module

## Development Guidelines

### Java Version
- **Legacy**: Java 17 support on `java17` branch

### Concurrency and Parallelism

**Core principle:** ArcadeDB avoids the JDK common ForkJoinPool (`ForkJoinPool.commonPool()`) for engine-internal parallelism. The common pool is shared with user-supplied code (Gremlin, Polyglot, custom SQL functions, application JVM) and JDK internals (parallel GC, reference handler), so long-running engine work submitted there starves user code and JDK housekeeping. Engine code that needs parallelism submits to one of the dedicated pools instead; the rule is documented at the head of `com.arcadedb.query.QueryEngineManager`'s class Javadoc.

Before adding any new parallelism to engine code, read the `engine-concurrency` skill (`.claude/skills/engine-concurrency/SKILL.md`). It carries the dedicated pool inventory with sizing and saturation policy, the lock-free read patterns and locking rules used on the hot paths, the Micrometer metric wiring, and the checklist a new pool must satisfy.

### Query Engine Notes
- **OpenCypher Engine**: `com.arcadedb.query.opencypher.*` has both optimizer and legacy execution paths — changes to clause handling may need updates in multiple paths
- **HTTP handlers**: `DatabaseAbstractHandler` is the base handler and wraps commands in transactions

#### Wire Protocol Module Dependencies
- **Standard**: All wire protocol modules (gremlin, graphql, mongodbw, redisw, postgresw, bolt, grpcw) must use `provided` scope for `arcadedb-server` dependency
- **Rationale**: Server remains the assembly point; prevents dependency duplication in distributions
- **Pattern**:
  - Main server dependency → scope: `provided`
  - Server test-jar → scope: `test`
  - Cross-module test dependencies → scope: `test` only (e.g., postgresw should not depend on gremlin for compilation)
  - Integration/format handlers → scope: `compile` only if in `src/main/java` (e.g., gremlin's GraphML/GraphSON handlers)
- **Enforcement**: Code review process ensures:
  - Protocol modules do NOT depend on other protocol modules in compile scope
  - Each protocol module has arcadedb-server in `provided` scope only (not compile)
  - Only the server assembly (package module) and coverage reporting modules can aggregate protocol modules

## Important Notes

- **Code formatting**: Prettier with `requirePragma: true` and `printWidth: 160` — only formats files with a `@format` pragma
- **Modular Builder**: Script to create custom distributions with selected modules (see `package/README-BUILDER.md`)
