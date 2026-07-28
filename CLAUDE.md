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
  - ✅ ALLOWED: Apache 2.0, MIT, BSD (2/3-Clause), EPL 1.0/2.0, UPL 1.0, EDL 1.0, LGPL 2.1+ (for libraries only), CC0/Public Domain
  - ❌ FORBIDDEN: GPL, AGPL, proprietary licenses without explicit permission, SSPL, Commons Clause
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
  - Apply at the class level when every method in the class is slow; at the method level when only some methods are slow
  - Required imports: `import org.junit.jupiter.api.Tag;`
- don't add Claude as author of any source code

## Build and Development Commands

Standard Maven and npm invocations apply. The non-obvious one:

- **Run unit tests**: `mvn verify` (use `verify`/`install`, not bare `mvn test`, for a full reactor run: the `arcadedb-gremlin-it` module consumes `arcadedb-gremlin`'s package-phase artifacts (the `shaded` uber-jar and `tests` test-jar), which a `test`-phase build never produces)

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
