# Design: Maven-coordinate consumer isolation for arcadedb-gremlin antlr

- Issue: [ArcadeData/arcadedb#5217](https://github.com/ArcadeData/arcadedb/issues/5217), follow-up to [#5216](https://github.com/ArcadeData/arcadedb/pull/5216), sub-issue chain of [#4647](https://github.com/ArcadeData/arcadedb/issues/4647)
- Date: 2026-07-12
- Status: approved (pending written-spec review)

## Problem

PR #5216 fixed antlr coexistence for two audiences:

- the **shipped distribution** (the builder copies gremlin's shaded uber-jar, with antlr
  relocated to `com.arcadedb.gremlin.shaded.org.antlr`, straight into `lib/` next to the plain
  engine jar on antlr 4.13.2), and
- **engine-only embedders** (engine is now a plain jar on real `org.antlr` 4.13.2).

It **deferred** the case of a **Maven-coordinate consumer**: a third-party app that declares
direct dependencies on both `arcadedb-gremlin` and `arcadedb-engine` (rather than consuming the
shipped distribution jars).

The leak: `gremlin/pom.xml` declares `org.antlr:antlr4-runtime:4.9.1` as an ordinary
(compile-scope, exported) dependency, and the parent sets `createDependencyReducedPom=false`, so
gremlin's published pom - for both the main jar and the `shaded` classifier - still lists
`antlr4-runtime:4.9.1` transitively. A consumer's dependency graph therefore contains **both**
the engine's `4.13.2` and gremlin's `4.9.1`; Maven mediation collapses them to a single version,
and whichever side loses hits the ATN-version mismatch again:

```
java.io.InvalidClassException: org.antlr.v4.runtime.atn.ATN;
Could not deserialize ATN with version 3 (expected 4).
```

## Key facts that shape the design

1. The **unshaded** `arcadedb-gremlin` jar references bare `org.antlr` and can never share a
   classpath with the engine's antlr - both resolve to the single mediated version, breaking one
   parser family. No pom change can fix that; the only supported "both modules" runtime path is
   the **`shaded` classifier**, whose relocated `com.arcadedb.gremlin.shaded.org.antlr` parser is
   self-contained.
2. `<optional>true</optional>` and `<scope>provided</scope>` are **not** interchangeable here:
   - `optional` (compile scope): antlr stays on gremlin's own compile/test classpath and
     **stays bundled + relocated in the shaded uber-jar** (maven-shade includes optional-flagged
     dependencies in its artifactSet), but is no longer exported to downstream consumers. ✅
   - `provided`: maven-shade **excludes** provided-scope dependencies from the uber-jar, so the
     relocated antlr would vanish and the shaded gremlin jar would break - a regression of #5216. ❌
   So `optional` is the correct lever.
3. With gremlin's own antlr marked `optional`, the only antlr edge left in a
   `engine + gremlin` consumer graph is the engine's `4.13.2`, reached via
   `arcadedb-gremlin` → `arcadedb-integration` (compile) → `arcadedb-engine` → `antlr 4.13.2`.
   That is the correct outcome for a `gremlin:shaded` consumer (whose parser needs no bare antlr).

## Goal

- Stop `arcadedb-gremlin` from exporting `antlr4-runtime:4.9.1` transitively, so a consumer of
  both coordinates resolves antlr cleanly to the engine's `4.13.2` with no ATN regression.
- Guard the invariant with a build-time check so it cannot silently regress.
- Document the supported embedding path (engine plain + `arcadedb-gremlin:shaded`).

## Non-goals

- **Do not** enable `createDependencyReducedPom` (a global parent setting; changing it is out of
  scope and affects every shaded module and the distribution's dependency bookkeeping).
- **Do not** bump TinkerPop's / gremlin's antlr (its precompiled parser needs the v3 ATN format).
- **Do not** touch the engine, the shipped distribution, or the `gremlin-it` coexistence suite.
- Making the **unshaded** gremlin coordinate coexist with the engine is impossible and is not
  attempted; the `shaded` classifier is the documented path.

## Approach

Three coordinated changes.

### 1. `gremlin/pom.xml` - mark antlr `optional`

```xml
<dependency>
    <groupId>org.antlr</groupId>
    <artifactId>antlr4-runtime</artifactId>
    <version>${antlr4.version}</version>
    <optional>true</optional>
</dependency>
```

`gremlin/pom.xml`'s `<antlr4.version>` stays **4.9.1** (gremlin still compiles/tests and shades
against 4.9.1). Only the transitive export changes.

### 2. New `gremlin-consumer-it` reactor module - regression guard

A minimal module that ships nothing, placed **after** `gremlin` (and after `gremlin-it`) in the
parent `<modules>` so the gremlin artifact is installed first. It exists only to assert, at build
time, that a real downstream consumer's dependency graph is free of antlr 4.9.1.

- `packaging`: `jar`; `maven.install.skip` = `true`, `maven.deploy.skip` = `true` (mirrors
  `gremlin-it` - produces no shipped artifact).
- Dependencies (the **plain** `arcadedb-gremlin` coordinate, which is the stricter test of the
  `optional` flag - it is the artifact that would most readily re-pull bare antlr):
  - `arcadedb-engine` (real `org.antlr` 4.13.2)
  - `arcadedb-gremlin` (plain coordinate, no classifier)
- `maven-enforcer-plugin`, `enforce` bound to the `validate` phase, `bannedDependencies` rule:
  - `<searchTransitive>true</searchTransitive>`
  - ban `org.antlr:antlr4-runtime:4.9.1`
  - `<fail>true</fail>`

Behavior: before change 1 the transitive graph contains `antlr4-runtime:4.9.1` → enforcer fails
the build; after change 1 it is gone → build passes. `bannedDependencies` (specific artifact) is
used rather than `banTransitiveDependencies` (which would blanket-ban everything transitive from
the engine and require a large allowlist).

No Java sources, no test execution: the unshaded engine + gremlin combination is runtime-
incompatible by design (see Key fact 1), so this module only ever asserts on the **resolved
dependency graph**, never runs code.

### 3. README docs + release-notes reminder

Add a short subsection to `README.md` immediately after the existing Maven-usage block
(around line 224):

> ### Embedding Gremlin alongside the engine
>
> When you depend on both `arcadedb-engine` and `arcadedb-gremlin` by Maven coordinate, use the
> `shaded` classifier for gremlin. Its antlr is relocated into a private package so it never
> collides with the engine's antlr 4.13.2. The plain `arcadedb-gremlin` jar references bare
> `org.antlr` and cannot share a classpath with the engine.
>
> ```xml
> <dependency>
>     <groupId>com.arcadedb</groupId>
>     <artifactId>arcadedb-engine</artifactId>
>     <version>26.8.1</version>
> </dependency>
> <dependency>
>     <groupId>com.arcadedb</groupId>
>     <artifactId>arcadedb-gremlin</artifactId>
>     <version>26.8.1</version>
>     <classifier>shaded</classifier>
> </dependency>
> ```

**Release-notes reminder (manual, maintainer action):** the 26.8.1 GitHub release notes (the
version shipping #5216 + this follow-up) should call out that embedders depending on both modules
must consume the `arcadedb-gremlin:shaded` classifier. GitHub Releases are published by the
maintainer and are not editable from this change.

## Files touched

- `gremlin/pom.xml` - add `<optional>true</optional>` to the `antlr4-runtime` dependency.
- `pom.xml` (parent) - add `<module>gremlin-consumer-it</module>` after `gremlin-it`.
- `gremlin-consumer-it/pom.xml` - new module (enforcer `bannedDependencies` only).
- `README.md` - "Embedding Gremlin alongside the engine" snippet.

## Testing / verification

1. **Guard bites:** temporarily revert change 1 (drop `<optional>`), run
   `mvn -pl gremlin-consumer-it -am validate` → enforcer **fails** with antlr 4.9.1 banned.
   Restore change 1 → same command **passes**. This is the regression proof.
2. **Clean graph:** `mvn -pl gremlin-consumer-it dependency:tree` (with change 1) shows only
   `org.antlr:antlr4-runtime:4.13.2`, no 4.9.1.
3. **Shaded jar unchanged:** `mvn -pl gremlin -am package` then assert the uber-jar still bundles
   `com/arcadedb/gremlin/shaded/org/antlr/…` and no bare `org/antlr/…` - confirms `optional` did
   not strip the relocated antlr from the shaded jar.
4. **gremlin-it still green:** `mvn -pl gremlin,gremlin-it -am install` - the coexistence suite is
   unaffected.
5. **Full build:** `mvn clean install -DskipTests` - BUILD SUCCESS across all modules.

## Deferred / follow-up

None. This closes the Maven-coordinate consumer gap explicitly deferred by #5216. If a future
change enables `createDependencyReducedPom` globally, the `gremlin-consumer-it` guard remains a
valid (and then redundant) safety net and can stay.
