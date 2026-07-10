# Design: Engine antlr 4.13.2 (pristine) + Gremlin antlr relocation, tested via a gremlin-it module

- Issue: [ArcadeData/arcadedb#5208](https://github.com/ArcadeData/arcadedb/issues/5208), sub-issue of [#4647](https://github.com/ArcadeData/arcadedb/issues/4647)
- Date: 2026-07-10
- Status: approved (pending written-spec review)

## Problem

`arcadedb-engine` ships ANTLR parsers generated with **antlr 4.9.1** (ATN serialized in
"version 3" format). Embedded in a Spring 6 / Hibernate 6 app, those frameworks pull
**antlr-runtime 4.10+** (reads only "version 4" ATN). Whichever `antlr4-runtime` wins on the
classpath breaks the other side:

```
java.io.InvalidClassException: org.antlr.v4.runtime.atn.ATN;
Could not deserialize ATN with version 3 (expected 4).
```

Two independent conflicts:

1. **Engine vs. host app** - the engine controls its own SQL/Cypher grammars and can move
   to any antlr version. It has no Gremlin/TinkerPop dependency, and regenerates its parsers
   at build time.
2. **Engine vs. Gremlin** - the `gremlin` module depends on TinkerPop 3.8.1, pinned to
   antlr 4.9.1, which **ships precompiled `GremlinParser`/`GremlinLexer`** (v3 ATN) that
   cannot be regenerated without forking TinkerPop. Gremlin is hard-locked to the 4.9.x
   runtime.

## Key finding that shapes the design

The ANTLR SQL parser is **always used** (`GlobalConfiguration`: *"The ANTLR4-based SQL parser
is always used."*). The gremlin module's own code (`ArcadeSQL`, `GremlinQueryEngine`) and 25
of its 36 test files open a database and run SQL/Cypher/Gremlin. So a single JVM in the
gremlin module loads **both** the engine's SQL/Cypher parser (v4 after the bump, needs 4.13.2)
**and** TinkerPop's `GremlinParser` (v3, needs 4.9.1).

Relocation only exists in the gremlin **shaded** jar, produced at the `package` phase. The
normal `test` phase runs against **unshaded** classes, where Maven mediation collapses antlr
to one version and the other parser family fails. Therefore gremlin's SQL/Cypher-touching
tests can only pass when run **against the shaded jar**.

## Goal

- Move `arcadedb-engine` to antlr **4.13.2** as a **plain, unshaded jar** (engine stays
  pristine - no fat-jar shading of the core module). This fixes the reporter's
  `InvalidClassException`.
- Keep Gremlin on antlr **4.9.1**, relocated into a private package inside its shaded jar so
  the two runtimes never collide.
- Run gremlin's tests against the shaded jar so coexistence is genuinely exercised, without
  moving test sources.

## Non-goals

- **Do not** bump Gremlin's / TinkerPop's antlr (its precompiled parser needs v3 ATN).
- **Do not** shade the engine (engine purity is the whole point of this variant).
- **Maven-coordinate consumer isolation** for third-party embedders is deferred (see below).

## Approach

Three coordinated changes.

### 1. Engine -> antlr 4.13.2 (plain jar)

- `engine/pom.xml`: `<antlr4.version>4.9.1</antlr4.version>` -> `4.13.2`. Governs the
  `antlr4-maven-plugin` (regenerates SQL/Cypher/Cypher25 parsers with v4 ATN) and the
  `antlr4-runtime` dependency.
- Fix any 4.9 -> 4.13 API drift in the hand-written parser code. antlr is fully encapsulated
  in the engine (0 references in any other module). The hand-written files already use the
  modern API (`CharStreams.fromString`, `CommonTokenStream`, `BailErrorStrategy`,
  `BaseErrorListener`, `ParserRuleContext`, `Token`, `ParseTree`, `TerminalNode`,
  `PredictionMode`, `Interval`, `ParseCancellationException`) - all stable across 4.9->4.13,
  so a clean recompile is expected. Files:
  - `com/arcadedb/query/sql/antlr/` - `SQLAntlrParser`, `SQLASTBuilder`, `SQLErrorListener`
  - `com/arcadedb/query/opencypher/parser/` - `Cypher25AntlrParser`, `CypherASTBuilder`,
    `CypherExpressionBuilder`, `CypherErrorListener`, `ExpressionTypeDetector`, `ParserUtils`
- Engine remains an ordinary (unshaded) jar. Downstream modules consume real
  `org.antlr.v4.runtime` 4.13.2.

### 2. Gremlin: relocate its bundled antlr (unchanged module otherwise)

`gremlin/pom.xml`, in the already-declared shade plugin (child config merges with the parent;
parent defines no `<relocations>`):

```xml
<plugin>
    <groupId>org.apache.maven.plugins</groupId>
    <artifactId>maven-shade-plugin</artifactId>
    <configuration>
        <relocations>
            <relocation>
                <pattern>org.antlr</pattern>
                <shadedPattern>com.arcadedb.gremlin.shaded.org.antlr</shadedPattern>
            </relocation>
        </relocations>
    </configuration>
</plugin>
```

- The shaded uber-jar bundles antlr 4.9.1 + gremlin-language's generated parser + gremlin-core's
  visitors together, so all `org.antlr` references are rewritten in one consistent pass.
- Only `org.antlr` is relocated. `org.apache.tinkerpop.gremlin.language.grammar` is left in
  place (no clash; `ArcadeGraph`'s `…grammar.VariableResolver` import stays valid).
- The generated parser's serialized ATN is a static data blob; relocation does not corrupt it.
  antlr relocation is well-precedented (Trino/Presto/Spark).
- `gremlin/pom.xml`'s `<antlr4.version>` stays **4.9.1**.
- Because gremlin's SQL/Cypher-touching tests cannot pass unshaded after the engine bump,
  the gremlin module **skips test execution** (surefire `<skipTests>true</skipTests>`, with a
  comment pointing at gremlin-it) while still **compiling** tests and building the **test-jar**
  (the `maven-jar-plugin` test-jar goal is bound to `package`, independent of surefire).

### 3. New `arcadedb-gremlin-it` module - runs gremlin's tests against the shaded jar

A thin reactor module placed **after** `gremlin` in the parent `<modules>` so the shaded jar
and test-jar exist first. It executes the existing gremlin tests (from the test-jar, no source
moves) plus a new explicit coexistence test, on a classpath where the two antlr runtimes
coexist:

- `arcadedb-gremlin` classifier `shaded`, scope `test`, with a wildcard exclusion
  (`<exclusion><groupId>*</groupId><artifactId>*</artifactId></exclusion>`, supported by
  Maven 3.9.12) so it contributes ONLY the self-contained uber-jar (which already bundles
  tinkerpop, groovy, netty, gremlin classes, and the relocated antlr 4.9.1) and does not
  re-pull the original antlr 4.9.1 / tinkerpop via its non-reduced pom.
- `arcadedb-gremlin` classifier `tests` (test-jar), scope `test`, same wildcard exclusion -
  supplies the compiled test classes and resources.
- `arcadedb-engine` (real `org.antlr.v4.runtime` 4.13.2), `arcadedb-server` test-jar, and the
  same test dependencies gremlin uses (junit, assertj, junit-vintage-engine).
- surefire configured with `<dependenciesToScan>com.arcadedb:arcadedb-gremlin</dependenciesToScan>`
  to discover and run the test classes from the test-jar.

Resulting test classpath: engine SQL/Cypher parser uses real `org.antlr.v4.runtime` 4.13.2;
Gremlin's parser uses the relocated `com.arcadedb.gremlin.shaded.org.antlr.v4.runtime` 4.9.1
inside the shaded jar. They coexist - the exact scenario #4647 needs.

### Resulting classpaths

Shipped distribution (the builder copies the shaded uber-jar directly into `lib/`):

| Component                                     | antlr package                                       | version         |
|-----------------------------------------------|-----------------------------------------------------|-----------------|
| arcadedb-engine + host app (Hibernate/Spring) | `org.antlr.v4.runtime`                              | 4.13.2 (real)   |
| arcadedb-gremlin (shaded jar)                 | `com.arcadedb.gremlin.shaded.org.antlr.v4.runtime` | 4.9.1 (private) |

gremlin-it test classpath mirrors this (engine real 4.13.2 + shaded gremlin 4.9.1).

## Files touched

- `engine/pom.xml` - antlr version bump.
- `engine/src/main/java/com/arcadedb/query/sql/antlr/*`,
  `engine/src/main/java/com/arcadedb/query/opencypher/parser/*` - fix API drift if any.
- `gremlin/pom.xml` - add `<relocations>`; skip surefire execution (keep test-jar).
- `pom.xml` (parent) - add `<module>gremlin-it</module>` after `gremlin`.
- `gremlin-it/pom.xml` - new module (dependencies + surefire `dependenciesToScan`).
- `gremlin-it/src/test/java/.../AntlrCoexistenceIT.java` - new coexistence test.
- `ATTRIBUTIONS.md` / `NOTICE` - confirm antlr attribution for the shaded gremlin jar and the
  engine's antlr lib; adjust version if listed.

## Testing

1. **Engine parses on 4.13.2:** full engine SQL + Cypher parser suites green after
   regeneration - `mvn -pl engine test` (API-drift safety net).
2. **Shaded gremlin jar is self-consistent:** after `mvn -pl gremlin -am package`, assert on
   the uber-jar that `com/arcadedb/gremlin/shaded/org/antlr/…` is present and `org/antlr/…`
   is absent; `javap -p` a couple of `GremlinParser` / `GremlinAntlrToJava` classes and
   confirm the constant pool references only `com/arcadedb/gremlin/shaded/org/antlr`.
3. **Gremlin behavior preserved:** the migrated gremlin suite (run in gremlin-it against the
   shaded jar) is green - proves the relocated parser still deserializes its v3 ATN.
4. **Coexistence (decisive for #4647):** `AntlrCoexistenceIT` in gremlin-it, in one JVM, runs
   an engine SQL parse + a Cypher parse (antlr 4.13.2) **and** a Gremlin traversal (shaded
   4.9.1) - all succeed, no `Could not deserialize ATN` error.

## Deferred / follow-up

- **Maven-coordinate consumer isolation.** The shaded artifact's published pom still lists
  `antlr4-runtime:4.9.1` (parent `createDependencyReducedPom=false`), so a Maven user
  depending on both `arcadedb-gremlin` and `arcadedb-engine` could still pull real antlr 4.9.1
  transitively. Does not affect the shipped distribution (direct jar copy) or the reporter's
  engine-only embedding. Follow-up can mark gremlin's `antlr4-runtime` `<optional>true</optional>`.
- **Reactor test-runtime note.** The gremlin module no longer runs its own tests directly; they
  run in gremlin-it. If a future change reintroduces unshaded coexistence, revisit.
