# Design: Decouple engine antlr from Gremlin via engine bump + Gremlin shading

- Issue: sub-issue of [ArcadeData/arcadedb#4647](https://github.com/ArcadeData/arcadedb/issues/4647)
- Date: 2026-07-10
- Status: approved (pending written-spec review)

## Problem

`arcadedb-engine` ships ANTLR parsers generated with **antlr 4.9.1**, whose ATN is
serialized in "version 3" format. When the engine runs embedded in a Spring 6 /
Hibernate 6 application, those frameworks pull **antlr-runtime 4.10+**, which only
reads "version 4" ATN. Whichever `antlr4-runtime` wins on the classpath breaks the
other side, producing:

```
java.io.InvalidClassException: org.antlr.v4.runtime.atn.ATN;
Could not deserialize ATN with version 3 (expected 4).
```

There are two independent conflicts:

1. **Engine vs. host app** - the engine controls its own SQL/Cypher grammars and can
   move to any antlr version. Verified: the engine module has no Gremlin/TinkerPop
   dependency, and its parsers are regenerated at build time from `.g4` files.
2. **Engine vs. Gremlin** - the `gremlin` module depends on TinkerPop 3.8.1, which is
   pinned to antlr 4.9.1 **and ships precompiled `GremlinParser`/`GremlinLexer` classes**
   (inside `gremlin-language-3.8.1.jar`) serialized in v3 format. These cannot be
   regenerated without forking TinkerPop, so Gremlin is hard-locked to the 4.9.x runtime.

To let the engine adopt a modern antlr without breaking Gremlin, the two antlr
runtimes must coexist in one JVM under different package names.

## Goal

- Move `arcadedb-engine` to antlr **4.13.2** so it converges with what Spring 6 /
  Hibernate 6 bring, resolving the reported `InvalidClassException`.
- Keep Gremlin working on antlr **4.9.1** by relocating its bundled antlr into a private
  package, so the two runtimes never collide on the classpath.
- Prove both coexist in a single JVM via an end-to-end test.

## Non-goals

- **Do not** bump Gremlin's / TinkerPop's antlr version. Its precompiled parser needs
  v3-format ATN; bumping it would break Gremlin.
- **Do not** change the parent shade settings (`createDependencyReducedPom`, attachment,
  transformers). The relocation is added only to the gremlin module.
- **Maven-coordinate consumer isolation is deferred** (see "Deferred / follow-up").

## Approach

Two coordinated changes, delivered together so the coexistence test is genuinely
end-to-end.

### 1. Engine → antlr 4.13.2

- `engine/pom.xml`: `<antlr4.version>4.9.1</antlr4.version>` → `4.13.2`. This governs both
  the `antlr4-maven-plugin` (regenerates SQL/Cypher/Cypher25 parsers with v4 ATN) and the
  `antlr4-runtime` dependency.
- Fix any 4.9 → 4.13 API drift in the hand-written parser code. antlr is fully
  encapsulated in the engine - only these files reference it, all inside the parser
  packages:
  - `com/arcadedb/query/sql/antlr/` - `SQLAntlrParser`, `SQLASTBuilder`, `SQLErrorListener`
  - `com/arcadedb/query/opencypher/parser/` - `Cypher25AntlrParser`, `CypherASTBuilder`,
    `CypherExpressionBuilder`, `CypherErrorListener`, `ExpressionTypeDetector`, `ParserUtils`
- Generated parsers regenerate automatically; only the hand-written references may need
  edits (e.g. `ANTLRInputStream` → `CharStreams` if used). Exact edits discovered at
  implementation time by recompiling against 4.13.2.
- No cross-module impact: verified 0 references to `org.antlr.v4.runtime` in any module
  other than engine, and the engine's public parser entrypoints do not expose antlr types.

### 2. Gremlin: relocate its bundled antlr

The gremlin module already produces a full uber-jar via `maven-shade-plugin`
(parent config: `shadedArtifactAttached=true`, classifier `shaded`), and the distribution
ships that jar (`SHADED_MODULES="gremlin …"` in `package/arcadedb-builder.sh`;
`<classifier>shaded</classifier>` in `package/pom.xml`). The shaded jar already bundles the
antlr 4.9.1 classes and the gremlin grammar classes. We add one relocation so those
bundled classes and every reference to them move to a private package.

`gremlin/pom.xml`, in the already-declared shade plugin:

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

The child `<configuration>` merges with the inherited parent config; the parent defines
no `<relocations>`, so this adds cleanly.

Why this works and what it does **not** touch:

- The uber-jar bundles, together: `antlr4-runtime` 4.9.1 (~229 classes), `gremlin-language`'s
  generated parser (`GremlinParser`/`Lexer`/`BaseVisitor`, 493 classes), and `gremlin-core`'s
  hand-written visitors in the same package (`GremlinAntlrToJava`, `VariableResolver`,
  `GremlinQueryParser`, 29 classes). All three reference `org.antlr`. Because they are in
  the same shade pass, all references are rewritten consistently to the shaded package.
- We relocate **only** `org.antlr`. The `org.apache.tinkerpop.gremlin.language.grammar`
  package is left in place - it does not clash with anything (the engine has no Gremlin
  grammar), and `gremlin/src/main/java/com/arcadedb/gremlin/ArcadeGraph.java` imports
  `…grammar.VariableResolver`, which stays valid.
- The generated parser's serialized ATN is a static data blob (not package-named strings),
  so relocation does not corrupt it; only the `ATNDeserializer` class reference is rewritten.
  antlr relocation is well-precedented (Trino/Presto/Spark shade antlr).
- No reflection/ServiceLoader on antlr: antlr4-runtime ships no `META-INF/services`, and
  Gremlin loads the parser by direct type reference.
- `gremlin/pom.xml`'s `<antlr4.version>` stays **4.9.1** (unchanged).

### Resulting classpath (shipped distribution)

| Component                                     | antlr package                                    | version         |
|-----------------------------------------------|--------------------------------------------------|-----------------|
| arcadedb-engine + host app (Hibernate/Spring) | `org.antlr.v4.runtime`                           | 4.13.2 (real)   |
| arcadedb-gremlin (shaded jar)                 | `com.arcadedb.gremlin.shaded.org.antlr.v4.runtime` | 4.9.1 (private) |

No collision by construction. The distribution copies the shaded uber-jar directly into
`lib/`, so no transitive re-resolution reintroduces a conflicting antlr.

## Files touched

- `engine/pom.xml` - antlr version bump.
- `engine/src/main/java/com/arcadedb/query/sql/antlr/*`,
  `engine/src/main/java/com/arcadedb/query/opencypher/parser/*` - fix API drift (as needed).
- `gremlin/pom.xml` - add the `<relocations>` block.
- `ATTRIBUTIONS.md` / `NOTICE` - confirm antlr attribution present for the shaded gremlin
  jar (which bundles antlr) and for antlr as a distribution lib. The engine is an unshaded
  plain jar and ships antlr as a separate `lib/` jar, so bumping its version changes no
  licensing; only confirm the existing attribution and adjust the version if it is listed.
- New coexistence test (see below).

## Testing

1. **Engine parses on 4.13.2 (API-drift safety net):** full engine SQL + Cypher parser
   test suites green after regeneration - `mvn -pl engine test`.
2. **Gremlin shaded jar is self-consistent:** after `mvn -pl gremlin -am package`,
   assert on the uber-jar that `com/arcadedb/gremlin/shaded/org/antlr/…` is present and
   `org/antlr/…` is absent; `javap -p` a couple of `GremlinParser` / `GremlinAntlrToJava`
   classes and confirm the constant pool contains only `com/arcadedb/gremlin/shaded/org/antlr`,
   zero `org/antlr`.
3. **Gremlin still works:** existing gremlin test suite green (proves the relocated parser
   still deserializes its v3 ATN and parses Gremlin).
4. **Coexistence (the decisive test for #4647):** a new test in the gremlin module that,
   in one JVM, exercises the engine's antlr 4.13.2 (run a SQL parse and a Cypher parse)
   **and** a Gremlin traversal through the shaded path - both succeed, no
   `Could not deserialize ATN` error. Placed in the gremlin module because it is the only
   module that simultaneously sees real antlr 4.13.2 (via the engine dependency) and its
   own shaded 4.9.1. Tagged `@Tag("slow")` if elapsed time warrants.

## Deferred / follow-up

- **Maven-coordinate consumer isolation.** The shaded artifact's published pom still lists
  `antlr4-runtime:4.9.1` (parent sets `createDependencyReducedPom=false`), so a Maven user
  depending on both `arcadedb-gremlin` and `arcadedb-engine` could still pull real antlr
  4.9.1 transitively and re-trigger mediation. This does not affect the shipped
  distribution (direct jar copy) or the reporter's engine-only embedding. A follow-up can
  mark gremlin's `antlr4-runtime` `<optional>true</optional>` (or `provided`) so it is not
  exported transitively.
