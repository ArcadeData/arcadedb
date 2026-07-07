# Bolt type-fidelity trio design (#4997 + #4998 + #4999)

**Date:** 2026-07-05
**Epic:** #4882 (Bolt Driver Compatibility Certification)
**Tracking issue:** #4890 (Group C - protocol/type-fidelity gaps)
**Closes:** #4997, #4998, #4999 (partial close of #4890)
**Conformance cells flipped:** TYPE-011, TYPE-012, TYPE-003, ERR-002

## Motivation

The Group B conformance suites pinned four red cells in `bolt/conformance/spec.yaml`, all
`current_status: expected-fail` and tracked by #4890. This design closes the three writer/validator-side
sub-issues that share one fix shape - mirroring the shipped temporal work (#4907):

- **TYPE-011 / TYPE-012 (#4997)**: `Duration` and spatial `Point` have no handling in
  `BoltStructureMapper`/`PackStreamWriter`; a returned duration falls through to `value.toString()` and a
  point serializes as a generic map, so official drivers never receive native `Duration`/`Point` objects.
- **TYPE-003 (#4998)**: `structure/BoltPath.java` exists (tag `0x50`) but has zero call sites - path
  results are emitted as a `toString()` string instead of a native Bolt `Path`.
- **ERR-002 (#4999)**: `CypherSemanticValidator` throws `CommandParsingException`, the same class used for
  genuine ANTLR syntax errors, so the Bolt RUN handler cannot distinguish them and
  `Neo.ClientError.Statement.SemanticError` is dead code.

The larger #4890 children (#5000 result counters, #5001 5.x negotiation, #5002 HA ROUTE) are out of scope
here - they touch the query engine, protocol negotiation, and a live-cluster harness respectively.

## Governing principle

Writer-side only, mirroring #4907. No query-engine changes. All three fixes live almost entirely in the
`bolt` module; #4999 adds one small exception class in `engine`. The single inbound param-decode point is
extended for round-tripping. Existing temporal/Node/Relationship serialization is untouched.

## Architecture

### Key files

- `bolt/src/main/java/com/arcadedb/bolt/structure/BoltStructureMapper.java` - outbound
  `toPackStreamValue`, inbound `fromPackStreamValue`/`fromTemporalStructure`. Primary change surface.
- `bolt/src/main/java/com/arcadedb/bolt/structure/BoltPath.java` - exists, tag `0x50`; gains call sites.
- `bolt/src/main/java/com/arcadedb/bolt/structure/BoltTemporalStructure.java` - generic
  `(signature, fields...)` `PackStreamStructure`; reused for Duration.
- New `bolt/src/main/java/com/arcadedb/bolt/structure/BoltPointStructure.java` - spatial struct
  (tag `0x58`/`0x59`).
- `bolt/src/main/java/com/arcadedb/bolt/BoltNetworkExecutor.java` - `handleRun` error dispatch
  (lines ~629-640).
- `bolt/src/main/java/com/arcadedb/bolt/BoltErrorCodes.java` - `SEMANTIC_ERROR` already defined.
- New `engine/src/main/java/com/arcadedb/exception/CommandSemanticException.java` - subclass of
  `CommandParsingException` (precedent: `CommandSQLParsingException`).
- `engine/src/main/java/com/arcadedb/query/opencypher/parser/CypherSemanticValidator.java` - retarget
  its ~51 `throw new CommandParsingException(...)` sites to `CommandSemanticException`.

### Engine value types (source of truth)

- **Duration**: `com.arcadedb.query.opencypher.temporal.CypherDuration` - concrete class,
  `getMonths()/getDays()/getSeconds()/getNanosAdjustment()`, public ctor
  `CypherDuration(long months, long days, long seconds, int nanosAdjustment)`. Maps 1:1 to Bolt tag `0x45`.
- **Point**: **no dedicated class**. Cypher `point()` returns `java.util.LinkedHashMap`. Keys: `x`,`y`,
  optional `z`; `crs` string (`cartesian`/`cartesian-3D`/`WGS-84`/`WGS-84-3D`); `srid` present for WGS-84
  and for explicit cartesian, otherwise absent. WGS-84 also carries `longitude`/`latitude`. Detection is
  structural.
- **Path**: `com.arcadedb.query.opencypher.traversal.TraversalPath` - parallel `getVertices()` /
  `getEdges()` lists, invariant `vertices.size()==edges.size()+1`, `edges[i]` joins `vertices[i]` and
  `vertices[i+1]`. Direction is **not** stored.

## Component 1 - #4997 Duration + Point (TYPE-011, TYPE-012)

### Outbound

In `BoltStructureMapper`:

- **Duration** (native, tag `0x45`, fields `[months, days, seconds, nanoseconds]`): add a `CypherDuration`
  branch inside the temporal handling (`toTemporalStructure`), returning
  `new BoltTemporalStructure(SIG_DURATION, months, days, seconds, (long) nanosAdjustment)`. Update the
  existing comment that states `CypherDuration` is intentionally not handled. Duration is a Cypher temporal
  type, so reusing `BoltTemporalStructure` is semantically consistent with #4907.
- **Point** (tag `0x58` 2D `[srid, x, y]` / `0x59` 3D `[srid, x, y, z]`): add a `toPointStructure(Object)`
  helper returning a new `BoltPointStructure` or `null`. Detection: value is a `Map` carrying key `crs`
  **and** numeric (`x` and `y`) or (`longitude` and `latitude`). This branch runs **before** the generic
  `Map` branch in `toPackStreamValue`. SRID resolution: use `srid` if present, else derive -
  `cartesian`->7203, `cartesian-3D`->9157, `WGS-84`->4326, `WGS-84-3D`->4979. Dimensionality: 3D iff a
  numeric `z` (or `height`) key is present. `srid` is written as an integer (Long), `x`/`y`/`z` as doubles.

`BoltPointStructure` is a thin `PackStreamStructure` like `BoltPath`: holds `srid,x,y` (+optional `z`),
`getSignature()` returns `0x58`/`0x59`, `writeTo` writes the header then fields.

### Inbound (param round-trip)

Single decode point: `BoltMessage:108` -> `BoltStructureMapper.fromPackStreamValue` ->
`fromTemporalStructure`. Generalize the struct decoder to also handle:

- **`0x45`** -> `new CypherDuration(months, days, seconds, (int) nanos)`.
- **`0x58`/`0x59`** -> a `LinkedHashMap` carrying `x`,`y`(,`z`), `srid`, and a **derived `crs` key**
  (7203/9157->`cartesian`/`cartesian-3D`, 4326/4979->`WGS-84`/`WGS-84-3D`). The `crs` key is required so
  `RETURN $p AS echo` re-encodes as a Point rather than a bare map.

Reuse the existing arity/type-guard pattern: a malformed struct (wrong field count/types) is returned
opaque rather than crashing the connection.

## Component 2 - #4998 native Path (TYPE-003)

Add a `TraversalPath` branch in `toPackStreamValue` that constructs a `BoltPath`:

1. Walk `getVertices()`/`getEdges()` in order.
2. Build **deduplicated** `nodes` (`BoltNode` via `toNode`) and `rels` (`BoltUnboundRelationship` via
   `toUnboundRelationship`) lists keyed by RID - Neo4j Path encoding requires unique node/rel lists.
3. Build the flat signed `indices` list `[relIdx, nodeIdx, ...]` per hop (Bolt convention documented in
   `BoltPath`): `relIdx` is 1-based into `rels`, **positive if the edge was traversed forward**
   (`edges[i].getOut().equals(vertices[i].getIdentity())`), negative otherwise; `nodeIdx` is the 0-based
   index into `nodes` of the node reached at `vertices[i+1]`. Node 0 (the start) is implicit.

Edge cases covered by unit tests: single-hop, multi-hop, repeated nodes (dedup), self-loop, and a
backward-traversed edge (negative index).

## Component 3 - #4999 semantic vs syntax errors (ERR-002)

1. New `engine/.../exception/CommandSemanticException extends CommandParsingException` (three ctors,
   mirroring `CommandSQLParsingException`).
2. Retarget every `throw new CommandParsingException(...)` in `CypherSemanticValidator` to
   `CommandSemanticException` - the entire class is semantic validation by definition.
3. In `BoltNetworkExecutor.handleRun`, add `catch (final CommandSemanticException e)` mapping to
   `BoltErrorCodes.SEMANTIC_ERROR`, ordered **before** the existing
   `catch (final CommandParsingException e)` -> `SYNTAX_ERROR`.

**Backward compatibility:** `CommandSemanticException` is a `CommandParsingException`, so every existing
`catch (CommandParsingException)` site (HTTP `AbstractServerHttpHandler:315`, Postgres, Gremlin, GraphQL,
Redis; and the rethrow points in `Cypher25AntlrParser`/`CypherStatementCache`) keeps treating semantic
errors exactly as today. Only the Bolt handler gains the new branch. Genuine ANTLR parse failures continue
to throw plain `CommandParsingException` -> `SYNTAX_ERROR`.

## Testing strategy (TDD)

### Hard gate - module + engine unit tests (fast, no server)

- `bolt/.../BoltTypeRoundTripTest.java`:
  - Flip TYPE-011: `toPackStreamValue(CypherDuration)` -> `BoltTemporalStructure` sig `0x45`, fields
    `[months, days, seconds, nanos]`.
  - Add TYPE-012: `toPackStreamValue(point-map)` -> `BoltPointStructure` sig `0x58`, srid 7203, x=12.34,
    y=56.78; a WGS-84 map -> srid 4326; a 3D map -> `0x59`.
  - Add TYPE-003: `toPackStreamValue(TraversalPath)` -> `BoltPath` with expected nodes/rels/signed
    indices (multi-hop, dedup, backward-edge cases).
  - Inbound decode: `fromPackStreamValue(StructureValue 0x45)` -> `CypherDuration`;
    `fromPackStreamValue(StructureValue 0x58)` -> map with `crs`; malformed struct stays opaque.
- Engine test: `CypherSemanticValidator.validate(undefined-variable stmt)` throws
  `CommandSemanticException`, and `CommandSemanticException` is a `CommandParsingException`.
- Bolt handler test: RUN `RETURN undefinedVariable` -> FAILURE code `SEMANTIC_ERROR`; a genuine syntax
  error -> `SYNTAX_ERROR` (mirror existing `BoltNetworkExecutor` test harness).

### Conformance flip (CI end-to-end against branch-built image)

- `bolt/conformance/spec.yaml`: TYPE-003/011/012 + ERR-002 -> `current_status: passing`; drop
  `known_limitation` and `tracking_issue` for those cells. Run `validate_spec.py`.
- Flip the per-suite markers: JS `it.failing`->`it` (`e2e-js`), Java `assertExpectedFailure(...)`->direct
  assertion (`e2e/RemoteBoltDatabaseIT` + module test), Python xfail removal (`e2e-python`), C# xfail
  (`e2e-csharp`), Go skip (`e2e-go`).

Per project convention, the compile-and-run gate is `mvn -q -pl bolt -am test` for the module tests plus
the affected engine tests; full e2e verification runs in CI (needs the branch-built Docker image).

## Delivery

- Worktree: `.worktrees/feat/4890-bolt-type-fidelity`, branch `feat/4890-bolt-type-fidelity` off `main`.
- Commits split by component for reviewability: (1) Duration+Point, (2) Path, (3) semantic errors,
  (4) conformance/spec + suite flips.
- One PR closing #4997/#4998/#4999 and ticking those boxes on #4890.
- Review loop: Claude + Gemini, >= 4 cycles, each answered via `/receiving-code-review`.

## Out of scope

- #5000 (ResultSummary write counters - query-engine change).
- #5001 (Bolt 5.x negotiation - protocol + decision, coordinates with #4884).
- #5002 (HA-aware ROUTE - needs live multi-node cluster harness).
- Any change to the advertised `Neo4j/5.26.0 compatible` server identity.
