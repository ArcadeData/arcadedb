# Bolt Server Identity

ArcadeDB's Bolt module advertises a specific Neo4j server identity to
official Neo4j drivers. This is a deliberate, already-shipped decision -
not a placeholder - documented here so every result produced by the
[conformance spec](conformance/spec.yaml) (issue #4883) is interpretable
against a stated envelope, per epic #4882.

## Advertised identity

Two independent places hardcode a Neo4j `5.26.0` identity. They are not
derived from a shared constant - a future change to one without the other
would be a real inconsistency, not a formatting nit.

1. **HELLO/SUCCESS response** - the `server` metadata field returned to
   every client on connection:

   ```java
   metadata.put("server", "Neo4j/5.26.0 compatible (ArcadeDB " + Constants.getRawVersion() + ")");
   ```

   `bolt/src/main/java/com/arcadedb/bolt/BoltNetworkExecutor.java:422`
   (`handleHello`).

2. **Synthetic `CALL dbms.components()` response** - intercepted and
   answered without touching the query engine:

   ```java
   syntheticResults.add(List.of("Neo4j Kernel", List.of("5.26.0"), "community"));
   ```

   `bolt/src/main/java/com/arcadedb/bolt/BoltNetworkExecutor.java:1016-1021`
   (`handleSystemQuery`).

## Why `5.26.0`

No commit or issue documents the specific choice of `5.26.0`. The only
relevant history is issue #3471 (Neo4j Desktop 1.6.0+ connectivity fix,
commit `19a97bb9a`), which establishes *that* a Neo4j-shaped version string
is required for Neo4j Desktop and official drivers to complete their
feature-negotiation handshake, but not *why this specific version*.

Re-derived rationale, documented here for the first time: `5.26` is
Neo4j's 5.x LTS (long-term support) release line - a reasonable choice for
a stable, widely-deployed feature-gating baseline, rather than pinning to a
fast-moving Neo4j 5.x/2025.x edge release that could introduce
driver-side feature checks ArcadeDB has no matching implementation for.

**This is a different axis from the Bolt wire protocol version actually
negotiated.** The advertised string is a *Neo4j server version claim* read
by drivers/tools for feature detection. The *Bolt wire protocol version*
is negotiated separately during the handshake and tops out at 4.4
(`SUPPORTED_VERSIONS` in
`bolt/src/main/java/com/arcadedb/bolt/BoltNetworkExecutor.java:96` lists
only `0x00000404`/`0x00000004`/`0x00000003` - Bolt 4.4, 4.0, 3.0). ArcadeDB
claims a Neo4j 5.26 server identity while speaking the 3.0/4.0/4.4 Bolt
wire protocol only. This gap between the two axes is exactly the kind of
thing this doc exists to make explicit instead of leaving implicit.

## Known gaps behind this claim

A driver that trusts the advertised `5.26.0` identity will find these four
areas do not behave like a real Neo4j 5.26 server. Each is already tracked
as an `expected-fail` scenario with a source reference in
[`conformance/spec.yaml`](conformance/spec.yaml) and rolled up under
tracking issue #4890.

- **No Bolt 5.x protocol negotiation** (`PROTO-002`) - `SUPPORTED_VERSIONS`
  (`BoltNetworkExecutor.java:96`) never advertises any Bolt 5.x version.
  Drivers that support Bolt 5.x today work only by silently downgrading to
  4.4 - an undocumented, untested compatibility stance until now.
- **`ROUTE` is single-node only** (`CONN-004`) - `handleRoute`
  (`BoltNetworkExecutor.java:903-939`) always returns the local node's own
  address as the `WRITE`, `READ`, and `ROUTE` role, regardless of actual HA
  cluster topology. `neo4j://` routing against a real cluster is unproven.
- **Temporal and spatial type-fidelity gaps** (`TYPE-007` through
  `TYPE-012`) - `LocalDate`, `LocalTime`, `LocalDateTime`,
  `OffsetDateTime`/`ZonedDateTime` serialize as ISO-8601 strings instead of
  native Bolt structures (`BoltStructureMapper.java`, from line 118).
  `Duration` and spatial `Point` have no handling anywhere in
  `BoltStructureMapper`/`PackStreamWriter` - not even a string fallback.
- **No `Neo.TransientError.*` codes** (`TX-005`, `ERR-004`) -
  `BoltErrorCodes.java` defines exactly 7 codes, all
  `Neo.ClientError.*`/`Neo.DatabaseError.*`. Driver-side transient-error
  retry logic (e.g. `executeRead`/`executeWrite` managed transaction
  functions) can never be triggered by ArcadeDB today.

## Related documents

- [`conformance/spec.yaml`](conformance/spec.yaml) - the full certification
  scenario matrix, including the `expected-fail` entries referenced above.
- [`conformance/README.md`](conformance/README.md) - how the conformance
  spec is consumed by each language's test suite.
- Tracking issue [#4890](https://github.com/ArcadeData/arcadedb/issues/4890) -
  closing these gaps.
- Epic [#4882](https://github.com/ArcadeData/arcadedb/issues/4882) - Bolt
  Driver Compatibility Certification.
