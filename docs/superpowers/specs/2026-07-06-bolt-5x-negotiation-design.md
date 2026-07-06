# Bolt 5.x Version Negotiation - Design Spec

**Issue:** [#5001](https://github.com/ArcadeData/arcadedb/issues/5001) - Bolt: decide and implement 5.x version negotiation
**Parent tracking:** [#4890](https://github.com/ArcadeData/arcadedb/issues/4890) - protocol/type-fidelity gaps
**Epic:** [#4882](https://github.com/ArcadeData/arcadedb/issues/4882) - Bolt Driver Compatibility Certification
**Conformance scenario:** `PROTO-002` (`bolt/conformance/spec.yaml`), currently `current_status: expected-fail`
**Date:** 2026-07-06
**Effort:** M

---

## 1. Decision

**Advertise Bolt 5.0 through 5.4 natively, and version-gate the outbound wire encoders in the same change.**

The alternative - formally accepting 4.4-only as a documented limitation - was rejected. It would leave the advertised `Neo4j/5.26.0 compatible` identity dishonest and keep 5.x drivers on an undocumented silent downgrade, which the epic's governing principle ("certify depth, not presence; never a silent omission") explicitly rules out.

The ceiling is **5.4**, not higher, because:

- All 5.x minors share the *same* struct-level wire deltas (`element_id` fields on graph elements; UTC datetime signatures). Higher minors only add message-flow surface, not encoding.
- 5.4 is a clean, bounded envelope: LOGON/LOGOFF auth (5.1, already implemented), notification-config + `bolt_agent` HELLO keys (5.2 / 5.3, safely ignored as unknown extra keys), and TELEMETRY (5.4, acked defensively).
- Staying at or below 5.6 keeps the server on the **classic handshake**. The 5.7/5.8 handshake manifest is out of scope; manifest-capable drivers fall back to classic negotiation against our advertised ceiling, so `bolt://` / `neo4j://` still connect.
- 5.5 / 5.6 (GQL-status notification metadata) add optional surface with no must-pass conformance cell, so they are deliberately excluded from this change.

---

## 2. The governing invariant: never lie to a driver

The negotiated Bolt version is the single switch that selects wire encoding. Adding 5.x to `SUPPORTED_VERSIONS` and version-gating the encoders **must land atomically**, because the pinned `neo4j-java-driver` 6.2.0 already proposes both 4.4 and 5.x during handshake. The instant 5.x is advertised, that driver negotiates 5.4 and then *expects* 5.0-style structures. Advertising 5.x without honoring the deltas would break the currently-green TYPE round-trip scenarios.

This coupling is a feature, not a hazard: the existing, passing round-trip tests become the regression guard for the encoder changes.

---

## 3. Handshake negotiation

**File:** `bolt/src/main/java/com/arcadedb/bolt/BoltNetworkExecutor.java`

`SUPPORTED_VERSIONS` changes from:

```java
private static final int[] SUPPORTED_VERSIONS = { 0x00000404, 0x00000004, 0x00000003 }; // v4.4, v4.0, v3.0
```

to (high-to-low preference; encoding is `[unused(8)][range(8)][minor(8)][major(8)]`):

```java
private static final int[] SUPPORTED_VERSIONS = {
    0x00000405, 0x00000305, 0x00000205, 0x00000105, 0x00000005, // v5.4, v5.3, v5.2, v5.1, v5.0
    0x00000404, 0x00000004, 0x00000003                          // v4.4, v4.0, v3.0
};
```

**No change to `negotiateVersion()` logic.** The existing loop iterates client proposals (outer) against `SUPPORTED_VERSIONS` (inner, ordered high-to-low) and picks the first server version inside the client's `[minor - range, minor]` window for a matching major. Consequences:

- A 5.x-only driver (e.g. proposes `5.8` with range down to `5.0`) → negotiates **5.4** (highest server 5.x inside its window).
- A driver proposing 5.x *and* 4.4 → now negotiates **5.4** (was 4.4). This is the intended upgrade.
- A driver proposing only `{4.4, 4.0, 3.0}` → unchanged.

The server response remains a single concrete version (no range byte); the range byte in `SUPPORTED_VERSIONS` entries stays `0x00` - it is only consumed on the *client* proposal side.

**Handshake manifest (5.7 / 5.8): explicitly not implemented.** Documented as an intentional boundary in the A2 server-identity doc (#4884).

---

## 4. Version-gated serialization

A negotiated-version signal is threaded into the serialization context (the `PackStreamWriter`, set once after handshake completes) so encoders branch on `major >= 5`.

### 4.1 Graph-element field counts

**Files:** `bolt/src/main/java/com/arcadedb/bolt/structure/BoltNode.java`, `BoltRelationship.java`, `BoltUnboundRelationship.java`

| Struct | Signature | 4.x (current, hardcoded) | 5.0+ (gated) |
|---|---|---|---|
| `BoltNode` | `0x4E` | 3 fields: `id, labels, properties` | 4 fields: `+ element_id` |
| `BoltRelationship` | `0x52` | 5 fields: `id, startId, endId, type, properties` | 8 fields: `+ element_id, start_node_element_id, end_node_element_id` |
| `BoltUnboundRelationship` | `0x72` | 3 fields: `id, type, properties` | 4 fields: `+ element_id` |

The `element_id` values are already carried on the objects (built by `BoltStructureMapper` from the RID). Today `writeTo` simply omits them by writing a fixed lower field count. The gate makes the field count and the trailing `element_id` writes conditional on the writer's negotiated major version. `BoltRelationship` additionally needs `start_node_element_id` / `end_node_element_id`; these derive from the start/end RIDs already available at mapping time (added to the object if not already present).

### 4.2 Temporal signatures

**File:** `bolt/src/main/java/com/arcadedb/bolt/structure/BoltStructureMapper.java`

Outbound `DateTime` / `DateTimeZoneId` construction is version-dependent - the signature *and* the epoch basis differ:

| Version | Offset DateTime | ZoneId DateTime | Seconds field |
|---|---|---|---|
| 4.x (current) | `'F'` `0x46` | `'f'` `0x66` | local epoch-second (offset folded in) |
| 5.0+ (gated) | `'I'` `0x49` | `'i'` `0x69` | true UTC epoch-second |

The temporal builder (currently hardcoding `SIG_DATE_TIME_OFFSET_LEGACY` at `BoltStructureMapper.java:523`) selects signature and computes the seconds field per negotiated version. Because the choice is baked into the `BoltTemporalStructure` value object at construction, the negotiated version must be available at *mapping* time, not only at `writeTo` time.

**Unchanged across 4.x <-> 5.x** (no gate needed): `Date` `'D'`, `Time` `'T'`, `LocalTime` `'t'`, `LocalDateTime` `'d'`, `Duration` `'E'`, `Point2D` `'X'`, `Point3D` `'Y'`.

### 4.3 Threading mechanism

The negotiated major version is set on the `PackStreamWriter` (the serialization context) per outgoing message from the connection's negotiated version. Every version-dependent struct reads it at `writeTo` time:

- Graph elements (`BoltNode`/`BoltRelationship`/`BoltUnboundRelationship`) already carry their `element_id` values; `writeTo` gates the field count and the trailing `element_id` writes on `writer.getBoltMajorVersion() >= 5`.
- Datetime values are emitted through a dedicated `BoltDateTimeStructure` that carries *both* epoch bases (local-epoch and true-UTC-epoch, computed at build time from the source `java.time` value) plus the offset/zone. `writeTo` selects signature (`'F'`/`'f'` vs `'I'`/`'i'`) and seconds field by the writer's major version.

This keeps all version logic at `writeTo` time driven by the single writer signal - no version parameter is threaded through the `BoltStructureMapper` recursion, so a datetime nested inside node/map/list properties is encoded correctly automatically. The 4.x path stays byte-identical (the gate is purely additive).

### 4.4 Inbound (parameters): no change

The `PackStreamReader` parameter-hydration path already decodes *both* legacy (`'F'`/`'f'`) and UTC (`'I'`/`'i'`) temporal signatures defensively (`BoltStructureMapper.java:599-616`). A 5.x driver sending a native datetime parameter is already handled.

---

## 5. Auth-flow gate (resolves the standing `NOTE (Bolt 5.1+)`)

**File:** `bolt/src/main/java/com/arcadedb/bolt/BoltNetworkExecutor.java` (`handleHello`, ~line 420)

The existing code has a documented latent bug: `handleHello` rejects any HELLO without credentials. A legitimate Bolt 5.1+ HELLO carries no auth (it moves to a separate LOGON), so it would be wrongly rejected. Safe today only because 5.x is never negotiated.

New behavior:

- **Negotiated `minor >= 1` (5.1+) AND HELLO has no scheme/principal/credentials** → HELLO succeeds (SUCCESS + server metadata), connection stays in the authentication state awaiting LOGON. The existing `handleLogon` then authenticates.
- **Negotiated `< 5.1`** → current HELLO-embedded auth behavior, unchanged.
- **`none` scheme** → still deliberately rejected in both flows (preserves `AUTH-003`). At 5.1+ this arrives as a LOGON with scheme `none`; `handleLogon` -> `authenticateUser(null, null)` rejects with a structured auth error.

### TELEMETRY (5.4)

A `TELEMETRY` message (signature `0x54`) is acked defensively with SUCCESS. Drivers send TELEMETRY only when the server advertises telemetry hints in HELLO SUCCESS metadata, which ArcadeDB will not do, so in practice no compliant driver sends it. The graceful ack is belt-and-suspenders against a misconfigured/eager driver rather than a required feature.

---

## 6. Testing (TDD - written first)

### 6.1 Bolt-module unit / ITs (`bolt/src/test/java`, fast guard)

1. **Handshake negotiation matrix**
   - 5.x-only proposal → server responds `5.4`.
   - 5.x-with-4.4-fallback proposal → `5.4`.
   - Pure `{4.4, 4.0, 3.0}` proposals → unchanged (4.4 / 4.0 / 3.0 respectively).
   - Directly pins PROTO-001 (stays green) and PROTO-002's mechanic.

2. **Version-gated struct encoding** (the "don't lie" guard)
   - Serialize Node / Relationship / UnboundRelationship / DateTime through a `PackStreamWriter` pinned to major 4 vs major 5.
   - Assert exact field counts: `3/5/3` (v4) vs `4/8/4` (v5).
   - Assert datetime signature `'F'` (v4) vs `'I'` (v5) and the correct seconds field (local vs UTC epoch).

3. **Auth-flow gate**
   - Auth-less HELLO at negotiated 5.1+ → SUCCESS + awaits LOGON.
   - Auth-less HELLO at 4.4 → unchanged rejection.
   - `none` scheme → rejected in both flows.

### 6.2 Cross-language conformance

- Flip `PROTO-002` in `bolt/conformance/spec.yaml`: `current_status: expected-fail` → `passing`; remove its `known_limitation` block.
- The real `neo4j-java-driver` IT (`RemoteBoltDatabaseIT`) negotiates 5.4 end-to-end.
- **All existing TYPE round-trip scenarios re-run under 5.4 and must stay green** - the atomic-change regression guard.

### 6.3 Verification commands

- `mvn -pl bolt test` (unit + module ITs).
- `RemoteBoltDatabaseIT` against a live server (real driver end-to-end).
- Confirm the currently-green TYPE / graph-element scenarios remain green under the new negotiated 5.4.

---

## 7. Risks & coordination

- **PROTO-001 test expectation.** Confirm the suite forces specific proposed versions rather than asserting "driver X lands on 4.4". If any test hard-codes the previously negotiated version for a modern driver, update its expectation to 5.4. Checked during implementation.
- **#4884 / A2 (server-identity doc).** This change makes the `Neo4j/5.26.0 compatible` identity honest up to Bolt 5.4 for the first time. The doc absorbs: the 5.4 ceiling, the classic-handshake-only stance, and the 5.5-5.8 / manifest non-support as documented boundaries.
- **Serialization hot-path blast radius.** The gate touches the outbound result-encoding path. Mitigated by the field-count/signature unit tests and by keeping the 4.x branch byte-identical (additive gate only).

---

## 8. Acceptance criteria (from #5001)

- [ ] PROTO-002 passes: a 5.x-only driver connects and negotiates a documented-supported 5.x version (5.4).
- [ ] The negotiated version set is consistent with the advertised `Neo4j/5.26.0 compatible` identity up to 5.4; the 5.4 ceiling and manifest non-support are documented (coordinated with #4884).
- [ ] Existing 3.0 / 4.0 / 4.4 negotiation unaffected.
- [ ] All existing TYPE round-trip conformance scenarios stay green under the new negotiated 5.4.

## 9. Non-goals

- Handshake manifest (Bolt 5.7 / 5.8).
- Bolt 5.5 / 5.6 GQL-status notification metadata.
- HA-aware ROUTE (CONN-004) - separate sub-issue #5002.
- Changing the advertised `server_agent` string away from `5.26.0` (epic non-goal; A2 documents the existing choice).
