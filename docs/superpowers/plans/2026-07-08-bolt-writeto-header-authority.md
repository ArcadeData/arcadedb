# Bolt writeTo() Sole Header Authority Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove `getSignature()` and `getFieldCount()` from the `PackStreamStructure` interface and all seven implementers, making `writeTo()` the sole authority for Bolt structure-header emission, with zero wire-behavior change on any negotiated Bolt version (3.0-5.4).

**Architecture:** Two-phase refactor. Phase 1 converts the three test files that assert on the *interface* accessors over to wire-byte / value-getter assertions - these new assertions pass against current behavior and act as characterization guards. Phase 2 deletes the accessors (inlining the one internal consumer, `BoltPointStructure.writeTo`), guarded green by the Phase-1 assertions plus the existing real-driver ITs.

**Tech Stack:** Java 21, Maven, JUnit 5 (Jupiter), AssertJ. Module: `bolt`.

## Global Constraints

- Java 21+ (main branch).
- Do NOT commit on the user's behalf beyond the commits this plan specifies on the feature branch; the user reviews before any push/merge.
- `final` on variables and parameters where the surrounding code does.
- Single-child `if` needs no braces (match existing style).
- No new dependencies.
- No `System.out` debug left behind.
- No issue-number references in code comments (behavioral invariants only).
- No `@author` / Claude attribution in source.
- Working directory / worktree: `.worktrees/feat/5082-bolt-writeto-header-authority` on branch `feat/5082-bolt-writeto-header-authority`.

---

## File Structure

**Production (Phase 2):**
- `bolt/src/main/java/com/arcadedb/bolt/packstream/PackStreamStructure.java` - drop two method declarations; becomes single-method contract.
- `bolt/src/main/java/com/arcadedb/bolt/structure/BoltPointStructure.java` - inline signature + field count into `writeTo()`; delete two accessors.
- `bolt/src/main/java/com/arcadedb/bolt/structure/BoltNode.java` - delete two accessors.
- `bolt/src/main/java/com/arcadedb/bolt/structure/BoltRelationship.java` - delete two accessors.
- `bolt/src/main/java/com/arcadedb/bolt/structure/BoltUnboundRelationship.java` - delete two accessors.
- `bolt/src/main/java/com/arcadedb/bolt/structure/BoltPath.java` - delete two accessors.
- `bolt/src/main/java/com/arcadedb/bolt/structure/BoltTemporalStructure.java` - delete two accessors.
- `bolt/src/main/java/com/arcadedb/bolt/structure/BoltDateTimeStructure.java` - delete two accessors.

**Tests (Phase 1):**
- `bolt/src/test/java/com/arcadedb/bolt/BoltStructureTest.java` - 4 creation tests → wire-byte header assertions.
- `bolt/src/test/java/com/arcadedb/bolt/BoltTypeRoundTripTest.java` - TYPE-011 drop pre-wire accessor asserts; TYPE-012 (x2) signature assert → `getZ()`.
- `bolt/src/test/java/com/arcadedb/bolt/Bolt4998PathMappingTest.java` - TYPE-012 point signature assert → `getZ()`.

**Untouched (verified different types / safe):** `BoltDateTimeStructureTest` (all `getSignature()` on `PackStreamReader.StructureValue` via its `roundTrip`/`mapperRoundTrip` helpers), `PackStreamTest` (`StructureValue`), `BoltMessageTest` (`BoltMessage`), and `BoltStructureMapper` / `BoltMessage` / `PackStreamReader.StructureValue` production code.

---

### Task 1: Convert test assertions off the interface accessors (characterization guard)

Rewrite every assertion that reads `PackStreamStructure.getSignature()` / `getFieldCount()` on an *interface implementer* so it instead asserts the serialized wire header (Node/Rel/UnboundRel/Path) or the z-coordinate that solely determines a Point's signature (Point). These assertions must PASS against current, unchanged production code - they are the guard that Task 2's removal is behavior-preserving.

**Files:**
- Modify: `bolt/src/test/java/com/arcadedb/bolt/BoltStructureTest.java`
- Modify: `bolt/src/test/java/com/arcadedb/bolt/BoltTypeRoundTripTest.java`
- Modify: `bolt/src/test/java/com/arcadedb/bolt/Bolt4998PathMappingTest.java`

**Interfaces:**
- Consumes: `PackStreamStructure.writeTo(PackStreamWriter)` (unchanged), `PackStreamWriter.toByteArray()`, `BoltPointStructure.getZ()`.
- Produces: nothing for later tasks; this task only de-couples tests from the soon-to-be-removed accessors.

- [ ] **Step 1: Add imports + a wire helper to `BoltStructureTest`**

Add to the import block (after the existing `com.arcadedb.bolt.packstream.PackStreamWriter` import and in the `java.io` group):

```java
import com.arcadedb.bolt.packstream.PackStreamStructure;
```
```java
import java.io.IOException;
```

Add this private helper inside the `BoltStructureTest` class (near the top, before the first `@Test`):

```java
  // Serializes a structure at the default (Bolt 4.x) negotiated version and returns the raw PackStream
  // bytes. The first byte is the TINY_STRUCT marker (0xB0 | fieldCount); the second is the signature.
  private static byte[] wireHeader(final PackStreamStructure s) throws IOException {
    final PackStreamWriter writer = new PackStreamWriter();
    s.writeTo(writer);
    return writer.toByteArray();
  }
```

- [ ] **Step 2: Rewrite the 4 creation tests in `BoltStructureTest`**

In `boltNodeCreation`, change the signature to `throws IOException` and replace:

```java
    assertThat(node.getSignature()).isEqualTo(BoltNode.SIGNATURE);
    assertThat(node.getFieldCount()).isEqualTo(3);
```
with:
```java
    final byte[] header = wireHeader(node);
    assertThat(header[0]).isEqualTo((byte) (0xB0 | 3)); // TINY_STRUCT, 3 fields (Bolt 4.x)
    assertThat(header[1]).isEqualTo(BoltNode.SIGNATURE);
```

In `boltRelationshipCreation` (`throws IOException`), replace:
```java
    assertThat(rel.getSignature()).isEqualTo(BoltRelationship.SIGNATURE);
    assertThat(rel.getFieldCount()).isEqualTo(5);
```
with:
```java
    final byte[] header = wireHeader(rel);
    assertThat(header[0]).isEqualTo((byte) (0xB0 | 5)); // TINY_STRUCT, 5 fields (Bolt 4.x)
    assertThat(header[1]).isEqualTo(BoltRelationship.SIGNATURE);
```

In `boltUnboundRelationshipCreation` (`throws IOException`), replace:
```java
    assertThat(rel.getSignature()).isEqualTo(BoltUnboundRelationship.SIGNATURE);
    assertThat(rel.getFieldCount()).isEqualTo(3);
```
with:
```java
    final byte[] header = wireHeader(rel);
    assertThat(header[0]).isEqualTo((byte) (0xB0 | 3)); // TINY_STRUCT, 3 fields (Bolt 4.x)
    assertThat(header[1]).isEqualTo(BoltUnboundRelationship.SIGNATURE);
```

In `boltPathCreation` (`throws IOException`), replace:
```java
    assertThat(path.getSignature()).isEqualTo(BoltPath.SIGNATURE);
    assertThat(path.getFieldCount()).isEqualTo(3);
```
with:
```java
    final byte[] header = wireHeader(path);
    assertThat(header[0]).isEqualTo((byte) (0xB0 | 3)); // TINY_STRUCT, 3 fields
    assertThat(header[1]).isEqualTo(BoltPath.SIGNATURE);
```

- [ ] **Step 3: Rewrite the TYPE-011 and TYPE-012 assertions in `BoltTypeRoundTripTest`**

In `type011_durationNative` (already `throws IOException`), delete these two now-redundant pre-wire lines (the code just below already round-trips through the wire and asserts `wire.getSignature()` == `0x45` and the exact field values):

```java
    assertThat(s.getSignature()).isEqualTo((byte) 0x45);
    assertThat(s.getFieldCount()).isEqualTo(4);
```

Keep the surrounding `final BoltTemporalStructure s = (BoltTemporalStructure) out;` line and everything after it - `s` is still written to the wire below.

In `type012_cartesianPointNative`, replace:
```java
    assertThat(p.getSignature()).isEqualTo((byte) 0x58);
```
with (a 2D point is exactly the case where `writeTo` emits the `0x58` Point2D signature - i.e. `z == null`):
```java
    assertThat(p.getZ()).isNull(); // z absent -> writeTo emits the Point2D (0x58) signature
```

In `type012_wgs84Point3DNative`, replace:
```java
    assertThat(p.getSignature()).isEqualTo((byte) 0x59);
```
with:
```java
    assertThat(p.getZ()).isNotNull(); // z present -> writeTo emits the Point3D (0x59) signature
```

(The `assertThat(p.getZ()).isEqualTo(100.0);` line further down remains and pins the actual value.)

- [ ] **Step 4: Rewrite the TYPE-012 point assertion in `Bolt4998PathMappingTest`**

In `type012_enginePointOutbound`, replace:
```java
    assertThat(pt.getSignature()).isEqualTo((byte) 0x58);
```
with:
```java
    assertThat(pt.getZ()).isNull(); // z absent -> writeTo emits the Point2D (0x58) signature
```

- [ ] **Step 5: Run the three converted test classes and verify they PASS against unchanged production code**

Run:
```bash
cd .worktrees/feat/5082-bolt-writeto-header-authority
mvn -q -pl bolt test -Dtest='BoltStructureTest,BoltTypeRoundTripTest,Bolt4998PathMappingTest'
```
Expected: BUILD SUCCESS, all three classes green. (Proves the new assertions correctly characterize current wire behavior before any accessor is removed.)

- [ ] **Step 6: Commit**

```bash
git add bolt/src/test/java/com/arcadedb/bolt/BoltStructureTest.java \
        bolt/src/test/java/com/arcadedb/bolt/BoltTypeRoundTripTest.java \
        bolt/src/test/java/com/arcadedb/bolt/Bolt4998PathMappingTest.java
git commit -m "test(#5082): assert Bolt structure headers on the wire, not via getSignature/getFieldCount"
```

---

### Task 2: Remove `getSignature()` / `getFieldCount()` from the interface and all implementers

Delete the two accessors from `PackStreamStructure` and all seven implementers. The only internal consumer, `BoltPointStructure.writeTo`, is rewritten to inline its own signature + field count. This is an atomic change: an `@Override` accessor left behind after the interface method is removed is a compile error, so interface + implementers change together in one commit.

**Files:**
- Modify: `bolt/src/main/java/com/arcadedb/bolt/packstream/PackStreamStructure.java`
- Modify: `bolt/src/main/java/com/arcadedb/bolt/structure/BoltPointStructure.java`
- Modify: `bolt/src/main/java/com/arcadedb/bolt/structure/BoltNode.java`
- Modify: `bolt/src/main/java/com/arcadedb/bolt/structure/BoltRelationship.java`
- Modify: `bolt/src/main/java/com/arcadedb/bolt/structure/BoltUnboundRelationship.java`
- Modify: `bolt/src/main/java/com/arcadedb/bolt/structure/BoltPath.java`
- Modify: `bolt/src/main/java/com/arcadedb/bolt/structure/BoltTemporalStructure.java`
- Modify: `bolt/src/main/java/com/arcadedb/bolt/structure/BoltDateTimeStructure.java`

**Interfaces:**
- Consumes: the Task-1 wire-header assertions (guard), the existing `bolt` ITs (`RemoteBoltDatabaseIT`, `BoltProtocolIT`, Bolt-version-negotiation ITs).
- Produces: `PackStreamStructure` reduced to `void writeTo(PackStreamWriter) throws IOException`. No new symbols other code depends on.

- [ ] **Step 1: Reduce the `PackStreamStructure` interface to a single method**

Replace the body of `PackStreamStructure.java` (the interface declaration through its methods) so only `writeTo` remains:

```java
public interface PackStreamStructure {

  /**
   * Write this structure to a PackStream writer. This is the sole authority for the structure's wire
   * header (marker + signature) and body; there is no separate accessor for signature or field count,
   * because the header can be version-dependent and only the writer carries the negotiated Bolt version.
   */
  void writeTo(PackStreamWriter writer) throws IOException;
}
```

Leave the license header, package statement, and `import java.io.IOException;` intact.

- [ ] **Step 2: Inline the header in `BoltPointStructure.writeTo` and delete its accessors**

Replace the `getSignature()`, `getFieldCount()`, and `writeTo()` members with:

```java
  @Override
  public void writeTo(final PackStreamWriter writer) throws IOException {
    final byte signature = z == null ? SIGNATURE_2D : SIGNATURE_3D;
    final int fieldCount = z == null ? 3 : 4;
    writer.writeStructureHeader(signature, fieldCount);
    writer.writeValue((long) srid);
    writer.writeValue(x);
    writer.writeValue(y);
    if (z != null)
      writer.writeValue(z);
  }
```

Keep the `SIGNATURE_2D` / `SIGNATURE_3D` constants and the `getSrid`/`getX`/`getY`/`getZ` getters.

- [ ] **Step 3: Delete the two accessors from the remaining six implementers**

In each of `BoltNode`, `BoltRelationship`, `BoltUnboundRelationship`, `BoltPath`, `BoltTemporalStructure`, `BoltDateTimeStructure`, delete the entire `@Override public byte getSignature() { ... }` and `@Override public int getFieldCount() { ... }` methods, including their preceding comment lines (e.g. the `// v4.x base count; writeTo is authoritative ...` fragments). Do NOT touch each class's `writeTo()` body, its `SIGNATURE` constant, or its value getters.

For example, in `BoltNode.java` remove exactly:

```java
  @Override
  public byte getSignature() {
    return SIGNATURE;
  }

  @Override
  public int getFieldCount() {
    // v4.x base count; writeTo is authoritative and emits 4 fields (adds element_id) when the writer negotiated Bolt >= 5.
    return 3;
  }
```

Apply the analogous deletion in the other five classes.

- [ ] **Step 4: Compile the module**

Run:
```bash
mvn -q -pl bolt -am test-compile
```
Expected: BUILD SUCCESS with no `method does not override a method from its superclass` and no `cannot find symbol` errors.

- [ ] **Step 5: Full module verify (unit tests + real-driver ITs across Bolt 3.0-5.4)**

Run:
```bash
mvn -q -pl bolt verify
```
Expected: BUILD SUCCESS. This is the guard that no wire behavior changed for any negotiated version - `BoltStructureTest`, `BoltDateTimeStructureTest`, `BoltTypeRoundTripTest`, `BoltProtocolIT`, `RemoteBoltDatabaseIT`, and the version-negotiation ITs all pass.

- [ ] **Step 6: Confirm no stray references to the removed accessors remain**

Run:
```bash
grep -rn 'getFieldCount\|getSignature' bolt/src/main/java/com/arcadedb/bolt/structure/ bolt/src/main/java/com/arcadedb/bolt/packstream/PackStreamStructure.java
```
Expected: no matches in `structure/*.java` and none in `PackStreamStructure.java`. (Matches elsewhere - `PackStreamReader.StructureValue`, `BoltMessage` - are the untouched different types and are fine.)

- [ ] **Step 7: Commit**

```bash
git add bolt/src/main/java/com/arcadedb/bolt/packstream/PackStreamStructure.java \
        bolt/src/main/java/com/arcadedb/bolt/structure/BoltPointStructure.java \
        bolt/src/main/java/com/arcadedb/bolt/structure/BoltNode.java \
        bolt/src/main/java/com/arcadedb/bolt/structure/BoltRelationship.java \
        bolt/src/main/java/com/arcadedb/bolt/structure/BoltUnboundRelationship.java \
        bolt/src/main/java/com/arcadedb/bolt/structure/BoltPath.java \
        bolt/src/main/java/com/arcadedb/bolt/structure/BoltTemporalStructure.java \
        bolt/src/main/java/com/arcadedb/bolt/structure/BoltDateTimeStructure.java
git commit -m "refactor(#5082): make writeTo() the sole header authority in PackStreamStructure

Remove getSignature()/getFieldCount() from the PackStreamStructure interface and
all seven implementers; inline the header write in BoltPointStructure.writeTo (its
only internal consumer). writeTo() is now the single source of truth for structure
headers, which the version-gated structs already require since only the writer
carries the negotiated Bolt version. No wire change on any version (3.0-5.4)."
```

---

## Self-Review

**Spec coverage:**
- AC "interface no longer exposes accessors" → Task 2 Step 1.
- AC "no @Override accessor remains on any implementer" → Task 2 Steps 2-3, verified Step 6.
- AC "no wire behavioral change 3.0-5.4" → Task 1 guard assertions + Task 2 Step 5 (`mvn -pl bolt verify`).
- AC "three affected test files compile and pass" → Task 1 Steps 2-5.
- AC "`mvn -pl bolt verify` green" → Task 2 Step 5.
- Spec's `BoltPointStructure` inline requirement → Task 2 Step 2.
- Spec's "untouched: BoltDateTimeStructureTest / decode path" → honored (not in any task's file list).

**Placeholder scan:** No TBD/TODO; every code step shows exact before/after code and exact commands with expected output.

**Type consistency:** `wireHeader(PackStreamStructure)` helper is defined in Task 1 Step 1 and used in Step 2. Marker byte `(byte)(0xB0 | n)` matches `PackStreamWriter.writeStructureHeader` (`TINY_STRUCT | fieldCount`, `TINY_STRUCT = 0xB0`). `SIGNATURE_2D`/`SIGNATURE_3D`, `getZ()`, per-class `SIGNATURE` constants all exist in the current source. Field counts (Node 3, Rel 5, UnboundRel 3, Path 3 at Bolt 4.x default) match each `writeTo()`'s else-branch literal.
