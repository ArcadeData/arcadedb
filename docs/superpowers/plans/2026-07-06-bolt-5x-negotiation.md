# Bolt 5.x Version Negotiation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Advertise Bolt 5.0-5.4 in the handshake and version-gate the outbound wire encoders so a 5.x driver connects natively without being lied to (closes `PROTO-002` / issue #5001).

**Architecture:** The negotiated Bolt major version is the single switch. Outbound graph-element structs (`BoltNode`/`BoltRelationship`/`BoltUnboundRelationship`) and datetime structs consult the negotiated major version at `writeTo` time via the `PackStreamWriter` (the serialization context, set once per outgoing message from the connection's negotiated version). Adding 5.x to `SUPPORTED_VERSIONS` and wiring the writer's version happen last and atomically, so every intermediate commit stays green and the existing (currently-passing) TYPE round-trip and temporal ITs become the regression guard for the flip.

**Tech Stack:** Java 21, JUnit 5 (Jupiter), AssertJ (`assertThat(...)`), the `bolt` Maven module, `neo4j-java-driver` 6.2.0 (real-driver ITs).

## Global Constraints

- Java 21; use the `final` keyword on new variables and parameters.
- New source files carry the standard Apache-2.0 license header (copy verbatim from any existing file in the module); do not add Claude as an author.
- Tests use AssertJ: `assertThat(actual).isEqualTo(expected)` / `.isTrue()` style.
- Do not commit to git beyond the per-task commits in this plan; the user reviews and merges. (These per-task commits happen on the feature branch inside the worktree.)
- Bolt version encoding is `[unused(8)][range(8)][minor(8)][major(8)]`: `major = v & 0xFF`, `minor = (v >> 8) & 0xFF`, `range = (v >> 16) & 0xFF`.
- Keep the Bolt 4.x wire path byte-identical; every 5.x branch is strictly additive.

---

### Task 1: Version-aware `PackStreamWriter` + gated graph-element structs

**Files:**
- Modify: `bolt/src/main/java/com/arcadedb/bolt/packstream/PackStreamWriter.java:71-82`
- Modify: `bolt/src/main/java/com/arcadedb/bolt/structure/BoltNode.java:59-67`
- Modify: `bolt/src/main/java/com/arcadedb/bolt/structure/BoltRelationship.java:68-78`
- Modify: `bolt/src/main/java/com/arcadedb/bolt/structure/BoltUnboundRelationship.java:59-67`
- Test: `bolt/src/test/java/com/arcadedb/bolt/BoltStructureTest.java`

**Interfaces:**
- Produces: `PackStreamWriter.boltMajorVersion(int)` (fluent setter, returns `this`) and `int PackStreamWriter.getBoltMajorVersion()` (default `4`). `BoltNode`/`BoltRelationship`/`BoltUnboundRelationship` `.writeTo(writer)` emit 4/8/4 fields when `writer.getBoltMajorVersion() >= 5`, else 3/5/3.
- Consumes: nothing from earlier tasks.

- [ ] **Step 1: Write the failing test** — append to `BoltStructureTest.java`:

```java
@Test
void nodeWritesElementIdOnBolt5() throws Exception {
  final BoltNode node = new BoltNode(1L, List.of("Person"), Map.of("name", "Alice"), "#1:0");

  final PackStreamWriter v4 = new PackStreamWriter();
  node.writeTo(v4);
  // Tiny-struct marker: 0xB0 | fieldCount. v4 => 3 fields, signature 0x4E.
  assertThat(v4.toByteArray()[0]).isEqualTo((byte) (0xB0 | 3));
  assertThat(v4.toByteArray()[1]).isEqualTo(BoltNode.SIGNATURE);

  final PackStreamWriter v5 = new PackStreamWriter().boltMajorVersion(5);
  node.writeTo(v5);
  assertThat(v5.toByteArray()[0]).isEqualTo((byte) (0xB0 | 4));
  assertThat(v5.toByteArray()[1]).isEqualTo(BoltNode.SIGNATURE);
  // Last field is the element_id string "#1:0" -> tiny-string 0x84 then ASCII bytes.
  final byte[] b = v5.toByteArray();
  assertThat(b[b.length - 5]).isEqualTo((byte) (0x80 | 4));
  assertThat(new String(b, b.length - 4, 4, java.nio.charset.StandardCharsets.UTF_8)).isEqualTo("#1:0");
}

@Test
void relationshipWritesElementIdsOnBolt5() throws Exception {
  final BoltRelationship rel = new BoltRelationship(10L, 1L, 2L, "KNOWS", Map.of(), "#10:0", "#1:0", "#2:0");

  final PackStreamWriter v4 = new PackStreamWriter();
  rel.writeTo(v4);
  assertThat(v4.toByteArray()[0]).isEqualTo((byte) (0xB0 | 5));

  final PackStreamWriter v5 = new PackStreamWriter().boltMajorVersion(5);
  rel.writeTo(v5);
  assertThat(v5.toByteArray()[0]).isEqualTo((byte) (0xB0 | 8));
  assertThat(v5.toByteArray()[1]).isEqualTo(BoltRelationship.SIGNATURE);
}

@Test
void unboundRelationshipWritesElementIdOnBolt5() throws Exception {
  final BoltUnboundRelationship rel = new BoltUnboundRelationship(10L, "KNOWS", Map.of(), "#10:0");

  final PackStreamWriter v4 = new PackStreamWriter();
  rel.writeTo(v4);
  assertThat(v4.toByteArray()[0]).isEqualTo((byte) (0xB0 | 3));

  final PackStreamWriter v5 = new PackStreamWriter().boltMajorVersion(5);
  rel.writeTo(v5);
  assertThat(v5.toByteArray()[0]).isEqualTo((byte) (0xB0 | 4));
  assertThat(v5.toByteArray()[1]).isEqualTo(BoltUnboundRelationship.SIGNATURE);
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `mvn -pl bolt test -Dtest=BoltStructureTest`
Expected: FAIL - `boltMajorVersion(int)` does not exist / v5 markers still show 3 and 5 fields.

- [ ] **Step 3: Add version awareness to `PackStreamWriter`** — after the constructors (line 82), add:

```java
  private int boltMajorVersion = 4;

  /**
   * Sets the negotiated Bolt major version driving version-dependent structure encoding
   * (element_id fields on graph elements, UTC vs legacy datetime signatures). Defaults to 4.
   */
  public PackStreamWriter boltMajorVersion(final int boltMajorVersion) {
    this.boltMajorVersion = boltMajorVersion;
    return this;
  }

  public int getBoltMajorVersion() {
    return boltMajorVersion;
  }
```

- [ ] **Step 4: Gate `BoltNode.writeTo`** — replace lines 59-67:

```java
  @Override
  public void writeTo(final PackStreamWriter writer) throws IOException {
    if (writer.getBoltMajorVersion() >= 5) {
      writer.writeStructureHeader(SIGNATURE, 4);
      writer.writeInteger(id);
      writer.writeList(labels);
      writer.writeMap(properties);
      writer.writeString(elementId);
    } else {
      writer.writeStructureHeader(SIGNATURE, 3);
      writer.writeInteger(id);
      writer.writeList(labels);
      writer.writeMap(properties);
    }
  }
```

- [ ] **Step 5: Gate `BoltRelationship.writeTo`** — replace lines 68-78:

```java
  @Override
  public void writeTo(final PackStreamWriter writer) throws IOException {
    if (writer.getBoltMajorVersion() >= 5) {
      writer.writeStructureHeader(SIGNATURE, 8);
      writer.writeInteger(id);
      writer.writeInteger(startNodeId);
      writer.writeInteger(endNodeId);
      writer.writeString(type);
      writer.writeMap(properties);
      writer.writeString(elementId);
      writer.writeString(startNodeElementId);
      writer.writeString(endNodeElementId);
    } else {
      writer.writeStructureHeader(SIGNATURE, 5);
      writer.writeInteger(id);
      writer.writeInteger(startNodeId);
      writer.writeInteger(endNodeId);
      writer.writeString(type);
      writer.writeMap(properties);
    }
  }
```

- [ ] **Step 6: Gate `BoltUnboundRelationship.writeTo`** — replace lines 59-67:

```java
  @Override
  public void writeTo(final PackStreamWriter writer) throws IOException {
    if (writer.getBoltMajorVersion() >= 5) {
      writer.writeStructureHeader(SIGNATURE, 4);
      writer.writeInteger(id);
      writer.writeString(type);
      writer.writeMap(properties);
      writer.writeString(elementId);
    } else {
      writer.writeStructureHeader(SIGNATURE, 3);
      writer.writeInteger(id);
      writer.writeString(type);
      writer.writeMap(properties);
    }
  }
```

Leave each `getFieldCount()` returning the v4 base count (3/5/3) - `BoltStructureTest` asserts those and `writeTo` is now authoritative for the wire.

- [ ] **Step 7: Run tests to verify they pass**

Run: `mvn -pl bolt test -Dtest=BoltStructureTest`
Expected: PASS (existing assertions and the three new v5 tests).

- [ ] **Step 8: Commit**

```bash
git add bolt/src/main/java/com/arcadedb/bolt/packstream/PackStreamWriter.java \
        bolt/src/main/java/com/arcadedb/bolt/structure/BoltNode.java \
        bolt/src/main/java/com/arcadedb/bolt/structure/BoltRelationship.java \
        bolt/src/main/java/com/arcadedb/bolt/structure/BoltUnboundRelationship.java \
        bolt/src/test/java/com/arcadedb/bolt/BoltStructureTest.java
git commit -m "feat(#5001): version-gate Bolt graph-element structs on negotiated major version"
```

---

### Task 2: Version-aware datetime structure (legacy vs UTC signatures)

**Files:**
- Create: `bolt/src/main/java/com/arcadedb/bolt/structure/BoltDateTimeStructure.java`
- Modify: `bolt/src/main/java/com/arcadedb/bolt/structure/BoltStructureMapper.java:480-525` (temporal builder)
- Test: `bolt/src/test/java/com/arcadedb/bolt/BoltDateTimeStructureTest.java`

**Interfaces:**
- Consumes: `PackStreamWriter.getBoltMajorVersion()` (Task 1).
- Produces: `BoltDateTimeStructure` (implements `PackStreamStructure`) that at `writeTo` emits signature `'F'`/`'f'` + local-epoch seconds for major < 5, and `'I'`/`'i'` + true-UTC-epoch seconds for major >= 5. `BoltStructureMapper.toTemporalStructure(Object)` now returns `PackStreamStructure` and builds `BoltDateTimeStructure` for the two DateTime variants.

- [ ] **Step 1: Write the failing test** — create `BoltDateTimeStructureTest.java`:

```java
/* <copy the Apache-2.0 license header from BoltStructureTest.java> */
package com.arcadedb.bolt;

import com.arcadedb.bolt.packstream.PackStreamReader;
import com.arcadedb.bolt.packstream.PackStreamWriter;
import com.arcadedb.bolt.structure.BoltDateTimeStructure;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class BoltDateTimeStructureTest {

  // 2024-01-15T10:30:45 at +02:00.
  private static final LocalDateTime LOCAL = LocalDateTime.of(2024, 1, 15, 10, 30, 45);
  private static final ZoneOffset    OFFSET = ZoneOffset.ofHours(2);

  private static PackStreamReader.StructureValue roundTrip(final BoltDateTimeStructure s, final int major)
      throws Exception {
    final PackStreamWriter writer = new PackStreamWriter().boltMajorVersion(major);
    s.writeTo(writer);
    return (PackStreamReader.StructureValue) new PackStreamReader(writer.toByteArray()).readValue();
  }

  @Test
  void offsetDateTimeUsesLegacySignatureAndLocalEpochOnBolt4() throws Exception {
    final BoltDateTimeStructure s = BoltDateTimeStructure.offset(
        LOCAL.toEpochSecond(ZoneOffset.UTC), LOCAL.toEpochSecond(OFFSET), LOCAL.getNano(), OFFSET.getTotalSeconds());
    final PackStreamReader.StructureValue v = roundTrip(s, 4);
    assertThat(v.getSignature()).isEqualTo((byte) 0x46);              // 'F'
    assertThat(((Number) v.getFields().get(0)).longValue())
        .isEqualTo(LOCAL.toEpochSecond(ZoneOffset.UTC));             // local epoch-second
    assertThat(((Number) v.getFields().get(2)).longValue()).isEqualTo(7200L);
  }

  @Test
  void offsetDateTimeUsesUtcSignatureAndUtcEpochOnBolt5() throws Exception {
    final BoltDateTimeStructure s = BoltDateTimeStructure.offset(
        LOCAL.toEpochSecond(ZoneOffset.UTC), LOCAL.toEpochSecond(OFFSET), LOCAL.getNano(), OFFSET.getTotalSeconds());
    final PackStreamReader.StructureValue v = roundTrip(s, 5);
    assertThat(v.getSignature()).isEqualTo((byte) 0x49);              // 'I'
    assertThat(((Number) v.getFields().get(0)).longValue())
        .isEqualTo(LOCAL.toEpochSecond(OFFSET));                     // true UTC epoch-second
    assertThat(((Number) v.getFields().get(2)).longValue()).isEqualTo(7200L);
  }

  @Test
  void zoneIdDateTimeSwitchesSignatureByVersion() throws Exception {
    final BoltDateTimeStructure s = BoltDateTimeStructure.zoneId(
        LOCAL.toEpochSecond(ZoneOffset.UTC), LOCAL.toEpochSecond(OFFSET), LOCAL.getNano(), "Europe/Rome");
    assertThat(roundTrip(s, 4).getSignature()).isEqualTo((byte) 0x66); // 'f'
    assertThat(roundTrip(s, 5).getSignature()).isEqualTo((byte) 0x69); // 'i'
    assertThat(List.of("Europe/Rome")).contains((String) roundTrip(s, 5).getFields().get(2));
  }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `mvn -pl bolt test -Dtest=BoltDateTimeStructureTest`
Expected: FAIL - `BoltDateTimeStructure` does not exist (compile error).

- [ ] **Step 3: Create `BoltDateTimeStructure`**:

```java
/* <Apache-2.0 license header copied from a sibling file> */
package com.arcadedb.bolt.structure;

import com.arcadedb.bolt.packstream.PackStreamStructure;
import com.arcadedb.bolt.packstream.PackStreamWriter;

import java.io.IOException;

/**
 * Bolt DateTime / DateTimeZoneId structure whose wire form depends on the negotiated protocol version.
 * Bolt 4.x uses the legacy signatures 'F'/'f' with the LOCAL epoch-second (wall clock treated as UTC).
 * Bolt 5.0+ uses 'I'/'i' with the true UTC epoch-second. Both epoch bases are computed at build time
 * from the source java.time value; writeTo selects by {@link PackStreamWriter#getBoltMajorVersion()}.
 */
public class BoltDateTimeStructure implements PackStreamStructure {
  private static final byte SIG_OFFSET_LEGACY = 0x46; // 'F' [secondsLocal, nanos, offsetSeconds]
  private static final byte SIG_ZONEID_LEGACY = 0x66; // 'f' [secondsLocal, nanos, zoneId]
  private static final byte SIG_OFFSET_UTC    = 0x49; // 'I' [secondsUtc,  nanos, offsetSeconds]
  private static final byte SIG_ZONEID_UTC    = 0x69; // 'i' [secondsUtc,  nanos, zoneId]

  private final boolean zoneId;
  private final long    localEpochSeconds;
  private final long    utcEpochSeconds;
  private final int     nanos;
  private final int     offsetSeconds; // used when zoneId == false
  private final String  zone;          // used when zoneId == true

  private BoltDateTimeStructure(final boolean zoneId, final long localEpochSeconds, final long utcEpochSeconds,
      final int nanos, final int offsetSeconds, final String zone) {
    this.zoneId = zoneId;
    this.localEpochSeconds = localEpochSeconds;
    this.utcEpochSeconds = utcEpochSeconds;
    this.nanos = nanos;
    this.offsetSeconds = offsetSeconds;
    this.zone = zone;
  }

  public static BoltDateTimeStructure offset(final long localEpochSeconds, final long utcEpochSeconds, final int nanos,
      final int offsetSeconds) {
    return new BoltDateTimeStructure(false, localEpochSeconds, utcEpochSeconds, nanos, offsetSeconds, null);
  }

  public static BoltDateTimeStructure zoneId(final long localEpochSeconds, final long utcEpochSeconds, final int nanos,
      final String zone) {
    return new BoltDateTimeStructure(true, localEpochSeconds, utcEpochSeconds, nanos, 0, zone);
  }

  @Override
  public byte getSignature() {
    return zoneId ? SIG_ZONEID_LEGACY : SIG_OFFSET_LEGACY;
  }

  @Override
  public int getFieldCount() {
    return 3;
  }

  @Override
  public void writeTo(final PackStreamWriter writer) throws IOException {
    final boolean utc = writer.getBoltMajorVersion() >= 5;
    final long seconds = utc ? utcEpochSeconds : localEpochSeconds;
    if (zoneId) {
      writer.writeStructureHeader(utc ? SIG_ZONEID_UTC : SIG_ZONEID_LEGACY, 3);
      writer.writeInteger(seconds);
      writer.writeInteger(nanos);
      writer.writeString(zone);
    } else {
      writer.writeStructureHeader(utc ? SIG_OFFSET_UTC : SIG_OFFSET_LEGACY, 3);
      writer.writeInteger(seconds);
      writer.writeInteger(nanos);
      writer.writeInteger(offsetSeconds);
    }
  }
}
```

- [ ] **Step 4: Build `BoltDateTimeStructure` from the mapper** — in `BoltStructureMapper.java`:

  1. Change the `toTemporalStructure` return type (line 480) from `static BoltTemporalStructure toTemporalStructure(final Object rawValue)` to `static PackStreamStructure toTemporalStructure(final Object rawValue)`. Add `import com.arcadedb.bolt.packstream.PackStreamStructure;` if absent.

  2. Replace the offset builder `dateTimeWithOffset` (lines 521-525) with:

```java
  private static BoltDateTimeStructure dateTimeWithOffset(final LocalDateTime local, final ZoneOffset offset) {
    // localEpoch: wall clock treated as UTC (legacy basis). utcEpoch: the true instant (5.0+ basis).
    return BoltDateTimeStructure.offset(local.toEpochSecond(ZoneOffset.UTC), local.toEpochSecond(offset),
        local.getNano(), offset.getTotalSeconds());
  }
```

  3. Replace the ZonedDateTime zone-id branch (around lines 496-497) with:

```java
      return BoltDateTimeStructure.zoneId(zdt.toLocalDateTime().toEpochSecond(ZoneOffset.UTC), zdt.toEpochSecond(),
          zdt.getNano(), zdt.getZone().getId());
```

`Date` / `Time` / `LocalTime` / `LocalDateTime` / `Duration` keep using `BoltTemporalStructure` unchanged (version-invariant).

- [ ] **Step 5: Run tests to verify they pass**

Run: `mvn -pl bolt test -Dtest=BoltDateTimeStructureTest,BoltStructureTest,PackStreamTemporalInputTest`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add bolt/src/main/java/com/arcadedb/bolt/structure/BoltDateTimeStructure.java \
        bolt/src/main/java/com/arcadedb/bolt/structure/BoltStructureMapper.java \
        bolt/src/test/java/com/arcadedb/bolt/BoltDateTimeStructureTest.java
git commit -m "feat(#5001): version-aware Bolt DateTime encoding (legacy 'F'/'f' vs UTC 'I'/'i')"
```

---

### Task 3: Bolt 5.1+ auth-flow gate + graceful TELEMETRY

**Files:**
- Modify: `bolt/src/main/java/com/arcadedb/bolt/BoltNetworkExecutor.java` (`handleHello` ~389-427; `processMessage` switch ~344-383; add helper near the version helpers ~1889)
- Create: `bolt/src/main/java/com/arcadedb/bolt/message/TelemetryMessage.java`
- Modify: `bolt/src/main/java/com/arcadedb/bolt/message/BoltMessage.java` (signature constant ~34-49; `parse` switch ~92)
- Test: `bolt/src/test/java/com/arcadedb/bolt/BoltVersionNegotiationTest.java` (add auth-gate unit tests)

**Interfaces:**
- Consumes: `BoltNetworkExecutor.getMajorVersion/getMinorVersion` (existing).
- Produces: `static boolean BoltNetworkExecutor.deferAuthToLogon(int protocolVersion, String scheme, String principal, String credentials)` returning true iff major >= 5, minor >= 1, and all three auth fields are null. `BoltMessage.TELEMETRY` signature `0x54` with a `TelemetryMessage` parsed and acked with SUCCESS.

- [ ] **Step 1: Write the failing test** — append to `BoltVersionNegotiationTest.java`:

```java
@Test
void deferAuthToLogonOnlyForBolt51PlusWithoutCredentials() {
  // 5.1+ HELLO with no auth fields defers to LOGON.
  assertThat(BoltNetworkExecutor.deferAuthToLogon(0x00000105, null, null, null)).isTrue(); // 5.1
  assertThat(BoltNetworkExecutor.deferAuthToLogon(0x00000405, null, null, null)).isTrue(); // 5.4
  // 5.0 keeps HELLO-embedded auth: no deferral.
  assertThat(BoltNetworkExecutor.deferAuthToLogon(0x00000005, null, null, null)).isFalse();
  // 4.4 never defers.
  assertThat(BoltNetworkExecutor.deferAuthToLogon(0x00000404, null, null, null)).isFalse();
  // Any auth field present means it is a real HELLO auth (or explicit none) - do not defer.
  assertThat(BoltNetworkExecutor.deferAuthToLogon(0x00000405, "basic", "root", "pw")).isFalse();
  assertThat(BoltNetworkExecutor.deferAuthToLogon(0x00000405, "none", null, null)).isFalse();
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `mvn -pl bolt test -Dtest=BoltVersionNegotiationTest`
Expected: FAIL - `deferAuthToLogon` does not exist.

- [ ] **Step 3: Add the helper** — in `BoltNetworkExecutor.java`, after `getVersionRange` (~line 1891):

```java
  /**
   * A Bolt 5.1+ HELLO carries no authentication - the driver authenticates with a separate LOGON.
   * Returns true only when the negotiated version is >= 5.1 and the HELLO omits all auth fields, so
   * such a HELLO is accepted (awaiting LOGON) instead of being rejected as "missing credentials".
   * Pre-5.1 keeps HELLO-embedded auth; an explicit scheme (incl. "none") is never a deferral.
   */
  static boolean deferAuthToLogon(final int protocolVersion, final String scheme, final String principal,
      final String credentials) {
    final int major = getMajorVersion(protocolVersion);
    final int minor = getMinorVersion(protocolVersion);
    return major >= 5 && minor >= 1 && scheme == null && principal == null && credentials == null;
  }
```

- [ ] **Step 4: Use the helper in `handleHello`** — in `BoltNetworkExecutor.java`, immediately before the `if ("none".equals(scheme))` block (~line 410), insert:

```java
    if (deferAuthToLogon(protocolVersion, scheme, principal, credentials)) {
      // Bolt 5.1+ handshake: accept HELLO now, authenticate on the subsequent LOGON.
      final Map<String, Object> metadata = new LinkedHashMap<>();
      metadata.put("server", "Neo4j/5.26.0 compatible (ArcadeDB " + Constants.getRawVersion() + ")");
      metadata.put("connection_id", "bolt-" + Thread.currentThread().getId());
      sendSuccess(metadata);
      state = State.AUTHENTICATION;
      return;
    }
```

- [ ] **Step 5: Add TELEMETRY message class** — create `bolt/src/main/java/com/arcadedb/bolt/message/TelemetryMessage.java`:

```java
/* <Apache-2.0 license header copied from a sibling message file> */
package com.arcadedb.bolt.message;

/**
 * Bolt 5.4 TELEMETRY request (signature 0x54). ArcadeDB does not advertise telemetry hints, so a
 * compliant driver never sends this; it is accepted and acknowledged defensively. The payload (a
 * single integer API code) is ignored.
 */
public class TelemetryMessage extends BoltMessage {
  public TelemetryMessage() {
    super(TELEMETRY);
  }
}
```

- [ ] **Step 6: Register the signature and parse path** — in `BoltMessage.java`:

  Add near the other 4.x request signatures (~line 45):

```java
  public static final byte TELEMETRY = 0x54; // Bolt 5.4
```

  Add a case in the `parse` switch (before the `default ->` at ~line 92):

```java
      case TELEMETRY -> new TelemetryMessage();
```

- [ ] **Step 7: Ack TELEMETRY in the executor** — in `processMessage` (~line 378, alongside the other cases), add:

```java
    case BoltMessage.TELEMETRY:
      sendSuccess(Map.of());
      break;
```

- [ ] **Step 8: Run tests to verify they pass**

Run: `mvn -pl bolt test -Dtest=BoltVersionNegotiationTest,BoltMessageTest`
Expected: PASS.

- [ ] **Step 9: Commit**

```bash
git add bolt/src/main/java/com/arcadedb/bolt/BoltNetworkExecutor.java \
        bolt/src/main/java/com/arcadedb/bolt/message/TelemetryMessage.java \
        bolt/src/main/java/com/arcadedb/bolt/message/BoltMessage.java \
        bolt/src/test/java/com/arcadedb/bolt/BoltVersionNegotiationTest.java
git commit -m "feat(#5001): defer auth to LOGON on Bolt 5.1+ HELLO and ack TELEMETRY"
```

---

### Task 4: Advertise Bolt 5.0-5.4 and wire the writer version (go-live)

**Files:**
- Modify: `bolt/src/main/java/com/arcadedb/bolt/BoltNetworkExecutor.java` (`SUPPORTED_VERSIONS` line 100; `sendMessage` line 1700)
- Modify: `bolt/src/test/java/com/arcadedb/bolt/BoltVersionNegotiationTest.java` (use the real array; add 5.x cases)

**Interfaces:**
- Consumes: Tasks 1-3 (version-gated encoders, auth gate).
- Produces: `SUPPORTED_VERSIONS` becomes package-private `static final int[]` advertising `{5.4, 5.3, 5.2, 5.1, 5.0, 4.4, 4.0, 3.0}`; every outgoing message is serialized with the negotiated major version.

- [ ] **Step 1: Write the failing test** — in `BoltVersionNegotiationTest.java`, delete the local `private static final int[] SUPPORTED_VERSIONS = {...}` (~line 71) and make `negotiate` use the production array `BoltNetworkExecutor.SUPPORTED_VERSIONS`. Then append:

```java
@Test
void negotiatesHighest5xForModern5xOnlyDriver() {
  // Driver offers 5.x with a wide range (5.0..5.8) plus padding.
  final int result = negotiate(new int[] { 0x00080805, 0, 0, 0 });
  assertThat(result).isEqualTo(0x00000405); // server ceiling v5.4
}

@Test
void negotiates5xWhenDriverOffersBoth5xAnd44() {
  // Modern driver proposes 5.x (range to 5.0) first, then 4.4 fallback.
  final int result = negotiate(new int[] { 0x00080805, 0x00000404, 0, 0 });
  assertThat(result).isEqualTo(0x00000405); // upgrades to v5.4, not v4.4
}

@Test
void stillNegotiates44ForLegacyOnlyDriver() {
  final int result = negotiate(new int[] { 0x00000404, 0x00000004, 0x00000003, 0 });
  assertThat(result).isEqualTo(0x00000404); // unchanged
}

@Test
void exactMatchV5_2() {
  assertThat(negotiate(new int[] { 0x00000205, 0, 0, 0 })).isEqualTo(0x00000205);
}
```

(Also update the existing `negotiate(...)` helper's inner loop to iterate `BoltNetworkExecutor.SUPPORTED_VERSIONS` instead of the deleted local copy, removing the duplication drift.)

- [ ] **Step 2: Run to verify it fails**

Run: `mvn -pl bolt test -Dtest=BoltVersionNegotiationTest`
Expected: FAIL - `SUPPORTED_VERSIONS` is `private` (compile error) and/or the 5.x expectations are unmet.

- [ ] **Step 3: Advertise 5.0-5.4** — in `BoltNetworkExecutor.java` replace line 100:

```java
  // Supported protocol versions (in order of preference). Package-private so the negotiation unit
  // test asserts against the real advertised set rather than a drifting copy.
  // Encoding: [unused(8)][range(8)][minor(8)][major(8)] — major = value & 0xFF, minor = (value >> 8) & 0xFF
  static final int[] SUPPORTED_VERSIONS = {
      0x00000405, 0x00000305, 0x00000205, 0x00000105, 0x00000005, // v5.4, v5.3, v5.2, v5.1, v5.0
      0x00000404, 0x00000004, 0x00000003                          // v4.4, v4.0, v3.0
  };
```

- [ ] **Step 4: Wire the negotiated version into the writer** — in `sendMessage` (line 1700) replace:

```java
    final PackStreamWriter writer = new PackStreamWriter();
```

with:

```java
    final PackStreamWriter writer = new PackStreamWriter().boltMajorVersion(getMajorVersion(protocolVersion));
```

- [ ] **Step 5: Run the full bolt unit suite**

Run: `mvn -pl bolt test -Dtest=BoltVersionNegotiationTest,BoltStructureTest,BoltDateTimeStructureTest,BoltMessageTest,BoltStructureTest`
Expected: PASS. This is the go-live point: the encoders (Tasks 1-2) now activate for any negotiated 5.x connection.

- [ ] **Step 6: Commit**

```bash
git add bolt/src/main/java/com/arcadedb/bolt/BoltNetworkExecutor.java \
        bolt/src/test/java/com/arcadedb/bolt/BoltVersionNegotiationTest.java
git commit -m "feat(#5001): advertise Bolt 5.0-5.4 and encode outbound records at the negotiated version"
```

---

### Task 5: Flip PROTO-002 in the conformance spec + full-driver verification

**Files:**
- Modify: `bolt/conformance/spec.yaml` (the `PROTO-002` scenario)
- Verify (no change expected): `bolt/src/test/java/com/arcadedb/bolt/Bolt4907TemporalOutputIT.java`, `BoltOffsetDatetimeWriteIT.java`, `BoltTypeRoundTripTest.java`, `BoltProtocolIT.java`, `BoltMultiLabelIT.java`

**Interfaces:**
- Consumes: Task 4 (5.x live end-to-end).
- Produces: `PROTO-002` marked `passing`; the real `neo4j-java-driver` ITs green under the now-negotiated Bolt 5.4.

- [ ] **Step 1: Flip the scenario** — in `bolt/conformance/spec.yaml`, in the `PROTO-002` block: change `current_status: expected-fail` to `current_status: passing`, remove the `known_limitation:` block, and remove the `tracking_issue: "#4890"` line. Leave `PROTO-001` untouched.

- [ ] **Step 2: Validate the spec file**

Run: `cd bolt/conformance && python3 validate_spec.py` (or `python3 -m pytest test_validate_spec.py` if that is the module's entry point)
Expected: PASS - spec is well-formed and PROTO-002 is now `passing`.

- [ ] **Step 3: Run the real-driver ITs (these exercise the 5.4 flip end-to-end)**

Run: `mvn -pl bolt verify -Dtest=Bolt4907TemporalOutputIT,BoltOffsetDatetimeWriteIT,BoltTypeRoundTripTest,BoltProtocolIT,BoltMultiLabelIT`
Expected: PASS. These assert decoded java.time / node / relationship values through the real driver; with the driver now negotiating 5.4, correct UTC datetime and element_id encoding keep them green. If any assert a raw wire signature/field-count for 4.x, update that assertion to the 5.x form and note it in the commit body.

- [ ] **Step 4: Run the whole bolt module**

Run: `mvn -pl bolt verify`
Expected: PASS (unit + ITs). Investigate and fix any red before committing.

- [ ] **Step 5: Commit**

```bash
git add bolt/conformance/spec.yaml
git commit -m "test(#5001): mark PROTO-002 passing - Bolt 5.x negotiation certified"
```

---

## Self-Review

**Spec coverage:**
- Decision to advertise 5.0-5.4 → Task 4 (`SUPPORTED_VERSIONS`). ✓
- Version-gated Node/Relationship/UnboundRelationship field counts → Task 1. ✓
- Version-gated DateTime signatures (legacy vs UTC) → Task 2. ✓
- Inbound temporal params unchanged → confirmed (no task needed; reader already decodes both). ✓
- Auth-flow gate for 5.1+ auth-less HELLO; `none` still rejected → Task 3. ✓
- TELEMETRY graceful ack → Task 3. ✓
- Existing 3.0/4.0/4.4 negotiation unaffected → Task 4 Step 1 `stillNegotiates44ForLegacyOnlyDriver`. ✓
- PROTO-002 flipped; TYPE round-trip stays green → Task 5. ✓
- Handshake manifest (5.7/5.8) and 5.5/5.6 excluded → not implemented (non-goal); documented for #4884. ✓

**Placeholder scan:** No TBD/TODO; every code step shows complete code. The one `<copy license header>` note is an explicit instruction, not a code placeholder.

**Type consistency:** `boltMajorVersion(int)` / `getBoltMajorVersion()` used consistently across Tasks 1, 2, 4. `deferAuthToLogon(int,String,String,String)` signature identical in Task 3 test and impl. `BoltDateTimeStructure.offset(...)` / `.zoneId(...)` factory signatures match between the class (Task 2 Step 3) and its test (Task 2 Step 1) and the mapper call sites (Task 2 Step 4). `toTemporalStructure` return type widened to `PackStreamStructure` in the same task that relies on it.

**Note on refinement vs spec §4.3:** the spec suggested making the negotiated version available at *mapping* time for temporals; this plan refines that to a *writer-driven* mechanism (the datetime struct carries both epoch bases and picks at `writeTo`), which is cleaner and yields identical wire bytes - nested temporals inside node/map/list properties are handled automatically without threading a version param through the mapper recursion.
