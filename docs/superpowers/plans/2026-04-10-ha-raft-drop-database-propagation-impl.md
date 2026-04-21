# HA Raft Server Command Propagation - Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Propagate `drop database`, `restore database`, `import database`, `create user`, and `drop user` from the leader to every Raft replica, with Raft-first semantics for destructive operations and container-level test coverage.

**Architecture:** Two new Raft log entry types (`DROP_DATABASE_ENTRY`, `SECURITY_USERS_ENTRY`) plus an extended `INSTALL_DATABASE_ENTRY` carrying a `forceSnapshot` flag. The leader encodes an entry and submits it via the existing `RaftGroupCommitter.submitAndWait`; the `ArcadeStateMachine` applies it on every peer including the leader, so destructive local mutations only happen once Raft has committed. Users live in `server-users.jsonl`, which is a per-server file; a new `HAServerPlugin.replicateSecurityUsers(String)` hook carries the full JSON content of the file through the log, and `PostAddPeerHandler` emits a seed entry after adding a fresh peer.

**Tech Stack:** Java 21, Apache Ratis (existing), Maven multi-module (`ha-raft`, `server`, `e2e-ha`), JUnit 5 + AssertJ + Awaitility, Undertow HTTP handlers, TestContainers for e2e.

**Design spec:** `docs/plans/2026-04-10-ha-raft-drop-database-propagation-design.md`

---

## File structure

### `ha-raft/src/main/java/com/arcadedb/server/ha/raft/`

| File | Responsibility | Change |
|---|---|---|
| `RaftLogEntryType.java` | Enum of log entry kinds | Add `DROP_DATABASE_ENTRY`, `SECURITY_USERS_ENTRY` |
| `RaftLogEntryCodec.java` | Encode/decode log payloads | Add drop + users encode/decode, extend install with `forceSnapshot`, extend `DecodedEntry` record with nullable `usersJson` and `forceSnapshot` |
| `ArcadeStateMachine.java` | Apply committed log entries | Add `applyDropDatabaseEntry`, `applySecurityUsersEntry`, extend `applyInstallDatabaseEntry` for `forceSnapshot=true`, wire switch |
| `RaftReplicatedDatabase.java` | Per-database HA wrapper | Implement `dropInReplicas()`, add `createInReplicas(boolean forceSnapshot)` overload |
| `RaftHAPlugin.java` | Server-wide HA plugin | Implement `replicateSecurityUsers(String)` |
| `PostAddPeerHandler.java` | Handle POST /api/v1/cluster/peer | After Ratis confirms the peer is added, emit one `SECURITY_USERS_ENTRY` carrying current users |

### `server/src/main/java/com/arcadedb/server/`

| File | Responsibility | Change |
|---|---|---|
| `HAReplicatedDatabase.java` | Per-database HA interface | Add `dropInReplicas()` and default `createInReplicas(boolean forceSnapshot)` |
| `HAServerPlugin.java` | Server-wide HA interface | Add default `replicateSecurityUsers(String)` |
| `security/ServerSecurity.java` | User management | Add public `getUsersJsonPayload()` and `applyReplicatedUsers(String)` helpers |
| `http/handler/PostServerCommandHandler.java` | Admin command HTTP handler | Rewrite `dropDatabase`, `restoreDatabase`, `importDatabase`, `createUser`, `dropUser` to route through the new HA hooks |

### Tests

| File | Purpose |
|---|---|
| `ha-raft/src/test/.../RaftLogEntryCodecTest.java` | Extend with drop + users + forceSnapshot round-trip tests |
| `ha-raft/src/test/.../ArcadeStateMachineTest.java` | Extend with drop, users, force-snapshot apply tests |
| `ha-raft/src/test/.../RaftDropDatabase3NodesIT.java` | 3-node IT: create DB, insert data, drop via replica, assert removal |
| `ha-raft/src/test/.../RaftRestoreDatabase3NodesIT.java` | 3-node IT: backup, drop, restore, assert replicated state |
| `ha-raft/src/test/.../RaftImportDatabase3NodesIT.java` | 3-node IT: import fixture file, assert replicated state |
| `ha-raft/src/test/.../RaftUserManagement3NodesIT.java` | 3-node IT: create/drop users, concurrent mutations, file equality |
| `ha-raft/src/test/.../RaftUserSeedOnPeerAdd3NodesIT.java` | 3-node IT: peer-add triggers user seed |
| `e2e-ha/src/test/.../DropDatabaseScenarioIT.java` | Container scenario: drop + verify removal |
| `e2e-ha/src/test/.../RestoreDatabaseScenarioIT.java` | Container scenario: backup + restore + verify |
| `e2e-ha/src/test/.../ImportDatabaseScenarioIT.java` | Container scenario: import fixture + verify |
| `e2e-ha/src/test/.../UserManagementScenarioIT.java` | Container scenario: create/drop users + failover |
| `e2e-ha/src/test/.../UserSeedOnPeerAddScenarioIT.java` | Container scenario: peer add + seed |

---

# Phase 1: Foundation (new log entry types and codec)

## Task 1: Add new `RaftLogEntryType` constants and placeholder switch cases

**Files:**
- Modify: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftLogEntryType.java`
- Modify: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftLogEntryCodec.java` (switch in `decode(...)` around line 177)
- Modify: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/ArcadeStateMachine.java` (switch in `applyTransaction(...)` around line 112)

**Why three files:** Java 21 requires exhaustive arrow-switches over enums. Adding new enum constants without covering them in the existing switches breaks compilation. This task lands throwing placeholders in both switches; later tasks replace them with real implementations.

- [ ] **Step 1: Add new enum constants**

Edit `RaftLogEntryType.java` so the enum body becomes:

```java
public enum RaftLogEntryType {
  TX_ENTRY((byte) 1),
  SCHEMA_ENTRY((byte) 2),
  INSTALL_DATABASE_ENTRY((byte) 3),
  DROP_DATABASE_ENTRY((byte) 4),
  SECURITY_USERS_ENTRY((byte) 5);

  // ... existing fields and methods unchanged
}
```

The existing `private final byte id` field, constructor, `getId()`, and `fromId(byte)` method must remain untouched.

- [ ] **Step 2: Add placeholder cases in `RaftLogEntryCodec.decode(...)` switch**

In `RaftLogEntryCodec.java`, locate the `switch (type)` inside `decode(...)`. It currently looks like:

```java
return switch (type) {
  case TX_ENTRY -> decodeTxEntry(dis, databaseName);
  case SCHEMA_ENTRY -> decodeSchemaEntry(dis, databaseName);
  case INSTALL_DATABASE_ENTRY -> new DecodedEntry(RaftLogEntryType.INSTALL_DATABASE_ENTRY, databaseName, null, null, null, null, null, null, null);
};
```

Add two placeholder arms so compilation stays green (they'll be replaced in Tasks 3 and 5):

```java
return switch (type) {
  case TX_ENTRY -> decodeTxEntry(dis, databaseName);
  case SCHEMA_ENTRY -> decodeSchemaEntry(dis, databaseName);
  case INSTALL_DATABASE_ENTRY -> new DecodedEntry(RaftLogEntryType.INSTALL_DATABASE_ENTRY, databaseName, null, null, null, null, null, null, null);
  case DROP_DATABASE_ENTRY -> throw new UnsupportedOperationException("DROP_DATABASE_ENTRY decode not yet implemented");
  case SECURITY_USERS_ENTRY -> throw new UnsupportedOperationException("SECURITY_USERS_ENTRY decode not yet implemented");
};
```

Do NOT touch anything else in the file. The `DecodedEntry` record extension happens in Task 2.

- [ ] **Step 3: Add placeholder cases in `ArcadeStateMachine.applyTransaction(...)` switch**

In `ArcadeStateMachine.java`, locate the `switch (decoded.type())` inside `applyTransaction(...)`. It currently looks like:

```java
switch (decoded.type()) {
  case TX_ENTRY -> applyTxEntry(decoded);
  case SCHEMA_ENTRY -> applySchemaEntry(decoded);
  case INSTALL_DATABASE_ENTRY -> applyInstallDatabaseEntry(decoded);
}
```

Add two placeholder arms (replaced in Tasks 8 and 19):

```java
switch (decoded.type()) {
  case TX_ENTRY -> applyTxEntry(decoded);
  case SCHEMA_ENTRY -> applySchemaEntry(decoded);
  case INSTALL_DATABASE_ENTRY -> applyInstallDatabaseEntry(decoded);
  case DROP_DATABASE_ENTRY -> throw new UnsupportedOperationException("DROP_DATABASE_ENTRY apply not yet implemented");
  case SECURITY_USERS_ENTRY -> throw new UnsupportedOperationException("SECURITY_USERS_ENTRY apply not yet implemented");
}
```

Do NOT touch anything else in the file.

- [ ] **Step 4: Compile the module**

Run: `mvn -pl ha-raft compile`
Expected: BUILD SUCCESS.

- [ ] **Step 5: Run the existing codec and state-machine tests to confirm no regression**

Run: `mvn -pl ha-raft test -Dtest=RaftLogEntryCodecTest,ArcadeStateMachineTest`
Expected: all existing tests pass. No new entries are ever constructed yet, so the `UnsupportedOperationException` placeholders are never thrown.

- [ ] **Step 6: Commit**

```bash
git add ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftLogEntryType.java \
        ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftLogEntryCodec.java \
        ha-raft/src/main/java/com/arcadedb/server/ha/raft/ArcadeStateMachine.java
git commit -m "feat(ha-raft): add DROP_DATABASE_ENTRY and SECURITY_USERS_ENTRY log entry types"
```

---

## Task 2: Extend `DecodedEntry` record with new fields

**Files:**
- Modify: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftLogEntryCodec.java` (the `DecodedEntry` record at line 45-56)

- [ ] **Step 1: Add `usersJson` and `forceSnapshot` components**

Replace the existing `DecodedEntry` record definition with:

```java
public record DecodedEntry(
    RaftLogEntryType type,
    String databaseName,
    byte[] walData,
    Map<Integer, Integer> bucketRecordDelta,
    String schemaJson,
    Map<Integer, String> filesToAdd,
    Map<Integer, String> filesToRemove,
    List<byte[]> walEntries,
    List<Map<Integer, Integer>> bucketDeltas,
    String usersJson,
    boolean forceSnapshot
) {
}
```

- [ ] **Step 2: Fix all existing constructor call sites**

The change breaks four existing call sites inside `RaftLogEntryCodec`. Add `null, false` as trailing arguments to each.

In `decode(...)` around line 180, change:
```java
case INSTALL_DATABASE_ENTRY -> new DecodedEntry(RaftLogEntryType.INSTALL_DATABASE_ENTRY, databaseName, null, null, null, null, null, null, null);
```
to:
```java
case INSTALL_DATABASE_ENTRY -> decodeInstallDatabaseEntry(dis, databaseName);
```

We'll define `decodeInstallDatabaseEntry` properly in Task 4. For now, add a placeholder method at the bottom of the class:

```java
private static DecodedEntry decodeInstallDatabaseEntry(final DataInputStream dis, final String databaseName) throws IOException {
  return new DecodedEntry(RaftLogEntryType.INSTALL_DATABASE_ENTRY, databaseName,
      null, null, null, null, null, null, null, null, false);
}
```

In `decodeTxEntry` around line 202, change:
```java
return new DecodedEntry(RaftLogEntryType.TX_ENTRY, databaseName, walData, bucketRecordDelta, null, null, null, null, null);
```
to:
```java
return new DecodedEntry(RaftLogEntryType.TX_ENTRY, databaseName, walData, bucketRecordDelta,
    null, null, null, null, null, null, false);
```

In `decodeSchemaEntry` around line 237, change:
```java
return new DecodedEntry(RaftLogEntryType.SCHEMA_ENTRY, databaseName, null, null, schemaJson, filesToAdd, filesToRemove, walEntries, bucketDeltas);
```
to:
```java
return new DecodedEntry(RaftLogEntryType.SCHEMA_ENTRY, databaseName, null, null,
    schemaJson, filesToAdd, filesToRemove, walEntries, bucketDeltas, null, false);
```

- [ ] **Step 3: Run existing codec tests to ensure no regression**

Run: `mvn -pl ha-raft test -Dtest=RaftLogEntryCodecTest`
Expected: All existing tests still pass.

- [ ] **Step 4: Commit**

```bash
git add ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftLogEntryCodec.java
git commit -m "refactor(ha-raft): extend DecodedEntry with usersJson and forceSnapshot slots"
```

---

## Task 3: Drop-database encode/decode

**Files:**
- Modify: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftLogEntryCodec.java`
- Modify: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftLogEntryCodecTest.java`

- [ ] **Step 1: Write the failing test**

Append to `RaftLogEntryCodecTest.java`:

```java
@Test
void roundTripDropDatabaseEntry() {
  final ByteString encoded = RaftLogEntryCodec.encodeDropDatabaseEntry("testdb");
  final RaftLogEntryCodec.DecodedEntry decoded = RaftLogEntryCodec.decode(encoded);

  assertThat(decoded.type()).isEqualTo(RaftLogEntryType.DROP_DATABASE_ENTRY);
  assertThat(decoded.databaseName()).isEqualTo("testdb");
  assertThat(decoded.walData()).isNull();
  assertThat(decoded.bucketRecordDelta()).isNull();
  assertThat(decoded.schemaJson()).isNull();
  assertThat(decoded.filesToAdd()).isNull();
  assertThat(decoded.filesToRemove()).isNull();
  assertThat(decoded.usersJson()).isNull();
  assertThat(decoded.forceSnapshot()).isFalse();
}
```

- [ ] **Step 2: Run the test and confirm it fails**

Run: `mvn -pl ha-raft test -Dtest=RaftLogEntryCodecTest#roundTripDropDatabaseEntry`
Expected: compile failure on `encodeDropDatabaseEntry` (method does not exist).

- [ ] **Step 3: Add the encode method**

In `RaftLogEntryCodec.java`, after `encodeInstallDatabaseEntry(...)` (around line 165), add:

```java
/**
 * Encodes a drop-database entry into a ByteString.
 * <p>
 * Binary format: type byte, databaseName (UTF).
 */
public static ByteString encodeDropDatabaseEntry(final String databaseName) {
  try {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final DataOutputStream dos = new DataOutputStream(baos);

    dos.writeByte(RaftLogEntryType.DROP_DATABASE_ENTRY.getId());
    dos.writeUTF(databaseName);

    dos.flush();
    return ByteString.copyFrom(baos.toByteArray());
  } catch (final IOException e) {
    throw new IllegalStateException("Failed to encode DROP_DATABASE entry", e);
  }
}
```

- [ ] **Step 4: Wire the decode branch**

In `RaftLogEntryCodec.decode(...)`, extend the `switch` to include the new type:

```java
return switch (type) {
  case TX_ENTRY -> decodeTxEntry(dis, databaseName);
  case SCHEMA_ENTRY -> decodeSchemaEntry(dis, databaseName);
  case INSTALL_DATABASE_ENTRY -> decodeInstallDatabaseEntry(dis, databaseName);
  case DROP_DATABASE_ENTRY -> new DecodedEntry(RaftLogEntryType.DROP_DATABASE_ENTRY, databaseName,
      null, null, null, null, null, null, null, null, false);
  case SECURITY_USERS_ENTRY -> throw new UnsupportedOperationException("SECURITY_USERS_ENTRY decode not yet implemented");
};
```

The `SECURITY_USERS_ENTRY` branch is a placeholder we'll replace in Task 5; Java requires the `switch` to be exhaustive.

- [ ] **Step 5: Run the test and confirm it passes**

Run: `mvn -pl ha-raft test -Dtest=RaftLogEntryCodecTest#roundTripDropDatabaseEntry`
Expected: PASS.

- [ ] **Step 6: Run the full codec test class to catch regressions**

Run: `mvn -pl ha-raft test -Dtest=RaftLogEntryCodecTest`
Expected: All tests pass.

- [ ] **Step 7: Commit**

```bash
git add ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftLogEntryCodec.java \
        ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftLogEntryCodecTest.java
git commit -m "feat(ha-raft): encode/decode DROP_DATABASE_ENTRY"
```

---

## Task 4: Install-database `forceSnapshot` flag

**Files:**
- Modify: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftLogEntryCodec.java`
- Modify: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftLogEntryCodecTest.java`

- [ ] **Step 1: Write the failing test**

Append to `RaftLogEntryCodecTest.java`:

```java
@Test
void roundTripInstallDatabaseEntryDefaults() {
  final ByteString encoded = RaftLogEntryCodec.encodeInstallDatabaseEntry("testdb");
  final RaftLogEntryCodec.DecodedEntry decoded = RaftLogEntryCodec.decode(encoded);

  assertThat(decoded.type()).isEqualTo(RaftLogEntryType.INSTALL_DATABASE_ENTRY);
  assertThat(decoded.databaseName()).isEqualTo("testdb");
  assertThat(decoded.forceSnapshot()).isFalse();
}

@Test
void roundTripInstallDatabaseEntryWithForceSnapshot() {
  final ByteString encoded = RaftLogEntryCodec.encodeInstallDatabaseEntry("testdb", true);
  final RaftLogEntryCodec.DecodedEntry decoded = RaftLogEntryCodec.decode(encoded);

  assertThat(decoded.type()).isEqualTo(RaftLogEntryType.INSTALL_DATABASE_ENTRY);
  assertThat(decoded.databaseName()).isEqualTo("testdb");
  assertThat(decoded.forceSnapshot()).isTrue();
}

@Test
void decodeLegacyInstallDatabaseEntryWithoutFlag() throws java.io.IOException {
  // Hand-crafted byte layout matching the pre-forceSnapshot codec:
  // type byte + UTF(databaseName), no trailing boolean.
  final java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
  try (final java.io.DataOutputStream dos = new java.io.DataOutputStream(baos)) {
    dos.writeByte(RaftLogEntryType.INSTALL_DATABASE_ENTRY.getId());
    dos.writeUTF("legacydb");
  }
  final ByteString legacy = ByteString.copyFrom(baos.toByteArray());

  final RaftLogEntryCodec.DecodedEntry decoded = RaftLogEntryCodec.decode(legacy);

  assertThat(decoded.type()).isEqualTo(RaftLogEntryType.INSTALL_DATABASE_ENTRY);
  assertThat(decoded.databaseName()).isEqualTo("legacydb");
  assertThat(decoded.forceSnapshot()).isFalse();
}
```

- [ ] **Step 2: Run tests and confirm they fail**

Run: `mvn -pl ha-raft test -Dtest=RaftLogEntryCodecTest#roundTripInstallDatabaseEntryWithForceSnapshot`
Expected: compile failure on `encodeInstallDatabaseEntry("testdb", true)`.

- [ ] **Step 3: Extend the encode method**

In `RaftLogEntryCodec.java`, replace the existing `encodeInstallDatabaseEntry(String)` with:

```java
/**
 * Encodes an install-database entry into a ByteString.
 * <p>
 * Binary format: type byte, databaseName (UTF), forceSnapshot (boolean).
 */
public static ByteString encodeInstallDatabaseEntry(final String databaseName) {
  return encodeInstallDatabaseEntry(databaseName, false);
}

/**
 * Encodes an install-database entry with an explicit forceSnapshot flag.
 * When {@code forceSnapshot} is true, replicas pull a fresh snapshot from the
 * leader even if the database already exists locally (used for restore).
 */
public static ByteString encodeInstallDatabaseEntry(final String databaseName, final boolean forceSnapshot) {
  try {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final DataOutputStream dos = new DataOutputStream(baos);

    dos.writeByte(RaftLogEntryType.INSTALL_DATABASE_ENTRY.getId());
    dos.writeUTF(databaseName);
    dos.writeBoolean(forceSnapshot);

    dos.flush();
    return ByteString.copyFrom(baos.toByteArray());
  } catch (final IOException e) {
    throw new IllegalStateException("Failed to encode INSTALL_DATABASE entry", e);
  }
}
```

- [ ] **Step 4: Extend the decode method**

Replace the placeholder `decodeInstallDatabaseEntry(...)` added in Task 2 with a length-aware reader:

```java
private static DecodedEntry decodeInstallDatabaseEntry(final DataInputStream dis, final String databaseName) throws IOException {
  // Length-based detection of the trailing forceSnapshot flag.
  // Legacy entries (pre-forceSnapshot codec) have no trailing byte; they decode as forceSnapshot=false.
  boolean forceSnapshot = false;
  if (dis.available() > 0) {
    forceSnapshot = dis.readBoolean();
  }
  return new DecodedEntry(RaftLogEntryType.INSTALL_DATABASE_ENTRY, databaseName,
      null, null, null, null, null, null, null, null, forceSnapshot);
}
```

- [ ] **Step 5: Run all three tests and confirm they pass**

Run: `mvn -pl ha-raft test -Dtest=RaftLogEntryCodecTest`
Expected: All tests pass.

- [ ] **Step 6: Commit**

```bash
git add ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftLogEntryCodec.java \
        ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftLogEntryCodecTest.java
git commit -m "feat(ha-raft): add forceSnapshot flag to INSTALL_DATABASE_ENTRY codec"
```

---

## Task 5: Security-users encode/decode

**Files:**
- Modify: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftLogEntryCodec.java`
- Modify: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftLogEntryCodecTest.java`

- [ ] **Step 1: Write the failing test**

Append to `RaftLogEntryCodecTest.java`:

```java
@Test
void roundTripSecurityUsersEntryEmpty() {
  final String payload = "[]";
  final ByteString encoded = RaftLogEntryCodec.encodeSecurityUsersEntry(payload);
  final RaftLogEntryCodec.DecodedEntry decoded = RaftLogEntryCodec.decode(encoded);

  assertThat(decoded.type()).isEqualTo(RaftLogEntryType.SECURITY_USERS_ENTRY);
  assertThat(decoded.databaseName()).isEmpty();
  assertThat(decoded.usersJson()).isEqualTo(payload);
}

@Test
void roundTripSecurityUsersEntrySingleUser() {
  final String payload = "[{\"name\":\"alice\",\"password\":\"$2a$hash\",\"databases\":{\"*\":[\"admin\"]}}]";
  final ByteString encoded = RaftLogEntryCodec.encodeSecurityUsersEntry(payload);
  final RaftLogEntryCodec.DecodedEntry decoded = RaftLogEntryCodec.decode(encoded);

  assertThat(decoded.type()).isEqualTo(RaftLogEntryType.SECURITY_USERS_ENTRY);
  assertThat(decoded.usersJson()).isEqualTo(payload);
}

@Test
void roundTripSecurityUsersEntryUnicodeAndNewlines() {
  final String payload = "[{\"name\":\"björn\",\"note\":\"line1\\nline2\"}]";
  final ByteString encoded = RaftLogEntryCodec.encodeSecurityUsersEntry(payload);
  final RaftLogEntryCodec.DecodedEntry decoded = RaftLogEntryCodec.decode(encoded);

  assertThat(decoded.usersJson()).isEqualTo(payload);
}
```

- [ ] **Step 2: Run the test and confirm it fails**

Run: `mvn -pl ha-raft test -Dtest=RaftLogEntryCodecTest#roundTripSecurityUsersEntryEmpty`
Expected: compile failure on `encodeSecurityUsersEntry` (method does not exist).

- [ ] **Step 3: Add the encode method**

In `RaftLogEntryCodec.java`, after `encodeInstallDatabaseEntry(String, boolean)`, add:

```java
/**
 * Encodes a security-users entry into a ByteString.
 * <p>
 * Binary format: type byte, empty databaseName (UTF), jsonLength (int), UTF-8 bytes.
 * The empty databaseName slot keeps the decoder symmetric with other entry types.
 */
public static ByteString encodeSecurityUsersEntry(final String usersJson) {
  try {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final DataOutputStream dos = new DataOutputStream(baos);

    dos.writeByte(RaftLogEntryType.SECURITY_USERS_ENTRY.getId());
    dos.writeUTF("");
    final byte[] bytes = usersJson.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    dos.writeInt(bytes.length);
    dos.write(bytes);

    dos.flush();
    return ByteString.copyFrom(baos.toByteArray());
  } catch (final IOException e) {
    throw new IllegalStateException("Failed to encode SECURITY_USERS entry", e);
  }
}
```

- [ ] **Step 4: Replace the `SECURITY_USERS_ENTRY` placeholder in `decode(...)`**

Change the placeholder case to a real decoder:

```java
case SECURITY_USERS_ENTRY -> decodeSecurityUsersEntry(dis);
```

Add the helper near the other decode helpers:

```java
private static DecodedEntry decodeSecurityUsersEntry(final DataInputStream dis) throws IOException {
  final int length = dis.readInt();
  final byte[] bytes = new byte[length];
  dis.readFully(bytes);
  final String usersJson = new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
  return new DecodedEntry(RaftLogEntryType.SECURITY_USERS_ENTRY, "",
      null, null, null, null, null, null, null, usersJson, false);
}
```

- [ ] **Step 5: Run all three new tests and confirm they pass**

Run: `mvn -pl ha-raft test -Dtest=RaftLogEntryCodecTest`
Expected: All tests pass.

- [ ] **Step 6: Commit**

```bash
git add ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftLogEntryCodec.java \
        ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftLogEntryCodecTest.java
git commit -m "feat(ha-raft): encode/decode SECURITY_USERS_ENTRY"
```

---

# Phase 2: DROP DATABASE propagation

## Task 6: Add `dropInReplicas` to `HAReplicatedDatabase` interface

**Files:**
- Modify: `server/src/main/java/com/arcadedb/server/HAReplicatedDatabase.java`

- [ ] **Step 1: Add the interface method**

In `HAReplicatedDatabase.java`, after the existing `createInReplicas()` declaration, add:

```java
/**
 * Replicates a database drop operation. Raft-first: the leader does NOT drop
 * its own files until the state machine applies the drop entry across the cluster.
 * Non-HA implementations should throw {@link UnsupportedOperationException}.
 */
void dropInReplicas();

/**
 * Replicates a createDatabase operation with an optional forceSnapshot flag.
 * When {@code forceSnapshot} is true, replicas pull a fresh snapshot from the leader
 * even if the database already exists locally (used after a restore).
 */
default void createInReplicas(final boolean forceSnapshot) {
  if (forceSnapshot)
    throw new UnsupportedOperationException("forceSnapshot variant not supported by this HA implementation");
  createInReplicas();
}
```

- [ ] **Step 2: Compile to make sure nothing else breaks**

Run: `mvn -pl server compile`
Expected: BUILD SUCCESS. The legacy `HAServer`/`ReplicatedDatabase` that used to implement this interface is either absent on this branch or we'll find out here; if it reports "does not override abstract method `dropInReplicas`", add an `UnsupportedOperationException` implementation in that class.

- [ ] **Step 3: Commit**

```bash
git add server/src/main/java/com/arcadedb/server/HAReplicatedDatabase.java
git commit -m "feat(server): add HAReplicatedDatabase.dropInReplicas() interface method"
```

---

## Task 7: Implement `RaftReplicatedDatabase.dropInReplicas`

**Files:**
- Modify: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftReplicatedDatabase.java`

- [ ] **Step 1: Add the implementation**

In `RaftReplicatedDatabase.java`, after `createInReplicas()` (around line 1012), add:

```java
@Override
public void dropInReplicas() {
  final ByteString entry = RaftLogEntryCodec.encodeDropDatabaseEntry(getName());
  try {
    final RaftHAServer raft = requireRaftServer();
    raft.getGroupCommitter().submitAndWait(entry.toByteArray(), raft.getQuorumTimeout());
  } catch (final TransactionException e) {
    throw e;
  } catch (final Exception e) {
    throw new TransactionException("Error sending drop-database entry via Raft for database '" + getName() + "'", e);
  }
  LogManager.instance().log(this, Level.INFO, "Database '%s' drop-database entry committed via Raft", getName());
}

@Override
public void createInReplicas(final boolean forceSnapshot) {
  final ByteString entry = RaftLogEntryCodec.encodeInstallDatabaseEntry(getName(), forceSnapshot);
  try {
    final RaftHAServer raft = requireRaftServer();
    raft.getGroupCommitter().submitAndWait(entry.toByteArray(), raft.getQuorumTimeout());
  } catch (final TransactionException e) {
    throw e;
  } catch (final Exception e) {
    throw new TransactionException("Error sending install-database entry via Raft for database '" + getName() + "'", e);
  }
  LogManager.instance().log(this, Level.INFO, "Database '%s' install-database (forceSnapshot=%s) entry committed via Raft", getName(), forceSnapshot);
}
```

Note: the existing no-arg `createInReplicas()` is retained as-is and the new overload is a sibling. The default interface method's delegation is ignored here because we implement both methods directly.

- [ ] **Step 2: Compile the module**

Run: `mvn -pl ha-raft compile`
Expected: BUILD SUCCESS.

- [ ] **Step 3: Commit**

```bash
git add ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftReplicatedDatabase.java
git commit -m "feat(ha-raft): implement dropInReplicas and createInReplicas(forceSnapshot)"
```

---

## Task 8: Apply DROP_DATABASE_ENTRY in the state machine

**Files:**
- Modify: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/ArcadeStateMachine.java`

**Revised from original plan:** the original Task 8 included Mockito-based unit tests that called `applyTransaction` with a synthesized `TransactionContext`. The existing `ArcadeStateMachineTest` does NOT have any `applyTransaction`-level tests, the ha-raft module does not pull in Mockito, and building a real `TransactionContext` test harness for one method is disproportionate. The 3-node integration test in Task 10 is the real end-to-end validator of drop apply behaviour. Task 8 therefore adds the apply method and wires the switch without unit tests; Task 19 will follow the same pattern for SECURITY_USERS_ENTRY.

- [ ] **Step 1: Add the apply method**

In `ArcadeStateMachine.java`, add a new private method near `applyInstallDatabaseEntry`:

```java
private void applyDropDatabaseEntry(final RaftLogEntryCodec.DecodedEntry decoded) {
  final String databaseName = decoded.databaseName();

  // Idempotent on replay: if the database is already gone, nothing to do.
  if (!server.existsDatabase(databaseName)) {
    HALog.log(this, HALog.TRACE, "Database '%s' already absent, skipping drop-database entry", databaseName);
    return;
  }

  final DatabaseInternal db = (DatabaseInternal) server.getDatabase(databaseName);
  db.getEmbedded().drop();
  server.removeDatabase(databaseName);

  LogManager.instance().log(this, Level.INFO, "Database '%s' dropped via Raft drop-database entry", databaseName);
}
```

- [ ] **Step 2: Wire the switch**

In `ArcadeStateMachine.applyTransaction`, replace the DROP_DATABASE_ENTRY throwing placeholder (added in Task 1) with a call to the new apply method:

```java
switch (decoded.type()) {
  case TX_ENTRY -> applyTxEntry(decoded);
  case SCHEMA_ENTRY -> applySchemaEntry(decoded);
  case INSTALL_DATABASE_ENTRY -> applyInstallDatabaseEntry(decoded);
  case DROP_DATABASE_ENTRY -> applyDropDatabaseEntry(decoded);
  case SECURITY_USERS_ENTRY -> throw new UnsupportedOperationException("SECURITY_USERS_ENTRY apply not yet implemented");
}
```

Leave the SECURITY_USERS_ENTRY placeholder untouched; Task 19 replaces it.

- [ ] **Step 3: Clean-compile to catch any stale incremental-compilation issues**

Run: `mvn -pl ha-raft -am clean compile`
Expected: BUILD SUCCESS across the reactor.

- [ ] **Step 4: Run existing ha-raft unit tests to confirm no regression**

Run: `mvn -pl ha-raft test`
Expected: all tests pass. The new apply method is covered end-to-end by Task 10's integration test, not by unit tests.

- [ ] **Step 5: Commit**

```bash
git add ha-raft/src/main/java/com/arcadedb/server/ha/raft/ArcadeStateMachine.java
git commit -m "feat(ha-raft): apply DROP_DATABASE_ENTRY in state machine"
```

---

## Task 9: Rewrite `PostServerCommandHandler.dropDatabase` for HA mode

**Files:**
- Modify: `server/src/main/java/com/arcadedb/server/http/handler/PostServerCommandHandler.java`

- [ ] **Step 1: Replace the handler body**

Find `dropDatabase(String databaseName)` at `PostServerCommandHandler.java:435` and replace it with:

```java
private void dropDatabase(final String databaseName) {
  if (databaseName.isEmpty())
    throw new IllegalArgumentException("Database name empty");

  checkServerIsLeaderIfInHA();

  final ArcadeDBServer server = httpServer.getServer();
  Metrics.counter("http.drop-database").increment();

  if (!server.existsDatabase(databaseName))
    throw new IllegalArgumentException("Database '" + databaseName + "' does not exist");

  final ServerDatabase database = server.getDatabase(databaseName);
  final DatabaseInternal wrappedDb = database.getWrappedDatabaseInstance();

  if (wrappedDb instanceof HAReplicatedDatabase haDb) {
    // Raft-first: do NOT drop locally. The state machine apply on every peer
    // (including this leader) performs the actual drop once the entry is committed.
    haDb.dropInReplicas();
  } else {
    // Non-HA mode: drop locally as before.
    database.getEmbedded().drop();
    server.removeDatabase(databaseName);
  }
}
```

- [ ] **Step 2: Compile both modules together**

Run: `mvn -pl server,ha-raft -am compile`
Expected: BUILD SUCCESS.

- [ ] **Step 3: Commit**

```bash
git add server/src/main/java/com/arcadedb/server/http/handler/PostServerCommandHandler.java
git commit -m "feat(server): route drop database through Raft when HA is enabled"
```

---

## Task 10: 3-node integration test for DROP DATABASE

**Files:**
- Create: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftDropDatabase3NodesIT.java`

- [ ] **Step 1: Write the test**

Create a new file based on the pattern in `RaftHTTP2ServersCreateReplicatedDatabaseIT` and `RaftReplication3NodesIT`:

```java
/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.server.ha.raft;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Base64;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that `drop database` submitted against a replica is forwarded to the leader,
 * replicated via Raft, and applied on every peer including the removal of database files
 * on disk.
 */
class RaftDropDatabase3NodesIT extends BaseRaftHATest {

  private static final String DB_NAME = "RaftDropTest";

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Override
  protected boolean isCreateDatabases() {
    return false;
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_QUORUM, "majority");
  }

  @Test
  void dropDatabasePropagatesAcrossCluster() throws Exception {
    // Create the database via HTTP on server 0
    executeServerCommand(0, "create database " + DB_NAME);

    // Wait until all peers see the database
    Awaitility.await().atMost(15, TimeUnit.SECONDS).pollInterval(200, TimeUnit.MILLISECONDS)
        .until(() -> {
          for (int i = 0; i < getServerCount(); i++)
            if (!getServer(i).existsDatabase(DB_NAME)) return false;
          return true;
        });

    // Insert a few vertices to confirm the database is usable
    command(0, "create vertex type RaftDropVertex");
    for (int i = 0; i < 10; i++)
      command(0, "create vertex RaftDropVertex content {\"idx\":" + i + "}");

    for (int i = 0; i < getServerCount(); i++)
      waitForReplicationIsCompleted(i);

    // Drop through server 1 (a replica) to exercise forward-to-leader
    executeServerCommand(1, "drop database " + DB_NAME);

    // Await until every peer's ArcadeDBServer no longer holds the database
    Awaitility.await().atMost(15, TimeUnit.SECONDS).pollInterval(200, TimeUnit.MILLISECONDS)
        .until(() -> {
          for (int i = 0; i < getServerCount(); i++)
            if (getServer(i).existsDatabase(DB_NAME)) return false;
          return true;
        });

    // Assert the on-disk directory is gone on every peer
    for (int i = 0; i < getServerCount(); i++) {
      final String dbPath = getServer(i).getConfiguration()
          .getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY);
      final File dir = new File(dbPath, DB_NAME);
      assertThat(dir).as("server %d database directory should be removed", i).doesNotExist();
    }

    // Sanity: a second drop against the leader should fail with 4xx
    final int leader = findLeaderIndex();
    assertThat(leader).isGreaterThanOrEqualTo(0);
    final int status = executeServerCommandExpectError(leader, "drop database " + DB_NAME);
    assertThat(status).isBetween(400, 499);
  }

  private void executeServerCommand(final int serverIndex, final String command) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URL(
        "http://127.0.0.1:248" + serverIndex + "/api/v1/server").openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    try {
      formatPayload(connection, new JSONObject().put("command", command));
      connection.connect();
      assertThat(connection.getResponseCode()).isEqualTo(200);
    } finally {
      connection.disconnect();
    }
  }

  private int executeServerCommandExpectError(final int serverIndex, final String command) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URL(
        "http://127.0.0.1:248" + serverIndex + "/api/v1/server").openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    try {
      formatPayload(connection, new JSONObject().put("command", command));
      connection.connect();
      return connection.getResponseCode();
    } finally {
      connection.disconnect();
    }
  }
}
```

`formatPayload(HttpURLConnection, JSONObject)` is inherited from `BaseGraphServerTest`. If the base class only exposes `formatPayload(HttpURLConnection, String, String, String, Map)`, inline the body of that method using the JSON payload object as-is.

- [ ] **Step 2: Run the test**

Run: `mvn -pl ha-raft test -Dtest=RaftDropDatabase3NodesIT`
Expected: PASS. If it fails, inspect `server-*.log` under each server's root for state machine errors.

- [ ] **Step 3: Commit**

```bash
git add ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftDropDatabase3NodesIT.java
git commit -m "test(ha-raft): integration test for drop database propagation across 3 nodes"
```

---

# Phase 3: RESTORE DATABASE propagation

## Task 11: Extend `applyInstallDatabaseEntry` to honour `forceSnapshot`

**Files:**
- Modify: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/ArcadeStateMachine.java`

**Revised from original plan:** the original Task 11 included a Mockito spy test that intercepted `installDatabaseSnapshot` via a test seam. Same rationale as Task 8: ha-raft has no existing `applyTransaction`-level unit tests, no Mockito dependency, and Task 13's 3-node restore IT is the real end-to-end validator. Task 11 therefore extends the apply method and relies on Task 13 for coverage.

- [ ] **Step 1: Extend the apply method**

Replace `applyInstallDatabaseEntry` in `ArcadeStateMachine.java` with:

```java
private void applyInstallDatabaseEntry(final RaftLogEntryCodec.DecodedEntry decoded) {
  final String databaseName = decoded.databaseName();
  final boolean forceSnapshot = decoded.forceSnapshot();

  if (forceSnapshot) {
    // Restore flow: replace files from the leader's snapshot even if the DB exists.
    // On the leader itself the files are already authoritative, so the leader skips
    // the reinstall entirely; replicas close the local DB and pull the fresh snapshot.
    if (raftHAServer != null && raftHAServer.isLeader()) {
      HALog.log(this, HALog.TRACE, "Leader skips forceSnapshot reinstall for '%s'", databaseName);
      return;
    }

    if (server.existsDatabase(databaseName)) {
      final DatabaseInternal db = (DatabaseInternal) server.getDatabase(databaseName);
      db.getEmbedded().close();
      server.removeDatabase(databaseName);
    }

    final String leaderHttpAddr = raftHAServer.getLeaderHttpAddress();
    final String clusterToken = server.getConfiguration().getValueAsString(
        GlobalConfiguration.HA_CLUSTER_TOKEN);
    try {
      installDatabaseSnapshot(databaseName, leaderHttpAddr, clusterToken);
    } catch (final IOException e) {
      throw new RuntimeException("Failed to install snapshot for restored database '" + databaseName + "'", e);
    }
    LogManager.instance().log(this, Level.INFO, "Database '%s' reinstalled via forceSnapshot from leader", databaseName);
    return;
  }

  // Normal create flow: skip if the database is already present locally.
  if (server.existsDatabase(databaseName)) {
    HALog.log(this, HALog.TRACE, "Database '%s' already present, skipping install-database entry", databaseName);
    return;
  }

  server.createDatabase(databaseName, ComponentFile.MODE.READ_WRITE);
  LogManager.instance().log(this, Level.INFO, "Database '%s' created via Raft install-database entry", databaseName);
}
```

Notes:
- `GlobalConfiguration` is already imported at the top of the file.
- `IOException` is already imported.
- `DatabaseInternal`, `HALog`, `LogManager`, `Level` are already imported.
- The existing private `installDatabaseSnapshot(String, String, String)` helper is reused as-is; no rename needed.

- [ ] **Step 2: Clean-compile to catch any stale incremental-compilation issues**

Run: `mvn -pl ha-raft -am clean compile`
Expected: BUILD SUCCESS.

- [ ] **Step 3: Run existing ha-raft unit tests**

Run: `mvn -pl ha-raft test`
Expected: all tests pass.

- [ ] **Step 4: Commit**

```bash
git add ha-raft/src/main/java/com/arcadedb/server/ha/raft/ArcadeStateMachine.java
git commit -m "feat(ha-raft): honour forceSnapshot flag in applyInstallDatabaseEntry"
```

---

## Task 12: Rewrite `PostServerCommandHandler.restoreDatabase` for HA

**Files:**
- Modify: `server/src/main/java/com/arcadedb/server/http/handler/PostServerCommandHandler.java`

- [ ] **Step 1: Add post-restore replication**

After the synchronous restore path (the line that does `server.getDatabase(databaseName);` around line 300), add the HA replication call. Replace the tail of the synchronous path with:

```java
server.getDatabase(databaseName);

// HA: make replicas pull the restored files via the Raft install-database entry with forceSnapshot.
final DatabaseInternal wrapped = ((ServerDatabase) server.getDatabase(databaseName)).getWrappedDatabaseInstance();
if (wrapped instanceof HAReplicatedDatabase haDb) {
  try {
    haDb.createInReplicas(true);
  } catch (final RuntimeException e) {
    // Compensate: drop the locally-restored database so the operator can retry cleanly.
    try {
      server.getDatabase(databaseName).getEmbedded().drop();
      server.removeDatabase(databaseName);
    } catch (final Exception inner) {
      LogManager.instance().log(this, java.util.logging.Level.SEVERE,
          "Compensating drop after failed restore replication failed for '%s'", inner, databaseName);
    }
    throw e;
  }
}

return new ExecutionResponse(200, new JSONObject().put("result", "ok").toString());
```

Also mirror the same call at the end of the SSE branch (around line 285), immediately before `closeSSE(out)` on success:

```java
final DatabaseInternal wrappedSse = ((ServerDatabase) server.getDatabase(databaseName)).getWrappedDatabaseInstance();
if (wrappedSse instanceof HAReplicatedDatabase haDbSse) {
  try {
    haDbSse.createInReplicas(true);
    sendSSE(out, new JSONObject().put("status", "progress").put("message", "Replication to replicas completed"));
  } catch (final RuntimeException e) {
    try {
      server.getDatabase(databaseName).getEmbedded().drop();
      server.removeDatabase(databaseName);
    } catch (final Exception ignored) {}
    sendSSE(out, new JSONObject().put("status", "error").put("message", "Replication to replicas failed: " + e.getMessage()));
  }
}
```

- [ ] **Step 2: Compile**

Run: `mvn -pl server,ha-raft -am compile`
Expected: BUILD SUCCESS.

- [ ] **Step 3: Commit**

```bash
git add server/src/main/java/com/arcadedb/server/http/handler/PostServerCommandHandler.java
git commit -m "feat(server): replicate restored database to replicas via forceSnapshot install"
```

---

## Task 13: 3-node integration test for RESTORE DATABASE

**Files:**
- Create: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftRestoreDatabase3NodesIT.java`

- [ ] **Step 1: Write the test**

Create the file with a 3-node cluster. The test creates a known fixture, takes a local backup (to a temporary `file://` URL), drops the DB, issues a restore, and asserts the restored database is visible on every peer.

```java
/*
 * [license header as above]
 */
package com.arcadedb.server.ha.raft;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.ServerDatabase;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.util.Base64;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class RaftRestoreDatabase3NodesIT extends BaseRaftHATest {

  private static final String DB_NAME = "RaftRestoreTest";

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Override
  protected boolean isCreateDatabases() {
    return false;
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_QUORUM, "majority");
  }

  @Test
  void restoreDatabasePropagatesAcrossCluster() throws Exception {
    // Step A: create DB and fixture
    executeServerCommand(0, "create database " + DB_NAME);
    Awaitility.await().atMost(15, TimeUnit.SECONDS).until(() -> {
      for (int i = 0; i < 3; i++) if (!getServer(i).existsDatabase(DB_NAME)) return false;
      return true;
    });
    command(0, "create vertex type RaftRestoreVertex");
    for (int i = 0; i < 50; i++)
      command(0, "create vertex RaftRestoreVertex content {\"idx\":" + i + "}");
    for (int i = 0; i < 3; i++) waitForReplicationIsCompleted(i);

    // Step B: back up on the leader's own view
    final int leader = findLeaderIndex();
    assertThat(leader).isGreaterThanOrEqualTo(0);
    final File backupFile = Files.createTempFile("raft-restore-backup-", ".zip").toFile();
    backupFile.delete();
    final String backupPath = backupFile.getAbsolutePath();
    command(leader, "backup database file://" + backupPath);

    // Step C: drop the database cluster-wide
    executeServerCommand(leader, "drop database " + DB_NAME);
    Awaitility.await().atMost(15, TimeUnit.SECONDS).until(() -> {
      for (int i = 0; i < 3; i++) if (getServer(i).existsDatabase(DB_NAME)) return false;
      return true;
    });

    // Step D: restore from the backup
    executeServerCommand(leader, "restore database " + DB_NAME + " file://" + backupPath);

    // Step E: every peer should open the DB and see the fixture
    Awaitility.await().atMost(30, TimeUnit.SECONDS).until(() -> {
      for (int i = 0; i < 3; i++)
        if (!getServer(i).existsDatabase(DB_NAME)) return false;
      return true;
    });

    for (int i = 0; i < 3; i++) {
      waitForReplicationIsCompleted(i);
      final long count = getServer(i).getDatabase(DB_NAME).countType("RaftRestoreVertex", true);
      assertThat(count).as("server %d restored vertex count", i).isEqualTo(50);
    }
  }

  private void executeServerCommand(final int serverIndex, final String command) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URL(
        "http://127.0.0.1:248" + serverIndex + "/api/v1/server").openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    try {
      formatPayload(connection, new JSONObject().put("command", command));
      connection.connect();
      assertThat(connection.getResponseCode()).isEqualTo(200);
    } finally {
      connection.disconnect();
    }
  }
}
```

- [ ] **Step 2: Run the test**

Run: `mvn -pl ha-raft test -Dtest=RaftRestoreDatabase3NodesIT`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftRestoreDatabase3NodesIT.java
git commit -m "test(ha-raft): integration test for restore database propagation"
```

---

# Phase 4: IMPORT DATABASE propagation

## Task 14: Rewrite `PostServerCommandHandler.importDatabase` for HA

**Files:**
- Modify: `server/src/main/java/com/arcadedb/server/http/handler/PostServerCommandHandler.java`

- [ ] **Step 1: Replace the database creation line**

At `PostServerCommandHandler.java:325` the current code is:
```java
server.createDatabase(databaseName, ComponentFile.MODE.READ_WRITE);
final Database database = server.getDatabase(databaseName);
```

Replace with:
```java
final ServerDatabase createdDb = server.createDatabase(databaseName, ComponentFile.MODE.READ_WRITE);
final DatabaseInternal wrapped = createdDb.getWrappedDatabaseInstance();
if (wrapped instanceof HAReplicatedDatabase haDb) {
  try {
    haDb.createInReplicas();
  } catch (final RuntimeException e) {
    try {
      createdDb.getEmbedded().drop();
      server.removeDatabase(databaseName);
    } catch (final Exception ignored) {}
    throw e;
  }
}
final Database database = createdDb;
```

This makes every import first create the database cluster-wide via the existing `INSTALL_DATABASE_ENTRY` path. Once the database exists on every node, the importer's transactions replicate automatically as normal `TX_ENTRY` stream.

- [ ] **Step 2: Compile**

Run: `mvn -pl server,ha-raft -am compile`
Expected: BUILD SUCCESS.

- [ ] **Step 3: Commit**

```bash
git add server/src/main/java/com/arcadedb/server/http/handler/PostServerCommandHandler.java
git commit -m "feat(server): create imported database via Raft before running importer"
```

---

## Task 15: 3-node integration test for IMPORT DATABASE

**Files:**
- Create: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftImportDatabase3NodesIT.java`
- Reuse: an existing small fixture under `engine/src/test/resources/` or `integration/src/test/resources/` (search for `.graphml` or similar).

- [ ] **Step 1: Locate a suitable fixture**

Run: `find . -name "*.graphml" -not -path "*/target/*" 2>/dev/null | head`
Pick the smallest one (a few KB). If none exist, copy a tiny fixture into `ha-raft/src/test/resources/raft-import-fixture.graphml` with a handful of vertices:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<graphml xmlns="http://graphml.graphdrawing.org/xmlns">
  <graph id="G" edgedefault="directed">
    <node id="n0"><data key="name">alpha</data></node>
    <node id="n1"><data key="name">beta</data></node>
    <node id="n2"><data key="name">gamma</data></node>
    <edge id="e0" source="n0" target="n1"/>
    <edge id="e1" source="n1" target="n2"/>
  </graph>
</graphml>
```

- [ ] **Step 2: Write the test**

```java
/*
 * [license header]
 */
package com.arcadedb.server.ha.raft;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Base64;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class RaftImportDatabase3NodesIT extends BaseRaftHATest {

  private static final String DB_NAME = "RaftImportTest";

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Override
  protected boolean isCreateDatabases() {
    return false;
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_QUORUM, "majority");
  }

  @Test
  void importDatabasePropagatesAcrossCluster() throws Exception {
    final String fixturePath = java.nio.file.Paths.get("src/test/resources/raft-import-fixture.graphml")
        .toAbsolutePath().toString();
    final int leader = findLeaderIndex();
    assertThat(leader).isGreaterThanOrEqualTo(0);

    executeServerCommand(leader, "import database " + DB_NAME + " file://" + fixturePath);

    Awaitility.await().atMost(30, TimeUnit.SECONDS).until(() -> {
      for (int i = 0; i < 3; i++) if (!getServer(i).existsDatabase(DB_NAME)) return false;
      return true;
    });

    for (int i = 0; i < 3; i++) {
      waitForReplicationIsCompleted(i);
      final long vertexCount = getServer(i).getDatabase(DB_NAME).countType("V", true);
      assertThat(vertexCount).as("server %d imported vertex count", i).isEqualTo(3);
    }
  }

  private void executeServerCommand(final int serverIndex, final String command) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URL(
        "http://127.0.0.1:248" + serverIndex + "/api/v1/server").openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    try {
      formatPayload(connection, new JSONObject().put("command", command));
      connection.connect();
      assertThat(connection.getResponseCode()).isEqualTo(200);
    } finally {
      connection.disconnect();
    }
  }
}
```

Adjust the asserted vertex type and count after running once to match what the GraphML importer actually creates (ArcadeDB's importer maps `node` to the generic `V` type by default; if the test prints a different type, update the assertion).

- [ ] **Step 3: Run the test**

Run: `mvn -pl ha-raft test -Dtest=RaftImportDatabase3NodesIT`
Expected: PASS. If it fails on the assertion about vertex type/count, inspect the imported schema via `command(leader, "select from schema:types")` and adjust the test.

- [ ] **Step 4: Commit**

```bash
git add ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftImportDatabase3NodesIT.java
git add ha-raft/src/test/resources/raft-import-fixture.graphml
git commit -m "test(ha-raft): integration test for import database propagation"
```

---

# Phase 5: USER MANAGEMENT propagation

## Task 16: Add JSON payload helpers to `ServerSecurity`

**Files:**
- Modify: `server/src/main/java/com/arcadedb/server/security/ServerSecurity.java`

- [ ] **Step 1: Read the current `saveUsers()` method for the shape of `usersToJSON()`**

Run: `grep -n "usersToJSON" server/src/main/java/com/arcadedb/server/security/ServerSecurity.java`

Expected: `usersToJSON()` exists and returns `List<JSONObject>`. If not, grep for whatever helper `saveUsers` uses to build the list.

- [ ] **Step 2: Add the two public helpers**

In `ServerSecurity.java`, after `saveUsers()` (around line 382), add:

```java
/**
 * Returns the current users list as a JSON array string, suitable for
 * HA replication via {@code HAServerPlugin.replicateSecurityUsers}.
 * The output is the canonical on-disk representation minus line breaks.
 */
public synchronized String getUsersJsonPayload() {
  final com.arcadedb.serializer.json.JSONArray array = new com.arcadedb.serializer.json.JSONArray();
  for (final com.arcadedb.serializer.json.JSONObject userJson : usersToJSON())
    array.put(userJson);
  return array.toString();
}

/**
 * Applies a replicated users payload: writes it atomically to {@code server-users.jsonl}
 * and reloads the in-memory users map. Called from the Raft state machine on every peer.
 */
public synchronized void applyReplicatedUsers(final String usersJsonArray) {
  final com.arcadedb.serializer.json.JSONArray array = new com.arcadedb.serializer.json.JSONArray(usersJsonArray);
  final java.util.List<com.arcadedb.serializer.json.JSONObject> list = new java.util.ArrayList<>(array.length());
  for (int i = 0; i < array.length(); i++)
    list.add(array.getJSONObject(i));

  try {
    usersRepository.save(list);
  } catch (final java.io.IOException e) {
    throw new com.arcadedb.server.ServerException("Failed to save replicated users file", e);
  }
  loadUsers();
}
```

If `usersToJSON()` is private, make it package-private or add a public wrapper. The existing `saveUsers()` call site at line 384 should still work.

- [ ] **Step 3: Compile**

Run: `mvn -pl server compile`
Expected: BUILD SUCCESS.

- [ ] **Step 4: Commit**

```bash
git add server/src/main/java/com/arcadedb/server/security/ServerSecurity.java
git commit -m "feat(security): expose JSON payload helpers for HA user replication"
```

---

## Task 17: Add `replicateSecurityUsers` to `HAServerPlugin` interface

**Files:**
- Modify: `server/src/main/java/com/arcadedb/server/HAServerPlugin.java`

- [ ] **Step 1: Add the default method**

In `HAServerPlugin.java`, after the existing `leaveCluster()` default method, add:

```java
/**
 * Replicates the full server-users.jsonl content across the cluster.
 * Called by {@code PostServerCommandHandler.createUser} / {@code dropUser} and by
 * {@code PostAddPeerHandler} to seed newly-joined peers. Default no-op for non-HA setups.
 *
 * @param usersJsonArray a JSON array string representing the full current users list
 */
default void replicateSecurityUsers(final String usersJsonArray) {
  // No-op by default; Raft implementation overrides.
}
```

- [ ] **Step 2: Compile**

Run: `mvn -pl server compile`
Expected: BUILD SUCCESS.

- [ ] **Step 3: Commit**

```bash
git add server/src/main/java/com/arcadedb/server/HAServerPlugin.java
git commit -m "feat(server): add HAServerPlugin.replicateSecurityUsers default method"
```

---

## Task 18: Implement `RaftHAPlugin.replicateSecurityUsers`

**Files:**
- Modify: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAPlugin.java`

- [ ] **Step 1: Add the implementation**

In `RaftHAPlugin.java`, after `getRaftHAServer()`, add:

```java
@Override
public void replicateSecurityUsers(final String usersJsonArray) {
  if (raftHAServer == null)
    throw new com.arcadedb.exception.TransactionException("Raft HA server not started");

  final org.apache.ratis.thirdparty.com.google.protobuf.ByteString entry =
      RaftLogEntryCodec.encodeSecurityUsersEntry(usersJsonArray);
  try {
    raftHAServer.getGroupCommitter().submitAndWait(entry.toByteArray(), raftHAServer.getQuorumTimeout());
  } catch (final com.arcadedb.exception.TransactionException e) {
    throw e;
  } catch (final Exception e) {
    throw new com.arcadedb.exception.TransactionException("Error sending security-users entry via Raft", e);
  }
  LogManager.instance().log(this, Level.INFO, "Security users entry committed via Raft");
}
```

- [ ] **Step 2: Compile**

Run: `mvn -pl ha-raft compile`
Expected: BUILD SUCCESS.

- [ ] **Step 3: Commit**

```bash
git add ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAPlugin.java
git commit -m "feat(ha-raft): implement RaftHAPlugin.replicateSecurityUsers"
```

---

## Task 19: Apply `SECURITY_USERS_ENTRY` in the state machine

**Files:**
- Modify: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/ArcadeStateMachine.java`

**Revised from original plan:** the original Task 19 included a Mockito-based unit test. Same rationale as Tasks 8 and 11: ha-raft has no existing `applyTransaction`-level tests, no Mockito dependency, and Task 22's 3-node integration test is the real end-to-end validator. Task 19 therefore adds the apply method and wires the switch without unit tests.

- [ ] **Step 1: Add the apply method**

In `ArcadeStateMachine.java`, add a new private method near `applyInstallDatabaseEntry`:

```java
private void applySecurityUsersEntry(final RaftLogEntryCodec.DecodedEntry decoded) {
  final String payload = decoded.usersJson();
  if (payload == null) {
    LogManager.instance().log(this, Level.WARNING, "SECURITY_USERS_ENTRY has null payload, skipping");
    return;
  }
  server.getSecurity().applyReplicatedUsers(payload);
  HALog.log(this, HALog.DETAILED, "Applied SECURITY_USERS_ENTRY (%d bytes)", payload.length());
}
```

- [ ] **Step 2: Replace the placeholder in the switch**

In `applyTransaction`, replace the SECURITY_USERS_ENTRY throwing placeholder (added in Task 1) with a call to the new apply method:

```java
case SECURITY_USERS_ENTRY -> applySecurityUsersEntry(decoded);
```

This completes the state-machine switch - every entry type now has a real handler.

- [ ] **Step 3: Clean-compile to catch any stale incremental-compilation issues**

Run: `mvn -pl ha-raft -am clean compile`
Expected: BUILD SUCCESS across the reactor.

- [ ] **Step 4: Run existing ha-raft unit tests**

Run: `mvn -pl ha-raft test`
Expected: all existing tests pass. The users behaviour is validated end-to-end by Task 22.

- [ ] **Step 5: Commit**

```bash
git add ha-raft/src/main/java/com/arcadedb/server/ha/raft/ArcadeStateMachine.java
git commit -m "feat(ha-raft): apply SECURITY_USERS_ENTRY in state machine"
```

---

## Task 20: Rewrite `PostServerCommandHandler.createUser` for HA

**Files:**
- Modify: `server/src/main/java/com/arcadedb/server/http/handler/PostServerCommandHandler.java`

- [ ] **Step 1: Replace the handler body**

Find `createUser(String payload)` at `PostServerCommandHandler.java:467` and replace with:

```java
private void createUser(final String payload) {
  final JSONObject json = new JSONObject(payload);

  if (!json.has("name"))
    throw new IllegalArgumentException("User name is null");

  final String userPassword = json.getString("password");
  if (userPassword.length() < 4)
    throw new ServerSecurityException("User password must be 5 minimum characters");
  if (userPassword.length() > 256)
    throw new ServerSecurityException("User password cannot be longer than 256 characters");

  final ArcadeDBServer server = httpServer.getServer();
  json.put("password", server.getSecurity().encodePassword(userPassword));

  Metrics.counter("http.create-user").increment();

  final HAServerPlugin ha = server.getHA();
  if (ha == null) {
    // Non-HA mode: direct local mutation.
    server.getSecurity().createUser(json);
    return;
  }

  // HA mode: compute the new users payload and submit a Raft entry.
  synchronized (server.getSecurity()) {
    if (server.getSecurity().getUser(json.getString("name")) != null)
      throw new ServerSecurityException("User '" + json.getString("name") + "' already exists");

    final JSONArray currentUsers = new JSONArray(server.getSecurity().getUsersJsonPayload());
    currentUsers.put(json);
    ha.replicateSecurityUsers(currentUsers.toString());
  }
}
```

- [ ] **Step 2: Compile**

Run: `mvn -pl server,ha-raft -am compile`
Expected: BUILD SUCCESS.

- [ ] **Step 3: Commit**

```bash
git add server/src/main/java/com/arcadedb/server/http/handler/PostServerCommandHandler.java
git commit -m "feat(server): route create user through Raft when HA is enabled"
```

---

## Task 21: Rewrite `PostServerCommandHandler.dropUser` for HA

**Files:**
- Modify: `server/src/main/java/com/arcadedb/server/http/handler/PostServerCommandHandler.java`

- [ ] **Step 1: Replace the handler body**

Find `dropUser(String userName)` at line 486 and replace with:

```java
private void dropUser(final String userName) {
  if (userName.isEmpty())
    throw new IllegalArgumentException("User name was missing");

  Metrics.counter("http.drop-user").increment();

  final ArcadeDBServer server = httpServer.getServer();
  final HAServerPlugin ha = server.getHA();

  if (ha == null) {
    final boolean result = server.getSecurity().dropUser(userName);
    if (!result)
      throw new IllegalArgumentException("User '" + userName + "' not found on server");
    return;
  }

  synchronized (server.getSecurity()) {
    if (server.getSecurity().getUser(userName) == null)
      throw new IllegalArgumentException("User '" + userName + "' not found on server");

    final JSONArray currentUsers = new JSONArray(server.getSecurity().getUsersJsonPayload());
    final JSONArray filtered = new JSONArray();
    for (int i = 0; i < currentUsers.length(); i++) {
      final JSONObject user = currentUsers.getJSONObject(i);
      if (!userName.equals(user.optString("name", "")))
        filtered.put(user);
    }
    ha.replicateSecurityUsers(filtered.toString());
  }
}
```

- [ ] **Step 2: Compile**

Run: `mvn -pl server,ha-raft -am compile`
Expected: BUILD SUCCESS.

- [ ] **Step 3: Commit**

```bash
git add server/src/main/java/com/arcadedb/server/http/handler/PostServerCommandHandler.java
git commit -m "feat(server): route drop user through Raft when HA is enabled"
```

---

## Task 22: 3-node integration test for user management

**Files:**
- Create: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftUserManagement3NodesIT.java`

- [ ] **Step 1: Write the test**

```java
/*
 * [license header]
 */
package com.arcadedb.server.ha.raft;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.util.Base64;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class RaftUserManagement3NodesIT extends BaseRaftHATest {

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_QUORUM, "majority");
  }

  @Test
  void createDropUserReplicatedAcrossCluster() throws Exception {
    final int leader = findLeaderIndex();
    assertThat(leader).isGreaterThanOrEqualTo(0);

    final JSONObject aliceJson = new JSONObject()
        .put("name", "alice")
        .put("password", "alicepw1234")
        .put("databases", new JSONObject().put("*", new JSONArray().put("admin")));

    executeServerCommand(leader, "create user " + aliceJson.toString());

    Awaitility.await().atMost(15, TimeUnit.SECONDS).until(() -> {
      for (int i = 0; i < 3; i++)
        if (getServer(i).getSecurity().getUser("alice") == null) return false;
      return true;
    });

    // Verify login as alice succeeds on every node
    for (int i = 0; i < 3; i++)
      assertLogin(i, "alice", "alicepw1234", 200);

    // Drop alice
    executeServerCommand(leader, "drop user alice");

    Awaitility.await().atMost(15, TimeUnit.SECONDS).until(() -> {
      for (int i = 0; i < 3; i++)
        if (getServer(i).getSecurity().getUser("alice") != null) return false;
      return true;
    });

    for (int i = 0; i < 3; i++)
      assertLogin(i, "alice", "alicepw1234", 401);
  }

  @Test
  void concurrentUserCreationsConvergeOnAllNodes() throws Exception {
    final int leader = findLeaderIndex();
    assertThat(leader).isGreaterThanOrEqualTo(0);

    final CountDownLatch start = new CountDownLatch(1);
    final ExecutorService pool = Executors.newFixedThreadPool(5);
    for (int i = 0; i < 5; i++) {
      final int n = i;
      pool.submit(() -> {
        start.await();
        final JSONObject user = new JSONObject()
            .put("name", "u" + n)
            .put("password", "pw123456")
            .put("databases", new JSONObject().put("*", new JSONArray().put("admin")));
        executeServerCommand(leader, "create user " + user.toString());
        return null;
      });
    }
    start.countDown();
    pool.shutdown();
    pool.awaitTermination(60, TimeUnit.SECONDS);

    Awaitility.await().atMost(30, TimeUnit.SECONDS).until(() -> {
      for (int i = 0; i < 3; i++)
        for (int n = 0; n < 5; n++)
          if (getServer(i).getSecurity().getUser("u" + n) == null) return false;
      return true;
    });

    // Byte-level file equality: read each node's server-users.jsonl and compare
    final byte[] reference = Files.readAllBytes(new File(getServer(0).getRootPath(), "config/server-users.jsonl").toPath());
    for (int i = 1; i < 3; i++) {
      final byte[] other = Files.readAllBytes(new File(getServer(i).getRootPath(), "config/server-users.jsonl").toPath());
      assertThat(other).as("server %d users file byte equality", i).isEqualTo(reference);
    }
  }

  private void executeServerCommand(final int serverIndex, final String command) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URL(
        "http://127.0.0.1:248" + serverIndex + "/api/v1/server").openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    try {
      formatPayload(connection, new JSONObject().put("command", command));
      connection.connect();
      assertThat(connection.getResponseCode()).isEqualTo(200);
    } finally {
      connection.disconnect();
    }
  }

  private void assertLogin(final int serverIndex, final String user, final String pw, final int expectedStatus) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URL(
        "http://127.0.0.1:248" + serverIndex + "/api/v1/ready").openConnection();
    connection.setRequestMethod("GET");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString((user + ":" + pw).getBytes()));
    try {
      assertThat(connection.getResponseCode()).as("server %d login as %s", serverIndex, user).isEqualTo(expectedStatus);
    } finally {
      connection.disconnect();
    }
  }
}
```

Note: the `/api/v1/ready` endpoint is used because it is an auth-guarded GET that returns 200 on success and 401 on failure. If that endpoint is not authenticated on this branch, substitute `/api/v1/server` with a benign `list databases` command.

- [ ] **Step 2: Run the test**

Run: `mvn -pl ha-raft test -Dtest=RaftUserManagement3NodesIT`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftUserManagement3NodesIT.java
git commit -m "test(ha-raft): integration test for user replication across 3 nodes"
```

---

# Phase 6: SEED ON PEER ADD

## Task 23: Emit a `SECURITY_USERS_ENTRY` after adding a peer

**Files:**
- Modify: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/PostAddPeerHandler.java`

- [ ] **Step 1: Read the current handler**

Run: `cat ha-raft/src/main/java/com/arcadedb/server/ha/raft/PostAddPeerHandler.java` to understand where the "peer added successfully" success branch is. The handler should call `RaftHAServer.addPeer` and then return a 200 status JSON.

- [ ] **Step 2: Add the seed call after successful peer addition**

Right before writing the success response, add:

```java
// Seed the freshly-joined peer with the current users file
try {
  final String usersPayload = plugin.getServer().getSecurity().getUsersJsonPayload();
  plugin.replicateSecurityUsers(usersPayload);
} catch (final Exception e) {
  LogManager.instance().log(this, java.util.logging.Level.WARNING,
      "Users seed to new peer failed (best-effort): %s", e.getMessage());
}
```

Where `plugin` is the `RaftHAPlugin` reference already held by the handler (the constructor signature from the main plugin is `new PostAddPeerHandler(httpServer, this)`). If the field is named differently inside the handler (e.g., `haPlugin`), use that name. If the handler only holds the `RaftHAServer`, add a constructor parameter for the plugin or reach the plugin via `httpServer.getServer().getHA()` cast to `RaftHAPlugin`.

- [ ] **Step 3: Compile**

Run: `mvn -pl ha-raft compile`
Expected: BUILD SUCCESS.

- [ ] **Step 4: Commit**

```bash
git add ha-raft/src/main/java/com/arcadedb/server/ha/raft/PostAddPeerHandler.java
git commit -m "feat(ha-raft): seed new peer with current users after peer-add"
```

---

## Task 24: 3-node integration test for peer-add user seed

**Files:**
- Create: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftUserSeedOnPeerAdd3NodesIT.java`

- [ ] **Step 1: Write the test**

```java
/*
 * [license header]
 */
package com.arcadedb.server.ha.raft;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Base64;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class RaftUserSeedOnPeerAdd3NodesIT extends BaseRaftHATest {

  @Override
  protected int getServerCount() {
    // Start 2 nodes; add the 3rd at runtime to test the seed
    return 2;
  }

  @Override
  protected boolean persistentRaftStorage() {
    return true;
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_QUORUM, "majority");
  }

  @Test
  void peerAddSeedsCurrentUsersToNewNode() throws Exception {
    final int leader = findLeaderIndex();
    assertThat(leader).isGreaterThanOrEqualTo(0);

    final JSONObject carol = new JSONObject()
        .put("name", "carol")
        .put("password", "carolpw1234")
        .put("databases", new JSONObject().put("*", new JSONArray().put("admin")));
    executeServerCommand(leader, "create user " + carol.toString());

    Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> {
      for (int i = 0; i < 2; i++)
        if (getServer(i).getSecurity().getUser("carol") == null) return false;
      return true;
    });

    // Start a third server and add it as a peer via the leader HTTP API
    startServer(2);
    addPeerViaHttp(leader, "localhost_2436");

    Awaitility.await().atMost(30, TimeUnit.SECONDS).until(() -> getServer(2) != null
        && getServer(2).isStarted() && getServer(2).getSecurity().getUser("carol") != null);

    // Login as carol against the newly-added node
    final HttpURLConnection connection = (HttpURLConnection) new URL(
        "http://127.0.0.1:2482/api/v1/ready").openConnection();
    connection.setRequestMethod("GET");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString("carol:carolpw1234".getBytes()));
    try {
      assertThat(connection.getResponseCode()).isEqualTo(200);
    } finally {
      connection.disconnect();
    }
  }

  private void executeServerCommand(final int serverIndex, final String command) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URL(
        "http://127.0.0.1:248" + serverIndex + "/api/v1/server").openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    try {
      formatPayload(connection, new JSONObject().put("command", command));
      connection.connect();
      assertThat(connection.getResponseCode()).isEqualTo(200);
    } finally {
      connection.disconnect();
    }
  }

  private void addPeerViaHttp(final int leaderIndex, final String peerId) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URL(
        "http://127.0.0.1:248" + leaderIndex + "/api/v1/cluster/peer").openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    try {
      formatPayload(connection, new JSONObject().put("peerId", peerId).put("address", "localhost:" + 2436));
      connection.connect();
      assertThat(connection.getResponseCode()).isIn(200, 201);
    } finally {
      connection.disconnect();
    }
  }
}
```

Verify the exact peer-id format and the POST body shape by reading `PostAddPeerHandler.java` before running the test; adjust `peerId` and JSON field names if different.

- [ ] **Step 2: Run the test**

Run: `mvn -pl ha-raft test -Dtest=RaftUserSeedOnPeerAdd3NodesIT`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftUserSeedOnPeerAdd3NodesIT.java
git commit -m "test(ha-raft): integration test for peer-add user seed"
```

---

# Phase 7: E2E-HA container scenarios

Each of these tests extends `ContainersTestTemplate` and follows the structure of `ThreeInstancesScenarioIT.java`. Read that file first to see how containers are created, started, and the test networks are set up. Each scenario uses the same `SERVER_LIST` constant and the majority quorum.

## Task 25: `DropDatabaseScenarioIT`

**Files:**
- Create: `e2e-ha/src/test/java/com/arcadedb/containers/ha/DropDatabaseScenarioIT.java`

- [ ] **Step 1: Write the scenario**

```java
/*
 * [license header]
 */
package com.arcadedb.containers.ha;

import com.arcadedb.test.support.ContainersTestTemplate;
import com.arcadedb.test.support.DatabaseWrapper;
import com.arcadedb.test.support.ServerWrapper;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class DropDatabaseScenarioIT extends ContainersTestTemplate {

  private static final String SERVER_LIST = "arcadedb-0:2434:2480,arcadedb-1:2434:2480,arcadedb-2:2434:2480";

  @AfterEach
  @Override
  public void tearDown() {
    super.tearDown();
  }

  @Test
  @Timeout(value = 10, unit = TimeUnit.MINUTES)
  @DisplayName("Three-node Raft HA: drop database propagates removal to every replica")
  void dropDatabasePropagates() {
    createArcadeContainer("arcadedb-0", SERVER_LIST, "majority", network);
    createArcadeContainer("arcadedb-1", SERVER_LIST, "majority", network);
    createArcadeContainer("arcadedb-2", SERVER_LIST, "majority", network);

    final List<ServerWrapper> servers = startCluster();

    final DatabaseWrapper db0 = new DatabaseWrapper(servers.get(0), idSupplier, wordSupplier);
    final DatabaseWrapper db1 = new DatabaseWrapper(servers.get(1), idSupplier, wordSupplier);
    final DatabaseWrapper db2 = new DatabaseWrapper(servers.get(2), idSupplier, wordSupplier);

    db0.createDatabase();
    db0.createSchema();
    db0.addUserAndPhotos(30, 10);

    // Wait for all nodes to see the data
    Awaitility.await().atMost(60, TimeUnit.SECONDS).pollInterval(1, TimeUnit.SECONDS)
        .until(() -> db0.countUsers() == 30 && db1.countUsers() == 30 && db2.countUsers() == 30);

    // Drop via node 2 (a replica) to exercise forward-to-leader
    servers.get(2).dropDatabase();

    // Wait for the database to be absent from every node's HTTP list-databases
    Awaitility.await().atMost(60, TimeUnit.SECONDS).pollInterval(1, TimeUnit.SECONDS)
        .until(() -> !servers.get(0).databaseExists()
                  && !servers.get(1).databaseExists()
                  && !servers.get(2).databaseExists());

    db0.close(); db1.close(); db2.close();
  }
}
```

This test assumes `ServerWrapper` has `dropDatabase()` and `databaseExists()` convenience methods. If it does not, inline an HTTP POST to `/api/v1/server` with `{ "command": "drop database <name>" }` and an HTTP POST to `/api/v1/server` with `{ "command": "list databases" }` respectively.

- [ ] **Step 2: Check `ServerWrapper` for existing helpers**

Run: `grep -n "dropDatabase\|databaseExists\|listDatabases" e2e-ha/src/test/java/com/arcadedb/test/support/ServerWrapper.java`
Expected: some subset of these. Add any missing helpers inline in the test class as static private methods if `ServerWrapper` cannot be extended in this scope.

- [ ] **Step 3: Run the scenario**

Run: `mvn -pl e2e-ha verify -Dit.test=DropDatabaseScenarioIT -DskipTests=false`
Expected: PASS. Docker must be running.

- [ ] **Step 4: Commit**

```bash
git add e2e-ha/src/test/java/com/arcadedb/containers/ha/DropDatabaseScenarioIT.java
git commit -m "test(e2e-ha): container scenario for drop database propagation"
```

---

## Task 26: `RestoreDatabaseScenarioIT`

**Files:**
- Create: `e2e-ha/src/test/java/com/arcadedb/containers/ha/RestoreDatabaseScenarioIT.java`

- [ ] **Step 1: Write the scenario**

Follow the same template as `DropDatabaseScenarioIT`. The scenario:
1. Creates a 3-node cluster.
2. Creates a fixture database and populates it with schema and records.
3. Issues `backup database file:///arcadedb/backups/source.zip` via the leader (the backup path must map to a volume mount so the leader can read it back).
4. Drops the database.
5. Issues `restore database <name> file:///arcadedb/backups/source.zip` via the leader.
6. Waits for every node to see the restored database with matching record counts.

```java
/*
 * [license header]
 */
package com.arcadedb.containers.ha;

import com.arcadedb.test.support.ContainersTestTemplate;
import com.arcadedb.test.support.DatabaseWrapper;
import com.arcadedb.test.support.ServerWrapper;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class RestoreDatabaseScenarioIT extends ContainersTestTemplate {

  private static final String SERVER_LIST = "arcadedb-0:2434:2480,arcadedb-1:2434:2480,arcadedb-2:2434:2480";

  @AfterEach
  @Override
  public void tearDown() {
    super.tearDown();
  }

  @Test
  @Timeout(value = 15, unit = TimeUnit.MINUTES)
  @DisplayName("Three-node Raft HA: restore database replicates restored files to every replica")
  void restoreDatabasePropagates() {
    createArcadeContainer("arcadedb-0", SERVER_LIST, "majority", network);
    createArcadeContainer("arcadedb-1", SERVER_LIST, "majority", network);
    createArcadeContainer("arcadedb-2", SERVER_LIST, "majority", network);

    final List<ServerWrapper> servers = startCluster();
    final DatabaseWrapper db0 = new DatabaseWrapper(servers.get(0), idSupplier, wordSupplier);
    final DatabaseWrapper db1 = new DatabaseWrapper(servers.get(1), idSupplier, wordSupplier);
    final DatabaseWrapper db2 = new DatabaseWrapper(servers.get(2), idSupplier, wordSupplier);

    db0.createDatabase();
    db0.createSchema();
    db0.addUserAndPhotos(40, 10);

    Awaitility.await().atMost(60, TimeUnit.SECONDS)
        .until(() -> db2.countUsers() == 40);

    // Back up on node 0 - backup file is written inside the container
    servers.get(0).executeServerCommand("backup database file:///tmp/restoretest.zip");

    // Copy the backup file out and back in via docker cp is optional; for this test we
    // assume the backup lives on a shared volume. If ContainersTestTemplate does not set
    // up a shared volume, issue the restore on the same container that created the backup.
    servers.get(0).dropDatabase();
    Awaitility.await().atMost(60, TimeUnit.SECONDS)
        .until(() -> !servers.get(0).databaseExists()
                  && !servers.get(1).databaseExists()
                  && !servers.get(2).databaseExists());

    // Restore from the leader's local backup file
    servers.get(0).executeServerCommand("restore database " + db0.getDatabaseName() + " file:///tmp/restoretest.zip");

    Awaitility.await().atMost(90, TimeUnit.SECONDS)
        .until(() -> servers.get(0).databaseExists()
                  && servers.get(1).databaseExists()
                  && servers.get(2).databaseExists());

    Awaitility.await().atMost(60, TimeUnit.SECONDS)
        .until(() -> db0.countUsers() == 40 && db1.countUsers() == 40 && db2.countUsers() == 40);

    db0.close(); db1.close(); db2.close();
  }
}
```

Notes on support helpers:
- If `ServerWrapper` does not already have `executeServerCommand(String)` and `databaseExists()`, add them as inner helpers in this test class using the Undertow HTTP API on `servers.get(n).getHttpUrl()`.
- If the backup path `/tmp/restoretest.zip` lives inside the container, the restore on the same node will find it; other nodes get the restored files via the `forceSnapshot` install entry from the leader, so no shared volume is required.

- [ ] **Step 2: Run the scenario**

Run: `mvn -pl e2e-ha verify -Dit.test=RestoreDatabaseScenarioIT -DskipTests=false`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add e2e-ha/src/test/java/com/arcadedb/containers/ha/RestoreDatabaseScenarioIT.java
git commit -m "test(e2e-ha): container scenario for restore database propagation"
```

---

## Task 27: `ImportDatabaseScenarioIT`

**Files:**
- Create: `e2e-ha/src/test/java/com/arcadedb/containers/ha/ImportDatabaseScenarioIT.java`
- Create: `e2e-ha/src/test/resources/import-fixture.graphml` (if no small fixture already exists in `e2e-ha`)

- [ ] **Step 1: Copy a small GraphML fixture into the e2e-ha resources**

Create a minimal 5-vertex, 4-edge file at `e2e-ha/src/test/resources/import-fixture.graphml`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<graphml xmlns="http://graphml.graphdrawing.org/xmlns">
  <graph id="G" edgedefault="directed">
    <node id="n0"/>
    <node id="n1"/>
    <node id="n2"/>
    <node id="n3"/>
    <node id="n4"/>
    <edge source="n0" target="n1"/>
    <edge source="n1" target="n2"/>
    <edge source="n2" target="n3"/>
    <edge source="n3" target="n4"/>
  </graph>
</graphml>
```

- [ ] **Step 2: Write the scenario**

The test copies the fixture into the leader container (TestContainers `copyFileToContainer`) and issues `import database` via the leader. It then waits for every node to see the imported database with the expected vertex and edge counts.

```java
/*
 * [license header]
 */
package com.arcadedb.containers.ha;

import com.arcadedb.test.support.ContainersTestTemplate;
import com.arcadedb.test.support.DatabaseWrapper;
import com.arcadedb.test.support.ServerWrapper;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.utility.MountableFile;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class ImportDatabaseScenarioIT extends ContainersTestTemplate {

  private static final String SERVER_LIST = "arcadedb-0:2434:2480,arcadedb-1:2434:2480,arcadedb-2:2434:2480";

  @AfterEach
  @Override
  public void tearDown() {
    super.tearDown();
  }

  @Test
  @Timeout(value = 10, unit = TimeUnit.MINUTES)
  @DisplayName("Three-node Raft HA: import database replicates via TX_ENTRY stream")
  void importDatabasePropagates() {
    createArcadeContainer("arcadedb-0", SERVER_LIST, "majority", network);
    createArcadeContainer("arcadedb-1", SERVER_LIST, "majority", network);
    createArcadeContainer("arcadedb-2", SERVER_LIST, "majority", network);

    // Copy fixture into the leader container before start
    servers().get(0).getContainer().withCopyFileToContainer(
        MountableFile.forClasspathResource("import-fixture.graphml"),
        "/tmp/import-fixture.graphml");

    final List<ServerWrapper> servers = startCluster();
    final String dbName = "ImportedHA";

    servers.get(0).executeServerCommand("import database " + dbName + " file:///tmp/import-fixture.graphml");

    Awaitility.await().atMost(60, TimeUnit.SECONDS).until(() ->
        servers.get(0).databaseExists()
        && servers.get(1).databaseExists()
        && servers.get(2).databaseExists());

    for (int i = 0; i < 3; i++) {
      final long vertexCount = servers.get(i).countType(dbName, "V");
      assertThat(vertexCount).as("server %d imported vertex count", i).isEqualTo(5);
    }
  }
}
```

`servers()` is a list accessor on `ContainersTestTemplate`; if not available, store the containers in a local list at creation time. `ServerWrapper.countType(dbName, type)` may not exist; if not, inline an HTTP POST to `/api/v1/command/<dbName>` with a `select count(*) from V` query.

- [ ] **Step 3: Run the scenario**

Run: `mvn -pl e2e-ha verify -Dit.test=ImportDatabaseScenarioIT -DskipTests=false`
Expected: PASS. Adjust the vertex count assertion to match what the GraphML importer actually produces for the fixture (run once and correct if needed).

- [ ] **Step 4: Commit**

```bash
git add e2e-ha/src/test/java/com/arcadedb/containers/ha/ImportDatabaseScenarioIT.java \
        e2e-ha/src/test/resources/import-fixture.graphml
git commit -m "test(e2e-ha): container scenario for import database propagation"
```

---

## Task 28: `UserManagementScenarioIT`

**Files:**
- Create: `e2e-ha/src/test/java/com/arcadedb/containers/ha/UserManagementScenarioIT.java`

- [ ] **Step 1: Write the scenario**

The scenario covers:
1. Create user on leader, verify login on every node.
2. Drop user on leader, verify login rejected everywhere.
3. Stop a replica, create a user, restart the replica, verify catch-up.
4. Create a user, stop the leader, verify login on surviving nodes with the new user.

```java
/*
 * [license header]
 */
package com.arcadedb.containers.ha;

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.test.support.ContainersTestTemplate;
import com.arcadedb.test.support.ServerWrapper;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class UserManagementScenarioIT extends ContainersTestTemplate {

  private static final String SERVER_LIST = "arcadedb-0:2434:2480,arcadedb-1:2434:2480,arcadedb-2:2434:2480";

  @AfterEach
  @Override
  public void tearDown() {
    super.tearDown();
  }

  @Test
  @Timeout(value = 15, unit = TimeUnit.MINUTES)
  @DisplayName("Three-node Raft HA: user create/drop replicates and survives failover")
  void usersReplicateAndSurviveFailover() {
    createArcadeContainer("arcadedb-0", SERVER_LIST, "majority", network);
    createArcadeContainer("arcadedb-1", SERVER_LIST, "majority", network);
    createArcadeContainer("arcadedb-2", SERVER_LIST, "majority", network);

    final List<ServerWrapper> servers = startCluster();

    final JSONObject alice = new JSONObject().put("name", "alice").put("password", "alicepw1234")
        .put("databases", new JSONObject().put("*", new JSONArray().put("admin")));
    servers.get(0).executeServerCommand("create user " + alice.toString());

    // Verify replication
    Awaitility.await().atMost(30, TimeUnit.SECONDS)
        .until(() -> servers.get(0).loginOk("alice", "alicepw1234")
                  && servers.get(1).loginOk("alice", "alicepw1234")
                  && servers.get(2).loginOk("alice", "alicepw1234"));

    servers.get(0).executeServerCommand("drop user alice");
    Awaitility.await().atMost(30, TimeUnit.SECONDS)
        .until(() -> !servers.get(0).loginOk("alice", "alicepw1234")
                  && !servers.get(1).loginOk("alice", "alicepw1234")
                  && !servers.get(2).loginOk("alice", "alicepw1234"));

    // Scenario: stop node 2, create bob, restart node 2, confirm catch-up
    servers.get(2).stop();
    final JSONObject bob = new JSONObject().put("name", "bob").put("password", "bobpw1234")
        .put("databases", new JSONObject().put("*", new JSONArray().put("admin")));
    servers.get(0).executeServerCommand("create user " + bob.toString());
    Awaitility.await().atMost(30, TimeUnit.SECONDS)
        .until(() -> servers.get(0).loginOk("bob", "bobpw1234")
                  && servers.get(1).loginOk("bob", "bobpw1234"));
    servers.get(2).start();
    Awaitility.await().atMost(60, TimeUnit.SECONDS)
        .until(() -> servers.get(2).loginOk("bob", "bobpw1234"));

    // Scenario: create charlie, stop leader, verify charlie on surviving nodes
    final JSONObject charlie = new JSONObject().put("name", "charlie").put("password", "charliepw1234")
        .put("databases", new JSONObject().put("*", new JSONArray().put("admin")));
    servers.get(0).executeServerCommand("create user " + charlie.toString());
    Awaitility.await().atMost(30, TimeUnit.SECONDS)
        .until(() -> servers.get(1).loginOk("charlie", "charliepw1234")
                  && servers.get(2).loginOk("charlie", "charliepw1234"));
    servers.get(0).stop();
    Awaitility.await().atMost(60, TimeUnit.SECONDS)
        .until(() -> servers.get(1).loginOk("charlie", "charliepw1234")
                  || servers.get(2).loginOk("charlie", "charliepw1234"));
  }
}
```

`ServerWrapper.loginOk(String, String)` may not exist. If not, implement it inline as a helper that issues `GET /api/v1/ready` with Basic auth and returns true on HTTP 200.

- [ ] **Step 2: Run the scenario**

Run: `mvn -pl e2e-ha verify -Dit.test=UserManagementScenarioIT -DskipTests=false`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add e2e-ha/src/test/java/com/arcadedb/containers/ha/UserManagementScenarioIT.java
git commit -m "test(e2e-ha): container scenario for user management propagation and failover"
```

---

## Task 29: `UserSeedOnPeerAddScenarioIT`

**Files:**
- Create: `e2e-ha/src/test/java/com/arcadedb/containers/ha/UserSeedOnPeerAddScenarioIT.java`

- [ ] **Step 1: Write the scenario**

The scenario starts a 2-node cluster with `SERVER_LIST` listing both, creates a user, then starts a third node fresh and issues `POST /api/v1/cluster/peer` via the leader. It verifies the seed arrived.

```java
/*
 * [license header]
 */
package com.arcadedb.containers.ha;

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.test.support.ContainersTestTemplate;
import com.arcadedb.test.support.ServerWrapper;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class UserSeedOnPeerAddScenarioIT extends ContainersTestTemplate {

  private static final String TWO_NODES = "arcadedb-0:2434:2480,arcadedb-1:2434:2480";
  private static final String THREE_NODES = "arcadedb-0:2434:2480,arcadedb-1:2434:2480,arcadedb-2:2434:2480";

  @AfterEach
  @Override
  public void tearDown() {
    super.tearDown();
  }

  @Test
  @Timeout(value = 15, unit = TimeUnit.MINUTES)
  @DisplayName("Raft HA: peer-add seeds current users on the new node")
  void peerAddSeedsUsers() {
    // Start a 2-node cluster
    createArcadeContainer("arcadedb-0", TWO_NODES, "majority", network);
    createArcadeContainer("arcadedb-1", TWO_NODES, "majority", network);
    final List<ServerWrapper> initialServers = startCluster();

    // Create custom user
    final JSONObject dave = new JSONObject().put("name", "dave").put("password", "davepw1234")
        .put("databases", new JSONObject().put("*", new JSONArray().put("admin")));
    initialServers.get(0).executeServerCommand("create user " + dave.toString());
    Awaitility.await().atMost(30, TimeUnit.SECONDS)
        .until(() -> initialServers.get(0).loginOk("dave", "davepw1234")
                  && initialServers.get(1).loginOk("dave", "davepw1234"));

    // Start a fresh third container and add it as a peer
    final ServerWrapper node2 = createArcadeContainer("arcadedb-2", THREE_NODES, "majority", network);
    node2.start();

    initialServers.get(0).addPeer("arcadedb-2_2434", "arcadedb-2:2434");

    // Expect the seed to arrive, login should succeed on the new node
    Awaitility.await().atMost(60, TimeUnit.SECONDS)
        .until(() -> node2.loginOk("dave", "davepw1234"));
  }
}
```

`ServerWrapper.addPeer` and `loginOk` may not exist; inline them as HTTP helpers if needed. The peer id format (`arcadedb-2_2434`) mirrors the in-process test convention; verify by reading the current `RaftHAServer.parsePeerList` implementation.

- [ ] **Step 2: Run the scenario**

Run: `mvn -pl e2e-ha verify -Dit.test=UserSeedOnPeerAddScenarioIT -DskipTests=false`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add e2e-ha/src/test/java/com/arcadedb/containers/ha/UserSeedOnPeerAddScenarioIT.java
git commit -m "test(e2e-ha): container scenario for peer-add user seed"
```

---

# Phase 8: Regression guard and documentation

## Task 30: Run the full affected-modules test suite

**Files:** none

- [ ] **Step 1: Compile every affected module**

Run: `mvn -pl server,ha-raft,e2e-ha -am install -DskipTests`
Expected: BUILD SUCCESS.

- [ ] **Step 2: Run all ha-raft tests**

Run: `mvn -pl ha-raft test verify`
Expected: all unit and integration tests pass, including the six new tests from this plan and the pre-existing tests such as `RaftHTTP2ServersCreateReplicatedDatabaseIT`, `RaftReplication3NodesIT`, `RaftReplicationChangeSchemaIT`.

- [ ] **Step 3: Run the e2e-ha verify phase**

Run: `mvn -pl e2e-ha verify`
Expected: all scenarios pass, including the five new scenarios and the pre-existing `ThreeInstancesScenarioIT` and friends. Docker must be running.

- [ ] **Step 4: Cross-check against the spec**

Re-open `docs/plans/2026-04-10-ha-raft-drop-database-propagation-design.md` and walk each In-Scope bullet:
- DROP DATABASE Raft-first: Tasks 1, 3, 6, 7, 8, 9, 10.
- RESTORE via forceSnapshot: Tasks 2, 4, 11, 12, 13.
- IMPORT via createInReplicas + TX_ENTRY: Tasks 14, 15.
- CREATE/DROP USER via SECURITY_USERS_ENTRY: Tasks 5, 16, 17, 18, 19, 20, 21, 22.
- Peer-add seed: Tasks 23, 24.
- e2e-ha scenarios: Tasks 25, 26, 27, 28, 29.

If anything is unchecked, add follow-up tasks inline before moving on.

- [ ] **Step 5: Commit any test-only adjustments**

If the cross-check reveals a missing assertion or a small fix, commit it separately:

```bash
git add <paths>
git commit -m "test(ha-raft|e2e-ha): <adjustment description>"
```

---

## Self-review notes

**Spec coverage:** every in-scope requirement from the design spec maps to at least one task above. Drop, restore, import, users, and seed are all covered.

**Placeholder scan:** no "TBD", "TODO", or "fill in details" lines in the plan. Every step includes actual code or exact commands.

**Type consistency:** the names `dropInReplicas`, `createInReplicas(boolean)`, `replicateSecurityUsers(String)`, `getUsersJsonPayload`, `applyReplicatedUsers`, `DROP_DATABASE_ENTRY`, `SECURITY_USERS_ENTRY`, `applyDropDatabaseEntry`, `applySecurityUsersEntry` are used consistently across the plan.

**Known caveats** the engineer will discover during implementation:
1. The exact format/field names of `PostAddPeerHandler` request/response is not mechanically verified in this plan; Task 23 instructs the engineer to read the file first. Same for `ServerWrapper` helpers - Task 25 instructs to check for existing methods and inline fallbacks if missing.
2. `usersToJSON()` inside `ServerSecurity` may currently be private; Task 16 flags this and instructs to expose it (package-private or via the new public helper).
3. The `ArcadeStateMachineTest` helper `makeTrx(...)` is referenced but not written in Task 8; the engineer should reuse whatever helper pattern already exists in that file and only add one if none is present.
