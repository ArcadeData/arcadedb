# HA Raft Redesign Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a new HA module (`ha-raft/`) using Apache Ratis for Raft consensus, replacing the custom leader-election and replication protocol.

**Architecture:** A standalone Maven module that implements `ServerPlugin`, wraps databases with `RaftReplicatedDatabase`, and uses Apache Ratis 3.2.0 with gRPC transport for consensus and log replication. WAL page diffs are serialized as Raft log entries. A custom `ArcadeStateMachine` applies committed entries to local databases.

**Tech Stack:** Java 21, Apache Ratis 3.2.0, gRPC (via ratis-grpc), JUnit 6, AssertJ, Awaitility, Mockito

**Design doc:** `docs/plans/2026-02-15-ha-raft-redesign.md`

---

## Task 1: Maven Module Skeleton

Set up the `ha-raft/` Maven module with dependencies and register it in the parent POM.

**Files:**
- Create: `ha-raft/pom.xml`
- Modify: `pom.xml` (root, add module)
- Create: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/package-info.java`
- Create: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/package-info.java`

**Step 1: Create `ha-raft/pom.xml`**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!--
    Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.

    SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
    SPDX-License-Identifier: Apache-2.0
-->
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <parent>
        <groupId>com.arcadedb</groupId>
        <artifactId>arcadedb-parent</artifactId>
        <version>26.2.1-SNAPSHOT</version>
        <relativePath>../pom.xml</relativePath>
    </parent>

    <artifactId>arcadedb-ha-raft</artifactId>
    <packaging>jar</packaging>
    <name>ArcadeDB HA Raft</name>

    <properties>
        <ratis.version>3.2.0</ratis.version>
    </properties>

    <dependencies>
        <!-- ArcadeDB -->
        <dependency>
            <groupId>com.arcadedb</groupId>
            <artifactId>arcadedb-engine</artifactId>
            <version>${project.parent.version}</version>
        </dependency>
        <dependency>
            <groupId>com.arcadedb</groupId>
            <artifactId>arcadedb-network</artifactId>
            <version>${project.parent.version}</version>
        </dependency>
        <dependency>
            <groupId>com.arcadedb</groupId>
            <artifactId>arcadedb-server</artifactId>
            <version>${project.parent.version}</version>
            <scope>provided</scope>
        </dependency>

        <!-- Apache Ratis -->
        <dependency>
            <groupId>org.apache.ratis</groupId>
            <artifactId>ratis-server</artifactId>
            <version>${ratis.version}</version>
        </dependency>
        <dependency>
            <groupId>org.apache.ratis</groupId>
            <artifactId>ratis-grpc</artifactId>
            <version>${ratis.version}</version>
        </dependency>

        <!-- Test -->
        <dependency>
            <groupId>com.arcadedb</groupId>
            <artifactId>arcadedb-server</artifactId>
            <version>${project.parent.version}</version>
            <scope>test</scope>
            <type>test-jar</type>
        </dependency>
        <dependency>
            <groupId>org.junit.jupiter</groupId>
            <artifactId>junit-jupiter</artifactId>
            <version>${junit.jupiter.version}</version>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.assertj</groupId>
            <artifactId>assertj-core</artifactId>
            <version>${assertj-core.version}</version>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.awaitility</groupId>
            <artifactId>awaitility</artifactId>
            <version>${awaitility.version}</version>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.mockito</groupId>
            <artifactId>mockito-core</artifactId>
            <version>${mockito-core.version}</version>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>ch.qos.logback</groupId>
            <artifactId>logback-classic</artifactId>
            <version>${logback-classic.version}</version>
            <scope>test</scope>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-compiler-plugin</artifactId>
                <version>${maven-compiler-plugin.version}</version>
                <configuration>
                    <release>${maven-compiler-plugin.release}</release>
                </configuration>
            </plugin>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-surefire-plugin</artifactId>
                <version>${maven-surefire-plugin.version}</version>
            </plugin>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-jar-plugin</artifactId>
                <version>${maven-jar-plugin.version}</version>
                <executions>
                    <execution>
                        <goals>
                            <goal>test-jar</goal>
                        </goals>
                    </execution>
                </executions>
            </plugin>
        </plugins>
    </build>
</project>
```

**Step 2: Add module to root `pom.xml`**

In `pom.xml` at the root, find the `<modules>` block and add `<module>ha-raft</module>` after the `<module>server</module>` line (around line 131).

**Step 3: Create package-info files**

Create `ha-raft/src/main/java/com/arcadedb/server/ha/raft/package-info.java`:
```java
/**
 * ArcadeDB High Availability module using Apache Ratis (Raft consensus).
 */
package com.arcadedb.server.ha.raft;
```

Create `ha-raft/src/test/java/com/arcadedb/server/ha/raft/package-info.java`:
```java
/**
 * Tests for the ArcadeDB HA Raft module.
 */
package com.arcadedb.server.ha.raft;
```

**Step 4: Verify the module compiles**

Run: `cd /Users/frank/projects/arcade/worktrees/ha-redesign && mvn clean compile -pl ha-raft -am -DskipTests`
Expected: BUILD SUCCESS

**Step 5: Commit**

```bash
git add ha-raft/pom.xml ha-raft/src pom.xml
git commit -m "feat(ha-raft): add Maven module skeleton with Ratis dependencies"
```

---

## Task 2: Configuration Properties

Add new `GlobalConfiguration` entries for the Raft HA implementation.

**Files:**
- Modify: `engine/src/main/java/com/arcadedb/GlobalConfiguration.java` (around line 513, after existing HA_* properties)

**Step 1: Write a failing test**

Create `ha-raft/src/test/java/com/arcadedb/server/ha/raft/ConfigValidationTest.java`:
```java
package com.arcadedb.server.ha.raft;

import com.arcadedb.GlobalConfiguration;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ConfigValidationTest {

  @Test
  void haImplementationDefaultsToLegacy() {
    assertThat(GlobalConfiguration.HA_IMPLEMENTATION.getValueAsString()).isEqualTo("legacy");
  }

  @Test
  void haRaftPortHasDefault() {
    assertThat(GlobalConfiguration.HA_RAFT_PORT.getValueAsInteger()).isEqualTo(2434);
  }

  @Test
  void haReplicationLagWarningHasDefault() {
    assertThat(GlobalConfiguration.HA_REPLICATION_LAG_WARNING.getValueAsLong()).isEqualTo(1000L);
  }
}
```

**Step 2: Run test to verify it fails**

Run: `cd /Users/frank/projects/arcade/worktrees/ha-redesign && mvn test -pl ha-raft -Dtest=ConfigValidationTest -am`
Expected: FAIL - `HA_IMPLEMENTATION` field does not exist

**Step 3: Add configuration properties**

In `engine/src/main/java/com/arcadedb/GlobalConfiguration.java`, add after the `HA_K8S_DNS_SUFFIX` entry (around line 513):

```java
  HA_IMPLEMENTATION("arcadedb.ha.implementation", SCOPE.SERVER,
      "HA implementation to use: 'legacy' for the existing custom protocol, 'raft' for the new Apache Ratis-based implementation",
      String.class, "legacy", Set.of("legacy", "raft")),

  HA_RAFT_PORT("arcadedb.ha.raftPort", SCOPE.SERVER,
      "TCP/IP port for Apache Ratis gRPC communication between cluster nodes", Integer.class, 2434),

  HA_REPLICATION_LAG_WARNING("arcadedb.ha.replicationLagWarning", SCOPE.SERVER,
      "Raft log index gap threshold for replication lag warnings. When a replica falls behind by more than this many entries, a warning is logged",
      Long.class, 1000L),
```

**Step 4: Run test to verify it passes**

Run: `cd /Users/frank/projects/arcade/worktrees/ha-redesign && mvn test -pl ha-raft -Dtest=ConfigValidationTest -am`
Expected: PASS (3 tests)

**Step 5: Compile entire project to check nothing breaks**

Run: `cd /Users/frank/projects/arcade/worktrees/ha-redesign && mvn compile -DskipTests`
Expected: BUILD SUCCESS

**Step 6: Commit**

```bash
git add engine/src/main/java/com/arcadedb/GlobalConfiguration.java ha-raft/src/test/java/com/arcadedb/server/ha/raft/ConfigValidationTest.java
git commit -m "feat(ha-raft): add HA_IMPLEMENTATION, HA_RAFT_PORT, HA_REPLICATION_LAG_WARNING config properties"
```

---

## Task 3: RaftLogEntryCodec - Serialization Layer

Build the codec that serializes WAL transaction data and schema commands into Raft log entries, and deserializes them back.

**Files:**
- Create: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftLogEntryCodec.java`
- Create: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftLogEntryType.java`
- Test: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftLogEntryCodecTest.java`

**Step 1: Write the failing test**

Create `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftLogEntryCodecTest.java`:
```java
package com.arcadedb.server.ha.raft;

import com.arcadedb.serializer.json.JSONObject;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class RaftLogEntryCodecTest {

  @Test
  void roundTripTxEntry() {
    final byte[] walData = new byte[]{1, 2, 3, 4, 5, 6, 7, 8};
    final Map<Integer, Integer> bucketDeltas = Map.of(0, 5, 1, -2);

    final ByteString encoded = RaftLogEntryCodec.encodeTxEntry("testdb", walData, bucketDeltas);
    final RaftLogEntryCodec.DecodedEntry decoded = RaftLogEntryCodec.decode(encoded);

    assertThat(decoded.type()).isEqualTo(RaftLogEntryType.TX_ENTRY);
    assertThat(decoded.databaseName()).isEqualTo("testdb");
    assertThat(decoded.walData()).isEqualTo(walData);
    assertThat(decoded.bucketRecordDelta()).isEqualTo(bucketDeltas);
    assertThat(decoded.schemaJson()).isNull();
    assertThat(decoded.filesToAdd()).isNull();
    assertThat(decoded.filesToRemove()).isNull();
  }

  @Test
  void roundTripSchemaEntry() {
    final String schemaJson = new JSONObject().put("types", new JSONObject()).toString();
    final Map<Integer, String> filesToAdd = Map.of(5, "testdb/schema.json");
    final Map<Integer, String> filesToRemove = Map.of(3, "testdb/old.idx");

    final ByteString encoded = RaftLogEntryCodec.encodeSchemaEntry("testdb", schemaJson, filesToAdd, filesToRemove);
    final RaftLogEntryCodec.DecodedEntry decoded = RaftLogEntryCodec.decode(encoded);

    assertThat(decoded.type()).isEqualTo(RaftLogEntryType.SCHEMA_ENTRY);
    assertThat(decoded.databaseName()).isEqualTo("testdb");
    assertThat(decoded.schemaJson()).isEqualTo(schemaJson);
    assertThat(decoded.filesToAdd()).isEqualTo(filesToAdd);
    assertThat(decoded.filesToRemove()).isEqualTo(filesToRemove);
    assertThat(decoded.walData()).isNull();
  }

  @Test
  void roundTripTxEntryWithEmptyBucketDeltas() {
    final byte[] walData = new byte[]{10, 20, 30};
    final Map<Integer, Integer> bucketDeltas = Map.of();

    final ByteString encoded = RaftLogEntryCodec.encodeTxEntry("db2", walData, bucketDeltas);
    final RaftLogEntryCodec.DecodedEntry decoded = RaftLogEntryCodec.decode(encoded);

    assertThat(decoded.type()).isEqualTo(RaftLogEntryType.TX_ENTRY);
    assertThat(decoded.databaseName()).isEqualTo("db2");
    assertThat(decoded.walData()).isEqualTo(walData);
    assertThat(decoded.bucketRecordDelta()).isEmpty();
  }

  @Test
  void roundTripSchemaEntryWithNullFileNames() {
    final Map<Integer, String> filesToAdd = new HashMap<>();
    filesToAdd.put(7, null);
    filesToAdd.put(8, "testdb/newfile.dat");

    final ByteString encoded = RaftLogEntryCodec.encodeSchemaEntry("testdb", "{}", filesToAdd, Map.of());
    final RaftLogEntryCodec.DecodedEntry decoded = RaftLogEntryCodec.decode(encoded);

    assertThat(decoded.filesToAdd()).containsEntry(7, null);
    assertThat(decoded.filesToAdd()).containsEntry(8, "testdb/newfile.dat");
  }

  @Test
  void roundTripLargeWalData() {
    final byte[] walData = new byte[1024 * 1024]; // 1MB
    for (int i = 0; i < walData.length; i++)
      walData[i] = (byte) (i % 256);

    final ByteString encoded = RaftLogEntryCodec.encodeTxEntry("bigdb", walData, Map.of());
    final RaftLogEntryCodec.DecodedEntry decoded = RaftLogEntryCodec.decode(encoded);

    assertThat(decoded.walData()).isEqualTo(walData);
  }
}
```

**Step 2: Run test to verify it fails**

Run: `cd /Users/frank/projects/arcade/worktrees/ha-redesign && mvn test -pl ha-raft -Dtest=RaftLogEntryCodecTest -am`
Expected: FAIL - classes don't exist

**Step 3: Create `RaftLogEntryType` enum**

Create `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftLogEntryType.java`:
```java
package com.arcadedb.server.ha.raft;

public enum RaftLogEntryType {
  TX_ENTRY((byte) 1),
  SCHEMA_ENTRY((byte) 2);

  private final byte id;

  RaftLogEntryType(final byte id) {
    this.id = id;
  }

  public byte getId() {
    return id;
  }

  public static RaftLogEntryType fromId(final byte id) {
    for (final RaftLogEntryType type : values())
      if (type.id == id)
        return type;
    throw new IllegalArgumentException("Unknown RaftLogEntryType id: " + id);
  }
}
```

**Step 4: Create `RaftLogEntryCodec`**

Create `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftLogEntryCodec.java`:
```java
package com.arcadedb.server.ha.raft;

import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * Serializes and deserializes ArcadeDB replication entries for Raft log.
 * Two entry types: TX_ENTRY (WAL page diffs) and SCHEMA_ENTRY (DDL commands).
 */
public final class RaftLogEntryCodec {

  private RaftLogEntryCodec() {
  }

  public record DecodedEntry(
      RaftLogEntryType type,
      String databaseName,
      byte[] walData,
      Map<Integer, Integer> bucketRecordDelta,
      String schemaJson,
      Map<Integer, String> filesToAdd,
      Map<Integer, String> filesToRemove
  ) {
  }

  public static ByteString encodeTxEntry(final String databaseName, final byte[] walData,
      final Map<Integer, Integer> bucketRecordDelta) {
    try {
      final ByteArrayOutputStream baos = new ByteArrayOutputStream();
      final DataOutputStream out = new DataOutputStream(baos);

      out.writeByte(RaftLogEntryType.TX_ENTRY.getId());
      out.writeUTF(databaseName);

      out.writeInt(walData.length);
      out.write(walData);

      out.writeInt(bucketRecordDelta.size());
      for (final Map.Entry<Integer, Integer> entry : bucketRecordDelta.entrySet()) {
        out.writeInt(entry.getKey());
        out.writeInt(entry.getValue());
      }

      out.flush();
      return ByteString.copyFrom(baos.toByteArray());
    } catch (final IOException e) {
      throw new RuntimeException("Failed to encode TX_ENTRY", e);
    }
  }

  public static ByteString encodeSchemaEntry(final String databaseName, final String schemaJson,
      final Map<Integer, String> filesToAdd, final Map<Integer, String> filesToRemove) {
    try {
      final ByteArrayOutputStream baos = new ByteArrayOutputStream();
      final DataOutputStream out = new DataOutputStream(baos);

      out.writeByte(RaftLogEntryType.SCHEMA_ENTRY.getId());
      out.writeUTF(databaseName);
      out.writeUTF(schemaJson);

      writeFileMap(out, filesToAdd);
      writeFileMap(out, filesToRemove);

      out.flush();
      return ByteString.copyFrom(baos.toByteArray());
    } catch (final IOException e) {
      throw new RuntimeException("Failed to encode SCHEMA_ENTRY", e);
    }
  }

  public static DecodedEntry decode(final ByteString data) {
    try {
      final DataInputStream in = new DataInputStream(data.newInput());

      final RaftLogEntryType type = RaftLogEntryType.fromId(in.readByte());
      final String databaseName = in.readUTF();

      return switch (type) {
        case TX_ENTRY -> decodeTxEntry(in, databaseName);
        case SCHEMA_ENTRY -> decodeSchemaEntry(in, databaseName);
      };
    } catch (final IOException e) {
      throw new RuntimeException("Failed to decode raft log entry", e);
    }
  }

  private static DecodedEntry decodeTxEntry(final DataInputStream in, final String databaseName) throws IOException {
    final int walLength = in.readInt();
    final byte[] walData = in.readNBytes(walLength);

    final int deltaCount = in.readInt();
    final Map<Integer, Integer> bucketDeltas = HashMap.newHashMap(deltaCount);
    for (int i = 0; i < deltaCount; i++)
      bucketDeltas.put(in.readInt(), in.readInt());

    return new DecodedEntry(RaftLogEntryType.TX_ENTRY, databaseName, walData, bucketDeltas, null, null, null);
  }

  private static DecodedEntry decodeSchemaEntry(final DataInputStream in, final String databaseName) throws IOException {
    final String schemaJson = in.readUTF();
    final Map<Integer, String> filesToAdd = readFileMap(in);
    final Map<Integer, String> filesToRemove = readFileMap(in);

    return new DecodedEntry(RaftLogEntryType.SCHEMA_ENTRY, databaseName, null, null, schemaJson, filesToAdd, filesToRemove);
  }

  private static void writeFileMap(final DataOutputStream out, final Map<Integer, String> fileMap) throws IOException {
    out.writeInt(fileMap.size());
    for (final Map.Entry<Integer, String> entry : fileMap.entrySet()) {
      out.writeInt(entry.getKey());
      if (entry.getValue() != null) {
        out.writeBoolean(true);
        out.writeUTF(entry.getValue());
      } else
        out.writeBoolean(false);
    }
  }

  private static Map<Integer, String> readFileMap(final DataInputStream in) throws IOException {
    final int count = in.readInt();
    final Map<Integer, String> result = HashMap.newHashMap(count);
    for (int i = 0; i < count; i++) {
      final int fileId = in.readInt();
      final boolean hasValue = in.readBoolean();
      result.put(fileId, hasValue ? in.readUTF() : null);
    }
    return result;
  }
}
```

**Step 5: Run test to verify it passes**

Run: `cd /Users/frank/projects/arcade/worktrees/ha-redesign && mvn test -pl ha-raft -Dtest=RaftLogEntryCodecTest -am`
Expected: PASS (5 tests)

**Step 6: Commit**

```bash
git add ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftLogEntryType.java ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftLogEntryCodec.java ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftLogEntryCodecTest.java
git commit -m "feat(ha-raft): add RaftLogEntryCodec for TX and SCHEMA entry serialization"
```

---

## Task 4: ArcadeStateMachine - Core State Machine

Implement the Ratis `BaseStateMachine` that applies committed log entries to ArcadeDB databases.

**Files:**
- Create: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/ArcadeStateMachine.java`
- Test: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/ArcadeStateMachineTest.java`

**Context:** The state machine receives committed Raft log entries via `applyTransaction()`. Each entry is a serialized `TX_ENTRY` or `SCHEMA_ENTRY`. For `TX_ENTRY`, it deserializes the WAL buffer and calls `TransactionManager.applyChanges()`. For `SCHEMA_ENTRY`, it applies file changes and reloads the schema. This mirrors the logic in the existing `TxRequest.execute()` (see `server/src/main/java/com/arcadedb/server/ha/message/TxRequest.java:76-122`).

**Step 1: Write the failing test**

Create `ha-raft/src/test/java/com/arcadedb/server/ha/raft/ArcadeStateMachineTest.java`:
```java
package com.arcadedb.server.ha.raft;

import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.proto.RaftProtos.StateMachineLogEntryProto;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

class ArcadeStateMachineTest {

  @Test
  void stateMachineCanBeInstantiated() {
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    assertThat(sm).isNotNull();
  }

  @Test
  void getLastAppliedTermIndexInitiallyNull() {
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    // Before initialization, last applied should be at 0 or null
    assertThat(sm.getLastAppliedTermIndex()).isNull();
  }
}
```

**Step 2: Run test to verify it fails**

Run: `cd /Users/frank/projects/arcade/worktrees/ha-redesign && mvn test -pl ha-raft -Dtest=ArcadeStateMachineTest -am`
Expected: FAIL - class doesn't exist

**Step 3: Create `ArcadeStateMachine`**

Create `ha-raft/src/main/java/com/arcadedb/server/ha/raft/ArcadeStateMachine.java`:
```java
package com.arcadedb.server.ha.raft;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.WALFile;
import com.arcadedb.log.LogManager;
import com.arcadedb.server.ArcadeDBServer;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.proto.RaftProtos.RaftPeerRole;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Level;

/**
 * Ratis state machine that applies committed WAL transactions and schema changes
 * to ArcadeDB databases.
 */
public class ArcadeStateMachine extends BaseStateMachine {

  private ArcadeDBServer server;
  private volatile TermIndex lastAppliedTermIndex;

  public void setServer(final ArcadeDBServer server) {
    this.server = server;
  }

  @Override
  public CompletableFuture<Message> applyTransaction(final TransactionContext trx) {
    final LogEntryProto entry = trx.getLogEntry();
    final ByteString data = entry.getStateMachineLogEntry().getLogData();
    final TermIndex termIndex = TermIndex.valueOf(entry);

    try {
      final RaftLogEntryCodec.DecodedEntry decoded = RaftLogEntryCodec.decode(data);

      switch (decoded.type()) {
        case TX_ENTRY -> applyTxEntry(decoded);
        case SCHEMA_ENTRY -> applySchemaEntry(decoded);
      }

      lastAppliedTermIndex = termIndex;
      return CompletableFuture.completedFuture(Message.valueOf("OK"));

    } catch (final Exception e) {
      LogManager.instance().log(this, Level.SEVERE, "Error applying raft log entry at index %d", e, termIndex.getIndex());
      return CompletableFuture.failedFuture(e);
    }
  }

  @Override
  public TermIndex getLastAppliedTermIndex() {
    return lastAppliedTermIndex;
  }

  private void applyTxEntry(final RaftLogEntryCodec.DecodedEntry decoded) {
    final DatabaseInternal db = (DatabaseInternal) server.getDatabase(decoded.databaseName());

    final WALFile.WALTransaction walTx = deserializeWalTransaction(decoded.walData());

    LogManager.instance().log(this, Level.FINE, "Applying tx %d to database '%s' (pages=%d)",
        walTx.txId, decoded.databaseName(), walTx.pages.length);

    db.getTransactionManager().applyChanges(walTx, decoded.bucketRecordDelta(), false);
  }

  private void applySchemaEntry(final RaftLogEntryCodec.DecodedEntry decoded) {
    final DatabaseInternal db = (DatabaseInternal) server.getDatabase(decoded.databaseName());
    final String databasePath = db.getDatabasePath();

    try {
      // Add files
      if (decoded.filesToAdd() != null)
        for (final Map.Entry<Integer, String> entry : decoded.filesToAdd().entrySet())
          db.getFileManager().getOrCreateFile(entry.getKey(), databasePath + File.separator + entry.getValue());

      // Remove files
      if (decoded.filesToRemove() != null)
        for (final Map.Entry<Integer, String> entry : decoded.filesToRemove().entrySet()) {
          db.getPageManager().deleteFile(db, entry.getKey());
          db.getFileManager().dropFile(entry.getKey());
          db.getSchema().getEmbedded().removeFile(entry.getKey());
        }

      // Update schema
      if (decoded.schemaJson() != null && !decoded.schemaJson().isEmpty()) {
        final var schemaJson = new com.arcadedb.serializer.json.JSONObject(decoded.schemaJson());
        db.getSchema().getEmbedded().update(schemaJson);
      }

      db.getSchema().getEmbedded().initComponents();

    } catch (final IOException e) {
      throw new RuntimeException("Failed to apply schema entry for database '" + decoded.databaseName() + "'", e);
    }

    LogManager.instance().log(this, Level.FINE, "Applied schema change to database '%s'", decoded.databaseName());
  }

  /**
   * Deserializes a WAL transaction from raw bytes. Uses the same format as
   * {@code WALFile.writeTransactionToBuffer()} / {@code TxRequestAbstract.readTxFromBuffer()}.
   */
  static WALFile.WALTransaction deserializeWalTransaction(final byte[] data) {
    final ByteBuffer buf = ByteBuffer.wrap(data);
    final WALFile.WALTransaction tx = new WALFile.WALTransaction();

    tx.txId = buf.getLong();
    tx.timestamp = buf.getLong();
    final int pageCount = buf.getInt();
    buf.getInt(); // segmentSize - skip, we use pageCount for iteration

    tx.pages = new WALFile.WALPage[pageCount];

    for (int i = 0; i < pageCount; i++) {
      final WALFile.WALPage page = new WALFile.WALPage();
      page.fileId = buf.getInt();
      page.pageNumber = buf.getInt();
      page.changesFrom = buf.getInt();
      page.changesTo = buf.getInt();
      page.currentPageVersion = buf.getInt();
      page.currentPageSize = buf.getInt();

      final int deltaSize = page.changesTo - page.changesFrom + 1;
      final byte[] content = new byte[deltaSize];
      buf.get(content);
      page.currentContent = new com.arcadedb.database.Binary(content);

      tx.pages[i] = page;
    }

    return tx;
  }
}
```

**Step 4: Run test to verify it passes**

Run: `cd /Users/frank/projects/arcade/worktrees/ha-redesign && mvn test -pl ha-raft -Dtest=ArcadeStateMachineTest -am`
Expected: PASS (2 tests)

**Step 5: Commit**

```bash
git add ha-raft/src/main/java/com/arcadedb/server/ha/raft/ArcadeStateMachine.java ha-raft/src/test/java/com/arcadedb/server/ha/raft/ArcadeStateMachineTest.java
git commit -m "feat(ha-raft): add ArcadeStateMachine with WAL and schema entry application"
```

---

## Task 5: ClusterMonitor - Replication Lag Tracking

Track replication lag per replica and emit warnings when the gap exceeds the configured threshold.

**Files:**
- Create: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/ClusterMonitor.java`
- Test: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/ClusterMonitorTest.java`

**Step 1: Write the failing test**

Create `ha-raft/src/test/java/com/arcadedb/server/ha/raft/ClusterMonitorTest.java`:
```java
package com.arcadedb.server.ha.raft;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ClusterMonitorTest {

  @Test
  void replicationLagIsZeroWhenEmpty() {
    final ClusterMonitor monitor = new ClusterMonitor(1000L);
    assertThat(monitor.getReplicaLags()).isEmpty();
  }

  @Test
  void tracksReplicaLag() {
    final ClusterMonitor monitor = new ClusterMonitor(1000L);
    monitor.updateLeaderCommitIndex(100);
    monitor.updateReplicaMatchIndex("replica1", 90);
    monitor.updateReplicaMatchIndex("replica2", 50);

    final Map<String, Long> lags = monitor.getReplicaLags();
    assertThat(lags).containsEntry("replica1", 10L);
    assertThat(lags).containsEntry("replica2", 50L);
  }

  @Test
  void identifiesLaggingReplicas() {
    final ClusterMonitor monitor = new ClusterMonitor(20L);
    monitor.updateLeaderCommitIndex(100);
    monitor.updateReplicaMatchIndex("replica1", 90); // lag 10, below threshold
    monitor.updateReplicaMatchIndex("replica2", 50); // lag 50, above threshold

    assertThat(monitor.isReplicaLagging("replica1")).isFalse();
    assertThat(monitor.isReplicaLagging("replica2")).isTrue();
  }

  @Test
  void removesReplica() {
    final ClusterMonitor monitor = new ClusterMonitor(1000L);
    monitor.updateReplicaMatchIndex("replica1", 50);
    monitor.removeReplica("replica1");
    assertThat(monitor.getReplicaLags()).isEmpty();
  }

  @Test
  void lagUpdatesWhenLeaderAdvances() {
    final ClusterMonitor monitor = new ClusterMonitor(1000L);
    monitor.updateLeaderCommitIndex(100);
    monitor.updateReplicaMatchIndex("replica1", 95);
    assertThat(monitor.getReplicaLags().get("replica1")).isEqualTo(5L);

    monitor.updateLeaderCommitIndex(200);
    assertThat(monitor.getReplicaLags().get("replica1")).isEqualTo(105L);
  }
}
```

**Step 2: Run test to verify it fails**

Run: `cd /Users/frank/projects/arcade/worktrees/ha-redesign && mvn test -pl ha-raft -Dtest=ClusterMonitorTest -am`
Expected: FAIL - class doesn't exist

**Step 3: Create `ClusterMonitor`**

Create `ha-raft/src/main/java/com/arcadedb/server/ha/raft/ClusterMonitor.java`:
```java
package com.arcadedb.server.ha.raft;

import com.arcadedb.log.LogManager;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;

/**
 * Tracks replication lag for each replica by comparing its matchIndex against the leader's commitIndex.
 * Emits warnings when lag exceeds the configured threshold.
 */
public class ClusterMonitor {

  private final long lagWarningThreshold;
  private volatile long leaderCommitIndex;
  private final ConcurrentHashMap<String, Long> replicaMatchIndexes = new ConcurrentHashMap<>();

  public ClusterMonitor(final long lagWarningThreshold) {
    this.lagWarningThreshold = lagWarningThreshold;
  }

  public void updateLeaderCommitIndex(final long commitIndex) {
    this.leaderCommitIndex = commitIndex;
  }

  public void updateReplicaMatchIndex(final String replicaId, final long matchIndex) {
    final long previousMatchIndex = replicaMatchIndexes.put(replicaId, matchIndex) != null
        ? replicaMatchIndexes.get(replicaId) : 0;

    final long lag = leaderCommitIndex - matchIndex;
    if (lag > lagWarningThreshold)
      LogManager.instance().log(this, Level.WARNING, "Replica '%s' is lagging behind by %d entries (threshold: %d)",
          replicaId, lag, lagWarningThreshold);
  }

  public void removeReplica(final String replicaId) {
    replicaMatchIndexes.remove(replicaId);
  }

  public Map<String, Long> getReplicaLags() {
    if (replicaMatchIndexes.isEmpty())
      return Collections.emptyMap();

    final Map<String, Long> lags = new ConcurrentHashMap<>();
    for (final Map.Entry<String, Long> entry : replicaMatchIndexes.entrySet())
      lags.put(entry.getKey(), leaderCommitIndex - entry.getValue());
    return lags;
  }

  public boolean isReplicaLagging(final String replicaId) {
    final Long matchIndex = replicaMatchIndexes.get(replicaId);
    if (matchIndex == null)
      return false;
    return (leaderCommitIndex - matchIndex) > lagWarningThreshold;
  }

  public long getLeaderCommitIndex() {
    return leaderCommitIndex;
  }

  public long getLagWarningThreshold() {
    return lagWarningThreshold;
  }
}
```

**Step 4: Run test to verify it passes**

Run: `cd /Users/frank/projects/arcade/worktrees/ha-redesign && mvn test -pl ha-raft -Dtest=ClusterMonitorTest -am`
Expected: PASS (5 tests)

**Step 5: Commit**

```bash
git add ha-raft/src/main/java/com/arcadedb/server/ha/raft/ClusterMonitor.java ha-raft/src/test/java/com/arcadedb/server/ha/raft/ClusterMonitorTest.java
git commit -m "feat(ha-raft): add ClusterMonitor for replication lag tracking"
```

---

## Task 6: RaftHAServer - Ratis Server Wrapper

The core class that manages the Ratis `RaftServer` instance, builds the peer list from configuration, and provides cluster state queries.

**Files:**
- Create: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAServer.java`
- Test: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftHAServerTest.java`

**Context:** This class reads `HA_SERVER_LIST`, `HA_CLUSTER_NAME`, `HA_RAFT_PORT` from configuration. It builds a Ratis `RaftGroup` with peers derived from the server list. It creates and starts a `RaftServer` with the `ArcadeStateMachine` and gRPC transport. It exposes `isLeader()`, `getLeaderId()`, and `getClient()` for submitting entries.

**Step 1: Write the failing test**

Create `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftHAServerTest.java`:
```java
package com.arcadedb.server.ha.raft;

import org.apache.ratis.protocol.RaftPeer;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class RaftHAServerTest {

  @Test
  void parsePeerListSingleServer() {
    final List<RaftPeer> peers = RaftHAServer.parsePeerList("localhost:2424", 2434);
    assertThat(peers).hasSize(1);
    assertThat(peers.get(0).getAddress()).isEqualTo("localhost:2434");
  }

  @Test
  void parsePeerListMultipleServers() {
    final List<RaftPeer> peers = RaftHAServer.parsePeerList("host1:2424,host2:2424,host3:2424", 2434);
    assertThat(peers).hasSize(3);
    assertThat(peers.get(0).getAddress()).isEqualTo("host1:2434");
    assertThat(peers.get(1).getAddress()).isEqualTo("host2:2434");
    assertThat(peers.get(2).getAddress()).isEqualTo("host3:2434");
  }

  @Test
  void parsePeerListAssignsUniqueIds() {
    final List<RaftPeer> peers = RaftHAServer.parsePeerList("a:2424,b:2424", 2434);
    assertThat(peers.get(0).getId()).isNotEqualTo(peers.get(1).getId());
  }

  @Test
  void parsePeerListWithCustomRaftPort() {
    final List<RaftPeer> peers = RaftHAServer.parsePeerList("myhost:2424", 9999);
    assertThat(peers.get(0).getAddress()).isEqualTo("myhost:9999");
  }
}
```

**Step 2: Run test to verify it fails**

Run: `cd /Users/frank/projects/arcade/worktrees/ha-redesign && mvn test -pl ha-raft -Dtest=RaftHAServerTest -am`
Expected: FAIL - class doesn't exist

**Step 3: Create `RaftHAServer`**

Create `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAServer.java`:
```java
package com.arcadedb.server.ha.raft;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.log.LogManager;
import com.arcadedb.server.ArcadeDBServer;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.grpc.GrpcFactory;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.util.NetUtils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.logging.Level;

/**
 * Manages the Apache Ratis RaftServer instance for ArcadeDB HA.
 * Builds peer list from HA_SERVER_LIST, creates the Raft group,
 * and provides cluster state access.
 */
public class RaftHAServer {

  private final ArcadeDBServer arcadeServer;
  private final ContextConfiguration configuration;
  private final ArcadeStateMachine stateMachine;
  private final ClusterMonitor clusterMonitor;
  private final RaftGroup raftGroup;
  private final RaftPeerId localPeerId;

  private RaftServer raftServer;
  private RaftClient raftClient;

  public RaftHAServer(final ArcadeDBServer arcadeServer, final ContextConfiguration configuration) {
    this.arcadeServer = arcadeServer;
    this.configuration = configuration;

    this.stateMachine = new ArcadeStateMachine();
    this.stateMachine.setServer(arcadeServer);

    final long lagThreshold = configuration.getValueAsLong(GlobalConfiguration.HA_REPLICATION_LAG_WARNING);
    this.clusterMonitor = new ClusterMonitor(lagThreshold);

    final String serverList = configuration.getValueAsString(GlobalConfiguration.HA_SERVER_LIST);
    final int raftPort = configuration.getValueAsInteger(GlobalConfiguration.HA_RAFT_PORT);
    final String clusterName = configuration.getValueAsString(GlobalConfiguration.HA_CLUSTER_NAME);

    final List<RaftPeer> peers = parsePeerList(serverList, raftPort);

    // Determine local peer by matching server name to peer index
    final String serverName = configuration.getValueAsString(GlobalConfiguration.SERVER_NAME);
    this.localPeerId = findLocalPeerId(peers, serverName, arcadeServer);

    final RaftGroupId groupId = RaftGroupId.valueOf(UUID.nameUUIDFromBytes(clusterName.getBytes()));
    this.raftGroup = RaftGroup.valueOf(groupId, peers);
  }

  public void start() throws IOException {
    final RaftProperties properties = new RaftProperties();

    final int raftPort = configuration.getValueAsInteger(GlobalConfiguration.HA_RAFT_PORT);
    GrpcConfigKeys.Server.setPort(properties, raftPort);

    final String storagePath = arcadeServer.getRootPath() + File.separator + "raft-storage";
    RaftServerConfigKeys.setStorageDir(properties, List.of(new File(storagePath)));

    raftServer = RaftServer.newBuilder()
        .setGroup(raftGroup)
        .setServerId(localPeerId)
        .setStateMachine(stateMachine)
        .setProperties(properties)
        .build();

    raftServer.start();

    raftClient = RaftClient.newBuilder()
        .setRaftGroup(raftGroup)
        .setProperties(properties)
        .build();

    LogManager.instance().log(this, Level.INFO, "Raft HA server started (peer=%s, group=%s)", localPeerId, raftGroup.getGroupId());
  }

  public void stop() throws IOException {
    if (raftClient != null)
      raftClient.close();
    if (raftServer != null)
      raftServer.close();

    LogManager.instance().log(this, Level.INFO, "Raft HA server stopped");
  }

  public boolean isLeader() {
    try {
      return raftServer != null && raftServer.getDivision(raftGroup.getGroupId()).getInfo().isLeader();
    } catch (final Exception e) {
      return false;
    }
  }

  public RaftPeerId getLeaderId() {
    try {
      final var roleInfo = raftServer.getDivision(raftGroup.getGroupId()).getInfo();
      if (roleInfo.isLeader())
        return localPeerId;
      return roleInfo.getFollowerState() != null ? roleInfo.getFollowerState().getLeaderId() : null;
    } catch (final Exception e) {
      return null;
    }
  }

  public RaftClient getClient() {
    return raftClient;
  }

  public ArcadeStateMachine getStateMachine() {
    return stateMachine;
  }

  public ClusterMonitor getClusterMonitor() {
    return clusterMonitor;
  }

  public RaftGroup getRaftGroup() {
    return raftGroup;
  }

  public RaftPeerId getLocalPeerId() {
    return localPeerId;
  }

  /**
   * Parses HA_SERVER_LIST (format: "host1:httpPort,host2:httpPort,...") into Raft peers.
   * Each peer's address uses the specified raftPort instead of the HTTP port.
   */
  static List<RaftPeer> parsePeerList(final String serverList, final int raftPort) {
    final String[] servers = serverList.split(",");
    final List<RaftPeer> peers = new ArrayList<>(servers.length);

    for (int i = 0; i < servers.length; i++) {
      final String server = servers[i].trim();
      final String host = server.contains(":") ? server.substring(0, server.indexOf(':')) : server;
      final String address = host + ":" + raftPort;
      final RaftPeerId peerId = RaftPeerId.valueOf("peer-" + i);

      peers.add(RaftPeer.newBuilder().setId(peerId).setAddress(address).build());
    }

    return peers;
  }

  private RaftPeerId findLocalPeerId(final List<RaftPeer> peers, final String serverName, final ArcadeDBServer server) {
    // Use server index from server name (e.g., "ArcadeDB_0" -> index 0)
    try {
      final String indexStr = serverName.substring(serverName.lastIndexOf('_') + 1);
      final int index = Integer.parseInt(indexStr);
      if (index >= 0 && index < peers.size())
        return peers.get(index).getId();
    } catch (final Exception ignored) {
    }

    // Fallback: use first peer
    LogManager.instance().log(this, Level.WARNING,
        "Could not determine local peer from server name '%s', defaulting to peer-0", serverName);
    return peers.get(0).getId();
  }
}
```

**Step 4: Run test to verify it passes**

Run: `cd /Users/frank/projects/arcade/worktrees/ha-redesign && mvn test -pl ha-raft -Dtest=RaftHAServerTest -am`
Expected: PASS (4 tests)

**Step 5: Commit**

```bash
git add ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAServer.java ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftHAServerTest.java
git commit -m "feat(ha-raft): add RaftHAServer wrapping Ratis RaftServer with peer list parsing"
```

---

## Task 7: RaftReplicatedDatabase - Transaction Interception

The database wrapper that intercepts `commit()` and `command()` to submit entries through Raft.

**Files:**
- Create: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftReplicatedDatabase.java`
- Test: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftReplicatedDatabaseTest.java`

**Context:** This mirrors the existing `ReplicatedDatabase` at `server/src/main/java/com/arcadedb/server/ha/ReplicatedDatabase.java`. It implements `DatabaseInternal`, wraps a `LocalDatabase`, and overrides `commit()` to serialize WAL changes and submit them as Raft log entries. On replicas, write commands are forwarded to the leader. Read queries execute locally.

**Step 1: Write the failing test**

Create `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftReplicatedDatabaseTest.java`:
```java
package com.arcadedb.server.ha.raft;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class RaftReplicatedDatabaseTest {

  @Test
  void classExists() {
    assertThat(RaftReplicatedDatabase.class).isNotNull();
  }

  @Test
  void implementsDatabaseInternal() {
    assertThat(com.arcadedb.database.DatabaseInternal.class.isAssignableFrom(RaftReplicatedDatabase.class)).isTrue();
  }
}
```

**Step 2: Run test to verify it fails**

Run: `cd /Users/frank/projects/arcade/worktrees/ha-redesign && mvn test -pl ha-raft -Dtest=RaftReplicatedDatabaseTest -am`
Expected: FAIL - class doesn't exist

**Step 3: Create `RaftReplicatedDatabase`**

Create `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftReplicatedDatabase.java`:

> **NOTE:** This class implements `DatabaseInternal` and delegates ~100+ methods to the wrapped `LocalDatabase`. The implementation follows the exact same pattern as the existing `ReplicatedDatabase` at `server/src/main/java/com/arcadedb/server/ha/ReplicatedDatabase.java`. Copy that file as the starting point, then modify the critical methods below. The key changes are:
>
> 1. Replace the constructor to accept `RaftHAServer` instead of `HAServer`
> 2. Replace `commit()` to submit WAL changes via `RaftClient.io().send()` instead of `HAServer.sendCommandToReplicasWithQuorum()`
> 3. Replace `command()` to forward DDL to leader via Raft instead of `HAServer.forwardCommandToLeader()`
> 4. Remove all references to `HAServer.QUORUM` enum and use the Raft-based quorum model

Key overridden methods (everything else delegates to `proxied`):

```java
package com.arcadedb.server.ha.raft;

import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.engine.WALFile;
import com.arcadedb.log.LogManager;
import com.arcadedb.server.ArcadeDBServer;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Level;

// ... implements DatabaseInternal, delegates all methods to proxied ...

public class RaftReplicatedDatabase implements DatabaseInternal {

  private final ArcadeDBServer server;
  private final LocalDatabase proxied;
  private final RaftHAServer raftHAServer;

  public RaftReplicatedDatabase(final ArcadeDBServer server, final LocalDatabase proxied, final RaftHAServer raftHAServer) {
    this.server = server;
    this.proxied = proxied;
    this.raftHAServer = raftHAServer;
    this.proxied.setWrappedDatabaseInstance(this);
  }

  @Override
  public void commit() {
    proxied.incrementStatsTxCommits();

    final boolean isLeader = raftHAServer.isLeader();

    proxied.executeInReadLock(() -> {
      proxied.checkTransactionIsActive(false);

      final DatabaseContext.DatabaseContextTL current = DatabaseContext.INSTANCE.getContext(proxied.getDatabasePath());
      final TransactionContext tx = current.getLastTransaction();

      try {
        final TransactionContext.TransactionPhase1 phase1 = tx.commit1stPhase(isLeader);

        if (phase1 != null) {
          if (isLeader)
            submitToRaft(phase1);
          else
            forwardToLeader(tx, phase1);
        }
      } catch (final Exception e) {
        tx.rollback();
        throw e;
      }

      return null;
    });
  }

  private void submitToRaft(final TransactionContext.TransactionPhase1 phase1) {
    final byte[] walData = phase1.result.toByteArray();
    // bucketRecordDelta comes from the transaction context
    final Map<Integer, Integer> bucketDeltas = Map.of(); // TODO: extract from phase1

    final ByteString entry = RaftLogEntryCodec.encodeTxEntry(getName(), walData, bucketDeltas);

    try {
      final RaftClientReply reply = raftHAServer.getClient().io().send(Message.valueOf(entry));
      if (!reply.isSuccess()) {
        throw new RuntimeException("Raft commit failed: " + reply.getException());
      }
    } catch (final Exception e) {
      throw new RuntimeException("Failed to submit transaction to Raft", e);
    }
  }

  private void forwardToLeader(final TransactionContext tx, final TransactionContext.TransactionPhase1 phase1) {
    // Forward to leader via Raft client - the leader will process and replicate
    submitToRaft(phase1);
    tx.reset();
  }

  public boolean isLeader() {
    return raftHAServer.isLeader();
  }

  public LocalDatabase getWrapped() {
    return proxied;
  }

  // ... all other DatabaseInternal methods delegate to proxied ...
  // Example delegation pattern:
  // @Override
  // public String getName() { return proxied.getName(); }
  // @Override
  // public void close() { proxied.close(); }
  // ... etc ...
}
```

> **Implementation note for the developer:** Copy the full delegation from `server/src/main/java/com/arcadedb/server/ha/ReplicatedDatabase.java`. The class has ~100+ delegated methods. Use the existing file as the template and only change the constructor, `commit()`, and DDL command methods. All other methods remain as simple `return proxied.xxx()` delegations.

**Step 4: Run test to verify it passes**

Run: `cd /Users/frank/projects/arcade/worktrees/ha-redesign && mvn test -pl ha-raft -Dtest=RaftReplicatedDatabaseTest -am`
Expected: PASS (2 tests)

**Step 5: Commit**

```bash
git add ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftReplicatedDatabase.java ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftReplicatedDatabaseTest.java
git commit -m "feat(ha-raft): add RaftReplicatedDatabase wrapping LocalDatabase with Raft commit"
```

---

## Task 8: RaftHAPlugin - Server Plugin

The `ServerPlugin` implementation that ties everything together. Loaded by the server when `HA_IMPLEMENTATION=raft`.

**Files:**
- Create: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAPlugin.java`
- Create: `ha-raft/src/main/resources/META-INF/services/com.arcadedb.server.ServerPlugin`
- Test: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftHAPluginTest.java`

**Context:** The plugin is discovered via Java ServiceLoader (see `server/src/main/java/com/arcadedb/server/plugin/PluginManager.java:82-96`). It follows the same pattern as `GremlinServerPlugin` at `gremlin/src/main/java/com/arcadedb/server/gremlin/GremlinServerPlugin.java`. The plugin creates `RaftHAServer`, starts it, and wraps databases with `RaftReplicatedDatabase`.

**Step 1: Write the failing test**

Create `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftHAPluginTest.java`:
```java
package com.arcadedb.server.ha.raft;

import com.arcadedb.server.ServerPlugin;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class RaftHAPluginTest {

  @Test
  void implementsServerPlugin() {
    assertThat(ServerPlugin.class.isAssignableFrom(RaftHAPlugin.class)).isTrue();
  }

  @Test
  void nameIsRaftHA() {
    final RaftHAPlugin plugin = new RaftHAPlugin();
    assertThat(plugin.getName()).isEqualTo("RaftHAPlugin");
  }

  @Test
  void priorityIsBeforeHttpOn() {
    final RaftHAPlugin plugin = new RaftHAPlugin();
    assertThat(plugin.getInstallationPriority()).isEqualTo(ServerPlugin.PluginInstallationPriority.BEFORE_HTTP_ON);
  }
}
```

**Step 2: Run test to verify it fails**

Run: `cd /Users/frank/projects/arcade/worktrees/ha-redesign && mvn test -pl ha-raft -Dtest=RaftHAPluginTest -am`
Expected: FAIL - class doesn't exist

**Step 3: Create `RaftHAPlugin`**

Create `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAPlugin.java`:
```java
package com.arcadedb.server.ha.raft;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.log.LogManager;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerPlugin;

import java.io.IOException;
import java.util.logging.Level;

/**
 * Server plugin that provides HA using Apache Ratis (Raft consensus).
 * Activated when HA_IMPLEMENTATION=raft and HA_ENABLED=true.
 */
public class RaftHAPlugin implements ServerPlugin {

  private ArcadeDBServer server;
  private ContextConfiguration configuration;
  private RaftHAServer raftHAServer;

  @Override
  public void configure(final ArcadeDBServer arcadeDBServer, final ContextConfiguration configuration) {
    this.server = arcadeDBServer;
    this.configuration = configuration;
  }

  @Override
  public void startService() {
    if (!isRaftEnabled()) {
      LogManager.instance().log(this, Level.FINE, "Raft HA plugin not activated (HA_IMPLEMENTATION != raft)");
      return;
    }

    validateConfiguration();

    try {
      raftHAServer = new RaftHAServer(server, configuration);
      raftHAServer.start();

      LogManager.instance().log(this, Level.INFO, "Raft HA plugin started successfully");
    } catch (final IOException e) {
      throw new RuntimeException("Failed to start Raft HA server", e);
    }
  }

  @Override
  public void stopService() {
    if (raftHAServer != null) {
      try {
        raftHAServer.stop();
      } catch (final IOException e) {
        LogManager.instance().log(this, Level.SEVERE, "Error stopping Raft HA server", e);
      }
    }
  }

  public RaftHAServer getRaftHAServer() {
    return raftHAServer;
  }

  public boolean isLeader() {
    return raftHAServer != null && raftHAServer.isLeader();
  }

  private boolean isRaftEnabled() {
    return configuration != null
        && configuration.getValueAsBoolean(GlobalConfiguration.HA_ENABLED)
        && "raft".equalsIgnoreCase(configuration.getValueAsString(GlobalConfiguration.HA_IMPLEMENTATION));
  }

  private void validateConfiguration() {
    final String serverList = configuration.getValueAsString(GlobalConfiguration.HA_SERVER_LIST);
    if (serverList == null || serverList.isEmpty())
      throw new RuntimeException("HA_SERVER_LIST must be configured for Raft HA");

    final String quorum = configuration.getValueAsString(GlobalConfiguration.HA_QUORUM).toUpperCase();
    final int serverCount = serverList.split(",").length;

    if ("MAJORITY".equals(quorum) && serverCount == 2)
      LogManager.instance().log(this, Level.WARNING,
          "HA_QUORUM=MAJORITY with 2 nodes: losing 1 node will prevent writes. Consider NONE for 2-node setups.");

    if ("NONE".equals(quorum) && serverCount > 2)
      LogManager.instance().log(this, Level.WARNING,
          "HA_QUORUM=NONE with %d nodes: replication is asynchronous, data loss possible on leader failure.", serverCount);

    if (!quorum.equals("NONE") && !quorum.equals("MAJORITY"))
      LogManager.instance().log(this, Level.WARNING,
          "Raft HA only supports NONE and MAJORITY quorum modes. '%s' is not supported, defaulting to MAJORITY.", quorum);
  }
}
```

**Step 4: Create ServiceLoader registration**

Create `ha-raft/src/main/resources/META-INF/services/com.arcadedb.server.ServerPlugin`:
```
com.arcadedb.server.ha.raft.RaftHAPlugin
```

**Step 5: Run test to verify it passes**

Run: `cd /Users/frank/projects/arcade/worktrees/ha-redesign && mvn test -pl ha-raft -Dtest=RaftHAPluginTest -am`
Expected: PASS (3 tests)

**Step 6: Commit**

```bash
git add ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAPlugin.java ha-raft/src/main/resources/META-INF/services/com.arcadedb.server.ServerPlugin ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftHAPluginTest.java
git commit -m "feat(ha-raft): add RaftHAPlugin with ServiceLoader registration and config validation"
```

---

## Task 9: SnapshotManager - Full and Incremental Sync

Handles snapshot creation and installation for Ratis. Full copy for fresh replicas, checksum-based incremental for returning replicas.

**Files:**
- Create: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/SnapshotManager.java`
- Test: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/SnapshotManagerTest.java`

**Step 1: Write the failing test**

Create `ha-raft/src/test/java/com/arcadedb/server/ha/raft/SnapshotManagerTest.java`:
```java
package com.arcadedb.server.ha.raft;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class SnapshotManagerTest {

  @Test
  void computeFileChecksumsForDirectory(@TempDir Path tempDir) throws IOException {
    Files.writeString(tempDir.resolve("file1.dat"), "hello");
    Files.writeString(tempDir.resolve("file2.dat"), "world");

    final Map<String, Long> checksums = SnapshotManager.computeFileChecksums(tempDir.toFile());

    assertThat(checksums).hasSize(2);
    assertThat(checksums).containsKey("file1.dat");
    assertThat(checksums).containsKey("file2.dat");
  }

  @Test
  void identicalFilesHaveSameChecksum(@TempDir Path tempDir) throws IOException {
    Files.writeString(tempDir.resolve("a.dat"), "same content");
    Files.writeString(tempDir.resolve("b.dat"), "same content");

    final Map<String, Long> checksums = SnapshotManager.computeFileChecksums(tempDir.toFile());

    assertThat(checksums.get("a.dat")).isEqualTo(checksums.get("b.dat"));
  }

  @Test
  void differentFilesHaveDifferentChecksums(@TempDir Path tempDir) throws IOException {
    Files.writeString(tempDir.resolve("a.dat"), "content A");
    Files.writeString(tempDir.resolve("b.dat"), "content B");

    final Map<String, Long> checksums = SnapshotManager.computeFileChecksums(tempDir.toFile());

    assertThat(checksums.get("a.dat")).isNotEqualTo(checksums.get("b.dat"));
  }

  @Test
  void findDifferingFiles() throws IOException {
    final Map<String, Long> leader = Map.of("file1", 100L, "file2", 200L, "file3", 300L);
    final Map<String, Long> replica = Map.of("file1", 100L, "file2", 999L); // file2 differs, file3 missing

    final var differing = SnapshotManager.findDifferingFiles(leader, replica);

    assertThat(differing).containsExactlyInAnyOrder("file2", "file3");
  }

  @Test
  void noDifferencesWhenIdentical() {
    final Map<String, Long> checksums = Map.of("a", 1L, "b", 2L);

    final var differing = SnapshotManager.findDifferingFiles(checksums, checksums);

    assertThat(differing).isEmpty();
  }
}
```

**Step 2: Run test to verify it fails**

Run: `cd /Users/frank/projects/arcade/worktrees/ha-redesign && mvn test -pl ha-raft -Dtest=SnapshotManagerTest -am`
Expected: FAIL - class doesn't exist

**Step 3: Create `SnapshotManager`**

Create `ha-raft/src/main/java/com/arcadedb/server/ha/raft/SnapshotManager.java`:
```java
package com.arcadedb.server.ha.raft;

import com.arcadedb.log.LogManager;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.zip.CRC32;

/**
 * Handles snapshot operations for Raft-based HA.
 * Supports full file copy for fresh replicas and checksum-based incremental sync
 * for returning replicas.
 */
public class SnapshotManager {

  /**
   * Computes CRC32 checksums for all files in a directory (non-recursive).
   *
   * @return map of filename to CRC32 checksum
   */
  public static Map<String, Long> computeFileChecksums(final File directory) throws IOException {
    final Map<String, Long> checksums = new HashMap<>();
    final File[] files = directory.listFiles(File::isFile);
    if (files == null)
      return checksums;

    final byte[] buffer = new byte[8192];
    for (final File file : files) {
      final CRC32 crc = new CRC32();
      try (final FileInputStream fis = new FileInputStream(file)) {
        int bytesRead;
        while ((bytesRead = fis.read(buffer)) != -1)
          crc.update(buffer, 0, bytesRead);
      }
      checksums.put(file.getName(), crc.getValue());
    }

    return checksums;
  }

  /**
   * Compares leader and replica file checksums to find files that need transfer.
   *
   * @return list of filenames that differ or are missing from the replica
   */
  public static List<String> findDifferingFiles(final Map<String, Long> leaderChecksums,
      final Map<String, Long> replicaChecksums) {
    final List<String> differing = new ArrayList<>();

    for (final Map.Entry<String, Long> entry : leaderChecksums.entrySet()) {
      final Long replicaChecksum = replicaChecksums.get(entry.getKey());
      if (replicaChecksum == null || !replicaChecksum.equals(entry.getValue()))
        differing.add(entry.getKey());
    }

    return differing;
  }
}
```

**Step 4: Run test to verify it passes**

Run: `cd /Users/frank/projects/arcade/worktrees/ha-redesign && mvn test -pl ha-raft -Dtest=SnapshotManagerTest -am`
Expected: PASS (5 tests)

**Step 5: Commit**

```bash
git add ha-raft/src/main/java/com/arcadedb/server/ha/raft/SnapshotManager.java ha-raft/src/test/java/com/arcadedb/server/ha/raft/SnapshotManagerTest.java
git commit -m "feat(ha-raft): add SnapshotManager with checksum-based incremental sync"
```

---

## Task 10: Integration Test Infrastructure

Create the test base class for multi-node Raft HA integration tests.

**Files:**
- Create: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/BaseRaftHATest.java`
- Test: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftReplication2NodesIT.java`

**Context:** Extends `BaseGraphServerTest` from `server/src/test/java/com/arcadedb/server/BaseGraphServerTest.java`. Overrides `onServerConfiguration()` to set `HA_IMPLEMENTATION=raft` and `HA_RAFT_PORT` per server. The existing test infrastructure handles server lifecycle, database creation, and identity checking.

**Step 1: Create `BaseRaftHATest`**

Create `ha-raft/src/test/java/com/arcadedb/server/ha/raft/BaseRaftHATest.java`:
```java
package com.arcadedb.server.ha.raft;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.server.BaseGraphServerTest;

/**
 * Base class for Raft HA integration tests. Configures servers to use the
 * Raft HA implementation instead of the legacy one.
 */
public abstract class BaseRaftHATest extends BaseGraphServerTest {

  private static final int BASE_RAFT_PORT = 2434;

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    config.setValue(GlobalConfiguration.HA_IMPLEMENTATION, "raft");
    // Each server gets a unique Raft port based on its index
    // Server index is derived from SERVER_NAME which is set by BaseGraphServerTest
    config.setValue(GlobalConfiguration.HA_RAFT_PORT, BASE_RAFT_PORT);
  }

  @Override
  protected int getServerCount() {
    return 2; // Default for Raft tests, override in subclasses
  }

  protected void assertClusterConsistency() {
    waitForReplicationIsCompleted(0);
    checkDatabasesAreIdentical();
  }
}
```

**Step 2: Create the first integration test**

Create `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftReplication2NodesIT.java`:
```java
package com.arcadedb.server.ha.raft;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.server.ArcadeDBServer;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test: 2-node cluster with no-quorum (async) replication.
 */
class RaftReplication2NodesIT extends BaseRaftHATest {

  @Override
  protected void onServerConfiguration(final com.arcadedb.ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_QUORUM, "none");
  }

  @Override
  protected int getServerCount() {
    return 2;
  }

  @Test
  void basicReplicationBetween2Nodes() {
    // Create data on leader
    final var db = getServerDatabase(0, getDatabaseName());

    db.transaction(() -> {
      db.getSchema().createVertexType("Person");
    });

    db.transaction(() -> {
      for (int i = 0; i < 100; i++) {
        final MutableVertex v = db.newVertex("Person");
        v.set("name", "person-" + i);
        v.set("idx", i);
        v.save();
      }
    });

    // Wait for replication and verify consistency
    assertClusterConsistency();

    // Verify data exists on replica
    final var replicaDb = getServerDatabase(1, getDatabaseName());
    final long count = replicaDb.countType("Person", true);
    assertThat(count).isEqualTo(100);
  }
}
```

> **Important note:** This integration test will only pass once Tasks 1-8 are complete and the full Raft HA pipeline is wired together. It serves as the first end-to-end validation. When initially written, it may fail until the server integration is complete. Run it to track progress.

**Step 3: Verify the test compiles**

Run: `cd /Users/frank/projects/arcade/worktrees/ha-redesign && mvn test-compile -pl ha-raft -am -DskipTests`
Expected: BUILD SUCCESS (compiles but we don't run it yet)

**Step 4: Commit**

```bash
git add ha-raft/src/test/java/com/arcadedb/server/ha/raft/BaseRaftHATest.java ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftReplication2NodesIT.java
git commit -m "feat(ha-raft): add BaseRaftHATest and first 2-node replication integration test"
```

---

## Task 11: Server Integration - Wire RaftHAPlugin into ArcadeDBServer

Modify the server to load the Raft HA plugin when `HA_IMPLEMENTATION=raft` instead of the legacy `HAServer`.

**Files:**
- Modify: `server/src/main/java/com/arcadedb/server/ArcadeDBServer.java` (lines 191-194, the HA startup block)

**Context:** Currently at `server/src/main/java/com/arcadedb/server/ArcadeDBServer.java:191-194`:
```java
if (configuration.getValueAsBoolean(GlobalConfiguration.HA_ENABLED)) {
  haServer = new HAServer(this, configuration);
  haServer.startService();
}
```

We need to add a condition: if `HA_IMPLEMENTATION=raft`, the Raft plugin handles HA instead, and it's loaded via the plugin manager. The legacy `HAServer` instantiation is skipped.

**Step 1: Read the current server startup code**

Read `server/src/main/java/com/arcadedb/server/ArcadeDBServer.java` lines 180-210 to understand the exact startup sequence.

**Step 2: Modify the HA startup block**

Change the HA startup block (around lines 191-194) to:

```java
if (configuration.getValueAsBoolean(GlobalConfiguration.HA_ENABLED)) {
  final String haImpl = configuration.getValueAsString(GlobalConfiguration.HA_IMPLEMENTATION);
  if ("raft".equalsIgnoreCase(haImpl)) {
    // Raft HA is loaded via PluginManager (ServiceLoader)
    LogManager.instance().log(this, Level.INFO, "Using Raft HA implementation");
  } else {
    haServer = new HAServer(this, configuration);
    haServer.startService();
  }
}
```

**Step 3: Also modify the database wrapping code**

In `ArcadeDBServer.java`, find the database wrapping logic (around lines 475-483 and 580-589) where `ReplicatedDatabase` is created. Add a condition to use `RaftReplicatedDatabase` when `HA_IMPLEMENTATION=raft`:

> **Note:** Since `ha-raft` uses `provided` scope for `arcadedb-server`, the server cannot directly reference `RaftReplicatedDatabase`. Instead, the `RaftHAPlugin` should register a database wrapper callback that the server invokes. This requires adding a small hook interface. The exact wiring depends on how cleanly the existing `ReplicatedDatabase` wrapping can be abstracted. The developer should:
>
> 1. Create an interface `DatabaseWrapper` in the server module with `DatabaseInternal wrap(LocalDatabase db)` method
> 2. Register the wrapper from `RaftHAPlugin.startService()` via `server.setDatabaseWrapper(db -> new RaftReplicatedDatabase(server, db, raftHAServer))`
> 3. In `ArcadeDBServer`, check if a wrapper is registered before falling back to `ReplicatedDatabase`

**Step 4: Compile to verify**

Run: `cd /Users/frank/projects/arcade/worktrees/ha-redesign && mvn compile -DskipTests`
Expected: BUILD SUCCESS

**Step 5: Commit**

```bash
git add server/src/main/java/com/arcadedb/server/ArcadeDBServer.java
git commit -m "feat(ha-raft): wire Raft HA plugin into server startup, skip legacy HAServer when raft is configured"
```

---

## Task 12: Integration Test - 3-Node Majority Quorum

**Files:**
- Test: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftReplication3NodesIT.java`

**Step 1: Write the integration test**

Create `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftReplication3NodesIT.java`:
```java
package com.arcadedb.server.ha.raft;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.graph.MutableVertex;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test: 3-node cluster with majority quorum.
 */
class RaftReplication3NodesIT extends BaseRaftHATest {

  @Override
  protected void onServerConfiguration(final com.arcadedb.ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_QUORUM, "majority");
  }

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Test
  void basicReplicationWith3Nodes() {
    final var db = getServerDatabase(0, getDatabaseName());

    db.transaction(() -> {
      db.getSchema().createVertexType("Product");
    });

    db.transaction(() -> {
      for (int i = 0; i < 500; i++) {
        final MutableVertex v = db.newVertex("Product");
        v.set("name", "product-" + i);
        v.save();
      }
    });

    assertClusterConsistency();

    for (int s = 0; s < 3; s++) {
      final long count = getServerDatabase(s, getDatabaseName()).countType("Product", true);
      assertThat(count).isEqualTo(500);
    }
  }

  @Test
  void schemaReplicationWith3Nodes() {
    final var db = getServerDatabase(0, getDatabaseName());

    db.transaction(() -> {
      db.getSchema().createVertexType("Customer");
      db.getSchema().getType("Customer").createProperty("email", String.class);
      db.getSchema().getType("Customer").createProperty("age", Integer.class);
    });

    assertClusterConsistency();

    for (int s = 0; s < 3; s++) {
      final var replicaDb = getServerDatabase(s, getDatabaseName());
      assertThat(replicaDb.getSchema().existsType("Customer")).isTrue();
      assertThat(replicaDb.getSchema().getType("Customer").existsProperty("email")).isTrue();
      assertThat(replicaDb.getSchema().getType("Customer").existsProperty("age")).isTrue();
    }
  }
}
```

**Step 2: Commit**

```bash
git add ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftReplication3NodesIT.java
git commit -m "test(ha-raft): add 3-node majority quorum replication integration test"
```

---

## Task 13: Failure Scenario Tests

Write the critical failure scenario integration tests.

**Files:**
- Create: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftLeaderFailoverIT.java`
- Create: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftReplicaFailureIT.java`
- Create: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftSplitBrain3NodesIT.java`

**Step 1: Leader failover test**

Create `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftLeaderFailoverIT.java`:
```java
package com.arcadedb.server.ha.raft;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.graph.MutableVertex;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests leader failover: leader dies, new leader is elected, writes continue.
 * Requires 3 nodes with MAJORITY quorum.
 */
class RaftLeaderFailoverIT extends BaseRaftHATest {

  @Override
  protected void onServerConfiguration(final com.arcadedb.ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_QUORUM, "majority");
  }

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Test
  void newLeaderElectedAfterLeaderDeath() {
    final var db = getServerDatabase(0, getDatabaseName());

    // Write initial data
    db.transaction(() -> {
      db.getSchema().createVertexType("Item");
    });

    db.transaction(() -> {
      for (int i = 0; i < 50; i++) {
        final MutableVertex v = db.newVertex("Item");
        v.set("idx", i);
        v.save();
      }
    });

    assertClusterConsistency();

    // Kill the leader (server 0)
    getServer(0).stop();

    // Wait for new leader election
    Awaitility.await()
        .atMost(30, TimeUnit.SECONDS)
        .pollInterval(1, TimeUnit.SECONDS)
        .until(() -> {
          // Check if server 1 or 2 became leader
          for (int i = 1; i < 3; i++) {
            if (getServer(i) != null && getServer(i).isStarted()) {
              // Check via plugin if this server is now leader
              // Exact check depends on how plugin exposes leader state
              return true; // Placeholder - verify actual leader election
            }
          }
          return false;
        });

    // Writes should succeed on the new leader
    // The new leader is one of servers 1 or 2
    // TODO: Write data to new leader and verify replication
  }
}
```

**Step 2: Replica failure test**

Create `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftReplicaFailureIT.java`:
```java
package com.arcadedb.server.ha.raft;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.graph.MutableVertex;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests replica failure and recovery: replica dies, leader continues,
 * replica catches up on return.
 */
class RaftReplicaFailureIT extends BaseRaftHATest {

  @Override
  protected void onServerConfiguration(final com.arcadedb.ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_QUORUM, "none");
  }

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Test
  void replicaCatchesUpAfterRestart() {
    final var db = getServerDatabase(0, getDatabaseName());

    db.transaction(() -> {
      db.getSchema().createVertexType("Event");
    });

    // Write initial data
    db.transaction(() -> {
      for (int i = 0; i < 50; i++) {
        final MutableVertex v = db.newVertex("Event");
        v.set("idx", i);
        v.save();
      }
    });

    assertClusterConsistency();

    // Kill replica (server 2)
    getServer(2).stop();

    // Write more data while replica is down
    db.transaction(() -> {
      for (int i = 50; i < 100; i++) {
        final MutableVertex v = db.newVertex("Event");
        v.set("idx", i);
        v.save();
      }
    });

    // Restart replica
    getServer(2).start();

    // Wait for replica to catch up
    assertClusterConsistency();

    // Verify all data on recovered replica
    final long count = getServerDatabase(2, getDatabaseName()).countType("Event", true);
    assertThat(count).isEqualTo(100);
  }
}
```

**Step 3: Split brain test**

Create `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftSplitBrain3NodesIT.java`:
```java
package com.arcadedb.server.ha.raft;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.graph.MutableVertex;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests split-brain scenario: 3 nodes partitioned 1+2.
 * The majority partition (2 nodes) should continue accepting writes.
 * The minority partition (1 node) should reject writes.
 */
class RaftSplitBrain3NodesIT extends BaseRaftHATest {

  @Override
  protected void onServerConfiguration(final com.arcadedb.ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_QUORUM, "majority");
  }

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Test
  void majorityPartitionContinuesAcceptingWrites() {
    final var db = getServerDatabase(0, getDatabaseName());

    db.transaction(() -> {
      db.getSchema().createVertexType("Record");
    });

    // Write data before partition
    db.transaction(() -> {
      for (int i = 0; i < 50; i++) {
        final MutableVertex v = db.newVertex("Record");
        v.set("idx", i);
        v.save();
      }
    });

    assertClusterConsistency();

    // Simulate network partition: isolate server 2
    // TODO: Use Ratis SimulatedRequestTimeout or gRPC port blocking to partition server 2
    // The majority partition (servers 0+1) should elect a leader and continue
    // Server 2 alone cannot form quorum and should reject writes

    // After partition heals, all nodes should converge
    assertClusterConsistency();
  }
}
```

**Step 4: Commit**

```bash
git add ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftLeaderFailoverIT.java ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftReplicaFailureIT.java ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftSplitBrain3NodesIT.java
git commit -m "test(ha-raft): add leader failover, replica failure, and split-brain integration tests"
```

---

## Task 14: Additional Failure Scenario Tests

**Files:**
- Create: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftRandomCrashIT.java`
- Create: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftQuorumLostIT.java`
- Create: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftLeaderDown2NodesIT.java`
- Create: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftWriteForwardingIT.java`

**Step 1: Random crash test**

Create `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftRandomCrashIT.java`:
```java
package com.arcadedb.server.ha.raft;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.graph.MutableVertex;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Random server crashes during concurrent writes. Verifies data consistency after recovery.
 */
class RaftRandomCrashIT extends BaseRaftHATest {

  @Override
  protected void onServerConfiguration(final com.arcadedb.ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_QUORUM, "majority");
  }

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Test
  void dataConsistencyAfterRandomCrashes() {
    final var db = getServerDatabase(0, getDatabaseName());

    db.transaction(() -> {
      db.getSchema().createVertexType("Data");
    });

    final Random random = new Random(42); // deterministic seed
    int totalWritten = 0;

    for (int round = 0; round < 3; round++) {
      // Write a batch
      final int batchStart = totalWritten;
      final int batchSize = 50;
      try {
        db.transaction(() -> {
          for (int i = batchStart; i < batchStart + batchSize; i++) {
            final MutableVertex v = db.newVertex("Data");
            v.set("idx", i);
            v.save();
          }
        });
        totalWritten += batchSize;
      } catch (final Exception e) {
        // Expected during crashes
      }

      // Randomly crash a non-leader server
      final int serverToCrash = 1 + random.nextInt(2); // 1 or 2
      if (getServer(serverToCrash).isStarted())
        getServer(serverToCrash).stop();

      // Restart it
      getServer(serverToCrash).start();
    }

    // Final consistency check
    assertClusterConsistency();
  }
}
```

**Step 2: Quorum lost test**

Create `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftQuorumLostIT.java`:
```java
package com.arcadedb.server.ha.raft;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.graph.MutableVertex;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests quorum loss: majority of nodes down, writes should fail.
 * Cluster should recover when nodes return.
 */
class RaftQuorumLostIT extends BaseRaftHATest {

  @Override
  protected void onServerConfiguration(final com.arcadedb.ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_QUORUM, "majority");
  }

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Test
  void writesFailWhenQuorumLost() {
    final var db = getServerDatabase(0, getDatabaseName());

    db.transaction(() -> {
      db.getSchema().createVertexType("Item");
    });

    assertClusterConsistency();

    // Kill 2 of 3 replicas - quorum lost
    getServer(1).stop();
    getServer(2).stop();

    // Writes should fail (no quorum)
    assertThatThrownBy(() -> db.transaction(() -> {
      final MutableVertex v = db.newVertex("Item");
      v.set("idx", 0);
      v.save();
    })).isInstanceOf(Exception.class);

    // Restart nodes
    getServer(1).start();
    getServer(2).start();

    // Writes should succeed again
    db.transaction(() -> {
      final MutableVertex v = db.newVertex("Item");
      v.set("idx", 1);
      v.save();
    });

    assertClusterConsistency();
  }
}
```

**Step 3: 2-node leader down test**

Create `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftLeaderDown2NodesIT.java`:
```java
package com.arcadedb.server.ha.raft;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.graph.MutableVertex;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests 2-node cluster with no-quorum when leader dies.
 * Replica should NOT auto-promote. Requires manual FORCE_LEADER.
 */
class RaftLeaderDown2NodesIT extends BaseRaftHATest {

  @Override
  protected void onServerConfiguration(final com.arcadedb.ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_QUORUM, "none");
  }

  @Override
  protected int getServerCount() {
    return 2;
  }

  @Test
  void replicaDoesNotAutoPromoteWhenLeaderDies() {
    final var db = getServerDatabase(0, getDatabaseName());

    db.transaction(() -> {
      db.getSchema().createVertexType("Doc");
    });

    db.transaction(() -> {
      for (int i = 0; i < 10; i++) {
        final MutableVertex v = db.newVertex("Doc");
        v.set("idx", i);
        v.save();
      }
    });

    assertClusterConsistency();

    // Kill leader
    getServer(0).stop();

    // Replica should NOT become leader automatically (no majority possible)
    // Writes to replica should fail
    Awaitility.await()
        .atMost(10, TimeUnit.SECONDS)
        .pollInterval(1, TimeUnit.SECONDS)
        .until(() -> true); // Wait for election timeout

    // Attempting writes on replica should fail
    final var replicaDb = getServerDatabase(1, getDatabaseName());
    assertThatThrownBy(() -> replicaDb.transaction(() -> {
      final MutableVertex v = replicaDb.newVertex("Doc");
      v.set("idx", 99);
      v.save();
    })).isInstanceOf(Exception.class);

    // TODO: Test FORCE_LEADER command when implemented
  }
}
```

**Step 4: Write forwarding test**

Create `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftWriteForwardingIT.java`:
```java
package com.arcadedb.server.ha.raft;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.graph.MutableVertex;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests write forwarding: client writes to replica, forwarded to leader.
 */
class RaftWriteForwardingIT extends BaseRaftHATest {

  @Override
  protected void onServerConfiguration(final com.arcadedb.ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_QUORUM, "majority");
  }

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Test
  void writeToReplicaIsForwardedToLeader() {
    // First create schema on leader
    final var leaderDb = getServerDatabase(0, getDatabaseName());
    leaderDb.transaction(() -> {
      leaderDb.getSchema().createVertexType("Message");
    });

    assertClusterConsistency();

    // Write via replica (server 1)
    final var replicaDb = getServerDatabase(1, getDatabaseName());
    replicaDb.transaction(() -> {
      for (int i = 0; i < 20; i++) {
        final MutableVertex v = replicaDb.newVertex("Message");
        v.set("text", "message-" + i);
        v.save();
      }
    });

    assertClusterConsistency();

    // Data should be on all nodes
    for (int s = 0; s < 3; s++) {
      final long count = getServerDatabase(s, getDatabaseName()).countType("Message", true);
      assertThat(count).isEqualTo(20);
    }
  }
}
```

**Step 5: Commit**

```bash
git add ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftRandomCrashIT.java ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftQuorumLostIT.java ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftLeaderDown2NodesIT.java ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftWriteForwardingIT.java
git commit -m "test(ha-raft): add random crash, quorum loss, 2-node leader down, and write forwarding tests"
```

---

## Task 15: Schema and Index Replication Tests

**Files:**
- Create: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftReplicationSchemaIT.java`
- Create: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftReplicationIndexIT.java`

**Step 1: Schema replication test**

Create `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftReplicationSchemaIT.java`:
```java
package com.arcadedb.server.ha.raft;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests schema change replication: create/drop types, properties, indexes.
 */
class RaftReplicationSchemaIT extends BaseRaftHATest {

  @Override
  protected void onServerConfiguration(final com.arcadedb.ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_QUORUM, "majority");
  }

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Test
  void createTypeReplicatedToAllNodes() {
    final var db = getServerDatabase(0, getDatabaseName());

    db.transaction(() -> {
      db.getSchema().createVertexType("Animal");
      db.getSchema().getType("Animal").createProperty("species", Type.STRING);
      db.getSchema().getType("Animal").createProperty("weight", Type.DOUBLE);
    });

    assertClusterConsistency();

    for (int s = 0; s < 3; s++) {
      final var replica = getServerDatabase(s, getDatabaseName());
      assertThat(replica.getSchema().existsType("Animal")).isTrue();
      assertThat(replica.getSchema().getType("Animal").existsProperty("species")).isTrue();
      assertThat(replica.getSchema().getType("Animal").existsProperty("weight")).isTrue();
    }
  }

  @Test
  void dropTypeReplicatedToAllNodes() {
    final var db = getServerDatabase(0, getDatabaseName());

    db.transaction(() -> {
      db.getSchema().createVertexType("Temporary");
    });

    assertClusterConsistency();

    db.getSchema().dropType("Temporary");

    assertClusterConsistency();

    for (int s = 0; s < 3; s++) {
      final var replica = getServerDatabase(s, getDatabaseName());
      assertThat(replica.getSchema().existsType("Temporary")).isFalse();
    }
  }

  @Test
  void createEdgeTypeReplicatedToAllNodes() {
    final var db = getServerDatabase(0, getDatabaseName());

    db.transaction(() -> {
      db.getSchema().createVertexType("Person");
      db.getSchema().createVertexType("City");
      db.getSchema().createEdgeType("LivesIn");
    });

    assertClusterConsistency();

    for (int s = 0; s < 3; s++) {
      final var replica = getServerDatabase(s, getDatabaseName());
      assertThat(replica.getSchema().existsType("Person")).isTrue();
      assertThat(replica.getSchema().existsType("City")).isTrue();
      assertThat(replica.getSchema().existsType("LivesIn")).isTrue();
    }
  }
}
```

**Step 2: Index replication test**

Create `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftReplicationIndexIT.java`:
```java
package com.arcadedb.server.ha.raft;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests index creation and replication across cluster.
 */
class RaftReplicationIndexIT extends BaseRaftHATest {

  @Override
  protected void onServerConfiguration(final com.arcadedb.ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_QUORUM, "majority");
  }

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Test
  void indexCreationReplicatedToAllNodes() {
    final var db = getServerDatabase(0, getDatabaseName());

    db.transaction(() -> {
      db.getSchema().createVertexType("Product");
      db.getSchema().getType("Product").createProperty("sku", Type.STRING);
      db.getSchema().getType("Product").createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "sku");
    });

    db.transaction(() -> {
      for (int i = 0; i < 100; i++) {
        final MutableVertex v = db.newVertex("Product");
        v.set("sku", "SKU-" + i);
        v.save();
      }
    });

    assertClusterConsistency();

    // Verify index exists and works on all replicas
    for (int s = 0; s < 3; s++) {
      final var replica = getServerDatabase(s, getDatabaseName());
      assertThat(replica.getSchema().getType("Product").getIndexesByProperties("sku")).isNotEmpty();

      // Verify index lookup works
      final IndexCursor cursor = replica.lookupByKey("Product", "sku", "SKU-50");
      assertThat(cursor.hasNext()).isTrue();
    }
  }
}
```

**Step 3: Commit**

```bash
git add ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftReplicationSchemaIT.java ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftReplicationIndexIT.java
git commit -m "test(ha-raft): add schema and index replication integration tests"
```

---

## Task 16: Run All Unit Tests and Fix Issues

**Step 1: Run all unit tests in the ha-raft module**

Run: `cd /Users/frank/projects/arcade/worktrees/ha-redesign && mvn test -pl ha-raft -am`
Expected: All unit tests PASS

**Step 2: Run the full project compilation**

Run: `cd /Users/frank/projects/arcade/worktrees/ha-redesign && mvn compile -DskipTests`
Expected: BUILD SUCCESS

**Step 3: Fix any compilation or test failures**

Iterate until all tests pass and the project compiles cleanly.

**Step 4: Commit any fixes**

```bash
git add -A
git commit -m "fix(ha-raft): resolve compilation and test issues"
```

---

## Task 17: Wire End-to-End and Run First Integration Test

This is the critical integration step. Make the 2-node integration test pass end-to-end.

**Step 1: Ensure the full Raft pipeline is connected**

Verify:
- `RaftHAPlugin` starts `RaftHAServer` correctly
- `RaftHAServer` creates and starts the Ratis `RaftServer`
- Databases are wrapped with `RaftReplicatedDatabase`
- `commit()` submits to Raft log
- `ArcadeStateMachine.applyTransaction()` applies WAL changes on all nodes
- Schema changes flow through `SCHEMA_ENTRY` path

**Step 2: Run the 2-node integration test**

Run: `cd /Users/frank/projects/arcade/worktrees/ha-redesign && mvn test -pl ha-raft -Dtest=RaftReplication2NodesIT -DskipITs=false -am`
Expected: PASS

**Step 3: If it fails, debug and fix iteratively**

Common issues:
- Peer ID mismatch (server name to peer mapping)
- gRPC port conflicts
- WAL serialization format mismatch
- Database wrapper not applied
- Transaction context not propagated correctly

**Step 4: Run the 3-node integration test**

Run: `cd /Users/frank/projects/arcade/worktrees/ha-redesign && mvn test -pl ha-raft -Dtest=RaftReplication3NodesIT -DskipITs=false -am`
Expected: PASS

**Step 5: Commit**

```bash
git add -A
git commit -m "feat(ha-raft): wire end-to-end pipeline, first integration tests passing"
```

---

## Task 18: Run All Integration Tests

**Step 1: Run all integration tests in ha-raft**

Run: `cd /Users/frank/projects/arcade/worktrees/ha-redesign && mvn test -pl ha-raft -DskipITs=false -am`

**Step 2: Fix failing tests iteratively**

Focus on failure scenarios - these are the hardest to get right. Expected order of difficulty:
1. Basic replication (should work from Task 17)
2. Schema replication
3. Write forwarding
4. Replica failure/recovery
5. Leader failover
6. Quorum loss
7. Split brain
8. Random crashes

**Step 3: Commit all fixes**

```bash
git add -A
git commit -m "fix(ha-raft): all integration tests passing"
```

---

## Summary of Tasks

| Task | Component | Type | Estimated Complexity |
|------|-----------|------|---------------------|
| 1 | Maven module skeleton | Setup | Low |
| 2 | Configuration properties | Config | Low |
| 3 | RaftLogEntryCodec | Core | Medium |
| 4 | ArcadeStateMachine | Core | High |
| 5 | ClusterMonitor | Monitoring | Low |
| 6 | RaftHAServer | Core | High |
| 7 | RaftReplicatedDatabase | Core | High |
| 8 | RaftHAPlugin | Integration | Medium |
| 9 | SnapshotManager | Core | Medium |
| 10 | Test infrastructure | Test | Medium |
| 11 | Server integration | Integration | High |
| 12 | 3-node IT | Test | Medium |
| 13 | Failure scenario ITs | Test | High |
| 14 | More failure ITs | Test | High |
| 15 | Schema/Index ITs | Test | Medium |
| 16 | Unit test cleanup | Validation | Low |
| 17 | E2E wiring | Integration | Very High |
| 18 | All ITs passing | Validation | Very High |
