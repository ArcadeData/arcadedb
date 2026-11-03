/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.server.TestServerHelper;
import com.arcadedb.utility.FileUtils;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.proto.RaftProtos.StateMachineLogEntryProto;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #11240: an LSM compaction on an HA leader persists its swap - the schema naming the new files, the drop of the files
 * they replace - only once the entry publishing it is accepted. It used to persist first, so a leader that crashed before the
 * publication, or whose entry was truncated by a leadership change after a publication that timed out, came back with a
 * layout no other node had, and its index silently missed what the next leader wrote into the files it had dropped.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CompactionPersistedOnlyOncePublishedTest {
  private static final String DB         = "compaction";
  private static final String INDEX_NAME = "Work[id]";
  private static final int    RECORDS    = 4_000;
  private static final int    PAGE_SIZE  = 16 * 1024;
  private static final long   TERM       = 7L;

  @TempDir
  Path tempDir;

  private Path          leaderPath;
  private LocalDatabase leaderDb;
  private LocalDatabase followerDb;

  @BeforeEach
  void setUp() throws IOException {
    GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE.setValue(0);
    leaderPath = tempDir.resolve("leader").resolve(DB);
    final Path followerPath = tempDir.resolve("follower").resolve(DB);

    leaderDb = (LocalDatabase) new DatabaseFactory(leaderPath.toString()).create();
    final DocumentType type = leaderDb.getSchema().buildDocumentType().withName("Work").withTotalBuckets(1).create();
    type.createProperty("id", Type.STRING);
    leaderDb.getSchema().buildTypeIndex("Work", new String[] { "id" }).withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(true)
        .withPageSize(PAGE_SIZE).create();
    for (int from = 0; from < RECORDS; from += 500) {
      final int start = from;
      leaderDb.transaction(() -> {
        for (int i = start; i < start + 500; i++)
          leaderDb.newDocument("Work").set("id", "k" + i).save();
      });
    }
    leaderDb.close();

    FileUtils.copyDirectory(leaderPath.toFile(), followerPath.toFile());
    leaderDb = (LocalDatabase) new DatabaseFactory(leaderPath.toString()).open();
    followerDb = (LocalDatabase) new DatabaseFactory(followerPath.toString()).open();
  }

  @AfterEach
  void tearDown() {
    try {
      if (leaderDb != null && leaderDb.isOpen())
        leaderDb.drop();
      if (followerDb != null && followerDb.isOpen())
        followerDb.drop();
    } finally {
      GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE.reset();
    }
  }

  /** The crash window: when the entry goes out, nothing of the swap is on the leader's disk yet. */
  @Test
  void nothingOfTheSwapIsPersistedBeforeTheEntryIsPublished() throws IOException, InterruptedException {
    final Set<Integer> filesBefore = indexFileIds(leaderDb);
    final String schemaBefore = schemaOnDisk();
    final boolean[] checked = { false };

    final FakeRaftTransactionBroker broker = new FakeRaftTransactionBroker();
    broker.on("replicateSchema", args -> {
      assertThat(schemaOnDisk()).as("schema.json while the entry is being published").isEqualTo(schemaBefore);
      for (final int fileId : filesBefore)
        assertThat(leaderDb.getFileManager().existsFile(fileId)).as("replaced file %d while publishing", fileId).isTrue();
      @SuppressWarnings("unchecked")
      final Map<Integer, String> removed = (Map<Integer, String>) args[3];
      assertThat(removed.keySet()).as("the entry announces the drop of the replaced files").containsAll(filesBefore);
      checked[0] = true;
      return null;
    });
    final ArcadeStateMachine leaderStateMachine = leaderOver(broker);

    final TypeIndex leaderIndex = (TypeIndex) leaderDb.getSchema().getIndexByName(INDEX_NAME);
    leaderIndex.scheduleCompaction();
    assertThat(leaderIndex.compact()).isTrue();
    assertThat(checked[0]).as("the compaction reached the publish step").isTrue();
    assertThat(leaderStateMachine.isDatabaseDiverged(DB)).isFalse();

    // published: now persisted
    assertThat(schemaOnDisk()).as("schema.json once published").isNotEqualTo(schemaBefore);
    for (final int fileId : filesBefore)
      assertThat(leaderDb.getFileManager().existsFile(fileId)).as("replaced file %d once published", fileId).isFalse();

    final Set<Integer> filesAfter = indexFileIds(leaderDb);
    leaderDb.close();
    leaderDb = (LocalDatabase) new DatabaseFactory(leaderPath.toString()).open();
    assertThat(indexFileIds(leaderDb)).as("the published layout survives a restart").isEqualTo(filesAfter);
    assertThat(leaderDb.getSchema().getIndexByName(INDEX_NAME).countEntries()).isEqualTo(RECORDS);
  }

  /**
   * The entry times out and is then truncated by a leadership change (an entry of a later term is applied first): the
   * leader is quarantined, nothing of the swap reached its disk, and after a restart its index takes the new leader's writes.
   */
  @Test
  void aPublicationThatTimesOutAndIsTruncatedQuarantinesTheLeaderAndPersistsNothing() throws IOException {
    final Set<Integer> filesBefore = indexFileIds(followerDb);
    final String schemaBefore = schemaOnDisk();

    final ArcadeStateMachine[] leaderStateMachine = new ArcadeStateMachine[1];
    final FakeRaftTransactionBroker broker = new FakeRaftTransactionBroker();
    broker.on("replicateSchema", args -> {
      // the leader is deposed: the next leader's first entry, of a later term, is applied here first
      leaderStateMachine[0].notifyTermIndexUpdated(TERM + 1, 101L);
      throw new ReplicationDispatchedTimeoutException("dispatched, outcome unknown");
    });
    leaderStateMachine[0] = leaderOver(broker);

    final TypeIndex leaderIndex = (TypeIndex) leaderDb.getSchema().getIndexByName(INDEX_NAME);
    leaderIndex.scheduleCompaction();
    assertThatThrownBy(leaderIndex::compact).isInstanceOf(ReplicationDispatchedTimeoutException.class);
    assertThat(leaderStateMachine[0].isDatabaseDiverged(DB)).as("a truncated publication is quarantined").isTrue();

    assertThat(schemaOnDisk()).as("schema.json after the truncated publication").isEqualTo(schemaBefore);
    for (final int fileId : filesBefore)
      assertThat(leaderDb.getFileManager().existsFile(fileId)).as("replaced file %d is still on the leader", fileId).isTrue();

    // What a crash leaves behind: the restart comes back on the layout every node holds
    leaderDb.close();
    leaderDb = (LocalDatabase) new DatabaseFactory(leaderPath.toString()).open();
    assertThat(indexFileIds(leaderDb)).as("the leader's index files after the restart").isEqualTo(filesBefore);
    assertThat(leaderDb.getSchema().getIndexByName(INDEX_NAME).countEntries()).isEqualTo(RECORDS);

    // The follower took over and indexes a new record: the old leader, applying it, has the index entry too
    final Captured tx = commitCapturingWal(followerDb, "after-failover");
    final ArcadeStateMachine oldLeaderAsFollower = stateMachineOver(() -> leaderDb);
    final CompletableFuture<Message> applied = oldLeaderAsFollower.applyTransaction(txEntry(oldLeaderAsFollower, tx, 100L));
    assertThat(applied.isCompletedExceptionally()).isFalse();
    assertThat(oldLeaderAsFollower.isDatabaseDiverged(DB)).isFalse();

    assertThat(leaderDb.countType("Work", false)).as("records on the old leader").isEqualTo(RECORDS + 1L);
    final Index oldLeaderIndex = leaderDb.getSchema().getIndexByName(INDEX_NAME);
    assertThat(oldLeaderIndex.get(new Object[] { "after-failover" }).hasNext()).as("the key on the old leader").isTrue();
    assertThat(oldLeaderIndex.countEntries()).as("index entries on the old leader").isEqualTo(RECORDS + 1L);
  }

  /** The entry times out, and the log then commits it: the swap is persisted as for a publication that succeeded. */
  @Test
  void aPublicationThatTimesOutButIsCommittedIsPersisted() throws IOException, InterruptedException {
    final Set<Integer> filesBefore = indexFileIds(leaderDb);
    final String schemaBefore = schemaOnDisk();

    final ArcadeStateMachine[] leaderStateMachine = new ArcadeStateMachine[1];
    final FakeRaftTransactionBroker broker = new FakeRaftTransactionBroker();
    broker.on("replicateSchema", args -> {
      // the entry is committed and applied on this node (its own, so the apply skips it) before the wait gives up
      @SuppressWarnings("unchecked")
      final ByteString payload = RaftLogEntryCodec.encodeSchemaEntry(DB, (String) args[1], (Map<Integer, String>) args[2],
          (Map<Integer, String>) args[3], List.of(), List.of());
      final LogEntryProto logEntry = LogEntryProto.newBuilder().setTerm(TERM).setIndex(100L)
          .setStateMachineLogEntry(StateMachineLogEntryProto.newBuilder().setLogData(payload).build()).build();
      leaderStateMachine[0].applyTransaction(TransactionContext.newBuilder().setStateMachine(leaderStateMachine[0])
          .setLogEntry(logEntry).setStateMachineContext(Boolean.TRUE).build());
      throw new ReplicationDispatchedTimeoutException("dispatched, outcome unknown");
    });
    leaderStateMachine[0] = leaderOver(broker);

    final TypeIndex leaderIndex = (TypeIndex) leaderDb.getSchema().getIndexByName(INDEX_NAME);
    leaderIndex.scheduleCompaction();
    assertThat(leaderIndex.compact()).as("a committed publication is a published compaction").isTrue();
    assertThat(leaderStateMachine[0].isDatabaseDiverged(DB)).isFalse();

    assertThat(schemaOnDisk()).isNotEqualTo(schemaBefore);
    for (final int fileId : filesBefore)
      assertThat(leaderDb.getFileManager().existsFile(fileId)).as("replaced file %d", fileId).isFalse();
    assertThat(leaderDb.getSchema().getIndexByName(INDEX_NAME).countEntries()).isEqualTo(RECORDS);
  }

  private ArcadeStateMachine leaderOver(final FakeRaftTransactionBroker broker) {
    final ArcadeStateMachine leaderStateMachine = stateMachineOver(() -> leaderDb);
    final FakeRaftHAServer raft = FakeRaftHAServer.detached().stateMachine(leaderStateMachine).leader(true).currentTerm(TERM)
        .transactionBroker(broker);
    leaderStateMachine.setRaftHAServer(raft);
    new RaftReplicatedDatabase(TestServerHelper.unstartedServer(), leaderDb, raft);
    return leaderStateMachine;
  }

  private String schemaOnDisk() {
    try {
      return Files.readString(new File(leaderPath.toFile(), LocalSchema.SCHEMA_FILE_NAME).toPath(), StandardCharsets.UTF_8);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static Set<Integer> indexFileIds(final LocalDatabase db) {
    final Set<Integer> ids = new HashSet<>();
    for (final IndexInternal index : ((TypeIndex) db.getSchema().getIndexByName(INDEX_NAME)).getIndexesOnBuckets())
      ids.addAll(index.getFileIds());
    return ids;
  }

  /** Commits one record on {@code db} the way the leader's commit does, and returns the WAL bytes it would replicate. */
  private static Captured commitCapturingWal(final LocalDatabase db, final String key) {
    db.begin();
    db.newDocument("Work").set("id", key).save();
    final var tx = db.getTransaction();
    final var phase1 = tx.commit1stPhase(true);
    final byte[] wal = phase1.result.toByteArray();
    final Map<Integer, Integer> deltas = tx.getBucketRecordDelta();
    tx.commit2ndPhase(phase1);
    return new Captured(wal, deltas);
  }

  private static TransactionContext txEntry(final ArcadeStateMachine sm, final Captured tx, final long index) {
    final ByteString payload = RaftLogEntryCodec.encodeTxEntry(DB, tx.wal, tx.deltas);
    final LogEntryProto logEntry = LogEntryProto.newBuilder().setTerm(TERM + 1).setIndex(index)
        .setStateMachineLogEntry(StateMachineLogEntryProto.newBuilder().setLogData(payload).build()).build();
    return TransactionContext.newBuilder().setStateMachine(sm).setLogEntry(logEntry).build();
  }

  private static ArcadeStateMachine stateMachineOver(final DatabaseSupplier database) {
    return new ArcadeStateMachine() {
      @Override
      DatabaseInternal databaseFor(final String databaseName) {
        return database.get();
      }

      @Override
      ClientId localClientId() {
        return null;
      }

      @Override
      long currentRaftTerm() {
        return TERM;
      }
    };
  }

  @FunctionalInterface
  private interface DatabaseSupplier {
    DatabaseInternal get();
  }

  private record Captured(byte[] wal, Map<Integer, Integer> deltas) {
  }
}
