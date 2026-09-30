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

import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.database.RID;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.exception.ConcurrentModificationException;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeerId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8686: a replica prepares its transaction against ITS schema. When the leader's {@code CREATE INDEX}
 * {@code SCHEMA_ENTRY} is committed but the replica has not applied it yet, the replica ships a {@code TX_ENTRY}
 * whose WAL carries no page changes for the new index. The entry is valid against the page versions - it comes after
 * the DDL in the log - so nothing refused it, every node applied it as it was, and the record was never added to the
 * index anywhere: records N+1, index N, on every node.
 * <p>
 * The leader now refuses such an entry, before Ratis appends it, when the entry says it was prepared at an applied
 * index older than the last schema-changing entry the leader applied. The refusal is retryable and names the index the
 * originator has to reach, so the retry is prepared under the new schema.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8686StaleSchemaTransactionRefusedTest {
  private static final String TYPE = "Counter";

  @TempDir
  Path tempDir;

  private LocalDatabase      db;
  private LocalDatabase      otherDb;
  private ArcadeStateMachine stateMachine;
  private RID                counter;

  @BeforeEach
  void setUp() {
    db = create("issue8686");
    counter = firstCounter(db);
    otherDb = create("issue8686other");
    stateMachine = new ArcadeStateMachine() {
      @Override
      DatabaseInternal databaseFor(final String databaseName) {
        return db.getName().equals(databaseName) ? db : otherDb.getName().equals(databaseName) ? otherDb : null;
      }
    };
  }

  @AfterEach
  void tearDown() {
    if (db != null && db.isOpen())
      db.drop();
    if (otherDb != null && otherDb.isOpen())
      otherDb.drop();
  }

  /** The bug: prepared at 40, the leader applied a schema change at 50, and the entry used to be accepted. */
  @Test
  void anEntryPreparedBeforeTheLastSchemaChangeIsRefusedWithARetryableConflict() throws Exception {
    final byte[] walData = prepareIncrement(db, counter);
    stateMachine.recordSchemaChangeApplied(db.getName(), 50L);

    final org.apache.ratis.statemachine.TransactionContext refused = stateMachine.startTransaction(
        request(db.getName(), walData, 40L, 1));

    assertThat(refused.getException()).isInstanceOf(ReplicatedSchemaConflictException.class)
        .isInstanceOf(ConcurrentModificationException.class);
    final ReplicatedSchemaConflictException conflict = (ReplicatedSchemaConflictException) refused.getException();
    assertThat(conflict.getSchemaIndex()).as("the index the originator has to apply before retrying").isEqualTo(50L);
    assertThat(conflict.getDatabaseName()).isEqualTo(db.getName());
    assertThat(stateMachine.reservedPageVersions(db.getName())).as("a refused entry reserves nothing").isZero();
  }

  /** The refusal survives the trip through Ratis, which rebuilds the exception from its message alone. */
  @Test
  void theRefusalRebuildsFromItsOwnMessage() {
    final ReplicatedSchemaConflictException original = new ReplicatedSchemaConflictException("db", 40L, 50L);

    final ReplicatedSchemaConflictException rebuilt = new ReplicatedSchemaConflictException(original.getMessage());

    assertThat(rebuilt.getSchemaIndex()).isEqualTo(50L);
    assertThat(rebuilt.getDatabaseName()).isEqualTo("db");
  }

  @Test
  void anEntryPreparedAtOrAfterTheSchemaChangeIsAccepted() throws Exception {
    final byte[] walData = prepareIncrement(db, counter);
    stateMachine.recordSchemaChangeApplied(db.getName(), 50L);

    assertThat(stateMachine.startTransaction(request(db.getName(), walData, 50L, 1)).getException())
        .as("applied exactly the schema change: it was prepared under the new schema").isNull();
    stateMachine.releaseReservedVersions(db.getName(), walData);
    assertThat(stateMachine.startTransaction(request(db.getName(), walData, 75L, 2)).getException()).isNull();
  }

  /** No section, no refusal: a peer that predates the field, or one that could not say, must keep working. */
  @Test
  void anEntryThatStatesNoIndexIsNeverRefusedForIt() throws Exception {
    final byte[] walData = prepareIncrement(db, counter);
    stateMachine.recordSchemaChangeApplied(db.getName(), 50L);

    assertThat(stateMachine.startTransaction(request(db.getName(), walData, -1L, 1)).getException()).isNull();
  }

  /** Nothing to compare against until a schema change has been applied on this node: nothing is refused. */
  @Test
  void nothingIsRefusedBeforeAnySchemaChangeWasApplied() throws Exception {
    final byte[] walData = prepareIncrement(db, counter);

    assertThat(stateMachine.startTransaction(request(db.getName(), walData, 0L, 1)).getException()).isNull();
  }

  @Test
  void aSchemaChangeOfAnotherDatabaseRefusesNothing() throws Exception {
    final byte[] walData = prepareIncrement(db, counter);
    stateMachine.recordSchemaChangeApplied(otherDb.getName(), 50L);

    assertThat(stateMachine.startTransaction(request(db.getName(), walData, 10L, 1)).getException()).isNull();
  }

  /**
   * The position only moves forward, so an entry replayed after a restart (an older schema entry re-applied) cannot
   * lower it and let a stale transaction through.
   */
  @Test
  void theRecordedSchemaChangeNeverMovesBackward() throws Exception {
    final byte[] walData = prepareIncrement(db, counter);
    stateMachine.recordSchemaChangeApplied(db.getName(), 50L);
    stateMachine.recordSchemaChangeApplied(db.getName(), 30L);

    assertThat(stateMachine.startTransaction(request(db.getName(), walData, 40L, 1)).getException())
        .isInstanceOf(ReplicatedSchemaConflictException.class);
  }

  /** Which entries count: only the ones that change what a transaction is prepared against. */
  @Test
  void onlyEntriesThatChangeTheSchemaCountAsSchemaChanges() {
    final byte[] wal = { 1 };
    final List<byte[]> walEntries = List.of(wal);
    final List<Map<Integer, Integer>> deltas = List.of(Collections.emptyMap());

    // A whole-document change.
    assertThat(ArcadeStateMachine.changesSchema(RaftLogEntryCodec.decode(RaftLogEntryCodec.encodeSchemaEntry("db",
        "{\"schemaVersion\":5}", Collections.emptyMap(), Collections.emptyMap(), walEntries, deltas)))).isTrue();
    // A change of the file set alone, as an index compaction: frequent, and no schema a transaction could be stale against.
    assertThat(ArcadeStateMachine.changesSchema(RaftLogEntryCodec.decode(RaftLogEntryCodec.encodeSchemaEntry("db",
        "", Map.of(7, "Counter_0.1.65536.v0.bucket"), Collections.emptyMap(), walEntries, deltas)))).isFalse();
    // WAL only, as a TimeSeries maintenance entry: prepared-against state is untouched.
    assertThat(ArcadeStateMachine.changesSchema(RaftLogEntryCodec.decode(RaftLogEntryCodec.encodeSchemaEntry("db",
        "", Collections.emptyMap(), Collections.emptyMap(), walEntries, deltas)))).isFalse();
    // Not a schema entry at all.
    assertThat(ArcadeStateMachine.changesSchema(RaftLogEntryCodec.decode(
        RaftLogEntryCodec.encodeTxEntry("db", wal, Collections.emptyMap())))).isFalse();
  }

  private static RaftClientRequest request(final String databaseName, final byte[] walData, final long preparedAtIndex,
      final long callId) {
    return RaftClientRequest.newBuilder()
        .setClientId(ClientId.randomId())
        .setServerId(RaftPeerId.valueOf("peer-0"))
        .setGroupId(RaftGroupId.randomId())
        .setCallId(callId)
        .setMessage(Message.valueOf(
            RaftLogEntryCodec.encodeTxEntry(databaseName, walData, Collections.emptyMap(), preparedAtIndex)))
        .setType(RaftClientRequest.writeRequestType())
        .build();
  }

  private LocalDatabase create(final String name) {
    final LocalDatabase database = (LocalDatabase) new DatabaseFactory(tempDir.resolve(name).toString()).create();
    database.getSchema().createDocumentType(TYPE, 1);
    database.transaction(() -> database.newDocument(TYPE).set("value", 0L).save());
    return database;
  }

  private static RID firstCounter(final LocalDatabase database) {
    return database.iterateType(TYPE, false).next().getIdentity();
  }

  /** Phase 1 of an increment, abandoned: the WAL bytes a node would ship for it (see Issue7438LeaderExclusiveWindowTest). */
  private static byte[] prepareIncrement(final LocalDatabase database, final RID rid) {
    database.begin();
    final Document doc = database.lookupByRID(rid, true).asDocument();
    doc.modify().set("value", doc.getLong("value") + 1).save();
    final TransactionContext tx = database.getTransaction();
    final TransactionContext.TransactionPhase1 phase1 = tx.commit1stPhase(true);
    final byte[] walData = phase1.result.toByteArray();
    tx.rollback();
    return walData;
  }
}
