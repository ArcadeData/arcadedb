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
import com.arcadedb.exception.NeedRetryException;
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
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10965: an {@code overwrite} restore puts a write refusal on the leader's target (#10689), but a replica commits its
 * phase 1 locally and ships a {@code TX_ENTRY} that the leader appended without looking at that refusal, so a commit made on a
 * replica during the download was acknowledged and then destroyed by the swap. The leader now refuses the entry of another node
 * for a database that refuses writes, before it reaches the log.
 */
class Issue10965ReplicaEntryRefusedDuringRestoreClaimTest {
  private static final String TYPE   = "Counter";
  private static final String REASON = "a restore is replacing it with a backup";

  @TempDir
  Path tempDir;

  private LocalDatabase      db;
  private LocalDatabase      otherDb;
  private ArcadeStateMachine stateMachine;
  private RID                counter;
  private RID                otherCounter;

  @BeforeEach
  void setUp() {
    db = createWithCounter("issue10965");
    counter = firstCounter(db);
    otherDb = createWithCounter("issue10965other");
    otherCounter = firstCounter(otherDb);
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

  @Test
  void aReplicaEntryIsRefusedWhileTheLeaderTargetRefusesWritesAndAcceptedAfter() throws Exception {
    final byte[] walData = prepareIncrement(db, counter);

    db.refuseWrites(REASON, true);
    final org.apache.ratis.statemachine.TransactionContext refused = stateMachine.startTransaction(request(db.getName(), walData, 1));
    assertThat(refused.getException()).isInstanceOf(NeedRetryException.class).hasMessageContaining(REASON);
    assertThat(stateMachine.reservedPageVersions(db.getName())).as("a refused entry reserves nothing").isZero();

    db.acceptWrites(REASON);
    final org.apache.ratis.statemachine.TransactionContext accepted = stateMachine.startTransaction(request(db.getName(), walData, 2));
    assertThat(accepted.getException()).isNull();
    assertThat(stateMachine.reservedPageVersions(db.getName())).isGreaterThan(0);
  }

  @Test
  void onlyTheDatabaseUnderTheClaimIsRefused() throws Exception {
    db.refuseWrites(REASON, true);
    final org.apache.ratis.statemachine.TransactionContext other = stateMachine.startTransaction(
        request(otherDb.getName(), prepareIncrement(otherDb, otherCounter), 1));
    assertThat(other.getException()).isNull();
  }

  /** A refusal that goes up after the first check and before the reservation is seen by the check made after it. */
  @Test
  void anEntryThatSlipsPastTheFirstCheckIsBackedOutAfterItsReservation() throws Exception {
    final byte[] walData = prepareIncrement(db, counter);
    final AtomicInteger lookups = new AtomicInteger();
    final ArcadeStateMachine racing = new ArcadeStateMachine() {
      @Override
      DatabaseInternal databaseFor(final String databaseName) {
        // The restore claims the target after the first look and before the entry reserves its page versions
        if (lookups.incrementAndGet() == 2)
          db.refuseWrites(REASON, true);
        return db;
      }
    };

    final org.apache.ratis.statemachine.TransactionContext refused = racing.startTransaction(request(db.getName(), walData, 1));

    assertThat(refused.getException()).isInstanceOf(NeedRetryException.class).hasMessageContaining(REASON);
    assertThat(racing.reservedPageVersions(db.getName())).as("the reservation it took is released").isZero();
  }

  private static RaftClientRequest request(final String databaseName, final byte[] walData, final long callId) {
    return RaftClientRequest.newBuilder()
        .setClientId(ClientId.randomId())
        .setServerId(RaftPeerId.valueOf("peer-0"))
        .setGroupId(RaftGroupId.randomId())
        .setCallId(callId)
        .setMessage(Message.valueOf(RaftLogEntryCodec.encodeTxEntry(databaseName, walData, Collections.emptyMap())))
        .setType(RaftClientRequest.writeRequestType())
        .build();
  }

  private LocalDatabase createWithCounter(final String name) {
    final LocalDatabase database = (LocalDatabase) new DatabaseFactory(tempDir.resolve(name).toString()).create();
    database.getSchema().createDocumentType(TYPE, 1);
    database.transaction(() -> database.newDocument(TYPE).set("value", 0L).save());
    return database;
  }

  private static RID firstCounter(final LocalDatabase database) {
    return database.iterateType(TYPE, false).next().getIdentity();
  }

  /** Phase 1 of an increment, abandoned: the WAL bytes a node would ship for it. */
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
