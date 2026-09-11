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
import com.arcadedb.database.Document;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.database.RID;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.exception.ConcurrentModificationException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The pre-append validation of issue #6965 against a real database: two transactions validated against the same
 * page version are the collision the issue reports (two nodes, one document each, one shared page), and the second
 * one must be refused at the point that decides the log order, not merged region by region on apply. Also covers the
 * engine-side half: while an entry's versions are reserved, a local transaction on the same page fails its phase 1
 * with the same retryable conflict, without a Raft round trip.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6965PreAppendValidationTest {
  private static final String TYPE = "Counter";

  @TempDir
  Path tempDir;

  private LocalDatabase      db;
  private ArcadeStateMachine stateMachine;
  private RID                leaderCounter;
  private RID                replicaCounter;

  @BeforeEach
  void setUp() {
    db = (LocalDatabase) new DatabaseFactory(tempDir.resolve("issue6965").toString()).create();
    db.getSchema().createDocumentType(TYPE, 1); // one bucket: both documents share a page
    db.transaction(() -> {
      leaderCounter = db.newDocument(TYPE).set("name", "leader").set("value", 0L).save().getIdentity();
      replicaCounter = db.newDocument(TYPE).set("name", "replica").set("value", 0L).save().getIdentity();
    });
    stateMachine = new ArcadeStateMachine();
  }

  @AfterEach
  void tearDown() {
    if (db != null && db.isOpen())
      db.drop();
  }

  @Test
  void theSecondWriterOnASharedPageIsRefusedNotMerged() throws Exception {
    // Two writers, one per document, both validated against the same page version: what the leader and a replica do
    // concurrently in the issue's scenario.
    final byte[] leaderEntry = prepareIncrement(leaderCounter);
    final byte[] replicaEntry = prepareIncrement(replicaCounter);

    stateMachine.validateBeforeAppend(db, leaderEntry, entry(1));
    assertThat(stateMachine.reservedPageVersions(db.getName())).isGreaterThan(0);

    assertThatThrownBy(() -> stateMachine.validateBeforeAppend(db, replicaEntry, entry(2)))
        .as("the second entry was validated against a version the log already gave away")
        .isInstanceOf(ConcurrentModificationException.class)
        .hasMessageContaining("retry");

    // Once the first entry is applied, the reservation is released and the local copy carries the version: the
    // replica's entry is still stale and still refused, now against the local copy.
    db.getTransactionManager().applyChanges(ArcadeStateMachine.deserializeWalTransaction(leaderEntry), Map.of(), false);
    stateMachine.releaseReservedVersions(db.getName(), leaderEntry);
    assertThat(stateMachine.reservedPageVersions(db.getName())).isZero();
    assertThatThrownBy(() -> stateMachine.validateBeforeAppend(db, replicaEntry, entry(2)))
        .isInstanceOf(ConcurrentModificationException.class);

    // The replica retries: a fresh transaction against the applied page is accepted.
    stateMachine.validateBeforeAppend(db, prepareIncrement(replicaCounter), entry(3));
    assertThat(db.lookupByRID(leaderCounter, true).asDocument().getLong("value")).isEqualTo(1L);
  }

  @Test
  void aReservedPageRefusesLocalTransactionsInPhaseOne() throws Exception {
    // A replica's entry has been accepted into the log but not applied here yet.
    final byte[] replicaEntry = prepareIncrement(replicaCounter);
    stateMachine.validateBeforeAppend(db, replicaEntry, entry(1));

    // A local transaction on the same page fails its own phase 1: the page it validated against is superseded, and
    // shipping it would only get it refused at append time one round trip later.
    assertThatThrownBy(() -> db.transaction(() -> {
      final Document counter = db.lookupByRID(leaderCounter, true).asDocument();
      counter.modify().set("value", counter.getLong("value") + 1).save();
    }, false, 0)).isInstanceOf(ConcurrentModificationException.class);

    // The entry is applied and its reservation released: the same local transaction now goes through.
    db.getTransactionManager().applyChanges(ArcadeStateMachine.deserializeWalTransaction(replicaEntry), Map.of(), false);
    stateMachine.releaseReservedVersions(db.getName(), replicaEntry);
    db.transaction(() -> {
      final Document counter = db.lookupByRID(leaderCounter, true).asDocument();
      counter.modify().set("value", counter.getLong("value") + 1).save();
    }, false, 0);

    assertThat(db.lookupByRID(leaderCounter, true).asDocument().getLong("value")).isEqualTo(1L);
    assertThat(db.lookupByRID(replicaCounter, true).asDocument().getLong("value")).isEqualTo(1L);
  }

  private static PageVersionLedger.EntryId entry(final long callId) {
    return new PageVersionLedger.EntryId("client", callId);
  }

  /**
   * Runs phase 1 of an increment of the given counter and returns the WAL bytes it would ship, then abandons the
   * transaction: the bytes describe a delta validated against the page version current at that moment, exactly what
   * a node ships to the leader.
   */
  private byte[] prepareIncrement(final RID rid) {
    db.begin();
    final Document counter = db.lookupByRID(rid, true).asDocument();
    counter.modify().set("value", counter.getLong("value") + 1).save();
    final TransactionContext tx = db.getTransaction();
    final TransactionContext.TransactionPhase1 phase1 = tx.commit1stPhase(true);
    final byte[] walData = phase1.result.toByteArray();
    tx.rollback();
    return walData;
  }
}
