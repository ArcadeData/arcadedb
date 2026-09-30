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
import com.arcadedb.utility.StallAwareStopwatch;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7438: while a leader runs a DDL, a replica transaction on the same database must not be accepted into the
 * log. The DDL publishes its pages locally before its {@code SCHEMA_ENTRY} is appended, so an entry validated in
 * between claims the same next page version as the DDL's embedded WAL, and the followers splice the two.
 * The leader refuses such an entry with a retryable error, and a DDL that starts while an entry is still in flight
 * waits for that entry to be applied first, so it never publishes over a page the log has already given away.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7438LeaderExclusiveWindowTest {
  private static final String TYPE = "Counter";

  @TempDir
  Path tempDir;

  private LocalDatabase      db;
  private LocalDatabase      otherDb;
  private ArcadeStateMachine stateMachine;
  private RID                counter;
  private RID                otherCounter;

  @BeforeEach
  void setUp() {
    db = createWithCounter("issue7438");
    counter = firstCounter(db);
    otherDb = createWithCounter("issue7438other");
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
  void aReplicaEntryIsRefusedWhileTheLeaderRunsADdlAndAcceptedAfter() throws Exception {
    final byte[] walData = prepareIncrement(db, counter);

    stateMachine.beginLeaderExclusive(db.getName(), 0);
    final org.apache.ratis.statemachine.TransactionContext refused = stateMachine.startTransaction(request(db.getName(), walData, 1));
    assertThat(refused.getException()).isInstanceOf(NeedRetryException.class).hasMessageContaining("schema change");
    assertThat(stateMachine.reservedPageVersions(db.getName())).as("a refused entry reserves nothing").isZero();

    stateMachine.endLeaderExclusive(db.getName());
    final org.apache.ratis.statemachine.TransactionContext accepted = stateMachine.startTransaction(request(db.getName(), walData, 2));
    assertThat(accepted.getException()).isNull();
    assertThat(stateMachine.reservedPageVersions(db.getName())).isGreaterThan(0);
  }

  @Test
  void onlyTheDatabaseRunningTheDdlIsExcluded() throws Exception {
    stateMachine.beginLeaderExclusive(db.getName(), 0);
    try {
      final org.apache.ratis.statemachine.TransactionContext other = stateMachine.startTransaction(
          request(otherDb.getName(), prepareIncrement(otherDb, otherCounter), 1));
      assertThat(other.getException()).isNull();
    } finally {
      stateMachine.endLeaderExclusive(db.getName());
    }
  }

  @Test
  void nestedWindowsHoldUntilTheLastOneEnds() throws Exception {
    final byte[] walData = prepareIncrement(db, counter);
    stateMachine.beginLeaderExclusive(db.getName(), 0);
    stateMachine.beginLeaderExclusive(db.getName(), 0);

    stateMachine.endLeaderExclusive(db.getName());
    assertThat(stateMachine.startTransaction(request(db.getName(), walData, 1)).getException())
        .isInstanceOf(NeedRetryException.class);

    stateMachine.endLeaderExclusive(db.getName());
    assertThat(stateMachine.startTransaction(request(db.getName(), walData, 2)).getException()).isNull();
  }

  /** An entry accepted before the DDL began holds a reservation the DDL must wait out, or it publishes over it. */
  @Test
  void theWindowWaitsForAnEntryAlreadyInFlightToBeApplied() throws Exception {
    final byte[] walData = prepareIncrement(db, counter);
    assertThat(stateMachine.startTransaction(request(db.getName(), walData, 1)).getException()).isNull();
    assertThat(stateMachine.reservedPageVersions(db.getName())).isGreaterThan(0);

    final CountDownLatch entered = new CountDownLatch(1);
    final AtomicBoolean drained = new AtomicBoolean();
    final Thread ddl = new Thread(() -> {
      entered.countDown();
      drained.set(stateMachine.beginLeaderExclusive(db.getName(), 30_000));
    });
    ddl.setDaemon(true);
    ddl.start();
    try {
      assertThat(entered.await(10, TimeUnit.SECONDS)).isTrue();
      // A lower bound only: a stall can make the DDL look blocked for longer, never make a blocked DDL finish.
      ddl.join(200);
      assertThat(ddl.isAlive()).as("the DDL waits while an accepted entry is still to be applied").isTrue();

      // The apply thread's release of the entry's reservation lets the DDL go on.
      stateMachine.releaseReservedVersions(db.getName(), walData);
      ddl.join(10_000);
      assertThat(ddl.isAlive()).isFalse();
      assertThat(drained.get()).isTrue();
    } finally {
      stateMachine.releaseReservedVersions(db.getName(), walData);
      ddl.join(10_000);
      stateMachine.endLeaderExclusive(db.getName());
    }
  }

  /** The wait is bounded: a reservation that never clears must not hold the DDL, and so its write lock, forever. */
  @Test
  void theWaitIsBounded() throws Exception {
    final byte[] first = prepareIncrement(db, counter);
    final byte[] second = prepareIncrement(db, counter);
    assertThat(stateMachine.startTransaction(request(db.getName(), first, 1)).getException()).isNull();

    final StallAwareStopwatch watch = StallAwareStopwatch.start();
    final boolean drained = stateMachine.beginLeaderExclusive(db.getName(), 100);
    watch.assertGaveUpWithin(30_000, "a wait that gives up at its budget from one that waits for the reservation to clear");

    assertThat(drained).isFalse();
    // Still exclusive: the DDL goes ahead, and the window protects it from anything that arrives from here.
    assertThat(stateMachine.startTransaction(request(db.getName(), second, 2)).getException())
        .isInstanceOf(NeedRetryException.class);
    stateMachine.endLeaderExclusive(db.getName());
  }

  /**
   * The check-then-act window: an entry passes the first check, and the DDL registers while the entry is being
   * validated. The registration is then seen by the check made after the reservation, which backs the entry out and
   * releases what it reserved, so neither order lets it in.
   */
  @Test
  void anEntryThatSlipsPastTheFirstCheckIsBackedOutAfterItsReservation() throws Exception {
    final byte[] walData = prepareIncrement(db, counter);
    final ArcadeStateMachine racing = new ArcadeStateMachine() {
      @Override
      DatabaseInternal databaseFor(final String databaseName) {
        // Runs after the first check and before the reservation: the DDL registers in between.
        beginLeaderExclusive(databaseName, 0);
        return db;
      }
    };

    final org.apache.ratis.statemachine.TransactionContext refused = racing.startTransaction(request(db.getName(), walData, 1));

    assertThat(refused.getException()).isInstanceOf(NeedRetryException.class).hasMessageContaining("schema change");
    assertThat(racing.reservedPageVersions(db.getName())).as("the reservation it took is released").isZero();
    racing.endLeaderExclusive(db.getName());
  }

  /** Ending a window that was never begun must not disturb one that is open. */
  @Test
  void anUnbalancedEndLeavesTheOpenWindowAlone() throws Exception {
    final byte[] walData = prepareIncrement(db, counter);
    stateMachine.endLeaderExclusive(otherDb.getName());
    stateMachine.beginLeaderExclusive(db.getName(), 0);
    stateMachine.endLeaderExclusive(otherDb.getName());
    try {
      assertThat(stateMachine.startTransaction(request(db.getName(), walData, 1)).getException())
          .isInstanceOf(NeedRetryException.class);
    } finally {
      stateMachine.endLeaderExclusive(db.getName());
    }
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

  /** Phase 1 of an increment, abandoned: the WAL bytes a node would ship for it (see Issue6965PreAppendValidationTest). */
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
