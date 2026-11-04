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
import org.apache.ratis.protocol.exceptions.StateMachineException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10974: an {@code overwrite} restore waited only for the leader's own commits, so a transaction entry of a replica that the
 * leader had accepted before the claim (its page versions reserved, not applied yet) could still be appended and acknowledged to
 * its client during the download, and then be destroyed by the swap. The restore now holds the database on the leader: entries of
 * other nodes are refused from the claim on, and the ones accepted before are waited for, bounded, before the download starts.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10974RestoreHoldDrainsReplicaEntriesTest {
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
    db = createWithCounter("issue10974");
    counter = firstCounter(db);
    otherDb = createWithCounter("issue10974other");
    otherCounter = firstCounter(otherDb);
    stateMachine = newStateMachine();
  }

  @AfterEach
  void tearDown() {
    if (db != null && db.isOpen())
      db.drop();
    if (otherDb != null && otherDb.isOpen())
      otherDb.drop();
  }

  /** An entry accepted before the claim holds a reservation the restore must wait out, or its commit is destroyed by the swap. */
  @Test
  void theHoldWaitsForAnEntryAcceptedBeforeTheClaim() throws Exception {
    final byte[] walData = prepareIncrement(db, counter);
    assertThat(stateMachine.startTransaction(request(db.getName(), walData, 1)).getException()).isNull();
    assertThat(stateMachine.reservedPageVersions(db.getName())).isGreaterThan(0);

    final CountDownLatch entered = new CountDownLatch(1);
    final AtomicBoolean drained = new AtomicBoolean();
    final Thread restore = new Thread(() -> {
      entered.countDown();
      drained.set(stateMachine.holdForRestore(db.getName(), 30_000));
    });
    restore.setDaemon(true);
    restore.start();
    try {
      assertThat(entered.await(10, TimeUnit.SECONDS)).isTrue();
      // A lower bound only: a stall can make the restore look blocked for longer, never make a blocked one finish.
      restore.join(200);
      assertThat(restore.isAlive()).as("the restore waits while an accepted entry is still to be applied").isTrue();

      // The apply thread's release of the entry's reservation lets the restore go on.
      stateMachine.releaseReservedVersions(db.getName(), walData);
      restore.join(60_000);
      assertThat(restore.isAlive()).isFalse();
      assertThat(drained.get()).isTrue();
    } finally {
      stateMachine.releaseReservedVersions(db.getName(), walData);
      restore.join(10_000);
      stateMachine.releaseRestoreHold(db.getName());
    }
  }

  @Test
  void anEntryOfAnotherNodeIsRefusedWhileHeldAndAcceptedAfterTheRelease() throws Exception {
    final byte[] walData = prepareIncrement(db, counter);

    assertThat(stateMachine.holdForRestore(db.getName(), 0)).isTrue();
    final org.apache.ratis.statemachine.TransactionContext refused = stateMachine.startTransaction(request(db.getName(), walData, 1));
    assertThat(refused.getException()).isInstanceOf(NeedRetryException.class).hasMessageContaining("restored");
    assertThat(stateMachine.reservedPageVersions(db.getName())).as("a refused entry reserves nothing").isZero();

    stateMachine.releaseRestoreHold(db.getName());
    final org.apache.ratis.statemachine.TransactionContext accepted = stateMachine.startTransaction(request(db.getName(), walData, 2));
    assertThat(accepted.getException()).isNull();
  }

  @Test
  void onlyTheDatabaseUnderRestoreIsHeld() throws Exception {
    stateMachine.holdForRestore(db.getName(), 0);
    try {
      assertThat(stateMachine.startTransaction(request(otherDb.getName(), prepareIncrement(otherDb, otherCounter), 1))
          .getException()).isNull();
    } finally {
      stateMachine.releaseRestoreHold(db.getName());
    }
  }

  /** The wait is bounded: a reservation that never clears refuses the restore rather than holding the operator for ever. */
  @Test
  void theWaitIsBoundedAndTheHoldStandsWhenItGivesUp() throws Exception {
    final byte[] first = prepareIncrement(db, counter);
    final byte[] second = prepareIncrement(db, counter);
    assertThat(stateMachine.startTransaction(request(db.getName(), first, 1)).getException()).isNull();

    final StallAwareStopwatch watch = StallAwareStopwatch.start();
    final boolean drained = stateMachine.holdForRestore(db.getName(), 100);
    watch.assertGaveUpWithin(30_000, "a wait that gives up at its budget from one that waits for the reservation to clear");

    assertThat(drained).isFalse();
    assertThat(stateMachine.isHeldForRestore(db.getName())).as("released by the caller's finally, not by the timeout").isTrue();
    assertThat(stateMachine.startTransaction(request(db.getName(), second, 2)).getException())
        .isInstanceOf(NeedRetryException.class);
    stateMachine.releaseRestoreHold(db.getName());
    assertThat(stateMachine.isHeldForRestore(db.getName())).isFalse();
  }

  /** The entry the drain stopped counting (unconfirmed past its grace) is refused when it finally reaches the append. */
  @Test
  void anEntryAcceptedBeforeTheHoldAndAppendedInsideItIsRefusedAtAppend() throws Exception {
    final org.apache.ratis.statemachine.TransactionContext accepted = stateMachine.startTransaction(
        request(db.getName(), prepareIncrement(db, counter), 1));
    assertThat(accepted.getException()).isNull();

    stateMachine.holdForRestore(db.getName(), 0);
    try {
      assertThatThrownBy(() -> stateMachine.preAppendTransaction(accepted)).isInstanceOf(StateMachineException.class)
          .hasCauseInstanceOf(NeedRetryException.class);
      assertThat(stateMachine.reservedPageVersions(db.getName())).as("its reservation is released").isZero();
    } finally {
      stateMachine.releaseRestoreHold(db.getName());
    }
  }

  /**
   * A restore holds the database once and releases it once; a DDL running on it meanwhile nests its own window, whose end must
   * not lift the hold, and the release must not end the DDL's window.
   */
  @Test
  void theHoldAndALeaderExclusiveWindowAreIndependent() throws Exception {
    final byte[] walData = prepareIncrement(db, counter);
    stateMachine.holdForRestore(db.getName(), 0);
    stateMachine.holdForRestore(db.getName(), 0);
    stateMachine.beginLeaderExclusive(db.getName(), 0);
    stateMachine.endLeaderExclusive(db.getName());
    assertThat(stateMachine.startTransaction(request(db.getName(), walData, 1)).getException())
        .as("the end of the DDL leaves the hold").isInstanceOf(NeedRetryException.class);

    stateMachine.beginLeaderExclusive(db.getName(), 0);
    stateMachine.releaseRestoreHold(db.getName());
    assertThat(stateMachine.startTransaction(request(db.getName(), walData, 2)).getException())
        .as("the release leaves the DDL's window").isInstanceOf(NeedRetryException.class);
    stateMachine.endLeaderExclusive(db.getName());
    assertThat(stateMachine.startTransaction(request(db.getName(), walData, 3)).getException()).isNull();
  }

  /** The machine an in-place Ratis restart builds shares the holds of the one it replaces, so a restore under way stays held. */
  @Test
  void aHoldSurvivesTheStateMachineBeingReplaced() throws Exception {
    final Set<String> holds = ConcurrentHashMap.newKeySet();
    final ArcadeStateMachine before = newStateMachine();
    before.shareRestoreHolds(holds);
    before.holdForRestore(db.getName(), 0);

    final ArcadeStateMachine after = newStateMachine();
    after.shareRestoreHolds(holds);
    assertThat(after.startTransaction(request(db.getName(), prepareIncrement(db, counter), 1)).getException())
        .isInstanceOf(NeedRetryException.class);

    after.releaseRestoreHold(db.getName());
    assertThat(before.isHeldForRestore(db.getName())).isFalse();
  }

  private ArcadeStateMachine newStateMachine() {
    return new ArcadeStateMachine() {
      @Override
      DatabaseInternal databaseFor(final String databaseName) {
        return db.getName().equals(databaseName) ? db : otherDb.getName().equals(databaseName) ? otherDb : null;
      }
    };
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
