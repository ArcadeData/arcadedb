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

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #8022: every install of a database's files clears that database's page-version ledger, not only the one
 * driven by an {@code INSTALL_DATABASE_ENTRY}. The targeted resync of a quarantined database, the operator resync and
 * the Ratis-initiated installs all run through {@link ArcadeStateMachine#runUnderInstallGate}, and before the fix none
 * of them evicted the reservations taken against the copy they discarded: a reservation above the installed copy's
 * page version keeps failing the engine's phase-1 check on that page with a {@link ConcurrentModificationException}
 * for as long as it stays.
 * <p>
 * The other half of the fix is what makes the clear safe: a node elected while it is replacing a database refuses to
 * validate (and so to reserve) an entry for it, so no reservation of an entry still waiting behind the install lock
 * can be the one the clear drops.
 */
class Issue8022InstallClearsPageVersionLedgerTest {
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
    db = createWithCounter("issue8022");
    counter = firstCounter(db);
    otherDb = createWithCounter("issue8022other");
    otherCounter = firstCounter(otherDb);
    // Resolves the database a Raft request names, as the server does on a real leader.
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
  void aSuccessfulInstallClearsTheReservationsThatFencedThePage() throws Exception {
    stateMachine.validateBeforeAppend(db, prepareIncrement(db, counter), entry(1));
    assertThat(stateMachine.reservedPageVersions(db.getName())).isGreaterThan(0);
    // The symptom the issue reports: the reservation fences the page for every local transaction on it.
    assertThatThrownBy(() -> increment(db, counter)).isInstanceOf(ConcurrentModificationException.class);

    // The targeted resync's install, with the actual file swap stubbed out: the page-version ledger is the subject.
    stateMachine.runUnderInstallGate(db.getName(), () -> {
    });

    assertThat(stateMachine.reservedPageVersions(db.getName())).as("the replaced copy's reservations are gone").isZero();
    increment(db, counter);
    assertThat(db.lookupByRID(counter, true).asDocument().getLong("value")).isEqualTo(1L);
  }

  @Test
  void theRatisInitiatedInstallClearsTheLedgerToo() throws Exception {
    stateMachine.validateBeforeAppend(db, prepareIncrement(db, counter), entry(1));
    assertThat(stateMachine.reservedPageVersions(db.getName())).isGreaterThan(0);

    // DatabaseReconciler's InstallGate wiring, the overload that records the installed boundary (issue #8577).
    stateMachine.runUnderInstallGate(db.getName(), 42L, () -> {
    });

    assertThat(stateMachine.reservedPageVersions(db.getName())).isZero();
  }

  @Test
  void aFailedInstallKeepsTheReservations() throws Exception {
    stateMachine.validateBeforeAppend(db, prepareIncrement(db, counter), entry(1));
    final int reserved = stateMachine.reservedPageVersions(db.getName());
    assertThat(reserved).isGreaterThan(0);

    // A failed install leaves the old copy in place, and the entry behind the reservation is still to be applied to it.
    assertThatThrownBy(() -> stateMachine.runUnderInstallGate(db.getName(), () -> {
      throw new IOException("download failed");
    })).isInstanceOf(IOException.class);

    assertThat(stateMachine.reservedPageVersions(db.getName())).isEqualTo(reserved);
  }

  @Test
  void anInstallClearsOnlyTheDatabaseItReplaces() throws Exception {
    stateMachine.validateBeforeAppend(db, prepareIncrement(db, counter), entry(1));
    stateMachine.validateBeforeAppend(otherDb, prepareIncrement(otherDb, otherCounter), entry(2));
    final int otherReserved = stateMachine.reservedPageVersions(otherDb.getName());
    assertThat(otherReserved).isGreaterThan(0);

    stateMachine.runUnderInstallGate(db.getName(), () -> {
    });

    assertThat(stateMachine.reservedPageVersions(db.getName())).isZero();
    assertThat(stateMachine.reservedPageVersions(otherDb.getName())).as("a database nothing replaced keeps its ledger")
        .isEqualTo(otherReserved);
  }

  @Test
  void aLeaderReplacingADatabaseRefusesToReserveAgainstIt() throws Exception {
    // Outside an install the entry is validated and reserved, which is what makes the refusal below meaningful.
    final org.apache.ratis.statemachine.TransactionContext accepted = stateMachine.startTransaction(
        request(db.getName(), prepareIncrement(db, counter), 1));
    assertThat(accepted.getException()).isNull();
    assertThat(stateMachine.reservedPageVersions(db.getName())).isGreaterThan(0);
    stateMachine.runUnderInstallGate(db.getName(), () -> {
    });
    assertThat(stateMachine.reservedPageVersions(db.getName())).isZero();

    // While the copy is being replaced (a node elected mid-install, issue #8491), nothing is reserved against it.
    final byte[] walData = prepareIncrement(db, counter);
    final AtomicReference<org.apache.ratis.statemachine.TransactionContext> duringInstall = new AtomicReference<>();
    final AtomicReference<org.apache.ratis.statemachine.TransactionContext> otherDuringInstall = new AtomicReference<>();
    final byte[] otherWalData = prepareIncrement(otherDb, otherCounter);
    stateMachine.runUnderInstallGate(db.getName(), () -> {
      duringInstall.set(stateMachine.startTransaction(request(db.getName(), walData, 2)));
      otherDuringInstall.set(stateMachine.startTransaction(request(otherDb.getName(), otherWalData, 3)));
      assertThat(stateMachine.reservedPageVersions(db.getName())).isZero();
    });

    assertThat(duringInstall.get().getException())
        .isInstanceOf(NeedRetryException.class)
        .hasMessageContaining("is being replaced");
    assertThat(otherDuringInstall.get().getException()).as("a database nothing replaces is still validated").isNull();
    assertThat(stateMachine.reservedPageVersions(otherDb.getName())).isGreaterThan(0);

    // Once the replacement ends the same entry is validated again.
    final org.apache.ratis.statemachine.TransactionContext after = stateMachine.startTransaction(
        request(db.getName(), walData, 4));
    assertThat(after.getException()).isNull();
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

  private static void increment(final LocalDatabase database, final RID rid) {
    database.transaction(() -> {
      final Document doc = database.lookupByRID(rid, true).asDocument();
      doc.modify().set("value", doc.getLong("value") + 1).save();
    }, false, 0);
  }

  private static PageVersionLedger.EntryId entry(final long callId) {
    return new PageVersionLedger.EntryId("client", callId);
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
