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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.database.RID;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.server.ArcadeDBServer;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Issue #8598: applying an {@code INSTALL_DATABASE_ENTRY} clears the database's page-version ledger only when the apply
 * actually replaces the database's files. The clear used to run unconditionally at the top of
 * {@link ArcadeStateMachine#applyInstallDatabaseEntry}, so the arms that replace nothing - the leader skipping its own
 * forced-snapshot reinstall, the replay guard skipping an install a previous session already ran, and the normal-create
 * arm finding the database already present - dropped the reservations of entries appended after the install entry and
 * not yet applied. A second entry validated against the same base could then be appended: the equal-version splice
 * #6965 guards against.
 * <p>
 * The arm that does replace the files, the follower's forced-snapshot reinstall, clears through
 * {@link ArcadeStateMachine#runUnderInstallGate} on success (issue #8022, covered by
 * {@code Issue8022InstallClearsPageVersionLedgerTest}).
 */
class Issue8598InstallEntryKeepsLedgerWhenNothingIsReplacedTest {
  private static final String DB          = "issue8598";
  private static final String TYPE        = "Counter";
  private static final long   ENTRY_INDEX = 42L;

  @TempDir
  Path serverDir;

  private ArcadeDBServer     server;
  private LocalDatabase      db;
  private RID                counter;
  private ArcadeStateMachine stateMachine;

  @BeforeEach
  void setUp() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, serverDir.toString());
    server = new ArcadeDBServer(config);

    db = (LocalDatabase) new DatabaseFactory(serverDir.resolve(DB).toString()).create();
    db.getSchema().createDocumentType(TYPE, 1);
    db.transaction(() -> db.newDocument(TYPE).set("value", 0L).save());
    counter = db.iterateType(TYPE, false).next().getIdentity();
    server.registerDatabase(DB, db);

    // Resolves the database a Raft request names, as the server does on a real leader.
    stateMachine = new ArcadeStateMachine() {
      @Override
      DatabaseInternal databaseFor(final String databaseName) {
        return DB.equals(databaseName) ? db : null;
      }
    };
    stateMachine.setServer(server);
  }

  @AfterEach
  void tearDown() {
    if (db != null && db.isOpen())
      db.drop();
  }

  @Test
  void theLeaderSkippingItsOwnForcedReinstallKeepsTheReservations() {
    final RaftHAServer raft = mock(RaftHAServer.class);
    when(raft.isLeader()).thenReturn(true);
    stateMachine.setRaftHAServer(raft);

    assertReservationsSurvive(RaftLogEntryCodec.encodeInstallDatabaseEntry(DB, true));
  }

  @Test
  void theReplayGuardSkippingAnAlreadyRunReinstallKeepsTheReservations() {
    // A previous session ran this install and the database is registered: the replay guard skips the re-download.
    stateMachine.writePersistedAppliedIndex(ENTRY_INDEX + 8, DB);

    assertReservationsSurvive(RaftLogEntryCodec.encodeInstallDatabaseEntry(DB, true));
  }

  @Test
  void theCreateArmFindingTheDatabasePresentKeepsTheReservations() {
    // The leader creates the database locally before it submits the entry, and can validate writes on it meanwhile.
    assertReservationsSurvive(RaftLogEntryCodec.encodeInstallDatabaseEntry(DB, false));
  }

  /**
   * Reserves an entry appended after the install entry, applies the install entry, then checks the reservation still
   * refuses a second entry validated against the same base - the splice the ledger exists to prevent.
   */
  private void assertReservationsSurvive(final ByteString installEntry) {
    final byte[] first = prepareIncrement();
    final byte[] second = prepareIncrement();
    stateMachine.validateBeforeAppend(db, first, new PageVersionLedger.EntryId("client", 1));
    final int reserved = stateMachine.reservedPageVersions(DB);
    assertThat(reserved).isGreaterThan(0);

    stateMachine.applyInstallDatabaseEntry(RaftLogEntryCodec.decode(installEntry), ENTRY_INDEX);

    assertThat(stateMachine.reservedPageVersions(DB)).as("nothing was replaced, so no reservation may be dropped")
        .isEqualTo(reserved);
    assertThatThrownBy(() -> stateMachine.validateBeforeAppend(db, second, new PageVersionLedger.EntryId("client", 2)))
        .as("a second entry validated against the same base must still be refused")
        .isInstanceOf(ConcurrentModificationException.class);
  }

  /** Phase 1 of an increment, abandoned: the WAL bytes a node would ship for it (see Issue6965PreAppendValidationTest). */
  private byte[] prepareIncrement() {
    db.begin();
    final Document doc = db.lookupByRID(counter, true).asDocument();
    doc.modify().set("value", doc.getLong("value") + 1).save();
    final TransactionContext tx = db.getTransaction();
    final TransactionContext.TransactionPhase1 phase1 = tx.commit1stPhase(true);
    final byte[] walData = phase1.result.toByteArray();
    tx.rollback();
    return walData;
  }
}
