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
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.database.RID;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.FakeArcadeDBServer;
import com.arcadedb.server.ServerDatabase;
import com.arcadedb.server.backup.BackupCoordinator;
import com.arcadedb.utility.StallAwareStopwatch;
import com.sun.net.httpserver.HttpServer;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.exceptions.StateMachineException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Restore and backup protection: restore ordering, write refusal and hold while a restore claim is active, and retained-backup handling.
 */
class RestoreAndBackupProtectionTest {

  /**
   * Regression test for issue #10426: {@code RestoreAndBackupProtectionTest.RetainedBackupNotDestroyed} went red on the CI Linux runners
   * and stayed green on macOS. The test refuses an install whose retained backup cannot be restored and then asserts
   * that {@code .snapshot-backup/schema.json} is still there, but {@code restoreBackup} moved the entries back in the
   * order the filesystem lists them. APFS lists a directory sorted (so {@code bucket} failed before {@code schema.json}
   * was reached); ext4, overlayfs and tmpfs list {@code schema.json} first, so it was already in the live directory when
   * the {@code bucket} move failed.
   * <p>
   * The assertion was right and the code was fragile: a restore that fails part way must leave the backup holding the
   * entry that makes a directory a database, so that a half-restored directory is never mistaken for a loadable one and
   * whatever still has to move is plainly in the backup. The schema files therefore go back LAST, on every filesystem.
   */
  @Nested
  class RestoreBackupOrder {

    private static final String DB = "mydb";

    @AfterEach
    void clearProgressHook() {
      SnapshotInstaller.swapProgressForTesting = null;
    }

    /** The order is asserted off the progress hook, so it holds whatever order the filesystem lists the backup in. */
    // Regression for #10426: a refused install must be asserted independent of file listing order
    @Test
    void theSchemaFilesAreTheLastEntriesMovedBackFromTheRetainedBackup(@TempDir final Path databasesDir) throws Exception {
      final Path dbPath = databasesDir.resolve(DB);
      final Path backup = dbPath.resolve(".snapshot-backup");
      Files.createDirectories(backup.resolve("bucket"));
      Files.writeString(backup.resolve("bucket").resolve("0.bucket"), "backed-up");
      // Names on both sides of "schema" in lexicographic order, so neither a sorted nor a reverse listing puts them last.
      Files.writeString(backup.resolve("aaa.dict"), "first");
      Files.writeString(backup.resolve("zzz.bucket"), "last");
      Files.writeString(backup.resolve("schema.prev.json"), "{\"original\":\"prev\"}");
      Files.writeString(backup.resolve("schema.json"), "{\"original\":true}");
      Files.writeString(dbPath.resolve(".snapshot-pending"), "");

      final List<String> restored = new ArrayList<>();
      SnapshotInstaller.swapProgressForTesting = point -> {
        if (point.startsWith("RESTORING:"))
          restored.add(point.substring("RESTORING:".length()));
      };

      SnapshotInstaller.recoverPendingSnapshotSwaps(databasesDir);

      assertThat(restored).as("every backed-up entry went back").containsExactlyInAnyOrder("aaa.dict", "bucket", "zzz.bucket",
          "schema.prev.json", "schema.json");
      assertThat(restored.subList(restored.size() - 2, restored.size()))
          .as("the schema files are what makes the directory a database, so they are the last to move back")
          .containsExactly("schema.prev.json", "schema.json");
      assertThat(backup).doesNotExist();
      assertThat(dbPath.resolve(".snapshot-pending")).doesNotExist();
      assertThat(dbPath.resolve("schema.json")).hasContent("{\"original\":true}");
      assertThat(dbPath.resolve("bucket").resolve("0.bucket")).hasContent("backed-up");
    }

    // Regression for #10426: a refused install must be asserted independent of file listing order
    /**
     * A restore that cannot finish leaves the schema file in the backup. Failing on the very first entry moved is what a
     * sorted listing produced by accident; here the failure is forced after every other entry has gone back, which is
     * what an unsorted listing did to {@code schema.json} before the fix.
     */
    @Test
    void aRestoreThatFailsAfterTheOtherEntriesLeavesTheSchemaInTheBackup(@TempDir final Path databasesDir) throws Exception {
      final Path dbPath = databasesDir.resolve(DB);
      final Path backup = dbPath.resolve(".snapshot-backup");
      Files.createDirectories(backup.resolve("bucket"));
      Files.writeString(backup.resolve("bucket").resolve("0.bucket"), "backed-up");
      Files.writeString(backup.resolve("zzz.bucket"), "last");
      Files.writeString(backup.resolve("schema.json"), "{\"original\":true}");
      // Same trick as Issue7139: a non-empty directory with a backed-up name makes that one move fail.
      Files.createDirectories(dbPath.resolve("bucket"));
      Files.writeString(dbPath.resolve("bucket").resolve("leftover.bucket"), "torn");
      Files.writeString(dbPath.resolve(".snapshot-pending"), "");

      SnapshotInstaller.recoverPendingSnapshotSwaps(databasesDir);

      assertThat(backup.resolve("schema.json")).as("the schema file only leaves the backup once everything else is back")
          .exists();
      assertThat(dbPath.resolve("schema.json")).doesNotExist();
      assertThat(dbPath.resolve(".snapshot-pending")).exists();
    }
  }

  /**
   * Issue #10965: an {@code overwrite} restore puts a write refusal on the leader's target (#10689), but a replica commits its
   * phase 1 locally and ships a {@code TX_ENTRY} that the leader appended without looking at that refusal, so a commit made on a
   * replica during the download was acknowledged and then destroyed by the swap. The leader now refuses the entry of another node
   * for a database that refuses writes, before it reaches the log.
   */
  @Nested
  class ReplicaEntryRefusedDuringRestoreClaim {

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

    // Regression for #10965: a replica TX_ENTRY is refused while an overwrite restore claim is held
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

    // Regression for #10965: a replica TX_ENTRY is refused while an overwrite restore claim is held
    @Test
    void onlyTheDatabaseUnderTheClaimIsRefused() throws Exception {
      db.refuseWrites(REASON, true);
      final org.apache.ratis.statemachine.TransactionContext other = stateMachine.startTransaction(
          request(otherDb.getName(), prepareIncrement(otherDb, otherCounter), 1));
      assertThat(other.getException()).isNull();
    }

    /** A refusal that goes up after the first check and before the reservation is seen by the check made after it. */
    // Regression for #10965: a replica TX_ENTRY is refused while an overwrite restore claim is held
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

  /**
   * Issue #10974: an {@code overwrite} restore waited only for the leader's own commits, so a transaction entry of a replica that the
   * leader had accepted before the claim (its page versions reserved, not applied yet) could still be appended and acknowledged to
   * its client during the download, and then be destroyed by the swap. The restore now holds the database on the leader: entries of
   * other nodes are refused from the claim on, and the ones accepted before are waited for, bounded, before the download starts.
   */
  @Nested
  class RestoreHoldDrainsReplicaEntries {

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
    // Regression for #10974: a restore hold drains the replica entries the leader already accepted
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

    // Regression for #10974: a restore hold drains the replica entries the leader already accepted
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

    // Regression for #10974: a restore hold drains the replica entries the leader already accepted
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
    // Regression for #10974: a restore hold drains the replica entries the leader already accepted
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
    // Regression for #10974: a restore hold drains the replica entries the leader already accepted
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

    // Regression for #10974: a restore hold drains the replica entries the leader already accepted
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
    // Regression for #10974: a restore hold drains the replica entries the leader already accepted
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

  /**
   * Issue #10974, review of PR #10979: the hold a restore takes on the writes of the other nodes is the leader's own, so a leadership
   * that moves during the download leaves the new leader accepting writes the swap would destroy. The restore is refused before the
   * swap when this node is no longer the leader in the term it took the hold in.
   */
  @Nested
  class RestoreHoldLeadership {

    private static final String DB = "restored";

    // Regression for #10974: the restore hold follows leadership changes during the download
    @Test
    void theHoldStandsWhileTheLeadershipItWasTakenInDoes() {
      final FakeRaftHAServer raft = FakeRaftHAServer.detached().leader(true).currentTerm(3L);
      raft.holdForRestore(DB);

      assertThatCode(() -> raft.ensureRestoreHoldStands(DB)).doesNotThrowAnyException();
      raft.releaseRestoreHold(DB);
    }

    // Regression for #10974: the restore hold follows leadership changes during the download
    @Test
    void aNewTermRefusesTheSwap() {
      final FakeRaftHAServer raft = FakeRaftHAServer.detached().leader(true).currentTerm(3L);
      raft.holdForRestore(DB);

      // re-elected, or another leader in between: either way the hold did not refuse the writes of that term
      raft.currentTerm(4L);
      assertThatThrownBy(() -> raft.ensureRestoreHoldStands(DB)).isInstanceOf(NeedRetryException.class)
          .hasMessageContaining("leadership");
      raft.releaseRestoreHold(DB);
    }

    // Regression for #10974: the restore hold follows leadership changes during the download
    @Test
    void aLostLeadershipRefusesTheSwap() {
      final FakeRaftHAServer raft = FakeRaftHAServer.detached().leader(true).currentTerm(3L);
      raft.holdForRestore(DB);

      raft.leader(false);
      assertThatThrownBy(() -> raft.ensureRestoreHoldStands(DB)).isInstanceOf(NeedRetryException.class);

      raft.releaseRestoreHold(DB);
    }

    // Regression for #10974: the restore hold follows leadership changes during the download
    /**
     * A restore that replaces a database cluster-wide runs on the leader: one that claimed it off the leadership (the leadership
     * moved between the forward of the request and the claim) is refused before its download.
     */
    @Test
    void aHoldTakenOffTheLeadershipIsRefused() {
      final FakeRaftHAServer raft = FakeRaftHAServer.detached().leader(false).currentTerm(3L);
      raft.holdForRestore(DB);

      assertThatThrownBy(() -> raft.ensureRestoreHoldStands(DB)).isInstanceOf(NeedRetryException.class)
          .hasMessageContaining("was not the leader");
      raft.releaseRestoreHold(DB);
    }

    /** The drop and the install of a restore carry the term its leader held the database in; nothing else is fenced. */
    // Regression for #10974: the restore hold follows leadership changes during the download
    @Test
    void theEntriesOfARestoreAreFencedToTheTermOfItsHold() {
      final FakeRaftHAServer raft = FakeRaftHAServer.detached().leader(true).currentTerm(7L);
      assertThat(raft.restoreFenceTerm(DB)).as("no restore holds it").isEqualTo(-1L);

      raft.holdForRestore(DB);
      assertThat(raft.restoreFenceTerm(DB)).isEqualTo(7L);
      assertThat(raft.restoreFenceTerm("other")).isEqualTo(-1L);

      raft.releaseRestoreHold(DB);
      assertThat(raft.restoreFenceTerm(DB)).isEqualTo(-1L);
    }

    /** A peer that has not advertised the section would halt on it (pre-#7138 framing): the entries are then not fenced. */
    // Regression for #10974: the restore hold follows leadership changes during the download
    @Test
    void noFenceIsWrittenWhileAPeerCannotReadIt() {
      final FakeRaftHAServer raft = FakeRaftHAServer.detached().leader(true).currentTerm(7L);
      raft.returns("peersMissingCapabilityNow", List.of("peer-2"));
      raft.holdForRestore(DB);

      assertThat(raft.restoreFenceTerm(DB)).isEqualTo(-1L);
      raft.releaseRestoreHold(DB);
    }

    /** The capability probe can wait on the peers: a leadership that moves meanwhile refuses the entry before it is submitted. */
    // Regression for #10974: the restore hold follows leadership changes during the download
    @Test
    void aLeadershipThatMovesDuringTheCapabilityProbeRefusesTheEntry() {
      final FakeRaftHAServer raft = FakeRaftHAServer.detached().leader(true).currentTerm(7L);
      raft.holdForRestore(DB);
      raft.on("peersMissingCapabilityNow", args -> {
        raft.currentTerm(8L);
        return List.of();
      });

      assertThatThrownBy(() -> raft.restoreFenceTerm(DB)).isInstanceOf(NeedRetryException.class).hasMessageContaining("leadership");
      raft.releaseRestoreHold(DB);
    }

    // Regression for #10974: the restore hold follows leadership changes during the download
    @Test
    void theFenceSurvivesTheCodecOfBothEntries() {
      final RaftLogEntryCodec.DecodedEntry drop = RaftLogEntryCodec.decode(RaftLogEntryCodec.encodeDropDatabaseEntry(DB, 7L));
      assertThat(drop.type()).isEqualTo(RaftLogEntryType.DROP_DATABASE_ENTRY);
      assertThat(drop.restoreFenceTerm()).isEqualTo(7L);

      final RaftLogEntryCodec.DecodedEntry install = RaftLogEntryCodec.decode(
          RaftLogEntryCodec.encodeInstallDatabaseEntry(DB, true, 7L));
      assertThat(install.forceSnapshot()).isTrue();
      assertThat(install.restoreFenceTerm()).isEqualTo(7L);

      // unfenced: byte for byte the entries a build without the section writes
      assertThat(RaftLogEntryCodec.encodeDropDatabaseEntry(DB, -1L)).isEqualTo(RaftLogEntryCodec.encodeDropDatabaseEntry(DB));
      assertThat(RaftLogEntryCodec.decode(RaftLogEntryCodec.encodeDropDatabaseEntry(DB)).restoreFenceTerm()).isEqualTo(-1L);
    }

    /** The leader the entries reach refuses them from another node, or in a term other than the hold's. */
    // Regression for #10974: the restore hold follows leadership changes during the download
    @Test
    void theLeaderRefusesAFencedEntryOfAnotherNodeOrTerm() {
      final ArcadeStateMachine leader = new ArcadeStateMachine() {
        @Override
        long currentRaftTerm() {
          return 7L;
        }
      };
      assertThat(leader.staleRestoreRefusal(DB, -1L, false)).as("no restore fenced it").isNull();
      assertThat(leader.staleRestoreRefusal(DB, 7L, true)).as("its own, in the term of the hold").isNull();
      assertThat(leader.staleRestoreRefusal(DB, 7L, false)).as("the resend of a deposed leader").isNotNull();
      assertThat(leader.staleRestoreRefusal(DB, 6L, true)).as("elected again after an interim leader").isNotNull()
          .hasMessageContaining("term 6");
    }
  }

  /**
   * Regression test for issue #7139: every {@code install()} began by deleting {@code .snapshot-backup}, which is
   * the documented reconciliation source for a previous install whose ROLLBACK failed and which deliberately left
   * its pending marker in place. A second install attempt therefore destroyed the only local copy of the database
   * before its own download had produced anything.
   * <p>
   * The sequence needs two consecutive install failures with the first one's rollback also failing, which is
   * narrow - but the condition that causes the first (a volume that has just filled, the #7037 scenario) is
   * exactly the condition that causes the second, which is what makes it plausible rather than hypothetical.
   * <p>
   * Startup recovery then read the resulting state - no completion marker, no backup - as "the download was
   * interrupted before the backup was created", cleaned up quietly and accepted a torn {@code dbPath} as a healthy
   * database, with nothing in the log saying so. That premise is only one of the two ways to reach that state, and
   * this covers both halves: the state is prevented, and it is reported if it arises another way.
   */
  @Nested
  class RetainedBackupNotDestroyed {

    private static final String DB = "mydb";

    /** A published phase recovery cannot act on must survive a new install, even when no backup remains. */
    // Regression for #7139: install() must not destroy a retained .snapshot-backup
    @Test
    void aNewInstallPreservesAnUnresolvedPublishedPhaseEvenWithoutABackup(@TempDir final Path databasesDir)
        throws Exception {
      final Path dbPath = databasesDir.resolve(DB);
      Files.createDirectories(dbPath);
      Files.writeString(dbPath.resolve("schema.json"), "{}");
      Files.writeString(dbPath.resolve("data.dat"), "retained-data");
      Files.writeString(dbPath.resolve(".snapshot-pending"), "");
      Files.writeString(dbPath.resolve(".snapshot-swap-state"), "UNKNOWN");
      final Path staged = Files.createDirectories(dbPath.resolve(".snapshot-new"));
      Files.writeString(staged.resolve(".snapshot-complete"), "");
      Files.writeString(staged.resolve("data.dat"), "new-data");

      assertThatThrownBy(() -> SnapshotInstaller.install(DB, dbPath.toString(), () -> null, () -> null, null,
          serverThatCannotDownload(databasesDir)))
          .isInstanceOf(IOException.class).hasMessageContaining("unresolved snapshot swap state");

      assertThat(dbPath.resolve("data.dat")).hasContent("retained-data");
      assertThat(dbPath.resolve(".snapshot-pending")).exists();
      assertThat(dbPath.resolve(".snapshot-swap-state")).hasContent("UNKNOWN");
      assertThat(staged.resolve("data.dat")).hasContent("new-data");
    }

    // Regression for #7139: install() must not destroy a retained .snapshot-backup
    /**
     * #7769: a crash or I/O failure after the staging was marked complete but before the first phase was published
     * moved nothing. The next install discards that possibly stale staging instead of refusing forever, then
     * proceeds with its own attempt (whose download fails here, leaving the live database untouched).
     */
    @ParameterizedTest
    @ValueSource(strings = { ".snapshot-swap-state.tmp", "ABSENT" })
    void aNewInstallDiscardsAStagedSnapshotWhosePhaseWasNeverPublished(final String stateFile,
        @TempDir final Path databasesDir) throws Exception {
      final Path dbPath = databasesDir.resolve(DB);
      Files.createDirectories(dbPath);
      Files.writeString(dbPath.resolve("schema.json"), "{}");
      Files.writeString(dbPath.resolve("data.dat"), "retained-data");
      Files.writeString(dbPath.resolve(".snapshot-pending"), "");
      if (!stateFile.equals("ABSENT"))
        Files.writeString(dbPath.resolve(stateFile), "BACK");
      final Path staged = Files.createDirectories(dbPath.resolve(".snapshot-new"));
      Files.writeString(staged.resolve(".snapshot-complete"), "");
      Files.writeString(staged.resolve("schema.json"), "{}");
      Files.writeString(staged.resolve("data.dat"), "new-data");

      assertThatThrownBy(() -> SnapshotInstaller.install(DB, dbPath.toString(), () -> null, () -> null, null,
          serverThatCannotDownload(databasesDir)))
          .isInstanceOf(IOException.class).hasMessageNotContaining("unresolved snapshot swap state");

      assertThat(dbPath.resolve("data.dat")).hasContent("retained-data");
      assertThat(dbPath.resolve(".snapshot-pending")).doesNotExist();
      assertThat(dbPath.resolve(".snapshot-swap-state")).doesNotExist();
      assertThat(dbPath.resolve(".snapshot-swap-state.tmp")).doesNotExist();
      assertThat(dbPath.resolve(".snapshot-backup")).doesNotExist();
    }

    // -------------------------------------------------------------------------------------------------
    // Part 1: a new install must not destroy a backup a failed rollback left behind
    // -------------------------------------------------------------------------------------------------

    // Regression for #7139: install() must not destroy a retained .snapshot-backup
    /**
     * The #7139 sequence: attempt 1 tore {@code dbPath} and its rollback failed, leaving the marker and the backup.
     * Attempt 2 starts and its own download fails. The node must end up with the previous database restored, not
     * with a torn directory and no copy of anything.
     */
    @Test
    void aNewInstallReconcilesTheRetainedBackupInsteadOfDeletingIt(@TempDir final Path databasesDir) throws Exception {
      final Path dbPath = databasesDir.resolve(DB);
      final Path backup = dbPath.resolve(".snapshot-backup");
      Files.createDirectories(backup);
      // dbPath is torn: clearLiveDatabaseFiles ran, restoreBackup did not. Everything is in the backup.
      Files.writeString(backup.resolve("schema.json"), "{\"original\":true}");
      Files.writeString(backup.resolve("data.dat"), "original-data");
      Files.writeString(dbPath.resolve(".snapshot-pending"), "");

      // The second attempt cannot download either (no leader address resolves), exactly like the first.
      assertThatThrownBy(() -> SnapshotInstaller.install(DB, dbPath.toString(), () -> null, () -> null, null,
          serverThatCannotDownload(databasesDir)))
          .isInstanceOf(IOException.class);

      assertThat(dbPath.resolve("schema.json"))
          .as("the retained backup must be reconciled into the live directory, not deleted")
          .exists();
      assertThat(Files.readString(dbPath.resolve("data.dat"))).isEqualTo("original-data");
      assertThat(backup).doesNotExist();
      assertThat(dbPath.resolve(".snapshot-pending")).doesNotExist();
    }

    // Regression for #7139: install() must not destroy a retained .snapshot-backup
    /**
     * When the reconciliation itself cannot complete, the install is refused and the backup stays: proceeding into
     * a download that may fail for the same reason would leave nothing behind at all.
     */
    @Test
    void anInstallIsRefusedWhenTheRetainedBackupCannotBeReconciled(@TempDir final Path databasesDir) throws Exception {
      final Path dbPath = databasesDir.resolve(DB);
      final Path backup = dbPath.resolve(".snapshot-backup");
      Files.createDirectories(backup.resolve("bucket"));
      Files.writeString(backup.resolve("bucket").resolve("0.bucket"), "backed-up");
      Files.writeString(backup.resolve("schema.json"), "{\"original\":true}");
      // A non-empty directory of the same name still in dbPath makes the restoring move fail, the way a
      // partially-cleared directory does when the volume that caused the first failure is still full.
      Files.createDirectories(dbPath.resolve("bucket"));
      Files.writeString(dbPath.resolve("bucket").resolve("leftover.bucket"), "torn");
      Files.writeString(dbPath.resolve(".snapshot-pending"), "");

      assertThatThrownBy(() -> SnapshotInstaller.install(DB, dbPath.toString(), () -> null, () -> null, null,
          serverThatCannotDownload(databasesDir)))
          .isInstanceOf(IOException.class)
          .hasMessageContaining("only intact copy")
          // the operator is sent to the restart-time recovery and told to delete nothing by hand: a live entry absent from the backup
          // can be an original the failed install never moved into it, and a name does not tell (#10426)
          .hasMessageContaining("restart the node")
          .hasMessageContaining("delete nothing by hand")
          .hasMessageNotContaining("start with '.snapshot'");

      assertThat(backup).as("the last copy of the database must survive a refused install").exists();
      // The restore moves the schema file back last, so this holds whatever order the filesystem lists the backup in (#10426).
      assertThat(backup.resolve("schema.json")).exists();
      assertThat(dbPath.resolve(".snapshot-pending")).exists();
    }

    // -------------------------------------------------------------------------------------------------
    // Part 2: the "orphaned" recovery branch must prove its premise
    // -------------------------------------------------------------------------------------------------

    // Regression for #7139: install() must not destroy a retained .snapshot-backup
    /**
     * No completion marker and no backup is reachable two ways, and only one of them leaves {@code dbDir} intact.
     * A torn directory must not be blessed as healthy and must keep its marker for an operator.
     */
    @Test
    void recoveryRefusesToBlessATornDirectoryAsOrphaned(@TempDir final Path databasesDir) throws Exception {
      final Path dbDir = databasesDir.resolve(DB);
      Files.createDirectories(dbDir);
      // What a failed rollback leaves: some files, but not a database - clearLiveDatabaseFiles took schema.json.
      Files.writeString(dbDir.resolve("leftover.bucket"), "half-cleared");
      Files.writeString(dbDir.resolve(".snapshot-pending"), "");

      SnapshotInstaller.recoverPendingSnapshotSwaps(databasesDir);

      assertThat(dbDir.resolve(".snapshot-pending"))
          .as("a torn directory keeps its marker so the state is preserved rather than silently accepted")
          .exists();
      assertThat(dbDir.resolve("leftover.bucket")).exists();
    }

    /** Control: a genuinely intact database whose download was interrupted before any backup is still cleaned up. */
    // Regression for #7139: install() must not destroy a retained .snapshot-backup
    @Test
    void recoveryStillCleansUpAGenuinelyOrphanedDownload(@TempDir final Path databasesDir) throws Exception {
      final Path dbDir = databasesDir.resolve(DB);
      final Path snapshotNew = dbDir.resolve(".snapshot-new");
      Files.createDirectories(snapshotNew);
      Files.writeString(dbDir.resolve("schema.json"), "{\"original\":true}");
      Files.writeString(dbDir.resolve("data.dat"), "existing-data");
      Files.writeString(dbDir.resolve(".snapshot-pending"), "");
      Files.writeString(snapshotNew.resolve("partial.dat"), "incomplete");

      SnapshotInstaller.recoverPendingSnapshotSwaps(databasesDir);

      assertThat(Files.readString(dbDir.resolve("data.dat"))).isEqualTo("existing-data");
      assertThat(snapshotNew).doesNotExist();
      assertThat(dbDir.resolve(".snapshot-pending")).doesNotExist();
    }

    // Regression for #7139: install() must not destroy a retained .snapshot-backup
    /**
     * A database interrupted during a SCHEMA REWRITE carries {@code schema.prev.json} and no {@code schema.json} -
     * and that is a database, not a torn directory. Testing only the final name would misread exactly the kind of
     * interrupted state this recovery path exists to reason about, which is why the check delegates to the
     * engine's own {@code DatabaseFactory.exists()} rather than restating it.
     */
    @Test
    void aDatabaseInterruptedMidSchemaRewriteIsNotMistakenForATornDirectory(@TempDir final Path databasesDir)
        throws Exception {
      final Path dbDir = databasesDir.resolve(DB);
      Files.createDirectories(dbDir);
      Files.writeString(dbDir.resolve("schema.prev.json"), "{\"original\":true}");
      Files.writeString(dbDir.resolve("data.dat"), "existing-data");
      Files.writeString(dbDir.resolve(".snapshot-pending"), "");

      SnapshotInstaller.recoverPendingSnapshotSwaps(databasesDir);

      assertThat(dbDir.resolve(".snapshot-pending"))
          .as("a mid-rewrite database is intact, so the orphaned branch may clean up around it")
          .doesNotExist();
      assertThat(Files.readString(dbDir.resolve("data.dat"))).isEqualTo("existing-data");
    }

    /**
     * A server whose snapshot download can never succeed (no leader address resolves), with retries turned off so
     * the test does not pay the backoff.
     */
    private static ArcadeDBServer serverThatCannotDownload(final Path databasesDir) {
      final ContextConfiguration config = new ContextConfiguration();
      config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, databasesDir.toString());
      config.setValue(GlobalConfiguration.HA_SNAPSHOT_INSTALL_RETRIES, 0);
      config.setValue(GlobalConfiguration.HA_SNAPSHOT_INSTALL_RETRY_BASE_MS, 1);

      final FakeArcadeDBServer server = FakeArcadeDBServer.create((String) null, config);
      return server;
    }
  }

  /**
   * Regression test for issue #7444.
   * <p>
   * A restore is leader-only, so restore-versus-restore of one database is already serialised cluster-wide. A backup is
   * not: {@code runOnServer} defaults to {@code "*"}, so every node runs its own scheduled backup. After a restore the
   * leader submits an install-database entry with {@code forceSnapshot=true} and every follower answers it by replacing
   * its own copy of the database directory from the leader's snapshot - while that follower's own scheduled backup may
   * be reading it. The follower's {@link BackupCoordinator} slot was the wrong one to consult only because nothing on
   * the install path ever took it: the restore happened on a different JVM, but the <i>reinstall</i> happens right here.
   * <p>
   * {@link SnapshotInstaller#install} is the single choke point every install driver goes through - the forceSnapshot
   * arm of {@code applyInstallDatabaseEntry}, the bootstrap installs, the reconciler's resync - so the slot is taken
   * there rather than at any one call site. These tests drive the real {@code install}, with a local HTTP server
   * standing in for the leader's snapshot endpoint, and pin both directions of the exclusion.
   */
  @Nested
  class SnapshotInstallExcludesBackup {

    private static final String DB_NAME        = "snap7444";
    private static final String PASSWORD       = "DefaultPasswordForTests";
    private static final int    LIVE_COUNT     = 12;
    private static final int    SNAPSHOT_COUNT = 27;

    private HttpServer     leader;
    private int            leaderPort;
    private ArcadeDBServer server;

    @BeforeEach
    void startLeaderEndpoint() throws IOException {
      leader = HttpServer.create(new InetSocketAddress(0), 0);
      leaderPort = leader.getAddress().getPort();
    }

    @AfterEach
    void stopEverything() {
      SnapshotInstaller.swapBarrierForTesting = null;
      if (server != null) {
        try {
          if (server.existsDatabase(DB_NAME))
            ((DatabaseInternal) server.getDatabaseForOperator(DB_NAME)).getEmbedded().drop();
        } catch (final Exception ignore) {
          // best-effort cleanup; the @TempDir is removed regardless
        }
        server.stop();
        server = null;
      }
      if (leader != null) {
        leader.stop(0);
        leader = null;
      }
    }

    // Regression for #7444: a snapshot install and a backup of one database exclude each other
    /**
     * The bug as reported: while this node replaces its copy of the database, a backup of it must not be admitted.
     * The install is paused inside the registry-locked swap - the exact moment the directory is being replaced - and a
     * backup is attempted from the test thread, standing in for the scheduled tick {@code BackupTask.run} would fire.
     * <p>
     * Without the fix the reservation succeeds and the tick goes on to read a directory that is being deleted.
     */
    @Test
    @Timeout(120)
    void aBackupIsRefusedWhileThisNodeIsReinstallingTheDatabase(@TempDir final Path root) throws Exception {
      server = startServer(root, 60_000);
      final Path dbPath = createLiveDatabase();
      serveSnapshot(root);

      final CountDownLatch insideSwap = new CountDownLatch(1);
      final CountDownLatch releaseSwap = new CountDownLatch(1);
      SnapshotInstaller.swapBarrierForTesting = () -> {
        insideSwap.countDown();
        try {
          releaseSwap.await();
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      };

      final AtomicBoolean installDone = new AtomicBoolean(false);
      final AtomicReference<Throwable> installError = new AtomicReference<>();
      final Thread installer = new Thread(() -> {
        try {
          SnapshotInstaller.install(DB_NAME, dbPath.toString(), "localhost:" + leaderPort, null, null, server);
          installDone.set(true);
        } catch (final Throwable t) {
          installError.set(t);
        }
      }, "snapshot-installer");
      installer.start();

      try {
        assertThat(insideSwap.await(60, TimeUnit.SECONDS)).as("the install reached the in-swap barrier").isTrue();

        final BackupCoordinator coordinator = server.getBackupCoordinator();
        assertThat(coordinator.isInProgress(DB_NAME, BackupCoordinator.Operation.RESTORE))
            .as("the install holds this node's maintenance slot while it replaces the files").isTrue();
        assertThat(coordinator.begin(DB_NAME, BackupCoordinator.Operation.BACKUP))
            .as("a scheduled backup of a database being reinstalled from the leader is refused")
            .isEqualTo(BackupCoordinator.Operation.RESTORE);
      } finally {
        releaseSwap.countDown();
      }

      installer.join(60_000);
      assertThat(installError.get()).as("the install completed without error").isNull();
      assertThat(installDone.get()).isTrue();

      // The slot is released when the install ends, whatever the outcome: a leaked reservation would block every later
      // backup of this database until the server restarts.
      assertThat(server.getBackupCoordinator().isInProgress(DB_NAME)).as("the install released the slot").isFalse();
      assertThat(server.getBackupCoordinator().begin(DB_NAME, BackupCoordinator.Operation.BACKUP)).isNull();
      server.getBackupCoordinator().end(DB_NAME, BackupCoordinator.Operation.BACKUP);

      assertThat(server.getDatabaseForOperator(DB_NAME).countType("Node", true)).isEqualTo(SNAPSHOT_COUNT);
    }

    // Regression for #7444: a snapshot install and a backup of one database exclude each other
    /**
     * The other direction: a backup already running when the entry arrives. The install waits for it rather than
     * pulling the directory out from under it, and proceeds the moment it is released.
     */
    @Test
    @Timeout(120)
    void anInstallWaitsForABackupThatIsAlreadyRunning(@TempDir final Path root) throws Exception {
      server = startServer(root, 60_000);
      final Path dbPath = createLiveDatabase();
      serveSnapshot(root);

      final BackupCoordinator coordinator = server.getBackupCoordinator();
      assertThat(coordinator.begin(DB_NAME, BackupCoordinator.Operation.BACKUP)).isNull();

      final AtomicBoolean installDone = new AtomicBoolean(false);
      final AtomicReference<Throwable> installError = new AtomicReference<>();
      final Thread installer = new Thread(() -> {
        try {
          SnapshotInstaller.install(DB_NAME, dbPath.toString(), "localhost:" + leaderPort, null, null, server);
          installDone.set(true);
        } catch (final Throwable t) {
          installError.set(t);
        }
      }, "snapshot-installer");
      installer.start();

      // The install must not have replaced anything yet: the live database is still the one the backup is reading.
      installer.join(2_000);
      assertThat(installDone.get()).as("the install waited instead of replacing the files under a running backup").isFalse();
      assertThat(server.getDatabaseForOperator(DB_NAME).countType("Node", true))
          .as("the database the backup is reading is untouched while the install waits").isEqualTo(LIVE_COUNT);

      coordinator.end(DB_NAME, BackupCoordinator.Operation.BACKUP);

      installer.join(60_000);
      assertThat(installError.get()).as("the install completed without error once the backup released the slot").isNull();
      assertThat(installDone.get()).isTrue();
      assertThat(server.getDatabaseForOperator(DB_NAME).countType("Node", true)).isEqualTo(SNAPSHOT_COUNT);
    }

    // Regression for #7444: a snapshot install and a backup of one database exclude each other
    /**
     * The wait is bounded, and that is not a detail: an install applies a committed Raft entry, and a follower that
     * blocks on it forever stops applying <i>every</i> database's entries - one state machine multiplexes them all.
     * With the wait configured short, a backup that never ends does not stop the install; it only makes it loud.
     */
    @Test
    @Timeout(120)
    void aBackupThatNeverEndsDoesNotBlockTheInstallForever(@TempDir final Path root) throws Exception {
      server = startServer(root, 500);
      final Path dbPath = createLiveDatabase();
      serveSnapshot(root);

      final BackupCoordinator coordinator = server.getBackupCoordinator();
      assertThat(coordinator.begin(DB_NAME, BackupCoordinator.Operation.BACKUP)).isNull();
      try {
        SnapshotInstaller.install(DB_NAME, dbPath.toString(), "localhost:" + leaderPort, null, null, server);

        assertThat(server.getDatabaseForOperator(DB_NAME).countType("Node", true))
            .as("the committed entry was applied even though the wait expired").isEqualTo(SNAPSHOT_COUNT);
        // The install never took the slot, so it must not have released the backup's reservation either.
        assertThat(coordinator.isInProgress(DB_NAME, BackupCoordinator.Operation.BACKUP))
            .as("a timed-out install leaves the reservation it never took alone").isTrue();
      } finally {
        coordinator.end(DB_NAME, BackupCoordinator.Operation.BACKUP);
      }
    }

    private ArcadeDBServer startServer(final Path root, final long backupWaitMs) throws IOException {
      final Path databasesDir = root.resolve("databases");
      Files.createDirectories(databasesDir);

      final ContextConfiguration config = new ContextConfiguration();
      config.setValue(GlobalConfiguration.SERVER_NAME, "ArcadeDB_7444");
      config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, databasesDir.toString());
      config.setValue(GlobalConfiguration.SERVER_ROOT_PATH, root.toString());
      config.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, PASSWORD);
      config.setValue(GlobalConfiguration.SERVER_HTTP_INCOMING_HOST, "localhost");
      config.setValue(GlobalConfiguration.HA_ENABLED, false);
      config.setValue(GlobalConfiguration.HA_SNAPSHOT_INSTALL_BACKUP_WAIT_MS, backupWaitMs);

      final ArcadeDBServer started = new ArcadeDBServer(config);
      started.start();
      return started;
    }

    private Path createLiveDatabase() {
      final ServerDatabase live = server.getOrCreateDatabase(DB_NAME);
      live.transaction(() -> {
        live.getSchema().createVertexType("Node");
        for (int i = 0; i < LIVE_COUNT; i++)
          live.newVertex("Node").set("v", i).save();
      });
      return Path.of(((DatabaseInternal) live).getDatabasePath());
    }

    /**
     * Publishes a real, loadable database with a DIFFERENT record count on the leader's snapshot endpoint, so a
     * successful install is observable as a count change rather than only as the absence of an exception.
     */
    private void serveSnapshot(final Path root) throws IOException {
      final Path source = root.resolve("leader-copy").resolve(DB_NAME);
      try (final Database snap = new DatabaseFactory(source.toString()).create()) {
        snap.transaction(() -> {
          snap.getSchema().createVertexType("Node");
          for (int i = 0; i < SNAPSHOT_COUNT; i++)
            snap.newVertex("Node").set("v", i).save();
        });
      }

      final byte[] zip = zipDirectory(source);
      leader.createContext("/api/v1/ha/snapshot/" + DB_NAME, exchange -> {
        exchange.sendResponseHeaders(200, zip.length);
        exchange.getResponseBody().write(zip);
        exchange.close();
      });
      leader.start();
    }

    private static byte[] zipDirectory(final Path dir) throws IOException {
      final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      try (final ZipOutputStream zip = new ZipOutputStream(bytes); final Stream<Path> files = Files.walk(dir)) {
        for (final Path file : files.filter(Files::isRegularFile).toList()) {
          zip.putNextEntry(new ZipEntry(dir.relativize(file).toString().replace('\\', '/')));
          zip.write(Files.readAllBytes(file));
          zip.closeEntry();
        }
      }
      return bytes.toByteArray();
    }
  }
}
