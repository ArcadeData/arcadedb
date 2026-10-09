/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.exception.DatabaseIsClosedException;
import com.arcadedb.server.ArcadeDBServer;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.proto.RaftProtos.StateMachineLogEntryProto;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9550: an apply that fails because its database was closed under it while the node is shutting down leaves the
 * entry for the replay on restart instead of quarantining the database.
 * <p>
 * #9548 made the engine's JVM shutdown hook wait for the server's, so a GRACEFUL stop stops Raft before the databases
 * close. Everything else that closes a database under a live apply thread - the server hook giving up on the lifecycle
 * lock, an embedder calling {@code DatabaseFactory.closeActiveDatabaseInstances()} on its way out - still reached the
 * per-database quarantine: the leader's double-failed publication ({@code publishLocalCommit}) and the follower's
 * {@code applyReplicatedTransaction} alike, and the reconcile in between tried to reopen the database through
 * {@code server.getDatabase}. A quarantine there is the wrong answer: nothing diverged, the node is going away, and the
 * entry is still in the Raft log.
 * <p>
 * The tests drive a real (unstarted) {@link ArcadeDBServer} with a real {@link LocalDatabase} registered in it, so the
 * real {@code databaseFor} lookup runs. Only whether the node is shutting down is set by the test, through the
 * {@code isNodeShuttingDown()} seam: the three real signals (Raft shutdown requested, server status, JVM shutdown) cannot
 * be raised in a unit test without stopping the JVM.
 */
class Issue9550ApplyOnDatabaseClosedDuringShutdownTest {
  private static final String DB_NAME     = "db9550";
  private static final long   ENTRY_INDEX = 7L;

  @TempDir
  Path tempDir;

  private ArcadeDBServer     server;
  private LocalDatabase      db;
  private Path               databaseDirectory;
  private ArcadeStateMachine sm;
  private RaftStorage        storage;
  private volatile boolean   shuttingDown;
  /** Set by the phase-2 fault, so only the reconcile that FOLLOWS a failed publication can be broken. */
  private volatile boolean   publishFailed;
  private volatile boolean   breakTheReconcile;

  @BeforeEach
  void setUp() throws IOException {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, tempDir.resolve("databases").toString());
    server = new ArcadeDBServer(config);

    databaseDirectory = tempDir.resolve("databases").resolve(DB_NAME);
    db = (LocalDatabase) new DatabaseFactory(databaseDirectory.toString()).create();
    db.getSchema().createDocumentType("Doc", 1);
    server.registerDatabase(DB_NAME, db);

    storage = RaftStorage.newBuilder().setDirectory(tempDir.resolve("raft").toFile())
        .setOption(RaftStorage.StartupOption.FORMAT).build();
    sm = new ArcadeStateMachine() {
      @Override
      boolean isNodeShuttingDown() {
        return shuttingDown;
      }

      @Override
      DatabaseInternal databaseFor(final String databaseName) {
        if (breakTheReconcile && publishFailed)
          throw new IllegalStateException("the database cannot be reached to reconcile from the replicated payload");
        return super.databaseFor(databaseName);
      }
    };
    sm.setServer(server);
    sm.initialize(stubServer(), RaftGroupId.valueOf(UUID.randomUUID()), storage);
  }

  @AfterEach
  void tearDown() throws IOException {
    RaftReplicatedDatabase.TEST_PHASE2_COMMIT_FAULT = null;
    if (sm != null)
      sm.close();
    if (storage != null)
      storage.close();
    if (db != null && db.isOpen()) {
      if (db.isTransactionActive())
        db.rollback();
      db.drop();
    }
    server.stop();
  }

  /**
   * The leader: its own entry comes back to the apply thread with the database closed under it. The publication fails,
   * the reconcile must not reopen the database, and the double failure must leave the entry for replay - not quarantine
   * the database. A later entry of another database still advances the applied index, so the checkpoint is what has to
   * stop short of the entry.
   */
  @Test
  void aLocallyOriginatedEntryOnADatabaseClosedDuringShutdownIsLeftForReplay() throws Exception {
    final Prepared prepared = prepare();
    final LocalCommit local = new LocalCommit(db.getName(), ArcadeStateMachine.peekWalTransactionId(prepared.walData),
        prepared.tx, prepared.phase1, prepared.walData);
    assertThat(sm.registerLocalCommit(local)).isTrue();

    closeUnderTheApplyThread();

    assertThatThrownBy(() -> sm.applyTransaction(ratisContext(txEntry(prepared, ENTRY_INDEX))).get())
        .isInstanceOf(ExecutionException.class)
        .hasCauseInstanceOf(EntryLeftForReplayException.class);

    assertThat(local.awaitOutcome(1_000)).as("the committing thread is still woken").isEqualTo(LocalCommit.Outcome.FAILED);
    assertThat(local.reconciled()).as("the pages are not on this node").isFalse();
    assertLeftForReplayNotQuarantined();
  }

  /** The follower: the same closed database under {@code applyReplicatedTransaction}. */
  @Test
  void aReplicatedEntryOnADatabaseClosedDuringShutdownIsLeftForReplay() throws Exception {
    final Prepared prepared = prepare();

    closeUnderTheApplyThread();

    assertThatThrownBy(() -> sm.applyTransaction(ratisContext(txEntry(prepared, ENTRY_INDEX))).get())
        .isInstanceOf(ExecutionException.class)
        .hasCauseInstanceOf(EntryLeftForReplayException.class);

    assertLeftForReplayNotQuarantined();
  }

  /** A schema entry resolves its database through the same lookup, and is left for replay the same way. */
  @Test
  void aSchemaEntryOnADatabaseClosedDuringShutdownIsLeftForReplay() throws Exception {
    closeUnderTheApplyThread();

    final ByteString schemaEntry = RaftLogEntryCodec.encodeSchemaEntry(DB_NAME, "", Collections.emptyMap(),
        Collections.emptyMap(), Collections.emptyList(), Collections.emptyList());
    assertThatThrownBy(() -> sm.applyTransaction(ratisContext(logEntry(schemaEntry, ENTRY_INDEX))).get())
        .isInstanceOf(ExecutionException.class)
        .hasCauseInstanceOf(EntryLeftForReplayException.class);

    assertLeftForReplayNotQuarantined();
  }

  /**
   * A drop of a registered database resolved it through {@code server.getDatabase}, which reopened a closed one only to
   * close it again for the drop. While shutting down it is not reopened, and the drop is left for replay: its directory
   * is still there.
   */
  @Test
  void aDropOfADatabaseClosedDuringShutdownIsLeftForReplay() throws Exception {
    closeUnderTheApplyThread();

    assertThatThrownBy(() -> sm.applyTransaction(ratisContext(dropEntry(DB_NAME, ENTRY_INDEX))).get())
        .isInstanceOf(ExecutionException.class)
        .hasCauseInstanceOf(EntryLeftForReplayException.class);

    assertThat(databaseDirectory).as("the drop did not run").isDirectory();
    assertThat(server.existsDatabase(DB_NAME)).isTrue();
    assertLeftForReplayNotQuarantined();
  }

  /**
   * The lookup every apply resolves its database through refuses to reopen a closed database while the node is shutting
   * down: that reopen is the "Found active instance ... already in use" of the #9548 log, and reopening anything on a
   * node that is going away is never wanted.
   */
  @Test
  void theDatabaseLookupDoesNotReopenAClosedDatabaseWhileShuttingDown() {
    closeUnderTheApplyThread();

    assertThatThrownBy(() -> sm.databaseFor(DB_NAME)).isInstanceOf(DatabaseIsClosedException.class)
        .hasMessageContaining(DB_NAME);
    assertThat(DatabaseFactory.getActiveDatabaseInstance(databaseDirectory.toString()))
        .as("nothing was opened").isNull();
  }

  /** An open database is served as before while shutting down: only the REOPEN is refused. */
  @Test
  void theDatabaseLookupStillServesAnOpenDatabaseWhileShuttingDown() {
    shuttingDown = true;
    assertThat(sm.databaseFor(DB_NAME).isOpen()).isTrue();
  }

  /**
   * Scoped to the closed database: a genuine double failure on a database that is still OPEN is a divergence whether
   * the node is shutting down or not, and is quarantined exactly as before (issue #7602).
   */
  @Test
  void aGenuineFailureOnAnOpenDatabaseIsStillQuarantinedWhileShuttingDown() throws Exception {
    final Prepared prepared = prepare();
    final LocalCommit local = new LocalCommit(db.getName(), ArcadeStateMachine.peekWalTransactionId(prepared.walData),
        prepared.tx, prepared.phase1, prepared.walData);
    assertThat(sm.registerLocalCommit(local)).isTrue();

    shuttingDown = true;
    breakTheReconcile = true;
    RaftReplicatedDatabase.TEST_PHASE2_COMMIT_FAULT = name -> {
      publishFailed = true;
      throw new IllegalStateException("the data volume is read-only");
    };

    assertThatThrownBy(() -> sm.applyTransaction(ratisContext(txEntry(prepared, ENTRY_INDEX))).get())
        .isInstanceOf(ExecutionException.class)
        .hasCauseInstanceOf(ReplicationException.class)
        .satisfies(e -> assertThat(e.getCause()).isNotInstanceOf(EntryLeftForReplayException.class));

    assertThat(sm.isDatabaseDiverged(DB_NAME)).isTrue();
  }

  /**
   * Scoped to the shutdown: a database closed on a node that is NOT shutting down - an embedder calling
   * {@code DatabaseFactory.closeActiveDatabaseInstances()} while the server keeps running - is not left for replay,
   * since nothing would replay it on a node that keeps running. The lookup reopens it, as it always did, and the entry
   * is applied.
   */
  @Test
  void aClosedDatabaseOnANodeThatIsNotShuttingDownIsReopenedAndApplied() throws Exception {
    final Prepared prepared = prepare();
    closeUnderTheApplyThread();
    shuttingDown = false;

    sm.applyTransaction(ratisContext(txEntry(prepared, ENTRY_INDEX))).get();

    assertThat(indexOf(sm.getLastAppliedTermIndex())).isEqualTo(ENTRY_INDEX);
    assertThat(sm.isDatabaseDiverged(DB_NAME)).isFalse();
    final Database reopened = DatabaseFactory.getActiveDatabaseInstance(databaseDirectory.toString());
    assertThat(reopened).as("the lookup reopened the database").isNotNull();
    assertThat(reopened.query("sql", "select from Doc").stream().count()).as("and the entry's record is in it").isEqualTo(1L);
    reopened.drop();
  }

  @Test
  void aRunningJvmIsNotShuttingDown() {
    assertThat(ArcadeStateMachine.isJvmShuttingDown()).isFalse();
  }

  /** The production wiring of one of the three signals: the Raft HA service having been asked to stop. */
  @Test
  void aRequestedRaftStopIsAShutdown() {
    final ArcadeStateMachine plain = new ArcadeStateMachine();
    plain.setServer(server);
    final FakeRaftHAServer raft = FakeRaftHAServer.detached(server);
    plain.setRaftHAServer(raft);

    raft.shutdownRequested(false);
    assertThat(plain.isNodeShuttingDown()).as("an unstarted server with Raft running").isFalse();

    raft.shutdownRequested(true);
    assertThat(plain.isNodeShuttingDown()).isTrue();
  }

  /** A second entry left for replay keeps the floor at the lowest one: the checkpoint still stops before the first. */
  @Test
  void theFloorIsTheLowestEntryLeftForReplay() throws Exception {
    closeUnderTheApplyThread();

    final ByteString schemaEntry = RaftLogEntryCodec.encodeSchemaEntry(DB_NAME, "", Collections.emptyMap(),
        Collections.emptyMap(), Collections.emptyList(), Collections.emptyList());
    assertThatThrownBy(() -> sm.applyTransaction(ratisContext(logEntry(schemaEntry, ENTRY_INDEX))).get())
        .hasCauseInstanceOf(EntryLeftForReplayException.class);
    assertThatThrownBy(() -> sm.applyTransaction(ratisContext(logEntry(schemaEntry, ENTRY_INDEX + 2))).get())
        .hasCauseInstanceOf(EntryLeftForReplayException.class);

    sm.applyTransaction(ratisContext(dropEntry("ghost", ENTRY_INDEX + 3))).get();
    assertThat(sm.takeSnapshot()).isEqualTo(ENTRY_INDEX - 1);
  }

  /** An entry left for replay at the very first index leaves nothing to checkpoint. */
  @Test
  void aFloorAtTheFirstIndexAuthorisesNoCheckpoint() throws Exception {
    closeUnderTheApplyThread();

    final ByteString schemaEntry = RaftLogEntryCodec.encodeSchemaEntry(DB_NAME, "", Collections.emptyMap(),
        Collections.emptyMap(), Collections.emptyList(), Collections.emptyList());
    assertThatThrownBy(() -> sm.applyTransaction(ratisContext(logEntry(schemaEntry, 0L))).get())
        .hasCauseInstanceOf(EntryLeftForReplayException.class);
    sm.applyTransaction(ratisContext(dropEntry("ghost", 1L))).get();

    assertThat(sm.takeSnapshot()).isEqualTo(RaftLog.INVALID_LOG_INDEX);
  }

  private void assertLeftForReplayNotQuarantined() throws Exception {
    assertThat(sm.isDatabaseDiverged(DB_NAME)).as("nothing diverged: the node is going away").isFalse();
    assertThat(indexOf(sm.getLastAppliedTermIndex())).as("the entry did not move the applied position")
        .isLessThan(ENTRY_INDEX);
    assertThat(DatabaseFactory.getActiveDatabaseInstance(databaseDirectory.toString()))
        .as("the database was not reopened").isNull();

    // A later entry of another database (a drop of one that does not exist here, which applies as a no-op) still
    // advances the applied index past the entry.
    sm.applyTransaction(ratisContext(dropEntry("ghost", ENTRY_INDEX + 1))).get();
    assertThat(indexOf(sm.getLastAppliedTermIndex())).isEqualTo(ENTRY_INDEX + 1);

    assertThat(sm.takeSnapshot()).as("the checkpoint stops right before the entry left for replay")
        .isEqualTo(ENTRY_INDEX - 1);
  }

  private void closeUnderTheApplyThread() {
    shuttingDown = true;
    db.close();
    assertThat(db.isOpen()).isFalse();
  }

  private record Prepared(TransactionContext tx, TransactionContext.TransactionPhase1 phase1, byte[] walData) {
  }

  /** A phase-1 transaction on the database, as the committing thread leaves it before dispatching the entry. */
  private Prepared prepare() {
    db.begin();
    db.newDocument("Doc").set("v", 1).save();
    final TransactionContext tx = db.getTransaction();
    final TransactionContext.TransactionPhase1 phase1 = tx.commit1stPhase(true);
    return new Prepared(tx, phase1, phase1.result.toByteArray());
  }

  private LogEntryProto txEntry(final Prepared prepared, final long index) {
    final ByteString payload = RaftLogEntryCodec.encodeTxEntry(DB_NAME, prepared.walData,
        prepared.tx.getBucketRecordDelta());
    return logEntry(payload, index);
  }

  private static LogEntryProto dropEntry(final String databaseName, final long index) {
    return logEntry(RaftLogEntryCodec.encodeDropDatabaseEntry(databaseName), index);
  }

  private static LogEntryProto logEntry(final ByteString payload, final long index) {
    return LogEntryProto.newBuilder().setTerm(1L).setIndex(index)
        .setStateMachineLogEntry(StateMachineLogEntryProto.newBuilder().setLogData(payload).build()).build();
  }

  private org.apache.ratis.statemachine.TransactionContext ratisContext(final LogEntryProto logEntry) {
    return org.apache.ratis.statemachine.TransactionContext.newBuilder().setStateMachine(sm).setLogEntry(logEntry).build();
  }

  private static long indexOf(final TermIndex termIndex) {
    return termIndex != null ? termIndex.getIndex() : -1L;
  }

  private RaftServer stubServer() {
    return (RaftServer) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[] { RaftServer.class },
        (proxy, method, args) -> {
          if ("getId".equals(method.getName()))
            return RaftPeerId.valueOf("test-peer");
          if ("close".equals(method.getName()) || "start".equals(method.getName()))
            return null;
          throw new UnsupportedOperationException("Stub: " + method.getName());
        });
  }
}
