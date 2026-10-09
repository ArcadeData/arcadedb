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
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.exception.DatabaseIsClosedException;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerDatabase;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.proto.RaftProtos.StateMachineLogEntryProto;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9568: a {@code BOOTSTRAP_FINGERPRINT_ENTRY} applied while the node is shutting down, on a database that is not
 * open, neither reopens the database nor starts a leader install. It is left for the replay on restart, like the entry
 * types #9550 covers.
 * <p>
 * Same harness as {@link Issue9550ApplyOnDatabaseClosedDuringShutdownTest}: a real (unstarted) {@link ArcadeDBServer}
 * with a real {@link LocalDatabase} registered in it, and only "is the node shutting down" set by the test through the
 * {@code isNodeShuttingDown()} seam. The server counts the {@code getDatabase(String)} calls, the one overload that
 * reopens a closed database, so a test can tell a reopen attempt from a lookup.
 */
class Issue9568BootstrapFingerprintApplyDuringShutdownTest {
  private static final String DB_NAME              = "db9568";
  private static final long   ENTRY_INDEX          = 7L;
  /** All-zero hex: never the fingerprint of a real database, so the verification reads it as a mismatch. */
  private static final String NO_MATCH_FINGERPRINT = "0".repeat(64);

  @TempDir
  Path tempDir;

  private ArcadeDBServer     server;
  private LocalDatabase      db;
  private Path               databaseDirectory;
  private ArcadeStateMachine sm;
  private RaftStorage        storage;
  private volatile boolean   shuttingDown;
  /** Makes the server read the database as deregistered while a bootstrap install of it is in flight. */
  private volatile boolean   installDeregisters;
  /** Closes the database right before the bootstrap verification reads it: the close racing the apply. */
  private volatile boolean   closeBeforeTheRead;
  private final AtomicInteger reopenAttempts = new AtomicInteger();

  @BeforeEach
  void setUp() throws IOException {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, tempDir.resolve("databases").toString());
    // A leader install fails at once (no leader in a unit test) instead of backing off between retries
    config.setValue(GlobalConfiguration.HA_SNAPSHOT_INSTALL_RETRIES, 0);
    config.setValue(GlobalConfiguration.HA_SNAPSHOT_INSTALL_RETRY_BASE_MS, 0L);
    server = new ArcadeDBServer(config) {
      @Override
      public ServerDatabase getDatabase(final String databaseName) {
        reopenAttempts.incrementAndGet();
        return super.getDatabase(databaseName);
      }

      @Override
      public boolean existsDatabase(final String databaseName) {
        if (installDeregisters && sm.isBootstrapInstallInFlight(databaseName))
          return false;
        return super.existsDatabase(databaseName);
      }
    };

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
      BootstrapBaseline readLocalBootstrapState(final String dbName) throws Exception {
        if (closeBeforeTheRead)
          db.close();
        return super.readLocalBootstrapState(dbName);
      }
    };
    sm.setServer(server);
    sm.initialize(stubServer(), RaftGroupId.valueOf(UUID.randomUUID()), storage);
  }

  @AfterEach
  void tearDown() throws IOException {
    if (sm != null)
      sm.close();
    if (storage != null)
      storage.close();
    final Database active = DatabaseFactory.getActiveDatabaseInstance(databaseDirectory.toString());
    if (active != null)
      active.drop();
    else if (db != null && db.isOpen())
      db.drop();
    server.stop();
  }

  /** The issue's case: the database was closed under the apply thread. Not reopened, no install, left for replay. */
  @Test
  void aBootstrapEntryOnADatabaseClosedDuringShutdownIsLeftForReplay() throws Exception {
    closeUnderTheApplyThread();

    assertThatThrownBy(() -> apply(bootstrapEntry(NO_MATCH_FINGERPRINT, Long.MAX_VALUE, ENTRY_INDEX)))
        .isInstanceOf(ExecutionException.class)
        .hasCauseInstanceOf(EntryLeftForReplayException.class);

    assertThat(reopenAttempts.get()).as("no reopen was attempted").isZero();
    assertThat(sm.isBootstrapInstallInFlight(DB_NAME)).as("no leader install started").isFalse();
    assertLeftForReplayNotQuarantined();
  }

  /**
   * A shutdown that already deregistered the closed database: the verification read it as "not here" and either
   * reinstalled it from the leader or recorded it as a late joiner, which advanced its applied index past the entry, so
   * the verification never ran again on restart. Left for replay instead, where the restart that registers the database
   * again verifies it.
   */
  @Test
  void aBootstrapEntryOnADatabaseTheShutdownDeregisteredIsLeftForReplay() throws Exception {
    closeUnderTheApplyThread();
    server.removeDatabase(DB_NAME);
    assertThat(server.existsDatabase(DB_NAME)).isFalse();

    assertThatThrownBy(() -> apply(bootstrapEntry(NO_MATCH_FINGERPRINT, Long.MAX_VALUE, ENTRY_INDEX)))
        .isInstanceOf(ExecutionException.class)
        .hasCauseInstanceOf(EntryLeftForReplayException.class);

    assertThat(reopenAttempts.get()).as("no reopen was attempted").isZero();
    assertThat(sm.isBootstrapInstallInFlight(DB_NAME)).isFalse();
    assertLeftForReplayNotQuarantined();
  }

  /**
   * The close lands after the entry found the database open, right before the verification reads it. The read does not
   * reopen it, and the failed read does not fall back to a leader install: the entry is left for replay.
   */
  @Test
  void aCloseRacingTheVerificationIsLeftForReplayRatherThanInstalled() throws Exception {
    shuttingDown = true;
    closeBeforeTheRead = true;

    assertThatThrownBy(() -> apply(bootstrapEntry(NO_MATCH_FINGERPRINT, Long.MAX_VALUE, ENTRY_INDEX)))
        .isInstanceOf(ExecutionException.class)
        .hasCauseInstanceOf(EntryLeftForReplayException.class);

    assertThat(reopenAttempts.get()).as("no reopen was attempted").isZero();
    assertThat(sm.isBootstrapInstallInFlight(DB_NAME)).isFalse();
    assertLeftForReplayNotQuarantined();
  }

  /**
   * The safety net after a failed bootstrap install: a database the install left deregistered was reopened with
   * {@code server.getDatabase}. While the node is shutting down it is not, and the entry is left for replay.
   */
  @Test
  void theFailedInstallSafetyNetDoesNotReopenWhileShuttingDown() throws Exception {
    shuttingDown = true;
    installDeregisters = true;

    assertThatThrownBy(() -> apply(bootstrapEntry(NO_MATCH_FINGERPRINT, Long.MAX_VALUE, ENTRY_INDEX)))
        .isInstanceOf(ExecutionException.class)
        .hasCauseInstanceOf(EntryLeftForReplayException.class);

    assertThat(reopenAttempts.get()).as("the safety net did not reopen the database").isZero();
    assertThat(sm.isBootstrapInstallInFlight(DB_NAME)).as("the install holder was released").isFalse();
    assertThat(sm.isDatabaseDiverged(DB_NAME)).isFalse();
    assertThat(indexOf(sm.getLastAppliedTermIndex())).isLessThan(ENTRY_INDEX);
    apply(logEntry(RaftLogEntryCodec.encodeDropDatabaseEntry("ghost"), ENTRY_INDEX + 1));
    assertThat(sm.takeSnapshot()).as("the checkpoint stops right before the entry left for replay")
        .isEqualTo(ENTRY_INDEX - 1);
  }

  /** The bootstrap read refuses to reopen a closed database while shutting down, like {@code databaseFor()}. */
  @Test
  void theLocalStateReadDoesNotReopenAClosedDatabaseWhileShuttingDown() {
    closeUnderTheApplyThread();

    assertThatThrownBy(() -> sm.readLocalBootstrapState(DB_NAME)).isInstanceOf(DatabaseIsClosedException.class)
        .hasMessageContaining(DB_NAME);
    assertThat(DatabaseFactory.getActiveDatabaseInstance(databaseDirectory.toString())).as("nothing was opened").isNull();
  }

  /** Only the closed database is refused: an open one is verified as before while the node shuts down. */
  @Test
  void aBootstrapEntryOnAnOpenDatabaseIsStillVerifiedWhileShuttingDown() throws Exception {
    final ArcadeStateMachine.BootstrapBaseline local = sm.readLocalBootstrapState(DB_NAME);
    shuttingDown = true;

    apply(bootstrapEntry(local.fingerprint(), local.lastTxId(), ENTRY_INDEX));

    assertThat(indexOf(sm.getLastAppliedTermIndex())).isEqualTo(ENTRY_INDEX);
    assertThat(sm.getBootstrapBaseline(DB_NAME)).isEqualTo(local);
  }

  /** Scoped to the shutdown: a node that keeps running reopens the closed database and verifies it, as it always did. */
  @Test
  void aClosedDatabaseOnANodeThatIsNotShuttingDownIsReopenedAndVerified() throws Exception {
    final ArcadeStateMachine.BootstrapBaseline local = sm.readLocalBootstrapState(DB_NAME);
    closeUnderTheApplyThread();
    shuttingDown = false;

    apply(bootstrapEntry(local.fingerprint(), local.lastTxId(), ENTRY_INDEX));

    assertThat(indexOf(sm.getLastAppliedTermIndex())).isEqualTo(ENTRY_INDEX);
    assertThat(DatabaseFactory.getActiveDatabaseInstance(databaseDirectory.toString()))
        .as("the read reopened the database").isNotNull();
    assertThat(sm.getBootstrapBaseline(DB_NAME)).isEqualTo(local);
  }

  private void assertLeftForReplayNotQuarantined() throws Exception {
    assertThat(sm.isDatabaseDiverged(DB_NAME)).as("nothing diverged: the node is going away").isFalse();
    assertThat(indexOf(sm.getLastAppliedTermIndex())).as("the entry did not move the applied position")
        .isLessThan(ENTRY_INDEX);
    assertThat(DatabaseFactory.getActiveDatabaseInstance(databaseDirectory.toString()))
        .as("the database was not reopened").isNull();

    // A later entry of another database still advances the applied index past the entry, so the checkpoint is what has
    // to stop short of it.
    apply(logEntry(RaftLogEntryCodec.encodeDropDatabaseEntry("ghost"), ENTRY_INDEX + 1));
    assertThat(indexOf(sm.getLastAppliedTermIndex())).isEqualTo(ENTRY_INDEX + 1);
    assertThat(sm.takeSnapshot()).as("the checkpoint stops right before the entry left for replay")
        .isEqualTo(ENTRY_INDEX - 1);
  }

  private void closeUnderTheApplyThread() {
    shuttingDown = true;
    db.close();
    assertThat(db.isOpen()).isFalse();
  }

  private void apply(final LogEntryProto entry) throws Exception {
    sm.applyTransaction(TransactionContext.newBuilder().setStateMachine(sm)
        .setLogEntry(entry).build()).get();
  }

  private static LogEntryProto bootstrapEntry(final String fingerprint, final long lastTxId, final long index) {
    return logEntry(RaftLogEntryCodec.encodeBootstrapFingerprintEntry(DB_NAME, fingerprint, lastTxId), index);
  }

  private static LogEntryProto logEntry(final ByteString payload, final long index) {
    return LogEntryProto.newBuilder().setTerm(1L).setIndex(index)
        .setStateMachineLogEntry(StateMachineLogEntryProto.newBuilder().setLogData(payload).build()).build();
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
