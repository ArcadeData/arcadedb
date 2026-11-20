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
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.engine.PaginatedComponentFile;
import com.arcadedb.server.ArcadeDBServer;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.server.storage.RaftStorage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12025: the Raft snapshot checkpoint forces the data files of the OPEN databases and then authorises Ratis to purge
 * the log up to the node-global applied index, on the premise that a database that is not open was forced when it closed. A
 * crash-equivalent close (a fence, a wedged flush, a failed fsync) does not force anything: it keeps the WAL and the lock
 * file for the next open's recovery, but the WAL of a replicated commit is not forced (#11746) and a follower writes none.
 * With another database still committing, the applied index moved past the closed database's entries, and a checkpoint
 * there let Ratis purge the only copy a power loss would leave of their pages. The checkpoint now refuses while such a
 * database has not been reopened and forced.
 * <p>
 * A real (unstarted) {@link ArcadeDBServer} with real {@link LocalDatabase}s, as in
 * {@code AppliedIndexCheckpointTest.CheckpointSyncsDataFiles}.
 *
 * @author robfrank (ro.franchini@gmail.com)
 */
class Issue12025CrashEquivalentCloseCheckpointTest {
  private static final String CLOSED_DB_NAME = "closedCrashEquivalent";
  private static final String OPEN_DB_NAME   = "keepsCommitting";
  private static final long   APPLIED_TERM   = 3L;

  @TempDir
  Path tempDir;

  private ArcadeDBServer     server;
  private LocalDatabase      closing;
  private LocalDatabase      committing;
  private Database           elsewhere;
  private RaftStorage        storage;
  private ArcadeStateMachine sm;

  @BeforeEach
  void setUp() throws Exception {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, databasesDirectory().toString());
    server = new ArcadeDBServer(config);

    closing = createDatabase(CLOSED_DB_NAME);
    committing = createDatabase(OPEN_DB_NAME);

    storage = RaftStorage.newBuilder().setDirectory(tempDir.resolve("raft").toFile()).setOption(RaftStorage.StartupOption.FORMAT)
        .build();
    sm = new ArcadeStateMachine();
    sm.setServer(server);
    sm.initialize(stubServer(), RaftGroupId.valueOf(UUID.randomUUID()), storage);
  }

  @AfterEach
  void tearDown() throws IOException {
    if (sm != null)
      sm.close();
    if (storage != null)
      storage.close();
    dropIfPresent(databasesDirectory().resolve(CLOSED_DB_NAME));
    dropIfPresent(databasesDirectory().resolve(OPEN_DB_NAME));
    if (elsewhere != null)
      dropIfPresent(Path.of(elsewhere.getDatabasePath()));
    server.stop();
  }

  /**
   * The scenario of the issue: entry 10 applied to the database that then closed crash-equivalent, entries up to 20 applied to
   * another one. The checkpoint must not cover entry 10, and the earlier marker stays where it was.
   */
  @Test
  void aCrashEquivalentCloseHoldsTheCheckpointWhileAnotherDatabaseKeepsCommitting() throws Exception {
    insert(closing, 50);
    setLastApplied(sm, APPLIED_TERM, 5L);
    assertThat(sm.takeSnapshot()).as("the fixture: a healthy checkpoint before the close").isEqualTo(5L);

    insert(closing, 50);
    setLastApplied(sm, APPLIED_TERM, 10L);
    closing.fenceForRecovery("test: issue #12025");
    closing.close();
    assertThat(new File(closing.getDatabasePath(), "database.lck")).as("the fixture: the close was crash-equivalent").exists();

    insert(committing, 50);
    setLastApplied(sm, APPLIED_TERM, 20L);

    assertThat(sm.takeSnapshot())
        .as("the closed database's pages since the last marker may be in the OS cache only: no purge may be authorised")
        .isEqualTo(RaftLog.INVALID_LOG_INDEX);
    assertThat(sm.getStateMachineStorage().getLatestSnapshot().getIndex()).as("the previous marker stands").isEqualTo(5L);
    assertThat(sm.databasesClosedWithPagesLeftToRecovery()).containsExactly(CLOSED_DB_NAME);
  }

  /** Reopening the database runs its recovery, and the next checkpoint's sync of the new instance releases the hold. */
  @Test
  void theCheckpointGoesThroughOnceTheDatabaseIsReopenedAndForced() throws Exception {
    insert(closing, 50);
    closing.fenceForRecovery("test: issue #12025");
    closing.close();
    insert(committing, 50);
    setLastApplied(sm, APPLIED_TERM, 20L);
    assertThat(sm.takeSnapshot()).isEqualTo(RaftLog.INVALID_LOG_INDEX);

    final LocalDatabase reopened = (LocalDatabase) new DatabaseFactory(closing.getDatabasePath()).open();
    assertThat(sm.databasesClosedWithPagesLeftToRecovery()).as("opening alone is not the proof: the checkpoint's own sync is")
        .containsExactly(CLOSED_DB_NAME);

    assertThat(sm.takeSnapshot()).as("the reopened database was forced by the checkpoint itself").isEqualTo(20L);
    assertThat(unsyncedFiles(reopened)).isEmpty();
    assertThat(sm.databasesClosedWithPagesLeftToRecovery()).isEmpty();
    assertThat(reopened.query("sql", "SELECT count(*) AS c FROM Account").next().<Long>getProperty("c")).isEqualTo(50L);
  }

  /** A dropped database has nothing left to lose. */
  @Test
  void aDroppedDatabaseNoLongerHoldsTheCheckpoint() throws Exception {
    insert(closing, 50);
    closing.fenceForRecovery("test: issue #12025");
    closing.close();
    setLastApplied(sm, APPLIED_TERM, 20L);
    assertThat(sm.takeSnapshot()).isEqualTo(RaftLog.INVALID_LOG_INDEX);

    new DatabaseFactory(closing.getDatabasePath()).open().drop();

    assertThat(sm.takeSnapshot()).isEqualTo(20L);
  }

  /** A clean close forced the files: it holds nothing, as before. */
  @Test
  void aCleanCloseDoesNotHoldTheCheckpoint() throws Exception {
    insert(closing, 50);
    closing.close();
    insert(committing, 50);
    setLastApplied(sm, APPLIED_TERM, 20L);

    assertThat(sm.takeSnapshot()).isEqualTo(20L);
  }

  /** A JVM hosting several servers: a crash-equivalent close under another server's directory is not this node's. */
  @Test
  void aDatabaseOfAnotherServerDirectoryDoesNotHoldTheCheckpoint() throws Exception {
    final LocalDatabase other = (LocalDatabase) new DatabaseFactory(tempDir.resolve("otherServer").resolve("db").toString()).create();
    elsewhere = other;
    other.getSchema().createDocumentType("Account");
    insert(other, 10);
    other.fenceForRecovery("test: issue #12025");
    other.close();
    assertThat(DatabaseFactory.getDatabasesClosedWithPagesLeftToRecovery())
        .as("the fixture: the engine lists it").contains(Path.of(other.getDatabasePath()).toAbsolutePath().normalize());

    setLastApplied(sm, APPLIED_TERM, 20L);
    assertThat(sm.takeSnapshot()).isEqualTo(20L);
  }

  private Path databasesDirectory() {
    return tempDir.resolve("databases");
  }

  private LocalDatabase createDatabase(final String name) {
    final LocalDatabase db = (LocalDatabase) new DatabaseFactory(databasesDirectory().resolve(name).toString()).create();
    db.getSchema().createDocumentType("Account");
    server.registerDatabase(name, db);
    return db;
  }

  private static void dropIfPresent(final Path path) {
    if (DatabaseFactory.getActiveDatabaseInstance(path.toString()) instanceof Database open && open.isOpen())
      open.drop();
    else {
      final DatabaseFactory factory = new DatabaseFactory(path.toString());
      if (factory.exists())
        factory.open().drop();
      factory.close();
    }
  }

  private static void insert(final LocalDatabase db, final int count) {
    db.transaction(() -> {
      for (int i = 0; i < count; i++)
        db.newDocument("Account").set("acctId", i).set("balance", 100).save();
    });
  }

  private static List<String> unsyncedFiles(final LocalDatabase db) {
    final List<String> unsynced = new ArrayList<>();
    for (final ComponentFile f : db.getFileManager().getFiles())
      if (f instanceof PaginatedComponentFile pcf && pcf.isModifiedSinceLastSync())
        unsynced.add(pcf.getFileName());
    return unsynced;
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

  /** Advances the applied position the way {@code applyTransaction} does (see {@code AppliedIndexCheckpointTest}). */
  private static void setLastApplied(final ArcadeStateMachine sm, final long term, final long index) throws Exception {
    final Field f = ArcadeStateMachine.class.getDeclaredField("lastAppliedIndex");
    f.setAccessible(true);
    ((AtomicLong) f.get(sm)).set(index);

    for (Class<?> c = sm.getClass(); c != null; c = c.getSuperclass())
      try {
        final Method m = c.getDeclaredMethod("updateLastAppliedTermIndex", long.class, long.class);
        m.setAccessible(true);
        m.invoke(sm, term, index);
        return;
      } catch (final NoSuchMethodException ignored) {
        // walk up to the superclass
      }
    throw new NoSuchMethodException("updateLastAppliedTermIndex");
  }
}
