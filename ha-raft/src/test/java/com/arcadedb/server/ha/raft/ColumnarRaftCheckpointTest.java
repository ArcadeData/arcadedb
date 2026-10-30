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
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.index.columnar.ColumnarStore;
import com.arcadedb.index.columnar.ColumnarTestData;
import com.arcadedb.index.columnar.ColumnarTestHooks;
import com.arcadedb.index.columnar.FacetOracle;
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

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The Raft checkpoint and the columnar recovery set (plan 3, Task 22). A follower writes no local WAL, so the
 * {@code cleanWALFiles} hook that persists the recovery set never covers a replicated change: the pages an applied
 * entry marked can live only in the trackers' memory. {@code takeSnapshot()} tells Ratis the log may be purged up to
 * the applied index, and a restart replays only what follows it, so the marks of every entry it covers must be on disk
 * before the marker is: the recovery set is persisted first, and a set that cannot be written aborts the checkpoint.
 * <p>
 * A real (unstarted) {@link ArcadeDBServer} with a real {@link LocalDatabase} holding a COLUMNAR index is registered,
 * so the state machine reaches the database the way it does in production.
 */
class ColumnarRaftCheckpointTest {
  private static final String DB_NAME        = "columnarCheckpoint";
  private static final long   APPLIED_INDEX  = 42L;
  private static final long   APPLIED_TERM   = 3L;
  /** {@code ColumnarIndexManager.RECOVERY_SET_PERSISTED}: reached once the recovery set is renamed into place. */
  private static final String PERSISTED_HOOK = "recoverySet.persisted";

  @TempDir
  Path tempDir;

  private ArcadeDBServer     server;
  private LocalDatabase      db;
  private RaftStorage        storage;
  private ArcadeStateMachine sm;

  @BeforeEach
  void setUp() throws Exception {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, tempDir.resolve("databases").toString());
    server = new ArcadeDBServer(config);

    db = (LocalDatabase) new DatabaseFactory(tempDir.resolve("databases").resolve(DB_NAME).toString()).create();
    final ColumnarTestData data = new ColumnarTestData(22L);
    data.createSchema(db);
    data.insertAssets(db, 300);
    FacetOracle.awaitCaughtUp(db);
    server.registerDatabase(DB_NAME, db);

    storage = RaftStorage.newBuilder().setDirectory(tempDir.resolve("raft").toFile()).setOption(RaftStorage.StartupOption.FORMAT)
        .build();
    sm = new ArcadeStateMachine();
    sm.setServer(server);
    sm.initialize(stubServer(), RaftGroupId.valueOf(UUID.randomUUID()), storage);
    setLastApplied(sm, APPLIED_TERM, APPLIED_INDEX);
  }

  @AfterEach
  void tearDown() throws IOException {
    ColumnarTestHooks.killPoint = null;
    if (sm != null)
      sm.close();
    if (storage != null)
      storage.close();
    if (db != null && db.isOpen())
      db.drop();
    server.stop();
  }

  /** A recovery set that cannot be written must keep Ratis from purging the log of the entries whose marks it holds. */
  @Test
  void aRecoverySetThatCannotBePersistedAbortsTheCheckpoint() throws IOException {
    // a non-empty directory where the recovery set's tmp file goes: the write fails with an IOException, as a full or
    // read-only volume makes it fail
    final Path tmp = Path.of(db.getDatabasePath()).resolve(ColumnarStore.ROOT_DIR).resolve("recovery.pages.tmp");
    Files.createDirectories(tmp);
    Files.writeString(tmp.resolve("blocker"), "x");

    assertThat(db.getColumnarIndexManager().persistRecoverySet()).as("the fixture: the recovery set cannot be written").isFalse();

    assertThat(sm.takeSnapshot()).as("the checkpoint is refused, as a WAL file is kept when the set cannot be written")
        .isEqualTo(RaftLog.INVALID_LOG_INDEX);
    assertThat(sm.getStateMachineStorage().getLatestSnapshot()).as("no snapshot marker authorises a purge").isNull();
  }

  /** The recovery set is on disk before the marker: a crash between the two leaves the log replayable, never a purged gap. */
  @Test
  void theRecoverySetIsPersistedBeforeTheSnapshotMarker() {
    final AtomicBoolean persisted = new AtomicBoolean();
    final AtomicBoolean markerAlreadyRegistered = new AtomicBoolean();
    ColumnarTestHooks.killPoint = point -> {
      if (PERSISTED_HOOK.equals(point)) {
        persisted.set(true);
        markerAlreadyRegistered.set(sm.getStateMachineStorage().getLatestSnapshot() != null);
      }
    };

    assertThat(sm.takeSnapshot()).as("a healthy checkpoint still reports the applied index").isEqualTo(APPLIED_INDEX);

    assertThat(persisted.get()).as("the checkpoint persisted the recovery set of the open database").isTrue();
    assertThat(markerAlreadyRegistered.get()).as("the recovery set is written before the snapshot marker").isFalse();
    assertThat(sm.getStateMachineStorage().getLatestSnapshot().getIndex()).isEqualTo(APPLIED_INDEX);
  }

  /** A database an embedder registered from outside the databases directory is still covered by the checkpoint. */
  @Test
  void aDatabaseRegisteredFromAnotherPathIsPersistedToo() throws IOException {
    final LocalDatabase elsewhere = (LocalDatabase) new DatabaseFactory(tempDir.resolve("elsewhere").resolve("other").toString()).create();
    try {
      final ColumnarTestData data = new ColumnarTestData(23L);
      data.createSchema(elsewhere);
      data.insertAssets(elsewhere, 50);
      FacetOracle.awaitCaughtUp(elsewhere);
      server.registerDatabase("registeredElsewhere", elsewhere);
      final Path tmp = Path.of(elsewhere.getDatabasePath()).resolve(ColumnarStore.ROOT_DIR).resolve("recovery.pages.tmp");
      Files.createDirectories(tmp);
      Files.writeString(tmp.resolve("blocker"), "x");

      assertThat(sm.takeSnapshot()).as("its recovery set was attempted, and its failure refuses the checkpoint")
          .isEqualTo(RaftLog.INVALID_LOG_INDEX);
    } finally {
      server.removeDatabase("registeredElsewhere");
      elsewhere.drop();
    }
  }

  /** A closed database has nothing in memory to lose: it neither blocks nor costs the checkpoint anything. */
  @Test
  void aClosedDatabaseIsSkipped() throws IOException {
    final Path tmp = Path.of(db.getDatabasePath()).resolve(ColumnarStore.ROOT_DIR).resolve("recovery.pages.tmp");
    db.close();
    Files.createDirectories(tmp);
    Files.writeString(tmp.resolve("blocker"), "x");

    assertThat(sm.takeSnapshot()).isEqualTo(APPLIED_INDEX);
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

  /** Advances the applied position the way {@code applyTransaction} does (see {@code ArcadeStateMachineSnapshotMarkerTest}). */
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
