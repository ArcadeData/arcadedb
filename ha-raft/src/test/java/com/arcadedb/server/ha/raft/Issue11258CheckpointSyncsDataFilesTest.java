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

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11258: the Raft snapshot checkpoint tells Ratis that the state up to the applied index needs no replay, and a
 * restart replays only what follows the marker. Neither a replicated apply nor a local commit fsyncs a data file, so a
 * marker written over pages that were only in the OS cache made a power loss lose them for good: the Jepsen LazyFS runs
 * found the Account bucket and the dictionary at 0 bytes, crash recovery `completed` over them, and no entry to replay.
 * The checkpoint now flushes and forces the data files of every open database before the marker, and refuses when it
 * cannot.
 * <p>
 * A real (unstarted) {@link ArcadeDBServer} with a real {@link LocalDatabase} is registered, so the state machine reaches
 * the database the way it does in production (see {@code ColumnarRaftCheckpointTest}).
 */
class Issue11258CheckpointSyncsDataFilesTest {
  private static final String DB_NAME       = "checkpointSync";
  private static final long   APPLIED_INDEX = 42L;
  private static final long   APPLIED_TERM  = 3L;

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
    db.getSchema().createDocumentType("Account");
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
    if (sm != null)
      sm.close();
    if (storage != null)
      storage.close();
    if (db != null && db.isOpen())
      db.drop();
    server.stop();
  }

  /** Pages written to the files but not forced are forced by the checkpoint, before Ratis may purge the log of them. */
  @Test
  void theCheckpointForcesTheDataFilesWrittenSinceTheLastFsync() {
    insertAccounts(200);
    assertThat(db.getPageManager().waitAllPagesOfDatabaseAreFlushed(db)).as("the fixture: the pages reached the files").isTrue();
    assertThat(unsyncedFiles()).as("the fixture: the pages are in the files but not forced, as after an apply").isNotEmpty();

    assertThat(sm.takeSnapshot()).as("a healthy checkpoint still reports the applied index").isEqualTo(APPLIED_INDEX);

    assertThat(unsyncedFiles()).as("no data file still owes the disk anything once the marker is written").isEmpty();
    assertThat(sm.getStateMachineStorage().getLatestSnapshot().getIndex()).isEqualTo(APPLIED_INDEX);
  }

  /** Pages a commit handed to the asynchronous flush are written and forced too, not only those already in the files. */
  @Test
  void pagesPendingInTheAsynchronousFlushAreWrittenAndForcedBeforeTheMarker() {
    insertAccounts(200);

    assertThat(sm.takeSnapshot()).isEqualTo(APPLIED_INDEX);

    assertThat(db.getPageManager().waitPagesPendingNowOfDatabaseAreFlushed(db, 0L))
        .as("nothing committed before the checkpoint is still waiting for the flush").isTrue();
    assertThat(unsyncedFiles()).as("and what it wrote was forced").isEmpty();
  }

  /**
   * While a backup suspends the flush, the committed pages are deferred and cannot reach the files: the checkpoint is
   * refused at once rather than waiting the suspension out on the apply thread, and no marker covers them.
   */
  @Test
  void aSuspendedFlushWithDeferredPagesRefusesTheCheckpoint() throws Exception {
    final AtomicReference<Long> checkpoint = new AtomicReference<>();
    db.getPageManager().suspendFlushAndExecute(db, () -> {
      insertAccounts(50);
      checkpoint.set(sm.takeSnapshot());
    });

    assertThat(checkpoint.get()).as("the pages deferred by the suspension are not durable, so no checkpoint covers them")
        .isEqualTo(RaftLog.INVALID_LOG_INDEX);
    assertThat(sm.getStateMachineStorage().getLatestSnapshot()).as("no snapshot marker authorises a purge").isNull();

    assertThat(sm.takeSnapshot()).as("once the suspension ended the next checkpoint goes through").isEqualTo(APPLIED_INDEX);
    assertThat(unsyncedFiles()).isEmpty();
  }

  /** A closed database was forced when it closed: it neither blocks nor costs the checkpoint anything. */
  @Test
  void aClosedDatabaseIsSkipped() {
    insertAccounts(10);
    db.close();

    assertThat(sm.takeSnapshot()).isEqualTo(APPLIED_INDEX);
  }

  private void insertAccounts(final int count) {
    db.transaction(() -> {
      for (int i = 0; i < count; i++)
        db.newDocument("Account").set("acctId", i).set("balance", 100).save();
    });
  }

  private List<String> unsyncedFiles() {
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
