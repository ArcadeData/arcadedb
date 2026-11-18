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
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.FakeArcadeDBServer;
import com.arcadedb.server.FakeServerSecurity;
import com.arcadedb.server.ha.raft.RaftLogEntryCodec.TsSealedChunk;
import com.arcadedb.server.security.ReplicatedSecurityConfigPersistenceException;
import com.arcadedb.server.security.ReplicatedUsersPersistenceException;
import com.arcadedb.utility.FileUtils;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.proto.RaftProtos.StateMachineLogEntryProto;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The applied index and its checkpoint: durability of .raft/applied-index, data files synced before the checkpoint, skipped entries
 * holding the checkpoint, and the tolerated applied-term regression.
 */
class AppliedIndexCheckpointTest {

  /**
   * Regression tests for issue #11517: {@code .raft/applied-index} carries the quarantine (#7735) and the read floors
   * (#8137), and was written with no fsync of the file nor of its directory, so a power loss could bring it back at 0
   * bytes and the node back without the quarantine it had recorded.
   * <p>
   * The file is rewritten once per applied entry, so fsyncing every write would put two fsyncs per Raft entry on the apply
   * thread. The invariant pinned here instead: <b>every write whose content a restart must not lose is fsynced</b> - any
   * write while a quarantine or a read floor stands, the write that clears the last of them, the first write of the state
   * machine (the file's existence is what the offline-bootstrap gate reads), the rare positions a snapshot install, a
   * resync or the operator's accept-stale-snapshot record, and the write that precedes a snapshot marker - <b>while the
   * per-entry write of a healthy node stays unsynced</b>.
   * <p>
   * Durability is observed through {@link FileUtils#TEST_FILE_SYNC_HOOK}, which reports every content fsync with the
   * path it publishes, so the tests count the flushes of this one file without mocking the JDK.
   */
  @Nested
  class AppliedIndexDurability {

    private static final String DB_A = "orders";
    private static final String DB_B = "invoices";

    private final List<Path> synced = new CopyOnWriteArrayList<>();

    @BeforeEach
    void captureSyncs() {
      GlobalConfiguration.FILE_SYNC.setValue(true);
      FileUtils.TEST_FILE_SYNC_HOOK = synced::add;
    }

    @AfterEach
    void releaseHook() {
      FileUtils.TEST_FILE_SYNC_HOOK = null;
      GlobalConfiguration.FILE_SYNC.reset();
    }

    // Regression for #11517: AppliedIndexDurability
    @Test
    void theFirstWriteOfAStateMachineIsFsyncedAndThePerEntryWritesAfterItAreNot(@TempDir final Path tempDir) throws Exception {
      final ArcadeStateMachine sm = newStateMachine(tempDir);
      try {
        sm.writePersistedAppliedIndex(1, DB_A);
        assertThat(syncsOfAppliedIndex(tempDir))
            .as("the file's existence is what hasNeverAppliedApplicationEntry() reads, so its creation must survive a power loss")
            .isEqualTo(1);

        for (long i = 2; i <= 50; i++)
          sm.writePersistedAppliedIndex(i, DB_A);
        sm.writePersistedDatabaseAppliedIndex(51, DB_B, false);
        sm.writePersistedAppliedIndexDroppingDatabase(52, DB_B);

        assertThat(syncsOfAppliedIndex(tempDir))
            .as("a healthy node's per-entry position is best effort: an fsync per Raft entry on the apply thread is the cost "
                + "issue #11517 declined")
            .isEqualTo(1);
      } finally {
        sm.close();
      }
    }

    // Regression for #11517: AppliedIndexDurability
    @Test
    void aQuarantineMarkIsFsynced(@TempDir final Path tempDir) throws Exception {
      final ArcadeStateMachine sm = newStateMachine(tempDir);
      try {
        sm.writePersistedAppliedIndex(1, DB_A);
        sm.writePersistedAppliedIndex(2, DB_A);
        final int before = syncsOfAppliedIndex(tempDir);

        sm.markStateDiverged(DB_A, DivergenceCause.WAL_VERSION_GAP);

        assertThat(syncsOfAppliedIndex(tempDir))
            .as("a quarantine written without an fsync comes back after a power loss as a 0-byte file, without it")
            .isEqualTo(before + 1);
      } finally {
        sm.close();
      }
    }

    // Regression for #11517: AppliedIndexDurability
    @Test
    void everyWriteIsFsyncedWhileAQuarantineStands(@TempDir final Path tempDir) throws Exception {
      final ArcadeStateMachine sm = newStateMachine(tempDir);
      try {
        sm.writePersistedAppliedIndex(1, DB_A);
        sm.markStateDiverged(DB_A, DivergenceCause.APPLY_ERROR);
        final int before = syncsOfAppliedIndex(tempDir);

        sm.writePersistedAppliedIndex(2, DB_B);
        sm.writePersistedAppliedIndex(3, DB_B);
        sm.writePersistedDatabaseAppliedIndex(4, DB_B, false);

        assertThat(syncsOfAppliedIndex(tempDir))
            .as("every rewrite carries the quarantine, and an unsynced rename over the durable copy can leave a 0-byte file "
                + "in its place after a power loss")
            .isEqualTo(before + 3);
      } finally {
        sm.close();
      }
    }

    // Regression for #11517: AppliedIndexDurability
    @Test
    void theWriteThatClearsTheQuarantineIsFsyncedAndTheNextOneIsNot(@TempDir final Path tempDir) throws Exception {
      final ArcadeStateMachine sm = newStateMachine(tempDir);
      try {
        sm.writePersistedAppliedIndex(1, DB_A);
        sm.markStateDiverged(DB_A, DivergenceCause.WAL_VERSION_GAP);
        final int before = syncsOfAppliedIndex(tempDir);

        sm.clearDivergedDatabase(DB_A);
        assertThat(syncsOfAppliedIndex(tempDir))
            .as("a lost clear brings a healed database back quarantined after the power loss")
            .isEqualTo(before + 1);

        sm.writePersistedAppliedIndex(2, DB_A);
        sm.writePersistedAppliedIndex(3, DB_A);
        assertThat(syncsOfAppliedIndex(tempDir))
            .as("once the cleared state is durable, nothing a later unsynced write loses is a quarantine")
            .isEqualTo(before + 1);
      } finally {
        sm.close();
      }
    }

    // Regression for #11517: AppliedIndexDurability
    @Test
    void aQuarantineRestoredFromDiskIsClearedDurablyAfterTheRestart(@TempDir final Path tempDir) throws Exception {
      final ArcadeStateMachine before = newStateMachine(tempDir);
      try {
        before.writePersistedAppliedIndex(1, DB_A);
        before.markStateDiverged(DB_A, DivergenceCause.UNDECODABLE_LOG_ENTRY);
      } finally {
        before.close();
      }

      final ArcadeStateMachine after = newStateMachine(tempDir);
      try {
        assertThat(after.isDatabaseDiverged(DB_A)).isTrue();
        final int syncsBefore = syncsOfAppliedIndex(tempDir);

        after.clearDivergedDatabase(DB_A);

        assertThat(syncsOfAppliedIndex(tempDir)).isEqualTo(syncsBefore + 1);
      } finally {
        after.close();
      }
    }

    // Regression for #11517: AppliedIndexDurability
    @Test
    void theSnapshotInstallAndTheResyncRecordTheirPositionDurably(@TempDir final Path tempDir) throws Exception {
      final ArcadeStateMachine sm = newStateMachine(tempDir);
      try {
        sm.writePersistedAppliedIndex(1, DB_A);
        int before = syncsOfAppliedIndex(tempDir);

        sm.completeSnapshotInstall(100, Set.of());
        assertThat(syncsOfAppliedIndex(tempDir))
            .as("the install's position is what the next reinitialize() compares the marker against")
            .isEqualTo(before + 1);

        before = syncsOfAppliedIndex(tempDir);
        sm.writePersistedAppliedIndexForAllDatabases(200);
        assertThat(syncsOfAppliedIndex(tempDir)).isEqualTo(before + 1);
      } finally {
        sm.close();
      }
    }

    // Regression for #11517: AppliedIndexDurability
    @Test
    void theOperatorsAcceptOfAStaleSnapshotIsRecordedDurably(@TempDir final Path tempDir) throws Exception {
      final RaftStorage raftStorage = RaftStorage.newBuilder()
          .setDirectory(tempDir.resolve("raft-storage").toFile())
          .setOption(RaftStorage.StartupOption.FORMAT)
          .build();
      final ArcadeStateMachine sm = newStateMachine(tempDir);
      try {
        sm.initialize(stubRaftServer(), RaftGroupId.valueOf(UUID.randomUUID()), raftStorage);
        sm.writePersistedAppliedIndex(10, DB_A);
        final Field floor = ArcadeStateMachine.class.getDeclaredField("staleSnapshotAppliedFloor");
        floor.setAccessible(true);
        ((AtomicLong) floor.get(sm)).set(10L);
        final int before = syncsOfAppliedIndex(tempDir);

        assertThat(sm.acceptStaleSnapshotFloor("root")).isNotNull();

        assertThat(syncsOfAppliedIndex(tempDir))
            .as("an accept lost to a power loss raises the floor again at the next reinitialize()")
            .isEqualTo(before + 1);
      } finally {
        sm.close();
        raftStorage.close();
      }
    }

    // Regression for #11517: AppliedIndexDurability
    @Test
    void strayTempFilesOfAnInterruptedDurableWriteAreRemovedOnLoad(@TempDir final Path tempDir) throws Exception {
      final Path raftDir = appliedIndexFile(tempDir).getParent();
      Files.createDirectories(raftDir);
      final Path stray = raftDir.resolve("applied-index.123456789.tmp");
      Files.writeString(stray, "{\"global\":");
      final Path unrelated = raftDir.resolve("bootstrap-baselines.987.tmp");
      Files.writeString(unrelated, "x");

      final ArcadeStateMachine sm = newStateMachine(tempDir);
      try {
        assertThat(sm.readPersistedAppliedIndex()).isEqualTo(-1L);
        assertThat(stray).as("left behind by a crash inside FileUtils.atomicWriteFile").doesNotExist();
        assertThat(unrelated).as("only this file's own temporaries are removed").exists();
      } finally {
        sm.close();
      }
    }

    // Regression for #11517: AppliedIndexDurability
    @Test
    void takeSnapshotFsyncsTheAppliedIndexBeforeItWritesTheMarker(@TempDir final Path tempDir) throws Exception {
      final RaftStorage raftStorage = RaftStorage.newBuilder()
          .setDirectory(tempDir.resolve("raft-storage").toFile())
          .setOption(RaftStorage.StartupOption.FORMAT)
          .build();
      final ArcadeStateMachine sm = newStateMachine(tempDir);
      try {
        sm.initialize(stubRaftServer(), RaftGroupId.valueOf(UUID.randomUUID()), raftStorage);
        sm.writePersistedAppliedIndex(4242, DB_A);
        setLastApplied(sm, 7L, 4242L);
        final int before = syncsOfAppliedIndex(tempDir);

        assertThat(sm.takeSnapshot()).isEqualTo(4242L);

        assertThat(syncsOfAppliedIndex(tempDir))
            .as("a position at or past the marker must be on disk, so a power loss cannot leave an older position behind "
                + "that reinitialize() reads as a gap below the marker and answers with a full resync")
            .isEqualTo(before + 1);
      } finally {
        sm.close();
        raftStorage.close();
      }
    }

    // Regression for #11517: AppliedIndexDurability
    @Test
    void aZeroByteFileReadsAsNothingPersistedAndStillCountsAsApplied(@TempDir final Path tempDir) throws Exception {
      writeAppliedIndexFile(tempDir, "");
      final ArcadeStateMachine sm = newStateMachine(tempDir);
      try {
        assertThat(sm.readPersistedAppliedIndex()).isEqualTo(-1L);
        assertThat(sm.readPersistedAppliedIndex(DB_A)).isEqualTo(-1L);
        assertThat(sm.isDatabaseDiverged(DB_A)).isFalse();
        assertThat(sm.hasNeverAppliedApplicationEntry())
            .as("the file exists, so the node is not fresh whatever it holds")
            .isFalse();
      } finally {
        sm.close();
      }
    }

    // Regression for #11517: AppliedIndexDurability
    @Test
    void aTornFileReadsAsNothingPersistedRatherThanFailing(@TempDir final Path tempDir) throws Exception {
      writeAppliedIndexFile(tempDir, "{\"global\":12,\"db\":{\"orders\":1");
      final ArcadeStateMachine sm = newStateMachine(tempDir);
      try {
        assertThat(sm.readPersistedAppliedIndex()).isEqualTo(-1L);
        assertThat(sm.readPersistedAppliedIndex(DB_A)).isEqualTo(-1L);
        assertThat(sm.hasNeverAppliedApplicationEntry()).isFalse();
      } finally {
        sm.close();
      }
    }

    // ---------------------------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------------------------

    private int syncsOfAppliedIndex(final Path tempDir) {
      final Path file = appliedIndexFile(tempDir).toAbsolutePath();
      int count = 0;
      for (final Path p : synced)
        if (p.toAbsolutePath().equals(file))
          count++;
      return count;
    }

    private static ArcadeStateMachine newStateMachine(final Path tempDir) {
      final ContextConfiguration config = new ContextConfiguration();
      config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, tempDir.resolve("databases").toString());
      final ArcadeStateMachine sm = new ArcadeStateMachine();
      sm.setServer(new ArcadeDBServer(config));
      return sm;
    }

    private static Path appliedIndexFile(final Path tempDir) {
      return tempDir.resolve("databases").resolve(".raft").resolve("applied-index");
    }

    private static void writeAppliedIndexFile(final Path tempDir, final String content) throws IOException {
      final Path file = appliedIndexFile(tempDir);
      Files.createDirectories(file.getParent());
      Files.writeString(file, content);
    }

    private static void setLastApplied(final ArcadeStateMachine sm, final long term, final long index) throws Exception {
      final Field f = ArcadeStateMachine.class.getDeclaredField("lastAppliedIndex");
      f.setAccessible(true);
      ((AtomicLong) f.get(sm)).set(index);

      final Method m = findMethod(sm.getClass(), "updateLastAppliedTermIndex", long.class, long.class);
      m.setAccessible(true);
      m.invoke(sm, term, index);
    }

    private static Method findMethod(final Class<?> type, final String name, final Class<?>... params)
        throws NoSuchMethodException {
      for (Class<?> c = type; c != null; c = c.getSuperclass()) {
        try {
          return c.getDeclaredMethod(name, params);
        } catch (final NoSuchMethodException ignored) {
          // walk up to the superclass
        }
      }
      throw new NoSuchMethodException(name);
    }

    private static RaftServer stubRaftServer() {
      return (RaftServer) Proxy.newProxyInstance(
          AppliedIndexCheckpointTest.class.getClassLoader(),
          new Class<?>[] { RaftServer.class },
          (proxy, method, args) -> {
            if ("getId".equals(method.getName()))
              return RaftPeerId.valueOf("test-peer");
            if ("close".equals(method.getName()) || "start".equals(method.getName()))
              return null;
            throw new UnsupportedOperationException("Stub: " + method.getName());
          });
    }
  }

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
  @Nested
  class CheckpointSyncsDataFiles {
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
    // Regression for #11258: CheckpointSyncsDataFiles
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
    // Regression for #11258: CheckpointSyncsDataFiles
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
    // Regression for #11258: CheckpointSyncsDataFiles
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
    // Regression for #11258: CheckpointSyncsDataFiles
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

  /**
   * Regression test for issue #10086: an entry that did not advance {@code lastAppliedIndex} must leave behind one of a
   * quarantine, a replay floor or a halt, because those are the only three things {@code takeSnapshot()} consults before
   * telling Ratis "state up to here is durable, you may purge the log up to it". Without one of them a later committed
   * entry for any database advances the index over the skipped one, the checkpoint covers it, Ratis purges an entry this
   * node never applied, and the persisted position moves with it so the restart does not replay it either.
   * <p>
   * The table is over the dispositions by which an apply can finish without advancing the index, rather than one test per
   * trigger, so the next throw site on the apply path is covered by construction: each row fails an apply, applies a LATER
   * entry that moves the index past it, and asks {@code takeSnapshot()} for a checkpoint, which must stop short of the
   * skipped entry.

   */
  @Nested
  class SkippedEntryHoldsTheCheckpoint {

    private static final String DB_NAME          = "db10086";
    private static final long   SKIPPED_INDEX    = 5L;
    private static final long   LATER_INDEX      = 9L;
    private static final String USERS_JSON       = "[{\"name\":\"root\",\"password\":\"x\"}]";
    private static final String GROUPS_JSON      = "{\"groups\":{}}";
    private static final String API_TOKENS_JSON  = "{\"tokens\":[]}";
    private static final String CONFIGURATION    = "{\"configuration\":{\"typeDefaultBuckets\":5},\"removed\":[]}";
    private static final String TYPE_NAME        = "Cpu";

    @TempDir
    Path tempDir;

    private int                prevRetries;
    private int                prevDelay;
    private RaftStorage        storage;
    private ArcadeStateMachine sm;

    @BeforeEach
    void setUp() throws IOException {
      prevRetries = GlobalConfiguration.TX_RETRIES.getValueAsInteger();
      prevDelay = GlobalConfiguration.TX_RETRY_DELAY.getValueAsInteger();
      GlobalConfiguration.TX_RETRIES.setValue(1);
      GlobalConfiguration.TX_RETRY_DELAY.setValue(0);
      storage = RaftStorage.newBuilder().setDirectory(tempDir.resolve("raft").toFile())
          .setOption(RaftStorage.StartupOption.FORMAT).build();
    }

    @AfterEach
    void tearDown() throws IOException {
      GlobalConfiguration.TX_RETRIES.setValue(prevRetries);
      GlobalConfiguration.TX_RETRY_DELAY.setValue(prevDelay);
      Thread.interrupted();
      if (sm != null)
        sm.close();
      storage.close();
    }

    /** How an apply fails without advancing the applied index. */
    enum Skipped {
      /** A page-version race that outlasts the retry budget, on a database. */
      RETRY_EXHAUSTED_ON_A_DATABASE,
      /** The same, on an entry that names no database. */
      RETRY_EXHAUSTED_WITHOUT_A_DATABASE,
      /** The retry loop interrupted, which is a node going down: the entry is left for the replay. */
      RETRY_INTERRUPTED,
      /** Any {@link ReplicationException} nobody has dealt with, which is what a future throw site raises. */
      BARE_REPLICATION_EXCEPTION_ON_A_DATABASE,
      /** A sealed slice that does not continue the sequence staged so far. */
      SEALED_SLICE_SEQUENCE_BROKEN,
      /** The security users file could not be written. */
      SECURITY_USERS_NOT_PERSISTED,
      /** The security groups file could not be written. */
      SECURITY_GROUPS_NOT_PERSISTED,
      /** The API tokens file could not be written. */
      SECURITY_API_TOKENS_NOT_PERSISTED
    }

    // Regression for #10086: SkippedEntryHoldsTheCheckpoint
    @ParameterizedTest
    @EnumSource(Skipped.class)
    void aSkippedEntryIsNeverCheckpointedOver(final Skipped skipped) throws Exception {
      sm = stateMachine(securityServer(skipped));

      final Throwable failure = failTheApply(skipped);
      // An interrupt is how a node going down ends the retry loop: it must not leak into the entries applied next
      Thread.interrupted();
      assertThat(failure).as("the apply fails, so the index does not move over it").isInstanceOf(ReplicationException.class);
      assertThat(sm.readAppliedIndexCounter()).isLessThan(SKIPPED_INDEX);

      // A later committed entry, of another kind of document, applies and carries the index past the skipped one
      final CompletableFuture<?> later = sm.applyTransaction(securityEntry(laterKind(skipped), LATER_INDEX));
      assertThat(later.isCompletedExceptionally()).as("the later entry applies").isFalse();
      assertThat(sm.readAppliedIndexCounter()).isEqualTo(LATER_INDEX);

      assertThat(sm.takeSnapshot())
          .as("%s: the checkpoint must stop short of the entry this node did not apply (quarantine, replay floor or halt)", skipped)
          .isLessThan(SKIPPED_INDEX);

      assertThat(sm.isDatabaseDiverged(DB_NAME)).as("%s: only a failure that left the database unknown quarantines it", skipped)
          .isEqualTo(quarantinesTheDatabase(skipped));
      assertThat(sm.isHaltedAfterCriticalError()).as("%s must not take the node down", skipped).isFalse();
    }

    /** A failure on a database that leaves its state unknown is quarantined; a node going down or a document is held instead. */
    private static boolean quarantinesTheDatabase(final Skipped skipped) {
      return skipped == Skipped.RETRY_EXHAUSTED_ON_A_DATABASE || skipped == Skipped.BARE_REPLICATION_EXCEPTION_ON_A_DATABASE
          || skipped == Skipped.SEALED_SLICE_SEQUENCE_BROKEN;
    }

    /** A document in force in memory whose file could not be written is replayed by the restart until a later one lands. */
    // Regression for #10086: SkippedEntryHoldsTheCheckpoint
    @Test
    void aLaterEntryOfTheSameDocumentSupersedesTheUnpersistedOne() throws Exception {
      sm = stateMachine(securityServer(Skipped.SECURITY_USERS_NOT_PERSISTED));

      assertThat(sm.applyTransaction(securityEntry(Kind.USERS, SKIPPED_INDEX)).isCompletedExceptionally()).isTrue();
      assertThat(sm.applyTransaction(securityEntry(Kind.GROUPS, 6L)).isCompletedExceptionally()).isFalse();
      assertThat(sm.takeSnapshot()).as("another document does not heal the users file").isLessThan(SKIPPED_INDEX);

      assertThat(sm.applyTransaction(securityEntry(Kind.USERS, LATER_INDEX)).isCompletedExceptionally())
          .as("the volume is writable again").isFalse();
      assertThat(sm.takeSnapshot())
          .as("a whole document written after the failed one carries everything the failed one did: nothing left to replay")
          .isEqualTo(LATER_INDEX);
    }

    /** An entry whose precondition no longer holds installs nothing, so it carries nothing the failed one did not. */
    // Regression for #10086: SkippedEntryHoldsTheCheckpoint
    @Test
    void aStaleLaterEntryOfTheSameDocumentDoesNotReleaseTheHold() {
      final FakeServerSecurity security = FakeServerSecurity.create();
      security.on("applyReplicatedUsers", args -> {
        if (args.length == 1)
          throw new ReplicatedUsersPersistenceException("could NOT be persisted", new IOException("No space left on device"));
        return false;
      });
      final FakeArcadeDBServer server = FakeArcadeDBServer.create();
      server.security(security);
      sm = stateMachine(server);

      assertThat(sm.applyTransaction(securityEntry(Kind.USERS, SKIPPED_INDEX)).isCompletedExceptionally()).isTrue();
      final CompletableFuture<?> stale = sm.applyTransaction(
          rawEntry(RaftLogEntryCodec.encodeSecurityUsersEntry(USERS_JSON, "a-fingerprint-nothing-holds"), LATER_INDEX));
      assertThat(stale.isCompletedExceptionally()).as("a superseded entry is applied, as a no-op").isFalse();
      assertThat(sm.readAppliedIndexCounter()).isEqualTo(LATER_INDEX);

      assertThat(sm.takeSnapshot()).as("nothing was written, so the failed entry is still the one to replay").isLessThan(SKIPPED_INDEX);
    }

    /** A hold at or below what a Raft install covered is dropped: the installed copy carries what the entry did. */
    // Regression for #10086: SkippedEntryHoldsTheCheckpoint
    @Test
    void aRaftInstallPastTheHeldEntryDropsTheHold() {
      sm = stateMachine(securityServer(Skipped.SECURITY_USERS_NOT_PERSISTED));

      assertThat(sm.applyTransaction(securityEntry(Kind.USERS, SKIPPED_INDEX)).isCompletedExceptionally()).isTrue();
      assertThat(sm.applyTransaction(securityEntry(Kind.GROUPS, LATER_INDEX)).isCompletedExceptionally()).isFalse();

      sm.recordInstalledRaftBoundary(SKIPPED_INDEX - 1);
      assertThat(sm.takeSnapshot()).as("an install short of the entry does not cover it").isLessThan(SKIPPED_INDEX);

      sm.recordInstalledRaftBoundary(SKIPPED_INDEX);
      assertThat(sm.takeSnapshot()).as("an install that reaches the entry carries its effect").isEqualTo(LATER_INDEX);
    }

    /** A configuration entry rewrites only the settings it names, so the held one waits for every one of them. */
    // Regression for #10086: SkippedEntryHoldsTheCheckpoint
    @Test
    void aHeldConfigurationIsReleasedOnlyOnceEverySettingItNamedIsWrittenAgain() throws Exception {
      final ContextConfiguration config = new ContextConfiguration();
      config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, tempDir.resolve("databases").toString());
      final ArcadeDBServer server = new ArcadeDBServer(config);
      final LocalDatabase database = (LocalDatabase) new DatabaseFactory(tempDir.resolve("databases").resolve(DB_NAME).toString())
          .create();
      server.registerDatabase(DB_NAME, database);
      try {
        final File file = new File(database.getDatabasePath(), LocalDatabase.CONFIGURATION_FILE_NAME);
        Files.deleteIfExists(file.toPath());
        assertThat(file.mkdirs()).isTrue();
        assertThat(new File(file, "x").createNewFile()).isTrue();

        sm = stateMachine(server);
        assertThat(sm.applyTransaction(rawEntry(RaftLogEntryCodec.encodeDatabaseConfigurationEntry(DB_NAME, CONFIGURATION),
            SKIPPED_INDEX)).isCompletedExceptionally()).isTrue();

        // The volume is fixed
        Files.delete(new File(file, "x").toPath());
        Files.delete(file.toPath());

        assertThat(sm.applyTransaction(rawEntry(RaftLogEntryCodec.encodeDatabaseConfigurationEntry(DB_NAME,
            "{\"configuration\":{\"txRetries\":7},\"removed\":[]}"), 6L)).isCompletedExceptionally()).isFalse();
        assertThat(sm.takeSnapshot()).as("another setting does not write the one that is missing").isLessThan(SKIPPED_INDEX);

        assertThat(sm.applyTransaction(rawEntry(RaftLogEntryCodec.encodeDatabaseConfigurationEntry(DB_NAME, CONFIGURATION),
            LATER_INDEX)).isCompletedExceptionally()).isFalse();
        assertThat(sm.takeSnapshot()).as("the file carries every setting the held entry named").isEqualTo(LATER_INDEX);
      } finally {
        server.stop();
        if (database.isOpen())
          database.drop();
      }
    }

    /** A failure on a database that is already quarantined has its disposition: it is not charged to the swallow budget again. */
    // Regression for #10086: SkippedEntryHoldsTheCheckpoint
    @Test
    void aQuarantinedDatabaseStillPassesThroughUntouched() {
      sm = stateMachine(FakeArcadeDBServer.create());
      sm.markStateDiverged(DB_NAME);
      final ReplicationException settled = new ReplicationException("already settled");

      assertThatThrownBy(() -> sm.applyWithRetry(SKIPPED_INDEX, DB_NAME, () -> {
        throw settled;
      })).isSameAs(settled);
      assertThat(sm.divergedSwallowedErrorCount()).as("a settled disposition is not charged to the swallow budget").isZero();
    }

    /** The configuration of a database whose {@code configuration.json} cannot be written stays replayable. */
    // Regression for #10086: SkippedEntryHoldsTheCheckpoint
    @Test
    void aConfigurationThatCouldNotBePersistedHoldsTheCheckpoint() throws Exception {
      final ContextConfiguration config = new ContextConfiguration();
      config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, tempDir.resolve("databases").toString());
      final ArcadeDBServer server = new ArcadeDBServer(config);
      final LocalDatabase unwritable = (LocalDatabase) new DatabaseFactory(tempDir.resolve("databases").resolve(DB_NAME).toString())
          .create();
      final LocalDatabase healthy = (LocalDatabase) new DatabaseFactory(tempDir.resolve("databases").resolve("healthy").toString())
          .create();
      server.registerDatabase(DB_NAME, unwritable);
      server.registerDatabase("healthy", healthy);
      try {
        // configuration.json is a non-empty directory: reading and replacing it both fail, as a read-only volume would
        final File asDirectory = new File(unwritable.getDatabasePath(), LocalDatabase.CONFIGURATION_FILE_NAME);
        Files.deleteIfExists(asDirectory.toPath());
        assertThat(asDirectory.mkdirs()).isTrue();
        assertThat(new File(asDirectory, "x").createNewFile()).isTrue();

        sm = stateMachine(server);
        assertThat(sm.applyTransaction(rawEntry(RaftLogEntryCodec.encodeDatabaseConfigurationEntry(DB_NAME, CONFIGURATION),
            SKIPPED_INDEX)).isCompletedExceptionally()).as("the write fails").isTrue();
        assertThat(sm.applyTransaction(rawEntry(RaftLogEntryCodec.encodeDatabaseConfigurationEntry("healthy", CONFIGURATION),
            LATER_INDEX)).isCompletedExceptionally()).as("another database is unaffected").isFalse();
        assertThat(sm.readAppliedIndexCounter()).isEqualTo(LATER_INDEX);

        assertThat(sm.takeSnapshot())
            .as("the configuration is in force in memory but not on disk, so a restart has to replay it")
            .isLessThan(SKIPPED_INDEX);
      } finally {
        server.stop();
        if (unwritable.isOpen())
          unwritable.drop();
        if (healthy.isOpen())
          healthy.drop();
      }
    }

    // ---- drivers ----

    private enum Kind {
      USERS, GROUPS, API_TOKENS
    }

    /** A later document of a kind other than the one that failed, so the failed one is not superseded by it. */
    private static Kind laterKind(final Skipped skipped) {
      return skipped == Skipped.SECURITY_GROUPS_NOT_PERSISTED ? Kind.USERS : Kind.GROUPS;
    }

    private Throwable failTheApply(final Skipped skipped) throws Exception {
      switch (skipped) {
      case RETRY_EXHAUSTED_ON_A_DATABASE:
        return thrownBy(() -> sm.applyWithRetry(SKIPPED_INDEX, DB_NAME, () -> {
          throw new ConcurrentModificationException("persistent page race");
        }));
      case RETRY_EXHAUSTED_WITHOUT_A_DATABASE:
        return thrownBy(() -> sm.applyWithRetry(SKIPPED_INDEX, () -> {
          throw new ConcurrentModificationException("persistent page race");
        }));
      case RETRY_INTERRUPTED:
        GlobalConfiguration.TX_RETRY_DELAY.setValue(50);
        Thread.currentThread().interrupt();
        return thrownBy(() -> sm.applyWithRetry(SKIPPED_INDEX, DB_NAME, () -> {
          throw new ConcurrentModificationException("page race");
        }));
      case BARE_REPLICATION_EXCEPTION_ON_A_DATABASE:
        return thrownBy(() -> sm.applyWithRetry(SKIPPED_INDEX, DB_NAME, () -> {
          throw new ReplicationException("Corrupted WAL transaction entry: invalid page count -1");
        }));
      case SEALED_SLICE_SEQUENCE_BROKEN:
        final LocalDatabase database = (LocalDatabase) new DatabaseFactory(tempDir.resolve("sealed").toString()).create();
        try {
          database.command("sql", "CREATE TIMESERIES TYPE " + TYPE_NAME
              + " TIMESTAMP ts TAGS (hostname STRING) FIELDS (usage DOUBLE) SHARDS 1");
          final byte[] bytes = new byte[64];
          // Starts at offset 32 with nothing staged: the sequence it belongs to never reached this node
          final TsSealedChunk chunk = new TsSealedChunk(TYPE_NAME, 0, TYPE_NAME + "_shard_0.ts.sealed", 128L, 0L, 32L, bytes, false);
          return thrownBy(() -> sm.applyWithRetry(SKIPPED_INDEX, DB_NAME, () -> {
            try {
              sm.applySealedChunks(database, List.of(chunk));
            } catch (final IOException e) {
              throw new IllegalStateException(e);
            }
          }));
        } finally {
          database.drop();
        }
      case SECURITY_USERS_NOT_PERSISTED:
        return failedFuture(securityEntry(Kind.USERS, SKIPPED_INDEX));
      case SECURITY_GROUPS_NOT_PERSISTED:
        return failedFuture(securityEntry(Kind.GROUPS, SKIPPED_INDEX));
      case SECURITY_API_TOKENS_NOT_PERSISTED:
        return failedFuture(securityEntry(Kind.API_TOKENS, SKIPPED_INDEX));
      }
      throw new IllegalStateException("unhandled " + skipped);
    }

    private static Throwable thrownBy(final Runnable action) {
      try {
        action.run();
      } catch (final Throwable t) {
        return t;
      }
      throw new AssertionError("the apply did not fail");
    }

    private Throwable failedFuture(final TransactionContext entry) {
      final CompletableFuture<?> future = sm.applyTransaction(entry);
      assertThat(future.isCompletedExceptionally()).isTrue();
      try {
        future.join();
      } catch (final java.util.concurrent.CompletionException e) {
        return e.getCause();
      }
      throw new AssertionError("the apply did not fail");
    }

    /** A server whose security layer cannot persist the document the row names, once, and accepts everything else. */
    private static ArcadeDBServer securityServer(final Skipped skipped) {
      final AtomicBoolean failed = new AtomicBoolean();
      final FakeServerSecurity security = FakeServerSecurity.create();
      security.on("applyReplicatedUsers", args -> {
        if (skipped == Skipped.SECURITY_USERS_NOT_PERSISTED && failed.compareAndSet(false, true))
          throw new ReplicatedUsersPersistenceException("could NOT be persisted", new IOException("No space left on device"));
        return null;
      });
      security.on("applyReplicatedGroups", args -> {
        if (skipped == Skipped.SECURITY_GROUPS_NOT_PERSISTED && failed.compareAndSet(false, true))
          throw new ReplicatedSecurityConfigPersistenceException("could NOT be persisted", new IOException("No space left on device"));
        return null;
      });
      security.on("applyReplicatedApiTokens", args -> {
        if (skipped == Skipped.SECURITY_API_TOKENS_NOT_PERSISTED && failed.compareAndSet(false, true))
          throw new ReplicatedSecurityConfigPersistenceException("could NOT be persisted", new IOException("No space left on device"));
        return null;
      });
      final FakeArcadeDBServer server = FakeArcadeDBServer.create();
      server.security(security);
      return server;
    }

    private ArcadeStateMachine stateMachine(final ArcadeDBServer server) {
      final ArcadeStateMachine machine = new ArcadeStateMachine();
      machine.setServer(server);
      try {
        machine.initialize(stubServer(), RaftGroupId.valueOf(UUID.randomUUID()), storage);
      } catch (final IOException e) {
        throw new IllegalStateException(e);
      }
      return machine;
    }

    private TransactionContext securityEntry(final Kind kind, final long index) {
      return rawEntry(switch (kind) {
        case USERS -> RaftLogEntryCodec.encodeSecurityUsersEntry(USERS_JSON);
        case GROUPS -> RaftLogEntryCodec.encodeSecurityGroupsEntry(GROUPS_JSON);
        case API_TOKENS -> RaftLogEntryCodec.encodeSecurityApiTokensEntry(API_TOKENS_JSON);
      }, index);
    }

    private TransactionContext rawEntry(final ByteString payload, final long index) {
      final LogEntryProto logEntry = LogEntryProto.newBuilder()
          .setTerm(1L)
          .setIndex(index)
          .setStateMachineLogEntry(StateMachineLogEntryProto.newBuilder().setLogData(payload).build())
          .build();
      return TransactionContext.newBuilder().setStateMachine(sm).setLogEntry(logEntry).build();
    }

    /** Minimal {@link RaftServer} stub: {@code BaseStateMachine.initialize()} only needs a non-null {@code getId()}. */
    private static RaftServer stubServer() {
      return (RaftServer) Proxy.newProxyInstance(AppliedIndexCheckpointTest.class.getClassLoader(),
          new Class<?>[] { RaftServer.class }, (proxy, method, args) -> {
            if ("getId".equals(method.getName()))
              return RaftPeerId.valueOf("test-peer");
            if ("close".equals(method.getName()) || "start".equals(method.getName()))
              return null;
            throw new UnsupportedOperationException("Stub: " + method.getName());
          });
    }
  }

  /**
   * Regression test for issues #575 / #593: a follower-installed snapshot can seed an inflated applied
   * TERM, and when the real next committed entry is applied - its INDEX advances but its TERM is lower -
   * Ratis's strict term-first monotonic check in {@code BaseStateMachine.updateLastAppliedTermIndex}
   * throws {@code IllegalStateException: Failed updateLastAppliedTermIndex}, halting the
   * {@code StateMachineUpdater} and crash-looping the node until the whole cluster is wedged with all
   * peers {@code VOTING_FOR_ME} (the field state: {@code snapshot.11_39707283} vs a term-10
   * entry at 39707284).
   * <p>
   * Unlike the earlier (reverted) attempt, this test does not stub the Raft log: it drives the exact
   * {@code updateLastAppliedTermIndex} method that throws in the field, on a real
   * {@link ArcadeStateMachine} over real {@link RaftStorage}, with the client's real numbers. It fails
   * on the unpatched engine (the throw propagates) and passes with the tolerant override.

   */
  @Nested
  class ToleratedAppliedTermRegression {

    private static final long SNAPSHOT_INDEX   = 39707283L;
    private static final long NEXT_ENTRY_INDEX = 39707284L;
    private static final long BOUNDARY_TERM    = 10L; // real term of the committed entry after the snapshot
    private static final long INFLATED_TERM    = 11L; // over-recorded snapshot term (term of the entry after)

    /**
     * The field scenario: seed the inflated (t:11, i:39707283) snapshot term, then apply the real next
     * committed entry (t:10, i:39707284). The tolerant override accepts the index-advancing/term-
     * regressing correction instead of halting, and the recorded term is realigned down to 10.
     */
    // Regression for #593: ToleratedAppliedTermRegression
    @Test
    void inflatedSnapshotTermIsToleratedWhenNextEntryAdvances(@TempDir final Path tempDir) throws Exception {
      final ArcadeStateMachine sm = newInitializedStateMachine(tempDir);
      try {
        sm.updateLastAppliedTermIndex(TermIndex.valueOf(INFLATED_TERM, SNAPSHOT_INDEX));

        assertThatCode(() -> sm.updateLastAppliedTermIndex(TermIndex.valueOf(BOUNDARY_TERM, NEXT_ENTRY_INDEX)))
            .as("an advancing index with a lower term (inflated-snapshot correction) must NOT halt the state machine")
            .doesNotThrowAnyException();

        assertThat(sm.getLastAppliedTermIndex())
            .as("the recorded term is realigned down to the real next-entry term")
            .isEqualTo(TermIndex.valueOf(BOUNDARY_TERM, NEXT_ENTRY_INDEX));
      } finally {
        sm.close();
      }
    }

    /**
     * The real crash site is {@code applyTransaction} -> {@code updateLastAppliedTermIndex(long, long)}:
     * the {@code (long,long)} overload, which in Ratis delegates virtually to the {@code TermIndex}
     * overload our override intercepts. This test locks in that dispatch by replaying the exact restart
     * lifecycle of a wedged node through the overload: {@code reinitialize()}-style seed of the inflated
     * marker term while the previous applied TermIndex is still null (no violation possible), then the
     * first re-applied entry - tolerated, so an upgraded node self-heals with no manual marker rename.
     */
    // Regression for #593: ToleratedAppliedTermRegression
    @Test
    void restartLifecycleSelfHealsThroughLongOverload(@TempDir final Path tempDir) throws Exception {
      final ArcadeStateMachine sm = newInitializedStateMachine(tempDir);
      try {
        assertThatCode(() -> {
          updateViaLongOverload(sm, INFLATED_TERM, SNAPSHOT_INDEX);   // reinitialize() seeding the inflated marker
          updateViaLongOverload(sm, BOUNDARY_TERM, NEXT_ENTRY_INDEX); // applyTransaction re-applying the next entry
        }).as("an upgraded node restarting on an inflated on-disk marker must self-heal, not crash-loop")
            .doesNotThrowAnyException();

        assertThat(sm.getLastAppliedTermIndex()).isEqualTo(TermIndex.valueOf(BOUNDARY_TERM, NEXT_ENTRY_INDEX));
      } finally {
        sm.close();
      }
    }

    /**
     * Pins the load-bearing Ratis 3.2.2 semantics the tolerant branch relies on: {@code super} completes
     * pending {@code queryStale()} transaction futures ONLY on the equal/no-op path, never on an
     * advancing update. The tolerated realignment must therefore behave exactly like super's advancing
     * path (future stays pending) without breaking the completion pipeline: the next duplicate update is
     * not a benign regression, delegates to super's no-op path and completes the future. A silently
     * dropped future here would hang stale linearizable reads.
     */
    // Regression for #593: ToleratedAppliedTermRegression
    @Test
    void pendingStaleQueryFutureStillCompletesAfterToleratedUpdate(@TempDir final Path tempDir) throws Exception {
      final ArcadeStateMachine sm = newInitializedStateMachine(tempDir);
      try {
        sm.updateLastAppliedTermIndex(TermIndex.valueOf(INFLATED_TERM, SNAPSHOT_INDEX));

        final CompletableFuture<Message> stale = sm.queryStale(null, NEXT_ENTRY_INDEX);
        assertThat(stale).as("registered while the applied index is behind minIndex").isNotDone();

        sm.updateLastAppliedTermIndex(TermIndex.valueOf(BOUNDARY_TERM, NEXT_ENTRY_INDEX));
        assertThat(stale)
            .as("identical to super's advancing path in Ratis 3.2.2: futures never complete on an advancing update")
            .isNotDone();

        // The duplicate update Ratis later issues (e.g. via notifyTermIndexUpdated) is equal, not a
        // benign regression, so it reaches super's no-op path - the only place futures complete.
        sm.updateLastAppliedTermIndex(TermIndex.valueOf(BOUNDARY_TERM, NEXT_ENTRY_INDEX));
        assertThat(stale).as("completion pipeline intact after the tolerated update").isDone();
      } finally {
        sm.close();
      }
    }

    /** Genuine violations must still fail loudly: a lower term at the SAME index is a real inconsistency. */
    // Regression for #593: ToleratedAppliedTermRegression
    @Test
    void genuineRegressionAtSameIndexStillThrows(@TempDir final Path tempDir) throws Exception {
      final ArcadeStateMachine sm = newInitializedStateMachine(tempDir);
      try {
        sm.updateLastAppliedTermIndex(TermIndex.valueOf(INFLATED_TERM, SNAPSHOT_INDEX));
        assertThatThrownBy(() -> sm.updateLastAppliedTermIndex(TermIndex.valueOf(BOUNDARY_TERM, SNAPSHOT_INDEX)))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("Failed updateLastAppliedTermIndex");
      } finally {
        sm.close();
      }
    }

    /** Genuine violations must still fail loudly: a lower index (index going backwards) is a real regression. */
    // Regression for #593: ToleratedAppliedTermRegression
    @Test
    void indexGoingBackwardsStillThrows(@TempDir final Path tempDir) throws Exception {
      final ArcadeStateMachine sm = newInitializedStateMachine(tempDir);
      try {
        sm.updateLastAppliedTermIndex(TermIndex.valueOf(BOUNDARY_TERM, NEXT_ENTRY_INDEX));
        assertThatThrownBy(() -> sm.updateLastAppliedTermIndex(TermIndex.valueOf(BOUNDARY_TERM, SNAPSHOT_INDEX)))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("Failed updateLastAppliedTermIndex");
      } finally {
        sm.close();
      }
    }

    /** Normal forward progress (index and term both non-decreasing) is unaffected. */
    // Regression for #593: ToleratedAppliedTermRegression
    @Test
    void normalForwardProgressIsUnaffected(@TempDir final Path tempDir) throws Exception {
      final ArcadeStateMachine sm = newInitializedStateMachine(tempDir);
      try {
        sm.updateLastAppliedTermIndex(TermIndex.valueOf(BOUNDARY_TERM, SNAPSHOT_INDEX));
        assertThatCode(() -> {
          sm.updateLastAppliedTermIndex(TermIndex.valueOf(BOUNDARY_TERM, NEXT_ENTRY_INDEX)); // same term, higher index
          sm.updateLastAppliedTermIndex(TermIndex.valueOf(INFLATED_TERM, NEXT_ENTRY_INDEX + 1)); // higher term, higher index
        }).doesNotThrowAnyException();
        assertThat(sm.getLastAppliedTermIndex()).isEqualTo(TermIndex.valueOf(INFLATED_TERM, NEXT_ENTRY_INDEX + 1));
      } finally {
        sm.close();
      }
    }

    /** Pure predicate checks: only "index up, term down" is benign. */
    // Regression for #593: ToleratedAppliedTermRegression
    @Test
    void benignPredicateMatchesOnlyIndexUpTermDown() {
      // index up, term down -> benign
      assertThat(ArcadeStateMachine.isBenignSnapshotTermRegression(
          TermIndex.valueOf(11, 39707283), TermIndex.valueOf(10, 39707284))).isTrue();
      // index up, term up -> not benign (normal progress)
      assertThat(ArcadeStateMachine.isBenignSnapshotTermRegression(
          TermIndex.valueOf(10, 39707283), TermIndex.valueOf(11, 39707284))).isFalse();
      // index up, term equal -> not benign
      assertThat(ArcadeStateMachine.isBenignSnapshotTermRegression(
          TermIndex.valueOf(10, 39707283), TermIndex.valueOf(10, 39707284))).isFalse();
      // same index, term down -> not benign (genuine)
      assertThat(ArcadeStateMachine.isBenignSnapshotTermRegression(
          TermIndex.valueOf(11, 39707283), TermIndex.valueOf(10, 39707283))).isFalse();
      // index down, term down -> not benign (genuine)
      assertThat(ArcadeStateMachine.isBenignSnapshotTermRegression(
          TermIndex.valueOf(11, 39707284), TermIndex.valueOf(10, 39707283))).isFalse();
      // null old (fresh seed) -> not benign
      assertThat(ArcadeStateMachine.isBenignSnapshotTermRegression(
          null, TermIndex.valueOf(10, 39707284))).isFalse();
    }

    // --- helpers -------------------------------------------------------------------------------------

    /**
     * Drives Ratis's protected {@code BaseStateMachine.updateLastAppliedTermIndex(long, long)} - the
     * exact entry point {@code applyTransaction} and {@code reinitialize()} use in production. It is
     * protected in a foreign package and not overridden locally, so reflection is required here;
     * {@code Method.invoke} still dispatches virtually, proving the overload reaches the override.
     */
    private static void updateViaLongOverload(final ArcadeStateMachine sm, final long term, final long index)
        throws Exception {
      final Method m = BaseStateMachine.class.getDeclaredMethod("updateLastAppliedTermIndex", long.class, long.class);
      m.setAccessible(true);
      try {
        m.invoke(sm, term, index);
      } catch (final InvocationTargetException e) {
        if (e.getCause() instanceof RuntimeException re)
          throw re;
        throw e;
      }
    }

    private static ArcadeStateMachine newInitializedStateMachine(final Path tempDir) throws IOException {
      final RaftGroupId groupId = RaftGroupId.valueOf(UUID.randomUUID());
      final RaftStorage raftStorage = RaftStorage.newBuilder()
          .setDirectory(tempDir.toFile())
          .setOption(RaftStorage.StartupOption.FORMAT)
          .build();
      final ArcadeStateMachine sm = new ArcadeStateMachine();
      sm.initialize(stubServer(), groupId, raftStorage);
      return sm;
    }

    private static RaftServer stubServer() {
      return (RaftServer) Proxy.newProxyInstance(
          AppliedIndexCheckpointTest.class.getClassLoader(),
          new Class<?>[] { RaftServer.class },
          (proxy, method, args) -> {
            if ("getId".equals(method.getName()))
              return RaftPeerId.valueOf("test-peer");
            if ("close".equals(method.getName()) || "start".equals(method.getName()))
              return null;
            throw new UnsupportedOperationException("Stub: " + method.getName());
          });
    }
  }
}
