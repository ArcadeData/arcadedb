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
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.utility.FileUtils;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
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
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

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
class Issue11517AppliedIndexDurabilityTest {

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
        Issue11517AppliedIndexDurabilityTest.class.getClassLoader(),
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
