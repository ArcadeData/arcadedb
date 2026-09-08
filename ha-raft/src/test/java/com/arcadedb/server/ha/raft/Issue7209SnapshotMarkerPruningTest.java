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

import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.apache.ratis.statemachine.impl.SingleFileSnapshotInfo;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issue #7209.
 * <p>
 * {@link ArcadeStateMachine} writes a zero-byte {@code snapshot.<term>_<index>} marker per checkpoint
 * and used to ask Ratis to retain only the newest via
 * {@code SimpleStateMachineStorage.cleanupOldSnapshots(policy)}. That call deletes nothing for
 * ArcadeDB: Ratis 3.3.0 advances its delete index only after counting {@code numSnapshotsRetained}
 * markers that <b>have</b> an {@code .md5} companion, and ArcadeDB writes none by design (see #6991),
 * so its {@code deleteIdx} stays {@code -1}. Every marker a node ever wrote therefore stayed on disk
 * for the life of that node, and every {@code getSingleFileSnapshotInfos()} scan - one per checkpoint,
 * one per restart - walked all of them.
 * <p>
 * The fix prunes the obsolete markers directly, at registration and once at startup.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue7209SnapshotMarkerPruningTest {

  /**
   * Entry point 1: the periodic checkpoint. {@link ArcadeStateMachine#takeSnapshot()} is what
   * {@code RaftLogCompactionScheduler} drives on a wall-clock cadence, so its markers are the ones
   * that accumulated fastest. After N checkpoints the directory must hold one marker, not N.
   */
  @Test
  void repeatedCheckpointsLeaveExactlyOneMarker(@TempDir final Path tempDir) throws Exception {
    final RaftStorage raftStorage = newFormattedStorage(tempDir);
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    try {
      sm.initialize(stubServer(), RaftGroupId.valueOf(UUID.randomUUID()), raftStorage);

      long lastIndex = 0;
      for (int i = 1; i <= 8; i++) {
        lastIndex = i * 100L;
        setLastApplied(sm, 3L, lastIndex);
        assertThat(sm.takeSnapshot())
            .as("checkpoint %d must authorise a purge at its applied index", i)
            .isEqualTo(lastIndex);
      }

      final File stateMachineDir = stateMachineDir(sm);
      assertThat(markerNames(stateMachineDir))
          .as("8 checkpoints must leave one marker, not 8 (issue #7209)")
          .containsExactly("snapshot.3_" + lastIndex);

      assertThat(sm.getStateMachineStorage().getLatestSnapshot().getIndex())
          .as("the surviving marker must still be the newest checkpoint")
          .isEqualTo(lastIndex);
    } finally {
      sm.close();
      raftStorage.close();
    }
  }

  /**
   * Entry point 2: the follower install. {@code notifyInstallSnapshotFromLeader()} registers its
   * marker through the very same private writer, at an index that advances with each install, so it
   * accumulated the same way. Driven through the writer directly - reaching
   * {@code notifyInstallSnapshotFromLeader} needs a live leader HTTP endpoint to resync against,
   * which is an IT concern; what this pins is that the pruning belongs to the writer both callers
   * share, not to {@code takeSnapshot()}.
   */
  @Test
  void registrationFromTheFollowerInstallPathPrunesToo(@TempDir final Path tempDir) throws Exception {
    final RaftStorage raftStorage = newFormattedStorage(tempDir);
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    try {
      sm.initialize(stubServer(), RaftGroupId.valueOf(UUID.randomUUID()), raftStorage);

      registerMarkerAt(sm, 4L, 500L);
      registerMarkerAt(sm, 5L, 900L);
      registerMarkerAt(sm, 6L, 1300L);

      assertThat(markerNames(stateMachineDir(sm)))
          .as("three successive follower installs must leave one marker (issue #7209)")
          .containsExactly("snapshot.6_1300");
    } finally {
      sm.close();
      raftStorage.close();
    }
  }

  /**
   * A marker at or above the retained index is never a prune candidate: registering an older index
   * (which {@code SimpleStateMachineStorage.updateLatestSnapshot} refuses to promote) must not remove
   * the newer marker the storage still points at.
   */
  @Test
  void aMarkerAboveTheRegisteredIndexSurvives(@TempDir final Path tempDir) throws Exception {
    final RaftStorage raftStorage = newFormattedStorage(tempDir);
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    try {
      sm.initialize(stubServer(), RaftGroupId.valueOf(UUID.randomUUID()), raftStorage);

      registerMarkerAt(sm, 9L, 2000L);
      registerMarkerAt(sm, 9L, 1500L);

      assertThat(markerNames(stateMachineDir(sm)))
          .as("the higher marker the storage still points at must not be pruned by a lower registration")
          .containsExactly("snapshot.9_1500", "snapshot.9_2000");
      assertThat(sm.getStateMachineStorage().getLatestSnapshot().getIndex())
          .as("the storage must still report the highest index")
          .isEqualTo(2000L);
    } finally {
      sm.close();
      raftStorage.close();
    }
  }

  /**
   * Entry point 3: a node upgrading with a directory full of markers written before this fix. The
   * accumulation must go away at startup, without waiting for the next checkpoint - the per-restart
   * directory scan is one of the costs #7209 names.
   */
  @Test
  void startupPrunesMarkersLeftByEarlierVersions(@TempDir final Path tempDir) throws Exception {
    // Format the storage and plant the markers an unpruned node would have left behind.
    final RaftStorage formatting = newFormattedStorage(tempDir);
    final File stateMachineDir;
    try {
      final SimpleStateMachineStorage planted = new SimpleStateMachineStorage();
      planted.init(formatting);
      stateMachineDir = planted.getSnapshotFile(1L, 1L).getParentFile();
      // RaftStorage.FORMAT does not create the state-machine directory; registerSnapshotMarker() is
      // what mkdirs it in production, so the fixture does it here before planting the markers.
      Files.createDirectories(stateMachineDir.toPath());
      for (int i = 1; i <= 12; i++)
        Files.createFile(new File(stateMachineDir, "snapshot.2_" + (i * 10L)).toPath());
      assertThat(markerNames(stateMachineDir)).as("fixture must plant 12 markers").hasSize(12);
    } finally {
      formatting.close();
    }

    final RaftStorage reopened = newRecoveredStorage(tempDir);
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    try {
      sm.initialize(stubServer(), RaftGroupId.valueOf(UUID.randomUUID()), reopened);

      assertThat(markerNames(stateMachineDir))
          .as("startup must drop the markers earlier versions never pruned (issue #7209)")
          .containsExactly("snapshot.2_120");
      assertThat(sm.getStateMachineStorage().getLatestSnapshot().getIndex())
          .as("the newest marker must still be the one the restart rediscovers")
          .isEqualTo(120L);
    } finally {
      sm.close();
      reopened.close();
    }
  }

  /**
   * The prune matches Ratis's own {@code snapshot.<term>_<index>} pattern and nothing else: md5
   * companions, {@code .tmp} and {@code .corrupt} leftovers and any other file in the state-machine
   * directory are not its business.
   */
  @Test
  void pruningLeavesNonMarkerFilesAlone(@TempDir final Path tempDir) throws Exception {
    final RaftStorage raftStorage = newFormattedStorage(tempDir);
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    try {
      sm.initialize(stubServer(), RaftGroupId.valueOf(UUID.randomUUID()), raftStorage);

      registerMarkerAt(sm, 1L, 10L);
      final File dir = stateMachineDir(sm);
      Files.createFile(new File(dir, "snapshot.1_10.md5").toPath());
      Files.createFile(new File(dir, "snapshot.1_5.tmp").toPath());
      Files.createFile(new File(dir, "snapshot.1_5.corrupt").toPath());
      Files.createFile(new File(dir, "unrelated.dat").toPath());

      registerMarkerAt(sm, 1L, 20L);

      assertThat(markerNames(dir))
          .as("only the obsolete marker itself may be pruned")
          .containsExactly("snapshot.1_20");
      assertThat(names(dir))
          .as("non-marker files in the state-machine directory must survive the prune")
          .contains("snapshot.1_10.md5", "snapshot.1_5.tmp", "snapshot.1_5.corrupt", "unrelated.dat");
    } finally {
      sm.close();
      raftStorage.close();
    }
  }

  /**
   * A restart after the prune must still rediscover the same {@code (term, index)} - the second
   * acceptance criterion of #7209. Uses a brand-new {@link SimpleStateMachineStorage} on the same
   * directory, which is exactly what a fresh process does.
   */
  @Test
  void restartAfterPruningRediscoversTheSameTermAndIndex(@TempDir final Path tempDir) throws Exception {
    final long expectedIndex = 700L;
    final long expectedTerm = 11L;

    final RaftStorage raftStorage = newFormattedStorage(tempDir);
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    try {
      sm.initialize(stubServer(), RaftGroupId.valueOf(UUID.randomUUID()), raftStorage);
      for (long index = 100L; index <= expectedIndex; index += 100L) {
        setLastApplied(sm, expectedTerm, index);
        sm.takeSnapshot();
      }
    } finally {
      sm.close();
      raftStorage.close();
    }

    final RaftStorage reopened = newRecoveredStorage(tempDir);
    try {
      final SimpleStateMachineStorage freshStorage = new SimpleStateMachineStorage();
      freshStorage.init(reopened);
      final SingleFileSnapshotInfo discovered = freshStorage.getLatestSnapshot();
      assertThat(discovered).as("the surviving marker must be rediscovered after a restart").isNotNull();
      assertThat(discovered.getIndex()).isEqualTo(expectedIndex);
      assertThat(discovered.getTerm()).isEqualTo(expectedTerm);
    } finally {
      reopened.close();
    }
  }

  /**
   * The startup sweep must not make a fresh node noisier. Before {@code storage.init()} has ever seen
   * a state-machine directory, its {@code loadLatestSnapshot()} fails the directory scan and logs
   * {@code "Failed to updateLatestSnapshot from ..."} - a WARNING the #6991 filter deliberately lets
   * through, because that same logger also reports genuine I/O failures. Nothing is cached after the
   * failure, so every further {@code getLatestSnapshot()} call repeats the scan and the warning.
   * <p>
   * {@code initialize()} already makes two such calls that predate this fix: one inside
   * {@code storage.init()} and one inside {@code reinitialize()}. The sweep must add none - so this
   * compares the state machine against exactly that raw Ratis sequence rather than against a hardcoded
   * count, which would drift the moment either call moved.
   */
  @Test
  void theStartupSweepDoesNotRepeatTheDirectoryScanWarning(@TempDir final Path tempDir) throws Exception {
    // RaftStorage.FORMAT leaves no state-machine directory behind, so every scan below fails and warns.
    final long baseline = warningsWhile(() -> {
      final RaftStorage raftStorage = newFormattedStorage(tempDir.resolve("baseline"));
      try {
        final SimpleStateMachineStorage storage = new SimpleStateMachineStorage();
        storage.init(raftStorage);       // what ArcadeStateMachine.initialize() does
        storage.getLatestSnapshot();     // what reinitialize() does
      } finally {
        raftStorage.close();
      }
    });
    assertThat(baseline).as("the fixture must actually provoke the warning, or this test proves nothing").isPositive();

    final long throughStateMachine = warningsWhile(() -> {
      final RaftStorage raftStorage = newFormattedStorage(tempDir.resolve("state-machine"));
      final ArcadeStateMachine sm = new ArcadeStateMachine();
      try {
        sm.initialize(stubServer(), RaftGroupId.valueOf(UUID.randomUUID()), raftStorage);
      } finally {
        sm.close();
        raftStorage.close();
      }
    });

    assertThat(throughStateMachine)
        .as("the startup marker sweep must not re-run the failing scan that storage.init() and "
            + "reinitialize() already run")
        .isEqualTo(baseline);
  }

  /**
   * Runs {@code body} with a handler on the Ratis storage logger and returns how many
   * {@code "Failed to updateLatestSnapshot"} warnings it published.
   */
  private static long warningsWhile(final ThrowingRunnable body) throws Exception {
    final Logger ratisLogger = Logger.getLogger("org.apache.ratis.statemachine.impl.SimpleStateMachineStorage");
    final boolean useParentHandlers = ratisLogger.getUseParentHandlers();
    final Level level = ratisLogger.getLevel();
    final CollectingHandler collected = new CollectingHandler();

    ratisLogger.setUseParentHandlers(false);
    ratisLogger.setLevel(Level.ALL);
    ratisLogger.addHandler(collected);
    try {
      body.run();
    } finally {
      ratisLogger.removeHandler(collected);
      ratisLogger.setLevel(level);
      ratisLogger.setUseParentHandlers(useParentHandlers);
    }
    return collected.countContaining("Failed to updateLatestSnapshot");
  }

  @FunctionalInterface
  private interface ThrowingRunnable {
    void run() throws Exception;
  }

  /** Records everything the Ratis storage logger publishes during one test. */
  private static final class CollectingHandler extends Handler {
    private final List<String> messages = new CopyOnWriteArrayList<>();

    private CollectingHandler() {
      setLevel(Level.ALL);
    }

    @Override
    public void publish(final LogRecord record) {
      if (record != null && record.getMessage() != null)
        messages.add(record.getMessage());
    }

    @Override
    public void flush() {
      // nothing buffered
    }

    @Override
    public void close() {
      // nothing to release
    }

    private long countContaining(final String needle) {
      return messages.stream().filter(m -> m.contains(needle)).count();
    }
  }

  /**
   * "Never fail a checkpoint over a cosmetic cleanup" has to hold for more than a {@code false} return
   * from {@code delete()}: the sweep walks the filesystem, and a filesystem can raise unchecked. A
   * {@code SecurityException} out of {@code listFiles()} must degrade to "pruned nothing", not
   * propagate out of {@code registerSnapshotMarker} and cost the node its log purge.
   */
  @Test
  void aFilesystemFailureDuringTheSweepIsSwallowed(@TempDir final Path tempDir) throws Exception {
    final RaftStorage raftStorage = newFormattedStorage(tempDir);
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    try {
      sm.initialize(stubServer(), RaftGroupId.valueOf(UUID.randomUUID()), raftStorage);

      final Method prune = ArcadeStateMachine.class.getDeclaredMethod(
          "pruneObsoleteSnapshotMarkers", File.class, long.class);
      prune.setAccessible(true);

      final File throwing = new File(stateMachineDir(sm).getPath()) {
        @Override
        public File[] listFiles() {
          throw new SecurityException("denied");
        }
      };

      assertThat((Integer) prune.invoke(sm, throwing, 100L))
          .as("a filesystem failure must report 'pruned nothing', not escape the sweep")
          .isZero();
    } finally {
      sm.close();
      raftStorage.close();
    }
  }

  private static List<String> markerNames(final File dir) {
    final List<String> names = new ArrayList<>();
    for (final String name : names(dir))
      if (SimpleStateMachineStorage.SNAPSHOT_REGEX.matcher(name).matches())
        names.add(name);
    return names;
  }

  private static List<String> names(final File dir) {
    final String[] found = dir.list();
    final List<String> names = new ArrayList<>();
    if (found != null)
      for (final String name : found)
        names.add(name);
    names.sort(String::compareTo);
    return names;
  }

  private static File stateMachineDir(final ArcadeStateMachine sm) {
    return ((SimpleStateMachineStorage) sm.getStateMachineStorage()).getSnapshotFile(1L, 1L).getParentFile();
  }

  private static RaftStorage newFormattedStorage(final Path dir) throws IOException {
    return RaftStorage.newBuilder()
        .setDirectory(dir.toFile())
        .setOption(RaftStorage.StartupOption.FORMAT)
        .build();
  }

  private static RaftStorage newRecoveredStorage(final Path dir) throws IOException {
    return RaftStorage.newBuilder()
        .setDirectory(dir.toFile())
        .setOption(RaftStorage.StartupOption.RECOVER)
        .build();
  }

  /**
   * Writes a real zero-byte marker through the state machine's own registration path - the private
   * writer both {@code takeSnapshot()} and {@code notifyInstallSnapshotFromLeader()} call.
   */
  private static void registerMarkerAt(final ArcadeStateMachine sm, final long term, final long index) throws Exception {
    final Method m = ArcadeStateMachine.class.getDeclaredMethod("registerSnapshotMarker", long.class, long.class);
    m.setAccessible(true);
    assertThat((Boolean) m.invoke(sm, term, index)).as("snapshot marker written").isTrue();
  }

  /**
   * Advances the applied position the way {@code applyTransaction} does, so {@code takeSnapshot()}
   * has something to checkpoint.
   */
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

  /**
   * Minimal {@link RaftServer} stub: {@code BaseStateMachine.initialize()} only needs a non-null
   * {@code getId()}; nothing else is reached on this path.
   */
  private static RaftServer stubServer() {
    return (RaftServer) Proxy.newProxyInstance(
        Issue7209SnapshotMarkerPruningTest.class.getClassLoader(),
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
