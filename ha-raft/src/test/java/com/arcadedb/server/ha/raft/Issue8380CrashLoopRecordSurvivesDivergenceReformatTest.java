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
import com.arcadedb.utility.SubclassMocks;
import org.apache.ratis.util.LifeCycle;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

/**
 * Regression tests for issue #8380. The crash-loop escalation record of issue #7736 lived inside the Raft storage
 * directory, which the divergence reformat ({@code RaftHAServer.restartRatis(true)}, reached automatically from
 * {@link HealthMonitor}'s stuck-follower check) deletes wholesale. Nothing told the monitor, so its
 * {@code crashLoopEscalationPersisted} flag stayed true over a record that was gone: the next escalation in that
 * lifetime skipped the write, and the next process start walked the whole restart/reformat/snapshot-download ladder
 * again - the per-lifetime cost #7736 was filed to remove.
 * <p>
 * The record now lives beside the storage directory, and every escalation writes it and re-derives the flag from the
 * disk instead of trusting the cached one.
 */
class Issue8380CrashLoopRecordSurvivesDivergenceReformatTest {

  private static final int  THRESHOLD         = 3;
  private static final long RECOVERY_DURATION = 5_000L;

  /** The lifecycle of a Raft division, including the stuck-at-stale-term signature that triggers a reformat. */
  private static final class Lifecycle {
    volatile LifeCycle.State state     = LifeCycle.State.CLOSED;
    volatile boolean         stuck     = false;
    final    AtomicInteger   restarts  = new AtomicInteger();
    final    AtomicInteger   reformats = new AtomicInteger();
  }

  /**
   * A health target whose escalation record is kept on disk by a real {@link RaftHAServer}, and whose divergence
   * reformat does what {@code restartRatis(true)} does to the disk: delete the Raft storage directory wholesale.
   */
  private static HealthMonitor.HealthTarget onDisk(final Lifecycle lifecycle, final RaftHAServer store, final File storageDir) {
    return new HealthMonitor.HealthTarget() {
      @Override
      public LifeCycle.State getRaftLifeCycleState() {
        return lifecycle.state;
      }

      @Override
      public boolean isShutdownRequested() {
        return false;
      }

      @Override
      public void restartRatisIfNeeded() {
        lifecycle.restarts.incrementAndGet();
      }

      @Override
      public boolean isFollowerStuckDiverged() {
        return lifecycle.stuck;
      }

      @Override
      public void recoverFromDivergence() {
        lifecycle.reformats.incrementAndGet();
        deleteRecursively(storageDir);
        assertThat(storageDir).as("the reformat deleted the Raft storage directory").doesNotExist();
      }

      @Override
      public boolean hasPersistedCrashLoopEscalation() {
        return store.hasPersistedCrashLoopEscalation();
      }

      @Override
      public boolean persistCrashLoopEscalation(final String reason) {
        return store.persistCrashLoopEscalation(reason);
      }

      @Override
      public void clearPersistedCrashLoopEscalation() {
        store.clearPersistedCrashLoopEscalation();
      }
    };
  }

  private static RaftHAServer detachedServer(final File raftStorageRoot) {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_SERVER_LIST, "localhost:2434:2480");
    config.setValue(GlobalConfiguration.HA_RAFT_STORAGE_DIRECTORY, raftStorageRoot.getAbsolutePath());
    final ArcadeDBServer server = SubclassMocks.mock(ArcadeDBServer.class);
    when(server.getServerName()).thenReturn("localhost");
    when(server.getRootPath()).thenReturn(raftStorageRoot.getAbsolutePath());
    return new RaftHAServer(server, config);
  }

  private static HealthMonitor newLifetime(final HealthMonitor.HealthTarget target, final AtomicLong clock) {
    final HealthMonitor monitor = new HealthMonitor(target, 1000, 0L, RECOVERY_DURATION, true, 2, THRESHOLD);
    monitor.setClock(clock::get);
    return monitor;
  }

  private static void ticks(final HealthMonitor monitor, final AtomicLong clock, final int count) {
    for (int i = 0; i < count; i++) {
      clock.addAndGet(1_000);
      monitor.tick();
    }
  }

  private static void deleteRecursively(final File dir) {
    if (!dir.exists())
      return;
    try (final Stream<Path> paths = Files.walk(dir.toPath())) {
      paths.sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
    } catch (final IOException e) {
      throw new IllegalStateException(e);
    }
  }

  @Test
  void theRecordIsNotInsideTheRaftStorageDirectory(@TempDir final Path root) {
    final File storageDir = new File(root.toFile(), "raft-storage-peer");
    final File record = RaftHAServer.crashLoopEscalationMarkerFile(storageDir);

    assertThat(record.getParentFile().getAbsoluteFile()).isEqualTo(root.toFile().getAbsoluteFile());
    assertThat(record.toPath().startsWith(storageDir.toPath())).as("a reformat deletes everything under the storage dir")
        .isFalse();
    assertThat(record.getName()).isEqualTo("raft-storage-peer." + RaftHAServer.CRASH_LOOP_ESCALATION_MARKER);
  }

  @Test
  void aRecordSurvivesTheDeletionOfTheRaftStorageDirectory(@TempDir final Path root) {
    final RaftHAServer server = detachedServer(root.toFile());
    assertThat(server.persistCrashLoopEscalation("Ratis crash-loop persists after 11 restarts")).isTrue();
    // The storage exists with a Raft group in it, as on a running node.
    final File storageDir = storageDirOrCreate(root.toFile());

    deleteRecursively(storageDir); // what restartRatis(true) does before it FORMATs

    assertThat(storageDir).doesNotExist();
    assertThat(server.hasPersistedCrashLoopEscalation()).as("the reformat must not take the record with it").isTrue();
    assertThat(detachedServer(root.toFile()).hasPersistedCrashLoopEscalation()).as("and the next process start reads it")
        .isTrue();
  }

  @Test
  void theIssueRepro_aDivergenceReformatInAnInheritedLifetimeDoesNotLoseTheEscalation(@TempDir final Path root) {
    final Lifecycle lifecycle = new Lifecycle();
    final AtomicLong clock = new AtomicLong();

    // 1. The first lifetime crash-loops, takes its one crash-loop reformat, crash-loops again and escalates.
    final HealthMonitor first = newLifetime(onDisk(lifecycle, detachedServer(root.toFile()), new File(root.toFile(), "none")),
        clock);
    ticks(first, clock, 2 * (THRESHOLD + 1));
    assertThat(first.isCrashLoopEscalated()).isTrue();
    assertThat(first.isCrashLoopRestartPending()).as("recorded: asks for its one process restart").isTrue();
    assertThat(lifecycle.reformats.get()).isEqualTo(1);

    // 2. The restarted process inherits the record. Its Raft storage exists with a group in it.
    final RaftHAServer secondStore = detachedServer(root.toFile());
    final File storageDir = storageDirOrCreate(root.toFile());
    final HealthMonitor second = newLifetime(onDisk(lifecycle, secondStore, storageDir), clock);
    assertThat(second.isCrashLoopEscalated()).isTrue();
    assertThat(second.isCrashLoopRestartPending()).isFalse();

    // 3. Its division comes up; 4. then it meets the stuck-at-stale-term signature long enough for the reformat.
    lifecycle.state = LifeCycle.State.RUNNING;
    lifecycle.stuck = true;
    ticks(second, clock, (int) (RECOVERY_DURATION / 1_000) + 2);
    assertThat(lifecycle.reformats.get()).as("the stuck-follower check reformatted the Raft storage").isEqualTo(2);
    assertThat(secondStore.hasPersistedCrashLoopEscalation())
        .as("the reformat deleted the storage directory, not the escalation record next to it").isTrue();

    // 5. The crash loop returns: the node re-escalates with the record on disk and asks for no restart.
    lifecycle.stuck = false;
    lifecycle.state = LifeCycle.State.CLOSED;
    ticks(second, clock, 2 * (THRESHOLD + 1));
    assertThat(second.isCrashLoopEscalated()).isTrue();
    assertThat(second.isCrashLoopRestartPending()).isFalse();
    assertThat(lifecycle.reformats.get()).as("no crash-loop reformat either: it is still spent").isEqualTo(2);

    // 6. The next process start inherits the escalation instead of re-arming the whole ladder.
    final int restartsBefore = lifecycle.restarts.get();
    final HealthMonitor third = newLifetime(onDisk(lifecycle, detachedServer(root.toFile()), storageDir), clock);
    assertThat(third.isCrashLoopEscalated()).as("inherited, not re-armed").isTrue();
    ticks(third, clock, 50);
    assertThat(lifecycle.restarts.get()).as("no restart ladder in the third lifetime").isEqualTo(restartsBefore);
    assertThat(lifecycle.reformats.get()).as("no reformat and snapshot download in the third lifetime").isEqualTo(2);
  }

  @Test
  void anEscalationRewritesARecordThatWasDeletedUnderTheMonitor() {
    // The monitor's flag is a cache of a file something else can delete (an operator, or a reformat on a layout
    // that keeps the record inside the storage). A stale-true flag must not skip the write of the next escalation.
    final Issue7736CrashLoopEscalationSurvivesRestartTest.RecordingTarget target =
        new Issue7736CrashLoopEscalationSurvivesRestartTest.RecordingTarget();
    final HealthMonitor first = new HealthMonitor(target, 1000, 0L, RECOVERY_DURATION, true, 2, THRESHOLD);
    for (int i = 0; i < 2 * (THRESHOLD + 1); i++)
      first.tick();
    assertThat(target.record.get()).isNotNull();

    final AtomicLong clock = new AtomicLong();
    final HealthMonitor second = newLifetime(target, clock);
    target.state.set(LifeCycle.State.RUNNING);
    ticks(second, clock, 1);
    assertThat(second.isCrashLoopEscalated()).isFalse();

    target.record.set(null); // gone from the disk; the monitor was not told

    target.state.set(LifeCycle.State.CLOSED);
    ticks(second, clock, 2 * (THRESHOLD + 1));
    assertThat(second.isCrashLoopEscalated()).isTrue();
    assertThat(target.record.get()).as("the escalation is on disk again for the next process start")
        .contains("Ratis crash-loop persists");
    assertThat(second.isCrashLoopRestartPending()).as("still the inherited incident: no second restart").isFalse();
  }

  @Test
  void aFailedRewriteOfARecordStillOnDiskKeepsItRecorded() {
    // Re-deriving the flag from the disk must not lower it when a rewrite fails but the earlier record is intact:
    // lowering it would re-arm the reformat for a node whose next start will inherit the escalation anyway.
    final Issue7736CrashLoopEscalationSurvivesRestartTest.RecordingTarget target =
        new Issue7736CrashLoopEscalationSurvivesRestartTest.RecordingTarget();
    target.record.set("left by the previous lifetime");
    target.recordWritable = false;
    final AtomicLong clock = new AtomicLong();
    final HealthMonitor monitor = newLifetime(target, clock);
    target.state.set(LifeCycle.State.RUNNING);
    ticks(monitor, clock, 1);

    target.state.set(LifeCycle.State.CLOSED);
    ticks(monitor, clock, 2 * (THRESHOLD + 1));
    assertThat(monitor.isCrashLoopEscalated()).isTrue();
    assertThat(target.reformats.get()).as("the inherited reformat stays spent").isZero();
    assertThat(target.record.get()).isEqualTo("left by the previous lifetime");

    // A long healthy spell afterwards still clears it through the ordinary ten-minute window.
    target.state.set(LifeCycle.State.RUNNING);
    ticks(monitor, clock, 1);
    clock.addAndGet(HealthMonitor.CRASH_LOOP_RECORD_RESET_MS);
    monitor.tick();
    assertThat(target.record.get()).isNull();
  }

  @Test
  void aNonPersistentStorageWipeDiscardsTheRecordWithTheStorage(@TempDir final Path root) {
    // With arcadedb.ha.raftPersistStorage=false every start formats a fresh log; before #8380 the record went with
    // the directory, and moving it beside the directory must not make it outlive the storage it describes.
    final RaftHAServer server = detachedServer(root.toFile());
    assertThat(server.persistCrashLoopEscalation("Ratis crash-loop persists after 11 restarts")).isTrue();
    final File storageDir = storageDirOrCreate(root.toFile());

    RaftHAServer.discardNonPersistentRaftStorage(storageDir);

    assertThat(storageDir).doesNotExist();
    assertThat(RaftHAServer.crashLoopEscalationMarkerFile(storageDir)).doesNotExist();
    assertThat(server.hasPersistedCrashLoopEscalation()).isFalse();
    RaftHAServer.discardNonPersistentRaftStorage(storageDir); // idempotent on a missing storage
  }

  /**
   * The peer's Raft storage directory as {@link RaftHAServer} resolves it for {@link #detachedServer}, created with a
   * Raft group inside as on a running node. Resolved independently of where the record is kept, so the tests below
   * fail on the behaviour - the record lost with the storage - rather than on a path.
   */
  private static File storageDirOrCreate(final File root) {
    final File storageDir = RaftHAServer.resolveRaftStorageDir(root.getAbsolutePath(), null, root.getAbsolutePath(),
        "localhost_2434");
    if (!storageDir.exists())
      assertThat(new File(storageDir, "group-0").mkdirs()).isTrue();
    return storageDir;
  }
}
