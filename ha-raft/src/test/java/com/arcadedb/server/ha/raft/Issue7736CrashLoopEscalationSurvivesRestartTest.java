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
import org.apache.ratis.util.LifeCycle;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Regression tests for issue #7736. Issue #7622 made the liveness probe fail once {@link HealthMonitor} escalated a
 * crash loop, but every escalation field was in-memory and the monitor is built fresh per process. So each pod
 * restart reset the ladder and walked it again on the same, still poisoned, storage: more non-sticking restarts, a
 * Raft-storage reformat, a full snapshot download from the leader, a new escalation, liveness red again - a
 * perpetual {@code CrashLoopBackOff} for a cause served by the leader that no restart of this node can cure.
 * <p>
 * The escalation is now recorded next to the Raft storage. The process that escalated asks for ONE restart
 * ({@link HealthMonitor#isCrashLoopRestartPending()}); the restarted process inherits the record, skips the ladder
 * - no restart, no reformat - and does not ask for another one.
 */
class Issue7736CrashLoopEscalationSurvivesRestartTest {

  private static final int THRESHOLD = 3;

  /** A lifecycle target whose escalation record lives in {@link #record}, standing in for the Raft storage. */
  static final class RecordingTarget implements HealthMonitor.HealthTarget {
    final    AtomicReference<LifeCycle.State> state             = new AtomicReference<>(LifeCycle.State.CLOSED);
    final    AtomicReference<String>          record            = new AtomicReference<>();
    final    AtomicInteger                    restarts          = new AtomicInteger();
    final    AtomicInteger                    reformats         = new AtomicInteger();
    volatile boolean                          recordWritable    = true;

    @Override
    public LifeCycle.State getRaftLifeCycleState() {
      return state.get();
    }

    @Override
    public boolean isShutdownRequested() {
      return false;
    }

    @Override
    public void restartRatisIfNeeded() {
      restarts.incrementAndGet();
    }

    @Override
    public void recoverFromDivergence() {
      reformats.incrementAndGet();
    }

    @Override
    public boolean hasPersistedCrashLoopEscalation() {
      return record.get() != null;
    }

    @Override
    public boolean persistCrashLoopEscalation(final String reason) {
      if (!recordWritable)
        return false;
      record.set(reason);
      return true;
    }

    @Override
    public void clearPersistedCrashLoopEscalation() {
      record.set(null);
    }
  }

  private static HealthMonitor newLifetime(final HealthMonitor.HealthTarget target) {
    return new HealthMonitor(target, 1000, 0L, 5000L, true, 2, THRESHOLD);
  }

  /** Walks a fresh monitor through the whole #5291 ladder: restarts, the one reformat, restarts, give-up. */
  private static void escalate(final HealthMonitor monitor) {
    for (int i = 0; i < 2 * (THRESHOLD + 1); i++)
      monitor.tick();
    assertThat(monitor.isCrashLoopEscalated()).as("the ladder ran to the give-up").isTrue();
  }

  @Test
  void theEscalatingLifetimeRecordsItAndAsksForExactlyOneRestart() {
    final RecordingTarget target = new RecordingTarget();
    final HealthMonitor first = newLifetime(target);

    escalate(first);

    assertThat(target.restarts.get()).isEqualTo(2 * THRESHOLD);
    assertThat(target.reformats.get()).isEqualTo(1);
    assertThat(target.record.get()).as("the escalation is recorded").contains("Ratis crash-loop persists");
    assertThat(first.isCrashLoopRestartPending()).as("this lifetime asks for the one restart").isTrue();
  }

  @Test
  void theRestartedLifetimeInheritsTheEscalationAndDoesNotWalkTheLadderAgain() {
    final RecordingTarget target = new RecordingTarget();
    escalate(newLifetime(target));
    final int restartsBefore = target.restarts.get();

    // The process restart: a new monitor on the same storage, whose Raft layer comes back to the same crash loop.
    final HealthMonitor second = newLifetime(target);
    assertThat(second.isCrashLoopEscalated()).as("escalated from the first tick on, not after a new ladder").isTrue();
    assertThat(second.isCrashLoopRestartPending())
        .as("a restart was already tried on this storage: liveness must stay green, not loop").isFalse();

    for (int i = 0; i < 50; i++)
      second.tick();

    assertThat(target.restarts.get()).as("no Ratis restart in the inherited lifetime").isEqualTo(restartsBefore);
    assertThat(target.reformats.get()).as("no second storage reformat and snapshot download").isEqualTo(1);
    assertThat(second.isCrashLoopEscalated()).isTrue();
    assertThat(second.isCrashLoopRestartPending()).isFalse();
    assertThat(target.record.get()).as("the record stays for the next lifetime").isNotNull();
  }

  @Test
  void aDivisionThatOnlyLooksUpForATickDoesNotRearmTheReformat() {
    // Restarted on poisoned storage, a division can be RUNNING for a tick or two before the state machine applies
    // the bad entry and it drops back to CLOSED. Clearing the record on that tick would hand the next escalation a
    // fresh reformat, a fresh snapshot download and a fresh request for a process restart - the loop again.
    final RecordingTarget target = new RecordingTarget();
    escalate(newLifetime(target));
    final int restartsBefore = target.restarts.get();

    final AtomicLong clock = new AtomicLong(0);
    target.state.set(LifeCycle.State.RUNNING);
    final HealthMonitor second = newLifetime(target);
    second.setClock(clock::get);
    second.tick();
    clock.addAndGet(3_000);
    second.tick();
    assertThat(second.isCrashLoopEscalated()).as("the division is up, so it is not escalated right now").isFalse();
    assertThat(target.record.get()).as("but one healthy tick does not end the incident").isNotNull();

    target.state.set(LifeCycle.State.CLOSED);
    for (int i = 0; i < 50; i++) {
      clock.addAndGet(3_000);
      second.tick();
    }

    assertThat(target.restarts.get() - restartsBefore).as("a bounded run of in-place restarts only").isEqualTo(THRESHOLD);
    assertThat(target.reformats.get()).as("no second reformat and snapshot download").isEqualTo(1);
    assertThat(second.isCrashLoopEscalated()).isTrue();
    assertThat(second.isCrashLoopRestartPending()).as("still the inherited incident: no second restart").isFalse();
  }

  @Test
  void aDivisionThatStaysUpClearsTheRecordAndRearmsTheLadder() {
    final RecordingTarget target = new RecordingTarget();
    escalate(newLifetime(target));

    // The restart did cure it this time (or the operator fixed the leader): the division comes up and stays up.
    final AtomicLong clock = new AtomicLong(0);
    target.state.set(LifeCycle.State.RUNNING);
    final HealthMonitor second = newLifetime(target);
    second.setClock(clock::get);
    second.tick();
    clock.addAndGet(HealthMonitor.CRASH_LOOP_RECORD_RESET_MS - 1);
    second.tick();
    assertThat(target.record.get()).as("not yet: the window is not over").isNotNull();
    clock.addAndGet(1);
    second.tick();
    assertThat(target.record.get()).as("record cleared once the division stayed up for the whole window").isNull();

    // A genuinely new incident later gets the whole ladder again, reformat included, and its own one restart.
    target.state.set(LifeCycle.State.CLOSED);
    final int restartsBefore = target.restarts.get();
    escalate(second);
    assertThat(target.restarts.get() - restartsBefore).isEqualTo(2 * THRESHOLD);
    assertThat(target.reformats.get()).isEqualTo(2);
    assertThat(target.record.get()).isNotNull();
    assertThat(second.isCrashLoopRestartPending()).as("a new escalation in this lifetime asks for its restart").isTrue();
  }

  @Test
  void anEscalationThatCouldNotBeRecordedDoesNotAskForARestart() {
    // A restart after an unrecorded escalation would walk the whole ladder again - reformat and snapshot download
    // included - which is the loop this issue is about. The node parks instead, as it did before #7622.
    final RecordingTarget target = new RecordingTarget();
    target.recordWritable = false;
    final HealthMonitor monitor = newLifetime(target);

    escalate(monitor);

    assertThat(monitor.isCrashLoopRestartPending()).isFalse();
  }

  @Test
  void aRecordIsDiscardedWhenTheEscalationIsDisabled() {
    // crashLoopRestartThreshold = 0 is the operator asking for the legacy restart-forever behaviour; a record left
    // by an earlier run with the escalation enabled must neither stop those restarts nor survive to disarm the
    // ladder the day the escalation is enabled again.
    final RecordingTarget target = new RecordingTarget();
    target.record.set("left by an earlier run");
    final HealthMonitor monitor = new HealthMonitor(target, 1000, 0L, 5000L, true, 2, 0);

    assertThat(monitor.isCrashLoopEscalated()).isFalse();
    assertThat(target.record.get()).isNull();
    for (int i = 0; i < 5; i++)
      monitor.tick();
    assertThat(target.restarts.get()).isEqualTo(5);
  }

  // --- The record on disk: RaftHAServer is the real HealthTarget ---

  private static RaftHAServer detachedServer(final File raftStorageRoot) {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_SERVER_LIST, "localhost:2434:2480");
    config.setValue(GlobalConfiguration.HA_RAFT_STORAGE_DIRECTORY, raftStorageRoot.getAbsolutePath());
    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.getServerName()).thenReturn("localhost");
    when(server.getRootPath()).thenReturn(raftStorageRoot.getAbsolutePath());
    return new RaftHAServer(server, config);
  }

  @Test
  void raftHAServerRecordsTheEscalationUnderItsRaftStorageDirectory(@TempDir final Path root) throws Exception {
    final RaftHAServer first = detachedServer(root.toFile());
    assertThat(first.hasPersistedCrashLoopEscalation()).isFalse();

    assertThat(first.persistCrashLoopEscalation("Ratis crash-loop persists after 11 restarts")).isTrue();

    final File[] storageDirs = root.toFile().listFiles(File::isDirectory);
    assertThat(storageDirs).as("the peer's own Raft storage directory").hasSize(1);
    final File marker = new File(storageDirs[0], RaftHAServer.CRASH_LOOP_ESCALATION_MARKER);
    assertThat(marker).isFile();
    assertThat(Files.readString(marker.toPath())).contains("Ratis crash-loop persists after 11 restarts");

    // A new process on the same storage sees it; that is the whole point of writing it to disk.
    final RaftHAServer second = detachedServer(root.toFile());
    assertThat(second.hasPersistedCrashLoopEscalation()).isTrue();

    second.clearPersistedCrashLoopEscalation();
    assertThat(marker).doesNotExist();
    assertThat(detachedServer(root.toFile()).hasPersistedCrashLoopEscalation()).isFalse();
    second.clearPersistedCrashLoopEscalation(); // idempotent
  }

  @Test
  void aProcessRestartOnTheSameStorageDoesNotReformatAgain(@TempDir final Path root) {
    // End to end through the real record: the first lifetime escalates against a RaftHAServer-backed store, the
    // second is a new RaftHAServer and a new monitor on the same directory, as after a pod restart.
    final RecordingTarget lifecycle = new RecordingTarget();
    final HealthMonitor.HealthTarget firstProcess = onDisk(lifecycle, detachedServer(root.toFile()));
    final HealthMonitor first = newLifetime(firstProcess);
    escalate(first);
    assertThat(first.isCrashLoopRestartPending()).isTrue();
    assertThat(lifecycle.reformats.get()).isEqualTo(1);

    final HealthMonitor second = newLifetime(onDisk(lifecycle, detachedServer(root.toFile())));
    for (int i = 0; i < 50; i++)
      second.tick();

    assertThat(lifecycle.reformats.get()).as("the reformat ran once per incident, not once per process").isEqualTo(1);
    assertThat(second.isCrashLoopEscalated()).isTrue();
    assertThat(second.isCrashLoopRestartPending()).isFalse();
  }

  /** The lifecycle of {@code lifecycle}, with the escalation record kept on disk by a real {@link RaftHAServer}. */
  private static HealthMonitor.HealthTarget onDisk(final RecordingTarget lifecycle, final RaftHAServer store) {
    return new HealthMonitor.HealthTarget() {
      @Override
      public LifeCycle.State getRaftLifeCycleState() {
        return lifecycle.getRaftLifeCycleState();
      }

      @Override
      public boolean isShutdownRequested() {
        return false;
      }

      @Override
      public void restartRatisIfNeeded() {
        lifecycle.restartRatisIfNeeded();
      }

      @Override
      public void recoverFromDivergence() {
        lifecycle.recoverFromDivergence();
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
}
