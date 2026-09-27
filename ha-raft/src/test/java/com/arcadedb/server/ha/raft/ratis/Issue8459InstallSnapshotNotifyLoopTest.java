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
package com.arcadedb.server.ha.raft.ratis;

import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.RaftTestUtil;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.MiniRaftClusterWithGrpc;
import org.apache.ratis.proto.RaftProtos.AppendEntriesRequestProto;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.proto.RaftProtos.RoleInfoProto;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.impl.BlockRequestHandlingInjection;
import org.apache.ratis.server.impl.MiniRaftCluster;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.server.storage.FileInfo;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.SnapshotInfo;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.statemachine.StateMachineStorage;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.apache.ratis.statemachine.impl.SingleFileSnapshotInfo;
import org.apache.ratis.util.CodeInjectionForTesting;
import org.apache.ratis.util.LifeCycle;
import org.apache.ratis.util.SizeInBytes;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Regression tests for issue #8459: a leader must stop re-notifying an install-snapshot boundary to a follower that
 * has already installed a snapshot ending exactly one index before the leader's log start.
 * <p>
 * In that state Ratis's {@code LogAppender.shouldInstallSnapshot} answers {@code true} forever, because
 * {@code getPrevious(nextIndex)} finds the previous entry neither in the (purged) leader log nor in the leader's own
 * snapshot marker, and the follower answers every notification with {@code ALREADY_INSTALLED} without calling its
 * state machine. Ratis itself already knows the previous entry is not needed there - both
 * {@code LogAppenderBase.newAppendEntriesRequest} and the follower's {@code ServerImplUtils.assertEntries} accept an
 * AppendEntries with no previous when its first entry is the follower's snapshot index + 1 - so
 * {@link FixedGrpcLogAppender} closes the one gap: it no longer asks for a snapshot in that state.
 */
class Issue8459InstallSnapshotNotifyLoopTest {

  // ---------------------------------------------------------------------------------------------
  // Decision function
  // ---------------------------------------------------------------------------------------------

  @Test
  void followerAnchoredOnItsInstalledSnapshotAtLeaderLogStartIsNotReNotified() {
    // The #8449/#8457 shape: leader log starts at 101, the follower installed up to 100 and has acknowledged nothing
    // since (the ALREADY_INSTALLED / SNAPSHOT_INSTALLED reply set matchIndex to the snapshot index).
    assertThat(FixedGrpcLogAppender.isAnchoredOnInstalledSnapshot(101, 101, 100, 100, true)).isTrue();
  }

  @Test
  void followerBehindTheLeaderLogStartStillNeedsASnapshot() {
    // nextIndex < log start: the entries it needs are gone, a real install must run.
    assertThat(FixedGrpcLogAppender.isAnchoredOnInstalledSnapshot(90, 101, 89, 89, true)).isFalse();
  }

  @Test
  void followerWhoseSnapshotIsNotTheEntryBeforeNextIndexIsNotAnchored() {
    // The follower's reported snapshot is not the entry before nextIndex: the append would need a real previous.
    assertThat(FixedGrpcLogAppender.isAnchoredOnInstalledSnapshot(101, 101, 90, 90, true)).isFalse();
  }

  @Test
  void followerThatAcknowledgedEntriesSinceItsSnapshotIsNotAnchored() {
    // matchIndex moved past the snapshot index: the snapshot index is no longer the follower's latest confirmed
    // position, so it is not trusted as the anchor.
    assertThat(FixedGrpcLogAppender.isAnchoredOnInstalledSnapshot(101, 101, 100, 150, true)).isFalse();
  }

  @Test
  void followerThatNeverRepliedToAnInstallIsNotAnchored() {
    // FollowerInfoImpl.snapshotIndex starts at 0 and is only written by an install reply: without one, the value is a
    // default, not something the follower said.
    assertThat(FixedGrpcLogAppender.isAnchoredOnInstalledSnapshot(101, 101, 100, 100, false)).isFalse();
  }

  @Test
  void defaultSnapshotIndexIsNeverAnAnchor() {
    // Index 0 is FollowerInfoImpl's initial value; with a log starting at 1 it would otherwise satisfy the equation.
    assertThat(FixedGrpcLogAppender.isAnchoredOnInstalledSnapshot(1, 1, 0, 0, true)).isFalse();
  }

  @Test
  void unknownOrNeverCompactedLeaderLogIsNotAnchored() {
    assertThat(FixedGrpcLogAppender.isAnchoredOnInstalledSnapshot(0, RaftLog.INVALID_LOG_INDEX, -1, -1, true)).isFalse();
    assertThat(FixedGrpcLogAppender.isAnchoredOnInstalledSnapshot(0, 0, -1, -1, true)).isFalse();
  }

  // ---------------------------------------------------------------------------------------------
  // In-process leader LogAppender (the test #8457 asked for)
  // ---------------------------------------------------------------------------------------------

  /**
   * Drives a real leader {@link FixedGrpcLogAppender} into the exact state of the loop: a follower that installs a
   * snapshot ending at the leader's log start - 1 while the leader's own snapshot marker is past that index. With
   * stock Ratis the follower never receives another entry (it answers ALREADY_INSTALLED forever); with the fix the
   * leader ships the entries from its log start on and the follower catches up after a single install.
   */
  @Test
  @Tag("slow")
  void leaderResumesAppendEntriesAfterInstallBoundaryOneBeforeItsLogStart() throws Exception {
    final Scenario scenario = new Scenario();
    scenario.run(false);

    // The follower installed exactly once, at the leader's log start - 1, and then received ordinary entries: no
    // second notification was needed.
    assertThat(scenario.installs.get()).isEqualTo(1);
    assertThat(scenario.renotifications.get()).isZero();
    assertThat(scenario.installedBoundary.get().getIndex()).isEqualTo(scenario.logStart - 1);
  }

  /**
   * The fallback: a follower that rejects the append sent right after the snapshot it reported - the shape of one
   * whose Raft storage was wiped after the report, whose {@code assertEntries} then expects index 0 - must be notified
   * again rather than retried forever. The stock {@code getNextIndexForError} never moves {@code nextIndex} below
   * {@code matchIndex + 1}, the anchor itself, so without the fallback no notification is ever sent.
   * <p>
   * Modelled with Ratis's own injection points: after the first install the follower throws on every data append until
   * it receives another install notification, which is what re-confirms (or replaces) its snapshot.
   */
  @Test
  @Tag("slow")
  void rejectedAnchoredAppendFallsBackToANewNotification() throws Exception {
    final Scenario scenario = new Scenario();
    scenario.run(true);

    // The injection really fired (a rename of the Ratis injection keys would otherwise pass this test for nothing),
    // and the follower caught up only through a second notification.
    assertThat(scenario.rejectedAppends.get()).isGreaterThan(0);
    assertThat(scenario.renotifications.get()).isGreaterThan(0);
    assertThat(scenario.mustReinstall.get()).isFalse();
  }

  /** One leader, one follower that falls behind a compacted log and installs at the leader's log start - 1. */
  private static final class Scenario {
    // RaftServerImpl.APPEND_ENTRIES / INSTALL_SNAPSHOT are package-private; these are their values.
    private static final String APPEND_ENTRIES_INJECTION   = "RaftServerImpl.appendEntries";
    private static final String INSTALL_SNAPSHOT_INJECTION = "RaftServerImpl.installSnapshot";

    final AtomicInteger              installs          = new AtomicInteger();
    final AtomicReference<TermIndex> installedBoundary = new AtomicReference<>();
    final AtomicBoolean              mustReinstall     = new AtomicBoolean();
    final AtomicInteger              rejectedAppends   = new AtomicInteger();
    final AtomicInteger              renotifications   = new AtomicInteger();
    long                             logStart;

    void run(final boolean rejectAnchoredAppendUntilRenotified) throws Exception {
      final RaftProperties properties = new RaftProperties();
      // The same wiring as RaftPropertiesBuilder: FixedGrpcRpcType.name() is "GRPC", so Rpc.setType(...) would select
      // the stock GrpcFactory; Ratis instantiates a class name instead.
      properties.set(RaftConfigKeys.Rpc.TYPE_KEY, FixedGrpcRpcType.class.getName());
      RaftServerConfigKeys.Log.Appender.setInstallSnapshotEnabled(properties, false);
      RaftServerConfigKeys.Snapshot.setAutoTriggerEnabled(properties, false);
      RaftServerConfigKeys.Log.setPurgeGap(properties, 1);
      // Small segments so the purge below leaves the log start inside the snapshot's range, as segment-granular
      // purging does in production (issue #8449).
      RaftServerConfigKeys.Log.setSegmentSizeMax(properties, SizeInBytes.valueOf("1KB"));

      final Runnable afterInstall = () -> {
        if (rejectAnchoredAppendUntilRenotified && installs.get() == 1)
          mustReinstall.set(true);
      };
      final MiniRaftClusterWithGrpc cluster = new MiniRaftClusterWithGrpc(MiniRaftCluster.generateIds(3, 0), properties,
          null);
      cluster.setStateMachineRegistry(
          (StateMachine.Registry) groupId -> new MarkerStateMachine(installs, installedBoundary, afterInstall));
      try {
        cluster.start();
        final RaftServer.Division leader = RaftTestUtil.waitForLeader(cluster);
        final RaftPeerId followerId = cluster.getFollowers().get(0).getId();

        CodeInjectionForTesting.put(APPEND_ENTRIES_INJECTION, (localId, remoteId, args) -> {
          if (mustReinstall.get() && followerId.equals(localId) && args.length > 1
              && args[1] instanceof AppendEntriesRequestProto request && request.getEntriesCount() > 0) {
            rejectedAppends.incrementAndGet();
            throw new IllegalStateException("simulated: follower no longer holds the snapshot it reported");
          }
          return false;
        });
        CodeInjectionForTesting.put(INSTALL_SNAPSHOT_INJECTION, (localId, remoteId, args) -> {
          // Any notification after the first install is the re-confirmation the fallback exists to obtain.
          if (followerId.equals(localId) && installs.get() > 0) {
            renotifications.incrementAndGet();
            mustReinstall.set(false);
          }
          return false;
        });

        try (final RaftClient client = cluster.createClient(leader.getId())) {
          send(client, 10);
          cluster.killServer(followerId);
          send(client, 200);

          // Leader snapshot marker M, then purge up to it: the log start S lands on a segment boundary at or before M.
          long marker = leader.getStateMachine().takeSnapshot();
          leader.getRaftLog().purge(marker).get(10, TimeUnit.SECONDS);
          logStart = leader.getRaftLog().getStartIndex();
          if (marker == logStart - 1) {
            // The one purge point where getPrevious(S) finds the marker and there is no loop to break: move the marker.
            send(client, 1);
            marker = leader.getStateMachine().takeSnapshot();
          }
          assertThat(logStart).as("the leader log must have been compacted").isGreaterThan(20);
          assertThat(marker).as("leader marker must be past the install boundary for the loop to arise")
              .isNotEqualTo(logStart - 1);

          cluster.restartServer(followerId, false);

          send(client, 5);
          final long leaderNext = leader.getRaftLog().getNextIndex();
          final RaftServer.Division follower = cluster.getDivision(followerId);
          await().atMost(30, TimeUnit.SECONDS).pollInterval(200, TimeUnit.MILLISECONDS).untilAsserted(
              () -> assertThat(follower.getRaftLog().getNextIndex()).as("follower caught up with the leader log")
                  .isGreaterThanOrEqualTo(leaderNext));
        }
      } finally {
        // Hand the injection points back to Ratis's own (inert unless a test blocks a peer) implementation.
        CodeInjectionForTesting.put(APPEND_ENTRIES_INJECTION, BlockRequestHandlingInjection.getInstance());
        CodeInjectionForTesting.put(INSTALL_SNAPSHOT_INJECTION, BlockRequestHandlingInjection.getInstance());
        cluster.shutdown();
      }
    }
  }

  private static void send(final RaftClient client, final int count) throws IOException {
    for (int i = 0; i < count; i++) {
      final RaftClientReply reply = client.io().send(Message.valueOf("entry-" + i + "-padding-to-fill-log-segments"));
      assertThat(reply.isSuccess()).isTrue();
    }
  }

  /**
   * Minimal state machine that snapshots the way {@code ArcadeStateMachine} does - a zero-byte
   * {@code snapshot.<term>_<index>} marker - and answers an install notification the way it did before #8449: with
   * the index one before the leader's first available log entry.
   */
  static final class MarkerStateMachine extends BaseStateMachine {
    private final SimpleStateMachineStorage storage = new SimpleStateMachineStorage();
    private final AtomicInteger              installs;
    private final AtomicReference<TermIndex> installedBoundary;
    private final Runnable                   afterInstall;

    MarkerStateMachine(final AtomicInteger installs, final AtomicReference<TermIndex> installedBoundary,
        final Runnable afterInstall) {
      this.installs = installs;
      this.installedBoundary = installedBoundary;
      this.afterInstall = afterInstall;
    }

    @Override
    public void initialize(final RaftServer server, final RaftGroupId groupId, final RaftStorage raftStorage)
        throws IOException {
      getLifeCycle().startAndTransition(() -> {
        super.initialize(server, groupId, raftStorage);
        storage.init(raftStorage);
        loadLatestSnapshot();
      });
    }

    @Override
    public StateMachineStorage getStateMachineStorage() {
      return storage;
    }

    @Override
    public CompletableFuture<Message> applyTransaction(final TransactionContext trx) {
      final LogEntryProto entry = trx.getLogEntry();
      updateLastAppliedTermIndex(entry.getTerm(), entry.getIndex());
      return CompletableFuture.completedFuture(Message.EMPTY);
    }

    @Override
    public long takeSnapshot() throws IOException {
      final TermIndex applied = getLastAppliedTermIndex();
      registerMarker(applied);
      return applied.getIndex();
    }

    @Override
    public void pause() {
      getLifeCycle().transition(LifeCycle.State.PAUSING);
      getLifeCycle().transition(LifeCycle.State.PAUSED);
    }

    @Override
    public void reinitialize() {
      loadLatestSnapshot();
      if (getLifeCycleState() == LifeCycle.State.PAUSED) {
        getLifeCycle().transition(LifeCycle.State.STARTING);
        getLifeCycle().transition(LifeCycle.State.RUNNING);
      }
    }

    @Override
    public CompletableFuture<TermIndex> notifyInstallSnapshotFromLeader(final RoleInfoProto roleInfoProto,
        final TermIndex firstTermIndexInLog) {
      installs.incrementAndGet();
      final TermIndex boundary = TermIndex.valueOf(firstTermIndexInLog.getTerm(), firstTermIndexInLog.getIndex() - 1);
      try {
        registerMarker(boundary);
      } catch (final IOException e) {
        return CompletableFuture.failedFuture(e);
      }
      installedBoundary.set(boundary);
      afterInstall.run();
      return CompletableFuture.completedFuture(boundary);
    }

    private void registerMarker(final TermIndex termIndex) throws IOException {
      final File file = storage.getSnapshotFile(termIndex.getTerm(), termIndex.getIndex());
      if (!file.exists() && !file.createNewFile())
        throw new IOException("Cannot create snapshot marker " + file);
      storage.updateLatestSnapshot(new SingleFileSnapshotInfo(new FileInfo(file.toPath(), null), termIndex));
    }

    private void loadLatestSnapshot() {
      final SnapshotInfo latest = storage.getLatestSnapshot();
      if (latest != null)
        setLastAppliedTermIndex(latest.getTermIndex());
    }
  }
}
