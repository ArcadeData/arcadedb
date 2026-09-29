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

import com.arcadedb.server.ha.raft.ratis.FixedGrpcRpcType;
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
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.impl.BlockRequestHandlingInjection;
import org.apache.ratis.server.impl.MiniRaftCluster;
import org.apache.ratis.server.protocol.TermIndex;
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
import org.awaitility.core.ConditionTimeoutException;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BinaryOperator;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Issue #8460: the leader-side proof of the #8449 install-boundary fix, with a real leader {@code LogAppender} against a
 * real compacted {@code RaftLog}, in process.
 * <p>
 * The leader's log starts at S after a purge and its own snapshot marker M is past S-1, which is what segment-granular
 * purging leaves behind. A follower that fell behind the purge is notified to install, and registers the boundary
 * through the production decision, {@code ArcadeStateMachine.resolveInstalledSnapshotBoundary}, fed with the leader's
 * real marker - the value the follower reads over the bootstrap-state RPC in production. With M past S-1 that is S
 * itself, so the leader's NEXT replication to the follower resolves {@code LogAppender.getPrevious(S + 1)} from its
 * log: an ordinary AppendEntries carrying entries, and no second install notification.
 * <p>
 * The control runs the same scenario with the pre-#8449 boundary S-1 on the STOCK Ratis appender - no
 * {@code FixedGrpcLogAppender}, so the outcome does not depend on any appender-side workaround. Up to Apache Ratis
 * 3.3.0 it observed the loop the fix removed: repeated notifications answered ALREADY_INSTALLED and a follower that
 * never received an entry. Ratis 3.3.1 exempts that state itself ({@code LogAppender.shouldInstallSnapshot} skips the
 * snapshot when the follower's {@code nextIndex} is its snapshot index + 1), so the stock appender now carries the
 * S-1 boundary too, and the control pins that instead (issue #8548). The consequence is worth stating: on 3.3.1 the
 * boundary is no longer what separates a follower that catches up from one that loops, so the main test pins the
 * value #8449 registers rather than proving it necessary. If a Ratis upgrade drops the exemption, the control fails
 * and the loop is back for any follower still registering S-1.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("slow")
class Issue8460LeaderAppenderAfterInstallBoundaryTest {
  // RaftServerImpl.APPEND_ENTRIES / INSTALL_SNAPSHOT are package-private; these are their values.
  private static final String APPEND_ENTRIES_INJECTION   = "RaftServerImpl.appendEntries";
  private static final String INSTALL_SNAPSHOT_INJECTION = "RaftServerImpl.installSnapshot";

  @Test
  void leaderSendsEntriesAfterTheFollowerRegistersItsLogStart() throws Exception {
    final Scenario scenario = new Scenario(true, ArcadeStateMachine::resolveInstalledSnapshotBoundary);
    scenario.run();

    assertThat(scenario.leaderMarker.get().getIndex()).as("the #8449 shape: the leader marker is past S-1")
        .isGreaterThan(scenario.logStart - 1);
    assertThat(scenario.installs.get()).as("one install, no second one").isEqualTo(1);
    assertThat(scenario.installedBoundary.get().getIndex()).as("the #8449 boundary: the leader's log start")
        .isEqualTo(scenario.logStart);
    assertThat(scenario.appendsWithEntriesAfterInstall.get()).as("the next replication carried log entries")
        .isGreaterThan(0);
    assertThat(scenario.notificationsAfterInstall.get()).as("and was not another install notification").isZero();
    assertThat(scenario.caughtUp).as("the follower reached the leader's log end").isTrue();
  }

  @Test
  void controlThePre8449BoundaryNoLongerLoopsOnTheStockRatis331Appender() throws Exception {
    final Scenario scenario = new Scenario(false,
        (firstTermIndexInLog, leaderMarker) -> TermIndex.valueOf(firstTermIndexInLog.getTerm(),
            firstTermIndexInLog.getIndex() - 1));
    scenario.run();

    assertThat(scenario.installedBoundary.get().getIndex()).isEqualTo(scenario.logStart - 1);
    // Up to Ratis 3.3.0 these read "more than 10 re-notifications, no entry, never caught up" (issue #8548).
    assertThat(scenario.notificationsAfterInstall.get())
        .as("Ratis 3.3.1's own exemption: no second notification for a follower anchored on its snapshot").isZero();
    assertThat(scenario.appendsWithEntriesAfterInstall.get()).as("the next replication carried log entries")
        .isGreaterThan(0);
    assertThat(scenario.caughtUp).as("the follower reached the leader's log end").isTrue();
  }

  /** One leader, two followers; one of them falls behind a compacted leader log and installs from it. */
  private static final class Scenario {
    final boolean                    productionAppender;
    final BinaryOperator<TermIndex>  boundaryDecision;
    final AtomicInteger              installs                       = new AtomicInteger();
    final AtomicReference<TermIndex> installedBoundary              = new AtomicReference<>();
    final AtomicReference<TermIndex> leaderMarker                   = new AtomicReference<>();
    final AtomicInteger              notificationsAfterInstall      = new AtomicInteger();
    final AtomicInteger              appendsWithEntriesAfterInstall = new AtomicInteger();
    long    logStart;
    boolean caughtUp;

    Scenario(final boolean productionAppender, final BinaryOperator<TermIndex> boundaryDecision) {
      this.productionAppender = productionAppender;
      this.boundaryDecision = boundaryDecision;
    }

    void run() throws Exception {
      final RaftProperties properties = new RaftProperties();
      if (productionAppender)
        // The wiring of RaftPropertiesBuilder: FixedGrpcRpcType.name() is "GRPC", so Rpc.setType(...) would select the
        // stock GrpcFactory; Ratis instantiates a class name instead.
        properties.set(RaftConfigKeys.Rpc.TYPE_KEY, FixedGrpcRpcType.class.getName());
      else
        RaftConfigKeys.Rpc.setType(properties, SupportedRpcType.GRPC);
      // Notification mode, as ArcadeDB runs it; snapshots only where this test takes them.
      RaftServerConfigKeys.Log.Appender.setInstallSnapshotEnabled(properties, false);
      RaftServerConfigKeys.Snapshot.setAutoTriggerEnabled(properties, false);
      RaftServerConfigKeys.Log.setPurgeGap(properties, 1);
      // Small segments, so the purge leaves the log start on a segment boundary inside the snapshot's range, as
      // segment-granular purging does in production (issue #8449).
      RaftServerConfigKeys.Log.setSegmentSizeMax(properties, SizeInBytes.valueOf("1KB"));

      final AtomicReference<Supplier<TermIndex>> leaderMarkerSource = new AtomicReference<>(() -> null);
      final MiniRaftClusterWithGrpc cluster = new MiniRaftClusterWithGrpc(MiniRaftCluster.generateIds(3, 0), properties,
          null);
      cluster.setStateMachineRegistry((StateMachine.Registry) groupId -> new MarkerStateMachine(
          firstTermIndexInLog -> boundaryDecision.apply(firstTermIndexInLog, leaderMarkerSource.get().get()),
          installs, installedBoundary));
      try {
        cluster.start();
        final RaftServer.Division leader = RaftTestUtil.waitForLeader(cluster);
        final RaftPeerId followerId = cluster.getFollowers().get(0).getId();
        leaderMarkerSource.set(() -> {
          final SnapshotInfo latest = leader.getStateMachine().getLatestSnapshot();
          return latest != null ? latest.getTermIndex() : null;
        });

        CodeInjectionForTesting.put(APPEND_ENTRIES_INJECTION, (localId, remoteId, args) -> {
          if (installs.get() > 0 && followerId.equals(localId) && args.length > 1
              && args[1] instanceof AppendEntriesRequestProto request && request.getEntriesCount() > 0)
            appendsWithEntriesAfterInstall.incrementAndGet();
          return false;
        });
        CodeInjectionForTesting.put(INSTALL_SNAPSHOT_INJECTION, (localId, remoteId, args) -> {
          if (installs.get() > 0 && followerId.equals(localId))
            notificationsAfterInstall.incrementAndGet();
          return false;
        });

        try (final RaftClient client = cluster.createClient(leader.getId())) {
          send(client, 10);
          cluster.killServer(followerId);
          send(client, 200);

          // Leader marker M, then a purge up to it: the log start S lands on a segment boundary at or before M.
          long marker = leader.getStateMachine().takeSnapshot();
          leader.getRaftLog().purge(marker).get(10, TimeUnit.SECONDS);
          logStart = leader.getRaftLog().getStartIndex();
          if (marker == logStart - 1) {
            // The one purge point where the marker already ends at S-1: move it past, into the #8449 shape.
            send(client, 1);
            marker = leader.getStateMachine().takeSnapshot();
          }
          leaderMarker.set(leader.getStateMachine().getLatestSnapshot().getTermIndex());
          assertThat(logStart).as("the leader log must have been compacted").isGreaterThan(20);
          assertThat(marker).as("the leader marker must be past S-1").isGreaterThan(logStart - 1);

          cluster.restartServer(followerId, false);
          send(client, 5);

          final long leaderNext = leader.getRaftLog().getNextIndex();
          final RaftServer.Division follower = cluster.getDivision(followerId);
          await().atMost(30, TimeUnit.SECONDS).pollInterval(100, TimeUnit.MILLISECONDS)
              .until(() -> installs.get() > 0);
          try {
            await().atMost(productionAppender ? 30 : 5, TimeUnit.SECONDS).pollInterval(100, TimeUnit.MILLISECONDS)
                .until(() -> follower.getRaftLog().getNextIndex() >= leaderNext);
            caughtUp = true;
          } catch (final ConditionTimeoutException e) {
            caughtUp = false;
          }
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
   * A state machine that snapshots the way {@code ArcadeStateMachine} does - a zero-byte {@code snapshot.<term>_<index>}
   * marker - and answers an install notification by registering the boundary the given decision picks, as
   * {@code ArcadeStateMachine.installSnapshotFromLeader} does once its database copy is done.
   */
  private static final class MarkerStateMachine extends BaseStateMachine {
    private final SimpleStateMachineStorage  storage = new SimpleStateMachineStorage();
    private final UnaryOperator<TermIndex>   boundary;
    private final AtomicInteger              installs;
    private final AtomicReference<TermIndex> installedBoundary;

    MarkerStateMachine(final UnaryOperator<TermIndex> boundary, final AtomicInteger installs,
        final AtomicReference<TermIndex> installedBoundary) {
      this.boundary = boundary;
      this.installs = installs;
      this.installedBoundary = installedBoundary;
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
      final TermIndex registered = boundary.apply(firstTermIndexInLog);
      try {
        registerMarker(registered);
      } catch (final IOException e) {
        return CompletableFuture.failedFuture(e);
      }
      installedBoundary.set(registered);
      installs.incrementAndGet();
      return CompletableFuture.completedFuture(registered);
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
