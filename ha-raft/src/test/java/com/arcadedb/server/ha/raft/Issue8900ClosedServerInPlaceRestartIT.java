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
import com.arcadedb.database.Database;
import com.arcadedb.log.LogManager;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.util.LifeCycle;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #8900 (follow-up of #8898): a follower whose Ratis server was closed the way the JVM-pause
 * monitor closes it must be restarted in place by the {@link HealthMonitor} and become a working voter again - caught
 * up to the leader, with its Raft log kept (no divergence reformat), and able to form a quorum once the leader goes.
 * <p>
 * Ratis's {@code RaftServerProxy.handleJvmPause} calls {@code close()} on the pause monitor's own thread, and the
 * in-place restart's {@code close()} of the old server ends with {@code pauseMonitor.stop()}, which INTERRUPTS that
 * thread. In the #8898 incident the close was still waiting for the old gRPC server to terminate when the restart
 * ran, so the interrupt cut it short ({@code Interrupted shutdown GrpcServerProtocolService}). The test drives both
 * shapes: a close that completed before the restart, and a close interrupted while it is shutting the gRPC services
 * down.
 */
@Tag("slow")
class Issue8900ClosedServerInPlaceRestartIT extends BaseRaftHATest {

  private static final long HEALTH_CHECK_INTERVAL_MS = 500L;

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Override
  protected boolean persistentRaftStorage() {
    return true;
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_HEALTH_CHECK_INTERVAL, HEALTH_CHECK_INTERVAL_MS);
  }

  @Test
  void followerClosedByThePauseMonitorRecoversInPlace() throws Exception {
    runScenario(false, false);
  }

  @Test
  void followerWhosePauseCloseIsInterruptedRecoversInPlace() throws Exception {
    runScenario(false, true);
  }

  /** The #8898 shape: the paused node was the LEADER, and the other two elected a new one while it was closed. */
  @Test
  void leaderClosedByThePauseMonitorRecoversInPlaceAsAFollower() throws Exception {
    runScenario(true, true);
  }

  private void runScenario(final boolean closeTheLeader, final boolean interruptTheClose) throws Exception {
    final int initialLeader = findLeaderIndex();
    assertThat(initialLeader).isGreaterThanOrEqualTo(0);
    final int target = closeTheLeader ? initialLeader : (initialLeader + 1) % getServerCount();
    final RaftHAServer node = getRaftPlugin(target).getRaftHAServer();

    writeVertices(initialLeader, "BeforePause", 20);
    waitForReplicationIsCompleted(target);

    final RaftServer oldServer = node.getRaftServerForTesting();
    closeLikeThePauseMonitor(oldServer, interruptTheClose);

    // The health monitor restarts the server in place and the new division runs.
    Awaitility.await().atMost(60, TimeUnit.SECONDS).pollInterval(200, TimeUnit.MILLISECONDS)
        .until(() -> node.getRaftServerForTesting() != oldServer && node.getRaftLifeCycleState() == LifeCycle.State.RUNNING);
    assertThat(node.getRecoverRestartCount()).isGreaterThanOrEqualTo(1);

    // The restart verified the OLD server's gRPC services against the real Ratis, not against nothing: they are found,
    // and none is still running beside the new server.
    final var oldGrpcServers = OldRatisServerTermination.grpcServersOf(oldServer.getServerRpc());
    assertThat(oldGrpcServers).as("the old server's gRPC services must be readable").isNotEmpty();
    assertThat(oldGrpcServers.values()).allMatch(server -> server.isTerminated());

    // Writes made after the restart reach it: the leader's appends land on the new division.
    final int leader = awaitLeader();
    writeVertices(leader, "AfterPause", 20);
    final long leaderCommit = getRaftPlugin(leader).getRaftHAServer().getCommitIndex();
    Awaitility.await().atMost(60, TimeUnit.SECONDS).pollInterval(500, TimeUnit.MILLISECONDS)
        .untilAsserted(() -> assertThat(node.getLastAppliedIndex())
            .as("the restarted node must catch up to the leader's commit index")
            .isGreaterThanOrEqualTo(leaderCommit));
    if (leader != target)
      Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(200, TimeUnit.MILLISECONDS)
          .untilAsserted(() -> assertThat(node.getLeaderContactElapsedMs())
              .as("the restarted follower must hear from its leader")
              .isBetween(0L, 5_000L));

    // Nothing was diverged: the restart kept the Raft log, and no reformat turned the node into an empty voter.
    assertThat(node.getFormatRestartCount()).as("no Raft-storage reformat").isZero();

    // The restarted node must be a working voter: stop another node (the leader when it is not the restarted one), and
    // the restarted node and the last one must elect a leader and commit.
    final int victim = leader != target ? leader : (target + 1) % getServerCount();
    final int survivor = 3 - target - victim;
    LogManager.instance().log(this, Level.INFO, "TEST: stopping server %d", victim);
    getServer(victim).stop();
    Awaitility.await().atMost(60, TimeUnit.SECONDS).pollInterval(200, TimeUnit.MILLISECONDS)
        .until(() -> getRaftPlugin(target).isLeader() || getRaftPlugin(survivor).isLeader());
    final int newLeader = getRaftPlugin(target).isLeader() ? target : survivor;
    writeVertices(newLeader, "AfterStop", 10);

    assertThat(node.getFormatRestartCount()).as("no Raft-storage reformat").isZero();
    Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(500, TimeUnit.MILLISECONDS)
        .untilAsserted(() -> {
          assertThat(countOn(target, "AfterStop")).isEqualTo(10L);
          assertThat(countOn(survivor, "AfterStop")).isEqualTo(10L);
        });
  }

  private int awaitLeader() {
    final int leader = findLeaderIndex();
    assertThat(leader).as("a leader must be elected").isGreaterThanOrEqualTo(0);
    return leader;
  }

  private void writeVertices(final int serverIndex, final String type, final int count) {
    final Database db = getServerDatabase(serverIndex, getDatabaseName());
    db.transaction(() -> {
      if (!db.getSchema().existsType(type))
        db.getSchema().createVertexType(type);
    });
    db.transaction(() -> {
      for (int i = 0; i < count; i++)
        db.newVertex(type).set("id", i).save();
    });
  }

  /**
   * Closes {@code server} the way {@code RaftServerProxy.handleJvmPause} does: {@code close()} on a thread that the
   * proxy's {@code JvmPauseMonitor} knows as its own, so the in-place restart's {@code pauseMonitor.stop()} interrupts
   * it exactly as it interrupts the real monitor thread. With {@code interrupt}, the close runs interrupted, the
   * cut-short shutdown of the #8898 incident.
   */
  private static void closeLikeThePauseMonitor(final RaftServer server, final boolean interrupt) throws Exception {
    final Object pauseMonitor = field(server.getClass(), "pauseMonitor").get(server);
    @SuppressWarnings("unchecked")
    final AtomicReference<Thread> threadRef = (AtomicReference<Thread>) field(pauseMonitor.getClass(), "threadRef")
        .get(pauseMonitor);

    final Thread closer = new Thread(() -> {
      // With the flag already set, every wait inside the close gives up at once - the gRPC awaitTermination included,
      // which logs "Interrupted shutdown GrpcServerProtocolService" exactly as in the #8898 incident.
      if (interrupt)
        Thread.currentThread().interrupt();
      try {
        server.close();
      } catch (final IOException e) {
        LogManager.instance().log(Issue8900ClosedServerInPlaceRestartIT.class, Level.WARNING, "TEST: close failed", e);
      }
    }, "Issue8900-JvmPauseMonitor-close");
    closer.setDaemon(true);

    // The real monitor thread leaves its loop once it is no longer the registered thread; this one takes its place.
    threadRef.set(closer);
    closer.start();
  }

  private static Field field(final Class<?> type, final String name) throws NoSuchFieldException {
    for (Class<?> c = type; c != null; c = c.getSuperclass()) {
      try {
        final Field f = c.getDeclaredField(name);
        f.setAccessible(true);
        return f;
      } catch (final NoSuchFieldException ignored) {
        // keep looking in the superclass
      }
    }
    throw new NoSuchFieldException(name);
  }
}
