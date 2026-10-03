/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
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

import com.arcadedb.log.WarningCapture;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.io.grpc.Server;
import org.apache.ratis.util.LifeCycle;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.ToLongFunction;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for the checks the in-place Ratis restart makes on the OLD server before it starts a new one (issue
 * #8900), and for the follower's leader-contact figure published next to {@code raftState}.
 */
class OldRatisServerTerminationTest {

  @Test
  void aCloseInProgressIsWaitedForUntilItFinishes() {
    final AtomicInteger reads = new AtomicInteger();
    // CLOSING for the first three reads, then CLOSED: the close running on another thread finished.
    final LifeCycle.State state = OldRatisServerTermination.awaitCloseInProgress(
        () -> reads.incrementAndGet() <= 3 ? LifeCycle.State.CLOSING : LifeCycle.State.CLOSED, 10_000L, () -> false);

    assertThat(state).isEqualTo(LifeCycle.State.CLOSED);
    assertThat(reads.get()).isEqualTo(4);
  }

  @Test
  void noCloseInProgressIsNotWaitedFor() {
    final AtomicInteger reads = new AtomicInteger();
    final LifeCycle.State state = OldRatisServerTermination.awaitCloseInProgress(() -> {
      reads.incrementAndGet();
      return LifeCycle.State.RUNNING;
    }, 10_000L, () -> false);

    assertThat(state).isEqualTo(LifeCycle.State.RUNNING);
    assertThat(reads.get()).isEqualTo(1);
  }

  @Test
  void aCloseThatNeverFinishesIsGivenUpOnAtTheBound() {
    // A short wait expected to time out: the assertion is on the answer, not on the elapsed time.
    final LifeCycle.State state = OldRatisServerTermination.awaitCloseInProgress(() -> LifeCycle.State.CLOSING, 200L, () -> false);
    assertThat(state).isEqualTo(LifeCycle.State.CLOSING);
  }

  @Test
  void aShutdownEndsTheWaitAtOnce() {
    final AtomicInteger checks = new AtomicInteger();
    // A close that never finishes, and a shutdown requested on the second check: the 60s wait must not run out.
    final LifeCycle.State state = OldRatisServerTermination.awaitCloseInProgress(() -> LifeCycle.State.CLOSING, 60_000L,
        () -> checks.incrementAndGet() >= 2);

    assertThat(state).isEqualTo(LifeCycle.State.CLOSING);
    assertThat(checks.get()).isEqualTo(2);
  }

  @Test
  void anAsynchronousDivisionCloseIsWaitedForUntilClosed() {
    final AtomicInteger reads = new AtomicInteger();
    // RUNNING before its close task starts, then CLOSING, then CLOSED.
    final LifeCycle.State state = OldRatisServerTermination.awaitClosed(() -> switch (reads.incrementAndGet()) {
      case 1 -> LifeCycle.State.RUNNING;
      case 2 -> LifeCycle.State.CLOSING;
      default -> LifeCycle.State.CLOSED;
    }, 10_000L, () -> false);

    assertThat(state).isEqualTo(LifeCycle.State.CLOSED);
    assertThat(reads.get()).isEqualTo(3);
  }

  @Test
  void terminatedServersAreLeftAlone() {
    final FakeServer server = new FakeServer(true, false);
    assertThat(OldRatisServerTermination.terminateGrpcServers(Map.of("protocol", server), 1_000L)).isEmpty();
    assertThat(server.shutdownNowCalls).isZero();
  }

  @Test
  void aServerStillRunningIsShutDownAgainAndWaitedFor() {
    // The #8898 shape: the close's awaitTermination was interrupted, so the server was shut down but never waited for.
    final FakeServer server = new FakeServer(false, true);
    assertThat(OldRatisServerTermination.terminateGrpcServers(Map.of("GrpcServerProtocolService", server), 1_000L)).isEmpty();
    assertThat(server.shutdownNowCalls).isEqualTo(1);
    assertThat(server.isTerminated()).isTrue();
  }

  @Test
  void aServerThatDoesNotTerminateIsReported() {
    final Map<String, Server> servers = new LinkedHashMap<>();
    servers.put("GrpcServerProtocolService", new FakeServer(false, false));
    servers.put("GrpcClientProtocolService", new FakeServer(false, true));

    assertThat(OldRatisServerTermination.terminateGrpcServers(servers, 200L)).containsExactly("GrpcServerProtocolService");
  }

  @Test
  void aNonGrpcRpcHasNoServersToCheck() {
    assertThat(OldRatisServerTermination.serversOf(null)).isEmpty();
  }

  @Test
  void theServersMapIsReadReflectively() {
    OldRatisServerTermination.resetForTesting();
    try {
      final FakeServer server = new FakeServer(false, true);
      final Map<String, Server> servers = OldRatisServerTermination.serversOf(new RpcWithServers(server));
      assertThat(servers).containsOnlyKeys("GrpcServerProtocolService");
      assertThat(servers.get("GrpcServerProtocolService")).isSameAs(server);

      // Another class without the field: nothing to check, and the check stays on for the next restart.
      assertThat(OldRatisServerTermination.serversOf(new Object())).isEmpty();
      assertThat(OldRatisServerTermination.serversOf(new RpcWithServers(server))).hasSize(1);
    } finally {
      OldRatisServerTermination.resetForTesting();
    }
  }

  @Test
  void aSkippedCheckOnAnotherRpcClassIsReportedOnceAtWarning() {
    OldRatisServerTermination.resetForTesting();
    try {
      final FakeServer server = new FakeServer(false, true);
      // Resolve the cached field on one class, then hand in an instance of a different class that also has one.
      assertThat(OldRatisServerTermination.serversOf(new RpcWithServers(server))).hasSize(1);

      final List<String> warnings = WarningCapture.captureWarnings(() -> {
        assertThat(OldRatisServerTermination.serversOf(new OtherRpcWithServers(server))).isEmpty();
        assertThat(OldRatisServerTermination.serversOf(new OtherRpcWithServers(server))).isEmpty();
      });
      assertThat(warnings).hasSize(1);
      assertThat(warnings.getFirst()).contains("will not verify");
    } finally {
      OldRatisServerTermination.resetForTesting();
    }
  }

  @Test
  void aServersFieldThatIsNotAMapIsIgnored() {
    OldRatisServerTermination.resetForTesting();
    try {
      assertThat(OldRatisServerTermination.serversOf(new RpcWithWrongField())).isEmpty();
    } finally {
      OldRatisServerTermination.resetForTesting();
    }
  }

  /** Shaped like {@code GrpcServicesImpl}: a private {@code servers} map from service name to gRPC server. */
  private static final class RpcWithServers {
    private final Map<String, Server> servers = new LinkedHashMap<>();

    RpcWithServers(final Server server) {
      servers.put("GrpcServerProtocolService", server);
    }
  }

  private static final class OtherRpcWithServers {
    @SuppressWarnings("unused")
    private final Map<String, Server> servers = new LinkedHashMap<>();

    OtherRpcWithServers(final Server server) {
      servers.put("GrpcServerProtocolService", server);
    }
  }

  private static final class RpcWithWrongField {
    @SuppressWarnings("unused")
    private final String servers = "not a map";
  }

  @Test
  void peerLastAnsweredAtIsTheFreshAdvertisementTime() {
    final AtomicLong now = new AtomicLong(1_000_000L);
    final PeerCapabilityRegistry registry = new PeerCapabilityRegistry();
    registry.setClock(now::get);
    final ToLongFunction<String> lastAnswered = RaftHAServer.peerLastAnsweredAt(registry);

    assertThat(lastAnswered.applyAsLong("peer-1")).isEqualTo(-1L);

    registry.record(registry.generation(), "peer-1", Set.of("schema-delta"), "test");
    assertThat(lastAnswered.applyAsLong("peer-1")).isEqualTo(1_000_000L);

    // Fed to ClusterMonitor on the same clock: an answer after the streak start is what the early reset needs.
    final List<String> resets = new ArrayList<>();
    final ClusterMonitor monitor = new ClusterMonitor(10L, 0L, null, false, 10_000L, 30_000L, resets::add);
    monitor.setClock(now::get);
    monitor.setPeerLastAnsweredAt(lastAnswered);
    monitor.updateLeaderCommitIndex(1000L);
    monitor.updateReplicaMatchIndex("peer-1", 1000L, 12_000L); // streak starts at 1_000_000
    now.addAndGet(5_000L);
    registry.record(registry.generation(), "peer-1", Set.of("schema-delta"), "test");
    monitor.updateReplicaMatchIndex("peer-1", 1000L, 17_000L);
    assertThat(resets).containsExactly("peer-1");

    // Expired past the TTL: unknown again.
    now.addAndGet(PeerCapabilityRegistry.ADVERTISEMENT_TTL_MS + 1);
    assertThat(lastAnswered.applyAsLong("peer-1")).isEqualTo(-1L);
  }

  @Test
  void leaderContactIsReadFromAFollowersRoleInfo() {
    final RaftProtos.RoleInfoProto follower = RaftProtos.RoleInfoProto.newBuilder()
        .setRole(RaftProtos.RaftPeerRole.FOLLOWER)
        .setFollowerInfo(RaftProtos.FollowerInfoProto.newBuilder()
            .setLeaderInfo(RaftProtos.ServerRpcProto.newBuilder()
                .setId(RaftProtos.RaftPeerProto.newBuilder().setId(ByteString.copyFromUtf8("leader")))
                .setLastRpcElapsedTimeMs(42_000L)))
        .build();
    assertThat(RaftHAServer.leaderContactElapsedMs(follower)).isEqualTo(42_000L);
  }

  @Test
  void leaderContactIsUnknownWithoutALeaderOrOnTheLeader() {
    final RaftProtos.RoleInfoProto noLeader = RaftProtos.RoleInfoProto.newBuilder()
        .setRole(RaftProtos.RaftPeerRole.FOLLOWER)
        .setFollowerInfo(RaftProtos.FollowerInfoProto.newBuilder()
            .setLeaderInfo(RaftProtos.ServerRpcProto.newBuilder().setLastRpcElapsedTimeMs(5L)))
        .build();
    assertThat(RaftHAServer.leaderContactElapsedMs(noLeader)).isEqualTo(-1L);

    final RaftProtos.RoleInfoProto leader = RaftProtos.RoleInfoProto.newBuilder()
        .setRole(RaftProtos.RaftPeerRole.LEADER)
        .build();
    assertThat(RaftHAServer.leaderContactElapsedMs(leader)).isEqualTo(-1L);
    assertThat(RaftHAServer.leaderContactElapsedMs(null)).isEqualTo(-1L);
  }

  /** A gRPC server that is terminated from the start, terminates on shutdownNow(), or never terminates. */
  private static final class FakeServer extends Server {
    private volatile boolean terminated;
    private final    boolean terminatesOnShutdown;
    private volatile int     shutdownNowCalls;

    FakeServer(final boolean terminated, final boolean terminatesOnShutdown) {
      this.terminated = terminated;
      this.terminatesOnShutdown = terminatesOnShutdown;
    }

    @Override
    public Server start() {
      return this;
    }

    @Override
    public Server shutdown() {
      return this;
    }

    @Override
    public Server shutdownNow() {
      shutdownNowCalls++;
      if (terminatesOnShutdown)
        terminated = true;
      return this;
    }

    @Override
    public boolean isShutdown() {
      return shutdownNowCalls > 0 || terminated;
    }

    @Override
    public boolean isTerminated() {
      return terminated;
    }

    @Override
    public boolean awaitTermination(final long timeout, final TimeUnit unit) throws InterruptedException {
      if (!terminated)
        Thread.sleep(unit.toMillis(timeout));
      return terminated;
    }

    @Override
    public void awaitTermination() {
      throw new UnsupportedOperationException("unbounded wait");
    }
  }
}
