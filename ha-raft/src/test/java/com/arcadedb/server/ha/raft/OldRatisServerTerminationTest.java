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

import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.io.grpc.Server;
import org.apache.ratis.util.LifeCycle;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

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
    assertThat(OldRatisServerTermination.grpcServersOf(null)).isEmpty();
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
