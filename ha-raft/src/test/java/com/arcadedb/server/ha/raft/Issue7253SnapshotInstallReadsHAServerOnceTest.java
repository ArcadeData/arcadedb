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
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Issue #7253: {@code installSnapshotFromLeader} must read the volatile {@code raftHAServer} field ONCE and use
 * that instance for the whole install.
 * <p>
 * It used to read it twice: {@code resolveSnapshotSource(leaderId)} takes its own local precisely so it can
 * <em>refuse</em> rather than throw when the field is unset, and two lines later the cluster token was read
 * straight off the field again. That is the shape #7221 fixed one branch away, in
 * {@code applyInstallDatabaseEntry}'s {@code forceSnapshot} arm (PR #7249), where {@code getLeaderId()} was
 * evaluated as the ARGUMENT to {@code resolveSnapshotSource} and so ran before the null guard inside it.
 * <p>
 * <b>The window this pins.</b> Unlike the #7221 case the second read sat after the refusal check, so a field that
 * was already null was refused before reaching it. What was left is the field turning null <em>between</em> the
 * two reads - a teardown racing an in-flight install - and the test drives exactly that: the mocked HA server
 * clears the state machine's reference as a side effect of the address resolution that happens inside
 * {@code PeerDialAddress.resolve}. Before the fix the install then died with a {@link NullPointerException} on
 * the automatic resync path, the one path #6202 exists because it had no checks at all.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7253SnapshotInstallReadsHAServerOnceTest {

  private static final RaftPeerId LEADER = RaftPeerId.valueOf("leader-peer");
  private static final RaftPeerId LOCAL  = RaftPeerId.valueOf("local-peer");

  /** The first log index AFTER the snapshot, so the install records {@code getIndex() - 1}. */
  private static final long FIRST_INDEX_IN_LOG = 10L;

  @Test
  void aTeardownBetweenTheTwoReadsNoLongerFailsTheInstall(@TempDir final Path raftDirectory,
      @TempDir final Path databaseDirectory) throws Exception {

    final ArcadeStateMachine sm = new ArcadeStateMachine();
    final RaftStorage storage = RaftStorage.newBuilder()
        .setDirectory(raftDirectory.toFile())
        .setOption(RaftStorage.StartupOption.FORMAT)
        .build();
    try {
      sm.setServer(followerServer(databaseDirectory));
      sm.initialize(stubServer(), RaftGroupId.valueOf(UUID.randomUUID()), storage);
      sm.setRaftHAServer(haServerThatIsTornDownMidResolution(sm));

      final TermIndex installed = sm.notifyInstallSnapshotFromLeader(
          leaderRoleInfo(), TermIndex.valueOf(3L, FIRST_INDEX_IN_LOG)).get();

      assertThat(sm.getRaftHAServer())
          .as("the teardown really happened while the install was resolving its source")
          .isNull();
      assertThat(installed.getIndex())
          .as("the install completed from the instance it had already resolved against, instead of "
              + "dereferencing the field a second time and throwing")
          .isEqualTo(FIRST_INDEX_IN_LOG - 1);
      assertThat(sm.readAppliedIndexCounter()).isEqualTo(FIRST_INDEX_IN_LOG - 1);
    } finally {
      sm.close();
      storage.close();
    }
  }

  /**
   * A leader whose HTTP address resolves cleanly, and which clears {@code sm}'s reference to itself the moment it
   * is asked for that address - the last read {@code PeerDialAddress.resolve} makes through its own local, and so
   * the exact instant at which a second read of the field would see {@code null}.
   */
  private static RaftHAServer haServerThatIsTornDownMidResolution(final ArcadeStateMachine sm) {
    final RaftHAServer raftHA = mock(RaftHAServer.class);
    when(raftHA.isLeader()).thenReturn(false);
    when(raftHA.getLocalPeerId()).thenReturn(LOCAL);
    when(raftHA.getLeaderId()).thenReturn(LEADER);
    when(raftHA.getClusterToken()).thenReturn("cluster-token");
    when(raftHA.getUnambiguousPeerHttpAddress(LEADER)).thenAnswer(invocation -> {
      sm.setRaftHAServer(null);
      return "leader-host:2480";
    });
    when(raftHA.getLocalHttpAddress()).thenReturn("local-host:2480");
    when(raftHA.getUnambiguousPeerHttpsAddress(LEADER)).thenReturn(null);
    when(raftHA.getLocalHttpsAddress()).thenReturn(null);
    return raftHA;
  }

  /**
   * A follower holding no databases, with auto-acquire off so the reconciler takes the refresh-existing path and
   * completes without dialling anything: this test is about which field read happens, not about the download.
   */
  private static ArcadeDBServer followerServer(final Path databaseDirectory) {
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, databaseDirectory.toString());
    configuration.setValue(GlobalConfiguration.HA_AUTO_ACQUIRE_DATABASES, false);

    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.getConfiguration()).thenReturn(configuration);
    when(server.getDatabaseNames()).thenReturn(Set.of());
    return server;
  }

  /** The Ratis callback payload the install reads the leader id out of. */
  private static RaftProtos.RoleInfoProto leaderRoleInfo() {
    return RaftProtos.RoleInfoProto.newBuilder()
        .setFollowerInfo(RaftProtos.FollowerInfoProto.newBuilder()
            .setLeaderInfo(RaftProtos.ServerRpcProto.newBuilder()
                .setId(RaftProtos.RaftPeerProto.newBuilder()
                    .setId(ByteString.copyFromUtf8(LEADER.toString()))
                    .build())
                .build())
            .build())
        .build();
  }

  /** Minimal {@link RaftServer} stub: {@code BaseStateMachine.initialize()} only needs a non-null {@code getId()}. */
  private static RaftServer stubServer() {
    return (RaftServer) Proxy.newProxyInstance(
        Issue7253SnapshotInstallReadsHAServerOnceTest.class.getClassLoader(),
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
