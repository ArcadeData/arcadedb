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

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Regression test for issue #6202: {@code notifyInstallSnapshotFromLeader} - the Ratis-initiated resync, the one
 * that runs when a follower's log is behind the leader's compacted log - resolved the leader's HTTP address and
 * dialled it with no check at all, while the manual {@code triggerSnapshotDownload()} refused twice before
 * dialling the very same kind of address.
 * <p>
 * The address is only as good as the configuration behind it. With no {@code http} port declared in
 * {@code arcadedb.ha.serverList} a peer's HTTP endpoint is derived from the peer's Raft host plus THIS node's HTTP
 * port, so on a cluster whose nodes share a host every peer collapses onto this node's own endpoint, and on one
 * with mixed ports it can name the wrong peer. Neither outcome reported an error: the reconcile succeeded, the
 * install was recorded, the stale-read floor was dropped and the node returned to the ready set carrying whatever
 * it had copied - a node "repaired" from its own incomplete databases, or from a peer that is itself behind.
 * <p>
 * Both paths now ask the same helper, so they cannot drift apart, and the helper also refuses an address that
 * does not identify one peer - the ambiguity check the client routing tables make (#6183), which matters more
 * here, because a confidently wrong routing address costs one redirect while a confidently wrong snapshot source
 * cannot be undone.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6202SnapshotInstallGuardTest {
  private static final String LEADER_PEER_ID    = "peer-b_2434";
  private static final String LOCAL_HTTP        = "localhost:2480";
  private static final long   FIRST_LOG_INDEX   = 500L;
  private static final long   SNAPSHOT_INDEX    = FIRST_LOG_INDEX - 1;
  private static final long   PERSISTED_APPLIED = 100L;

  // -----------------------------------------------------------------------------------------------
  // The Ratis-initiated install must make the refusals the manual resync makes
  // -----------------------------------------------------------------------------------------------

  /**
   * The reachable misconfiguration: every peer derives to this node's own endpoint, so the follower "downloads"
   * its own incomplete databases onto themselves. The manual path has refused this since #6111; this one did not.
   */
  @Test
  void anInstallFromThisNodesOwnAddressIsRefused(@TempDir final Path tempDir) throws Exception {
    final RaftHAServer raft = mock(RaftHAServer.class);
    when(raft.isLeader()).thenReturn(false);
    when(raft.getUnambiguousPeerHttpAddress(RaftPeerId.valueOf(LEADER_PEER_ID))).thenReturn(LOCAL_HTTP);
    when(raft.getLocalHttpAddress()).thenReturn(LOCAL_HTTP);

    assertRefused(tempDir, raft, "is this node's own");
  }

  /**
   * Leadership can move while an install is in flight, and a leader has no peer to pull from: "downloading" from
   * itself would let the install drop the read floor and durably record the snapshot index as applied on state
   * this node never had.
   */
  @Test
  void anInstallOnANodeThatBelievesItIsTheLeaderIsRefused(@TempDir final Path tempDir) throws Exception {
    final RaftHAServer raft = mock(RaftHAServer.class);
    when(raft.isLeader()).thenReturn(true);
    when(raft.getUnambiguousPeerHttpAddress(RaftPeerId.valueOf(LEADER_PEER_ID))).thenReturn("peer-b:2480");

    assertRefused(tempDir, raft, "this node is the leader");
  }

  /**
   * The refusal #6202 asks for on top of the two the manual path already made: an address two peers both resolve
   * to identifies at most one of them, and reconciling every database from the wrong node is not recoverable.
   */
  @Test
  void anInstallFromAnAddressThatIdentifiesNoSinglePeerIsRefused(@TempDir final Path tempDir) throws Exception {
    final RaftHAServer raft = mock(RaftHAServer.class);
    when(raft.isLeader()).thenReturn(false);
    // The resolver withheld the address: it is claimed by more than one peer, or resolves to nothing.
    when(raft.getUnambiguousPeerHttpAddress(RaftPeerId.valueOf(LEADER_PEER_ID))).thenReturn(null);
    when(raft.getLocalHttpAddress()).thenReturn(LOCAL_HTTP);

    assertRefused(tempDir, raft, "identifies leader");
  }

  /**
   * Control: a genuine peer leader is still installed from, and the install still resolves the state it is
   * supposed to. Without it the three tests above would also pass against a method that refused everything.
   */
  @Test
  void aGenuinePeerLeaderIsStillInstalledFrom(@TempDir final Path tempDir) throws Exception {
    final RaftHAServer raft = mock(RaftHAServer.class);
    when(raft.isLeader()).thenReturn(false);
    when(raft.getUnambiguousPeerHttpAddress(RaftPeerId.valueOf(LEADER_PEER_ID))).thenReturn("peer-b:2480");
    when(raft.getLocalHttpAddress()).thenReturn(LOCAL_HTTP);

    final ArcadeStateMachine sm = newInitializedStateMachine(tempDir);
    sm.setRaftHAServer(raft);
    try {
      setStaleSnapshotAppliedFloor(sm, PERSISTED_APPLIED);

      final TermIndex installed = sm.notifyInstallSnapshotFromLeader(leaderRoleInfo(), TermIndex.valueOf(9L,
          FIRST_LOG_INDEX)).get();

      assertThat(installed.getIndex())
          .as("the snapshot covers every entry BEFORE the first one still in the log")
          .isEqualTo(SNAPSHOT_INDEX);
      assertThat(sm.getStaleSnapshotAppliedFloor())
          .as("an install from a real peer resolves the read floor")
          .isEqualTo(-1L);
      assertThat(sm.readPersistedAppliedIndex()).isEqualTo(SNAPSHOT_INDEX);
    } finally {
      sm.close();
    }
  }

  // -----------------------------------------------------------------------------------------------
  // Helpers
  // -----------------------------------------------------------------------------------------------

  /**
   * Drives the Ratis-initiated install against {@code raft} and asserts it fails with a refusal naming
   * {@code reasonFragment}, leaving the node visibly behind rather than recorded as caught up.
   */
  private void assertRefused(final Path tempDir, final RaftHAServer raft, final String reasonFragment)
      throws Exception {
    final ArcadeStateMachine sm = newInitializedStateMachine(tempDir);
    sm.setRaftHAServer(raft);
    try {
      sm.writePersistedAppliedIndex(PERSISTED_APPLIED, "testdb");
      setStaleSnapshotAppliedFloor(sm, PERSISTED_APPLIED);

      final CompletableFuture<TermIndex> install = sm.notifyInstallSnapshotFromLeader(leaderRoleInfo(),
          TermIndex.valueOf(9L, FIRST_LOG_INDEX));

      assertThatThrownBy(install::get)
          .as("the install must fail so Ratis retries it, rather than record a snapshot it never installed")
          .rootCause()
          .hasMessageStartingWith("Refusing a leader-initiated snapshot install: ")
          .hasMessageContaining(reasonFragment);

      assertThat(sm.getStaleSnapshotAppliedFloor())
          .as("a refused install resolves nothing, so the stale-read floor must stand")
          .isEqualTo(PERSISTED_APPLIED);
      assertThat(sm.readPersistedAppliedIndex())
          .as("and the snapshot index must not be durably recorded as applied")
          .isEqualTo(PERSISTED_APPLIED);
      assertThat(sm.isResyncInProgress())
          .as("the node keeps itself out of the ready set instead of pretending it caught up")
          .isTrue();
    } finally {
      sm.close();
    }
  }

  /** The Ratis role info a follower receives, naming {@link #LEADER_PEER_ID} as the installing leader. */
  private static RaftProtos.RoleInfoProto leaderRoleInfo() {
    return RaftProtos.RoleInfoProto.newBuilder()
        .setFollowerInfo(RaftProtos.FollowerInfoProto.newBuilder()
            .setLeaderInfo(RaftProtos.ServerRpcProto.newBuilder()
                .setId(RaftProtos.RaftPeerProto.newBuilder()
                    .setId(ByteString.copyFromUtf8(LEADER_PEER_ID)))))
        .build();
  }

  /**
   * A state machine with real Ratis storage (the install registers a snapshot marker through it) rooted at
   * {@code tempDir}, and auto-acquire off so a reconcile over zero local databases needs no leader to answer.
   */
  private static ArcadeStateMachine newInitializedStateMachine(final Path tempDir) throws IOException {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, tempDir.resolve("databases").toString());
    config.setValue(GlobalConfiguration.HA_AUTO_ACQUIRE_DATABASES, false);

    final ArcadeStateMachine sm = new ArcadeStateMachine();
    sm.setServer(new ArcadeDBServer(config));
    sm.initialize(stubRaftServer(), RaftGroupId.valueOf(UUID.randomUUID()),
        newFormattedStorage(tempDir.resolve("raft")));
    return sm;
  }

  private static RaftStorage newFormattedStorage(final Path dir) throws IOException {
    return RaftStorage.newBuilder()
        .setDirectory(dir.toFile())
        .setOption(RaftStorage.StartupOption.FORMAT)
        .build();
  }

  private static void setStaleSnapshotAppliedFloor(final ArcadeStateMachine sm, final long floor) throws Exception {
    final Field f = ArcadeStateMachine.class.getDeclaredField("staleSnapshotAppliedFloor");
    f.setAccessible(true);
    ((AtomicLong) f.get(sm)).set(floor);
  }

  /**
   * Minimal {@link RaftServer} stub: {@code BaseStateMachine.initialize()} only needs a non-null {@code getId()};
   * nothing else is reached on this path.
   */
  private static RaftServer stubRaftServer() {
    return (RaftServer) Proxy.newProxyInstance(
        Issue6202SnapshotInstallGuardTest.class.getClassLoader(),
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
