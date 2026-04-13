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

import com.arcadedb.log.LogManager;
import org.apache.ratis.RaftTestUtil;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.MiniRaftClusterWithGrpc;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Base class for Raft HA split-brain and partition tests using Ratis MiniRaftClusterWithGrpc.
 * <p>
 * Each Ratis peer runs a {@link CountingStateMachine} that tracks applied entries.
 * This tests Raft consensus mechanics (leader election, log replication, convergence)
 * without requiring full ArcadeDB server instances.
 * <p>
 * Subclasses submit entries via {@link #submitEntry}, simulate partitions via
 * {@link #killPeer} and {@link #restartPeer}, and verify convergence via
 * {@link #assertAllPeersConverged}.
 */
@Tag("IntegrationTest")
public abstract class BaseMiniRaftTest {

  private MiniRaftClusterWithGrpc cluster;
  private List<RaftPeer>          peers;

  /**
   * Number of Raft peers in this test cluster. Override to return 3 or 5.
   */
  protected abstract int getPeerCount();

  /**
   * Simple state machine that counts applied entries for convergence assertions.
   */
  static class CountingStateMachine extends BaseStateMachine {
    @Override
    public CompletableFuture<Message> applyTransaction(final TransactionContext trx) {
      updateLastAppliedTermIndex(trx.getLogEntry().getTerm(), trx.getLogEntry().getIndex());
      return CompletableFuture.completedFuture(Message.EMPTY);
    }
  }

  @BeforeEach
  public void setUp() throws Exception {
    final RaftProperties properties = new RaftProperties();

    cluster = (MiniRaftClusterWithGrpc) MiniRaftClusterWithGrpc.FACTORY.newCluster(getPeerCount(), properties);
    cluster.setStateMachineRegistry(groupId -> new CountingStateMachine());
    cluster.start();

    peers = new ArrayList<>(cluster.getPeers());

    // Wait for leader election before any test operations
    RaftTestUtil.waitForLeader(cluster);
    LogManager.instance().log(this, Level.INFO, "BaseMiniRaftTest: %d-node cluster started, leader elected", getPeerCount());
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (cluster != null)
      cluster.close();
  }

  /**
   * Submits a Raft log entry via the Raft client. The entry content is a simple marker string;
   * the purpose is to exercise Raft consensus, not database operations.
   */
  protected RaftClientReply submitEntry(final String marker) throws IOException {
    final ByteString payload = ByteString.copyFrom(marker, StandardCharsets.UTF_8);
    try (final RaftClient client = cluster.createClient()) {
      return client.io().send(Message.valueOf(payload));
    }
  }

  /**
   * Compatibility alias used by split-brain tests ported from ha-redesign.
   * Submits a schema-like entry (the actual content is a marker string since the
   * counting state machine does not interpret the payload).
   */
  protected RaftClientReply submitSchemaEntry(final String databaseName, final String schemaJson) throws IOException {
    return submitEntry("schema:" + databaseName + ":" + schemaJson);
  }

  /**
   * Kills the Raft peer at {@code peerIndex} (simulates a crash or network partition).
   */
  protected void killPeer(final int peerIndex) {
    final RaftPeerId peerId = peers.get(peerIndex).getId();
    LogManager.instance().log(this, Level.INFO, "BaseMiniRaftTest: killing peer %s (index %d)", peerId, peerIndex);
    cluster.killServer(peerId);
  }

  /**
   * Restarts the Raft peer at {@code peerIndex}.
   * The peer rejoins the cluster and catches up via log replay or snapshot install.
   */
  protected void restartPeer(final int peerIndex) throws IOException {
    final RaftPeerId peerId = peers.get(peerIndex).getId();
    LogManager.instance().log(this, Level.INFO, "BaseMiniRaftTest: restarting peer %s (index %d)", peerId, peerIndex);
    cluster.restartServer(peerId, false);
  }

  /**
   * Returns the peer index of the current Raft leader, or -1 if no leader is elected.
   */
  protected int findLeaderPeerIndex() {
    try {
      final RaftServer.Division leader = cluster.getLeader();
      if (leader == null)
        return -1;
      final RaftPeerId leaderId = leader.getId();
      for (int i = 0; i < peers.size(); i++)
        if (peers.get(i).getId().equals(leaderId))
          return i;
    } catch (final Exception e) {
      // No leader yet or cluster in flux
    }
    return -1;
  }

  /**
   * Returns the MiniRaftClusterWithGrpc for direct cluster control.
   */
  protected MiniRaftClusterWithGrpc getCluster() {
    return cluster;
  }

  /**
   * Returns the ordered list of Raft peers (index matches setup order).
   */
  protected List<RaftPeer> getPeers() {
    return peers;
  }

  /**
   * Waits until all live peers have applied at least {@code minEntryCount} log entries,
   * then asserts convergence.
   */
  protected void assertAllPeersConverged(final long minEntryCount) throws InterruptedException {
    final long deadline = System.currentTimeMillis() + 30_000;
    while (System.currentTimeMillis() < deadline) {
      boolean allReady = true;
      for (int i = 0; i < getPeerCount(); i++) {
        try {
          final RaftServer.Division division = cluster.getDivision(peers.get(i).getId());
          if (division == null) {
            allReady = false;
            break;
          }
          final TermIndex ti = division.getStateMachine().getLastAppliedTermIndex();
          if (ti == null || ti.getIndex() < minEntryCount) {
            allReady = false;
            break;
          }
        } catch (final Exception e) {
          allReady = false;
          break;
        }
      }
      if (allReady)
        break;
      Thread.sleep(200);
    }

    // Assert all peers reached at least minEntryCount
    for (int i = 0; i < getPeerCount(); i++) {
      try {
        final RaftServer.Division division = cluster.getDivision(peers.get(i).getId());
        if (division == null)
          continue;
        final TermIndex ti = division.getStateMachine().getLastAppliedTermIndex();
        assertThat(ti).as("Peer %d lastAppliedTermIndex should not be null", i).isNotNull();
        assertThat(ti.getIndex())
            .as("Peer %d should have applied at least %d entries", i, minEntryCount)
            .isGreaterThanOrEqualTo(minEntryCount);
      } catch (final Exception e) {
        // Peer may have been killed; skip
      }
    }
  }
}
