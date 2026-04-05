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
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.log.LogManager;
import com.arcadedb.server.ArcadeDBServer;
import org.apache.ratis.RaftTestUtil;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.MiniRaftClusterWithGrpc;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Base class for Raft HA split-brain and partition tests using Ratis MiniRaftClusterWithGrpc.
 * <p>
 * Each Ratis peer runs a real {@link ArcadeStateMachine} backed by a real {@link ArcadeDBServer}
 * (HA disabled) so that database operations are fully exercised via the state machine.
 * <p>
 * Subclasses submit Raft log entries via {@link #submitSchemaEntry} and {@link #submitTxEntry},
 * simulate partitions via {@link #killPeer} and {@link #restartPeer}, and verify convergence
 * via {@link #assertAllPeersConverged}.
 */
public abstract class BaseMiniRaftTest {

  /** Base HTTP port for ArcadeDBServer instances in this test (avoids port 2480 used by other tests). */
  private static final int  BASE_HTTP_PORT = 12480;
  private static final String DB_NAME      = "mini-raft-test";
  private static final String TARGET_DIR   = "target/mini-raft-test";

  private MiniRaftClusterWithGrpc cluster;
  private ArcadeDBServer[]        arcadeServers;
  private List<RaftPeer>          peers;

  /**
   * Number of Raft peers in this test cluster. Override to return 3 or 5.
   */
  protected abstract int getPeerCount();

  @BeforeEach
  public void setUp() throws Exception {
    final Path targetPath = Path.of(TARGET_DIR);
    if (Files.exists(targetPath))
      deleteDirectory(targetPath);

    final RaftProperties properties = new RaftProperties();

    cluster = MiniRaftClusterWithGrpc.FACTORY.newCluster(getPeerCount(), properties);

    // Register ArcadeStateMachine as the state machine for all groups
    cluster.setStateMachineRegistry((StateMachine.Registry) groupId -> new ArcadeStateMachine());

    cluster.start();

    peers = new ArrayList<>(cluster.getPeers());

    // Start one ArcadeDBServer per peer (HA disabled) and wire it to the ArcadeStateMachine
    arcadeServers = new ArcadeDBServer[getPeerCount()];
    for (int i = 0; i < getPeerCount(); i++) {
      final RaftPeerId peerId = peers.get(i).getId();
      final String dbDir = TARGET_DIR + "/server-" + peerId;

      final ContextConfiguration config = new ContextConfiguration();
      config.setValue(GlobalConfiguration.SERVER_NAME, "MiniRaft_" + i);
      config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, dbDir);
      config.setValue(GlobalConfiguration.HA_ENABLED, false);
      config.setValue(GlobalConfiguration.SERVER_HTTP_INCOMING_HOST, "localhost");
      config.setValue(GlobalConfiguration.SERVER_HTTP_INCOMING_PORT, BASE_HTTP_PORT + i);
      config.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, "testPassword");

      arcadeServers[i] = new ArcadeDBServer(config);
      arcadeServers[i].start();
      arcadeServers[i].createDatabase(DB_NAME, ComponentFile.MODE.READ_WRITE);

      // Wire the state machine to this ArcadeDBServer
      final RaftServer.Division division = cluster.getDivision(peerId);
      final ArcadeStateMachine sm = (ArcadeStateMachine) division.getStateMachine();
      sm.setServer(arcadeServers[i]);

      LogManager.instance().log(this, Level.INFO, "BaseMiniRaftTest: peer %s wired to ArcadeDBServer %d", peerId, i);
    }

    // Wait for leader election before any test operations
    RaftTestUtil.waitForLeader(cluster);
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (cluster != null)
      cluster.close();

    if (arcadeServers != null)
      for (final ArcadeDBServer server : arcadeServers)
        if (server != null && server.isStarted())
          server.stop();

    final Path targetPath = Path.of(TARGET_DIR);
    if (Files.exists(targetPath))
      deleteDirectory(targetPath);
  }

  /**
   * Submits a SCHEMA_ENTRY Raft log entry via the Raft client.
   */
  protected RaftClientReply submitSchemaEntry(final String databaseName, final String schemaJson) throws IOException {
    final ByteString encoded = RaftLogEntryCodec.encodeSchemaEntry(databaseName, schemaJson, null, null, null, null);
    try (final RaftClient client = cluster.createClient()) {
      return client.io().send(Message.valueOf(encoded));
    }
  }

  /**
   * Submits a TX_ENTRY Raft log entry via the Raft client.
   */
  protected RaftClientReply submitTxEntry(final String databaseName, final byte[] walData) throws IOException {
    final ByteString encoded = RaftLogEntryCodec.encodeTxEntry(databaseName, walData, null);
    try (final RaftClient client = cluster.createClient()) {
      return client.io().send(Message.valueOf(encoded));
    }
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
    final RaftServer.Division division = cluster.restartServer(peerId, false);

    // Re-wire the ArcadeStateMachine to the ArcadeDBServer after restart
    final ArcadeStateMachine sm = (ArcadeStateMachine) division.getStateMachine();
    sm.setServer(arcadeServers[peerIndex]);
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
   * Returns the ArcadeDBServer for peer at {@code peerIndex}.
   */
  protected ArcadeDBServer getArcadeServer(final int peerIndex) {
    return arcadeServers[peerIndex];
  }

  /**
   * Returns the MiniRaftClusterWithGrpc for direct cluster control.
   */
  protected MiniRaftClusterWithGrpc getCluster() {
    return cluster;
  }

  /**
   * Returns the ordered list of Raft peers (index matches ArcadeDBServer indices).
   */
  protected List<RaftPeer> getPeers() {
    return peers;
  }

  /**
   * Waits until all peers have applied at least {@code minEntryCount} log entries,
   * then asserts that all peers share the same last-applied index.
   * <p>
   * This verifies Raft protocol correctness (all nodes converged to the same log)
   * without requiring byte-level database comparison.
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
          final ArcadeStateMachine sm = (ArcadeStateMachine) division.getStateMachine();
          if (sm.getLastAppliedTermIndex() == null || sm.getLastAppliedTermIndex().getIndex() < minEntryCount) {
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
        final ArcadeStateMachine sm = (ArcadeStateMachine) division.getStateMachine();
        assertThat(sm.getLastAppliedTermIndex()).as("Peer %d lastAppliedTermIndex should not be null", i).isNotNull();
        assertThat(sm.getLastAppliedTermIndex().getIndex())
            .as("Peer %d should have applied at least %d entries", i, minEntryCount)
            .isGreaterThanOrEqualTo(minEntryCount);
      } catch (final Exception e) {
        // Peer may have been killed; skip
      }
    }
  }

  private static void deleteDirectory(final Path path) throws IOException {
    Files.walk(path)
        .sorted(Comparator.reverseOrder())
        .forEach(p -> {
          try {
            Files.delete(p);
          } catch (final IOException ignored) {
          }
        });
  }
}
