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

import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.SizeInBytes;
import org.apache.ratis.util.TimeDuration;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test that verifies basic Ratis consensus works: 3 nodes elect a leader,
 * replicate an entry, and all nodes apply it via the state machine.
 */
@Tag("IntegrationTest")
class RaftHAServerIT {

  private static final int BASE_PORT = 19860;

  private final List<RaftServer>           servers       = new ArrayList<>();
  private final List<CountingStateMachine> stateMachines = new ArrayList<>();
  private       RaftGroup                  group;
  private       Path                       tempDir;

  @BeforeEach
  void setUp() throws Exception {
    tempDir = Files.createTempDirectory("ratis-test-");

    // Define 3 peers
    final List<RaftPeer> peers = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      final RaftPeerId peerId = RaftPeerId.valueOf("node" + i);
      peers.add(RaftPeer.newBuilder()
          .setId(peerId)
          .setAddress("localhost:" + (BASE_PORT + i))
          .build());
    }

    group = RaftGroup.valueOf(
        RaftGroupId.valueOf(UUID.nameUUIDFromBytes("test-cluster".getBytes())),
        peers);

    // Start all 3 servers
    for (int i = 0; i < 3; i++) {
      final RaftProperties properties = new RaftProperties();

      final Path storagePath = tempDir.resolve("node" + i);
      Files.createDirectories(storagePath);
      RaftServerConfigKeys.setStorageDir(properties, Collections.singletonList(storagePath.toFile()));

      GrpcConfigKeys.Server.setPort(properties, BASE_PORT + i);
      properties.set("raft.server.rpc.type", "GRPC");

      // Fast election for tests
      RaftServerConfigKeys.Rpc.setTimeoutMin(properties, TimeDuration.valueOf(500, TimeUnit.MILLISECONDS));
      RaftServerConfigKeys.Rpc.setTimeoutMax(properties, TimeDuration.valueOf(1000, TimeUnit.MILLISECONDS));

      // Disable snapshot auto-trigger for test
      RaftServerConfigKeys.Snapshot.setAutoTriggerEnabled(properties, false);
      RaftServerConfigKeys.Log.Appender.setInstallSnapshotEnabled(properties, false);
      RaftServerConfigKeys.Log.setSegmentSizeMax(properties, SizeInBytes.valueOf("8MB"));

      final CountingStateMachine sm = new CountingStateMachine();
      stateMachines.add(sm);

      final RaftServer server = RaftServer.newBuilder()
          .setServerId(peers.get(i).getId())
          .setStateMachine(sm)
          .setProperties(properties)
          .setGroup(group)
          .build();

      server.start();
      servers.add(server);
    }

    // Wait for leader election
    waitForLeader();
  }

  @AfterEach
  void tearDown() {
    for (final RaftServer server : servers)
      try {
        server.close();
      } catch (final Exception e) {
        // ignore
      }

    // Clean up temp directories
    try {
      deleteRecursive(tempDir);
    } catch (final Exception e) {
      // ignore
    }
  }

  @Test
  void leaderElection() throws Exception {
    // Verify exactly one leader exists
    int leaderCount = 0;
    for (final RaftServer server : servers)
      if (server.getDivision(group.getGroupId()).getInfo().isLeader())
        leaderCount++;

    assertThat(leaderCount).isEqualTo(1);
  }

  @Test
  void basicReplication() throws Exception {
    try (final RaftClient client = createClient()) {
      // Send a single entry
      final byte[] data = "hello-raft".getBytes();
      final RaftClientReply reply = client.io().send(Message.valueOf(ByteString.copyFrom(data)));
      assertThat(reply.isSuccess()).isTrue();

      // Wait for all state machines to apply the entry
      Awaitility.await()
          .atMost(10, TimeUnit.SECONDS)
          .pollInterval(200, TimeUnit.MILLISECONDS)
          .until(() -> stateMachines.stream().allMatch(sm -> sm.getApplyCount() >= 1));

      // All 3 nodes should have applied exactly 1 entry
      for (final CountingStateMachine sm : stateMachines)
        assertThat(sm.getApplyCount()).isEqualTo(1);
    }
  }

  @Test
  void multipleEntries() throws Exception {
    final int entryCount = 10;

    try (final RaftClient client = createClient()) {
      for (int i = 0; i < entryCount; i++) {
        final byte[] data = ("entry-" + i).getBytes();
        final RaftClientReply reply = client.io().send(Message.valueOf(ByteString.copyFrom(data)));
        assertThat(reply.isSuccess()).isTrue();
      }

      // Wait for all state machines to apply all entries
      Awaitility.await()
          .atMost(10, TimeUnit.SECONDS)
          .pollInterval(200, TimeUnit.MILLISECONDS)
          .until(() -> stateMachines.stream().allMatch(sm -> sm.getApplyCount() >= entryCount));

      // All 3 nodes should have applied exactly 10 entries
      for (int i = 0; i < 3; i++)
        assertThat(stateMachines.get(i).getApplyCount())
            .as("Node %d should have applied %d entries", i, entryCount)
            .isEqualTo(entryCount);
    }
  }

  // -- Helpers --

  private void waitForLeader() {
    Awaitility.await()
        .atMost(10, TimeUnit.SECONDS)
        .pollInterval(200, TimeUnit.MILLISECONDS)
        .ignoreExceptions()
        .until(() -> servers.stream().anyMatch(s -> {
          try {
            return s.getDivision(group.getGroupId()).getInfo().isLeader();
          } catch (final Exception e) {
            return false;
          }
        }));
  }

  private RaftClient createClient() {
    final RaftProperties properties = new RaftProperties();
    properties.set("raft.server.rpc.type", "GRPC");
    return RaftClient.newBuilder()
        .setRaftGroup(group)
        .setProperties(properties)
        .build();
  }

  private static void deleteRecursive(final Path path) throws IOException {
    if (Files.isDirectory(path))
      try (final var entries = Files.list(path)) {
        for (final Path entry : entries.toList())
          deleteRecursive(entry);
      }
    Files.deleteIfExists(path);
  }

  /**
   * Simple state machine that counts how many entries were applied.
   */
  static class CountingStateMachine extends BaseStateMachine {
    private final AtomicInteger applyCount = new AtomicInteger(0);

    @Override
    public CompletableFuture<Message> applyTransaction(final TransactionContext trx) {
      applyCount.incrementAndGet();
      updateLastAppliedTermIndex(trx.getLogEntry().getTerm(), trx.getLogEntry().getIndex());
      return CompletableFuture.completedFuture(Message.EMPTY);
    }

    int getApplyCount() {
      return applyCount.get();
    }
  }
}
