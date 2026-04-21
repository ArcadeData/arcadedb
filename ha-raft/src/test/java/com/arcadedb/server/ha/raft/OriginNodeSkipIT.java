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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests the origin-node skip invariant: a log entry tagged with an originPeerId must be
 * skipped on the origin node and applied on all other nodes. This is the core correctness
 * property that prevents double-application during leadership changes.
 * <p>
 * Uses a 3-node Ratis cluster with a lightweight state machine that records apply/skip
 * decisions per entry, isolating the skip logic from database machinery.
 */
@Tag("IntegrationTest")
class OriginNodeSkipIT {

  private static final int BASE_PORT  = 19870;
  private static final int NODE_COUNT = 3;

  private final List<RaftServer>                 servers       = new ArrayList<>();
  private final List<OriginTrackingStateMachine> stateMachines = new ArrayList<>();
  private       RaftGroup                        group;
  private       Path                             tempDir;

  @BeforeEach
  void setUp() throws Exception {
    tempDir = Files.createTempDirectory("ratis-origin-skip-");

    final List<RaftPeer> peers = new ArrayList<>();
    for (int i = 0; i < NODE_COUNT; i++)
      peers.add(RaftPeer.newBuilder()
          .setId(RaftPeerId.valueOf("node" + i))
          .setAddress("localhost:" + (BASE_PORT + i))
          .build());

    group = RaftGroup.valueOf(
        RaftGroupId.valueOf(UUID.nameUUIDFromBytes("origin-skip-test".getBytes())),
        peers);

    for (int i = 0; i < NODE_COUNT; i++) {
      final RaftProperties properties = new RaftProperties();
      final Path storagePath = tempDir.resolve("node" + i);
      Files.createDirectories(storagePath);
      RaftServerConfigKeys.setStorageDir(properties, Collections.singletonList(storagePath.toFile()));
      GrpcConfigKeys.Server.setPort(properties, BASE_PORT + i);
      properties.set("raft.server.rpc.type", "GRPC");

      RaftServerConfigKeys.Rpc.setTimeoutMin(properties, TimeDuration.valueOf(500, TimeUnit.MILLISECONDS));
      RaftServerConfigKeys.Rpc.setTimeoutMax(properties, TimeDuration.valueOf(1000, TimeUnit.MILLISECONDS));
      RaftServerConfigKeys.Snapshot.setAutoTriggerEnabled(properties, false);
      RaftServerConfigKeys.Log.Appender.setInstallSnapshotEnabled(properties, false);
      RaftServerConfigKeys.Log.setSegmentSizeMax(properties, SizeInBytes.valueOf("8MB"));

      final OriginTrackingStateMachine sm = new OriginTrackingStateMachine("node" + i);
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

    waitForLeader();
  }

  @AfterEach
  void tearDown() {
    for (final RaftServer server : servers)
      try {
        server.close();
      } catch (final Exception ignored) {
      }
    try {
      deleteRecursive(tempDir);
    } catch (final Exception ignored) {
    }
  }

  /**
   * Sends an entry tagged with originPeerId="node0". Node0 should skip it,
   * nodes 1 and 2 should apply it.
   */
  @Test
  void originNodeSkipsWhileOthersApply() throws Exception {
    try (final RaftClient client = createClient()) {
      final RaftClientReply reply = client.io().send(
          Message.valueOf(ByteString.copyFrom(encodeEntry("node0", "tx-1"))));
      assertThat(reply.isSuccess()).isTrue();
    }

    waitForAllStateMachines(1);

    // node0 is the origin - should have skipped
    assertThat(stateMachines.get(0).getAppliedEntries()).isEmpty();
    assertThat(stateMachines.get(0).getSkippedEntries()).containsExactly("tx-1");

    // node1 and node2 are not the origin - should have applied
    assertThat(stateMachines.get(1).getAppliedEntries()).containsExactly("tx-1");
    assertThat(stateMachines.get(1).getSkippedEntries()).isEmpty();
    assertThat(stateMachines.get(2).getAppliedEntries()).containsExactly("tx-1");
    assertThat(stateMachines.get(2).getSkippedEntries()).isEmpty();
  }

  /**
   * After a leadership change, entries from the old leader are still correctly
   * skipped on the old leader and applied on all others. This is the key TOCTOU
   * scenario: comparing against the immutable originPeerId in the log entry
   * (not live leadership state) ensures correctness.
   */
  @Test
  void originSkipSurvivesLeadershipChange() throws Exception {
    // Step 1: Submit entry from node0
    try (final RaftClient client = createClient()) {
      client.io().send(Message.valueOf(ByteString.copyFrom(encodeEntry("node0", "before-change"))));
    }
    waitForAllStateMachines(1);

    // Step 2: Submit entry from node1 (simulating node1 as the new origin after leadership change)
    try (final RaftClient client = createClient()) {
      client.io().send(Message.valueOf(ByteString.copyFrom(encodeEntry("node1", "after-change"))));
    }
    waitForAllStateMachines(2);

    // node0: skipped "before-change" (origin), applied "after-change" (not origin)
    assertThat(stateMachines.get(0).getSkippedEntries()).containsExactly("before-change");
    assertThat(stateMachines.get(0).getAppliedEntries()).containsExactly("after-change");

    // node1: applied "before-change" (not origin), skipped "after-change" (origin)
    assertThat(stateMachines.get(1).getAppliedEntries()).containsExactly("before-change");
    assertThat(stateMachines.get(1).getSkippedEntries()).containsExactly("after-change");

    // node2: applied both (never the origin)
    assertThat(stateMachines.get(2).getAppliedEntries()).containsExactlyInAnyOrder("before-change", "after-change");
    assertThat(stateMachines.get(2).getSkippedEntries()).isEmpty();
  }

  /**
   * Entries with an unknown originPeerId (not matching any node) should be applied on all nodes.
   * This covers the case where a node was removed from the cluster after submitting entries.
   */
  @Test
  void unknownOriginAppliedOnAllNodes() throws Exception {
    try (final RaftClient client = createClient()) {
      client.io().send(Message.valueOf(ByteString.copyFrom(encodeEntry("removed-node", "orphan-tx"))));
    }

    waitForAllStateMachines(1);

    for (int i = 0; i < NODE_COUNT; i++) {
      assertThat(stateMachines.get(i).getAppliedEntries())
          .as("node%d should apply entry from unknown origin", i)
          .containsExactly("orphan-tx");
      assertThat(stateMachines.get(i).getSkippedEntries()).isEmpty();
    }
  }

  // -- Helpers --

  /**
   * Encodes a test entry as "originPeerId\0label" for the state machine to parse.
   */
  private static byte[] encodeEntry(final String originPeerId, final String label) {
    return (originPeerId + "\0" + label).getBytes(StandardCharsets.UTF_8);
  }

  private void waitForLeader() throws Exception {
    final long deadline = System.currentTimeMillis() + 10_000;
    while (System.currentTimeMillis() < deadline) {
      for (final RaftServer server : servers)
        try {
          if (server.getDivision(group.getGroupId()).getInfo().isLeader())
            return;
        } catch (final Exception ignored) {
        }
      Thread.sleep(200);
    }
    throw new RuntimeException("No leader elected within 10 seconds");
  }

  private void waitForAllStateMachines(final int expectedTotal) throws InterruptedException {
    final long deadline = System.currentTimeMillis() + 10_000;
    while (System.currentTimeMillis() < deadline) {
      boolean allReady = true;
      for (final OriginTrackingStateMachine sm : stateMachines)
        if (sm.getTotalCount() < expectedTotal) {
          allReady = false;
          break;
        }
      if (allReady)
        return;
      Thread.sleep(100);
    }
    // Don't fail here - let the assertions in the test provide the diagnostic
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
   * Lightweight state machine that implements the same origin-skip logic as
   * ArcadeStateMachine but records decisions instead of applying WAL changes.
   * Each entry is "originPeerId\0label" - the state machine compares originPeerId
   * against its own localPeerId to decide skip vs apply.
   */
  static class OriginTrackingStateMachine extends BaseStateMachine {
    private final String       localPeerId;
    private final List<String> appliedEntries = new CopyOnWriteArrayList<>();
    private final List<String> skippedEntries = new CopyOnWriteArrayList<>();

    OriginTrackingStateMachine(final String localPeerId) {
      this.localPeerId = localPeerId;
    }

    @Override
    public CompletableFuture<Message> applyTransaction(final TransactionContext trx) {
      final var logEntry = trx.getLogEntry();
      final String payload = logEntry.getStateMachineLogEntry().getLogData().toStringUtf8();
      final int sep = payload.indexOf('\0');

      if (sep > 0) {
        final String originPeerId = payload.substring(0, sep);
        final String label = payload.substring(sep + 1);

        if (localPeerId.equals(originPeerId))
          skippedEntries.add(label);
        else
          appliedEntries.add(label);
      }

      updateLastAppliedTermIndex(logEntry.getTerm(), logEntry.getIndex());
      return CompletableFuture.completedFuture(Message.EMPTY);
    }

    List<String> getAppliedEntries() {
      return appliedEntries;
    }

    List<String> getSkippedEntries() {
      return skippedEntries;
    }

    int getTotalCount() {
      return appliedEntries.size() + skippedEntries.size();
    }
  }
}
