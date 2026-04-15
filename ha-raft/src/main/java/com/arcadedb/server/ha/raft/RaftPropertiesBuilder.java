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
import com.arcadedb.exception.ConfigurationException;
import com.arcadedb.log.LogManager;
import org.apache.ratis.client.RaftClientConfigKeys;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.util.SizeInBytes;
import org.apache.ratis.util.TimeDuration;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

/**
 * Translates ArcadeDB HA configuration into Ratis {@link RaftProperties}.
 * Pure function: reads configuration, produces properties, no side effects beyond
 * creating the storage directory on disk.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class RaftPropertiesBuilder {

  // Server-side RPC request timeout: how long the leader waits for a follower AppendEntries response.
  private static final int RPC_REQUEST_TIMEOUT_SECS = 10;
  // Slowness/close thresholds: how long before a follower is marked slow or its connection is closed.
  // Set high (5 min) to survive network partitions without prematurely evicting followers.
  private static final int FOLLOWER_SLOWNESS_TIMEOUT_SECS = 300;
  private static final int FOLLOWER_CLOSE_THRESHOLD_SECS  = 300;

  // Maximum log entries per AppendEntries RPC batch. Balances throughput vs. memory per batch.
  private static final int APPEND_ENTRIES_MAX_ELEMENTS = 256;

  // Leader lease ratio: fraction of the election timeout during which the leader considers
  // its lease valid for serving linearizable reads without a round-trip. 0.9 means the lease
  // expires at 90% of the election timeout, leaving a 10% safety margin.
  private static final double LEADER_LEASE_TIMEOUT_RATIO = 0.9;

  private RaftPropertiesBuilder() {
  }

  /**
   * Builds a {@link RaftProperties} object from the given ArcadeDB configuration.
   *
   * @param configuration ArcadeDB HA configuration
   * @param serverRootPath root path for storage directories
   * @param localPeerId local peer ID (used for storage directory naming)
   * @param quorumTimeout quorum timeout in milliseconds (used for client RPC timeout)
   */
  static RaftProperties build(final ContextConfiguration configuration, final String serverRootPath,
                              final String localPeerId, final long quorumTimeout) {
    final RaftProperties properties = new RaftProperties();

    // Storage directory
    final Path storagePath = Path.of(serverRootPath, "ratis-storage", localPeerId);
    try {
      Files.createDirectories(storagePath);
    } catch (final IOException e) {
      throw new ConfigurationException("Cannot create Ratis storage directory: " + storagePath, e);
    }
    RaftServerConfigKeys.setStorageDir(properties, Collections.singletonList(storagePath.toFile()));

    // gRPC transport
    final int port = RaftPeerAddressResolver.parseFirstPort(
        configuration.getValueAsString(GlobalConfiguration.HA_REPLICATION_INCOMING_PORTS));
    GrpcConfigKeys.Server.setPort(properties, port);

    // RPC factory
    properties.set("raft.server.rpc.type", "GRPC");

    // Election timeouts (configurable for WAN clusters)
    final int electionMin = configuration.getValueAsInteger(GlobalConfiguration.HA_ELECTION_TIMEOUT_MIN);
    final int electionMax = configuration.getValueAsInteger(GlobalConfiguration.HA_ELECTION_TIMEOUT_MAX);
    RaftServerConfigKeys.Rpc.setTimeoutMin(properties, TimeDuration.valueOf(electionMin, TimeUnit.MILLISECONDS));
    RaftServerConfigKeys.Rpc.setTimeoutMax(properties, TimeDuration.valueOf(electionMax, TimeUnit.MILLISECONDS));

    // Snapshot: chunk mode (Ratis sends the marker file, ArcadeDB downloads the actual database via HTTP).
    // The default LogAppender only supports chunk-based transfer, not notification mode.
    // When the follower receives the marker, reinitialize() detects the index gap and triggers the HTTP download.
    RaftServerConfigKeys.Log.Appender.setInstallSnapshotEnabled(properties, true);
    final long snapshotThreshold = configuration.getValueAsLong(GlobalConfiguration.HA_SNAPSHOT_THRESHOLD);
    RaftServerConfigKeys.Snapshot.setAutoTriggerEnabled(properties, true);
    RaftServerConfigKeys.Snapshot.setAutoTriggerThreshold(properties, snapshotThreshold);
    // Allow frequent snapshot creation (default 1024 gap prevents snapshots in short-lived tests)
    RaftServerConfigKeys.Snapshot.setCreationGap(properties, 0);

    // Log segment size
    final String logSegmentSize = configuration.getValueAsString(GlobalConfiguration.HA_LOG_SEGMENT_SIZE);
    RaftServerConfigKeys.Log.setSegmentSizeMax(properties, SizeInBytes.valueOf(logSegmentSize));

    // Log purging: controls how aggressively old log segments are deleted after snapshots
    final int purgeGap = configuration.getValueAsInteger(GlobalConfiguration.HA_LOG_PURGE_GAP);
    RaftServerConfigKeys.Log.setPurgeGap(properties, purgeGap);
    final boolean purgeUptoSnapshot = configuration.getValueAsBoolean(GlobalConfiguration.HA_LOG_PURGE_UPTO_SNAPSHOT);
    RaftServerConfigKeys.Log.setPurgeUptoSnapshotIndex(properties, purgeUptoSnapshot);

    // AppendEntries batching: allow multiple log entries in a single gRPC call to followers.
    // Combined with the group committer, this allows many transactions to be replicated in one round-trip.
    final String appendBufferSize = configuration.getValueAsString(GlobalConfiguration.HA_APPEND_BUFFER_SIZE);
    RaftServerConfigKeys.Log.Appender.setBufferByteLimit(properties, SizeInBytes.valueOf(appendBufferSize));

    // Write buffer (must be >= appender buffer byte-limit + 8)
    final long appendBytes = SizeInBytes.valueOf(appendBufferSize).getSize();
    final long minWriteBuffer = appendBytes + 8;
    final SizeInBytes writeBuffer =
        SizeInBytes.valueOf(configuration.getValueAsString(GlobalConfiguration.HA_WRITE_BUFFER_SIZE));
    if (writeBuffer.getSize() < minWriteBuffer)
      throw new ConfigurationException(
          "arcadedb.ha.writeBufferSize (" + writeBuffer + ") must be >= arcadedb.ha.appendBufferSize + 8 ("
              + minWriteBuffer + " bytes). Increase writeBufferSize or decrease appendBufferSize");
    RaftServerConfigKeys.Log.setWriteBufferSize(properties, writeBuffer);
    RaftServerConfigKeys.Log.Appender.setBufferElementLimit(properties, APPEND_ENTRIES_MAX_ELEMENTS);

    // Leader lease: enables consistent reads from the leader without a round-trip to followers.
    // The leader can serve reads as long as its lease hasn't expired (based on heartbeat responses).
    RaftServerConfigKeys.Read.setLeaderLeaseEnabled(properties, true);
    RaftServerConfigKeys.Read.setLeaderLeaseTimeoutRatio(properties, LEADER_LEASE_TIMEOUT_RATIO);
    RaftServerConfigKeys.Read.setOption(properties, RaftServerConfigKeys.Read.Option.LINEARIZABLE);

    // Note: Ratis uses MAJORITY consensus by default.
    // For ALL quorum mode, we use the Watch API after each write to wait for ALL replicas.
    // See RaftTransactionBroker.sendToRaft() for the ALL quorum implementation.

    RaftServerConfigKeys.Rpc.setRequestTimeout(properties,
        TimeDuration.valueOf(RPC_REQUEST_TIMEOUT_SECS, TimeUnit.SECONDS));
    RaftServerConfigKeys.Rpc.setSlownessTimeout(properties,
        TimeDuration.valueOf(FOLLOWER_SLOWNESS_TIMEOUT_SECS, TimeUnit.SECONDS));
    RaftServerConfigKeys.setCloseThreshold(properties,
        TimeDuration.valueOf(FOLLOWER_CLOSE_THRESHOLD_SECS, TimeUnit.SECONDS));

    // gRPC flow control window: larger window helps with catch-up replication after partitions
    final String flowControlWindow = configuration.getValueAsString(GlobalConfiguration.HA_GRPC_FLOW_CONTROL_WINDOW);
    GrpcConfigKeys.setFlowControlWindow(properties, SizeInBytes.valueOf(flowControlWindow));

    // Client request timeout: bounds how long the Ratis client waits for a single RPC.
    // Without this, the client retries indefinitely when the majority is unreachable.
    RaftClientConfigKeys.Rpc.setRequestTimeout(properties, TimeDuration.valueOf(quorumTimeout, TimeUnit.MILLISECONDS));

    return properties;
  }
}
