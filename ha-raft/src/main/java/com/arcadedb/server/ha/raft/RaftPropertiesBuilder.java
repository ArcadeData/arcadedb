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
import com.arcadedb.exception.ConfigurationException;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.util.SizeInBytes;
import org.apache.ratis.util.TimeDuration;

import java.util.concurrent.TimeUnit;

/**
 * Utility class that constructs a {@link RaftProperties} instance from an ArcadeDB
 * {@link ContextConfiguration}. Extracted so both the normal start path and the
 * health-monitor recovery path can share the same configuration logic.
 */
class RaftPropertiesBuilder {

  private RaftPropertiesBuilder() {
  }

  static RaftProperties build(final ContextConfiguration configuration) {
    final RaftProperties properties = new RaftProperties();

    // Use the configured Raft port for the local gRPC bind address.
    // Note: the peer address in the server list may differ from the bind port when traffic
    // is routed through a proxy (e.g., Toxiproxy in e2e tests). The peer address is what
    // remote nodes use to connect; the bind port is what this node actually listens on.
    final int localRaftPort = configuration.getValueAsInteger(GlobalConfiguration.HA_RAFT_PORT);
    GrpcConfigKeys.Server.setPort(properties, localRaftPort);

    // Configure Raft RPC timeouts for cluster stability
    final int electionMin = configuration.getValueAsInteger(GlobalConfiguration.HA_ELECTION_TIMEOUT_MIN);
    final int electionMax = configuration.getValueAsInteger(GlobalConfiguration.HA_ELECTION_TIMEOUT_MAX);
    RaftServerConfigKeys.Rpc.setTimeoutMin(properties, TimeDuration.valueOf(electionMin, TimeUnit.MILLISECONDS));
    RaftServerConfigKeys.Rpc.setTimeoutMax(properties, TimeDuration.valueOf(electionMax, TimeUnit.MILLISECONDS));
    RaftServerConfigKeys.Rpc.setRequestTimeout(properties, TimeDuration.valueOf(10, TimeUnit.SECONDS));

    final long flowControlWindow = configuration.getValueAsLong(GlobalConfiguration.HA_GRPC_FLOW_CONTROL_WINDOW);
    GrpcConfigKeys.setFlowControlWindow(properties, SizeInBytes.valueOf(flowControlWindow));

    // Staging timeout: when adding a new peer, the leader syncs it before committing the
    // config change. This bounds how long the leader waits for the new peer to catch up.
    RaftServerConfigKeys.setStagingTimeout(properties, TimeDuration.valueOf(30, TimeUnit.SECONDS));

    final long snapshotThreshold = configuration.getValueAsLong(GlobalConfiguration.HA_SNAPSHOT_THRESHOLD);
    RaftServerConfigKeys.Snapshot.setAutoTriggerThreshold(properties, snapshotThreshold);

    // Log purging: controls how aggressively old log segments are deleted after snapshots
    final int purgeGap = configuration.getValueAsInteger(GlobalConfiguration.HA_LOG_PURGE_GAP);
    RaftServerConfigKeys.Log.setPurgeGap(properties, purgeGap);
    final boolean purgeUptoSnapshot = configuration.getValueAsBoolean(GlobalConfiguration.HA_LOG_PURGE_UPTO_SNAPSHOT);
    RaftServerConfigKeys.Log.setPurgeUptoSnapshotIndex(properties, purgeUptoSnapshot);

    // Disable Ratis built-in snapshot transfer; use notification mode
    // so ArcadeDB controls the snapshot transfer via HTTP
    RaftServerConfigKeys.Log.Appender.setInstallSnapshotEnabled(properties, false);
    RaftServerConfigKeys.Snapshot.setAutoTriggerEnabled(properties, true);

    // AppendEntries batching: allow multiple entries per gRPC call to followers
    final String appendBufferSize = configuration.getValueAsString(GlobalConfiguration.HA_APPEND_BUFFER_SIZE);
    final SizeInBytes appendBuffer = SizeInBytes.valueOf(appendBufferSize);
    RaftServerConfigKeys.Log.Appender.setBufferByteLimit(properties, appendBuffer);
    RaftServerConfigKeys.Log.Appender.setBufferElementLimit(properties, 256);

    // Log segment size
    final String logSegmentSize = configuration.getValueAsString(GlobalConfiguration.HA_LOG_SEGMENT_SIZE);
    RaftServerConfigKeys.Log.setSegmentSizeMax(properties, SizeInBytes.valueOf(logSegmentSize));

    // Write buffer: must be >= appendBufferSize + 8 bytes (Ratis internal framing)
    final SizeInBytes writeBuffer = SizeInBytes.valueOf(
        configuration.getValueAsString(GlobalConfiguration.HA_WRITE_BUFFER_SIZE));
    final long minWriteBuffer = appendBuffer.getSize() + 8;
    if (writeBuffer.getSize() < minWriteBuffer)
      throw new ConfigurationException(
          "arcadedb.ha.writeBufferSize (" + writeBuffer + ") must be >= arcadedb.ha.appendBufferSize + 8 ("
              + minWriteBuffer + " bytes). Increase writeBufferSize or decrease appendBufferSize");
    RaftServerConfigKeys.Log.setWriteBufferSize(properties, writeBuffer);

    // Leader lease: consistent reads without round-trip
    RaftServerConfigKeys.Read.setLeaderLeaseEnabled(properties, true);
    RaftServerConfigKeys.Read.setLeaderLeaseTimeoutRatio(properties, 0.9);
    RaftServerConfigKeys.Read.setOption(properties, RaftServerConfigKeys.Read.Option.LINEARIZABLE);

    return properties;
  }
}
