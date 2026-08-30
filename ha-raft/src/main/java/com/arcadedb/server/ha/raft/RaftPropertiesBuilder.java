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
import com.arcadedb.log.LogManager;
import com.arcadedb.server.ha.raft.ratis.FixedGrpcRpcType;
import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.grpc.GrpcTlsConfig;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.util.SizeInBytes;
import org.apache.ratis.util.TimeDuration;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

/**
 * Utility class that constructs a {@link RaftProperties} instance from an ArcadeDB
 * {@link ContextConfiguration}. Extracted so both the normal start path and the
 * health-monitor recovery path can share the same configuration logic.
 */
class RaftPropertiesBuilder {

  private RaftPropertiesBuilder() {
  }

  /**
   * Maximum size, in bytes, of a SINGLE replicated Raft log entry.
   * <p>
   * Two independent Ratis limits bound one entry and the SMALLER of the two wins:
   * <ul>
   *   <li>{@code raft.grpc.message.size.max} ({@code arcadedb.ha.grpcMessageSizeMax}) - the gRPC
   *       transport frame cap. Exceeding it leaves the client {@code SlidingWindow} CLOSED.</li>
   *   <li>{@code raft.server.log.appender.buffer.byte-limit} ({@code arcadedb.ha.appendBufferSize}) -
   *       also enforced per-entry by {@code RaftLogBase.append}, which throws a
   *       {@code StateMachineException} for an entry above it. That exception has
   *       {@code leaderShouldStepDown() == true}, so {@code RaftServerImpl.appendTransaction} makes
   *       the LEADER STEP DOWN. Because the caller then retries the same oversized entry against the
   *       newly elected leader, the cluster enters an unbounded election-churn loop and the write
   *       never lands (issue #4743): terms advance every few minutes, every node answers
   *       {@code NotLeaderException}, and phase-2 applies stay unconfirmed forever. The default
   *       (4MB) is an order of magnitude below the gRPC cap (128MB), so in practice THIS is the
   *       binding limit.</li>
   * </ul>
   * Every producer of a Raft entry must keep its payload under this value: the group committer
   * pre-flights it, and index-compaction replication chunks its synthetic WAL against it.
   */
  static long maxReplicatedEntrySize(final ContextConfiguration configuration) {
    return GlobalConfiguration.maxReplicatedRaftEntrySize(configuration);
  }

  /**
   * Builds the TLS configuration for the Raft gRPC transport, or returns {@code null} when
   * {@link GlobalConfiguration#HA_TLS_ENABLED} is false - in which case the transport stays plaintext,
   * exactly as before this setting existed.
   * <p>
   * The three PEM paths are validated here rather than left to Netty, so a typo or an unmounted secret
   * volume fails the node at startup with the offending setting named, instead of surfacing later as an
   * opaque handshake failure on every peer connection.
   *
   * @throws ConfigurationException if TLS is enabled and any of the cert/key/trust paths is unset, missing,
   *                                not a regular file, or unreadable
   */
  static GrpcTlsConfig buildTlsConfig(final ContextConfiguration configuration) {
    if (!configuration.getValueAsBoolean(GlobalConfiguration.HA_TLS_ENABLED))
      return null;

    final File certChain = requireReadableFile(configuration, GlobalConfiguration.HA_TLS_CERT_CHAIN_FILE);
    final File privateKey = requireReadableFile(configuration, GlobalConfiguration.HA_TLS_PRIVATE_KEY_FILE);
    final File trustCerts = requireReadableFile(configuration, GlobalConfiguration.HA_TLS_TRUST_CERT_COLLECTION_FILE);
    final boolean mutualAuth = configuration.getValueAsBoolean(GlobalConfiguration.HA_TLS_MUTUAL_AUTH);

    warnIfPrivateKeyIsReadableByOthers(privateKey);

    if (!mutualAuth)
      LogManager.instance().log(RaftPropertiesBuilder.class, Level.WARNING,
          "Raft gRPC TLS is enabled with %s=false: the transport is encrypted but the dialling peer is NOT "
              + "authenticated, so any client trusting the cluster CA can still open a Raft stream",
          GlobalConfiguration.HA_TLS_MUTUAL_AUTH.getKey());

    return new GrpcTlsConfig(privateKey, certChain, trustCerts, mutualAuth);
  }

  /**
   * Installs the TLS configuration built by {@link #buildTlsConfig(ContextConfiguration)} on the
   * {@link Parameters} handed to both {@code RaftServer.Builder} and {@code RaftClient.Builder}.
   * <p>
   * A single {@code GrpcConfigKeys.TLS} entry is enough: Ratis's {@code GrpcFactory} reads it as the default
   * for all three gRPC endpoints (admin, client and server-to-server), so AppendEntries, InstallSnapshot and
   * RequestVote traffic are all covered by this one call. No-op when TLS is disabled, which leaves
   * {@code Parameters} exactly as Ratis's plaintext default expects it.
   * <p>
   * <b>That defaulting is a Ratis implementation detail, not a documented contract</b>, verified against
   * <b>Ratis 3.3.0</b>: {@code GrpcFactory(Parameters)} reads {@code TLS.conf} plus the three per-endpoint
   * keys, and its {@code SslContexts} constructor builds the {@code TLS.conf} context first and passes it as
   * the fallback for admin, client and server. If a Ratis upgrade changes that, this one entry stops reaching
   * the endpoints that do not set their own, and the symptom is the bug this method exists to prevent - a
   * client dialling the Raft port in plaintext against a TLS server. On any Ratis version bump, re-read
   * {@code GrpcFactory} and run {@code Issue3890RaftMtlsIT}, which is the test that would catch it.
   *
   * @return the TLS configuration that was installed, or {@code null} when TLS is disabled
   */
  static GrpcTlsConfig applyTls(final ContextConfiguration configuration, final Parameters parameters) {
    final GrpcTlsConfig tlsConfig = buildTlsConfig(configuration);
    if (tlsConfig == null)
      return null;
    GrpcConfigKeys.TLS.setConf(parameters, tlsConfig);
    return tlsConfig;
  }

  /**
   * Warns when the private key is readable by the group or by everyone. Deliberately a warning and not a
   * refusal: file ownership is the deployment's business, a container image or a mounted secret may legitimately
   * arrive with permissions this node cannot change, and refusing to start would be a worse outcome than a
   * noisy log. Silently accepting it would be worse still - a world-readable Raft key hands anyone on the host
   * a cluster identity, which is precisely what this setting exists to prevent.
   * <p>
   * No-op on a file system with no POSIX view (Windows), where the question has no answer in these terms.
   */
  private static void warnIfPrivateKeyIsReadableByOthers(final File privateKey) {
    final Set<PosixFilePermission> permissions;
    try {
      permissions = Files.getPosixFilePermissions(privateKey.toPath());
    } catch (final Exception e) {
      // UnsupportedOperationException on a non-POSIX file system, IOException on a file that vanished
      // between the readability check and here. Neither is worth failing a startup over.
      return;
    }

    if (permissions.contains(PosixFilePermission.GROUP_READ) || permissions.contains(PosixFilePermission.OTHERS_READ))
      LogManager.instance().log(RaftPropertiesBuilder.class, Level.WARNING,
          "The Raft gRPC private key %s (%s) is readable beyond its owner: any local account that can read it "
              + "can present this node's identity to the cluster. Restrict it to owner-only (chmod 600)",
          GlobalConfiguration.HA_TLS_PRIVATE_KEY_FILE.getKey(), privateKey.getAbsolutePath());
  }

  private static File requireReadableFile(final ContextConfiguration configuration,
      final GlobalConfiguration setting) {
    final String path = configuration.getValueAsString(setting);
    if (path == null || path.isBlank())
      throw new ConfigurationException(
          setting.getKey() + " must be set when " + GlobalConfiguration.HA_TLS_ENABLED.getKey() + " is true");

    final File file = new File(path);
    if (!file.isFile() || !file.canRead())
      throw new ConfigurationException(
          setting.getKey() + " (" + path + ") is not a readable file: Raft gRPC TLS cannot be initialized");
    return file;
  }

  static RaftProperties build(final ContextConfiguration configuration) {
    final RaftProperties properties = new RaftProperties();

    // Replace Ratis's stock GrpcLogAppender with our subclass that fixes RATIS-2523
    // (heartbeat-only INCONSISTENCY loop after a follower restarts with empty Raft storage on
    // an idle cluster). Wired via RpcType.valueOf which accepts a fully-qualified class name
    // and reflectively instantiates it; FixedGrpcRpcType returns a FixedGrpcFactory that
    // returns a FixedGrpcLogAppender subclass. Drop these three classes and revert this line
    // to SupportedRpcType.GRPC once RATIS-2523 ships in an Apache Ratis release.
    properties.set(RaftConfigKeys.Rpc.TYPE_KEY, FixedGrpcRpcType.class.getName());

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

    // Override Ratis's 64MB stock per-message cap. A single replicated transaction (e.g. a 50k-vertex
    // GraphBatch with all its index updates) can exceed 64MB; with the stock cap that one transaction
    // is rejected by the gRPC client and the SlidingWindow stays CLOSED indefinitely. The single key
    // applies symmetrically to inbound and outbound gRPC messages.
    final long grpcMessageSizeMax = configuration.getValueAsLong(GlobalConfiguration.HA_GRPC_MESSAGE_SIZE_MAX);
    GrpcConfigKeys.setMessageSizeMax(properties, SizeInBytes.valueOf(grpcMessageSizeMax));

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

    // AppendEntries batching: allow multiple entries per gRPC call to followers.
    // Element limit bounds the per-batch in-memory footprint on the follower during catch-up,
    // where many batches may queue before the state machine can apply them (issue #4752).
    final String appendBufferSize = configuration.getValueAsString(GlobalConfiguration.HA_APPEND_BUFFER_SIZE);
    final SizeInBytes appendBuffer = SizeInBytes.valueOf(appendBufferSize);
    RaftServerConfigKeys.Log.Appender.setBufferByteLimit(properties, appendBuffer);
    final int appendElementLimit = configuration.getValueAsInteger(GlobalConfiguration.HA_APPEND_ELEMENT_LIMIT);
    if (appendElementLimit < 1)
      throw new ConfigurationException(
          "arcadedb.ha.appendElementLimit (" + appendElementLimit + ") must be >= 1");
    RaftServerConfigKeys.Log.Appender.setBufferElementLimit(properties, appendElementLimit);

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

    // #5933: the codec's decode ceiling is a wire-format constant, identical on every node, so the configured
    // entry cap must never sit above it. An entry between the two would pass the submit gate, reach a quorum
    // and COMMIT - and only then fail to decode on every node applying it. A committed entry cannot be taken
    // back, so the cluster halts on that log index and replays into the same failure on every restart. Fail at
    // startup, where the operator can act on it.
    final long maxEntrySize = maxReplicatedEntrySize(configuration);
    if (maxEntrySize > RaftLogEntryCodec.MAX_DECODED_ENTRY_BYTES)
      throw new ConfigurationException(
          "The maximum replicated Raft entry size (" + maxEntrySize + " bytes, the smaller of "
              + "arcadedb.ha.appendBufferSize and arcadedb.ha.grpcMessageSizeMax) is above the "
              + RaftLogEntryCodec.MAX_DECODED_ENTRY_BYTES + " bytes a Raft log entry can be decoded from. "
              + "Lower arcadedb.ha.appendBufferSize (and arcadedb.ha.grpcMessageSizeMax with it, if that is the "
              + "smaller of the two) to at most that value");

    // #4743: the gRPC frame cap is NOT the effective per-entry ceiling - the appender byte limit is
    // enforced per-entry too, and an entry above it makes the leader step down (see
    // maxReplicatedEntrySize). Surface the mismatch once at startup: without it operators tune
    // grpcMessageSizeMax (the number our own error messages used to name) and nothing changes.
    if (grpcMessageSizeMax > appendBuffer.getSize())
      LogManager.instance().log(RaftPropertiesBuilder.class, Level.INFO,
          """
          Maximum replicated Raft entry size is %d bytes (arcadedb.ha.appendBufferSize), NOT the %d bytes \
          of arcadedb.ha.grpcMessageSizeMax: Ratis rejects a single log entry above the appender byte limit \
          and the leader steps down. Raise arcadedb.ha.appendBufferSize (and arcadedb.ha.writeBufferSize, \
          which must stay >= appendBufferSize + 8 bytes) if transactions or records bigger than that must \
          replicate.""",
          appendBuffer.getSize(), grpcMessageSizeMax);

    // Leader lease: consistent reads without round-trip
    RaftServerConfigKeys.Read.setLeaderLeaseEnabled(properties, true);
    RaftServerConfigKeys.Read.setLeaderLeaseTimeoutRatio(properties, 0.9);
    RaftServerConfigKeys.Read.setOption(properties, RaftServerConfigKeys.Read.Option.LINEARIZABLE);

    return properties;
  }
}
