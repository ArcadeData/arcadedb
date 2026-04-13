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
import com.arcadedb.database.Binary;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ha.HAPlugin;
import com.arcadedb.server.http.HttpServer;
import io.undertow.server.handlers.PathHandler;

import java.util.Map;

/**
 * ServiceLoader entry point for the Ratis-based HA plugin. This class is the thin plugin
 * adapter that implements the {@link HAPlugin} interface and delegates all functionality to
 * the underlying {@link RaftHAServer}.
 * <p>
 * Separation of concerns:
 * <ul>
 *   <li>{@code RaftHAPlugin} - plugin lifecycle (configure, start, stop, registerAPI)</li>
 *   <li>{@link RaftHAServer} - Raft consensus, replication, membership, and cluster management</li>
 *   <li>{@link RaftPeerAddressResolver} - peer address parsing and HTTP address mapping</li>
 *   <li>{@link KubernetesAutoJoin} - Kubernetes StatefulSet auto-join on scale-up</li>
 *   <li>{@link SnapshotInstaller} - crash-safe snapshot download and installation</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class RaftHAPlugin implements HAPlugin {

  private RaftHAServer raftServer;
  private boolean      active;

  /** ServiceLoader requires a no-arg constructor. */
  public RaftHAPlugin() {
  }

  /** Constructor for programmatic creation (e.g., in tests). */
  public RaftHAPlugin(final ArcadeDBServer server, final ContextConfiguration configuration) {
    configure(server, configuration);
  }

  @Override
  public void configure(final ArcadeDBServer server, final ContextConfiguration configuration) {
    if (!configuration.getValueAsBoolean(GlobalConfiguration.HA_ENABLED))
      return;

    raftServer = new RaftHAServer(server, configuration);
    this.active = raftServer.isActive();

    // RaftHAServer.configure() called server.setHA(raftServer); override it so that
    // server.getHA() returns this plugin (the HAPlugin contract), not the internal raftServer.
    if (this.active)
      server.setHA(this);
  }

  @Override
  public boolean isActive() {
    return active;
  }

  @Override
  public PluginInstallationPriority getInstallationPriority() {
    return PluginInstallationPriority.AFTER_HTTP_ON;
  }

  @Override
  public void startService() {
    if (!active)
      return;
    raftServer.startService();
  }

  @Override
  public void stopService() {
    if (!active)
      return;
    raftServer.stopService();
  }

  @Override
  public void registerAPI(final HttpServer httpServer, final PathHandler routes) {
    routes.addPrefixPath("/api/v1/ha/snapshot", new SnapshotHttpHandler(httpServer));
    routes.addExactPath("/api/v1/cluster", new GetClusterHandler(httpServer, raftServer));
    routes.addExactPath("/api/v1/cluster/peer", new PostAddPeerHandler(httpServer, raftServer));
    routes.addPrefixPath("/api/v1/cluster/peer/", new DeletePeerHandler(httpServer, raftServer));
    routes.addExactPath("/api/v1/cluster/leader", new PostTransferLeaderHandler(httpServer, raftServer));
    routes.addExactPath("/api/v1/cluster/stepdown", new PostStepDownHandler(httpServer, raftServer));
    routes.addExactPath("/api/v1/cluster/leave", new PostLeaveHandler(httpServer, raftServer));
    routes.addPrefixPath("/api/v1/cluster/verify/", new PostVerifyDatabaseHandler(httpServer, raftServer));
  }

  @Override
  public void recoverBeforeDatabaseLoad(final java.nio.file.Path databaseDirectory) {
    SnapshotInstaller.recoverPendingSnapshotSwaps(databaseDirectory);
  }

  // -- HAPlugin interface delegation --

  @Override
  public boolean isLeader() {
    return raftServer != null && raftServer.isLeader();
  }

  @Override
  public String getClusterToken() {
    return raftServer.getClusterToken();
  }

  @Override
  public long getCommitIndex() {
    return raftServer.getCommitIndex();
  }

  @Override
  public String getLeaderHTTPAddress() {
    return raftServer.getLeaderHTTPAddress();
  }

  @Override
  public String getLeaderName() {
    return raftServer.getLeaderName();
  }

  @Override
  public String getElectionStatus() {
    return raftServer.getElectionStatus();
  }

  @Override
  public String getClusterName() {
    return raftServer.getClusterName();
  }

  @Override
  public int getConfiguredServers() {
    return raftServer.getConfiguredServers();
  }

  @Override
  public String getReplicaAddresses() {
    return raftServer.getReplicaAddresses();
  }

  @Override
  public String getServerName() {
    return raftServer.getServerName();
  }

  @Override
  public long getLastAppliedIndex() {
    return raftServer.getLastAppliedIndex();
  }

  @Override
  public JSONObject exportClusterStatus() {
    return raftServer.exportClusterStatus();
  }

  @Override
  public void replicateCreateDatabase(final String databaseName) {
    raftServer.replicateCreateDatabase(databaseName);
  }

  @Override
  public void replicateDropDatabase(final String databaseName) {
    raftServer.replicateDropDatabase(databaseName);
  }

  @Override
  public void replicateTransaction(final String databaseName, final Map<Integer, Integer> bucketRecordDelta,
      final Binary walBuffer, final String schemaJson, final Map<Integer, String> filesToAdd,
      final Map<Integer, String> filesToRemove) {
    raftServer.replicateTransaction(databaseName, bucketRecordDelta, walBuffer, schemaJson, filesToAdd, filesToRemove);
  }

  @Override
  public boolean isLeaderReady() {
    return raftServer.isLeaderReady();
  }

  @Override
  public void waitForLeaderReady() {
    raftServer.waitForLeaderReady();
  }

  @Override
  public void waitForAppliedIndex(final long targetIndex) {
    raftServer.waitForAppliedIndex(targetIndex);
  }

  @Override
  public void ensureLinearizableRead() {
    raftServer.ensureLinearizableRead();
  }

  @Override
  public void waitForLocalApply() {
    raftServer.waitForLocalApply();
  }

  @Override
  public void stepDown() {
    raftServer.stepDown();
  }

  @Override
  public void leaveCluster() {
    raftServer.leaveCluster();
  }

  @Override
  public void addPeer(final String peerId, final String raftAddress, final String httpAddress) {
    raftServer.addPeer(peerId, raftAddress, httpAddress);
  }

  @Override
  public void removePeer(final String peerId) {
    raftServer.removePeer(peerId);
  }

  @Override
  public void transferLeadership(final String targetPeerId, final long timeoutMs) {
    raftServer.transferLeadership(targetPeerId, timeoutMs);
  }

  // -- Accessors for internal use and tests --

  /** Returns the underlying Raft server for direct Raft-level access in tests and handlers. */
  public RaftHAServer getRaftServer() {
    return raftServer;
  }

  /** Returns the configured Quorum mode. */
  public Quorum getQuorum() {
    return raftServer.getQuorum();
  }
}
