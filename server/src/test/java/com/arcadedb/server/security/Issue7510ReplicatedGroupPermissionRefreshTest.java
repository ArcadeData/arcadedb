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
package com.arcadedb.server.security;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.engine.FileManager;
import com.arcadedb.schema.Schema;
import com.arcadedb.security.SecurityDatabaseUser.DATABASE_ACCESS;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.HAServerPlugin;
import com.arcadedb.server.ServerControlPlane;
import com.arcadedb.server.ServerDatabase;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Regression test for issue #7510.
 * <p>
 * #7373 made a group change replicate, so {@code server-groups.json} converges on every node at once. What did
 * not converge is the state <i>derived</i> from it: each open database caches the permissions of the principals
 * connected to it, and only {@link ServerSecurity#updateSchema} re-derives them. The node that served the
 * request refreshes synchronously in {@code ServerControlPlane}; a peer had nothing at all, so it kept
 * enforcing the PREVIOUS document until {@code SecurityGroupFileRepository}'s file watcher happened to tick -
 * up to {@code arcadedb.server.security.reloadEvery} later. For a permission the operator had just narrowed
 * that is a window in which the peer grants access the cluster has already revoked, while
 * {@code GET /server/groups} on that same peer already shows the new definition.
 * <p>
 * Every test here runs with {@code reloadEvery} set to an hour, so the watcher cannot be what makes them pass:
 * convergence inside a few seconds can only come from the refresh the apply now schedules itself.
 * <p>
 * {@link #CONVERGENCE} is that separation and nothing finer - a tripwire between "the apply refreshed" and "we
 * are waiting for the hourly tick", with three orders of magnitude of headroom. It is not a latency budget, and
 * the refresh takes milliseconds in practice: widening it can only make a run greener, so a slow or stalled CI
 * box is not a reason to loosen it and a passing run is not evidence about how fast the refresh is.
 */
class Issue7510ReplicatedGroupPermissionRefreshTest {

  private static final String   CONFIG_PATH  = "target/test-security-7510";
  private static final String   DATABASE     = "graph";
  private static final String   GROUP        = "editors";
  /** Long enough that the group file's watcher cannot be the mechanism under test. */
  private static final int      NEVER_RELOAD = 60 * 60 * 1000;
  /** Separates "refreshed by the apply" from "waiting for {@link #NEVER_RELOAD}"; see the class javadoc. */
  private static final Duration CONVERGENCE  = Duration.ofSeconds(20);

  private FixtureServer  server;
  private ServerSecurity security;
  private ServerDatabase database;

  @BeforeEach
  void setUp() {
    GlobalConfiguration.SERVER_SECURITY_SALT_ITERATIONS.setValue(1000);
    final File dir = new File(CONFIG_PATH);
    if (dir.exists())
      FileUtils.deleteRecursively(dir);
    assertThat(dir.mkdirs()).isTrue();

    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.SERVER_ROOT_PATH, "./target");
    configuration.setValue(GlobalConfiguration.SERVER_SECURITY_RELOAD_EVERY, NEVER_RELOAD);

    server = new FixtureServer(configuration);
    security = new ServerSecurity(server, configuration, CONFIG_PATH);
    server.setSecurity(security);

    database = mockDatabase(DATABASE);
    server.register(database);
  }

  @AfterEach
  void tearDown() {
    if (security != null)
      security.stopService();
    GlobalConfiguration.SERVER_SECURITY_SALT_ITERATIONS.reset();
    FileUtils.deleteRecursively(new File(CONFIG_PATH));
  }

  // -------------------------------------------------------------------------------------------
  // The peer's half: what a SECURITY_GROUPS_ENTRY does to a principal already connected there
  // -------------------------------------------------------------------------------------------

  /** The direction that matters: a narrowed permission must stop granting without waiting for the tick. */
  @Test
  void aReplicatedRevocationReachesTheCachedPrincipalWithoutTheReloadTick() {
    security.applyReplicatedGroups(documentGranting(new JSONArray().put("updateSchema")));
    final ServerSecurityDatabaseUser cached = connectedPrincipal();
    assertThat(cached.requestAccessOnDatabase(DATABASE_ACCESS.UPDATE_SCHEMA))
        .as("the grant is in force before the revocation").isTrue();

    security.applyReplicatedGroups(documentGranting(new JSONArray()));

    await().atMost(CONVERGENCE).untilAsserted(() -> assertThat(
        cached.requestAccessOnDatabase(DATABASE_ACCESS.UPDATE_SCHEMA))
        .as("the peer must stop granting the revoked permission without waiting for reloadEvery")
        .isFalse());

    // The same cached instance, so this is a refresh and not the cache being rebuilt behind the test's back.
    assertThat(security.getUser("alice").getDatabaseUser(database)).isSameAs(cached);
  }

  /** The other direction, so the test cannot pass by denying everything. */
  @Test
  void aReplicatedGrantReachesTheCachedPrincipalWithoutTheReloadTick() {
    security.applyReplicatedGroups(documentGranting(new JSONArray()));
    final ServerSecurityDatabaseUser cached = connectedPrincipal();
    assertThat(cached.requestAccessOnDatabase(DATABASE_ACCESS.UPDATE_SCHEMA)).isFalse();

    security.applyReplicatedGroups(documentGranting(new JSONArray().put("updateSchema")));

    await().atMost(CONVERGENCE).untilAsserted(() -> assertThat(
        cached.requestAccessOnDatabase(DATABASE_ACCESS.UPDATE_SCHEMA)).isTrue());
  }

  /**
   * A deletion travels as the same entry type - {@code deleteGroupClusterWide} replicates the whole document
   * with the group removed - so the peer must lose the permissions the group carried, not merely the entry in
   * {@code GET /server/groups}.
   */
  @Test
  void aReplicatedGroupDeletionReachesTheCachedPrincipalWithoutTheReloadTick() {
    security.applyReplicatedGroups(documentGranting(new JSONArray().put("updateSchema")));
    final ServerSecurityDatabaseUser cached = connectedPrincipal();
    assertThat(cached.requestAccessOnDatabase(DATABASE_ACCESS.UPDATE_SCHEMA)).isTrue();

    // The document deleteGroupClusterWide() submits: the whole file, without that group.
    security.applyReplicatedGroups(new JSONObject()
        .put("version", ServerSecurity.LATEST_VERSION)
        .put("databases", new JSONObject().put(DATABASE, new JSONObject().put("groups", new JSONObject())))
        .toString());

    await().atMost(CONVERGENCE).untilAsserted(() -> assertThat(
        cached.requestAccessOnDatabase(DATABASE_ACCESS.UPDATE_SCHEMA))
        .as("a deleted group must stop granting on the peer too").isFalse());
  }

  /**
   * End to end through the producer both transports share: {@code ServerControlPlane.saveGroup} on the node
   * that serves the request (HTTP {@code PostGroupHandler} and gRPC {@code ArcadeDbGrpcAdminService} both call
   * it), with the submitted document handed to a second {@link ServerSecurity} standing in for a peer.
   */
  @Test
  void aGroupNarrowedOnTheLeaderReachesAPeersCachedPrincipal() {
    final CapturingHAPlugin ha = new CapturingHAPlugin();
    server.setHA(ha);

    final FixtureServer peerServer = new FixtureServer(server.getConfiguration());
    final File peerDir = new File(CONFIG_PATH, "peer");
    assertThat(peerDir.mkdirs()).isTrue();
    final ServerSecurity peer = new ServerSecurity(peerServer, server.getConfiguration(), peerDir.getPath());
    peerServer.setSecurity(peer);
    final ServerDatabase peerDatabase = mockDatabase(DATABASE);
    peerServer.register(peerDatabase);

    try {
      final ServerControlPlane controlPlane = new ServerControlPlane(server);
      controlPlane.saveGroup(DATABASE, GROUP, group(new JSONArray().put("updateSchema")));
      peer.applyReplicatedGroups(ha.lastDocument());

      final ServerSecurityDatabaseUser cached = connectedPrincipal(peer, peerDatabase);
      assertThat(cached.requestAccessOnDatabase(DATABASE_ACCESS.UPDATE_SCHEMA))
          .as("the peer starts with the grant the leader replicated").isTrue();

      controlPlane.saveGroup(DATABASE, GROUP, group(new JSONArray()));
      peer.applyReplicatedGroups(ha.lastDocument());

      await().atMost(CONVERGENCE).untilAsserted(() -> assertThat(
          cached.requestAccessOnDatabase(DATABASE_ACCESS.UPDATE_SCHEMA)).isFalse());
    } finally {
      peer.stopService();
    }
  }

  /**
   * The refresh must be scheduled on the path that then throws. A write failure leaves the document in force in
   * memory by design (issue #7137/#7373), so the derived permissions have to follow it - refreshing only on the
   * happy path would leave the node enforcing the previous groups for as long as the volume stayed broken,
   * which is precisely the case the publish-before-persist ordering exists to prevent.
   */
  @Test
  void aRefreshIsScheduledEvenWhenTheReplicatedDocumentCouldNotBePersisted() throws Exception {
    security.applyReplicatedGroups(documentGranting(new JSONArray().put("updateSchema")));
    final ServerSecurityDatabaseUser cached = connectedPrincipal();
    assertThat(cached.requestAccessOnDatabase(DATABASE_ACCESS.UPDATE_SCHEMA)).isTrue();

    makeGroupFileUnwritable();

    assertThatThrownBy(() -> security.applyReplicatedGroups(documentGranting(new JSONArray())))
        .as("the caller still learns the document did not reach the disk")
        .isInstanceOf(ReplicatedSecurityConfigPersistenceException.class);

    await().atMost(CONVERGENCE).untilAsserted(() -> assertThat(
        cached.requestAccessOnDatabase(DATABASE_ACCESS.UPDATE_SCHEMA))
        .as("the revocation IS in force in memory, so the cached principal must follow it").isFalse());
  }

  /**
   * The constraint the hand-off exists for: {@code applyReplicatedGroups} runs on the Raft state-machine apply
   * thread, which must never block, so the sweep - which walks every open database - must happen on another
   * one.
   */
  @Test
  void theRefreshRunsOffTheCallingApplyThread() {
    security.applyReplicatedGroups(documentGranting(new JSONArray().put("updateSchema")));
    final ServerSecurityDatabaseUser cached = connectedPrincipal();
    server.sweepThreads.clear();

    final String applyThread = Thread.currentThread().getName();
    security.applyReplicatedGroups(documentGranting(new JSONArray()));

    await().atMost(CONVERGENCE).untilAsserted(() -> assertThat(
        cached.requestAccessOnDatabase(DATABASE_ACCESS.UPDATE_SCHEMA)).isFalse());

    // Copied before asserting: a coalesced second sweep may still be appending to the live list.
    assertThat(new ArrayList<>(server.sweepThreads))
        .as("the sweep must not have run on the thread that applied the entry").isNotEmpty()
        .doesNotContain(applyThread)
        .allSatisfy(name -> assertThat(name).startsWith("arcadedb-security-permission-refresh"));
  }

  /**
   * The refreshes are coalesced, so a burst of entries must still converge on the LAST document rather than on
   * whichever one a sweep happened to read. The final document here revokes, so a lost update leaves the peer
   * granting a permission the cluster has revoked - the failure this test exists to catch.
   */
  @Test
  void aBurstOfReplicatedEntriesConvergesOnTheLastDocument() {
    security.applyReplicatedGroups(documentGranting(new JSONArray().put("updateSchema")));
    final ServerSecurityDatabaseUser cached = connectedPrincipal();

    for (int i = 0; i < 200; i++)
      security.applyReplicatedGroups(documentGranting(
          i % 2 == 0 ? new JSONArray() : new JSONArray().put("updateSchema")));
    security.applyReplicatedGroups(documentGranting(new JSONArray()));

    await().atMost(CONVERGENCE).untilAsserted(() -> assertThat(
        cached.requestAccessOnDatabase(DATABASE_ACCESS.UPDATE_SCHEMA))
        .as("the last document wins; a coalesced refresh must not drop it").isFalse());
  }

  // -------------------------------------------------------------------------------------------
  // Fixtures
  // -------------------------------------------------------------------------------------------

  private ServerSecurityDatabaseUser connectedPrincipal() {
    return connectedPrincipal(security, database);
  }

  private static ServerSecurityDatabaseUser connectedPrincipal(final ServerSecurity target,
      final ServerDatabase targetDatabase) {
    final ServerSecurityUser alice = target.createUser(new JSONObject()
        .put("name", "alice")
        .put("password", target.encodePassword("alice-password"))
        .put("databases", new JSONObject().put(DATABASE, new JSONArray().put(GROUP))));
    return alice.getDatabaseUser(targetDatabase);
  }

  /** One group definition, in the shape {@code ServerControlPlane.saveGroup} normalizes a request body into. */
  private static JSONObject group(final JSONArray databaseAccess) {
    return new JSONObject()
        .put("access", databaseAccess)
        .put("resultSetLimit", -1L)
        .put("readTimeout", -1L)
        .put("types", new JSONObject().put("*", new JSONObject().put("access",
            new JSONArray().put("createRecord").put("readRecord").put("updateRecord").put("deleteRecord"))));
  }

  /** A whole group document, in the shape a {@code SECURITY_GROUPS_ENTRY} carries. */
  private static String documentGranting(final JSONArray databaseAccess) {
    return new JSONObject()
        .put("version", ServerSecurity.LATEST_VERSION)
        .put("databases", new JSONObject().put(DATABASE,
            new JSONObject().put("groups", new JSONObject().put(GROUP, group(databaseAccess)))))
        .toString();
  }

  /**
   * Makes the group file unpersistable in a way that holds on every platform and does not depend on the test
   * running as an unprivileged user: the target path becomes a non-empty directory, so the publishing rename
   * cannot replace it. Same device the users half of issue #7137 uses.
   */
  private static void makeGroupFileUnwritable() throws Exception {
    final File asDirectory = new File(CONFIG_PATH, SecurityGroupFileRepository.FILE_NAME);
    FileUtils.deleteRecursively(asDirectory);
    assertThat(asDirectory.mkdirs()).isTrue();
    assertThat(new File(asDirectory, "occupied").createNewFile()).isTrue();
  }

  private static ServerDatabase mockDatabase(final String name) {
    final FileManager fileManager = mock(FileManager.class);
    when(fileManager.getFiles()).thenReturn(List.of());

    final Schema schema = mock(Schema.class);
    when(schema.getTypes()).thenReturn(List.of());

    final ServerDatabase db = mock(ServerDatabase.class);
    when(db.getName()).thenReturn(name);
    when(db.getFileManager()).thenReturn(fileManager);
    when(db.getSchema()).thenReturn(schema);
    return db;
  }

  /** Records the document each mutation submits, the way the Raft broker would ship it to the peers. */
  private static class CapturingHAPlugin implements HAServerPlugin {
    private final List<String> groupDocuments = Collections.synchronizedList(new ArrayList<>());

    String lastDocument() {
      return groupDocuments.getLast();
    }

    @Override
    public void replicateSecurityGroups(final String groupsJson) {
      groupDocuments.add(groupsJson);
    }

    @Override
    public ELECTION_STATUS getElectionStatus() {
      return ELECTION_STATUS.DONE;
    }

    @Override
    public void startService() {
    }

    @Override
    public boolean isLeader() {
      return true;
    }

    @Override
    public String getLeaderName() {
      return "leader";
    }

    @Override
    public String getClusterName() {
      return "test";
    }

    @Override
    public Map<String, Object> getStats() {
      return Collections.emptyMap();
    }

    @Override
    public int getConfiguredServers() {
      return 3;
    }

    @Override
    public String getLeaderAddress() {
      return null;
    }

    @Override
    public String getReplicaAddresses() {
      return "";
    }

    @Override
    public void shutdownRemoteServer(final String serverName) {
    }

    @Override
    public void disconnectCluster() {
    }
  }

  /**
   * An {@link ArcadeDBServer} that is never started: the fixture supplies the security store and the open
   * databases, and records which thread asked for each one - which is how the "not on the apply thread"
   * assertion is made, since the sweep is the only thing here that calls {@code getDatabase}.
   */
  private static final class FixtureServer extends ArcadeDBServer {
    private final Map<String, ServerDatabase> openDatabases = new LinkedHashMap<>();
    final         List<String>                sweepThreads  = Collections.synchronizedList(new ArrayList<>());
    private       ServerSecurity              security;

    private FixtureServer(final ContextConfiguration configuration) {
      super(configuration);
    }

    private void setSecurity(final ServerSecurity security) {
      this.security = security;
    }

    private void register(final ServerDatabase database) {
      openDatabases.put(database.getName(), database);
    }

    @Override
    public ServerSecurity getSecurity() {
      return security;
    }

    @Override
    public Set<String> getDatabaseNames() {
      return openDatabases.keySet();
    }

    @Override
    public ServerDatabase getDatabase(final String databaseName) {
      sweepThreads.add(Thread.currentThread().getName());
      return openDatabases.get(databaseName);
    }
  }
}
