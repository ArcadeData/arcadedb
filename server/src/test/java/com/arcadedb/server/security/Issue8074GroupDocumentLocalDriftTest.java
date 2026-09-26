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
package com.arcadedb.server.security;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.security.SecurityManager;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.HAServerPlugin;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Issue #8074, the group-document follow-up to #7601: the group document is changed node-locally by two supported
 * paths - the v1 to v2 migration {@link SecurityGroupFileRepository} runs when it loads a v1 file, and the hot
 * reload of a hand-edited {@code server-groups.json} - and neither goes through the replicated log.
 * <p>
 * Since #7693 every node judges a replicated group entry against the fingerprint of the last group document the
 * CLUSTER installed. The submitter used to send the fingerprint of its LIVE document instead, so after either local
 * change the precondition matched no node's recorded fingerprint: every node refused the entry, the bounded
 * compare-and-set loop gave up, and group administration stopped working from that node - for good, since the local
 * change never goes away by itself.
 * <p>
 * The fixture is a real two-node security state under a minimal Raft log ({@link LogHAPlugin}): entries are
 * appended in one order, every node applies them in that order with the precondition they carry, and a node that
 * is behind catches up before it applies anything new - which is what the state machine does.
 */
class Issue8074GroupDocumentLocalDriftTest {

  private static final String ROOT_PATH = "target/test-security-8074";
  private static final String DATABASE  = "graph";
  /** A watcher tick short enough that a hot reload is observed in milliseconds. */
  private static final int    FAST_RELOAD_MS = 100;
  /** A watcher tick that never fires during a test, so only a restart loads the file. */
  private static final int    NO_RELOAD_MS   = 3_600_000;

  private ServerSecurity nodeA;
  private ServerSecurity nodeB;
  private LogHAPlugin    cluster;
  private int            reloadEveryMs;

  @BeforeEach
  void setUp() {
    GlobalConfiguration.SERVER_SECURITY_SALT_ITERATIONS.setValue(1000);
    FileUtils.deleteRecursively(new File(ROOT_PATH));
    cluster = new LogHAPlugin();
  }

  private void startNodes(final int reloadEveryMs) {
    this.reloadEveryMs = reloadEveryMs;
    nodeA = node("a");
    nodeB = node("b");
  }

  @AfterEach
  void tearDown() {
    if (nodeA != null)
      nodeA.stopService();
    if (nodeB != null)
      nodeB.stopService();
    GlobalConfiguration.SERVER_SECURITY_SALT_ITERATIONS.reset();
    FileUtils.deleteRecursively(new File(ROOT_PATH));
  }

  // ===========================================================================================
  // Finding 2: the supported hot reload of server-groups.json
  // ===========================================================================================

  /**
   * An operator edits {@code server-groups.json} on node B and the watcher reloads it. Node B must still be able to
   * save a group cluster-wide afterwards, and the change must land on both nodes.
   */
  @Test
  void aHotReloadedGroupFileDoesNotBlockThatNodesGroupSaves() throws Exception {
    startNodes(FAST_RELOAD_MS);
    seedBothNodes();
    hotReload(nodeB, "b", "reloaded-locally");

    nodeB.saveGroupClusterWide(DATABASE, "editors", group());

    assertThat(groupNames(nodeA)).as("the save landed on the peer").contains("editors");
    assertThat(groupNames(nodeB)).as("and on the node that submitted it").contains("editors");
    assertThat(nodeA.groupsFingerprint()).as("and the two nodes hold the same document again")
        .isEqualTo(nodeB.groupsFingerprint());
  }

  /** The same path through the delete, which builds its precondition the same way. */
  @Test
  void aHotReloadedGroupFileDoesNotBlockThatNodesGroupDeletes() throws Exception {
    startNodes(FAST_RELOAD_MS);
    seedBothNodes();
    nodeA.saveGroupClusterWide(DATABASE, "editors", group());
    hotReload(nodeB, "b", "reloaded-locally");

    assertThat(nodeB.deleteGroupClusterWide(DATABASE, "editors")).isTrue();

    assertThat(groupNames(nodeA)).doesNotContain("editors");
    assertThat(groupNames(nodeB)).doesNotContain("editors");
  }

  // ===========================================================================================
  // Finding 1: the v1 to v2 migration
  // ===========================================================================================

  /**
   * The cluster installed a v1 document - one whose admin group lacks {@code updateDatabaseSettings}, as an older
   * leader submits it. Node B restarts, loads that file and migrates it; node A has not. A group save from node B
   * must still be accepted, and it carries the migration to node A with it, so both nodes move together.
   */
  @Test
  void aNodeThatMigratedAV1DocumentCanStillSaveGroups() {
    startNodes(NO_RELOAD_MS);
    final JSONObject v1 = v1Document();
    cluster.submit(v1.toString(), null);
    assertThat(adminAccess(nodeA)).as("the fixture's premise: the installed document is unmigrated")
        .doesNotContain("updateDatabaseSettings");

    nodeB = restart(nodeB, "b");
    assertThat(adminAccess(nodeB)).as("the restart migrated node B's copy, and only node B's")
        .contains("updateDatabaseSettings");
    assertThat(nodeB.groupsFingerprint()).isNotEqualTo(nodeA.groupsFingerprint());

    nodeB.saveGroupClusterWide(DATABASE, "editors", group());

    assertThat(groupNames(nodeA)).contains("editors");
    assertThat(adminAccess(nodeA)).as("the migrated document reached the node that had not migrated")
        .contains("updateDatabaseSettings");
    assertThat(nodeA.groupsFingerprint()).isEqualTo(nodeB.groupsFingerprint());
  }

  /**
   * The same migration reached without a restart. A node installs a replicated v1 document and writes it to
   * {@code server-groups.json}; its own file watcher then reloads that file and migrates it. Every node does this,
   * so every node's live document moves away from the one the cluster recorded - and before the fix no node could
   * change a group at all.
   */
  @Test
  void aV1DocumentMigratedByEveryNodesWatcherDoesNotBlockGroupSaves() {
    startNodes(FAST_RELOAD_MS);
    cluster.submit(v1Document().toString(), null);
    await().atMost(Duration.ofSeconds(10)).pollInterval(Duration.ofMillis(50))
        .until(() -> adminAccess(nodeA).contains("updateDatabaseSettings")
            && adminAccess(nodeB).contains("updateDatabaseSettings"));

    nodeA.saveGroupClusterWide(DATABASE, "editors", group());

    assertThat(groupNames(nodeA)).contains("editors");
    assertThat(groupNames(nodeB)).contains("editors");
  }

  // ===========================================================================================
  // The counter-case: the #7509 lost-update protection still holds
  // ===========================================================================================

  /**
   * What the precondition is for. Node B builds its entry before it has applied a change node A committed, so the
   * entry would revert it: it must still be refused, and the retry - once node B has caught up - must keep both.
   */
  @Test
  void aSubmitterThatMissedAnEntryIsStillRefusedAndTheOtherChangeSurvives() {
    startNodes(NO_RELOAD_MS);
    seedBothNodes();

    // Node A commits a group that node B has not applied yet: B is one entry behind when it builds its own.
    cluster.lagBehind(nodeB);
    nodeA.saveGroupClusterWide(DATABASE, "auditors", group());
    assertThat(groupNames(nodeB)).as("the fixture's premise: node B has not applied it").doesNotContain("auditors");

    nodeB.saveGroupClusterWide(DATABASE, "editors", group());

    assertThat(cluster.refusals).as("the entry built from the stale view was refused").isEqualTo(1);
    assertThat(groupNames(nodeA)).contains("auditors", "editors");
    assertThat(groupNames(nodeB)).contains("auditors", "editors");
  }

  // ===========================================================================================
  // Fixtures
  // ===========================================================================================

  /** Installs node A's current group document on both nodes as a replicated entry: the cluster's baseline. */
  private void seedBothNodes() {
    cluster.submit(nodeA.getGroupsJsonPayload(), null);
    assertThat(nodeA.groupsFingerprint()).isEqualTo(nodeB.groupsFingerprint());
  }

  /** Rewrites {@code node}'s {@code server-groups.json} with an extra group and waits for the watcher to load it. */
  private static void hotReload(final ServerSecurity node, final String name, final String extraGroup)
      throws IOException {
    final String before = node.groupsFingerprint();
    final File file = new File(configPath(name), SecurityGroupFileRepository.FILE_NAME);

    final JSONObject edited = new JSONObject(node.getGroupsJsonPayload());
    final JSONObject databases = edited.getJSONObject("databases");
    if (!databases.has(DATABASE))
      databases.put(DATABASE, new JSONObject().put("groups", new JSONObject()));
    databases.getJSONObject(DATABASE).getJSONObject("groups").put(extraGroup, group());

    Files.writeString(file.toPath(), edited.toString(2), DatabaseFactory.getDefaultCharset());
    // Strictly newer than anything the watcher has seen, whatever the filesystem's timestamp resolution.
    assertThat(file.setLastModified(System.currentTimeMillis() + 5_000)).isTrue();

    await().atMost(Duration.ofSeconds(10)).pollInterval(Duration.ofMillis(50))
        .until(() -> !before.equals(node.groupsFingerprint()));
    assertThat(groupNames(node)).as("the fixture's premise: the watcher reloaded the edit").contains(extraGroup);
  }

  private static JSONObject v1Document() {
    final JSONObject admin = new JSONObject().put("resultSetLimit", -1L).put("readTimeout", -1L)
        .put("access", new JSONArray(new String[] { "updateSecurity", "updateSchema" }))
        .put("types", new JSONObject().put(SecurityManager.ANY, new JSONObject().put("access",
            new JSONArray(new String[] { "createRecord", "readRecord", "updateRecord", "deleteRecord" }))));
    final JSONObject anyGroup = new JSONObject().put("resultSetLimit", -1L).put("readTimeout", -1L)
        .put("access", new JSONArray())
        .put("types", new JSONObject().put(SecurityManager.ANY, new JSONObject().put("access", new JSONArray())));
    return new JSONObject()
        .put("databases", new JSONObject().put(SecurityManager.ANY,
            new JSONObject().put("groups", new JSONObject().put("admin", admin).put(SecurityManager.ANY, anyGroup))))
        .put("version", 1);
  }

  private static List<String> adminAccess(final ServerSecurity node) {
    final JSONArray access = node.groupsToJSON().getJSONObject("databases").getJSONObject(SecurityManager.ANY)
        .getJSONObject("groups").getJSONObject("admin").getJSONArray("access");
    final List<String> names = new ArrayList<>(access.length());
    for (int i = 0; i < access.length(); i++)
      names.add(access.getString(i));
    return names;
  }

  private static List<String> groupNames(final ServerSecurity node) {
    final JSONObject databases = node.groupsToJSON().getJSONObject("databases");
    if (!databases.has(DATABASE))
      return List.of();
    return new ArrayList<>(databases.getJSONObject(DATABASE).getJSONObject("groups").keySet());
  }

  private static JSONObject group() {
    return new JSONObject()
        .put("resultSetLimit", -1L)
        .put("readTimeout", -1L)
        .put("access", new JSONArray())
        .put("types", new JSONObject());
  }

  /** Stops {@code security} and opens a fresh one over the same configuration directory: a node restart. */
  private ServerSecurity restart(final ServerSecurity security, final String name) {
    security.stopService();
    final ServerSecurity restarted = open(name);
    cluster.replace(security, restarted);
    return restarted;
  }

  private ServerSecurity node(final String name) {
    assertThat(new File(configPath(name)).mkdirs()).isTrue();
    final ServerSecurity security = open(name);
    cluster.join(security);
    return security;
  }

  private static String configPath(final String name) {
    return ROOT_PATH + "/" + name + "/config";
  }

  private ServerSecurity open(final String name) {
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.SERVER_ROOT_PATH, ROOT_PATH + "/" + name);
    configuration.setValue(GlobalConfiguration.SERVER_SECURITY_RELOAD_EVERY, reloadEveryMs);

    final FixtureServer server = new FixtureServer(configuration);
    final ServerSecurity security = new ServerSecurity(server, configuration, configPath(name));
    server.setSecurity(security);
    server.setHA(cluster);
    return security;
  }

  /**
   * A Raft log reduced to what this test needs: one ordered list of group entries, each node applying every entry
   * it has not applied yet - with the precondition the entry carried - before the next one is evaluated. A node can
   * be told to lag, which leaves it one entry behind until the next submission catches it up, the way a follower's
   * state machine trails the leader's.
   */
  private static final class LogHAPlugin implements HAServerPlugin {
    private final List<String[]>       log     = new ArrayList<>();
    private final List<ServerSecurity> nodes   = new ArrayList<>();
    private final List<Integer>        applied = new ArrayList<>();
    private       ServerSecurity       lagging;
    int refusals;

    void join(final ServerSecurity node) {
      nodes.add(node);
      applied.add(log.size());
    }

    void replace(final ServerSecurity previous, final ServerSecurity next) {
      nodes.set(nodes.indexOf(previous), next);
    }

    void lagBehind(final ServerSecurity node) {
      lagging = node;
    }

    /** Appends an entry and applies the log on every node; the result is the entry's, identical on every node. */
    boolean submit(final String payload, final String precondition) {
      log.add(new String[] { payload, precondition });
      Boolean outcome = null;
      for (int n = 0; n < nodes.size(); n++) {
        final ServerSecurity node = nodes.get(n);
        if (node == lagging) {
          // Held back for this one entry: it catches up on the next submission, in log order.
          lagging = null;
          continue;
        }
        for (int i = applied.get(n); i < log.size(); i++) {
          final boolean installed = node.applyReplicatedGroups(log.get(i)[0], log.get(i)[1]);
          if (i == log.size() - 1) {
            if (outcome == null)
              outcome = installed;
            assertThat(installed).as("every node must reach the same verdict on the same entry").isEqualTo(outcome);
          }
        }
        applied.set(n, log.size());
      }
      if (!outcome)
        refusals++;
      return outcome;
    }

    @Override
    public boolean replicateSecurityGroups(final String groupsJson, final String expectedFingerprint) {
      return submit(groupsJson, expectedFingerprint);
    }

    @Override
    public void awaitLocalApply() {
      // Every submission already applies the log on every non-lagging node before it returns.
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
      return "a";
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
      return 2;
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

  private static final class FixtureServer extends ArcadeDBServer {
    private ServerSecurity security;

    private FixtureServer(final ContextConfiguration configuration) {
      super(configuration);
    }

    private void setSecurity(final ServerSecurity security) {
      this.security = security;
    }

    @Override
    public ServerSecurity getSecurity() {
      return security;
    }
  }
}
