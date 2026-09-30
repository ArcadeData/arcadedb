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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #8430, the user-list and API-token follow-up to #8074.
 * <p>
 * A cluster-wide user or token change carries the fingerprint of the document in force on the submitting node, and
 * every node judges it against the fingerprint of the last document the CLUSTER installed (issue #7693). When the
 * submitting node's copy was changed locally - a hand edit or a restored backup of {@code server-users.jsonl} or
 * {@code server-api-tokens.json}, picked up at restart or by the reload, including the legacy plaintext-token
 * migration a restored file triggers - no node accepts it. That refusal is kept on purpose: a node out of step with
 * the cluster must not push its own copy of the credentials over everyone else's (issue #6808), which is the
 * trade #8074 made the other way for groups only.
 * <p>
 * What this issue fixes is what the operator is told. The bounded compare-and-set loop used to give up with "the
 * document keeps being changed concurrently on another node ... retry the request", which is false and sends the
 * operator to retry a request that can never succeed from that node. It now names the local change and the two
 * ways out of it.
 * <p>
 * The fixture is a real two-node security state under a minimal Raft log ({@link LogHAPlugin}): entries are
 * appended in one order and every node applies them in that order with the precondition they carry.
 */
class Issue8430UserAndTokenLocalDriftTest {

  private static final String ROOT_PATH     = "target/test-security-8430";
  private static final String ROOT_PASSWORD = "Issue8430RootPassword";
  private static final String DATABASE      = "graph";

  private ServerSecurity nodeA;
  private ServerSecurity nodeB;
  private LogHAPlugin    cluster;

  @BeforeEach
  void setUp() {
    GlobalConfiguration.SERVER_SECURITY_SALT_ITERATIONS.setValue(1000);
    FileUtils.deleteRecursively(new File(ROOT_PATH));
    cluster = new LogHAPlugin();
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
  // Users
  // ===========================================================================================

  /**
   * Node B's {@code server-users.jsonl} is edited by hand and the node restarts. A user change from node B is still
   * refused - its local user must not reach node A - but the failure now says why, and does not tell the operator to
   * retry.
   */
  @Test
  void aUserChangeFromANodeWhoseUserFileWasEditedLocallyNamesTheLocalEdit() throws IOException {
    seedUsers();
    nodeB = restartWithEditedUserFile(nodeB, "b", "local-only");

    assertThatThrownBy(() -> nodeB.createUserClusterWide(userJson(nodeB, "alice")))
        .isInstanceOf(ServerSecurityException.class)
        .hasMessageContaining("create user 'alice'")
        .hasMessageContaining("changed locally")
        .hasMessageContaining(SecurityUserFileRepository.FILE_NAME)
        .hasMessageContaining("another node")
        .hasMessageNotContaining("retry the request");

    assertThat(nodeA.getUsers()).as("the #6808 protection is unchanged: nothing reached node A")
        .doesNotContain("alice", "local-only");
  }

  /** The update and the drop build their precondition the same way, and fail the same way. */
  @Test
  void aUserUpdateAndDropFromTheEditedNodeNameTheLocalEditToo() throws IOException {
    seedUsers();
    nodeA.createUserClusterWide(userJson(nodeA, "alice"));
    nodeB = restartWithEditedUserFile(nodeB, "b", "local-only");

    assertThatThrownBy(() -> nodeB.updateUserClusterWide(userJson(nodeB, "alice")))
        .isInstanceOf(ServerSecurityException.class)
        .hasMessageContaining("changed locally")
        .hasMessageNotContaining("retry the request");
    assertThatThrownBy(() -> nodeB.dropUserClusterWide("alice"))
        .isInstanceOf(ServerSecurityException.class)
        .hasMessageContaining("changed locally")
        .hasMessageNotContaining("retry the request");

    assertThat(nodeA.getUsers()).contains("alice").doesNotContain("local-only");
  }

  /**
   * The way out the message names: the same change made through another node is accepted everywhere, and its entry
   * replaces the edited node's local copy, so both nodes hold one list again.
   */
  @Test
  void theSameUserChangeThroughAnotherNodeSucceedsAndReconvergesTheEditedNode() throws IOException {
    seedUsers();
    nodeB = restartWithEditedUserFile(nodeB, "b", "local-only");

    nodeA.createUserClusterWide(userJson(nodeA, "alice"));

    assertThat(nodeB.getUsers()).contains("alice").doesNotContain("local-only");
    assertThat(nodeB.usersFingerprint()).isEqualTo(nodeA.usersFingerprint());
  }

  /**
   * The counter-case: a node that is merely BEHIND - not edited - is refused once and succeeds on the retry, and
   * never sees the local-edit message.
   */
  @Test
  void aNodeThatIsOnlyBehindStillSucceedsOnTheRetry() {
    seedUsers();
    cluster.lagBehind(nodeB);
    nodeA.createUserClusterWide(userJson(nodeA, "bob"));

    nodeB.createUserClusterWide(userJson(nodeB, "alice"));

    assertThat(cluster.refusals).as("the entry built from the stale view was refused").isEqualTo(1);
    assertThat(nodeA.getUsers()).contains("alice", "bob");
    assertThat(nodeB.getUsers()).contains("alice", "bob");
  }

  /**
   * The counter-case for the message: when the compare-and-set really does lose to another node every time, and
   * this node's copy is exactly the cluster's, the failure keeps saying so rather than blaming a local edit.
   */
  @Test
  void aGenuinelyContendedUserChangeKeepsTheConcurrencyMessage() {
    seedUsers();
    final int[] competitor = { 0 };
    cluster.beforeEachUserSubmit(() -> {
      final JSONArray list = new JSONArray(nodeA.getUsersJsonPayload());
      list.put(userJson(nodeA, "competitor-" + competitor[0]++));
      cluster.submitUsers(list.toString(), nodeA.usersFingerprint());
    });

    assertThatThrownBy(() -> nodeB.createUserClusterWide(userJson(nodeB, "alice")))
        .isInstanceOf(ServerSecurityException.class)
        .hasMessageContaining("changed concurrently on another node")
        .hasMessageNotContaining("changed locally");
  }

  // ===========================================================================================
  // API tokens
  // ===========================================================================================

  /**
   * Node B restarts on a restored {@code server-api-tokens.json} that still holds a legacy plaintext token, and
   * {@link ApiTokenConfiguration#load()} migrates it node-locally. A token mint from node B is refused - the restored
   * token must not reach node A - with the local-change message.
   */
  @Test
  void aTokenMintFromANodeThatMigratedARestoredTokenFileNamesTheLocalChange() throws IOException {
    seedTokens();
    nodeB = restartWithLegacyTokenFile(nodeB, "b");

    assertThatThrownBy(() -> nodeB.createApiTokenClusterWide("mine", DATABASE, -1L, new JSONObject()))
        .isInstanceOf(ServerSecurityException.class)
        .hasMessageContaining("create API token 'mine'")
        .hasMessageContaining("changed locally")
        .hasMessageContaining(ApiTokenConfiguration.FILE_NAME)
        .hasMessageNotContaining("retry the request");

    assertThat(tokenNames(nodeA)).doesNotContain("mine", "legacy");
  }

  /** The revocation pins its precondition the same way. */
  @Test
  void aTokenRevocationFromTheChangedNodeNamesTheLocalChangeToo() throws IOException {
    seedTokens();
    final JSONObject minted = nodeA.createApiTokenClusterWide("shared", DATABASE, -1L, new JSONObject());
    nodeB = restartWithLegacyTokenFile(nodeB, "b");

    assertThatThrownBy(() -> nodeB.deleteApiTokenClusterWide(minted.getString("tokenHash")))
        .isInstanceOf(ServerSecurityException.class)
        .hasMessageContaining("changed locally")
        .hasMessageNotContaining("retry the request");

    assertThat(tokenNames(nodeA)).contains("shared");
  }

  /** And the same way out: the mint through the other node lands everywhere and reconverges node B. */
  @Test
  void theSameTokenMintThroughAnotherNodeReconvergesTheChangedNode() throws IOException {
    seedTokens();
    nodeB = restartWithLegacyTokenFile(nodeB, "b");

    nodeA.createApiTokenClusterWide("mine", DATABASE, -1L, new JSONObject());

    assertThat(tokenNames(nodeB)).contains("mine").doesNotContain("legacy");
    assertThat(nodeB.apiTokensFingerprint()).isEqualTo(nodeA.apiTokensFingerprint());
  }

  // ===========================================================================================
  // Fixtures
  // ===========================================================================================

  /** Installs node A's user list on both nodes as a replicated entry: the cluster's baseline. */
  private void seedUsers() {
    cluster.submitUsers(nodeA.getUsersJsonPayload(), null);
    assertThat(nodeB.usersFingerprint()).isEqualTo(nodeA.usersFingerprint());
  }

  /** Installs node A's token document, with one token in it, on both nodes: the cluster's baseline. */
  private void seedTokens() {
    nodeA.createApiTokenClusterWide("baseline", DATABASE, -1L, new JSONObject());
    assertThat(nodeB.apiTokensFingerprint()).isEqualTo(nodeA.apiTokensFingerprint());
  }

  /** Stops {@code node}, appends a user to its {@code server-users.jsonl} by hand, and starts it again. */
  private ServerSecurity restartWithEditedUserFile(final ServerSecurity node, final String name, final String extraUser)
      throws IOException {
    final JSONObject extra = userJson(node, extraUser);
    node.stopService();

    final File file = new File(configPath(name), SecurityUserFileRepository.FILE_NAME);
    Files.writeString(file.toPath(), Files.readString(file.toPath(), DatabaseFactory.getDefaultCharset())
        + extra + "\n", DatabaseFactory.getDefaultCharset());

    final ServerSecurity restarted = restart(node, name);
    assertThat(restarted.getUsers()).as("the fixture's premise: the restart loaded the local edit").contains(extraUser);
    assertThat(restarted.usersFingerprint()).isNotEqualTo(nodeA.usersFingerprint());
    return restarted;
  }

  /**
   * Stops {@code node}, adds a legacy plaintext token to its {@code server-api-tokens.json} - a backup from before
   * tokens were hashed - and starts it again, which migrates that entry node-locally.
   */
  private ServerSecurity restartWithLegacyTokenFile(final ServerSecurity node, final String name) throws IOException {
    node.stopService();

    final File file = new File(configPath(name), ApiTokenConfiguration.FILE_NAME);
    final JSONObject document = new JSONObject(Files.readString(file.toPath(), DatabaseFactory.getDefaultCharset()));
    document.getJSONArray("tokens").put(new JSONObject()
        .put("token", "at-0123456789abcdef0123456789abcdef")
        .put("name", "legacy")
        .put("database", DATABASE)
        .put("createdAt", 0L)
        .put("expiresAt", -1L)
        .put("permissions", new JSONObject()));
    Files.writeString(file.toPath(), document.toString(2), DatabaseFactory.getDefaultCharset());

    final ServerSecurity restarted = restart(node, name);
    assertThat(tokenNames(restarted)).as("the fixture's premise: the restart loaded and migrated the legacy token")
        .contains("legacy");
    assertThat(restarted.apiTokensFingerprint()).isNotEqualTo(nodeA.apiTokensFingerprint());
    return restarted;
  }

  private static JSONObject userJson(final ServerSecurity node, final String name) {
    return new JSONObject()
        .put("name", name)
        .put("password", node.encodePassword(name + "-password"))
        .put("databases", new JSONObject());
  }

  private static List<String> tokenNames(final ServerSecurity node) {
    final JSONArray tokens = new JSONObject(node.getApiTokensJsonPayload()).getJSONArray("tokens");
    final List<String> names = new ArrayList<>(tokens.length());
    for (int i = 0; i < tokens.length(); i++)
      names.add(tokens.getJSONObject(i).getString("name"));
    return names;
  }

  /** Opens a fresh security service over {@code name}'s configuration directory: a node restart. */
  private ServerSecurity restart(final ServerSecurity previous, final String name) {
    final ServerSecurity restarted = open(name);
    cluster.replace(previous, restarted);
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
    configuration.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, ROOT_PASSWORD);
    // Never fires during a test: only a restart loads the files.
    configuration.setValue(GlobalConfiguration.SERVER_SECURITY_RELOAD_EVERY, 3_600_000);

    final FixtureServer server = new FixtureServer(configuration);
    final ServerSecurity security = new ServerSecurity(server, configuration, configPath(name));
    server.setSecurity(security);
    server.setHA(cluster);
    security.loadUsers();
    return security;
  }

  /**
   * A Raft log reduced to what this test needs: one ordered list of user and token entries, each node applying
   * every entry it has not applied yet - with the precondition the entry carried - before the next one is
   * evaluated. A node can be told to lag, which leaves it one entry behind until the next submission catches it
   * up, and every user submission can be preceded by a competing one, the way Raft orders another node's entry
   * first.
   */
  private static final class LogHAPlugin implements HAServerPlugin {
    private static final char USERS  = 'U';
    private static final char TOKENS = 'T';

    private final List<Object[]>       log     = new ArrayList<>();
    private final List<ServerSecurity> nodes   = new ArrayList<>();
    private final List<Integer>        applied = new ArrayList<>();
    private       ServerSecurity       lagging;
    private       Runnable             competing;
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

    void beforeEachUserSubmit(final Runnable competing) {
      this.competing = competing;
    }

    boolean submitUsers(final String payload, final String precondition) {
      return submit(USERS, payload, precondition);
    }

    private boolean submit(final char kind, final String payload, final String precondition) {
      log.add(new Object[] { kind, payload, precondition });
      Boolean outcome = null;
      for (int n = 0; n < nodes.size(); n++) {
        final ServerSecurity node = nodes.get(n);
        if (node == lagging) {
          lagging = null;
          continue;
        }
        for (int i = applied.get(n); i < log.size(); i++) {
          final Object[] entry = log.get(i);
          final boolean installed = (char) entry[0] == USERS ?
              node.applyReplicatedUsers((String) entry[1], (String) entry[2]) :
              node.applyReplicatedApiTokens((String) entry[1], (String) entry[2]);
          if (i == log.size() - 1) {
            if (outcome == null)
              outcome = installed;
            assertThat(installed).as("every node must reach the same verdict on the same entry").isEqualTo(outcome);
          }
        }
        applied.set(n, log.size());
      }
      assertThat(outcome).as("at least one node that is not lagging must have evaluated the entry").isNotNull();
      if (!outcome)
        refusals++;
      return outcome;
    }

    @Override
    public boolean replicateSecurityUsers(final String usersJson, final String expectedFingerprint) {
      // Another node's entry, ordered first by the log. It goes through submitUsers() directly, so it is not raced.
      if (competing != null)
        competing.run();
      return submit(USERS, usersJson, expectedFingerprint);
    }

    @Override
    public boolean replicateSecurityApiTokens(final String apiTokensJson, final String expectedFingerprint) {
      return submit(TOKENS, apiTokensJson, expectedFingerprint);
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
