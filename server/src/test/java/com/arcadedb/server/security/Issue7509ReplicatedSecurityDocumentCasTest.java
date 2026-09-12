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
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.HAServerPlugin;
import com.arcadedb.server.security.SecurityDocumentVersions.Document;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #7509: a replicated security document was read-modify-write, so a concurrent admin
 * change on another node was lost.
 * <p>
 * All three documents - the user list, the group document and the API-token document - are replicated as the
 * WHOLE document: the submitter reads the current one, mutates a copy and submits the result. The
 * {@code synchronized} block that serialises that sequence is a PER-NODE monitor, so node B doing the same
 * thing at the same time built its payload from its own view. Raft linearised the two entries and the second
 * one carried a document that had been built without the first one in it: the first change was reverted on
 * every node, including the one that accepted it and answered 200.
 * <p>
 * Every test here drives the same shape - <b>the race, deterministically</b>. Node A's entry is captured on its
 * way to the log but not applied; node B's change is applied to the whole cluster; then A's stale entry is
 * released. The assertions are that it is refused on EVERY node (a verdict that differs between nodes would be
 * divergence, which is worse than the lost update) and that B's change is still in force.
 * <p>
 * The cluster is two {@link ServerSecurity} instances with their own configuration directories, driven through
 * the same {@code applyReplicated*} methods the Raft state machine calls.
 */
class Issue7509ReplicatedSecurityDocumentCasTest {

  private static final String CONFIG_PATH = "target/test-security-7509";

  private FixtureServer   server;
  private ServerSecurity  nodeA;
  private ServerSecurity  nodeB;
  private CapturingCluster cluster;

  @BeforeEach
  void setUp() {
    GlobalConfiguration.SERVER_SECURITY_SALT_ITERATIONS.setValue(1000);
    final File dir = new File(CONFIG_PATH);
    if (dir.exists())
      FileUtils.deleteRecursively(dir);
    assertThat(new File(CONFIG_PATH, "nodeA").mkdirs()).isTrue();
    assertThat(new File(CONFIG_PATH, "nodeB").mkdirs()).isTrue();

    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.SERVER_ROOT_PATH, "./target");

    server = new FixtureServer(configuration);
    nodeA = new ServerSecurity(server, configuration, new File(CONFIG_PATH, "nodeA").getPath());
    nodeB = new ServerSecurity(null, new ContextConfiguration(), new File(CONFIG_PATH, "nodeB").getPath());
    server.setSecurity(nodeA);

    cluster = new CapturingCluster(List.of(nodeA, nodeB));
    server.setHA(cluster);
  }

  @AfterEach
  void tearDown() {
    if (nodeA != null)
      nodeA.stopService();
    if (nodeB != null)
      nodeB.stopService();
    GlobalConfiguration.SERVER_SECURITY_SALT_ITERATIONS.reset();
    FileUtils.deleteRecursively(new File(CONFIG_PATH));
  }

  // -------------------------------------------------------------------------------------------
  // The user list - the path the issue was reported on
  // -------------------------------------------------------------------------------------------

  @Test
  void aUserCreatedOnAnotherNodeIsNotRevertedByAConcurrentCreate() {
    // Node A reads the list and submits "add alice". Its entry reaches the log but is not applied yet.
    cluster.captureNext();
    nodeA.createUserClusterWide(user("alice"));
    final SubmittedEntry stale = cluster.releaseCaptured();

    // Meanwhile node B adds bob, built from the same starting list, and that entry is applied everywhere.
    cluster.apply(new SubmittedEntry(Document.USERS, usersDocumentWith("bob"), 0L, 1L));
    assertThat(nodeA.existsUser("bob")).isTrue();

    // A's entry now carries a list with alice and WITHOUT bob. Before the fix it was installed and bob vanished.
    assertThatThrownBy(() -> cluster.apply(stale)).isInstanceOf(SecurityDocumentConflictException.class);

    assertThat(nodeA.existsUser("bob")).as("bob must survive a stale entry from another node").isTrue();
    assertThat(nodeB.existsUser("bob")).isTrue();
    assertThat(nodeA.existsUser("alice")).as("and the refused change must not be half-applied").isFalse();
    assertThat(nodeB.existsUser("alice")).isFalse();
  }

  @Test
  void aUserUpdateBuiltFromAStaleListIsRefused() {
    nodeA.createUserClusterWide(user("alice"));

    cluster.captureNext();
    nodeA.updateUserClusterWide(user("alice", "rotated-password"));
    final SubmittedEntry stale = cluster.releaseCaptured();

    cluster.apply(new SubmittedEntry(Document.USERS, usersDocumentWith("bob"), 1L, 2L));

    assertThatThrownBy(() -> cluster.apply(stale)).isInstanceOf(SecurityDocumentConflictException.class);
    assertThat(nodeA.existsUser("bob")).isTrue();
    assertThat(nodeB.existsUser("bob")).isTrue();
  }

  /**
   * The revocation direction, which is the worse one: a DROP submitted on node A undone by an unrelated change
   * on node B means an account the operator removed - and was told had been removed - is still able to log in.
   */
  @Test
  void aUserDropIsNotUndoneByAConcurrentUnrelatedChange() {
    nodeA.createUserClusterWide(user("alice"));

    cluster.captureNext();
    assertThat(nodeA.dropUserClusterWide("alice")).isTrue();
    final SubmittedEntry staleDrop = cluster.releaseCaptured();

    // Node B adds bob. Its document still contains alice, because alice's drop has not been applied yet.
    cluster.apply(new SubmittedEntry(Document.USERS, usersDocumentWith("bob", "alice"), 1L, 2L));

    assertThatThrownBy(() -> cluster.apply(staleDrop)).isInstanceOf(SecurityDocumentConflictException.class);
    assertThat(nodeA.existsUser("bob")).as("bob is not reverted").isTrue();
  }

  /** The happy path still works, and moves every node's counter by exactly one. */
  @Test
  void anUncontendedUserChangeAppliesEverywhereAndAdvancesTheVersion() {
    assertThat(nodeA.getSecurityDocumentVersion(Document.USERS)).isZero();

    nodeA.createUserClusterWide(user("alice"));

    assertThat(nodeA.existsUser("alice")).isTrue();
    assertThat(nodeB.existsUser("alice")).isTrue();
    assertThat(nodeA.getSecurityDocumentVersion(Document.USERS)).isEqualTo(1L);
    assertThat(nodeB.getSecurityDocumentVersion(Document.USERS)).isEqualTo(1L);
  }

  // -------------------------------------------------------------------------------------------
  // The group document (#7373 gave it the same shape deliberately)
  // -------------------------------------------------------------------------------------------

  @Test
  void aGroupSavedOnAnotherNodeIsNotRevertedByAConcurrentSave() {
    cluster.captureNext();
    nodeA.saveGroupClusterWide("*", "reader", group());
    final SubmittedEntry stale = cluster.releaseCaptured();

    cluster.apply(new SubmittedEntry(Document.GROUPS, groupsDocumentWith("writer"), 0L, 1L));
    assertThat(groupsOf(nodeA).has("writer")).isTrue();

    assertThatThrownBy(() -> cluster.apply(stale)).isInstanceOf(SecurityDocumentConflictException.class);

    assertThat(groupsOf(nodeA).has("writer")).as("the group added on the other node survives").isTrue();
    assertThat(groupsOf(nodeB).has("writer")).isTrue();
    assertThat(groupsOf(nodeA).has("reader")).as("and the refused save installed nothing").isFalse();
  }

  @Test
  void aGroupDeleteBuiltFromAStaleDocumentIsRefused() {
    nodeA.saveGroupClusterWide("*", "reader", group());

    cluster.captureNext();
    assertThat(nodeA.deleteGroupClusterWide("*", "reader")).isTrue();
    final SubmittedEntry staleDelete = cluster.releaseCaptured();

    cluster.apply(new SubmittedEntry(Document.GROUPS, groupsDocumentWith("writer", "reader"), 1L, 2L));

    assertThatThrownBy(() -> cluster.apply(staleDelete)).isInstanceOf(SecurityDocumentConflictException.class);
    assertThat(groupsOf(nodeA).has("writer")).isTrue();
  }

  // -------------------------------------------------------------------------------------------
  // The API-token document
  // -------------------------------------------------------------------------------------------

  @Test
  void anApiTokenMintedOnAnotherNodeIsNotRevertedByAConcurrentMint() {
    cluster.captureNext();
    nodeA.createApiTokenClusterWide("ci", "*", 0L, new JSONObject());
    final SubmittedEntry stale = cluster.releaseCaptured();

    // Another node mints its own token from the same empty starting document.
    final JSONObject otherToken = mintedElsewhere("deploy");
    cluster.apply(new SubmittedEntry(Document.API_TOKENS, tokensDocumentWith(otherToken), 0L, 1L));
    assertThat(nodeA.getApiTokenConfiguration().listTokens()).hasSize(1);

    assertThatThrownBy(() -> cluster.apply(stale)).isInstanceOf(SecurityDocumentConflictException.class);

    assertThat(nodeA.getApiTokenConfiguration().listTokens())
        .as("the token minted on the other node is still there").hasSize(1);
    assertThat(nodeB.getApiTokenConfiguration().listTokens()).hasSize(1);
  }

  /**
   * The revocation direction for tokens. A token the operator revoked coming back because somebody minted an
   * unrelated one on another node in the same second is a security failure, not a consistency one.
   */
  @Test
  void anApiTokenRevocationIsNotUndoneByAConcurrentMint() {
    final JSONObject created = nodeA.createApiTokenClusterWide("ci", "*", 0L, new JSONObject());
    final String hash = created.getString("tokenHash");

    cluster.captureNext();
    assertThat(nodeA.deleteApiTokenClusterWide(hash)).isTrue();
    final SubmittedEntry staleRevocation = cluster.releaseCaptured();

    // Node B mints a second token from a document that still contains the revoked one.
    final JSONObject existing = nodeA.getApiTokenConfiguration().listTokens().getFirst();
    cluster.apply(new SubmittedEntry(Document.API_TOKENS,
        tokensDocumentWith(existing, mintedElsewhere("deploy")), 1L, 2L));

    assertThatThrownBy(() -> cluster.apply(staleRevocation)).isInstanceOf(SecurityDocumentConflictException.class);
    assertThat(nodeA.getApiTokenConfiguration().listTokens())
        .as("nothing was installed, so the mint on the other node stands")
        .hasSize(2);
  }

  // -------------------------------------------------------------------------------------------
  // Peer seeding stays unconditional
  // -------------------------------------------------------------------------------------------

  /**
   * A seed must never be refused: the joining peer holds nothing to compare against, and a refused seed leaves
   * it authenticating against whatever its own configuration directory happened to contain. It still stamps the
   * version it establishes, so the next ordinary change is conditional again on every node.
   */
  @Test
  void seedingAPeerIsUnconditionalAndStillStampsTheVersion() {
    nodeA.createUserClusterWide(user("alice"));
    // Move the cluster's counters out from under the seed, the way a change on another node would.
    cluster.apply(new SubmittedEntry(Document.USERS, usersDocumentWith("alice", "bob"), 1L, 5L));

    assertThatCode(() -> assertThat(nodeA.seedSecurityStateClusterWide()).isEmpty()).doesNotThrowAnyException();

    assertThat(nodeA.getSecurityDocumentVersion(Document.USERS)).isEqualTo(6L);
    assertThat(nodeB.getSecurityDocumentVersion(Document.USERS))
        .as("every node ends the seed on the same counter").isEqualTo(6L);
  }

  // -------------------------------------------------------------------------------------------
  // Durability of the counter
  // -------------------------------------------------------------------------------------------

  /**
   * The counter has to survive a restart, or a restarted node would disagree with its peers about the very next
   * entry - accepting what they refuse, or refusing what they accept, which is divergence rather than a lost
   * update.
   */
  @Test
  void theVersionSurvivesARestart() {
    nodeA.createUserClusterWide(user("alice"));
    nodeA.saveGroupClusterWide("*", "reader", group());
    assertThat(new File(CONFIG_PATH + "/nodeA", SecurityDocumentVersions.FILE_NAME)).isFile();

    final ServerSecurity restarted = new ServerSecurity(null, new ContextConfiguration(),
        new File(CONFIG_PATH, "nodeA").getPath());
    try {
      assertThat(restarted.getSecurityDocumentVersion(Document.USERS)).isEqualTo(1L);
      assertThat(restarted.getSecurityDocumentVersion(Document.GROUPS)).isEqualTo(1L);
      assertThat(restarted.getSecurityDocumentVersion(Document.API_TOKENS)).isZero();
    } finally {
      restarted.stopService();
    }
  }

  /**
   * A cluster upgrading into this fix has no counter file anywhere, and every node reads that as 0 - so the
   * first conditional entry is agreed on by all of them rather than needing a migration step.
   */
  @Test
  void aNodeWithNoCounterFileStartsFromZero() {
    assertThat(new File(CONFIG_PATH + "/nodeB", SecurityDocumentVersions.FILE_NAME)).doesNotExist();
    assertThat(nodeB.getSecurityDocumentVersion(Document.USERS)).isZero();
    assertThat(nodeB.getSecurityDocumentVersion(Document.GROUPS)).isZero();
    assertThat(nodeB.getSecurityDocumentVersion(Document.API_TOKENS)).isZero();
  }

  /**
   * An entry from a node that predates the fix carries no versions at all, which the applier reads as "apply
   * regardless and touch no counter" - so a mixed-version cluster keeps behaving exactly as it did rather than
   * refusing entries no node can satisfy.
   */
  @Test
  void anEntryWithNoVersionsAppliesUnconditionallyAndLeavesTheCounterAlone() {
    nodeA.createUserClusterWide(user("alice"));
    assertThat(nodeA.getSecurityDocumentVersion(Document.USERS)).isEqualTo(1L);

    cluster.apply(new SubmittedEntry(Document.USERS, usersDocumentWith("alice", "legacy"),
        SecurityDocumentVersions.UNCONDITIONAL, SecurityDocumentVersions.NO_VERSION));

    assertThat(nodeA.existsUser("legacy")).isTrue();
    assertThat(nodeA.getSecurityDocumentVersion(Document.USERS)).isEqualTo(1L);
  }

  // -------------------------------------------------------------------------------------------
  // Fixture
  // -------------------------------------------------------------------------------------------

  private static JSONObject user(final String name) {
    return user(name, "hashed-password");
  }

  private static JSONObject user(final String name, final String password) {
    return new JSONObject().put("name", name).put("password", password).put("databases", new JSONObject());
  }

  /** The user list as a peer that has not seen this node's in-flight change would build it. */
  private static String usersDocumentWith(final String... names) {
    final JSONArray array = new JSONArray();
    array.put(new JSONObject().put("name", "root").put("databases", new JSONObject()));
    for (final String name : names)
      array.put(user(name));
    return array.toString();
  }

  private static JSONObject group() {
    return new JSONObject()
        .put("resultSetLimit", -1L)
        .put("readTimeout", -1L)
        .put("access", new JSONArray())
        .put("types", new JSONObject());
  }

  private static String groupsDocumentWith(final String... groupNames) {
    final JSONObject groups = new JSONObject();
    for (final String name : groupNames)
      groups.put(name, group());
    return new JSONObject()
        .put("version", ServerSecurity.LATEST_VERSION)
        .put("databases", new JSONObject().put("*", new JSONObject().put("groups", groups)))
        .toString();
  }

  private static JSONObject groupsOf(final ServerSecurity security) {
    return security.groupsToJSON().getJSONObject("databases").getJSONObject("*").getJSONObject("groups");
  }

  /** A token document entry as another node would have produced it, carrying a hash and no token material. */
  private static JSONObject mintedElsewhere(final String name) {
    return new JSONObject()
        .put("name", name)
        .put("tokenHash", ApiTokenConfiguration.hashToken("at-" + name + "-elsewhere"))
        .put("database", "*")
        .put("createdAt", System.currentTimeMillis())
        .put("expiresAt", 0L)
        .put("permissions", new JSONObject());
  }

  private static String tokensDocumentWith(final JSONObject... tokens) {
    final JSONArray array = new JSONArray();
    for (final JSONObject token : tokens)
      array.put(token);
    return new JSONObject().put("version", 1).put("tokens", array).toString();
  }

  /** One entry on its way through the Raft log: the document, and the versions it compares and establishes. */
  private record SubmittedEntry(Document document, String payload, long expectedVersion, long newVersion) {
  }

  /**
   * Stands in for the Raft round trip across a two-node cluster, with one addition the ordinary fixture does not
   * need: an entry can be CAPTURED - accepted into the log but not applied - so a second entry can be applied
   * ahead of it. That is the race of issue #7509 made deterministic; nothing else about the ordering matters.
   * <p>
   * An apply that fails on a node rethrows to the submitter, which is what Ratis does with a state-machine
   * failure, and every node is applied to so the test can assert the verdict is the same on all of them.
   */
  private static final class CapturingCluster implements HAServerPlugin {
    private final List<ServerSecurity>  nodes;
    private final Deque<SubmittedEntry> captured = new ArrayDeque<>();
    private       boolean               capturing;

    private CapturingCluster(final List<ServerSecurity> nodes) {
      this.nodes = nodes;
    }

    /** The next submitted entry reaches the log but is not applied until {@link #releaseCaptured()}. */
    void captureNext() {
      capturing = true;
    }

    SubmittedEntry releaseCaptured() {
      return captured.removeFirst();
    }

    void apply(final SubmittedEntry entry) {
      RuntimeException failure = null;
      for (final ServerSecurity node : nodes) {
        try {
          applyOn(node, entry);
        } catch (final RuntimeException e) {
          // Collected rather than thrown immediately, so a refusal is proved to happen on EVERY node and not
          // only on the first one the loop reaches.
          if (failure == null)
            failure = e;
        }
      }
      if (failure != null)
        throw failure;
    }

    /**
     * The three steps {@code ArcadeStateMachine.applySecurity*Entry} performs on every node, in that order:
     * refuse an entry built from a version this node no longer holds, install the document, record the version
     * it establishes. Kept in this order deliberately - a check after the install would install a stale
     * document first, and a record before it would move the counter for a document that never arrived.
     */
    private static void applyOn(final ServerSecurity node, final SubmittedEntry entry) {
      node.checkReplicatedSecurityVersion(entry.document(), entry.expectedVersion());
      switch (entry.document()) {
      case USERS -> node.applyReplicatedUsers(entry.payload());
      case GROUPS -> node.applyReplicatedGroups(entry.payload());
      case API_TOKENS -> node.applyReplicatedApiTokens(entry.payload());
      }
      node.recordReplicatedSecurityVersion(entry.document(), entry.newVersion());
    }

    private void submit(final Document document, final String payload, final long expectedVersion,
        final long newVersion) {
      final SubmittedEntry entry = new SubmittedEntry(document, payload, expectedVersion, newVersion);
      if (capturing) {
        capturing = false;
        captured.addLast(entry);
        return;
      }
      apply(entry);
    }

    @Override
    public void replicateSecurityUsers(final String usersJsonArray) {
      submit(Document.USERS, usersJsonArray, SecurityDocumentVersions.UNCONDITIONAL,
          SecurityDocumentVersions.NO_VERSION);
    }

    @Override
    public void replicateSecurityUsers(final String usersJsonArray, final long expectedVersion, final long newVersion) {
      submit(Document.USERS, usersJsonArray, expectedVersion, newVersion);
    }

    @Override
    public void replicateSecurityGroups(final String groupsJson) {
      submit(Document.GROUPS, groupsJson, SecurityDocumentVersions.UNCONDITIONAL, SecurityDocumentVersions.NO_VERSION);
    }

    @Override
    public void replicateSecurityGroups(final String groupsJson, final long expectedVersion, final long newVersion) {
      submit(Document.GROUPS, groupsJson, expectedVersion, newVersion);
    }

    @Override
    public void replicateSecurityApiTokens(final String apiTokensJson) {
      submit(Document.API_TOKENS, apiTokensJson, SecurityDocumentVersions.UNCONDITIONAL,
          SecurityDocumentVersions.NO_VERSION);
    }

    @Override
    public void replicateSecurityApiTokens(final String apiTokensJson, final long expectedVersion,
        final long newVersion) {
      submit(Document.API_TOKENS, apiTokensJson, expectedVersion, newVersion);
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
    public void disconnectCluster() {
    }

    @Override
    public void shutdownRemoteServer(final String serverName) {
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
