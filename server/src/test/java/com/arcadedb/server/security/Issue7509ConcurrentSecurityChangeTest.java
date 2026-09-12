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
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #7509: a replicated security document is read-modify-write, so a concurrent admin
 * change on another node used to be lost.
 * <p>
 * All three node-scoped security documents - the user list, the group document and the API-token document - are
 * replicated WHOLE: the submitter reads the current one, mutates a copy and submits it. The read-compute-submit
 * sequence is serialised by {@code synchronized (this)} on {@code ServerSecurity}, which is a per-NODE monitor.
 * Two nodes doing it at the same time each build a document from their own view; Raft linearises the two
 * entries and the one ordered second, built without the first in it, silently reverted the first - on every
 * node, including the one that accepted it and answered 200.
 * <p>
 * <b>How the race is reproduced deterministically.</b> {@link RacingHAPlugin} stands in for the Raft round
 * trip. Before it evaluates the submitted entry it applies ONE competing document - the one another node built
 * from the same starting state - which is exactly what "Raft ordered the other node's entry first" looks like
 * from here. It then evaluates the submitted entry against the state that competing document left behind,
 * which is what {@code ArcadeStateMachine} does on the leader, and answers whether it was installed.
 * <p>
 * Every test therefore asserts two things: the competing change SURVIVED (that is the defect), and the
 * submitted change landed too (the retry converged rather than giving up).
 */
class Issue7509ConcurrentSecurityChangeTest {

  private static final String CONFIG_PATH = "target/test-security-7509";
  private static final String DATABASE    = "graph";

  private FixtureServer   server;
  private ServerSecurity  security;
  private RacingHAPlugin  ha;

  @BeforeEach
  void setUp() {
    GlobalConfiguration.SERVER_SECURITY_SALT_ITERATIONS.setValue(1000);
    final File dir = new File(CONFIG_PATH);
    if (dir.exists())
      FileUtils.deleteRecursively(dir);
    assertThat(dir.mkdirs()).isTrue();

    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.SERVER_ROOT_PATH, "./target");

    server = new FixtureServer(configuration);
    security = new ServerSecurity(server, configuration, CONFIG_PATH);
    server.setSecurity(security);
  }

  @AfterEach
  void tearDown() {
    if (security != null)
      security.stopService();
    GlobalConfiguration.SERVER_SECURITY_SALT_ITERATIONS.reset();
    FileUtils.deleteRecursively(new File(CONFIG_PATH));
  }

  /** Puts this node in a cluster whose next entry is preceded by {@code competing}, committed on another node. */
  private void joinClusterRacing(final Runnable competing) {
    ha = new RacingHAPlugin(security, competing, 1);
    server.setHA(ha);
  }

  // ===========================================================================================
  // Users: createUserClusterWide / updateUserClusterWide / dropUserClusterWide
  // ===========================================================================================

  @Test
  void aConcurrentUserCreateOnAnotherNodeSurvivesThisNodesCreate() {
    joinClusterRacing(() -> applyUserListWith("bob"));

    security.createUserClusterWide(userJson("alice"));

    assertThat(security.getUsers())
        .as("the user another node created in the same window must not be reverted by this node's create")
        .contains("bob", "alice");
    assertThat(ha.submits)
        .as("the first submit lost the compare-and-set and was retried against the fresh list").isEqualTo(2);
  }

  @Test
  void aConcurrentUserCreateOnAnotherNodeSurvivesThisNodesUpdate() {
    security.createUser(userJson("alice"));
    joinClusterRacing(() -> applyUserListWith("bob"));

    final JSONObject updated = userJson("alice").put("databases", new JSONObject().put(DATABASE, new JSONArray()));
    security.updateUserClusterWide(updated);

    assertThat(security.getUsers()).contains("bob", "alice");
    assertThat(security.getUser("alice").toJSON().getJSONObject("databases").has(DATABASE))
        .as("this node's own update landed as well").isTrue();
    assertThat(ha.submits).isEqualTo(2);
  }

  @Test
  void aConcurrentUserCreateOnAnotherNodeSurvivesThisNodesDrop() {
    security.createUser(userJson("alice"));
    joinClusterRacing(() -> applyUserListWith("bob"));

    assertThat(security.dropUserClusterWide("alice")).isTrue();

    assertThat(security.getUsers())
        .as("the concurrently created user survives and the drop still took effect")
        .contains("bob").doesNotContain("alice");
    assertThat(ha.submits).isEqualTo(2);
  }

  // ===========================================================================================
  // Groups: saveGroupClusterWide / deleteGroupClusterWide
  // ===========================================================================================

  @Test
  void aConcurrentGroupSaveOnAnotherNodeSurvivesThisNodesSave() {
    joinClusterRacing(() -> applyGroupDocumentWith("auditors"));

    security.saveGroupClusterWide(DATABASE, "editors", group());

    assertThat(groupNames()).contains("auditors", "editors");
    assertThat(ha.submits).isEqualTo(2);
  }

  @Test
  void aConcurrentGroupSaveOnAnotherNodeSurvivesThisNodesDelete() {
    security.saveGroup(DATABASE, "editors", group());
    joinClusterRacing(() -> applyGroupDocumentWith("auditors"));

    assertThat(security.deleteGroupClusterWide(DATABASE, "editors")).isTrue();

    assertThat(groupNames()).contains("auditors").doesNotContain("editors");
    assertThat(ha.submits).isEqualTo(2);
  }

  // ===========================================================================================
  // API tokens: createApiTokenClusterWide / deleteApiTokenClusterWide
  // ===========================================================================================

  @Test
  void aConcurrentTokenMintOnAnotherNodeSurvivesThisNodesMint() {
    joinClusterRacing(() -> applyTokenDocumentWith("other-node-token"));

    final JSONObject minted = security.createApiTokenClusterWide("my-token", DATABASE, -1L, new JSONObject());

    assertThat(minted.getString("token"))
        .as("the plaintext returned is the one from the attempt that actually committed").isNotEmpty();
    assertThat(tokenNames()).contains("other-node-token", "my-token");
    assertThat(ha.submits).isEqualTo(2);
  }

  @Test
  void aConcurrentTokenMintOnAnotherNodeSurvivesThisNodesRevocation() {
    final JSONObject mine = security.createApiTokenClusterWide("my-token", DATABASE, -1L, new JSONObject());
    joinClusterRacing(() -> applyTokenDocumentWith("other-node-token"));

    assertThat(security.deleteApiTokenClusterWide(mine.getString("tokenHash"))).isTrue();

    assertThat(tokenNames())
        .as("a revocation must not put back a token another node minted in the same window")
        .contains("other-node-token").doesNotContain("my-token");
    assertThat(ha.submits).isEqualTo(2);
  }

  // ===========================================================================================
  // The apply, driven directly: this is what every peer does with the entry
  // ===========================================================================================

  @Test
  void theApplyRefusesADocumentBuiltFromAListThatIsNoLongerInForce() {
    security.createUser(userJson("alice"));
    final String staleList = security.getUsersJsonPayload();
    final String stale = security.usersFingerprint();

    // Another node's change lands first, so the fingerprint the stale submission carries no longer matches.
    applyUserListWith("bob");

    assertThat(security.applyReplicatedUsers(staleList, stale))
        .as("an entry whose precondition no longer holds is refused").isFalse();
    assertThat(security.getUsers()).as("and the list it would have reverted is untouched").contains("bob");

    // The same payload with the CURRENT fingerprint is installed: the refusal is the precondition, not the
    // payload, so this test cannot pass by refusing everything.
    assertThat(security.applyReplicatedUsers(security.getUsersJsonPayload(), security.usersFingerprint())).isTrue();
  }

  @Test
  void theApplyInstallsAnEntryThatCarriesNoPreconditionAtAll() {
    security.createUser(userJson("alice"));

    // A seed, and every entry written by a node that predates issue #7509: no precondition, installed as before.
    assertThat(security.applyReplicatedUsers(new JSONArray().put(userJson("bob")).toString(), null)).isTrue();
    assertThat(security.getUsers()).containsExactly("bob");
  }

  @Test
  void theGroupAndTokenAppliesRefuseAStalePreconditionToo() {
    security.saveGroup(DATABASE, "editors", group());
    final String staleGroups = security.getGroupsJsonPayload();
    final String staleGroupFingerprint = security.groupsFingerprint();
    applyGroupDocumentWith("auditors");
    assertThat(security.applyReplicatedGroups(staleGroups, staleGroupFingerprint)).isFalse();
    assertThat(groupNames()).contains("auditors");

    final String staleTokens = security.getApiTokensJsonPayload();
    final String staleTokenFingerprint = security.apiTokensFingerprint();
    applyTokenDocumentWith("other-node-token");
    assertThat(security.applyReplicatedApiTokens(staleTokens, staleTokenFingerprint)).isFalse();
    assertThat(tokenNames()).contains("other-node-token");
  }

  // ===========================================================================================
  // The budget
  // ===========================================================================================

  @Test
  void aMutationThatKeepsLosingTheRaceReportsAConflictInsteadOfASilentSuccess() {
    // A competing change before EVERY submit, so no attempt can ever win.
    ha = new RacingHAPlugin(security, () -> applyUserListWith("filler-" + System.nanoTime()), Integer.MAX_VALUE);
    server.setHA(ha);

    assertThatThrownBy(() -> security.createUserClusterWide(userJson("alice")))
        .isInstanceOf(ServerSecurityException.class)
        .hasMessageContaining("changed concurrently")
        .hasMessageContaining("Nothing was changed");

    assertThat(security.getUsers()).as("and the user really was not created").doesNotContain("alice");
    assertThat(ha.submits).as("the budget is bounded, not unlimited").isEqualTo(5);
  }

  // ===========================================================================================
  // Fixtures
  // ===========================================================================================

  private JSONObject userJson(final String name) {
    return new JSONObject()
        .put("name", name)
        .put("password", security.encodePassword(name + "-password"))
        .put("databases", new JSONObject());
  }

  private static JSONObject group() {
    return new JSONObject()
        .put("resultSetLimit", -1L)
        .put("readTimeout", -1L)
        .put("access", new JSONArray())
        .put("types", new JSONObject());
  }

  /** Another node's user list: the one in force here, plus {@code name}. Applied as a committed entry. */
  private void applyUserListWith(final String name) {
    final JSONArray list = new JSONArray(security.getUsersJsonPayload());
    list.put(userJson(name));
    security.applyReplicatedUsers(list.toString(), null);
  }

  /** Another node's group document: the one in force here, plus a group named {@code name}. */
  private void applyGroupDocumentWith(final String name) {
    final JSONObject root = new JSONObject(security.getGroupsJsonPayload());
    final JSONObject databases = root.getJSONObject("databases");
    if (!databases.has(DATABASE))
      databases.put(DATABASE, new JSONObject().put("groups", new JSONObject()));
    databases.getJSONObject(DATABASE).getJSONObject("groups").put(name, group());
    security.applyReplicatedGroups(root.toString(), null);
  }

  /** Another node's token document: the one in force here, plus a token named {@code name}. */
  private void applyTokenDocumentWith(final String name) {
    final JSONObject root = new JSONObject(security.getApiTokensJsonPayload());
    root.getJSONArray("tokens").put(new JSONObject()
        .put("tokenHash", "hash-of-" + name)
        .put("name", name)
        .put("database", DATABASE)
        .put("createdAt", 0L)
        .put("expiresAt", -1L)
        .put("permissions", new JSONObject()));
    security.applyReplicatedApiTokens(root.toString(), null);
  }

  private List<String> groupNames() {
    final JSONObject databases = security.groupsToJSON().getJSONObject("databases");
    if (!databases.has(DATABASE))
      return List.of();
    return new ArrayList<>(databases.getJSONObject(DATABASE).getJSONObject("groups").keySet());
  }

  private List<String> tokenNames() {
    final JSONArray tokens = new JSONObject(security.getApiTokensJsonPayload()).getJSONArray("tokens");
    final List<String> names = new ArrayList<>(tokens.length());
    for (int i = 0; i < tokens.length(); i++)
      names.add(tokens.getJSONObject(i).getString("name"));
    return names;
  }

  /**
   * The Raft round trip, reduced to what this test needs: another node's entry commits FIRST, then the
   * submitted entry is evaluated against the state that left behind - which is what the leader's state machine
   * does before {@code submitAndWait} returns.
   */
  private static final class RacingHAPlugin implements HAServerPlugin {
    private final ServerSecurity security;
    private final Runnable       competing;
    private final int            competeForFirstNSubmits;
    int                          submits;

    private RacingHAPlugin(final ServerSecurity security, final Runnable competing, final int competeForFirstNSubmits) {
      this.security = security;
      this.competing = competing;
      this.competeForFirstNSubmits = competeForFirstNSubmits;
    }

    private void race() {
      if (submits < competeForFirstNSubmits)
        competing.run();
      submits++;
    }

    @Override
    public boolean replicateSecurityUsers(final String usersJson, final String expectedFingerprint) {
      race();
      return security.applyReplicatedUsers(usersJson, expectedFingerprint);
    }

    @Override
    public boolean replicateSecurityGroups(final String groupsJson, final String expectedFingerprint) {
      race();
      return security.applyReplicatedGroups(groupsJson, expectedFingerprint);
    }

    @Override
    public boolean replicateSecurityApiTokens(final String apiTokensJson, final String expectedFingerprint) {
      race();
      return security.applyReplicatedApiTokens(apiTokensJson, expectedFingerprint);
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
