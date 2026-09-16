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
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7693: every method of {@code RaftUserManagement3NodesIT} timed out waiting for a user to appear on all
 * three nodes, on {@code main} and on every branch alike. The user did not replicate at all - and the reason is
 * the compare-and-set of issue #7509, which compares two things that are not comparable yet.
 * <p>
 * The precondition an entry carries is the fingerprint of the user document its SUBMITTER read; every applying
 * node compares it against the fingerprint of its OWN document. That works once the nodes hold the same
 * document, and until the first replicated entry lands they do not: each node bootstraps its own {@code root}
 * with an independently salted password hash, so three nodes of a statically configured cluster - one that never
 * ran the {@code addPeer} / {@code connect cluster} seed - start with three different documents and three
 * different fingerprints. The observed log says it exactly:
 * <pre>
 * Refusing a replicated user list: ... (expected fingerprint fce987eb..., current 0d8c2180...)
 * Refusing a replicated user list: ... (expected fingerprint fce987eb..., current 455355f6...)
 * </pre>
 * Three different values for three nodes: the entry installed on the submitter and was refused by the other two.
 * <p>
 * <b>That is worse than the lost update #7509 is about</b>, and #7509's own javadoc says why: a refusal is safe
 * only while it is the SAME on every node ("the payload and the state it is compared against are both replicated
 * and applies are ordered"). Here the state compared against is NOT replicated - it is whatever the node's own
 * configuration directory held at bootstrap - so a non-uniform refusal splits the cluster's security state, and
 * the same credentials resolve differently depending on which node answers.
 * <p>
 * The fix is to let a node judge an entry only once its own document came from the cluster. Until then it has
 * nothing to compare against and installs - uniformly, because no node has one either.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7693FirstReplicatedSecurityEntryTest {

  private static final String ROOT_PATH = "target/test-security-7693";

  private ServerSecurity nodeA;
  private ServerSecurity nodeB;

  @BeforeEach
  void setUp() {
    GlobalConfiguration.SERVER_SECURITY_SALT_ITERATIONS.setValue(1000);
    FileUtils.deleteRecursively(new File(ROOT_PATH));
    nodeA = node("a");
    nodeB = node("b");

    // What a statically configured cluster looks like before its first replicated security entry: the same
    // principal on both nodes, bootstrapped independently, so the stored hashes - and therefore the documents,
    // and therefore the fingerprints - differ. encodePassword() salts randomly.
    nodeA.createUser(user(nodeA, "root", "rootpassword"));
    nodeB.createUser(user(nodeB, "root", "rootpassword"));
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

  /**
   * The fixture's own premise. Without this the test below would pass on a cluster whose nodes happened to agree,
   * which is the case that never failed.
   */
  @Test
  void twoIndependentlyBootstrappedNodesDoNotShareAFingerprint() {
    assertThat(nodeA.usersFingerprint())
        .as("the same principal with the same password hashes differently on each node, so the documents differ")
        .isNotEqualTo(nodeB.usersFingerprint());
  }

  /**
   * The defect: node A creates a user and submits the entry under its own fingerprint. Node B must install it.
   * Before the fix it refused, so the user existed on exactly one node of three and the cluster never converged.
   */
  @Test
  void theFirstReplicatedUserListIsInstalledOnEveryNode() {
    final String payload = usersOf(nodeA, "alice");
    final String precondition = nodeA.usersFingerprint();

    assertThat(nodeA.applyReplicatedUsers(payload, precondition))
        .as("the submitter's own apply matched, which is why the user appeared on one node").isTrue();
    assertThat(nodeB.applyReplicatedUsers(payload, precondition))
        .as("a peer whose document is still its own bootstrap has nothing to compare against and must install")
        .isTrue();

    assertThat(nodeA.getUser("alice")).isNotNull();
    assertThat(nodeB.getUser("alice")).isNotNull();
  }

  /**
   * And the cluster has converged after it: both nodes now hold the document the entry carried, so their
   * fingerprints agree and the concurrency check has something real to compare.
   */
  @Test
  void theNodesAgreeOnTheDocumentOnceTheFirstEntryHasLanded() {
    final String payload = usersOf(nodeA, "alice");
    nodeA.applyReplicatedUsers(payload, nodeA.usersFingerprint());
    nodeB.applyReplicatedUsers(payload, null);

    assertThat(nodeA.usersFingerprint()).isEqualTo(nodeB.usersFingerprint());
  }

  /**
   * The counter-case, and the whole point of not simply deleting the precondition: once both nodes hold the
   * cluster's document, a STALE precondition is refused, and refused on BOTH - which is the lost-update
   * protection of issue #7509, working and uniform.
   */
  @Test
  void aStalePreconditionIsStillRefusedOnceTheDocumentCameFromTheCluster() {
    final String firstEntry = usersOf(nodeA, "alice");
    final String staleFingerprint = nodeA.usersFingerprint();
    nodeA.applyReplicatedUsers(firstEntry, staleFingerprint);
    nodeB.applyReplicatedUsers(firstEntry, staleFingerprint);

    // A second entry built from the pre-alice view: installing it would revert the create both nodes just made.
    final String supersededEntry = usersOf(nodeB, "bob");

    assertThat(nodeA.applyReplicatedUsers(supersededEntry, staleFingerprint))
        .as("the document moved on, so the entry is refused").isFalse();
    assertThat(nodeB.applyReplicatedUsers(supersededEntry, staleFingerprint))
        .as("and refused identically here, which is what keeps the refusal from diverging the cluster").isFalse();

    assertThat(nodeA.getUser("alice")).as("the change the stale entry would have reverted is still there").isNotNull();
    assertThat(nodeB.getUser("alice")).isNotNull();
  }

  /** The same first-entry rule for the two documents issue #7373 added, which share {@code isSuperseded}. */
  @Test
  void theFirstReplicatedGroupAndTokenDocumentsAreInstalledOnEveryNode() {
    final JSONObject groups = new JSONObject(nodeA.getGroupsJsonPayload());
    assertThat(nodeB.applyReplicatedGroups(groups.toString(), "a-fingerprint-node-b-has-never-had")).isTrue();

    final JSONObject tokens = new JSONObject(nodeA.getApiTokensJsonPayload());
    assertThat(nodeB.applyReplicatedApiTokens(tokens.toString(), "a-fingerprint-node-b-has-never-had")).isTrue();
  }

  private static ServerSecurity node(final String name) {
    final String configPath = ROOT_PATH + "/" + name + "/config";
    assertThat(new File(configPath).mkdirs()).isTrue();

    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.SERVER_ROOT_PATH, ROOT_PATH + "/" + name);

    final FixtureServer server = new FixtureServer(configuration);
    final ServerSecurity security = new ServerSecurity(server, configuration, configPath);
    server.setSecurity(security);
    return security;
  }

  private static JSONObject user(final ServerSecurity security, final String name, final String password) {
    return new JSONObject()
        .put("name", name)
        .put("password", security.encodePassword(password))
        .put("databases", new JSONObject());
  }

  /** The list {@code security} holds, plus a new user: the payload a cluster-wide create submits. */
  private static String usersOf(final ServerSecurity security, final String newUser) {
    final JSONArray list = new JSONArray(security.getUsersJsonPayload());
    list.put(user(security, newUser, newUser + "-password"));
    return list.toString();
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
