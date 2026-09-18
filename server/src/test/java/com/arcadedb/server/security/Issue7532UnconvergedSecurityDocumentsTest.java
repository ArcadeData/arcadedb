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
 * Issue #7532: the signal the readiness gate reads, driven against a real {@link ServerSecurity} rather than
 * through the mock the control-plane test uses.
 * <p>
 * {@code unconvergedClusterSecurityDocuments()} answers the one question a node can answer by itself: is what it
 * enforces something the cluster installed, or is it its own config directory? It is read off
 * {@link ReplicatedSecurityFingerprintRepository}, which records a fingerprint exactly when an
 * {@code applyReplicated*} installs a document from the replicated log - so absence means the credentials,
 * groups and API tokens in force came off local disk, which is what a freshly admitted peer looks like until
 * the admission seed of issue #7521 lands, and what it stays like when that seed never lands at all.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue7532UnconvergedSecurityDocumentsTest {

  private static final String ROOT_PATH = "target/test-security-7532";

  private ServerSecurity node;

  @BeforeEach
  void setUp() {
    GlobalConfiguration.SERVER_SECURITY_SALT_ITERATIONS.setValue(1000);
    FileUtils.deleteRecursively(new File(ROOT_PATH));
    assertThat(new File(ROOT_PATH + "/a/config").mkdirs()).isTrue();
    node = open("a");
    node.createUser(user(node, "root", "rootpassword"));
  }

  @AfterEach
  void tearDown() {
    if (node != null)
      node.stopService();
    GlobalConfiguration.SERVER_SECURITY_SALT_ITERATIONS.reset();
    FileUtils.deleteRecursively(new File(ROOT_PATH));
  }

  /**
   * A node that has bootstrapped its own documents and received none from the cluster reports all three. This is
   * the joined-but-unseeded peer, and the state every node of a cluster that has never replicated a security
   * document is also in - which is why the readiness gate reading this is bounded and off by default.
   */
  @Test
  void aNodeThatHasInstalledNoReplicatedDocumentReportsAllThree() {
    assertThat(node.unconvergedClusterSecurityDocuments())
        .containsExactly("users", "groups", "API tokens");
  }

  /** Each document is independent: one landing must not make the other two look converged. */
  @Test
  void aDocumentDropsOutOfTheListWhenTheClusterInstallsIt() {
    node.applyReplicatedUsers(usersOf(node, "alice"), null);

    assertThat(node.unconvergedClusterSecurityDocuments()).containsExactly("groups", "API tokens");

    node.applyReplicatedGroups(node.getGroupsJsonPayload(), null);
    assertThat(node.unconvergedClusterSecurityDocuments()).containsExactly("API tokens");
  }

  /** All three landing - what a clean admission seed does - leaves nothing for the gate to hold readiness on. */
  @Test
  void aFullySeededNodeReportsNothing() {
    seedAllThree(node);

    assertThat(node.unconvergedClusterSecurityDocuments()).isEmpty();
  }

  /**
   * The answer survives a restart, because the fingerprints do: a converged node must not go back to looking
   * unseeded on every start, or the readiness gate would hold a healthy pod out of the Service at each rollout.
   */
  @Test
  void convergenceSurvivesARestart() {
    seedAllThree(node);

    node.stopService();
    node = open("a");
    node.loadUsers();

    assertThat(node.unconvergedClusterSecurityDocuments()).isEmpty();
  }

  private static void seedAllThree(final ServerSecurity security) {
    security.applyReplicatedUsers(usersOf(security, "alice"), null);
    security.applyReplicatedGroups(security.getGroupsJsonPayload(), null);
    security.applyReplicatedApiTokens(security.getApiTokensJsonPayload(), null);
  }

  private static ServerSecurity open(final String name) {
    final String configPath = ROOT_PATH + "/" + name + "/config";

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
