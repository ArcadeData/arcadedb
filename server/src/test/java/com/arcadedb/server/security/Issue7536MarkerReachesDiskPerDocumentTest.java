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
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The other half of issue #7536: the three {@code applyReplicated*} methods are the only writers of the
 * replicated-security marker, and the discard that #7536 adds to {@code ReplicatedSecurityFingerprintRepository}
 * is worth nothing if one of them never reaches it, or reaches it with the wrong document kind.
 * <p>
 * {@code Issue7693FirstReplicatedSecurityEntryTest} pins that a restarted node still REFUSES a stale precondition,
 * which proves the marker survives; it does not pin WHICH value reached the disk, so a call site recording the
 * fingerprint of a different document than the one it installed would pass it. These tests read the file back and
 * compare it with the document actually in force, per kind.
 * <p>
 * The last test pins the direction #7536 trades into: a node that comes up with NO marker installs and rejoins
 * its peers' baseline on that entry, where a node with a STALE one refuses and never rejoins. The residual - the
 * first entry after such a restart being itself a superseded one - is issue #7752.
 *
 * @see Issue7536StaleFingerprintMarkerTest for the discard itself
 */
class Issue7536MarkerReachesDiskPerDocumentTest {

  private static final String ROOT_PATH = "target/test-security-7536";

  private ServerSecurity nodeA;
  private ServerSecurity nodeB;

  @BeforeEach
  void setUp() {
    GlobalConfiguration.SERVER_SECURITY_SALT_ITERATIONS.setValue(1000);
    FileUtils.deleteRecursively(new File(ROOT_PATH));
    nodeA = node("a");
    nodeB = node("b");
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

  @Test
  void applyingAReplicatedUserListRecordsThatVeryListOnDisk() throws IOException {
    final JSONArray list = new JSONArray(nodeA.getUsersJsonPayload());
    list.put(user(nodeA, "alice", "alice-password"));
    nodeB.applyReplicatedUsers(list.toString(), null);

    assertThat(markerOf("b", ReplicatedSecurityFingerprintRepository.USERS))
        .as("the baseline every peer compares against has to be the list this node just installed")
        .isEqualTo(nodeB.usersFingerprint());
  }

  @Test
  void applyingAReplicatedGroupDocumentRecordsThatVeryDocumentOnDisk() throws IOException {
    nodeB.applyReplicatedGroups(nodeA.getGroupsJsonPayload(), null);

    assertThat(markerOf("b", ReplicatedSecurityFingerprintRepository.GROUPS))
        .isEqualTo(nodeB.groupsFingerprint());
  }

  @Test
  void applyingAReplicatedApiTokenDocumentRecordsThatVeryDocumentOnDisk() throws IOException {
    nodeB.applyReplicatedApiTokens(nodeA.getApiTokensJsonPayload(), null);

    assertThat(markerOf("b", ReplicatedSecurityFingerprintRepository.API_TOKENS))
        .isEqualTo(nodeB.apiTokensFingerprint());
  }

  /** One kind landing must not record another: a users entry cannot make the group document judgeable. */
  @Test
  void aUsersEntryRecordsOnlyTheUsersKind() throws IOException {
    nodeB.applyReplicatedUsers(nodeA.getUsersJsonPayload(), null);

    assertThat(markerOf("b", ReplicatedSecurityFingerprintRepository.GROUPS)).isNull();
    assertThat(markerOf("b", ReplicatedSecurityFingerprintRepository.API_TOKENS)).isNull();
  }

  /**
   * Why losing the marker is the direction to fail in. The node comes back with none - the state a failed write
   * now leaves behind - and installs the entry its peers install, ending up on their baseline. With the stale
   * marker that used to survive, it would have refused this entry while every peer accepted it, and gone on
   * refusing: nothing it refuses can move its baseline forward.
   */
  @Test
  void aNodeThatLostItsMarkerRejoinsTheClusterBaselineOnTheNextEntry() throws IOException {
    final String firstEntry = usersWith(nodeA, "alice");
    final String firstPrecondition = nodeA.usersFingerprint();
    nodeA.applyReplicatedUsers(firstEntry, firstPrecondition);
    nodeB.applyReplicatedUsers(firstEntry, firstPrecondition);

    // Exactly what a discarded marker leaves on disk, as opposed to the previous value staying there.
    Files.deleteIfExists(configOf("b").resolve(ReplicatedSecurityFingerprintRepository.FILE_NAME));
    nodeB = restart(nodeB, "b");

    final String secondEntry = usersWith(nodeA, "bob");
    final String secondPrecondition = nodeA.usersFingerprint();
    assertThat(nodeA.applyReplicatedUsers(secondEntry, secondPrecondition))
        .as("the converged peer installs it").isTrue();
    assertThat(nodeB.applyReplicatedUsers(secondEntry, secondPrecondition))
        .as("and so does the node that lost its marker, rather than refusing what its peers accepted").isTrue();

    assertThat(nodeB.usersFingerprint()).as("which puts both nodes back on one document")
        .isEqualTo(nodeA.usersFingerprint());
    assertThat(markerOf("b", ReplicatedSecurityFingerprintRepository.USERS))
        .as("and the node can judge again from the entry it just applied").isEqualTo(nodeA.usersFingerprint());
  }

  private static String markerOf(final String name, final String documentKind) throws IOException {
    final Path file = configOf(name).resolve(ReplicatedSecurityFingerprintRepository.FILE_NAME);
    if (!Files.exists(file))
      return null;
    return new JSONObject(Files.readString(file, StandardCharsets.UTF_8)).getString(documentKind, null);
  }

  private static Path configOf(final String name) {
    return Path.of(ROOT_PATH, name, "config");
  }

  private static ServerSecurity restart(final ServerSecurity security, final String name) {
    security.stopService();
    final ServerSecurity restarted = open(name);
    restarted.loadUsers();
    return restarted;
  }

  private static ServerSecurity node(final String name) {
    assertThat(new File(ROOT_PATH + "/" + name + "/config").mkdirs()).isTrue();
    return open(name);
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

  private static String usersWith(final ServerSecurity security, final String newUser) {
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
