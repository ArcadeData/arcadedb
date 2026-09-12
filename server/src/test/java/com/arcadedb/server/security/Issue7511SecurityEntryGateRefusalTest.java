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
import com.arcadedb.server.ClusterCapabilityNotReadyException;
import com.arcadedb.server.HAServerPlugin;
import com.arcadedb.server.ServerControlPlane;
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
 * Issue #7511, the half that lives above the HA plugin: what an operator-facing entry point does when the
 * cluster-capability interlock refuses to replicate a group or API-token change.
 * <p>
 * The interlock itself is pinned in {@code Issue7511SecurityEntryCapabilityGateTest}, in the module that owns it.
 * What is asserted here is everything that has to be true of the caller for the refusal to be SAFE rather than
 * merely loud: that the refusal reaches the transport unchanged (it is the exception both transports map to a
 * status), and that no entry point leaves half a change behind on the node that served the request. A mint that
 * installed the token locally before the refusal, or a revocation that removed it, would turn one node into the
 * divergence the gate exists to prevent - by a different route.
 * <p>
 * The fixture is {@code Issue7373ClusterWideGroupsAndTokensTest}'s, with the plugin replaced by one that refuses
 * the way {@code RaftHAPlugin} does when a peer has not advertised the capability.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7511SecurityEntryGateRefusalTest {

  private static final String CONFIG_PATH = "target/test-security-7511";
  private static final String REFUSAL     = "peer(s) [arcadedb2] have not advertised the 'security-groups-entry' "
      + "capability";

  private FixtureServer      server;
  private ServerSecurity     security;
  private RefusingHAPlugin   ha;
  private ServerControlPlane controlPlane;

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
    controlPlane = new ServerControlPlane(server);
  }

  @AfterEach
  void tearDown() {
    if (security != null)
      security.stopService();
    GlobalConfiguration.SERVER_SECURITY_SALT_ITERATIONS.reset();
    FileUtils.deleteRecursively(new File(CONFIG_PATH));
  }

  /** Joins a cluster whose peers are all upgraded: every replication is accepted. */
  private void joinReadyCluster() {
    ha = new RefusingHAPlugin(security, false);
    server.setHA(ha);
  }

  /** Joins a cluster with one node too old to decode either entry type: every replication is refused. */
  private void joinMixedVersionCluster() {
    ha = new RefusingHAPlugin(security, true);
    server.setHA(ha);
  }

  private static JSONObject readerGroup() {
    return new JSONObject()
        .put("resultSetLimit", -1L)
        .put("readTimeout", -1L)
        .put("access", new JSONArray())
        .put("types", new JSONObject());
  }

  private JSONObject groupsOf(final String database) {
    return security.groupsToJSON().getJSONObject("databases").getJSONObject(database).getJSONObject("groups");
  }

  // -------------------------------------------------------------------------------------------------------
  // Groups: POST /api/v1/server/groups and the gRPC SaveGroup that calls the same method
  // -------------------------------------------------------------------------------------------------------

  @Test
  void savingAGroupOnAMixedVersionClusterIsRefusedAndChangesNothingLocally() {
    joinMixedVersionCluster();

    assertThatThrownBy(() -> controlPlane.saveGroup("*", "reader", readerGroup()))
        .isInstanceOf(ClusterCapabilityNotReadyException.class)
        .hasMessageContaining(REFUSAL);

    assertThat(groupsOf("*").has("reader"))
        .as("the group must not exist on the node that served the refused request either")
        .isFalse();
    assertThat(ha.groupDocuments).isEmpty();
  }

  @Test
  void deletingAGroupOnAMixedVersionClusterIsRefusedAndLeavesTheGroupInPlace() {
    joinReadyCluster();
    controlPlane.saveGroup("*", "reader", readerGroup());
    assertThat(groupsOf("*").has("reader")).isTrue();

    joinMixedVersionCluster();

    assertThatThrownBy(() -> controlPlane.deleteGroup("*", "reader"))
        .isInstanceOf(ClusterCapabilityNotReadyException.class);

    assertThat(groupsOf("*").has("reader"))
        .as("a half-applied delete would leave this node authorizing differently from every other")
        .isTrue();
  }

  @Test
  void savingAGroupOnAFullyUpgradedClusterStillWorks() {
    joinReadyCluster();

    controlPlane.saveGroup("*", "reader", readerGroup());

    assertThat(groupsOf("*").has("reader")).isTrue();
    assertThat(ha.groupDocuments).hasSize(1);
  }

  // -------------------------------------------------------------------------------------------------------
  // API tokens: POST / DELETE /api/v1/server/api-tokens and their gRPC equivalents
  // -------------------------------------------------------------------------------------------------------

  @Test
  void mintingAnApiTokenOnAMixedVersionClusterIsRefusedAndInstallsNothing() {
    joinMixedVersionCluster();

    assertThatThrownBy(() -> controlPlane.createApiToken("ci", "*", 0, new JSONObject()))
        .isInstanceOf(ClusterCapabilityNotReadyException.class);

    assertThat(controlPlane.listApiTokens().toList())
        .as("a token that exists on one node only is the intermittent 401 issue #7373 set out to remove")
        .isEmpty();
    assertThat(ha.apiTokenDocuments).isEmpty();
  }

  /**
   * The case that matters most. A revocation whose entry would halt the nodes still serving the token has not
   * revoked anything, so it must be refused - and the token must stay usable here rather than be dropped locally,
   * or this node starts disagreeing with the rest of the cluster about a live credential.
   */
  @Test
  void revokingAnApiTokenOnAMixedVersionClusterIsRefusedAndTheTokenStaysUniformlyLive() {
    joinReadyCluster();
    final JSONObject minted = controlPlane.createApiToken("ci", "*", 0, new JSONObject());
    final String tokenHash = minted.getString("tokenHash");

    joinMixedVersionCluster();

    assertThatThrownBy(() -> controlPlane.deleteApiToken(tokenHash))
        .isInstanceOf(ClusterCapabilityNotReadyException.class);

    assertThat(controlPlane.listApiTokens().toList())
        .as("the revocation reached no node, so it must not have reached this one")
        .hasSize(1);
  }

  // -------------------------------------------------------------------------------------------------------
  // addPeer: POST /api/v1/cluster/addPeer seeds all three documents
  // -------------------------------------------------------------------------------------------------------

  /**
   * {@code seedSecurityStateClusterWide} is best-effort per document by design, so a refused group seed must be
   * REPORTED rather than thrown - and must not take the users seed down with it, which is the one that predates
   * the entry types and can always be replicated.
   */
  @Test
  void seedingAJoiningPeerReportsTheRefusedDocumentsAndStillSeedsTheUsers() {
    joinMixedVersionCluster();

    final List<String> failed = security.seedSecurityStateClusterWide();

    assertThat(failed).containsExactlyInAnyOrder("groups", "API tokens");
    assertThat(ha.userDocuments)
        .as("SECURITY_USERS_ENTRY predates every build in a supported rolling upgrade and is never gated")
        .hasSize(1);
  }

  @Test
  void seedingAJoiningPeerOnAFullyUpgradedClusterReportsNothingFailed() {
    joinReadyCluster();

    assertThat(security.seedSecurityStateClusterWide()).isEmpty();
    assertThat(ha.groupDocuments).hasSize(1);
    assertThat(ha.apiTokenDocuments).hasSize(1);
  }

  // -------------------------------------------------------------------------------------------------------

  /**
   * Stands in for {@code RaftHAPlugin} with the #7511 interlock in front of it: when {@code refusing}, the two
   * gated hooks throw before anything is submitted, exactly as
   * {@code SecurityEntryCapabilityGate.requireEveryPeerCanDecode} makes them. The users hook is never gated.
   */
  private static class RefusingHAPlugin implements HAServerPlugin {
    private final ServerSecurity security;
    private final boolean        refusing;
    final         List<String>   userDocuments     = new ArrayList<>();
    final         List<String>   groupDocuments    = new ArrayList<>();
    final         List<String>   apiTokenDocuments = new ArrayList<>();

    private RefusingHAPlugin(final ServerSecurity security, final boolean refusing) {
      this.security = security;
      this.refusing = refusing;
    }

    @Override
    public void replicateSecurityUsers(final String usersJsonArray) {
      userDocuments.add(usersJsonArray);
      security.applyReplicatedUsers(usersJsonArray);
    }

    @Override
    public void replicateSecurityGroups(final String groupsJson) {
      if (refusing)
        throw new ClusterCapabilityNotReadyException("Refusing to replicate the group document: " + REFUSAL,
            "security-groups-entry", List.of("arcadedb2"));
      groupDocuments.add(groupsJson);
      security.applyReplicatedGroups(groupsJson);
    }

    @Override
    public void replicateSecurityApiTokens(final String apiTokensJson) {
      if (refusing)
        throw new ClusterCapabilityNotReadyException("Refusing to replicate the API-token document: peer(s) "
            + "[arcadedb2] have not advertised the 'security-api-tokens-entry' capability",
            "security-api-tokens-entry", List.of("arcadedb2"));
      apiTokenDocuments.add(apiTokensJson);
      security.applyReplicatedApiTokens(apiTokensJson);
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

  /** An {@link ArcadeDBServer} that is never started, so the security store can be supplied by the fixture. */
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
