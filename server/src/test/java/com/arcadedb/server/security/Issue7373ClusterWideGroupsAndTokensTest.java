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
import com.arcadedb.server.ServerControlPlane;
import com.arcadedb.server.ServerControlPlane.NotFoundException;
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
 * Regression test for issue #7373: groups and API tokens were node-local while users were replicated.
 * <p>
 * A group created on node A did not exist on B or C. The user holding it authenticated everywhere - the user
 * document <i>is</i> replicated - and then resolved to no permissions on two nodes out of three, so the same
 * credentials got different authorization depending on which node answered. An API token minted on A
 * authenticated only against A, which behind a load balancer is an intermittent 401. Deleting either on one node
 * left it live on the others, and for a <b>revoked token</b> that is a security failure rather than a
 * consistency one.
 * <p>
 * The fix routes both through the cluster the way users already went: the mutation is submitted as a Raft entry
 * and installed by the apply, on every node including the one that served the request.
 * <p>
 * This test drives the two transports' convergence point - {@link ServerControlPlane}, which is what both
 * {@code PostGroupHandler}/{@code DeleteGroupHandler} and {@code ArcadeDbGrpcAdminService} call - against a
 * {@link RecordingHAPlugin} that captures each submitted document and then applies it, which is exactly what the
 * Raft state machine does on the leader before {@code submitAndWait} returns. The peers' half is the same
 * {@code applyReplicated*} call, driven directly.
 */
class Issue7373ClusterWideGroupsAndTokensTest {

  private static final String CONFIG_PATH = "target/test-security-7373";

  private FixtureServer      server;
  private ServerSecurity     security;
  private RecordingHAPlugin  ha;
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

  /** Puts the server in a cluster: from here on every security mutation must go through {@code ha}. */
  private void joinCluster() {
    ha = new RecordingHAPlugin(security);
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

  // -------------------------------------------------------------------------------------------
  // Groups: POST /api/v1/server/groups/{db}/{name} and the gRPC SaveGroup that calls the same method
  // -------------------------------------------------------------------------------------------

  @Test
  void savingAGroupInAClusterIsReplicatedAndNotJustWrittenLocally() {
    joinCluster();

    controlPlane.saveGroup("*", "reader", readerGroup());

    assertThat(ha.groupDocuments)
        .as("the group document must reach the cluster, not only this node's file")
        .hasSize(1);
    assertThat(new JSONObject(ha.groupDocuments.getFirst()).getJSONObject("databases").getJSONObject("*")
        .getJSONObject("groups").has("reader")).isTrue();

    // And the serving node has it too, because it applied the entry it submitted.
    assertThat(groupsOf("*").has("reader")).isTrue();
  }

  /** The peers' half: applying the replicated document installs the group on a node that never saw the request. */
  @Test
  void aPeerApplyingTheReplicatedGroupDocumentGetsTheGroup() {
    joinCluster();
    controlPlane.saveGroup("*", "reader", readerGroup());
    final String replicated = ha.groupDocuments.getFirst();

    final ServerSecurity peer = peerSecurity("peer-groups");
    try {
      assertThat(peer.groupsToJSON().getJSONObject("databases").getJSONObject("*").getJSONObject("groups")
          .has("reader")).as("the peer starts without the group").isFalse();

      peer.applyReplicatedGroups(replicated);

      assertThat(peer.groupsToJSON().getJSONObject("databases").getJSONObject("*").getJSONObject("groups")
          .has("reader")).isTrue();
      assertThat(new File(CONFIG_PATH + "/peer-groups", SecurityGroupFileRepository.FILE_NAME))
          .as("and writes it down, so a restart keeps it").isFile();
    } finally {
      peer.stopService();
    }
  }

  @Test
  void deletingAGroupInAClusterIsReplicated() {
    joinCluster();
    controlPlane.saveGroup("*", "reader", readerGroup());
    ha.groupDocuments.clear();

    controlPlane.deleteGroup("*", "reader");

    assertThat(ha.groupDocuments).hasSize(1);
    assertThat(new JSONObject(ha.groupDocuments.getFirst()).getJSONObject("databases").getJSONObject("*")
        .getJSONObject("groups").has("reader"))
        .as("a deletion must replicate the document WITHOUT the group")
        .isFalse();
    assertThat(groupsOf("*").has("reader")).isFalse();
  }

  /**
   * A deletion that finds nothing must not submit an entry. Replicating a no-op would burn a Raft round trip on
   * every 404, and - because the payload is the whole document - would also latch this node's copy onto peers.
   */
  @Test
  void deletingAGroupThatDoesNotExistReplicatesNothing() {
    joinCluster();

    assertThatThrownBy(() -> controlPlane.deleteGroup("*", "absent")).isInstanceOf(NotFoundException.class);

    assertThat(ha.groupDocuments).isEmpty();
  }

  /** Without HA the same call must still work, writing locally exactly as it did before the fix. */
  @Test
  void savingAGroupWithoutHAStillWritesLocally() {
    controlPlane.saveGroup("*", "reader", readerGroup());

    assertThat(groupsOf("*").has("reader")).isTrue();
    assertThat(new File(CONFIG_PATH, SecurityGroupFileRepository.FILE_NAME)).isFile();
  }

  // -------------------------------------------------------------------------------------------
  // API tokens: POST/DELETE /api/v1/server/api-tokens and the gRPC RPCs that call the same methods
  // -------------------------------------------------------------------------------------------

  @Test
  void mintingAnApiTokenInAClusterIsReplicatedAndAuthenticatesOnAPeer() {
    joinCluster();

    final JSONObject created = controlPlane.createApiToken("ci", "*", 0, new JSONObject());
    final String plaintext = created.getString("token");

    assertThat(ha.apiTokenDocuments).as("the token must reach the cluster").hasSize(1);
    assertThat(ha.apiTokenDocuments.getFirst())
        .as("the replicated document carries the hash, never the token material")
        .doesNotContain(plaintext)
        .contains(ApiTokenConfiguration.hashToken(plaintext));

    // The serving node holds it, because it applied the entry it submitted.
    assertThat(security.getApiTokenConfiguration().getToken(plaintext)).isNotNull();

    final ServerSecurity peer = peerSecurity("peer-tokens");
    try {
      assertThat(peer.getApiTokenConfiguration().getToken(plaintext))
          .as("the peer starts without the token").isNull();

      peer.applyReplicatedApiTokens(ha.apiTokenDocuments.getFirst());

      assertThat(peer.getApiTokenConfiguration().getToken(plaintext))
          .as("after the entry is applied the same token authenticates on the peer too").isNotNull();
    } finally {
      peer.stopService();
    }
  }

  @Test
  void revokingAnApiTokenInAClusterIsReplicatedAndTakesEffectOnAPeer() {
    joinCluster();
    final JSONObject created = controlPlane.createApiToken("ci", "*", 0, new JSONObject());
    final String plaintext = created.getString("token");
    final String hash = created.getString("tokenHash");

    final ServerSecurity peer = peerSecurity("peer-revoke");
    try {
      peer.applyReplicatedApiTokens(ha.apiTokenDocuments.getFirst());
      assertThat(peer.getApiTokenConfiguration().getToken(plaintext)).isNotNull();

      ha.apiTokenDocuments.clear();
      controlPlane.deleteApiToken(hash);

      assertThat(ha.apiTokenDocuments).as("a revocation must replicate").hasSize(1);
      assertThat(security.getApiTokenConfiguration().getToken(plaintext))
          .as("gone on the node that revoked it").isNull();

      peer.applyReplicatedApiTokens(ha.apiTokenDocuments.getFirst());
      assertThat(peer.getApiTokenConfiguration().getToken(plaintext))
          .as("and gone on the peer: a revocation that applies to one third of the cluster is not a revocation")
          .isNull();
    } finally {
      peer.stopService();
    }
  }

  /** As for groups: a revocation of a token nobody holds must not submit an entry. */
  @Test
  void revokingAnUnknownApiTokenReplicatesNothing() {
    joinCluster();

    assertThatThrownBy(() -> controlPlane.deleteApiToken(ApiTokenConfiguration.hashToken("at-nothing")))
        .isInstanceOf(NotFoundException.class);

    assertThat(ha.apiTokenDocuments).isEmpty();
  }

  /**
   * A mint whose Raft entry never commits must leave no token behind on the node that served the request -
   * otherwise the caller gets an error and one node out of three still honours the credential it was handed.
   */
  @Test
  void aMintWhoseEntryFailsLeavesNoTokenOnTheServingNode() {
    ha = new RecordingHAPlugin(security) {
      @Override
      public void replicateSecurityApiTokens(final String apiTokensJson) {
        throw new IllegalStateException("consensus lost");
      }
    };
    server.setHA(ha);

    assertThatThrownBy(() -> controlPlane.createApiToken("ci", "*", 0, new JSONObject()))
        .isInstanceOf(IllegalStateException.class);

    assertThat(security.getApiTokenConfiguration().listTokens()).isEmpty();
    assertThat(new File(CONFIG_PATH, ApiTokenConfiguration.FILE_NAME))
        .as("and nothing reached the local file either").doesNotExist();
  }

  /** Without HA the token store behaves exactly as it did before the fix. */
  @Test
  void mintingAnApiTokenWithoutHAStillWritesLocally() {
    final JSONObject created = controlPlane.createApiToken("ci", "*", 0, new JSONObject());

    assertThat(security.getApiTokenConfiguration().getToken(created.getString("token"))).isNotNull();
    assertThat(new File(CONFIG_PATH, ApiTokenConfiguration.FILE_NAME)).isFile();
  }

  // -------------------------------------------------------------------------------------------
  // Peer seeding and payload integrity
  // -------------------------------------------------------------------------------------------

  /**
   * What {@code PostAddPeerHandler} sends to a newly-joined peer. Snapshot install covers neither file, so a peer
   * that joins between two mutations would otherwise run on whatever its own config directory holds.
   */
  @Test
  void theSeedPayloadsRoundTripIntoAFreshNode() {
    controlPlane.saveGroup("*", "reader", readerGroup());
    final JSONObject created = controlPlane.createApiToken("ci", "*", 0, new JSONObject());

    final String groupsSeed = security.getGroupsJsonPayload();
    final String tokensSeed = security.getApiTokensJsonPayload();
    assertThat(tokensSeed).doesNotContain(created.getString("token"));

    final ServerSecurity joiner = peerSecurity("joiner");
    try {
      joiner.applyReplicatedGroups(groupsSeed);
      joiner.applyReplicatedApiTokens(tokensSeed);

      assertThat(joiner.groupsToJSON().getJSONObject("databases").getJSONObject("*").getJSONObject("groups")
          .has("reader")).isTrue();
      assertThat(joiner.getApiTokenConfiguration().getToken(created.getString("token"))).isNotNull();
    } finally {
      joiner.stopService();
    }
  }

  /**
   * A document this node cannot read is not "the disk is full": it is a committed entry the peers applied and
   * this one cannot, and it must reach the node-wide halt rather than be installed half-way.
   */
  @Test
  void aGroupDocumentWithoutDatabasesIsRefusedAndChangesNothing() {
    controlPlane.saveGroup("*", "reader", readerGroup());

    assertThatThrownBy(() -> security.applyReplicatedGroups(new JSONObject().put("version", 2).toString()))
        .isInstanceOf(ServerSecurityException.class);

    assertThat(groupsOf("*").has("reader")).as("the groups already in force are untouched").isTrue();
  }

  /**
   * A versionless group document installs cleanly and then disappears on the next restart:
   * {@code SecurityGroupFileRepository.load()} discards a file with no {@code version} and falls back to
   * {@code createDefault()}, which widens every database back to the default permissions. Refusing the document
   * is the only outcome that is not a silent permission change at some later restart.
   */
  @Test
  void aGroupDocumentWithoutAVersionIsRefused() {
    controlPlane.saveGroup("*", "reader", readerGroup());
    final JSONObject versionless = security.groupsToJSON();
    versionless.remove("version");

    assertThatThrownBy(() -> security.applyReplicatedGroups(versionless.toString()))
        .isInstanceOf(ServerSecurityException.class)
        .hasMessageContaining("version");

    assertThat(groupsOf("*").has("reader")).isTrue();
  }

  /** A {@code databases} key that is not an object is as unusable as a missing one, and must be caught too. */
  @Test
  void aGroupDocumentWhoseDatabasesSectionIsNotAnObjectIsRefused() {
    controlPlane.saveGroup("*", "reader", readerGroup());

    final String malformed = new JSONObject().put("version", 2).put("databases", "everything").toString();

    assertThatThrownBy(() -> security.applyReplicatedGroups(malformed))
        .isInstanceOf(ServerSecurityException.class);

    assertThat(groupsOf("*").has("reader")).isTrue();
  }

  /** A save is never a delete in disguise: a null definition is rejected rather than silently removing the group. */
  @Test
  void savingANullGroupDefinitionIsRejected() {
    joinCluster();

    assertThatThrownBy(() -> security.saveGroupClusterWide("*", "reader", null))
        .isInstanceOf(IllegalArgumentException.class);

    assertThat(ha.groupDocuments).isEmpty();
  }

  @Test
  void anApiTokenDocumentThatCannotBeReadChangesNothing() {
    final JSONObject created = controlPlane.createApiToken("ci", "*", 0, new JSONObject());

    // An entry with no "tokenHash": it cannot be indexed, and the swap must not happen.
    final String malformed = new JSONObject()
        .put("version", 1)
        .put("tokens", new JSONArray().put(new JSONObject().put("name", "broken")))
        .toString();

    assertThatThrownBy(() -> security.applyReplicatedApiTokens(malformed)).isInstanceOf(Exception.class);

    assertThat(security.getApiTokenConfiguration().getToken(created.getString("token")))
        .as("the token set already in force is untouched").isNotNull();
  }

  private ServerSecurity peerSecurity(final String subDirectory) {
    final File dir = new File(CONFIG_PATH, subDirectory);
    assertThat(dir.mkdirs()).isTrue();
    return new ServerSecurity(null, new ContextConfiguration(), dir.getPath());
  }

  /**
   * Stands in for the Raft round trip: records the submitted document and applies it, which is what the state
   * machine does on the leader before {@code submitAndWait} returns.
   */
  private static class RecordingHAPlugin implements HAServerPlugin {
    private final ServerSecurity security;
    final         List<String>   groupDocuments    = new ArrayList<>();
    final         List<String>   apiTokenDocuments = new ArrayList<>();

    private RecordingHAPlugin(final ServerSecurity security) {
      this.security = security;
    }

    @Override
    public void replicateSecurityGroups(final String groupsJson) {
      groupDocuments.add(groupsJson);
      security.applyReplicatedGroups(groupsJson);
    }

    @Override
    public void replicateSecurityApiTokens(final String apiTokensJson) {
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
