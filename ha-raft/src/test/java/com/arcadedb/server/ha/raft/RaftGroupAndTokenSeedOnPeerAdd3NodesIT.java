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
package com.arcadedb.server.ha.raft;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.security.ApiTokenConfiguration;
import com.arcadedb.server.security.ServerSecurity;
import com.arcadedb.utility.FileUtils;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7524: the group and API-token halves of the peer-add seed, driven through a <b>real</b> three-node Raft
 * cluster.
 * <p>
 * Issue #7373 made {@code addPeer} seed the group document and the API-token document beside the user list, and
 * #7513 moved all three reads under the {@code ServerSecurity} monitor. Both were covered only against a fake
 * {@code HAServerPlugin} in {@code Issue7373ClusterWideGroupsAndTokensTest}: nothing drove the seed over real
 * Raft entries, applied by a real state machine, on a node that really held a stale copy. The only multi-node
 * tests that existed - {@code RaftUserSeedOnPeerAdd3NodesIT} and {@code UserSeedOnPeerAddScenarioIT} - asserted
 * the <b>users</b> half alone.
 * <p>
 * Each test puts a node into the state a newly-admitted peer is in - a cluster member holding its own,
 * out-of-date copy of a document that snapshot install does not carry, because all three live under
 * {@code <server-root>/config/} rather than in the database directory - and then runs
 * {@link ServerSecurity#seedSecurityStateClusterWide(long)}, which is the call
 * {@link PostAddPeerHandler} makes after a successful {@code addPeer}.
 * <p>
 * {@link #aTokenRevokedBeforeTheJoinDoesNotAuthenticateOnTheNewPeer} is the one that matters. A seed carries a
 * WHOLE document, so the direction that can go wrong is not a grant that fails to arrive - that is an
 * inconvenience - but a revocation that is undone: a peer re-admitted after time out of the cluster holds a token
 * revoked since, and a seed that restored it would put a withdrawn credential back into service on every node.
 * <p>
 * The response shape for a seed that does not commit (issue #7521's {@code error}/{@code detail}/
 * {@code failedSeeds} and the 503 that replaced the original {@code warning} field) is pinned by
 * {@code Issue7521AddPeerSeedFailureIsReportedTest}, which needs no cluster.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class RaftGroupAndTokenSeedOnPeerAdd3NodesIT extends BaseRaftHATest {

  private static final String GROUP = "seeded-readers";

  RaftGroupAndTokenSeedOnPeerAdd3NodesIT() {
    FileUtils.deleteRecursively(new File("./target/config"));
    FileUtils.deleteRecursively(new File("./target/databases"));
    GlobalConfiguration.SERVER_DATABASE_DIRECTORY.setValue("./target/databases");
    GlobalConfiguration.SERVER_ROOT_PATH.setValue("./target");
  }

  @AfterEach
  @Override
  public void endTest() {
    super.endTest();
    FileUtils.deleteRecursively(new File("./target/config"));
    FileUtils.deleteRecursively(new File("./target/databases"));
  }

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Override
  protected boolean isCreateDatabases() {
    return false;
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_QUORUM, "majority");
  }

  @Override
  protected void checkDatabasesAreIdentical() {
    // No test database is created (isCreateDatabases=false); groups and API tokens are server-wide state.
  }

  /**
   * Coverage item 1: a peer whose own {@code server-groups.json} is stale answers with the CLUSTER's group
   * definitions after the seed, not with whatever it held.
   */
  @Test
  void aGroupDefinedBeforeTheJoinIsSeededToTheNewPeer() throws Exception {
    final int leader = findLeaderIndex();
    assertThat(leader).isGreaterThanOrEqualTo(0);
    final int target = (leader + 1) % getServerCount();

    security(leader).saveGroupClusterWide("*", GROUP, readerGroup());
    awaitOn(target, () -> hasGroup(target, GROUP));

    // The state a peer that joined between two mutations is in: a member of the cluster, holding its own,
    // older copy of the document. Installed the same way the state machine installs one, so this is the peer's
    // real document and not a file the test edited behind its back.
    security(target).applyReplicatedGroups(documentWithoutTheGroup());
    assertThat(hasGroup(target, GROUP))
        .as("the target must really be stale, or the seed below would have nothing to fix").isFalse();

    assertThat(addPeerVia(leader, target)).as("the add-peer route must answer 200 on a healthy cluster")
        .isEqualTo(200);

    awaitOn(target, () -> hasGroup(target, GROUP));
    assertThat(groupAccessOn(target, GROUP))
        .as("the peer serves the cluster's permissions, not its own stale ones")
        .contains("readRecord");
  }

  /**
   * Coverage item 2: an API token minted on the leader BEFORE the peer joined authenticates against that peer
   * once it has been seeded. Behind a load balancer, the failure this prevents is an intermittent 401 with no
   * pattern an operator can see.
   */
  @Test
  void anApiTokenMintedBeforeTheJoinAuthenticatesOnTheNewPeer() throws Exception {
    final int leader = findLeaderIndex();
    assertThat(leader).isGreaterThanOrEqualTo(0);
    final int target = (leader + 1) % getServerCount();

    final JSONObject created = security(leader).createApiTokenClusterWide("ci-7524", "*", 0, new JSONObject());
    final String plaintext = created.getString("token");
    awaitOn(target, () -> hasToken(target, plaintext));

    security(target).applyReplicatedApiTokens(emptyTokenDocument());
    assertThat(hasToken(target, plaintext))
        .as("the target must really be without the token, or the seed below proves nothing").isFalse();

    assertThat(addPeerVia(leader, target)).isEqualTo(200);

    awaitOn(target, () -> hasToken(target, plaintext));
  }

  /**
   * Coverage item 3, the security direction. A token revoked before the peer joined must NOT come back to life on
   * it: the seed carries the whole token document, so a read taken before the revocation and submitted after it
   * would restore a withdrawn credential on every node in the cluster. #7513 closed that window by reading and
   * submitting under the {@code ServerSecurity} monitor; this is the assertion that says so over real Raft.
   */
  @Test
  void aTokenRevokedBeforeTheJoinDoesNotAuthenticateOnTheNewPeer() throws Exception {
    final int leader = findLeaderIndex();
    assertThat(leader).isGreaterThanOrEqualTo(0);
    final int target = (leader + 1) % getServerCount();

    final JSONObject created = security(leader).createApiTokenClusterWide("revoked-7524", "*", 0, new JSONObject());
    final String plaintext = created.getString("token");
    final String hash = created.getString("tokenHash");
    awaitOn(target, () -> hasToken(target, plaintext));

    // Kept for the assertion below: the document as it stood WHILE the token was valid. This is what a peer that
    // was out of the cluster across the revocation still holds.
    final String documentBeforeTheRevocation = security(leader).getApiTokensJsonPayload();

    assertThat(security(leader).deleteApiTokenClusterWide(hash)).isTrue();
    awaitOn(target, () -> !hasToken(target, plaintext));

    security(target).applyReplicatedApiTokens(documentBeforeTheRevocation);
    assertThat(hasToken(target, plaintext))
        .as("the target now holds the revoked token, which is the state the seed has to correct").isTrue();

    assertThat(addPeerVia(leader, target)).isEqualTo(200);

    awaitOn(target, () -> !hasToken(target, plaintext));
    assertThat(hasToken(leader, plaintext))
        .as("and the seed did not resurrect it on the leader either").isFalse();
  }

  // ---------------------------------------------------------------------------------------------------------

  /**
   * {@code POST /api/v1/cluster/peer} on {@code leader}, naming {@code target} - the whole route, over HTTP,
   * through the real {@link PostAddPeerHandler}.
   * <p>
   * Driven through the endpoint rather than by calling {@code seedSecurityStateClusterWide} directly, because the
   * coupling under test is <b>that the add-peer route seeds at all</b>: a test that calls the seed itself keeps
   * passing if the handler stops calling it, while a peer admitted afterwards silently keeps its stale groups and
   * tokens, which is the bug this IT exists for.
   * <p>
   * {@code target} is already a committed member, so the membership half is the idempotent no-op
   * {@code RaftClusterManager.buildAddArgs} documents - which is what makes this runnable on a fixed-size
   * in-process cluster, where there is no spare node to admit. The seed that follows it is not conditional on the
   * configuration having changed, so it runs exactly as it does for a genuine admission; that is the half being
   * asserted. The 200 also pins the other direction, since a seed that failed to commit is answered 503 with
   * {@code failedSeeds} (issue #7521).
   */
  private int addPeerVia(final int leader, final int target) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URI(
        "http://127.0.0.1:" + getServer(leader).getHttpServer().getPort() + "/api/v1/cluster/peer")
        .toURL().openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization", "Basic " + Base64.getEncoder().encodeToString(
        ("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)));
    try {
      formatPayload(connection, new JSONObject()
          .put("peerId", peerIdForIndex(target))
          .put("address", raftAddressOf(target)));
      connection.connect();
      return connection.getResponseCode();
    } finally {
      connection.disconnect();
    }
  }

  /**
   * The Raft address the CLUSTER knows {@code serverIndex} by, read from the live configuration rather than
   * rebuilt from the test's port arithmetic - so the payload names the peer the same way every other member
   * does, which is what makes the add the idempotent no-op this helper relies on.
   */
  private String raftAddressOf(final int serverIndex) {
    final RaftPeerId peerId = RaftPeerId.valueOf(peerIdForIndex(serverIndex));
    for (final RaftPeer peer : getRaftPlugin(serverIndex).getRaftHAServer().getLivePeers())
      if (peerId.equals(peer.getId()))
        return peer.getAddress();
    throw new IllegalStateException("Server " + serverIndex + " is not in the live Raft configuration");
  }

  private ServerSecurity security(final int serverIndex) {
    return getServer(serverIndex).getSecurity();
  }

  private boolean hasGroup(final int serverIndex, final String name) {
    final JSONObject databases = security(serverIndex).groupsToJSON().getJSONObject("databases");
    return databases.has("*") && databases.getJSONObject("*").getJSONObject("groups").has(name);
  }

  private JSONArray groupAccessOn(final int serverIndex, final String name) {
    return security(serverIndex).groupsToJSON().getJSONObject("databases").getJSONObject("*")
        .getJSONObject("groups").getJSONObject(name).getJSONArray("access");
  }

  private boolean hasToken(final int serverIndex, final String plaintext) {
    return security(serverIndex).getApiTokenConfiguration().getToken(plaintext) != null;
  }

  private static void awaitOn(final int serverIndex, final Callable<Boolean> condition) {
    Awaitility.await("server " + serverIndex)
        .atMost(30, TimeUnit.SECONDS).pollInterval(200, TimeUnit.MILLISECONDS)
        .until(condition);
  }

  private static JSONObject readerGroup() {
    return new JSONObject()
        .put("access", new JSONArray().put("readRecord"))
        .put("resultSetLimit", -1L)
        .put("readTimeout", -1L)
        .put("types", new JSONObject().put("*", new JSONObject()
            .put("access", new JSONArray().put("readRecord"))));
  }

  /** A valid group document that simply does not contain {@link #GROUP}: the peer's own, older copy. */
  private static String documentWithoutTheGroup() {
    return new JSONObject()
        .put("version", ServerSecurity.LATEST_VERSION)
        .put("databases", new JSONObject().put("*", new JSONObject().put("groups", new JSONObject())))
        .toString();
  }

  /** A valid API-token document holding no tokens - what a node that has never seen one has. */
  private static String emptyTokenDocument() {
    return new JSONObject()
        .put("version", 1)
        .put("tokens", new JSONArray())
        .toString();
  }
}
