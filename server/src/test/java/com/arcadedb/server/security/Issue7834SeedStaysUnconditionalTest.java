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
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
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

/**
 * Why a security seed is submitted with NO compare-and-set precondition, and must stay that way (issue #7834).
 * <p>
 * Issue #7834 observes that the seed paths call the single-argument {@code replicateSecurity*} forms, so issue
 * #7509's concurrency check does not cover them, and a revocation committing between a seeder's read and its
 * submit can be undone by the whole document the seed carries. Adding the precondition is the obvious
 * hardening. It does not work, and it fails in exactly the case a seed exists for - which is worth a test,
 * because the next person to read that sentence in the issue will reach for the same fix.
 * <p>
 * {@code ServerSecurity.isSuperseded} judges a precondition against the fingerprint of the last replicated
 * document THE APPLYING NODE installed, because that is the only value that is identical on every node (issue
 * #7693). A peer whose baseline has drifted - one that missed entries, joined late, or caught up by a snapshot
 * install, which is the whole population a seed is aimed at - does not hold that value, so it would refuse the
 * very seed sent to repair it. Weakening the comparison so the stale peer accepts while a caught-up peer
 * refuses is worse still: that is a divergence, with the revoked credential surviving on the node that was
 * already behind.
 * <p>
 * What issue #7834 actually asks for - one seeder per admission instead of two, on two nodes, under two
 * monitors - is pinned in {@code Issue7834SingleSecuritySeederTest}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7834SeedStaysUnconditionalTest {

  private static final String CONFIG_PATH = "target/test-security-7834";

  private FixtureServer      server;
  private ServerSecurity     security;
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

  // -------------------------------------------------------------------------------------------
  // Why the seed carries no precondition
  // -------------------------------------------------------------------------------------------

  /**
   * The seed installs unconditionally. Stated as a test rather than left to the call site, because the
   * alternative is one keyword away and the consequence is the test below.
   */
  @Test
  void theSeedSubmitsNoPrecondition() {
    final RecordingHAPlugin ha = new RecordingHAPlugin();
    server.setHA(ha);

    security.applyReplicatedUsers(security.getUsersJsonPayload());

    assertThat(security.seedSecurityStateClusterWide()).isEmpty();

    assertThat(ha.userPreconditions).containsExactly((String) null);
    assertThat(ha.groupPreconditions).containsExactly((String) null);
    assertThat(ha.apiTokenPreconditions).containsExactly((String) null);
  }

  /**
   * And what would happen if it did. This node stands in for the peer being seeded: its baseline is the
   * document it held when it went out of step, and the seed's precondition describes the document the cluster
   * holds now. The apply refuses - so a preconditioned seed repairs every node except the one it was sent for.
   */
  @Test
  void aPreconditionedSeedWouldBeRefusedByThePeerItIsMeantToRepair() {
    final RecordingHAPlugin ha = new RecordingHAPlugin();
    server.setHA(ha);

    // The peer's baseline: the document it last installed, from before it missed anything.
    final String stale = security.getUsersJsonPayload();
    security.applyReplicatedUsers(stale);

    // The cluster has moved on since; this is the fingerprint a seed built on the leader would carry.
    final String cluster = security.usersFingerprint() + "-the-cluster-has-moved-on";

    assertThat(security.applyReplicatedUsers(stale, cluster))
        .as("a stale peer cannot judge a precondition computed from a document it never installed")
        .isFalse();
  }

  /**
   * The unconditional seed does reach that peer, which is the behaviour the test above says must not be traded
   * away. Driven through the same apply, with no precondition.
   */
  @Test
  void anUnconditionalSeedReachesThatSamePeer() {
    // Minted before the HA plugin is installed, so it lands in this node's own store rather than waiting for a
    // replicated apply the fixture does not perform.
    final JSONObject created = controlPlane.createApiToken("ci", "*", 0, new JSONObject());
    final String hash = created.getString("tokenHash");
    final String withToken = security.getApiTokensJsonPayload();
    assertThat(withToken).contains(hash);

    final RecordingHAPlugin ha = new RecordingHAPlugin();
    server.setHA(ha);

    // The peer's stale baseline is a document without the token, recorded as a replicated install.
    security.getApiTokenConfiguration().deleteToken(hash);
    security.applyReplicatedApiTokens(security.getApiTokensJsonPayload());
    assertThat(security.getApiTokensJsonPayload()).doesNotContain(hash);

    security.applyReplicatedApiTokens(withToken);

    assertThat(security.getApiTokensJsonPayload())
        .as("the seed must install on a peer whose baseline does not match the cluster's")
        .contains(hash);
  }

  // -------------------------------------------------------------------------------------------
  // Fixtures
  // -------------------------------------------------------------------------------------------

  /** Records the precondition each seeded document carried, standing in for the Raft round trip. */
  private static class RecordingHAPlugin implements HAServerPlugin {
    final List<String> userPreconditions     = new ArrayList<>();
    final List<String> groupPreconditions    = new ArrayList<>();
    final List<String> apiTokenPreconditions = new ArrayList<>();
    volatile boolean   refuseEverything;

    @Override
    public void replicateSecurityUsers(final String usersJsonArray) {
      userPreconditions.add(null);
    }

    @Override
    public boolean replicateSecurityUsers(final String usersJsonArray, final String expectedFingerprint) {
      userPreconditions.add(expectedFingerprint);
      return !refuseEverything;
    }

    @Override
    public void replicateSecurityGroups(final String groupsJson) {
      groupPreconditions.add(null);
    }

    @Override
    public boolean replicateSecurityGroups(final String groupsJson, final String expectedFingerprint) {
      groupPreconditions.add(expectedFingerprint);
      return !refuseEverything;
    }

    @Override
    public void replicateSecurityApiTokens(final String apiTokensJson) {
      apiTokenPreconditions.add(null);
    }

    @Override
    public boolean replicateSecurityApiTokens(final String apiTokensJson, final String expectedFingerprint) {
      apiTokenPreconditions.add(expectedFingerprint);
      return !refuseEverything;
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
