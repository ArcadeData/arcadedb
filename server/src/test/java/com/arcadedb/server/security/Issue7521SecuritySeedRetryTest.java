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
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #7521: a peer whose security seed failed was admitted and served traffic with its
 * own stale credentials.
 * <p>
 * The three security documents - {@code server-users.jsonl}, {@code server-groups.json} and
 * {@code server-api-tokens.json} - live under {@code <server-root>/config/}, outside the database directory, so
 * Raft snapshot install carries none of them. The seed an admission path issues is the only thing that converges
 * them on a joining peer, and it is submitted as Raft entries, so its usual failure is "no quorum right now" -
 * the same condition that makes adding a peer interesting in the first place.
 * <p>
 * Before the fix that seed was one attempt, and a failure was reported inside an HTTP 200. A peer re-added after
 * having been out of the cluster then ran on the security state from whenever it left: a user dropped since, a
 * group narrowed since or <b>a token revoked since</b> still authenticated and authorized there.
 * <p>
 * What is pinned here is the {@link ServerSecurity} half - the retry itself, that only the failing document is
 * retried, and that every attempt re-reads under the security monitor - plus the {@code connect cluster} entry
 * point, which seeded the users document alone and swallowed the failure. The HTTP status half of
 * {@code POST /api/v1/cluster/peer} is pinned by {@code Issue7521SeedOutcomeResponseTest} in the ha-raft module,
 * where that handler lives.
 */
class Issue7521SecuritySeedRetryTest {

  private static final String CONFIG_PATH = "target/test-security-7521";

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
    // No pause between attempts: what these tests assert is the attempt count and the payload each attempt
    // carries, never the elapsed time, so the backoff has nothing to contribute but seconds of test runtime.
    configuration.setValue(GlobalConfiguration.HA_SECURITY_SEED_RETRY_BASE_MS, 0L);

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
  // The retry itself
  // -------------------------------------------------------------------------------------------

  /**
   * The condition this retry exists for: quorum is briefly unavailable, the first submission is rejected, and a
   * moment later the same submission commits. One attempt reported that as a permanently unseeded peer.
   */
  @Test
  void aSeedThatFailsOnceAndThenSucceedsIsReportedAsSeeded() {
    final SeedingHAPlugin ha = new SeedingHAPlugin(security);
    ha.failUsersTimes = 1;
    server.setHA(ha);

    assertThat(security.seedSecurityStateClusterWide(3, 0L))
        .as("the second attempt landed, so nothing is left unseeded")
        .isEmpty();

    assertThat(ha.userDocuments).as("one rejected submission and one that committed").hasSize(2);
    assertThat(ha.groupDocuments).as("the group document never failed, so it is submitted once").hasSize(1);
    assertThat(ha.apiTokenDocuments).as("the token document never failed, so it is submitted once").hasSize(1);
  }

  /**
   * A retry must re-send only what failed. Re-sending the documents that already committed costs a Raft round
   * trip each and widens the window in which an unrelated admin change is overwritten by a document read before
   * it.
   */
  @Test
  void onlyTheDocumentThatFailedIsRetried() {
    final SeedingHAPlugin ha = new SeedingHAPlugin(security);
    ha.failApiTokensTimes = Integer.MAX_VALUE;
    server.setHA(ha);

    assertThat(security.seedSecurityStateClusterWide(3, 0L)).containsExactly("API tokens");

    assertThat(ha.apiTokenDocuments).as("attempted once per attempt in the budget").hasSize(3);
    assertThat(ha.userDocuments).as("committed on the first attempt, never re-sent").hasSize(1);
    assertThat(ha.groupDocuments).as("committed on the first attempt, never re-sent").hasSize(1);
  }

  /** The budget is a bound: a document that never lands is reported, not retried forever. */
  @Test
  void aSeedThatNeverLandsIsReportedAfterTheBudgetIsSpent() {
    final SeedingHAPlugin ha = new SeedingHAPlugin(security);
    ha.failGroupsTimes = Integer.MAX_VALUE;
    ha.failUsersTimes = Integer.MAX_VALUE;
    server.setHA(ha);

    assertThat(security.seedSecurityStateClusterWide(2, 0L))
        .as("both are named, so the operator knows which documents the peer is stale on")
        .containsExactly("users", "groups");

    assertThat(ha.userDocuments).hasSize(2);
    assertThat(ha.groupDocuments).hasSize(2);
  }

  /** A budget of one, or of nonsense, is one attempt - never zero, which would seed nothing at all. */
  @Test
  void aBudgetBelowOneStillMakesOneAttempt() {
    final SeedingHAPlugin ha = new SeedingHAPlugin(security);
    server.setHA(ha);

    assertThat(security.seedSecurityStateClusterWide(0, 0L)).isEmpty();

    assertThat(ha.userDocuments).hasSize(1);
    assertThat(ha.groupDocuments).hasSize(1);
    assertThat(ha.apiTokenDocuments).hasSize(1);
  }

  /**
   * The other end of the budget. Both settings are operator-supplied and both are spent inside the request that
   * already made the membership change, so an unclamped {@code Integer.MAX_VALUE} would be an unbounded stream
   * of Raft submissions on a request thread rather than a generous retry.
   */
  @Test
  void anAbsurdBudgetIsClampedRatherThanHonoured() {
    final SeedingHAPlugin ha = new SeedingHAPlugin(security);
    ha.failUsersTimes = Integer.MAX_VALUE;
    server.setHA(ha);

    assertThat(security.seedSecurityStateClusterWide(Integer.MAX_VALUE, 0L)).containsExactly("users");

    assertThat(ha.userDocuments).as("clamped to the 10-attempt ceiling").hasSize(10);
  }

  /**
   * A base delay near {@code Long.MAX_VALUE} used to be shifted left, and {@code Long.MAX_VALUE << 1} is
   * <b>negative</b> - which makes {@code Thread.sleep} throw {@code IllegalArgumentException} out of a method
   * whose whole contract is to return the documents that could not be seeded. The backoff saturates instead.
   */
  @Test
  void anAbsurdBackoffSaturatesInsteadOfOverflowingNegative() {
    assertThat(ServerSecurity.backoffMs(Long.MAX_VALUE, 1)).isEqualTo(30_000L);
    assertThat(ServerSecurity.backoffMs(Long.MAX_VALUE, 9)).isEqualTo(30_000L);
    assertThat(ServerSecurity.backoffMs(500L, 1)).isEqualTo(500L);
    assertThat(ServerSecurity.backoffMs(500L, 2)).isEqualTo(1_000L);
    assertThat(ServerSecurity.backoffMs(500L, 3)).isEqualTo(2_000L);
    assertThat(ServerSecurity.backoffMs(500L, 9)).as("saturates rather than growing to two minutes")
        .isEqualTo(30_000L);
    assertThat(ServerSecurity.backoffMs(0L, 4)).isZero();
    assertThat(ServerSecurity.backoffMs(-1L, 4)).as("a negative setting is no pause, never a negative sleep")
        .isZero();
  }

  /**
   * The point of the monitor, applied to the retry. A retry that replayed the payload read before the first
   * attempt would carry a pre-revocation document and put the revoked token back on every node in the cluster -
   * the exact failure the seed's monitor exists to prevent, arriving through the door added to fix a different
   * one.
   * <p>
   * Driven by revoking the token locally from inside the failing first attempt, which stands in for a revocation
   * committing in that window.
   */
  @Test
  void everyAttemptRereadsTheDocumentInsteadOfReplayingTheFirstPayload() {
    final JSONObject created = security.getApiTokenConfiguration().createToken("ci", "*", 0, new JSONObject());
    final String hash = created.getString("tokenHash");

    final SeedingHAPlugin ha = new SeedingHAPlugin(security) {
      @Override
      public void replicateSecurityApiTokens(final String apiTokensJson) {
        if (apiTokenDocuments.isEmpty()) {
          apiTokenDocuments.add(apiTokensJson);
          // A revocation lands between attempt 1 and attempt 2. Applied straight to the store rather than
          // through the control plane, so it does not re-enter this method.
          assertThat(security.getApiTokenConfiguration().deleteToken(hash)).isTrue();
          throw new IllegalStateException("consensus lost");
        }
        super.replicateSecurityApiTokens(apiTokensJson);
      }
    };
    server.setHA(ha);

    assertThat(security.seedSecurityStateClusterWide(2, 0L)).isEmpty();

    assertThat(ha.apiTokenDocuments).hasSize(2);
    assertThat(ha.apiTokenDocuments.getFirst())
        .as("the first attempt read the document while the token was still live")
        .contains(hash);
    assertThat(ha.apiTokenDocuments.get(1))
        .as("the retry must carry the post-revocation document, not a replay of the first payload")
        .doesNotContain(hash);
  }

  /** With no HA plugin at all there is no cluster to seed, and the retry loop must not turn that into work. */
  @Test
  void aStandaloneServerSeedsNothing() {
    assertThat(security.seedSecurityStateClusterWide(3, 0L)).isEmpty();
    assertThat(security.seedSecurityStateClusterWideWithRetry()).isEmpty();
  }

  /** The configured budget is read per call, so a {@code SET SERVER SETTING} applies without a restart. */
  @Test
  void theConfiguredBudgetIsReadPerCall() {
    final SeedingHAPlugin ha = new SeedingHAPlugin(security);
    ha.failUsersTimes = Integer.MAX_VALUE;
    server.setHA(ha);

    server.getConfiguration().setValue(GlobalConfiguration.HA_SECURITY_SEED_RETRIES, 2);
    assertThat(security.seedSecurityStateClusterWideWithRetry()).containsExactly("users");
    assertThat(ha.userDocuments).hasSize(2);

    ha.userDocuments.clear();
    server.getConfiguration().setValue(GlobalConfiguration.HA_SECURITY_SEED_RETRIES, 4);
    assertThat(security.seedSecurityStateClusterWideWithRetry()).containsExactly("users");
    assertThat(ha.userDocuments).as("the new budget applies to the very next admission").hasSize(4);
  }

  // -------------------------------------------------------------------------------------------
  // connect cluster: the sibling admission path, which seeded the users document alone
  // -------------------------------------------------------------------------------------------

  /**
   * {@code connect cluster} used to seed {@code server-users.jsonl} and nothing else, so a group narrowed or a
   * token revoked while the joining server was away stayed in force on it.
   */
  @Test
  void connectClusterSeedsAllThreeDocuments() {
    final SeedingHAPlugin ha = new SeedingHAPlugin(security);
    server.setHA(ha);

    assertThatCode(() -> controlPlane.connectCluster("node2@localhost:2424")).doesNotThrowAnyException();

    assertThat(ha.joined).containsExactly("node2@localhost:2424");
    assertThat(ha.userDocuments).hasSize(1);
    assertThat(ha.groupDocuments).as("the group document was never seeded by this path before").hasSize(1);
    assertThat(ha.apiTokenDocuments).as("nor was the API-token document").hasSize(1);
  }

  /**
   * The seed on this verb spends the configured budget and then gives up without failing the join.
   * <p>
   * Not failing it is the contract issue #7401 chose and {@code Issue7401ServerControlPlaneConnectClusterTest}
   * pins: the server is a committed member by the time the seed runs, so reporting a failed join would send the
   * caller to retry something that already happened. The sibling {@code POST /api/v1/cluster/peer} answers 503 in
   * the same situation, and reconciling the two verbs is issue #7550 rather than this one. What this pins is that
   * the budget IS spent on this path - a single attempt, which is what it used to make, is the defect - and that
   * the other two documents still went out.
   */
  @Test
  void connectClusterSpendsTheRetryBudgetAndDoesNotFailTheJoin() {
    final SeedingHAPlugin ha = new SeedingHAPlugin(security);
    ha.failGroupsTimes = Integer.MAX_VALUE;
    server.setHA(ha);
    server.getConfiguration().setValue(GlobalConfiguration.HA_SECURITY_SEED_RETRIES, 2);

    assertThatCode(() -> controlPlane.connectCluster("node2@localhost:2424")).doesNotThrowAnyException();

    assertThat(ha.joined)
        .as("the membership change is NOT rolled back - removing a committed member is a second failure mode")
        .containsExactly("node2@localhost:2424");
    assertThat(ha.groupDocuments).as("the whole budget was spent on the document that failed").hasSize(2);
    assertThat(ha.userDocuments).as("a failing group seed does not skip the others").hasSize(1);
    assertThat(ha.apiTokenDocuments).hasSize(1);
  }

  /** A join that never happened must not be reported as a seed problem. */
  @Test
  void connectClusterStillRefusesABlankAddressBeforeSeedingAnything() {
    final SeedingHAPlugin ha = new SeedingHAPlugin(security);
    server.setHA(ha);

    assertThatThrownBy(() -> controlPlane.connectCluster("  "))
        .isInstanceOf(IllegalArgumentException.class);

    assertThat(ha.joined).isEmpty();
    assertThat(ha.userDocuments).isEmpty();
    assertThat(ha.groupDocuments).as("validation runs before any document is submitted").isEmpty();
    assertThat(ha.apiTokenDocuments).isEmpty();
  }

  // -------------------------------------------------------------------------------------------

  /**
   * Stands in for the Raft round trip: records each submitted document and applies it, which is what the state
   * machine does on the leader before {@code submitAndWait} returns. Each document can be made to fail a given
   * number of times first, which is what a momentary loss of quorum looks like from here.
   */
  private static class SeedingHAPlugin implements HAServerPlugin {
    protected final ServerSecurity security;
    final           List<String>   userDocuments     = new ArrayList<>();
    final           List<String>   groupDocuments    = new ArrayList<>();
    final           List<String>   apiTokenDocuments = new ArrayList<>();
    final           List<String>   joined            = new ArrayList<>();
    int failUsersTimes;
    int failGroupsTimes;
    int failApiTokensTimes;

    private SeedingHAPlugin(final ServerSecurity security) {
      this.security = security;
    }

    @Override
    public void replicateSecurityUsers(final String usersJsonArray) {
      userDocuments.add(usersJsonArray);
      if (failUsersTimes > 0) {
        --failUsersTimes;
        throw new IllegalStateException("consensus lost");
      }
      security.applyReplicatedUsers(usersJsonArray);
    }

    @Override
    public void replicateSecurityGroups(final String groupsJson) {
      groupDocuments.add(groupsJson);
      if (failGroupsTimes > 0) {
        --failGroupsTimes;
        throw new IllegalStateException("consensus lost");
      }
      security.applyReplicatedGroups(groupsJson);
    }

    @Override
    public void replicateSecurityApiTokens(final String apiTokensJson) {
      apiTokenDocuments.add(apiTokensJson);
      if (failApiTokensTimes > 0) {
        --failApiTokensTimes;
        throw new IllegalStateException("consensus lost");
      }
      security.applyReplicatedApiTokens(apiTokensJson);
    }

    @Override
    public void connectCluster(final String serverAddress) {
      joined.add(serverAddress);
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
