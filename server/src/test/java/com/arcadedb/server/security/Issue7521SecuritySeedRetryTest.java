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
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #7521: a peer whose security seed failed was admitted and served traffic with its
 * own stale credentials.
 * <p>
 * An admission seeds the joining peer with {@code server-users.jsonl}, {@code server-groups.json} and
 * {@code server-api-tokens.json}, none of which a Raft snapshot install carries. That seed used to be a single
 * best-effort attempt. {@code replicateSecurity*} submits a Raft entry and waits for it to commit, so the way
 * it usually fails is that there is no quorum <i>at this instant</i> - which is transient, and is the same
 * condition that makes an {@code addPeer} interesting in the first place. One attempt against a transient
 * failure leaves a committed cluster member running on whatever its own config directory holds: for a node
 * re-added after time out of the cluster, a user dropped since, a group narrowed since, or a token revoked
 * since is still good on that one node.
 * <p>
 * Two halves are pinned here: {@link ServerSecurity#seedSecurityStateClusterWide(long)}, which retries within
 * a bounded budget, and {@link ServerControlPlane#connectCluster}, the second admission entry point - it
 * seeded the users document alone, and read it outside the security monitor, which is exactly the window
 * #7373 closed on the add-peer route.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue7521SecuritySeedRetryTest {

  private static final String CONFIG_PATH = "target/test-security-7521";

  /** Long enough for several backoff rounds, short enough that a test that must exhaust it stays quick. */
  private static final long   BUDGET_MS   = 1_500L;

  private FixtureServer      server;
  private ServerSecurity     security;
  private ServerControlPlane controlPlane;

  @BeforeEach
  void setUp() {
    GlobalConfiguration.SERVER_SECURITY_SALT_ITERATIONS.setValue(1000);
    GlobalConfiguration.HA_SECURITY_SEED_RETRY_TIMEOUT.setValue(BUDGET_MS);

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
    GlobalConfiguration.HA_SECURITY_SEED_RETRY_TIMEOUT.reset();
    FileUtils.deleteRecursively(new File(CONFIG_PATH));
  }

  // -------------------------------------------------------------------------------------------
  // The retry itself
  // -------------------------------------------------------------------------------------------

  /** The failure the retry exists for: no quorum for a moment, then there is one. */
  @Test
  void aSeedThatFailsAndThenSucceedsWithinTheBudgetIsNotReportedAsAFailure() {
    final SeedingHAPlugin ha = new SeedingHAPlugin();
    ha.failUsersForFirstAttempts = 2;
    server.setHA(ha);

    assertThat(security.seedSecurityStateClusterWide(BUDGET_MS))
        .as("a transient absence of quorum must not leave the peer running on its own documents")
        .isEmpty();

    assertThat(ha.userDocuments).as("the users document did eventually commit").hasSize(1);
  }

  /**
   * A budget that expires with the document still not committed is reported, not swallowed. The caller turns
   * that list into the operator's remediation, so it has to name the documents rather than merely be non-empty.
   */
  @Test
  void aSeedThatNeverCommitsIsReportedOnceTheBudgetIsSpent() {
    final SeedingHAPlugin ha = new SeedingHAPlugin();
    ha.failUsersForFirstAttempts = Integer.MAX_VALUE;
    server.setHA(ha);

    assertThat(security.seedSecurityStateClusterWide(BUDGET_MS)).containsExactly("users");
    assertThat(ha.usersAttempts.get()).as("it was retried, not attempted once").isGreaterThan(1);
  }

  /**
   * A document that committed must not be resubmitted by a later round. Resubmitting it would also re-read it,
   * and every extra read of a whole document is another chance to overwrite a revocation that committed in the
   * meantime - the failure mode #7373 closed.
   */
  @Test
  void onlyTheDocumentsThatFailedAreRetried() {
    final SeedingHAPlugin ha = new SeedingHAPlugin();
    ha.failUsersForFirstAttempts = Integer.MAX_VALUE;
    server.setHA(ha);

    security.seedSecurityStateClusterWide(BUDGET_MS);

    assertThat(ha.usersAttempts.get()).isGreaterThan(1);
    assertThat(ha.groupDocuments).as("the group document committed on the first round and is left alone").hasSize(1);
    assertThat(ha.apiTokenDocuments).as("the token document committed on the first round too").hasSize(1);
  }

  /**
   * Each retry re-reads the document under the security monitor rather than resubmitting the payload the first
   * attempt read. Otherwise a revocation that commits between two attempts is undone cluster-wide by the next
   * one - the seed carries a whole document - which is the bug arriving through the door meant to fix it.
   * <p>
   * Driven by having the failing submit revoke the token before it throws, which stands in for a revocation
   * landing in that window.
   */
  @Test
  void aRetryRereadsTheDocumentInsteadOfResubmittingTheFirstPayload() {
    final JSONObject created = controlPlane.createApiToken("ci", "*", 0, new JSONObject());
    final String hash = created.getString("tokenHash");

    final SeedingHAPlugin ha = new SeedingHAPlugin() {
      @Override
      public void replicateSecurityApiTokens(final String apiTokensJson) {
        if (apiTokenAttempts.incrementAndGet() == 1) {
          // A revocation commits in the window between this attempt and the next one.
          security.getApiTokenConfiguration().deleteToken(hash);
          throw new IllegalStateException("no quorum right now");
        }
        apiTokenDocuments.add(apiTokensJson);
      }
    };
    server.setHA(ha);

    assertThat(security.seedSecurityStateClusterWide(BUDGET_MS)).isEmpty();

    assertThat(ha.apiTokenDocuments).hasSize(1);
    assertThat(ha.apiTokenDocuments.getFirst())
        .as("the retry must not resurrect a token revoked between the two attempts")
        .doesNotContain(hash);
  }

  /** The no-argument form is still the single best-effort attempt its callers were written against. */
  @Test
  void theNoArgumentFormStillMakesExactlyOneAttemptPerDocument() {
    final SeedingHAPlugin ha = new SeedingHAPlugin();
    ha.failUsersForFirstAttempts = Integer.MAX_VALUE;
    server.setHA(ha);

    assertThat(security.seedSecurityStateClusterWide()).containsExactly("users");
    assertThat(ha.usersAttempts.get()).isEqualTo(1);
  }

  /**
   * A budget of zero is the same single attempt, so an operator who sets
   * {@code arcadedb.ha.securitySeedRetryTimeout=0} gets the old behaviour rather than an unbounded loop.
   */
  @Test
  void aZeroBudgetDisablesTheRetryRatherThanLoopingForever() {
    final SeedingHAPlugin ha = new SeedingHAPlugin();
    ha.failUsersForFirstAttempts = Integer.MAX_VALUE;
    server.setHA(ha);

    assertThat(security.seedSecurityStateClusterWide(0L)).containsExactly("users");
    assertThat(ha.usersAttempts.get()).isEqualTo(1);
  }

  /**
   * An interrupt ends the retrying at once, reports what is still failing and leaves the flag set. A server
   * being torn down must not be held in the backoff, and swallowing the interrupt would strand the shutdown.
   */
  @Test
  void anInterruptEndsTheRetryAndLeavesTheFlagSet() throws Exception {
    final SeedingHAPlugin ha = new SeedingHAPlugin();
    ha.failUsersForFirstAttempts = Integer.MAX_VALUE;
    server.setHA(ha);

    final List<String> failed = new ArrayList<>();
    final boolean[] interruptFlagStillSet = new boolean[1];

    // A very large budget: only the interrupt can end this, so a return proves the interrupt ended it.
    final Thread worker = new Thread(() -> {
      failed.addAll(security.seedSecurityStateClusterWide(Long.MAX_VALUE / 4));
      interruptFlagStillSet[0] = Thread.currentThread().isInterrupted();
    });
    worker.start();

    // Wait until it is actually retrying before interrupting, so the interrupt lands in the backoff.
    while (ha.usersAttempts.get() < 2 && worker.isAlive())
      Thread.onSpinWait();
    worker.interrupt();
    worker.join(30_000);

    assertThat(worker.isAlive()).as("the interrupt must end the retry, not be swallowed").isFalse();
    assertThat(failed).containsExactly("users");
    assertThat(interruptFlagStillSet[0]).as("the interrupt flag is restored for the caller").isTrue();
  }

  // -------------------------------------------------------------------------------------------
  // connect cluster: the second admission entry point
  // -------------------------------------------------------------------------------------------

  /**
   * {@code connect cluster} seeded the users document and nothing else, so a peer joined through this verb got
   * neither the group document nor the token store that #7373 made cluster-wide - it kept its own, which for a
   * re-added node still holds the groups and tokens it had when it left.
   */
  @Test
  void connectClusterSeedsAllThreeSecurityDocumentsAndNotOnlyUsers() {
    final SeedingHAPlugin ha = new SeedingHAPlugin();
    server.setHA(ha);

    controlPlane.connectCluster("db2:2435");

    assertThat(ha.userDocuments).as("users").hasSize(1);
    assertThat(ha.groupDocuments).as("groups were never seeded by this verb").hasSize(1);
    assertThat(ha.apiTokenDocuments).as("API tokens were never seeded by this verb").hasSize(1);
  }

  /**
   * And each attempt re-reads the document rather than resubmitting a payload read once up front. It used to
   * call {@code getUsersJsonPayload()} directly - whose javadoc has always said the caller must hold the
   * security monitor - so a revocation committing between the read and the submit was undone on every node by
   * the seed, which carries a whole document.
   * <p>
   * Driven by having the failing submit revoke the token before it throws, which stands in for a revocation
   * landing in that window. A payload captured before the first attempt would still carry the revoked token.
   */
  @Test
  void connectClusterRereadsTheSeededDocumentsOnEachAttempt() {
    final JSONObject created = controlPlane.createApiToken("ci", "*", 0, new JSONObject());
    final String hash = created.getString("tokenHash");

    final SeedingHAPlugin ha = new SeedingHAPlugin() {
      @Override
      public void replicateSecurityApiTokens(final String apiTokensJson) {
        if (apiTokenAttempts.incrementAndGet() == 1) {
          security.getApiTokenConfiguration().deleteToken(hash);
          throw new IllegalStateException("no quorum right now");
        }
        apiTokenDocuments.add(apiTokensJson);
      }
    };
    server.setHA(ha);

    controlPlane.connectCluster("db2:2435");

    assertThat(ha.apiTokenDocuments).hasSize(1);
    assertThat(ha.apiTokenDocuments.getFirst())
        .as("the seed must not resurrect the revoked token on the joining peer")
        .doesNotContain(hash);
  }

  /**
   * The join is not rolled back by a failing seed, and must not be reported as failed either: the peer is
   * already a committed member, and a caller that retried the join would be retrying something that happened.
   * That contract predates this issue (#7401) and the retry must not change it.
   */
  @Test
  void connectClusterStillDoesNotFailTheJoinWhenASeedNeverCommits() {
    final SeedingHAPlugin ha = new SeedingHAPlugin();
    ha.failUsersForFirstAttempts = Integer.MAX_VALUE;
    server.setHA(ha);

    controlPlane.connectCluster("db2:2435");

    assertThat(ha.connectedAddress).isEqualTo("db2:2435");
    assertThat(ha.usersAttempts.get()).as("and it did retry before giving up").isGreaterThan(1);
  }

  /**
   * Nothing raised on the way to or around the seed may escape as a failed join either, not only a per-document
   * failure the seed itself collects. By the time the seed runs the peer is a committed member, and a caller
   * that saw a failure would retry a join that already happened - which is the contract the bare
   * {@code try/catch} around the old users-only seed was really holding, and the reason the new call keeps one.
   * <p>
   * Driven with a server that has no security store at all, the one realistic way the call itself blows up.
   */
  @Test
  void connectClusterSurvivesASeedThatCannotRunAtAll() {
    final SeedingHAPlugin ha = new SeedingHAPlugin();
    server.setHA(ha);
    server.setSecurity(null);

    controlPlane.connectCluster("db2:2435");

    assertThat(ha.connectedAddress).as("the join must stand whatever the seed did").isEqualTo("db2:2435");
  }

  // -------------------------------------------------------------------------------------------
  // Fixtures
  // -------------------------------------------------------------------------------------------

  /**
   * Stands in for the Raft round trip: records each submitted document, optionally failing the users one for a
   * configurable number of attempts, which is what an absent quorum looks like from here.
   */
  private static class SeedingHAPlugin implements HAServerPlugin {
    final List<String>  userDocuments           = new ArrayList<>();
    final List<String>  groupDocuments          = new ArrayList<>();
    final List<String>  apiTokenDocuments       = new ArrayList<>();
    final AtomicInteger usersAttempts           = new AtomicInteger();
    final AtomicInteger apiTokenAttempts        = new AtomicInteger();
    volatile int        failUsersForFirstAttempts;
    volatile String     connectedAddress;

    @Override
    public void connectCluster(final String serverAddress) {
      connectedAddress = serverAddress;
    }

    @Override
    public void replicateSecurityUsers(final String usersJsonArray) {
      if (usersAttempts.incrementAndGet() <= failUsersForFirstAttempts)
        throw new IllegalStateException("no quorum right now");
      userDocuments.add(usersJsonArray);
    }

    @Override
    public void replicateSecurityGroups(final String groupsJson) {
      groupDocuments.add(groupsJson);
    }

    @Override
    public void replicateSecurityApiTokens(final String apiTokensJson) {
      apiTokenAttempts.incrementAndGet();
      apiTokenDocuments.add(apiTokensJson);
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
