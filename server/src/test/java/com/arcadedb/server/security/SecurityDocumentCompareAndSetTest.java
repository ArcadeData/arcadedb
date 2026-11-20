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
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.security.SecurityManager;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.HAServerPlugin;
import com.arcadedb.server.ServerControlPlane;
import com.arcadedb.utility.FileUtils;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * The compare-and-set precondition a replicated security document carries: concurrent changes on two nodes,
 * the atomicity of a token document and its precondition, the unconditional seed, and documents changed locally.
 */
class SecurityDocumentCompareAndSetTest {
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
  @Nested
  class ConcurrentChangeOnAnotherNode {

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
    // The precondition must describe the document the payload was BUILT FROM
    // ===========================================================================================

    /**
     * The payload and its precondition are both derived from the security document, and if they come from two
     * separate reads of it a replicated apply can land in the gap: the payload is then built from the OLD document
     * while the precondition describes the NEW one, the compare-and-set passes, and the stale payload installs
     * over a change it was never aware of - issue #7509 reopened on a window a few bytecodes wide. The apply
     * thread swaps that reference deliberately WITHOUT this monitor, so holding the monitor does not close it.
     * <p>
     * What is asserted is the property that makes the gap unreachable: the submitted precondition is the
     * fingerprint of the submitted payload with this node's own change undone - i.e. of the document the payload
     * was built from - rather than of whatever the live document happened to be a moment later.
     */
    @Test
    void theUserPreconditionFingerprintsTheListTheSubmittedPayloadWasBuiltFrom() {
      joinClusterRacing(() -> applyUserListWith("bob"));

      security.createUserClusterWide(userJson("alice"));

      final String[] winning = ha.submitted.getLast();
      assertThat(fingerprintOfUsersWithout(winning[0], "alice"))
          .as("the precondition must be the fingerprint of the list this payload was derived from")
          .isEqualTo(winning[1]);
    }

    @Test
    void theGroupPreconditionFingerprintsTheDocumentTheSubmittedPayloadWasBuiltFrom() {
      joinClusterRacing(() -> applyGroupDocumentWith("auditors"));

      security.saveGroupClusterWide(DATABASE, "editors", group());

      final String[] winning = ha.submitted.getLast();
      assertThat(fingerprintOfGroupsWithout(winning[0], "editors")).isEqualTo(winning[1]);
    }

    @Test
    void theTokenPreconditionFingerprintsTheDocumentTheSubmittedPayloadWasBuiltFrom() {
      joinClusterRacing(() -> applyTokenDocumentWith("other-node-token"));

      final JSONObject minted = security.createApiTokenClusterWide("my-token", DATABASE, -1L, new JSONObject());

      final String[] winning = ha.submitted.getLast();
      assertThat(fingerprintOfTokensWithout(winning[0], minted.getString("tokenHash"))).isEqualTo(winning[1]);
    }

    // ===========================================================================================
    // The retry's catch-up wait must not be done under the shared monitor
    // ===========================================================================================

    /**
     * A refused submitter has to see the winning entry before it rebuilds, and on a follower that means waiting for
     * the local state machine - bounded by the quorum timeout. Doing that inside {@code synchronized (this)} would
     * hold the monitor all seven cluster-wide mutators share for up to five such waits, so an unrelated group or
     * token change on the same node would queue behind one caller's retry storm.
     */
    @Test
    void theCatchUpWaitBetweenRetriesHappensOutsideTheSharedMonitor() {
      joinClusterRacing(() -> applyUserListWith("bob"));

      security.createUserClusterWide(userJson("alice"));

      assertThat(ha.awaits).as("a refused attempt must wait for this node to catch up before rebuilding").isEqualTo(1);
      assertThat(ha.awaitedUnderTheMonitor)
          .as("but never while holding the monitor every security mutation on this node shares").isFalse();
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

    /** The fingerprint of {@code usersPayload} with {@code name} removed: the list it was built from. */
    private static String fingerprintOfUsersWithout(final String usersPayload, final String name) {
      final JSONArray payload = new JSONArray(usersPayload);
      final JSONArray before = new JSONArray();
      for (int i = 0; i < payload.length(); i++)
        if (!name.equals(payload.getJSONObject(i).getString("name")))
          before.put(payload.getJSONObject(i));
      return SecurityDocumentFingerprint.of(before.toString());
    }

    private static String fingerprintOfGroupsWithout(final String groupsPayload, final String name) {
      final JSONObject before = new JSONObject(groupsPayload);
      before.getJSONObject("databases").getJSONObject(DATABASE).getJSONObject("groups").remove(name);
      return SecurityDocumentFingerprint.of(before.toString());
    }

    private static String fingerprintOfTokensWithout(final String tokensPayload, final String tokenHash) {
      final JSONObject payload = new JSONObject(tokensPayload);
      final JSONArray tokens = payload.getJSONArray("tokens");
      final JSONArray before = new JSONArray();
      for (int i = 0; i < tokens.length(); i++)
        if (!tokenHash.equals(tokens.getJSONObject(i).getString("tokenHash")))
          before.put(tokens.getJSONObject(i));
      return SecurityDocumentFingerprint.of(new JSONObject().put("version", payload.get("version"))
          .put("tokens", before).toString());
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

      /** The (payload, precondition) pair of every submission, newest last. */
      final List<String[]> submitted = new ArrayList<>();

      /** True if any {@link #awaitLocalApply()} happened with the {@code ServerSecurity} monitor held. */
      boolean awaitedUnderTheMonitor;
      int     awaits;

      private RacingHAPlugin(final ServerSecurity security, final Runnable competing, final int competeForFirstNSubmits) {
        this.security = security;
        this.competing = competing;
        this.competeForFirstNSubmits = competeForFirstNSubmits;
      }

      private void race(final String payload, final String expectedFingerprint) {
        submitted.add(new String[] { payload, expectedFingerprint });
        if (submits < competeForFirstNSubmits)
          competing.run();
        submits++;
      }

      @Override
      public void awaitLocalApply() {
        awaits++;
        // The monitor is shared by all seven cluster-wide mutators, so waiting for the local state machine while
        // holding it would queue every unrelated security change on this node behind one caller's retry storm.
        if (Thread.holdsLock(security))
          awaitedUnderTheMonitor = true;
      }

      @Override
      public boolean replicateSecurityUsers(final String usersJson, final String expectedFingerprint) {
        race(usersJson, expectedFingerprint);
        return security.applyReplicatedUsers(usersJson, expectedFingerprint);
      }

      @Override
      public boolean replicateSecurityGroups(final String groupsJson, final String expectedFingerprint) {
        race(groupsJson, expectedFingerprint);
        return security.applyReplicatedGroups(groupsJson, expectedFingerprint);
      }

      @Override
      public boolean replicateSecurityApiTokens(final String apiTokensJson, final String expectedFingerprint) {
        race(apiTokensJson, expectedFingerprint);
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

  /**
   * Issue #7509: a cluster-wide token mint or revocation submits a document AND the compare-and-set precondition
   * of the document it was derived from, and the two have to leave {@link ApiTokenConfiguration}'s monitor
   * together.
   * <p>
   * Reading them separately - mint under the monitor, then fingerprint the live store outside it - lets a
   * replicated apply land in between, and the submission then carries a payload built from the OLD token set under
   * a precondition describing the NEW one. The compare-and-set passes and the stale payload installs, putting back
   * tokens the winning entry revoked, which is worse than the lost update it was meant to prevent.
   */
  @Nested
  class TokenDocumentPairAtomicity {

    private static final String CONFIG_PATH = "target/test-api-tokens-7509";

    private ApiTokenConfiguration config;

    @BeforeEach
    void setUp() {
      final File dir = new File(CONFIG_PATH);
      if (dir.exists())
        FileUtils.deleteRecursively(dir);
      assertThat(dir.mkdirs()).isTrue();
      config = new ApiTokenConfiguration(CONFIG_PATH);
    }

    @AfterEach
    void tearDown() {
      FileUtils.deleteRecursively(new File(CONFIG_PATH));
    }

    @Test
    void aMintCarriesTheDocumentItWasDerivedFrom() {
      config.createToken("existing", "mydb", 0, new JSONObject());
      final String before = config.toJsonPayload();

      final ApiTokenConfiguration.MintedToken minted = config.mintToken("new", "mydb", 0, new JSONObject());

      assertThat(minted.documentBeforeJson())
          .as("the precondition half is the store as it was, not a second read of it").isEqualTo(before);
      assertThat(minted.documentJson())
          .as("and the payload half carries the new token").contains("\"name\":\"new\"");
      assertThat(config.toJsonPayload())
          .as("mintToken installs nothing locally; only the replicated apply does").isEqualTo(before);
    }

    @Test
    void aRevocationCarriesTheDocumentItWasRemovedFrom() {
      final JSONObject doomed = config.createToken("doomed", "mydb", 0, new JSONObject());
      config.createToken("kept", "mydb", 0, new JSONObject());
      final String before = config.toJsonPayload();

      final ApiTokenConfiguration.DocumentChange revocation = config.documentWithout(doomed.getString("tokenHash"));

      assertThat(revocation.before()).isEqualTo(before);
      assertThat(revocation.after())
          .as("the payload drops the revoked token and keeps the other")
          .doesNotContain(doomed.getString("tokenHash")).contains("\"name\":\"kept\"");
      assertThat(config.toJsonPayload()).as("nothing local is mutated").isEqualTo(before);
    }

    @Test
    void revokingAnUnknownTokenReportsNoChangeAtAll() {
      assertThat(config.documentWithout("no-such-hash")).isNull();
    }
  }

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
   * monitors - is pinned in {@code MembershipSecuritySeederTest}.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class SeedStaysUnconditional {

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

  /**
   * Issue #8074, the group-document follow-up to #7601: the group document is changed node-locally by two supported
   * paths - the v1 to v2 migration {@link SecurityGroupFileRepository} runs when it loads a v1 file, and the hot
   * reload of a hand-edited {@code server-groups.json} - and neither goes through the replicated log.
   * <p>
   * Since #7693 every node judges a replicated group entry against the fingerprint of the last group document the
   * CLUSTER installed. The submitter used to send the fingerprint of its LIVE document instead, so after either local
   * change the precondition matched no node's recorded fingerprint: every node refused the entry, the bounded
   * compare-and-set loop gave up, and group administration stopped working from that node - for good, since the local
   * change never goes away by itself.
   * <p>
   * The fixture is a real two-node security state under a minimal Raft log ({@link LogHAPlugin}): entries are
   * appended in one order, every node applies them in that order with the precondition they carry, and a node that
   * is behind catches up before it applies anything new - which is what the state machine does.
   */
  @Nested
  class GroupDocumentLocalDrift {

    private static final String ROOT_PATH = "target/test-security-8074";
    private static final String DATABASE  = "graph";
    /** A watcher tick short enough that a hot reload is observed in milliseconds. */
    private static final int    FAST_RELOAD_MS = 100;
    /** A watcher tick that never fires during a test, so only a restart loads the file. */
    private static final int    NO_RELOAD_MS   = 3_600_000;

    private ServerSecurity nodeA;
    private ServerSecurity nodeB;
    private LogHAPlugin    cluster;
    private int            reloadEveryMs;

    @BeforeEach
    void setUp() {
      GlobalConfiguration.SERVER_SECURITY_SALT_ITERATIONS.setValue(1000);
      FileUtils.deleteRecursively(new File(ROOT_PATH));
      cluster = new LogHAPlugin();
    }

    private void startNodes(final int reloadEveryMs) {
      this.reloadEveryMs = reloadEveryMs;
      nodeA = node("a");
      nodeB = node("b");
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

    // ===========================================================================================
    // Finding 2: the supported hot reload of server-groups.json
    // ===========================================================================================

    /**
     * An operator edits {@code server-groups.json} on node B and the watcher reloads it. Node B must still be able to
     * save a group cluster-wide afterwards, and the change must land on both nodes.
     */
    @Test
    void aHotReloadedGroupFileDoesNotBlockThatNodesGroupSaves() throws Exception {
      startNodes(FAST_RELOAD_MS);
      seedBothNodes();
      hotReload(nodeB, "b", "reloaded-locally");

      nodeB.saveGroupClusterWide(DATABASE, "editors", group());

      assertThat(groupNames(nodeA)).as("the save landed on the peer").contains("editors");
      assertThat(groupNames(nodeB)).as("and on the node that submitted it").contains("editors");
      assertThat(nodeA.groupsFingerprint()).as("and the two nodes hold the same document again")
          .isEqualTo(nodeB.groupsFingerprint());
    }

    /** The same path through the delete, which builds its precondition the same way. */
    @Test
    void aHotReloadedGroupFileDoesNotBlockThatNodesGroupDeletes() throws Exception {
      startNodes(FAST_RELOAD_MS);
      seedBothNodes();
      nodeA.saveGroupClusterWide(DATABASE, "editors", group());
      hotReload(nodeB, "b", "reloaded-locally");

      assertThat(nodeB.deleteGroupClusterWide(DATABASE, "editors")).isTrue();

      assertThat(groupNames(nodeA)).doesNotContain("editors");
      assertThat(groupNames(nodeB)).doesNotContain("editors");
    }

    // ===========================================================================================
    // Finding 1: the v1 to v2 migration
    // ===========================================================================================

    /**
     * The cluster installed a v1 document - one whose admin group lacks {@code updateDatabaseSettings}, as an older
     * leader submits it. Node B restarts, loads that file and migrates it; node A has not. A group save from node B
     * must still be accepted, and it carries the migration to node A with it, so both nodes move together.
     */
    @Test
    void aNodeThatMigratedAV1DocumentCanStillSaveGroups() {
      startNodes(NO_RELOAD_MS);
      final JSONObject v1 = v1Document();
      cluster.submit(v1.toString(), null);
      assertThat(adminAccess(nodeA)).as("the fixture's premise: the installed document is unmigrated")
          .doesNotContain("updateDatabaseSettings");

      nodeB = restart(nodeB, "b");
      assertThat(adminAccess(nodeB)).as("the restart migrated node B's copy, and only node B's")
          .contains("updateDatabaseSettings");
      assertThat(nodeB.groupsFingerprint()).isNotEqualTo(nodeA.groupsFingerprint());

      nodeB.saveGroupClusterWide(DATABASE, "editors", group());

      assertThat(groupNames(nodeA)).contains("editors");
      assertThat(adminAccess(nodeA)).as("the migrated document reached the node that had not migrated")
          .contains("updateDatabaseSettings");
      assertThat(nodeA.groupsFingerprint()).isEqualTo(nodeB.groupsFingerprint());
    }

    /**
     * The same migration reached without a restart. A node installs a replicated v1 document and writes it to
     * {@code server-groups.json}; its own file watcher then reloads that file and migrates it. Every node does this,
     * so every node's live document moves away from the one the cluster recorded - and before the fix no node could
     * change a group at all.
     */
    @Test
    void aV1DocumentMigratedByEveryNodesWatcherDoesNotBlockGroupSaves() {
      startNodes(FAST_RELOAD_MS);
      cluster.submit(v1Document().toString(), null);
      await().atMost(Duration.ofSeconds(10)).pollInterval(Duration.ofMillis(50))
          .until(() -> adminAccess(nodeA).contains("updateDatabaseSettings")
              && adminAccess(nodeB).contains("updateDatabaseSettings"));

      nodeA.saveGroupClusterWide(DATABASE, "editors", group());

      assertThat(groupNames(nodeA)).contains("editors");
      assertThat(groupNames(nodeB)).contains("editors");
    }

    // ===========================================================================================
    // The counter-case: the #7509 lost-update protection still holds
    // ===========================================================================================

    /**
     * What the precondition is for. Node B builds its entry before it has applied a change node A committed, so the
     * entry would revert it: it must still be refused, and the retry - once node B has caught up - must keep both.
     */
    @Test
    void aSubmitterThatMissedAnEntryIsStillRefusedAndTheOtherChangeSurvives() {
      startNodes(NO_RELOAD_MS);
      seedBothNodes();

      // Node A commits a group that node B has not applied yet: B is one entry behind when it builds its own.
      cluster.lagBehind(nodeB);
      nodeA.saveGroupClusterWide(DATABASE, "auditors", group());
      assertThat(groupNames(nodeB)).as("the fixture's premise: node B has not applied it").doesNotContain("auditors");

      nodeB.saveGroupClusterWide(DATABASE, "editors", group());

      assertThat(cluster.refusals).as("the entry built from the stale view was refused").isEqualTo(1);
      assertThat(groupNames(nodeA)).contains("auditors", "editors");
      assertThat(groupNames(nodeB)).contains("auditors", "editors");
    }

    // ===========================================================================================
    // Fixtures
    // ===========================================================================================

    /**
     * Installs node A's current group document on both nodes as a replicated entry: the cluster's baseline.
     * <p>
     * It is also what starts each node's {@code server-groups.json} watcher, which {@link #hotReload} relies on: the
     * fixture never calls {@code startService()}, and {@link SecurityGroupFileRepository#applyReplicated} schedules
     * the watcher before it writes the file (issue #7545), as does the lazy {@code load()} behind
     * {@code getGroupsJsonPayload()}.
     */
    private void seedBothNodes() {
      cluster.submit(nodeA.getGroupsJsonPayload(), null);
      assertThat(nodeA.groupsFingerprint()).isEqualTo(nodeB.groupsFingerprint());
    }

    /** Rewrites {@code node}'s {@code server-groups.json} with an extra group and waits for the watcher to load it. */
    private static void hotReload(final ServerSecurity node, final String name, final String extraGroup)
        throws IOException {
      final String before = node.groupsFingerprint();
      final File file = new File(configPath(name), SecurityGroupFileRepository.FILE_NAME);

      final JSONObject edited = new JSONObject(node.getGroupsJsonPayload());
      final JSONObject databases = edited.getJSONObject("databases");
      if (!databases.has(DATABASE))
        databases.put(DATABASE, new JSONObject().put("groups", new JSONObject()));
      databases.getJSONObject(DATABASE).getJSONObject("groups").put(extraGroup, group());

      Files.writeString(file.toPath(), edited.toString(2), DatabaseFactory.getDefaultCharset());
      // Strictly newer than anything the watcher has seen, whatever the filesystem's timestamp resolution.
      assertThat(file.setLastModified(System.currentTimeMillis() + 5_000)).isTrue();

      await().atMost(Duration.ofSeconds(10)).pollInterval(Duration.ofMillis(50))
          .until(() -> !before.equals(node.groupsFingerprint()));
      assertThat(groupNames(node)).as("the fixture's premise: the watcher reloaded the edit").contains(extraGroup);
    }

    private static JSONObject v1Document() {
      final JSONObject admin = new JSONObject().put("resultSetLimit", -1L).put("readTimeout", -1L)
          .put("access", new JSONArray(new String[] { "updateSecurity", "updateSchema" }))
          .put("types", new JSONObject().put(SecurityManager.ANY, new JSONObject().put("access",
              new JSONArray(new String[] { "createRecord", "readRecord", "updateRecord", "deleteRecord" }))));
      final JSONObject anyGroup = new JSONObject().put("resultSetLimit", -1L).put("readTimeout", -1L)
          .put("access", new JSONArray())
          .put("types", new JSONObject().put(SecurityManager.ANY, new JSONObject().put("access", new JSONArray())));
      return new JSONObject()
          .put("databases", new JSONObject().put(SecurityManager.ANY,
              new JSONObject().put("groups", new JSONObject().put("admin", admin).put(SecurityManager.ANY, anyGroup))))
          .put("version", 1);
    }

    private static List<String> adminAccess(final ServerSecurity node) {
      final JSONArray access = node.groupsToJSON().getJSONObject("databases").getJSONObject(SecurityManager.ANY)
          .getJSONObject("groups").getJSONObject("admin").getJSONArray("access");
      final List<String> names = new ArrayList<>(access.length());
      for (int i = 0; i < access.length(); i++)
        names.add(access.getString(i));
      return names;
    }

    private static List<String> groupNames(final ServerSecurity node) {
      final JSONObject databases = node.groupsToJSON().getJSONObject("databases");
      if (!databases.has(DATABASE))
        return List.of();
      return new ArrayList<>(databases.getJSONObject(DATABASE).getJSONObject("groups").keySet());
    }

    private static JSONObject group() {
      return new JSONObject()
          .put("resultSetLimit", -1L)
          .put("readTimeout", -1L)
          .put("access", new JSONArray())
          .put("types", new JSONObject());
    }

    /** Stops {@code security} and opens a fresh one over the same configuration directory: a node restart. */
    private ServerSecurity restart(final ServerSecurity security, final String name) {
      security.stopService();
      final ServerSecurity restarted = open(name);
      cluster.replace(security, restarted);
      return restarted;
    }

    private ServerSecurity node(final String name) {
      assertThat(new File(configPath(name)).mkdirs()).isTrue();
      final ServerSecurity security = open(name);
      cluster.join(security);
      return security;
    }

    private static String configPath(final String name) {
      return ROOT_PATH + "/" + name + "/config";
    }

    private ServerSecurity open(final String name) {
      final ContextConfiguration configuration = new ContextConfiguration();
      configuration.setValue(GlobalConfiguration.SERVER_ROOT_PATH, ROOT_PATH + "/" + name);
      configuration.setValue(GlobalConfiguration.SERVER_SECURITY_RELOAD_EVERY, reloadEveryMs);

      final FixtureServer server = new FixtureServer(configuration);
      final ServerSecurity security = new ServerSecurity(server, configuration, configPath(name));
      server.setSecurity(security);
      server.setHA(cluster);
      return security;
    }

    /**
     * A Raft log reduced to what this test needs: one ordered list of group entries, each node applying every entry
     * it has not applied yet - with the precondition the entry carried - before the next one is evaluated. A node can
     * be told to lag, which leaves it one entry behind until the next submission catches it up, the way a follower's
     * state machine trails the leader's.
     */
    private static final class LogHAPlugin implements HAServerPlugin {
      private final List<String[]>       log     = new ArrayList<>();
      private final List<ServerSecurity> nodes   = new ArrayList<>();
      private final List<Integer>        applied = new ArrayList<>();
      private       ServerSecurity       lagging;
      int refusals;

      void join(final ServerSecurity node) {
        nodes.add(node);
        applied.add(log.size());
      }

      void replace(final ServerSecurity previous, final ServerSecurity next) {
        nodes.set(nodes.indexOf(previous), next);
      }

      void lagBehind(final ServerSecurity node) {
        lagging = node;
      }

      /** Appends an entry and applies the log on every node; the result is the entry's, identical on every node. */
      boolean submit(final String payload, final String precondition) {
        log.add(new String[] { payload, precondition });
        Boolean outcome = null;
        for (int n = 0; n < nodes.size(); n++) {
          final ServerSecurity node = nodes.get(n);
          if (node == lagging) {
            // Held back for this one entry: it catches up on the next submission, in log order.
            lagging = null;
            continue;
          }
          for (int i = applied.get(n); i < log.size(); i++) {
            final boolean installed = node.applyReplicatedGroups(log.get(i)[0], log.get(i)[1]);
            if (i == log.size() - 1) {
              if (outcome == null)
                outcome = installed;
              assertThat(installed).as("every node must reach the same verdict on the same entry").isEqualTo(outcome);
            }
          }
          applied.set(n, log.size());
        }
        assertThat(outcome).as("at least one node that is not lagging must have evaluated the entry").isNotNull();
        if (!outcome)
          refusals++;
        return outcome;
      }

      @Override
      public boolean replicateSecurityGroups(final String groupsJson, final String expectedFingerprint) {
        return submit(groupsJson, expectedFingerprint);
      }

      @Override
      public void awaitLocalApply() {
        // Every submission already applies the log on every non-lagging node before it returns.
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
        return "a";
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
        return 2;
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

  /**
   * Issue #8430, the user-list and API-token follow-up to #8074.
   * <p>
   * A cluster-wide user or token change carries the fingerprint of the document in force on the submitting node, and
   * every node judges it against the fingerprint of the last document the CLUSTER installed (issue #7693). When the
   * submitting node's copy was changed locally - a hand edit or a restored backup of {@code server-users.jsonl} or
   * {@code server-api-tokens.json}, picked up at restart or by the reload, including the legacy plaintext-token
   * migration a restored file triggers - no node accepts it. That refusal is kept on purpose: a node out of step with
   * the cluster must not push its own copy of the credentials over everyone else's (issue #6808), which is the
   * trade #8074 made the other way for groups only.
   * <p>
   * What this issue fixes is what the operator is told. The bounded compare-and-set loop used to give up with "the
   * document keeps being changed concurrently on another node ... retry the request", which is false and sends the
   * operator to retry a request that can never succeed from that node. It now names the local change and the two
   * ways out of it.
   * <p>
   * The fixture is a real two-node security state under a minimal Raft log ({@link LogHAPlugin}): entries are
   * appended in one order and every node applies them in that order with the precondition they carry.
   */
  @Nested
  class UserAndTokenLocalDrift {

    private static final String ROOT_PATH     = "target/test-security-8430";
    private static final String ROOT_PASSWORD = "Issue8430RootPassword";
    private static final String DATABASE      = "graph";

    private ServerSecurity nodeA;
    private ServerSecurity nodeB;
    private LogHAPlugin    cluster;

    @BeforeEach
    void setUp() {
      GlobalConfiguration.SERVER_SECURITY_SALT_ITERATIONS.setValue(1000);
      FileUtils.deleteRecursively(new File(ROOT_PATH));
      cluster = new LogHAPlugin();
      nodeA = node("a");
      nodeB = node("b");
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

    // ===========================================================================================
    // Users
    // ===========================================================================================

    /**
     * Node B's {@code server-users.jsonl} is edited by hand and the node restarts. A user change from node B is still
     * refused - its local user must not reach node A - but the failure now says why, and does not tell the operator to
     * retry.
     */
    @Test
    void aUserChangeFromANodeWhoseUserFileWasEditedLocallyNamesTheLocalEdit() throws IOException {
      seedUsers();
      nodeB = restartWithEditedUserFile(nodeB, "b", "local-only");

      assertThatThrownBy(() -> nodeB.createUserClusterWide(userJson(nodeB, "alice")))
          .isInstanceOf(ServerSecurityException.class)
          .hasMessageContaining("create user 'alice'")
          .hasMessageContaining("changed locally")
          .hasMessageContaining(SecurityUserFileRepository.FILE_NAME)
          .hasMessageContaining("another node")
          .hasMessageNotContaining("retry the request");

      assertThat(nodeA.getUsers()).as("the #6808 protection is unchanged: nothing reached node A")
          .doesNotContain("alice", "local-only");
    }

    /** The update and the drop build their precondition the same way, and fail the same way. */
    @Test
    void aUserUpdateAndDropFromTheEditedNodeNameTheLocalEditToo() throws IOException {
      seedUsers();
      nodeA.createUserClusterWide(userJson(nodeA, "alice"));
      nodeB = restartWithEditedUserFile(nodeB, "b", "local-only");

      assertThatThrownBy(() -> nodeB.updateUserClusterWide(userJson(nodeB, "alice")))
          .isInstanceOf(ServerSecurityException.class)
          .hasMessageContaining("changed locally")
          .hasMessageNotContaining("retry the request");
      assertThatThrownBy(() -> nodeB.dropUserClusterWide("alice"))
          .isInstanceOf(ServerSecurityException.class)
          .hasMessageContaining("changed locally")
          .hasMessageNotContaining("retry the request");

      assertThat(nodeA.getUsers()).contains("alice").doesNotContain("local-only");
    }

    /**
     * The way out the message names: the same change made through another node is accepted everywhere, and its entry
     * replaces the edited node's local copy, so both nodes hold one list again.
     */
    @Test
    void theSameUserChangeThroughAnotherNodeSucceedsAndReconvergesTheEditedNode() throws IOException {
      seedUsers();
      nodeB = restartWithEditedUserFile(nodeB, "b", "local-only");

      nodeA.createUserClusterWide(userJson(nodeA, "alice"));

      assertThat(nodeB.getUsers()).contains("alice").doesNotContain("local-only");
      assertThat(nodeB.usersFingerprint()).isEqualTo(nodeA.usersFingerprint());
    }

    /**
     * The counter-case: a node that is merely BEHIND - not edited - is refused once and succeeds on the retry, and
     * never sees the local-edit message.
     */
    @Test
    void aNodeThatIsOnlyBehindStillSucceedsOnTheRetry() {
      seedUsers();
      cluster.lagBehind(nodeB);
      nodeA.createUserClusterWide(userJson(nodeA, "bob"));

      nodeB.createUserClusterWide(userJson(nodeB, "alice"));

      assertThat(cluster.refusals).as("the entry built from the stale view was refused").isEqualTo(1);
      assertThat(nodeA.getUsers()).contains("alice", "bob");
      assertThat(nodeB.getUsers()).contains("alice", "bob");
    }

    /**
     * The counter-case for the message: when the compare-and-set really does lose to another node every time, and
     * this node's copy is exactly the cluster's, the failure keeps saying so rather than blaming a local edit.
     */
    @Test
    void aGenuinelyContendedUserChangeKeepsTheConcurrencyMessage() {
      seedUsers();
      final int[] competitor = { 0 };
      cluster.beforeEachUserSubmit(() -> {
        final JSONArray list = new JSONArray(nodeA.getUsersJsonPayload());
        list.put(userJson(nodeA, "competitor-" + competitor[0]++));
        cluster.submitUsers(list.toString(), nodeA.usersFingerprint());
      });

      assertThatThrownBy(() -> nodeB.createUserClusterWide(userJson(nodeB, "alice")))
          .isInstanceOf(ServerSecurityException.class)
          .hasMessageContaining("changed concurrently on another node")
          .hasMessageNotContaining("changed locally");
    }

    // ===========================================================================================
    // API tokens
    // ===========================================================================================

    /**
     * Node B restarts on a restored {@code server-api-tokens.json} that still holds a legacy plaintext token, and
     * {@link ApiTokenConfiguration#load()} migrates it node-locally. A token mint from node B is refused - the restored
     * token must not reach node A - with the local-change message.
     */
    @Test
    void aTokenMintFromANodeThatMigratedARestoredTokenFileNamesTheLocalChange() throws IOException {
      seedTokens();
      nodeB = restartWithLegacyTokenFile(nodeB, "b");

      assertThatThrownBy(() -> nodeB.createApiTokenClusterWide("mine", DATABASE, -1L, new JSONObject()))
          .isInstanceOf(ServerSecurityException.class)
          .hasMessageContaining("create API token 'mine'")
          .hasMessageContaining("changed locally")
          .hasMessageContaining(ApiTokenConfiguration.FILE_NAME)
          .hasMessageNotContaining("retry the request");

      assertThat(tokenNames(nodeA)).doesNotContain("mine", "legacy");
    }

    /** The revocation pins its precondition the same way. */
    @Test
    void aTokenRevocationFromTheChangedNodeNamesTheLocalChangeToo() throws IOException {
      seedTokens();
      final JSONObject minted = nodeA.createApiTokenClusterWide("shared", DATABASE, -1L, new JSONObject());
      nodeB = restartWithLegacyTokenFile(nodeB, "b");

      assertThatThrownBy(() -> nodeB.deleteApiTokenClusterWide(minted.getString("tokenHash")))
          .isInstanceOf(ServerSecurityException.class)
          .hasMessageContaining("changed locally")
          .hasMessageNotContaining("retry the request");

      assertThat(tokenNames(nodeA)).contains("shared");
    }

    /** And the same way out: the mint through the other node lands everywhere and reconverges node B. */
    @Test
    void theSameTokenMintThroughAnotherNodeReconvergesTheChangedNode() throws IOException {
      seedTokens();
      nodeB = restartWithLegacyTokenFile(nodeB, "b");

      nodeA.createApiTokenClusterWide("mine", DATABASE, -1L, new JSONObject());

      assertThat(tokenNames(nodeB)).contains("mine").doesNotContain("legacy");
      assertThat(nodeB.apiTokensFingerprint()).isEqualTo(nodeA.apiTokensFingerprint());
    }

    // ===========================================================================================
    // Fixtures
    // ===========================================================================================

    /** Installs node A's user list on both nodes as a replicated entry: the cluster's baseline. */
    private void seedUsers() {
      cluster.submitUsers(nodeA.getUsersJsonPayload(), null);
      assertThat(nodeB.usersFingerprint()).isEqualTo(nodeA.usersFingerprint());
    }

    /** Installs node A's token document, with one token in it, on both nodes: the cluster's baseline. */
    private void seedTokens() {
      nodeA.createApiTokenClusterWide("baseline", DATABASE, -1L, new JSONObject());
      assertThat(nodeB.apiTokensFingerprint()).isEqualTo(nodeA.apiTokensFingerprint());
    }

    /** Stops {@code node}, appends a user to its {@code server-users.jsonl} by hand, and starts it again. */
    private ServerSecurity restartWithEditedUserFile(final ServerSecurity node, final String name, final String extraUser)
        throws IOException {
      final JSONObject extra = userJson(node, extraUser);
      node.stopService();

      final File file = new File(configPath(name), SecurityUserFileRepository.FILE_NAME);
      Files.writeString(file.toPath(), Files.readString(file.toPath(), DatabaseFactory.getDefaultCharset())
          + extra + "\n", DatabaseFactory.getDefaultCharset());

      final ServerSecurity restarted = restart(node, name);
      assertThat(restarted.getUsers()).as("the fixture's premise: the restart loaded the local edit").contains(extraUser);
      assertThat(restarted.usersFingerprint()).isNotEqualTo(nodeA.usersFingerprint());
      return restarted;
    }

    /**
     * Stops {@code node}, adds a legacy plaintext token to its {@code server-api-tokens.json} - a backup from before
     * tokens were hashed - and starts it again, which migrates that entry node-locally.
     */
    private ServerSecurity restartWithLegacyTokenFile(final ServerSecurity node, final String name) throws IOException {
      node.stopService();

      final File file = new File(configPath(name), ApiTokenConfiguration.FILE_NAME);
      final JSONObject document = new JSONObject(Files.readString(file.toPath(), DatabaseFactory.getDefaultCharset()));
      document.getJSONArray("tokens").put(new JSONObject()
          .put("token", "at-0123456789abcdef0123456789abcdef")
          .put("name", "legacy")
          .put("database", DATABASE)
          .put("createdAt", 0L)
          .put("expiresAt", -1L)
          .put("permissions", new JSONObject()));
      Files.writeString(file.toPath(), document.toString(2), DatabaseFactory.getDefaultCharset());

      final ServerSecurity restarted = restart(node, name);
      assertThat(tokenNames(restarted)).as("the fixture's premise: the restart loaded and migrated the legacy token")
          .contains("legacy");
      assertThat(restarted.apiTokensFingerprint()).isNotEqualTo(nodeA.apiTokensFingerprint());
      return restarted;
    }

    private static JSONObject userJson(final ServerSecurity node, final String name) {
      return new JSONObject()
          .put("name", name)
          .put("password", node.encodePassword(name + "-password"))
          .put("databases", new JSONObject());
    }

    private static List<String> tokenNames(final ServerSecurity node) {
      final JSONArray tokens = new JSONObject(node.getApiTokensJsonPayload()).getJSONArray("tokens");
      final List<String> names = new ArrayList<>(tokens.length());
      for (int i = 0; i < tokens.length(); i++)
        names.add(tokens.getJSONObject(i).getString("name"));
      return names;
    }

    /** Opens a fresh security service over {@code name}'s configuration directory: a node restart. */
    private ServerSecurity restart(final ServerSecurity previous, final String name) {
      final ServerSecurity restarted = open(name);
      cluster.replace(previous, restarted);
      return restarted;
    }

    private ServerSecurity node(final String name) {
      assertThat(new File(configPath(name)).mkdirs()).isTrue();
      final ServerSecurity security = open(name);
      cluster.join(security);
      return security;
    }

    private static String configPath(final String name) {
      return ROOT_PATH + "/" + name + "/config";
    }

    private ServerSecurity open(final String name) {
      final ContextConfiguration configuration = new ContextConfiguration();
      configuration.setValue(GlobalConfiguration.SERVER_ROOT_PATH, ROOT_PATH + "/" + name);
      configuration.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, ROOT_PASSWORD);
      // Never fires during a test: only a restart loads the files.
      configuration.setValue(GlobalConfiguration.SERVER_SECURITY_RELOAD_EVERY, 3_600_000);

      final FixtureServer server = new FixtureServer(configuration);
      final ServerSecurity security = new ServerSecurity(server, configuration, configPath(name));
      server.setSecurity(security);
      server.setHA(cluster);
      security.loadUsers();
      return security;
    }

    /**
     * A Raft log reduced to what this test needs: one ordered list of user and token entries, each node applying
     * every entry it has not applied yet - with the precondition the entry carried - before the next one is
     * evaluated. A node can be told to lag, which leaves it one entry behind until the next submission catches it
     * up, and every user submission can be preceded by a competing one, the way Raft orders another node's entry
     * first.
     */
    private static final class LogHAPlugin implements HAServerPlugin {
      private static final char USERS  = 'U';
      private static final char TOKENS = 'T';

      private final List<Object[]>       log     = new ArrayList<>();
      private final List<ServerSecurity> nodes   = new ArrayList<>();
      private final List<Integer>        applied = new ArrayList<>();
      private       ServerSecurity       lagging;
      private       Runnable             competing;
      int refusals;

      void join(final ServerSecurity node) {
        nodes.add(node);
        applied.add(log.size());
      }

      void replace(final ServerSecurity previous, final ServerSecurity next) {
        nodes.set(nodes.indexOf(previous), next);
      }

      void lagBehind(final ServerSecurity node) {
        lagging = node;
      }

      void beforeEachUserSubmit(final Runnable competing) {
        this.competing = competing;
      }

      boolean submitUsers(final String payload, final String precondition) {
        return submit(USERS, payload, precondition);
      }

      private boolean submit(final char kind, final String payload, final String precondition) {
        log.add(new Object[] { kind, payload, precondition });
        Boolean outcome = null;
        for (int n = 0; n < nodes.size(); n++) {
          final ServerSecurity node = nodes.get(n);
          if (node == lagging) {
            lagging = null;
            continue;
          }
          for (int i = applied.get(n); i < log.size(); i++) {
            final Object[] entry = log.get(i);
            final boolean installed = (char) entry[0] == USERS ?
                node.applyReplicatedUsers((String) entry[1], (String) entry[2]) :
                node.applyReplicatedApiTokens((String) entry[1], (String) entry[2]);
            if (i == log.size() - 1) {
              if (outcome == null)
                outcome = installed;
              assertThat(installed).as("every node must reach the same verdict on the same entry").isEqualTo(outcome);
            }
          }
          applied.set(n, log.size());
        }
        assertThat(outcome).as("at least one node that is not lagging must have evaluated the entry").isNotNull();
        if (!outcome)
          refusals++;
        return outcome;
      }

      @Override
      public boolean replicateSecurityUsers(final String usersJson, final String expectedFingerprint) {
        // Another node's entry, ordered first by the log. It goes through submitUsers() directly, so it is not raced.
        if (competing != null)
          competing.run();
        return submit(USERS, usersJson, expectedFingerprint);
      }

      @Override
      public boolean replicateSecurityApiTokens(final String apiTokensJson, final String expectedFingerprint) {
        return submit(TOKENS, apiTokensJson, expectedFingerprint);
      }

      @Override
      public void awaitLocalApply() {
        // Every submission already applies the log on every non-lagging node before it returns.
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
        return "a";
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
        return 2;
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
}
