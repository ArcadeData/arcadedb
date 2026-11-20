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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.FileManager;
import com.arcadedb.schema.Schema;
import com.arcadedb.security.SecurityDatabaseUser.DATABASE_ACCESS;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ClusterCapabilityNotReadyException;
import com.arcadedb.server.FakeArcadeDBServer;
import com.arcadedb.server.HAServerPlugin;
import com.arcadedb.server.ServerControlPlane;
import com.arcadedb.server.ServerException;
import com.arcadedb.server.monitor.HAReplicationMetrics;
import com.arcadedb.utility.FileUtils;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static com.arcadedb.utility.SubclassMocks.mock;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.Mockito.when;

/**
 * Applying a replicated security entry (users, groups, API tokens) on a node: a failed write, the authorization
 * a peer derives from it, a refusal by the capability gate, the first entry of a cluster and the replay after a restart.
 */
class ReplicatedSecurityEntryTest {
  /**
   * Regression test for the security half of issue #7137.
   * <p>
   * Making a users-file write failure non-fatal to the node (so a full or read-only config volume stops
   * crash-looping the pod) must not turn into "this node keeps honouring credentials the operator has just
   * revoked". Returning early on the write failure would do exactly that: the entry committed cluster-wide, every
   * other node applied it, and this one would go on authenticating against the PREVIOUS list for as long as the
   * volume stayed broken.
   * <p>
   * So the new list is published in memory FIRST and the persistence failure is reported afterwards. The
   * revocation takes effect here immediately; what is outstanding is only its durability.
   * <p>
   * That durability cannot be counted on to come back on its own, which is what this javadoc used to say it could
   * (issue #7302, follow-up to #7227). The failing entry does not record itself as applied, but it does not halt
   * the node either, so the next entry moves the applied counter past it and any snapshot taken afterwards puts it
   * out of replay range for good. The operator's instruction does not depend on which of those happened: reissue
   * the user change on the leader once the volume is fixed. See {@code ArcadeStateMachine.applySecurityUsersEntry},
   * {@code SecurityEntryApplyFailureTest} and
   * {@code SecurityEntryApplyFailureTest}, which pin the two halves of it.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class UsersAppliedOnWriteFailure {

    private static final String CONFIG_PATH = "target/test-security-7137";
    private static final String PASSWORD    = "new-password-secret";

    @BeforeEach
    void setUp() {
      GlobalConfiguration.SERVER_SECURITY_SALT_ITERATIONS.setValue(1000);
      final File dir = new File(CONFIG_PATH);
      if (dir.exists())
        FileUtils.deleteRecursively(dir);
      dir.mkdirs();
    }

    @AfterEach
    void tearDown() {
      GlobalConfiguration.SERVER_SECURITY_SALT_ITERATIONS.reset();
      FileUtils.deleteRecursively(new File(CONFIG_PATH));
    }

    /**
     * Makes the users file unwritable in a way that is deterministic on every platform and does not depend on
     * running as an unprivileged user: the target path is a non-empty directory, so the publishing rename cannot
     * replace it. That stands in for the full / read-only / bad-mode volume of the report.
     */
    private static void makeUsersFileUnwritable() throws Exception {
      final File asDirectory = new File(CONFIG_PATH, SecurityUserFileRepository.FILE_NAME);
      FileUtils.deleteRecursively(asDirectory);
      assertThat(asDirectory.mkdirs()).isTrue();
      assertThat(new File(asDirectory, "occupied").createNewFile()).isTrue();
    }

    private static String usersPayload(final ServerSecurity security) {
      return new JSONArray()
          .put(new JSONObject()
              .put("name", "alice")
              .put("password", security.encodePassword(PASSWORD))
              .put("databases", new JSONObject().put("*", new JSONArray().put("admin"))))
          .toString();
    }

    @Test
    void aWriteFailureIsReportedButTheNewListIsStillEnforced() throws Exception {
      final ServerSecurity security = new ServerSecurity(null, new ContextConfiguration(), CONFIG_PATH);
      makeUsersFileUnwritable();

      assertThatThrownBy(() -> security.applyReplicatedUsers(usersPayload(security)))
          .as("the caller must learn the list did not reach disk")
          .isInstanceOf(ServerException.class)
          .hasMessageContaining(SecurityUserFileRepository.FILE_NAME);

      // The crux: the entry was applied in memory anyway, so a password change or a drop the operator just made
      // is effective on this node NOW rather than after the volume is fixed.
      assertThatCode(() -> assertThat(security.authenticate("alice", PASSWORD, null).getName()).isEqualTo("alice"))
          .doesNotThrowAnyException();
    }

    /**
     * The other ordering the fix depends on: a payload this node cannot turn into users must leave BOTH the file
     * and the in-memory map untouched. Writing first would persist a snapshot the node itself could not load, and
     * the caller converts every exception out of this method into a non-halting one - so a half-applied state
     * here would be reported as "the node stayed up safely".
     */
    @Test
    void aPayloadThatCannotBeTurnedIntoUsersChangesNothing() {
      final ServerSecurity security = new ServerSecurity(null, new ContextConfiguration(), CONFIG_PATH);
      security.applyReplicatedUsers(usersPayload(security));
      final File usersFile = new File(CONFIG_PATH, SecurityUserFileRepository.FILE_NAME);
      final long sizeBefore = usersFile.length();

      // No "name": ServerSecurityUser cannot be constructed from it.
      final String malformed = new JSONArray().put(new JSONObject().put("nome", "bob")).toString();

      assertThatThrownBy(() -> security.applyReplicatedUsers(malformed)).isInstanceOf(Exception.class);

      assertThat(usersFile.length()).as("a payload that cannot be loaded must not be persisted").isEqualTo(sizeBefore);
      assertThat(security.authenticate("alice", PASSWORD, null).getName())
          .as("and must not disturb the users already in memory")
          .isEqualTo("alice");
    }

    /** Control: with a writable file the same payload applies cleanly and reports nothing. */
    @Test
    void aWritableFileAppliesWithoutReportingAFailure() {
      final ServerSecurity security = new ServerSecurity(null, new ContextConfiguration(), CONFIG_PATH);

      assertThatCode(() -> security.applyReplicatedUsers(usersPayload(security))).doesNotThrowAnyException();
      assertThat(security.authenticate("alice", PASSWORD, null).getName()).isEqualTo("alice");
      assertThat(new File(CONFIG_PATH, SecurityUserFileRepository.FILE_NAME)).isFile();
    }
  }

  /**
   * Issue #7373, the half a JSON assertion cannot reach: what a replicated group document does to an
   * <b>authorization decision</b> on a peer, rather than to a file.
   * <p>
   * The two halves are deliberately separate in the fix, and this test is what makes that separation visible.
   * {@link ServerSecurity#applyReplicatedGroups} installs the document; it does not walk the databases ON the
   * calling thread, because that thread is the Raft state-machine apply thread and {@link ServerSecurity#updateSchema}
   * opens and walks every database. The cached {@code ServerSecurityDatabaseUser} of an already-connected principal
   * therefore keeps answering from the PREVIOUS document until a refresh runs, and the refresh is somebody else's
   * call: {@code ServerControlPlane} makes it inline on the node that served the request, and on a peer issue #7510
   * hands it to a dedicated worker (with the group file's watcher left behind it as the safety net).
   * <p>
   * So both assertions below matter, and the first one is not a bug being pinned as a feature - it is the statement
   * that the apply itself stays non-blocking, and it will fail if someone calls {@code updateSchema} directly from
   * it. The mocked server here reports no open database, so #7510's asynchronous refresh has nothing to walk and
   * the assertion is not a race against it.
   */
  @Nested
  class GroupAuthorizationOnPeer {

    private static final String CONFIG_PATH = "target/test-security-7373-authz";
    private static final String DATABASE    = "graph";
    private static final String GROUP       = "editors";

    private ServerSecurity security;

    @BeforeEach
    void setUp() {
      GlobalConfiguration.SERVER_SECURITY_SALT_ITERATIONS.setValue(1000);
      final File dir = new File(CONFIG_PATH);
      if (dir.exists())
        FileUtils.deleteRecursively(dir);
      assertThat(dir.mkdirs()).isTrue();

      // A mocked server rather than null: ServerSecurityUser resolves its permissions through
      // server.getSecurity().getDatabaseGroupsConfiguration(...), so the principal has to be able to see the very
      // store the replicated document is applied to. getHA() answers null, so this node is not in a cluster and
      // createUser() takes the local path - which is what a peer applying someone else's entry looks like.
      final FakeArcadeDBServer server = FakeArcadeDBServer.create();
      security = new ServerSecurity(server, new ContextConfiguration(), CONFIG_PATH);
      server.security(security);
    }

    @AfterEach
    void tearDown() {
      if (security != null)
        security.stopService();
      GlobalConfiguration.SERVER_SECURITY_SALT_ITERATIONS.reset();
      FileUtils.deleteRecursively(new File(CONFIG_PATH));
    }

    @Test
    void aReplicatedRevocationDeniesTheCachedPrincipalOnceTheRefreshRuns() {
      final DatabaseInternal database = mockDatabase();

      // The peer starts with the grant in force, and a principal that has already resolved its permissions.
      security.applyReplicatedGroups(documentGranting(new JSONArray().put("updateSchema")));
      final ServerSecurityUser alice = security.createUser(new JSONObject()
          .put("name", "alice")
          .put("password", security.encodePassword("alice-password"))
          .put("databases", new JSONObject().put(DATABASE, new JSONArray().put(GROUP))));

      final ServerSecurityDatabaseUser cached = alice.getDatabaseUser(database);
      assertThat(cached.requestAccessOnDatabase(DATABASE_ACCESS.UPDATE_SCHEMA))
          .as("the grant is in force before the revocation").isTrue();

      // The revocation arrives as a replicated entry, exactly as the Raft state machine delivers it.
      security.applyReplicatedGroups(documentGranting(new JSONArray()));

      assertThat(security.getDatabaseGroupsConfiguration(DATABASE).getJSONObject(GROUP).getJSONArray("access"))
          .as("the document itself is revoked immediately on the peer").isEmpty();

      // ...but the apply itself does not walk the databases, so the principal that was already connected still
      // answers from its cache. This is what would fail if someone called updateSchema() directly from the
      // state-machine apply thread; #7510's off-thread refresh finds no open database on this mocked server.
      assertThat(alice.getDatabaseUser(database))
          .as("getDatabaseUser caches per database, and nothing in the apply clears it").isSameAs(cached);
      assertThat(cached.requestAccessOnDatabase(DATABASE_ACCESS.UPDATE_SCHEMA))
          .as("the cached principal has not been refreshed yet").isTrue();

      // The refresh is the second half: ServerControlPlane runs it inline on the serving node, and on a peer the
      // worker added by #7510 does. Once it has run, the replicated revocation is enforced against the live
      // principal.
      security.updateSchema(database);

      assertThat(cached.requestAccessOnDatabase(DATABASE_ACCESS.UPDATE_SCHEMA))
          .as("after the refresh the replicated revocation denies the principal that was already connected")
          .isFalse();
    }

    /** The grant direction, so the test cannot pass by denying everything. */
    @Test
    void aReplicatedGrantAllowsThePrincipalOnceTheRefreshRuns() {
      final DatabaseInternal database = mockDatabase();

      security.applyReplicatedGroups(documentGranting(new JSONArray()));
      final ServerSecurityUser alice = security.createUser(new JSONObject()
          .put("name", "alice")
          .put("password", security.encodePassword("alice-password"))
          .put("databases", new JSONObject().put(DATABASE, new JSONArray().put(GROUP))));

      final ServerSecurityDatabaseUser cached = alice.getDatabaseUser(database);
      assertThat(cached.requestAccessOnDatabase(DATABASE_ACCESS.UPDATE_SCHEMA)).isFalse();

      security.applyReplicatedGroups(documentGranting(new JSONArray().put("updateSchema")));
      security.updateSchema(database);

      assertThat(cached.requestAccessOnDatabase(DATABASE_ACCESS.UPDATE_SCHEMA)).isTrue();
    }

    /** A whole group document, in the shape a {@code SECURITY_GROUPS_ENTRY} carries. */
    private static String documentGranting(final JSONArray databaseAccess) {
      final JSONObject group = new JSONObject()
          .put("access", databaseAccess)
          .put("resultSetLimit", -1L)
          .put("readTimeout", -1L)
          .put("types", new JSONObject().put("*", new JSONObject().put("access",
              new JSONArray().put("createRecord").put("readRecord").put("updateRecord").put("deleteRecord"))));

      return new JSONObject()
          .put("version", ServerSecurity.LATEST_VERSION)
          .put("databases", new JSONObject().put(DATABASE,
              new JSONObject().put("groups", new JSONObject().put(GROUP, group))))
          .toString();
    }

    private static DatabaseInternal mockDatabase() {
      final FileManager fileManager = mock(FileManager.class);
      when(fileManager.getFiles()).thenReturn(List.of());

      final Schema schema = mock(Schema.class);
      when(schema.getTypes()).thenReturn(List.of());

      final DatabaseInternal database = mock(DatabaseInternal.class);
      when(database.getName()).thenReturn(DATABASE);
      when(database.getFileManager()).thenReturn(fileManager);
      when(database.getSchema()).thenReturn(schema);
      return database;
    }
  }

  /**
   * Issue #7511, the half that lives above the HA plugin: what an operator-facing entry point does when the
   * cluster-capability interlock refuses to replicate a group or API-token change.
   * <p>
   * The interlock itself is pinned in {@code [theTwoEntryTypesOfIssue7373HaveStableCapabilityTokens]}, in the module that owns it.
   * What is asserted here is everything that has to be true of the caller for the refusal to be SAFE rather than
   * merely loud: that the refusal reaches the transport unchanged (it is the exception both transports map to a
   * status), and that no entry point leaves half a change behind on the node that served the request. A mint that
   * installed the token locally before the refusal, or a revocation that removed it, would turn one node into the
   * divergence the gate exists to prevent - by a different route.
   * <p>
   * The fixture is {@code ClusterWideGroupsAndTokensTest}'s, with the plugin replaced by one that refuses
   * the way {@code RaftHAPlugin} does when a peer has not advertised the capability.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class GateRefusal {

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
  @Nested
  class FirstReplicatedEntry {

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

    /**
     * The restart case, raised by the code review on PR #7748. An in-memory "this node has converged" flag is reset by
     * every restart, so a node that had long since converged would go back to "cannot judge" on each start and
     * install the very next entry unconditionally - the identical bypass, on a far more common trigger (a routine
     * restart, a rolling upgrade) than the never-seeded cluster the rule exists for. What is recorded is therefore
     * on disk, and this pins that a restarted node still refuses a stale precondition.
     */
    @Test
    void aRestartedNodeStillRefusesAStalePrecondition() {
      final String firstEntry = usersOf(nodeA, "alice");
      final String staleFingerprint = nodeA.usersFingerprint();
      nodeA.applyReplicatedUsers(firstEntry, staleFingerprint);
      nodeB.applyReplicatedUsers(firstEntry, staleFingerprint);

      nodeB = restart(nodeB, "b");

      assertThat(nodeB.getUser("alice")).as("the restarted node read back the document the entry installed").isNotNull();
      assertThat(nodeB.applyReplicatedUsers(usersOf(nodeA, "bob"), staleFingerprint))
          .as("the restart must not hand this node a free pass to install an entry its peers refuse")
          .isFalse();
    }

    /**
     * And the reason the baseline is the RECORDED fingerprint rather than the document in force (CodeRabbit on PR
     * #7748). A node whose file has drifted locally - hand-edited, restored from a backup - still answers the same
     * way its peers do about the same entry. Comparing against the live document instead would have this node
     * install a stale entry that every converged peer refuses, which restores users, grants or tokens the cluster
     * had already moved past: the identical divergence, reached from a node that is merely out of date.
     */
    @Test
    void aLocallyEditedDocumentDoesNotEscapeTheConcurrencyCheck() {
      final String firstEntry = usersOf(nodeA, "alice");
      final String staleFingerprint = nodeA.usersFingerprint();
      nodeA.applyReplicatedUsers(firstEntry, staleFingerprint);
      nodeB.applyReplicatedUsers(firstEntry, staleFingerprint);

      // A local mutation, the shape an operator editing server-users.jsonl leaves behind.
      nodeB.createUser(user(nodeB, "edited-in-locally", "localpassword"));
      nodeB = restart(nodeB, "b");

      final String supersededEntry = usersOf(nodeA, "bob");
      assertThat(nodeA.applyReplicatedUsers(supersededEntry, staleFingerprint))
          .as("the converged peer refuses it").isFalse();
      assertThat(nodeB.applyReplicatedUsers(supersededEntry, staleFingerprint))
          .as("and the drifted node refuses it identically, rather than installing what its peers rejected")
          .isFalse();
      assertThat(nodeB.getUser("alice"))
          .as("the change the stale entry would have reverted is still in force here too").isNotNull();
    }

    /** The same first-entry rule for the two documents issue #7373 added, which share {@code isSuperseded}. */
    @Test
    void theFirstReplicatedGroupAndTokenDocumentsAreInstalledOnEveryNode() {
      final JSONObject groups = new JSONObject(nodeA.getGroupsJsonPayload());
      assertThat(nodeB.applyReplicatedGroups(groups.toString(), "a-fingerprint-node-b-has-never-had")).isTrue();

      final JSONObject tokens = new JSONObject(nodeA.getApiTokensJsonPayload());
      assertThat(nodeB.applyReplicatedApiTokens(tokens.toString(), "a-fingerprint-node-b-has-never-had")).isTrue();
    }

    /**
     * And the rest of the rule for those two: once a replicated document HAS landed, a stale precondition is refused
     * there as well, and the refusal survives a restart. The three documents share {@code isSuperseded} but each
     * carries its own recorded fingerprint, so a users entry landing must not make the group document judgeable
     * (or the other way round).
     */
    @Test
    void aRestartedNodeStillRefusesAStaleGroupOrTokenPrecondition() {
      final String groupsDocument = nodeA.getGroupsJsonPayload();
      final String staleGroups = nodeA.groupsFingerprint();
      final String tokensDocument = nodeA.getApiTokensJsonPayload();
      final String staleTokens = nodeA.apiTokensFingerprint();

      nodeB.applyReplicatedGroups(groupsDocument, null);
      nodeB.applyReplicatedApiTokens(tokensDocument, null);

      // A users entry lands too: the three documents must stay independent of one another.
      nodeB.applyReplicatedUsers(usersOf(nodeA, "alice"), null);

      nodeB = restart(nodeB, "b");

      assertThat(nodeB.applyReplicatedGroups(groupsDocument, staleGroups + "-stale"))
          .as("a group precondition that is not the one the cluster installed is refused after a restart").isFalse();
      assertThat(nodeB.applyReplicatedApiTokens(tokensDocument, staleTokens + "-stale"))
          .as("and so is an API-token one").isFalse();
    }

    /** Stops {@code security} and opens a fresh one over the same configuration directory: a node restart. */
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

  /**
   * Issue #7752: a replicated security entry whose effects did not reach the disk is replayed from the Raft log after a
   * restart, and the replay has to reach the same decision every peer reached.
   * <p>
   * Two real {@link ServerSecurity} instances stand for two nodes; node B's configuration directory is made read-only
   * while it applies an entry - the volume of the sub-case recorded on the issue, where neither the document nor the
   * record of its fingerprint can be written, and nothing can be removed either. The HA half - that no snapshot
   * checkpoint covers such an entry, so the restart does replay it - is {@code SecurityEntryApplyFailureTest} in
   * the {@code ha-raft} module; this class pins the half that lives here: that the failure is reported, that the files a
   * restart reads back are the ones the replay needs, and that the retry repairs them once the volume is writable again.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class ReplayAfterFailedWrite {

    private static final String ROOT_PATH = "target/test-security-7752";

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
      configOf("b").toFile().setWritable(true);
      if (nodeA != null)
        nodeA.stopService();
      if (nodeB != null)
        nodeB.stopService();
      GlobalConfiguration.SERVER_SECURITY_SALT_ITERATIONS.reset();
      FileUtils.deleteRecursively(new File(ROOT_PATH));
    }

    /**
     * The defect, end to end. Before the fix the restart read back a stale document and, depending on what the failed
     * write had managed to remove, either no baseline (installing what the peers refused) or a baseline the cluster had
     * moved past with the entry that moved it already compacted away (refusing what the peers accepted, forever). Now the
     * entry is replayed, and the record left on disk is the baseline every peer judged it against.
     */
    @Test
    void anEntryReplayedAfterARestartIsJudgedAsEveryPeerJudgedIt() throws IOException {
      final String first = usersWith(nodeA, "alice");
      final String firstPrecondition = nodeA.usersFingerprint();
      assertThat(nodeA.applyReplicatedUsers(first, firstPrecondition)).isTrue();
      assertThat(nodeB.applyReplicatedUsers(first, firstPrecondition)).isTrue();

      final String second = usersWith(nodeA, "bob");
      final String secondPrecondition = nodeA.usersFingerprint();
      assertThat(nodeA.applyReplicatedUsers(second, secondPrecondition)).isTrue();

      makeReadOnly(configOf("b"));
      assertThatThrownBy(() -> nodeB.applyReplicatedUsers(second, secondPrecondition))
          .as("the list is in force, only its durability failed (issue #7137)")
          .isInstanceOf(ReplicatedUsersPersistenceException.class);
      assertThat(nodeB.usersFingerprint()).as("in force on B").isEqualTo(nodeA.usersFingerprint());
      assertThat(nodeB.isReplicatedDocumentDurable(ReplicatedSecurityFingerprintRepository.USERS))
          .as("reported, so the HA layer keeps the entry replayable").isFalse();
      assertThat(nodeB.retryReplicatedSecurityPersistence()).as("the volume is still read-only").isFalse();
      assertThat(markerOf("b", ReplicatedSecurityFingerprintRepository.USERS))
          .as("the record left on disk is the baseline right before the failed entry").isEqualTo(secondPrecondition);

      // The process dies before the volume is fixed: what B holds in memory is lost, the Raft log is not.
      configOf("b").toFile().setWritable(true);
      nodeB = restart(nodeB, "b");
      assertThat(nodeB.usersFingerprint()).as("the restart read back the stale list").isNotEqualTo(nodeA.usersFingerprint());

      assertThat(nodeB.applyReplicatedUsers(second, secondPrecondition))
          .as("the replayed entry is accepted, as every peer accepted it").isTrue();
      assertThat(nodeB.usersFingerprint()).isEqualTo(nodeA.usersFingerprint());
      assertThat(nodeB.isReplicatedDocumentDurable(ReplicatedSecurityFingerprintRepository.USERS)).isTrue();

      // And B judges the next race exactly like A: a precondition the cluster has moved past is refused on both.
      final String stale = usersWith(nodeA, "carol");
      assertThat(nodeA.applyReplicatedUsers(stale, secondPrecondition)).isFalse();
      assertThat(nodeB.applyReplicatedUsers(stale, secondPrecondition)).isFalse();
    }

    /**
     * Without a restart the retry is what repairs the files, so the Raft log stops being pinned once the volume is
     * writable again instead of at the next security change: every document kind and the record are written from what is
     * in force.
     */
    @Test
    void theRetryWritesEveryDocumentInForceOnceTheVolumeIsWritableAgain() throws IOException {
      final String users = usersWith(nodeA, "alice");
      final String tokens = new JSONObject().put("version", 1).put("tokens", new JSONArray()).toString();

      makeReadOnly(configOf("b"));
      assertThatThrownBy(() -> nodeB.applyReplicatedUsers(users)).isInstanceOf(ReplicatedUsersPersistenceException.class);
      assertThatThrownBy(() -> nodeB.applyReplicatedApiTokens(tokens))
          .isInstanceOf(ReplicatedSecurityConfigPersistenceException.class);
      assertThat(nodeB.isReplicatedDocumentDurable(ReplicatedSecurityFingerprintRepository.USERS)).isFalse();
      assertThat(nodeB.isReplicatedDocumentDurable(ReplicatedSecurityFingerprintRepository.API_TOKENS)).isFalse();
      assertThat(nodeB.isReplicatedDocumentDurable(ReplicatedSecurityFingerprintRepository.GROUPS))
          .as("never installed here, so nothing of it can be lost").isTrue();

      configOf("b").toFile().setWritable(true);
      assertThat(nodeB.retryReplicatedSecurityPersistence()).isTrue();
      assertThat(nodeB.isReplicatedDocumentDurable(ReplicatedSecurityFingerprintRepository.USERS)).isTrue();
      assertThat(nodeB.isReplicatedDocumentDurable(ReplicatedSecurityFingerprintRepository.API_TOKENS)).isTrue();

      final String inForce = nodeB.usersFingerprint();
      nodeB = restart(nodeB, "b");
      assertThat(nodeB.usersFingerprint()).as("the list written by the retry is the one a restart reads").isEqualTo(inForce);
      assertThat(markerOf("b", ReplicatedSecurityFingerprintRepository.USERS)).isEqualTo(inForce);
    }

    /**
     * What keeps the Raft log from being compacted is visible to a scrape before it is visible as disk pressure:
     * {@code arcadedb.ha.security.documents_not_durable} counts the documents in force but not on disk, and returns to 0
     * on its own when the retry succeeds.
     */
    @Test
    void theDocumentsNotOnDiskArePublishedAsAGauge() {
      final SimpleMeterRegistry registry = new SimpleMeterRegistry();
      final FixtureServer server = new FixtureServer(new ContextConfiguration());
      server.setSecurity(nodeB);
      try (final HAReplicationMetrics metrics = new HAReplicationMetrics(server)) {
        metrics.bindTo(registry);
        assertThat(registry.find("arcadedb.ha.security.documents_not_durable").gauge().value()).isZero();

        makeReadOnly(configOf("b"));
        assertThatThrownBy(() -> nodeB.applyReplicatedUsers(usersWith(nodeA, "alice")))
            .isInstanceOf(ReplicatedUsersPersistenceException.class);
        assertThat(registry.find("arcadedb.ha.security.documents_not_durable").gauge().value()).isEqualTo(1.0);

        configOf("b").toFile().setWritable(true);
        assertThat(nodeB.retryReplicatedSecurityPersistence()).isTrue();
        assertThat(registry.find("arcadedb.ha.security.documents_not_durable").gauge().value()).isZero();
      } finally {
        registry.close();
      }
    }

    /**
     * The narrower failure, where only the compare-and-set record cannot be written and the document itself is: the
     * document is in force and on disk, the baseline a restart would judge the next entry against is not. That alone must
     * keep the entry replayable, and the record is written again by the retry once it can be.
     */
    @Test
    void aRecordThatCannotBeWrittenAloneStillReportsTheDocumentNotDurable() throws IOException {
      // A non-empty directory where the record file belongs: the record's rename fails, every document write succeeds
      final Path record = configOf("b").resolve(ReplicatedSecurityFingerprintRepository.FILE_NAME);
      Files.deleteIfExists(record);
      Files.createDirectories(record);
      final Path blocker = record.resolve("blocker");
      Files.writeString(blocker, "x", StandardCharsets.UTF_8);

      nodeB.applyReplicatedUsers(usersWith(nodeA, "alice"));
      assertThat(nodeB.isReplicatedDocumentDurable(ReplicatedSecurityFingerprintRepository.USERS))
          .as("the list reached the disk, its record did not, and the record is what a restart judges from").isFalse();
      assertThat(nodeB.retryReplicatedSecurityPersistence()).isFalse();

      Files.delete(blocker);
      Files.delete(record);
      assertThat(nodeB.retryReplicatedSecurityPersistence()).isTrue();
      assertThat(nodeB.isReplicatedDocumentDurable(ReplicatedSecurityFingerprintRepository.USERS)).isTrue();
      assertThat(markerOf("b", ReplicatedSecurityFingerprintRepository.USERS)).isEqualTo(nodeB.usersFingerprint());
    }

    private static void makeReadOnly(final Path directory) {
      // Root ignores directory permissions, so on such a runner there is no read-only volume to simulate
      assumeTrue(directory.toFile().setWritable(false) && !Files.isWritable(directory),
          "the configuration directory cannot be made read-only for this user");
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
}
