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
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * The replicated-security marker ({@code server-security-cluster.json}): what a failed write leaves behind, which
 * value each {@code applyReplicated*} writes, and the unconverged-documents signal read off it.
 */
class ReplicatedSecurityFingerprintMarkerTest {
  /**
   * Issue #7536, as issue #7752 reshaped it: what a failed write of {@code server-security-cluster.json} leaves on disk,
   * and what it reports.
   * <p>
   * #7536 found that a failed write left the PREVIOUS record behind, and that a restart then judged its peers' entries
   * against a baseline one change out of date - refusing, forever, the very entry they accepted, because the entry that
   * moved the baseline forward could already have been compacted out of the Raft log. It answered by REMOVING the file.
   * <p>
   * Issue #7752 moved the guarantee into the Raft log instead: {@link ReplicatedSecurityFingerprintRepository#isDurable}
   * reports the failed write, and the HA layer keeps the entry that installed the document out of every snapshot
   * checkpoint until it is durable, so a restart REPLAYS it. Against a replay the previous record is exactly the right
   * baseline - the one every peer judged that entry against - while no record at all is the harmful one: the file holds
   * all three kinds, and a replay judged with no baseline installs the entries its peers refused. So a failed write now
   * leaves the previous record in place and says so; these tests pin that for each kind, plus the retry.
   *
   * @see ReplicatedSecurityFingerprintRepositoryTest for the load-side fail-open branches
   */
  @Nested
  class FailedWriteKeepsPreviousRecord {

    @TempDir
    private Path configDir;

    @Test
    void aFailedUsersRecordWriteKeepsThePreviousRecordAndReportsItNotDurable() {
      assertPreviousRecordSurvives(ReplicatedSecurityFingerprintRepository.USERS);
    }

    @Test
    void aFailedGroupsRecordWriteKeepsThePreviousRecordAndReportsItNotDurable() {
      assertPreviousRecordSurvives(ReplicatedSecurityFingerprintRepository.GROUPS);
    }

    @Test
    void aFailedApiTokensRecordWriteKeepsThePreviousRecordAndReportsItNotDurable() {
      assertPreviousRecordSurvives(ReplicatedSecurityFingerprintRepository.API_TOKENS);
    }

    /**
     * The three kinds share one file, so a failed write of one must not cost the others their baseline: a replay judged
     * without one would install what the peers refused for a kind whose own write never failed.
     */
    @Test
    void aFailedWriteOfOneKindKeepsTheOtherKindsDurable() {
      final FailableFingerprintRepository repository = new FailableFingerprintRepository(configDir.toString());
      repository.record(ReplicatedSecurityFingerprintRepository.USERS, "fp-users-1");
      repository.record(ReplicatedSecurityFingerprintRepository.GROUPS, "fp-groups-1");

      repository.failWrites = true;
      repository.record(ReplicatedSecurityFingerprintRepository.API_TOKENS, "fp-tokens-1");

      assertThat(repository.isDurable(ReplicatedSecurityFingerprintRepository.API_TOKENS)).isFalse();
      assertThat(repository.isDurable(ReplicatedSecurityFingerprintRepository.USERS)).isTrue();
      assertThat(repository.isDurable(ReplicatedSecurityFingerprintRepository.GROUPS)).isTrue();

      final ReplicatedSecurityFingerprintRepository reopened =
          new ReplicatedSecurityFingerprintRepository(configDir.toString());
      assertThat(reopened.get(ReplicatedSecurityFingerprintRepository.USERS)).isEqualTo("fp-users-1");
      assertThat(reopened.get(ReplicatedSecurityFingerprintRepository.GROUPS)).isEqualTo("fp-groups-1");
      assertThat(reopened.get(ReplicatedSecurityFingerprintRepository.API_TOKENS))
          .as("never recorded on disk, so a replay of its entry installs it like every peer did").isNull();
    }

    /**
     * The running node is unaffected: it applied the document, so it holds the cluster's baseline in memory and goes
     * on judging with it.
     */
    @Test
    void theValueStaysInForceInMemoryAfterAFailedWrite() {
      final FailableFingerprintRepository repository = new FailableFingerprintRepository(configDir.toString());
      repository.record(ReplicatedSecurityFingerprintRepository.USERS, "fp-users-1");

      repository.failWrites = true;
      repository.record(ReplicatedSecurityFingerprintRepository.USERS, "fp-users-2");

      assertThat(repository.get(ReplicatedSecurityFingerprintRepository.USERS))
          .as("this node installed the document, so it judges from it").isEqualTo("fp-users-2");
    }

    /**
     * The re-record that used to be skipped. {@code record()} writes nothing when the fingerprint has not moved, which
     * keeps the Ratis replay of already-applied entries off the fsync path. After a write that FAILED, "has not moved" is
     * true of memory and false of the disk, so the skip tracks what reached the disk, not what is in memory.
     */
    @Test
    void aRepeatOfTheSameFingerprintIsRetriedAfterAFailedWrite() {
      final FailableFingerprintRepository repository = new FailableFingerprintRepository(configDir.toString());

      repository.failWrites = true;
      repository.record(ReplicatedSecurityFingerprintRepository.USERS, "fp-users-1");

      repository.failWrites = false;
      repository.record(ReplicatedSecurityFingerprintRepository.USERS, "fp-users-1");

      assertThat(repository.isDurable(ReplicatedSecurityFingerprintRepository.USERS)).isTrue();
      assertThat(new ReplicatedSecurityFingerprintRepository(configDir.toString())
          .get(ReplicatedSecurityFingerprintRepository.USERS))
          .as("the volume came back and the very next apply of the same document had to re-record it")
          .isEqualTo("fp-users-1");
    }

    /**
     * {@link ReplicatedSecurityFingerprintRepository#flush()} is the retry issue #7752 runs while the Raft log is pinned:
     * it writes what is in force without waiting for another entry, and is a no-op once everything is on disk.
     */
    @Test
    void flushWritesWhatIsInForceOnceTheVolumeIsWritableAgain() {
      final FailableFingerprintRepository repository = new FailableFingerprintRepository(configDir.toString());
      repository.record(ReplicatedSecurityFingerprintRepository.USERS, "fp-users-1");

      repository.failWrites = true;
      repository.record(ReplicatedSecurityFingerprintRepository.USERS, "fp-users-2");
      assertThat(repository.flush()).as("the volume is still failing").isFalse();
      assertThat(repository.isDurable(ReplicatedSecurityFingerprintRepository.USERS)).isFalse();

      repository.failWrites = false;
      assertThat(repository.flush()).isTrue();
      assertThat(repository.isDurable(ReplicatedSecurityFingerprintRepository.USERS)).isTrue();
      assertThat(new ReplicatedSecurityFingerprintRepository(configDir.toString())
          .get(ReplicatedSecurityFingerprintRepository.USERS)).isEqualTo("fp-users-2");

      repository.writes = 0;
      assertThat(repository.flush()).isTrue();
      assertThat(repository.writes).as("nothing left to write, so nothing is written").isZero();
    }

    /**
     * A volume on which the record can be neither rewritten nor removed - the read-only sub-case recorded on issue #7752.
     * Nothing more can be done on the filesystem, so all that is required is that the apply the record came from is not
     * failed by it ({@code record()} runs on the Raft apply thread, and a throw there is the crash loop of issue #7137),
     * and that the failure is reported so the HA layer keeps the entry replayable.
     */
    @Test
    void aRecordThatCannotBeWrittenNeverFailsTheApplyAndIsReportedNotDurable() throws IOException {
      // A non-empty directory where the marker file belongs: the rename over it fails.
      final Path occupied = configDir.resolve(ReplicatedSecurityFingerprintRepository.FILE_NAME);
      Files.createDirectories(occupied);
      Files.writeString(occupied.resolve("keep-me"), "x", StandardCharsets.UTF_8);

      final ReplicatedSecurityFingerprintRepository repository =
          new ReplicatedSecurityFingerprintRepository(configDir.toString());

      assertThatCode(() -> repository.record(ReplicatedSecurityFingerprintRepository.USERS, "fp-users"))
          .as("a record that cannot be written must still never fail a committed entry")
          .doesNotThrowAnyException();
      assertThat(repository.get(ReplicatedSecurityFingerprintRepository.USERS)).isEqualTo("fp-users");
      assertThat(repository.isDurable(ReplicatedSecurityFingerprintRepository.USERS)).isFalse();

      assertThat(Files.isDirectory(occupied)).as("the rename could not replace it").isTrue();
      assertThat(Files.exists(occupied.resolve("keep-me"))).as("and nothing tried to remove it").isTrue();
      try (final Stream<Path> entries = Files.list(configDir)) {
        assertThat(entries.map(entry -> entry.getFileName().toString()).filter(name -> name.endsWith(".tmp")))
            .as("and the temp file it wrote on the way out is not left behind either").isEmpty();
      }
    }

    /** Records one fingerprint successfully, then fails the next write, and asserts the first one is what survives. */
    private void assertPreviousRecordSurvives(final String documentKind) {
      final FailableFingerprintRepository repository = new FailableFingerprintRepository(configDir.toString());
      repository.record(documentKind, "fp-1");
      assertThat(repository.isDurable(documentKind)).isTrue();

      repository.failWrites = true;
      repository.record(documentKind, "fp-2");

      assertThat(repository.isDurable(documentKind))
          .as("what is in force is not what a restart would read back, which is what pins the Raft log").isFalse();
      assertThat(new ReplicatedSecurityFingerprintRepository(configDir.toString()).get(documentKind))
          .as("the baseline a replay of the 'fp-2' entry must be judged against, as every peer judged it").isEqualTo("fp-1");
    }

    /**
     * The repository with its one atomic write made injectable. A real object rather than a mock: only the write step
     * is replaced, and everything this test is about - the failure policy, the retry bookkeeping - is the production code.
     */
    private static final class FailableFingerprintRepository extends ReplicatedSecurityFingerprintRepository {
      private boolean failWrites;
      private int     writes;

      private FailableFingerprintRepository(final String securityConfPath) {
        super(securityConfPath);
      }

      @Override
      void writeAtomically(final Path target, final byte[] bytes) throws IOException {
        ++writes;
        if (failWrites)
          throw new IOException("No space left on device");
        super.writeAtomically(target, bytes);
      }
    }
  }

  /**
   * The other half of issue #7536: the three {@code applyReplicated*} methods are the only writers of the
   * replicated-security marker, and the failure policy of {@code ReplicatedSecurityFingerprintRepository} is worth
   * nothing if one of them never reaches it, or reaches it with the wrong document kind.
   * <p>
   * {@code ReplicatedSecurityEntryTest.FirstReplicatedEntry} pins that a restarted node still REFUSES a stale precondition,
   * which proves the marker survives; it does not pin WHICH value reached the disk, so a call site recording the
   * fingerprint of a different document than the one it installed would pass it. These tests read the file back and
   * compare it with the document actually in force, per kind.
   * <p>
   * The last test pins what a node with NO marker does - one whose file was removed by hand, or never written: it
   * installs and rejoins its peers' baseline on that entry. A failed write no longer leaves that state behind; since
   * issue #7752 it keeps the previous record and the Raft log keeps the entry replayable, see
   * {@code ReplicatedSecurityEntryTest.ReplayAfterFailedWrite}.
   *
   * @see ReplicatedSecurityFingerprintMarkerTest.FailedWriteKeepsPreviousRecord for the failure policy itself
   */
  @Nested
  class MarkerReachesDiskPerDocument {

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
     * A node that comes back with no marker installs the entry its peers install, ending up on their baseline.
     */
    @Test
    void aNodeThatLostItsMarkerRejoinsTheClusterBaselineOnTheNextEntry() throws IOException {
      final String firstEntry = usersWith(nodeA, "alice");
      final String firstPrecondition = nodeA.usersFingerprint();
      nodeA.applyReplicatedUsers(firstEntry, firstPrecondition);
      nodeB.applyReplicatedUsers(firstEntry, firstPrecondition);

      // A marker removed by hand.
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
  @Nested
  class UnconvergedDocuments {

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
}
