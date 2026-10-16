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
import com.arcadedb.server.monitor.HAReplicationMetrics;
import com.arcadedb.utility.FileUtils;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Issue #7752: a replicated security entry whose effects did not reach the disk is replayed from the Raft log after a
 * restart, and the replay has to reach the same decision every peer reached.
 * <p>
 * Two real {@link ServerSecurity} instances stand for two nodes; node B's configuration directory is made read-only
 * while it applies an entry - the volume of the sub-case recorded on the issue, where neither the document nor the
 * record of its fingerprint can be written, and nothing can be removed either. The HA half - that no snapshot
 * checkpoint covers such an entry, so the restart does replay it - is {@code Issue7752SecurityDurabilityFloorTest} in
 * the {@code ha-raft} module; this class pins the half that lives here: that the failure is reported, that the files a
 * restart reads back are the ones the replay needs, and that the retry repairs them once the volume is writable again.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7752ReplicatedSecurityReplayTest {

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
