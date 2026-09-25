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
import com.arcadedb.database.BootstrapFingerprint;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.database.ProtocolContext;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerDatabase;
import com.arcadedb.utility.FileUtils;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Regression tests for issue #8368: a follower had no local signal that a first-formation bootstrap pass was under
 * way until the committed {@code BOOTSTRAP_FINGERPRINT_ENTRY} reached its apply thread, so for the start of the
 * window it served - and reported ready with - a copy the pass could be about to reject.
 * <p>
 * The signal is the pass's own probe. The leader running the pass already reaches every follower with
 * {@code POST /api/v1/cluster/bootstrap-state}; the probe now says so, and the follower holds the databases it names
 * until the baseline for each is applied here, the leader reports the pass concluded, or a bounded deadline lapses.
 * Readiness and the request path stay locally decidable: the peer round trip is the one the pass already makes, and
 * nothing on a hot path waits on another node.
 */
class Issue8368BootstrapPassWindowTest {

  private static final String DB_DIR  = "./target/databases";
  private static final String DB_NAME = "test-8368-bootstrap-pass";
  private static final String DB_PATH = DB_DIR + "/" + DB_NAME;
  private static final long   HOLD_MS = 60_000L;

  private LocalDatabase localDb;

  @BeforeEach
  void setUp() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    FileUtils.deleteRecursively(new File(DB_DIR + "/.raft"));
    localDb = (LocalDatabase) new DatabaseFactory(DB_PATH).create();
    localDb.getSchema().createDocumentType("Seed");
    localDb.transaction(() -> localDb.newDocument("Seed").set("k", 1).save());
  }

  @AfterEach
  void tearDown() {
    ProtocolContext.clear();
    if (localDb != null && localDb.isOpen()) {
      if (localDb.isTransactionActive())
        localDb.rollbackAllNested();
      localDb.close();
    }
    FileUtils.deleteRecursively(new File(DB_PATH));
    FileUtils.deleteRecursively(new File(DB_DIR + "/.raft"));
  }

  private static ContextConfiguration configuration() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, DB_DIR);
    config.setValue(GlobalConfiguration.HA_SNAPSHOT_INSTALL_RETRIES, 0);
    config.setValue(GlobalConfiguration.HA_SNAPSHOT_INSTALL_RETRY_BASE_MS, 0L);
    config.setValue(GlobalConfiguration.NETWORK_USE_SSL, false);
    return config;
  }

  private ArcadeDBServer stubbedServer() {
    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.getConfiguration()).thenReturn(configuration());
    when(server.existsDatabase(DB_NAME)).thenReturn(true);
    when(server.getDatabase(DB_NAME)).thenReturn(new ServerDatabase(null, localDb));
    return server;
  }

  private ArcadeStateMachine stateMachine() {
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    sm.setServer(stubbedServer());
    return sm;
  }

  /** The baseline this node's own copy produces: it matches, so the apply bootstraps locally and installs nothing. */
  private RaftLogEntryCodec.DecodedEntry matchingBaseline() throws Exception {
    final String fingerprint = BootstrapFingerprint.compute(new File(localDb.getDatabasePath()));
    final ByteString encoded = RaftLogEntryCodec.encodeBootstrapFingerprintEntry(DB_NAME, fingerprint,
        localDb.getLastTransactionId());
    return RaftLogEntryCodec.decode(encoded);
  }

  /** What the pass's probe carries, run through the same handler logic the follower answers it with. */
  private static void receiveAnnounce(final ArcadeStateMachine sm, final String passId, final String... dbs) {
    PostBootstrapStateHandler.applyPassMarker(new JSONObject(BootstrapElection.announcePassBody(passId, List.of(dbs))),
        sm, HOLD_MS);
  }

  private static void receiveConclude(final ArcadeStateMachine sm, final String passId, final String... committed) {
    PostBootstrapStateHandler.applyPassMarker(new JSONObject(BootstrapElection.concludePassBody(passId, List.of(committed))),
        sm, HOLD_MS);
  }

  @Test
  void theProbeOfARunningPassTakesTheFollowerOutOfTheServiceBeforeTheBaselineArrives() {
    final ArcadeStateMachine sm = stateMachine();
    assertThat(sm.bootstrapWindowReason()).as("nothing is running yet").isNull();

    receiveAnnounce(sm, "pass-1", DB_NAME);

    assertThat(sm.isBootstrapPassPending(DB_NAME)).isTrue();
    assertThat(sm.bootstrapWindowReason())
        .as("the readiness probe reports the node unfit to serve from the moment the pass reaches it")
        .isNotNull()
        .contains("deciding which copy of 1 database(s)");
    assertThat(sm.bootstrapWindowReason())
        .as("GET /api/v1/ready is unauthenticated, so the body counts the databases and never names them")
        .doesNotContain(DB_NAME);
  }

  @Test
  void aProbeThatIsNotPartOfAPassHoldsNothing() {
    final ArcadeStateMachine sm = stateMachine();
    // The presence matrix, the database reconciler, the #8360 install and the divergence re-check all send "{}".
    PostBootstrapStateHandler.applyPassMarker(new JSONObject("{}"), sm, HOLD_MS);
    PostBootstrapStateHandler.applyPassMarker(null, sm, HOLD_MS);

    assertThat(sm.isBootstrapPassPending(DB_NAME)).isFalse();
    assertThat(sm.bootstrapWindowReason()).isNull();
  }

  @Test
  void applyingTheBaselineReleasesTheHold() throws Exception {
    final ArcadeStateMachine sm = stateMachine();
    receiveAnnounce(sm, "pass-1", DB_NAME);

    assertThatNoException().isThrownBy(() -> sm.applyBootstrapFingerprintEntry(matchingBaseline(), 7L));

    assertThat(sm.isBootstrapPassPending(DB_NAME)).as("the baseline decided: the copy on disk is the cluster's").isFalse();
    assertThat(sm.bootstrapWindowReason()).isNull();
  }

  @Test
  void anAnnounceThatArrivesAfterTheBaselineWasAppliedHoldsNothing() throws Exception {
    final ArcadeStateMachine sm = stateMachine();
    assertThatNoException().isThrownBy(() -> sm.applyBootstrapFingerprintEntry(matchingBaseline(), 7L));

    receiveAnnounce(sm, "pass-1", DB_NAME);

    assertThat(sm.isBootstrapPassPending(DB_NAME)).isFalse();
    assertThat(sm.bootstrapWindowReason()).isNull();
  }

  /**
   * A pass that commits no baseline for a database - every copy empty, or the elected source not holding it - never
   * sends the entry that would release it. The conclusion does, and it must not release one whose entry is still on
   * its way.
   */
  @Test
  void theConclusionReleasesWhatThePassDidNotCommitAndKeepsWhatIsStillInFlight() throws Exception {
    final ArcadeStateMachine sm = stateMachine();
    receiveAnnounce(sm, "pass-1", DB_NAME, "other-db");

    receiveConclude(sm, "pass-1", DB_NAME);

    assertThat(sm.isBootstrapPassPending("other-db")).as("no baseline is coming for it").isFalse();
    assertThat(sm.isBootstrapPassPending(DB_NAME)).as("its baseline is committed but not yet applied here").isTrue();

    assertThatNoException().isThrownBy(() -> sm.applyBootstrapFingerprintEntry(matchingBaseline(), 7L));
    assertThat(sm.isBootstrapPassPending(DB_NAME)).isFalse();
  }

  /**
   * The conclusion of a pass that lost leadership can land after the next leader's pass announced: it must not
   * release what the newer pass holds.
   */
  @Test
  void theConclusionOfAnEarlierPassDoesNotReleaseALaterOne() {
    final ArcadeStateMachine sm = stateMachine();
    receiveAnnounce(sm, "pass-1", DB_NAME);
    receiveAnnounce(sm, "pass-2", DB_NAME);

    receiveConclude(sm, "pass-1");

    assertThat(sm.isBootstrapPassPending(DB_NAME)).isTrue();

    receiveConclude(sm, "pass-2");
    assertThat(sm.isBootstrapPassPending(DB_NAME)).isFalse();
  }

  /** A leader that dies mid-pass sends no conclusion: the hold lapses by itself instead of wedging the node. */
  @Test
  void theHoldLapsesOnItsOwnWhenNothingEverSettlesIt() {
    final ArcadeStateMachine sm = stateMachine();
    PostBootstrapStateHandler.applyPassMarker(
        new JSONObject(BootstrapElection.announcePassBody("pass-1", List.of(DB_NAME))), sm, 0L);

    assertThat(sm.isBootstrapPassPending(DB_NAME)).isFalse();
    assertThat(sm.bootstrapWindowReason()).isNull();
  }

  /** The #8045 rule: a database this node does not hold serves nothing in the cluster's stead. */
  @Test
  void aDatabaseThisNodeDoesNotHoldDoesNotHoldReadiness() {
    final ArcadeStateMachine sm = stateMachine();
    receiveAnnounce(sm, "pass-1", "not-on-this-node-8368");

    assertThat(sm.bootstrapWindowReason()).isNull();
  }

  /** A node that has already applied an application entry is past first formation: a stray announce is ignored. */
  @Test
  void aNodePastFirstFormationIgnoresTheAnnounce() throws Exception {
    final ArcadeStateMachine sm = stateMachine();
    assertThatNoException().isThrownBy(() -> sm.applyBootstrapFingerprintEntry(matchingBaseline(), 7L));
    sm.writePersistedAppliedIndex(7L, null);

    receiveAnnounce(sm, "pass-1", "other-db");

    assertThat(sm.isBootstrapPassPending("other-db")).isFalse();
  }

  /** The request path: every protocol is refused while the pass decides, and the engine's own threads are not. */
  @Test
  void aClientIsRefusedWhileThePassDecidesAndServedOnceItHas() throws Exception {
    final ArcadeStateMachine sm = stateMachine();
    final RaftHAServer raft = mock(RaftHAServer.class);
    when(raft.isLeader()).thenReturn(true);
    when(raft.getStateMachine()).thenReturn(sm);
    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.getConfiguration()).thenReturn(configuration());
    final RaftReplicatedDatabase replicated = new RaftReplicatedDatabase(server, localDb, raft);
    localDb.setAutoTransaction(true);

    receiveAnnounce(sm, "pass-1", DB_NAME);

    for (final String protocol : new String[] { "http", "bolt", "postgres", "grpc" }) {
      ProtocolContext.set(protocol);
      try {
        assertThatThrownBy(() -> replicated.countType("Seed", true))
            .as("a %s client during the pass", protocol)
            .isInstanceOf(NeedRetryException.class)
            .hasMessageContaining(DB_NAME)
            .hasMessageContaining("first-formation bootstrap");
      } finally {
        ProtocolContext.clear();
      }
    }

    assertThat(ProtocolContext.get()).isEqualTo(ProtocolContext.INTERNAL);
    assertThat(replicated.countType("Seed", true)).as("the engine is never refused").isEqualTo(1);

    assertThatNoException().isThrownBy(() -> sm.applyBootstrapFingerprintEntry(matchingBaseline(), 7L));
    ProtocolContext.set("bolt");
    try {
      assertThat(replicated.countType("Seed", true)).isEqualTo(1);
    } finally {
      ProtocolContext.clear();
    }
  }
}
