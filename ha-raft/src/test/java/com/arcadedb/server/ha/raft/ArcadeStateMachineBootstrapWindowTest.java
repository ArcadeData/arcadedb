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
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.FakeArcadeDBServer;
import com.arcadedb.server.ServerDatabase;
import com.arcadedb.utility.FileUtils;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

/**
 * Tests of the first-formation bootstrap window of the {@link ArcadeStateMachine}: when {@code bootstrapWindowReason()}
 * reports a node unfit to serve, what the status document publishes for it, and how the pass probe and the committed
 * baseline open and release it.
 * <p>
 * Same harness throughout: a real {@link LocalDatabase} and a stubbed {@link ArcadeDBServer}, zero snapshot-install retries
 * so a mismatch fails its download immediately with no leader to pull from.
 */
class ArcadeStateMachineBootstrapWindowTest {

  private static final String DB_DIR  = "./target/databases";
  private static final String DB_NAME = "test-bootstrap-window";
  private static final String DB_PATH = DB_DIR + "/" + DB_NAME;
  private static final long   HOLD_MS = 60_000L;
  private LocalDatabase localDb;
  private static final RaftPeerId LOCAL_PEER  = RaftPeerId.valueOf("local-8368");
  private FakeArcadeDBServer          passServer;
  private static final RaftPeerId REMOTE_PEER = RaftPeerId.valueOf("remote-8368");

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

  private FakeArcadeDBServer stubbedServer() {
    final FakeArcadeDBServer server = FakeArcadeDBServer.create();
    server.returns("getConfiguration", configuration());
    server.on("existsDatabase", args -> Objects.equals(args[0], DB_NAME));
    final ServerDatabase servedDb = new ServerDatabase(null, localDb);
    server.on("getDatabaseForOperator", args -> Objects.equals(args[0], DB_NAME) ? servedDb : null);
    return server;
  }

  private ArcadeStateMachine stateMachine() {
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    sm.setServer(stubbedServer());
    return sm;
  }

  /** The baseline this node's own copy produces: it matches, so the apply bootstraps locally and installs nothing. */
  private RaftLogEntryCodec.DecodedEntry matchingBaseline() throws Exception {
    // Sampled from the settled copy (issue #8177): setUp's commit may still be in flight to the disk.
    final String fingerprint = BootstrapFingerprint.computeSettled(localDb);
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

  // Regression for #8368: a follower holds its databases from the moment the pass probes it
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

  // Regression for #8368: a follower holds its databases from the moment the pass probes it
  @Test
  void aProbeThatIsNotPartOfAPassHoldsNothing() {
    final ArcadeStateMachine sm = stateMachine();
    // The presence matrix, the database reconciler, the #8360 install and the divergence re-check all send "{}".
    PostBootstrapStateHandler.applyPassMarker(new JSONObject("{}"), sm, HOLD_MS);
    PostBootstrapStateHandler.applyPassMarker(null, sm, HOLD_MS);

    assertThat(sm.isBootstrapPassPending(DB_NAME)).isFalse();
    assertThat(sm.bootstrapWindowReason()).isNull();
  }

  // Regression for #8368: a follower holds its databases from the moment the pass probes it
  @Test
  void applyingTheBaselineReleasesTheHold() throws Exception {
    final ArcadeStateMachine sm = stateMachine();
    receiveAnnounce(sm, "pass-1", DB_NAME);

    assertThatNoException().isThrownBy(() -> sm.applyBootstrapFingerprintEntry(matchingBaseline(), 7L));

    assertThat(sm.isBootstrapPassPending(DB_NAME)).as("the baseline decided: the copy on disk is the cluster's").isFalse();
    assertThat(sm.bootstrapWindowReason()).isNull();
  }

  // Regression for #8368: a follower holds its databases from the moment the pass probes it
  @Test
  void anAnnounceThatArrivesAfterTheBaselineWasAppliedHoldsNothing() throws Exception {
    final ArcadeStateMachine sm = stateMachine();
    assertThatNoException().isThrownBy(() -> sm.applyBootstrapFingerprintEntry(matchingBaseline(), 7L));

    receiveAnnounce(sm, "pass-1", DB_NAME);

    assertThat(sm.isBootstrapPassPending(DB_NAME)).isFalse();
    assertThat(sm.bootstrapWindowReason()).isNull();
  }

  // Regression for #8368: a follower holds its databases from the moment the pass probes it
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

  // Regression for #8368: a follower holds its databases from the moment the pass probes it
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

  // Regression for #8368: a follower holds its databases from the moment the pass probes it
  /** A leader that dies mid-pass sends no conclusion: the hold lapses by itself instead of wedging the node. */
  @Test
  void theHoldLapsesOnItsOwnWhenNothingEverSettlesIt() {
    final ArcadeStateMachine sm = stateMachine();
    PostBootstrapStateHandler.applyPassMarker(
        new JSONObject(BootstrapElection.announcePassBody("pass-1", List.of(DB_NAME))), sm, 0L);

    assertThat(sm.isBootstrapPassPending(DB_NAME)).isFalse();
    assertThat(sm.bootstrapWindowReason()).isNull();
  }

  // Regression for #8368: a follower holds its databases from the moment the pass probes it
  /** The #8045 rule: a database this node does not hold serves nothing in the cluster's stead. */
  @Test
  void aDatabaseThisNodeDoesNotHoldDoesNotHoldReadiness() {
    final ArcadeStateMachine sm = stateMachine();
    receiveAnnounce(sm, "pass-1", "not-on-this-node-8368");

    assertThat(sm.bootstrapWindowReason()).isNull();
  }

  // Regression for #8368: a follower holds its databases from the moment the pass probes it
  /** A node that has already applied an application entry is past first formation: a stray announce is ignored. */
  @Test
  void aNodePastFirstFormationIgnoresTheAnnounce() throws Exception {
    final ArcadeStateMachine sm = stateMachine();
    assertThatNoException().isThrownBy(() -> sm.applyBootstrapFingerprintEntry(matchingBaseline(), 7L));
    sm.writePersistedAppliedIndex(7L, null);

    receiveAnnounce(sm, "pass-1", "other-db");

    assertThat(sm.isBootstrapPassPending("other-db")).isFalse();
  }

  // Regression for #8368: a follower holds its databases from the moment the pass probes it
  /** The request path: every protocol is refused while the pass decides, and the engine's own threads are not. */
  @Test
  void aClientIsRefusedWhileThePassDecidesAndServedOnceItHas() throws Exception {
    final ArcadeStateMachine sm = stateMachine();
    final FakeRaftHAServer raft = FakeRaftHAServer.detached();
    raft.leader(true);
    raft.stateMachine(sm);
    final FakeArcadeDBServer server = FakeArcadeDBServer.create();
    server.returns("getConfiguration", configuration());
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

  // Regression for #8368: a follower holds its databases from the moment the pass probes it
  /**
   * A leader whose pass elects a remote source is about to become a follower of a pass that may reject its copy: it
   * holds its own databases from that decision on, and keeps holding them once leadership has moved - the elected
   * source's own pass announces and concludes for it.
   */
  @Test
  void aLeaderThatTransfersHoldsItsOwnCopyThroughTheTransfer() {
    final ArcadeStateMachine sm = stateMachine();
    final FakeRaftHAServer ha = leaderOfAPassThatElects(sm);
    final BootstrapElection election = electionWhereTheRemotePeerIsFresher(ha);

    final boolean[] heldAtTheTransfer = new boolean[1];
    ha.on("transferLeadership", args -> {
      // The targeted form (peer, timeout) is the one the election asks for; the others answer as unanswered
      if (args[0] instanceof String)
        heldAtTheTransfer[0] = sm.isBootstrapPassPending(DB_NAME);
      return false;
    });

    assertThat(election.runIfEligible()).isEqualTo(BootstrapElection.Outcome.TRANSFERRED);
    assertThat(heldAtTheTransfer[0]).as("held from the decision, before leadership moves").isTrue();
    assertThat(sm.isBootstrapPassPending(DB_NAME)).as("not concluded by the pass that handed over").isTrue();
  }

  // Regression for #8368: a follower holds its databases from the moment the pass probes it
  /**
   * The transfer the self-hold anticipated never happens: the pass fails, and its conclusion releases the hold, so a
   * failed transfer never keeps the leader out of the Service while it waits for a retry on the next term.
   */
  @Test
  void aFailedTransferReleasesTheLeadersOwnHold() {
    final ArcadeStateMachine sm = stateMachine();
    final FakeRaftHAServer ha = leaderOfAPassThatElects(sm);
    final BootstrapElection election = electionWhereTheRemotePeerIsFresher(ha);
    ha.fails("transferLeadership", new IllegalStateException("transfer timed out"));

    assertThat(election.runIfEligible()).isEqualTo(BootstrapElection.Outcome.FAILED);
    assertThat(sm.isBootstrapPassPending(DB_NAME)).isFalse();
    assertThat(sm.bootstrapWindowReason()).isNull();
  }

  // Regression for #8368: a follower holds its databases from the moment the pass probes it
  /**
   * Issue #8409: the leader running the pass holds its own copy from the start of the collection, before it knows
   * whether its copy is the baseline, on readiness and on the request path alike.
   */
  @Test
  void aLeaderHoldsItsOwnCopyWhileItCollects() {
    final ArcadeStateMachine sm = stateMachine();
    final FakeRaftHAServer ha = leaderOfAPassThatElects(sm);
    final BootstrapElection election = electionWhereTheRemotePeerIsFresher(ha);
    final boolean[] heldWhileCollecting = new boolean[1];
    final String[] reasonWhileCollecting = new String[1];
    doAnswer(invocation -> {
      heldWhileCollecting[0] = sm.isBootstrapPassPending(DB_NAME);
      reasonWhileCollecting[0] = sm.bootstrapWindowReason();
      return CompletableFuture.completedFuture(BootstrapElection.ProbeOutcome.ok(
          Map.of(DB_NAME, new BootstrapElection.PeerState(REMOTE_PEER, DB_NAME, "f".repeat(64), Long.MAX_VALUE / 2))));
    }).when(election).queryPeer(eq(REMOTE_PEER), anyString(), any(), anyLong(), any(), anyString());

    assertThat(election.runIfEligible()).isEqualTo(BootstrapElection.Outcome.TRANSFERRED);
    assertThat(heldWhileCollecting[0]).as("the defect: the leader served its own copy while it collected").isTrue();
    assertThat(reasonWhileCollecting[0]).contains("deciding which copy of 1 database(s)");
  }

  // Regression for #8368: a follower holds its databases from the moment the pass probes it
  /**
   * Issue #8409, the common case: the leader IS the source. Its hold outlives the collection only for the databases
   * it committed a baseline for, until that baseline is applied here - the same rule its followers follow.
   */
  @Test
  void aLeaderThatIsTheSourceIsReleasedByItsOwnBaseline() throws Exception {
    final ArcadeStateMachine sm = stateMachine();
    final FakeRaftHAServer ha = leaderOfAPassThatElects(sm);
    ha.transactionBroker(new FakeRaftTransactionBroker());
    final BootstrapElection election = spy(new BootstrapElection(ha, passServer));
    election.probeRetryBackoffMs = 0L;
    final boolean[] heldWhileCollecting = new boolean[1];
    doAnswer(invocation -> {
      heldWhileCollecting[0] = sm.isBootstrapPassPending(DB_NAME);
      // The remote copy is older than this node's: this node is the source.
      return CompletableFuture.completedFuture(BootstrapElection.ProbeOutcome.ok(
          Map.of(DB_NAME, new BootstrapElection.PeerState(REMOTE_PEER, DB_NAME, "f".repeat(64), -1L))));
    }).when(election).queryPeer(eq(REMOTE_PEER), anyString(), any(), anyLong(), any(), anyString());

    assertThat(election.runIfEligible()).isEqualTo(BootstrapElection.Outcome.COMMITTED);
    assertThat(heldWhileCollecting[0]).isTrue();
    assertThat(sm.isBootstrapPassPending(DB_NAME)).as("its baseline is committed but not yet applied here").isTrue();

    assertThatNoException().isThrownBy(() -> sm.applyBootstrapFingerprintEntry(matchingBaseline(), 7L));
    assertThat(sm.isBootstrapPassPending(DB_NAME)).isFalse();
    assertThat(sm.bootstrapWindowReason()).isNull();
  }

  // Regression for #8368: a follower holds its databases from the moment the pass probes it
  /**
   * Review of PR #8477: the leader's own hold has no deadline while its pass runs - the pass itself concludes it - so a
   * collection slower than twice {@code arcadedb.ha.bootstrapTimeoutMs} (here 1 s, a 2 s deadline) cannot let it lapse
   * before a source is elected. The probe answers after 2.5 s.
   */
  @Test
  @Tag("slow")
  void theLeadersHoldCannotLapseWhileItsPassIsStillCollecting() {
    final ArcadeStateMachine sm = stateMachine();
    final FakeRaftHAServer ha = leaderOfAPassThatElects(sm);
    final BootstrapElection election = electionWhereTheRemotePeerIsFresher(ha);
    final boolean[] heldAfterTheOldDeadline = new boolean[1];
    doAnswer(invocation -> {
      Thread.sleep(2_500L);
      heldAfterTheOldDeadline[0] = sm.isBootstrapPassPending(DB_NAME);
      return CompletableFuture.completedFuture(BootstrapElection.ProbeOutcome.ok(
          Map.of(DB_NAME, new BootstrapElection.PeerState(REMOTE_PEER, DB_NAME, "f".repeat(64), Long.MAX_VALUE / 2))));
    }).when(election).queryPeer(eq(REMOTE_PEER), anyString(), any(), anyLong(), any(), anyString());

    election.runIfEligible();
    assertThat(heldAfterTheOldDeadline[0]).isTrue();
  }

  // Regression for #8368: a follower holds its databases from the moment the pass probes it
  /**
   * Review of PR #8477: the leader's self-hold has no deadline at all, not even the one-hour ceiling a follower's
   * announce is capped at, and its pass still settles it.
   */
  @Test
  void theLeadersSelfHoldHasNoDeadlineAndItsConclusionReleasesIt() {
    final ArcadeStateMachine sm = stateMachine();
    sm.holdOwnBootstrapPass("pass-1", List.of(DB_NAME));
    receiveAnnounce(sm, "pass-other", "other-db");
    assertThat(sm.isBootstrapPassPending(DB_NAME)).isTrue();

    // A bounded announce from the same pass (the transfer) replaces it with a deadline again: zero lapses at once.
    PostBootstrapStateHandler.applyPassMarker(
        new JSONObject(BootstrapElection.announcePassBody("pass-1", List.of(DB_NAME))), sm, 0L);
    assertThat(sm.isBootstrapPassPending(DB_NAME)).isFalse();

    sm.holdOwnBootstrapPass("pass-2", List.of(DB_NAME));
    sm.concludeBootstrapPass(null, List.of());
    assertThat(sm.isBootstrapPassPending(DB_NAME)).isFalse();
  }

  // Regression for #8368: a follower holds its databases from the moment the pass probes it
  /**
   * Review of PR #8477: a database the pass holds but the collection did not report (dropped locally mid-pass) is
   * bounded at the transfer like the others, rather than keeping the leader's unbounded self-hold for good.
   */
  @Test
  void aTransferBoundsEveryDatabaseThePassHolds() {
    final ArcadeStateMachine sm = spy(stateMachine());
    final FakeRaftHAServer ha = leaderOfAPassThatElects(sm);
    // Listed when the pass starts, gone by the time the local states are computed: no peer reports it.
    passServer.returns("getDatabaseNames", Set.of(DB_NAME, "gone-8477"));
    final BootstrapElection election = electionWhereTheRemotePeerIsFresher(ha);
    final Collection<String>[] boundedAtTransfer = new Collection[1];
    doAnswer(invocation -> {
      boundedAtTransfer[0] = invocation.getArgument(1);
      return invocation.callRealMethod();
    }).when(sm).announceBootstrapPass(anyString(), any(), anyLong());

    assertThat(election.runIfEligible()).isEqualTo(BootstrapElection.Outcome.TRANSFERRED);
    assertThat(boundedAtTransfer[0]).as("the pass's held databases, not only the reported states")
        .contains(DB_NAME, "gone-8477");
  }

  // Regression for #8368: a follower holds its databases from the moment the pass probes it
  /** Issue #8409: a pass that fails while collecting releases the hold it took at its start. */
  @Test
  void aPassThatFailsWhileCollectingReleasesTheLeadersHold() {
    final ArcadeStateMachine sm = stateMachine();
    final FakeRaftHAServer ha = leaderOfAPassThatElects(sm);
    final BootstrapElection election = spy(new BootstrapElection(ha, passServer));
    election.probeRetryBackoffMs = 0L;
    final boolean[] heldWhileCollecting = new boolean[1];
    doAnswer(invocation -> {
      heldWhileCollecting[0] = sm.isBootstrapPassPending(DB_NAME);
      throw new IllegalStateException("simulated collection failure");
    }).when(election).queryPeer(eq(REMOTE_PEER), anyString(), any(), anyLong(), any(), anyString());

    assertThat(election.runIfEligible()).isEqualTo(BootstrapElection.Outcome.FAILED);
    assertThat(heldWhileCollecting[0]).isTrue();
    assertThat(sm.isBootstrapPassPending(DB_NAME)).isFalse();
    assertThat(sm.bootstrapWindowReason()).isNull();
  }

  /** A first-formation leader over {@code sm}, with one remote peer whose HTTP port refuses at once. */
  private FakeRaftHAServer leaderOfAPassThatElects(final ArcadeStateMachine sm) {
    final ContextConfiguration config = configuration();
    config.setValue(GlobalConfiguration.HA_BOOTSTRAP_FROM_LOCAL_DATABASE, true);
    config.setValue(GlobalConfiguration.HA_BOOTSTRAP_TIMEOUT_MS, 1_000L);
    final FakeArcadeDBServer server = stubbedServer();
    server.returns("getConfiguration", config);
    server.databaseNames(DB_NAME);
    sm.setServer(server);
    passServer = server;

    final FakeRaftHAServer ha = FakeRaftHAServer.detached();
    ha.leader(true);
    ha.commitIndex(0L);
    ha.stateMachine(sm);
    ha.localPeerId(LOCAL_PEER);
    ha.returns("getLivePeers", List.of(RaftPeer.newBuilder().setId(LOCAL_PEER).build(),
        RaftPeer.newBuilder().setId(REMOTE_PEER).build()));
    // Port 1 refuses at once: the conclusion the failed pass sends it is best effort and must not delay the test.
    ha.httpAddresses(Map.of(REMOTE_PEER, "localhost:1"));
    // The elected source has answered this leader recently, which the transfer to it requires (issue #8714).
    ha.returns("followerContactPeers", Set.of(REMOTE_PEER.toString()));
    // The election dials a declared endpoint only when it identifies the peer alone (issue #8033).
    ha.peerHttpAddress(REMOTE_PEER, "localhost:1");
    return ha;
  }

  /** The remote peer answers the probe with a fresher copy, so the pass elects it and transfers. */
  private BootstrapElection electionWhereTheRemotePeerIsFresher(final RaftHAServer ha) {
    final BootstrapElection election = spy(new BootstrapElection(ha, passServer));
    election.probeRetryBackoffMs = 0L;
    doReturn(CompletableFuture.completedFuture(BootstrapElection.ProbeOutcome.ok(
        Map.of(DB_NAME, new BootstrapElection.PeerState(REMOTE_PEER, DB_NAME, "f".repeat(64), Long.MAX_VALUE / 2)))))
        .when(election).queryPeer(eq(REMOTE_PEER), anyString(), any(), anyLong(), any(), anyString());
    return election;
  }

  private FakeArcadeDBServer stubbedServer(final ContextConfiguration config) {
    final FakeArcadeDBServer server = FakeArcadeDBServer.create();
    server.returns("getConfiguration", config);
    server.on("existsDatabase", args -> Objects.equals(args[0], DB_NAME));
    // The server arg is null: the paths reached here dereference the wrapped localDb, not the server.
    final ServerDatabase servedDb = new ServerDatabase(null, localDb);
    server.on("getDatabaseForOperator", args -> Objects.equals(args[0], DB_NAME) ? servedDb : null);
    return server;
  }

  /** The baseline of a copy this node does not have: never matches, so the mismatch arm reinstalls. */
  private static RaftLogEntryCodec.DecodedEntry baselineOfAnotherCopy(final long lastTxId) {
    final ByteString encoded = RaftLogEntryCodec.encodeBootstrapFingerprintEntry(DB_NAME, "0".repeat(64), lastTxId);
    return RaftLogEntryCodec.decode(encoded);
  }

  // Regression for #7519: the peer half of the bootstrap window takes the node out of the Service
  /**
   * Regression tests for issue #7519: the peer half of the first-formation bootstrap window, which nothing gated a
   * production client out of.
   * <p>
   * A peer whose copy does not match the committed baseline replaces its whole database directory from the
   * leader's snapshot. {@code SnapshotInstaller.install} downloads BEFORE it touches the live files - deliberately,
   * so a failed download costs no availability - so for the length of that download the local copy is open and
   * serving on every protocol, and what it serves is the copy the cluster's committed baseline has just decided
   * against. The node-wide {@code snapshotInstallInProgress} 503 covers only the file swap at the end, and only
   * HTTP.
   * <p>
   * {@link ArcadeStateMachine#bootstrapWindowReason()} is what the readiness probe reads to take the node out of
   * the Service for the duration, which gates every protocol at once. These tests drive the real apply path, not
   * the accessor: the entry points that must set it are the ones {@code applyBootstrapFingerprintEntry} reaches.
   * <p>
   * Same harness as {@code ArcadeStateMachineBootstrapMismatchTest}: a real {@link LocalDatabase} and a stubbed
   * {@link ArcadeDBServer}, zero snapshot-install retries so the download fails immediately with no leader to pull
   * from.
   * <p>
   * The window itself. The install is observed from inside - {@code SnapshotInstaller.install} asks the server
   * for its backup coordinator as its first act, which is strictly after the mark is taken and strictly before
   * anything is downloaded - and at that moment the node must already be reporting itself unfit to serve.
   * <p>
   * This is the assertion the issue asks for: the local copy is open and serving right there, and before this
   * change nothing on any request path said so.
   */
  @Test
  void theNodeLeavesTheServiceForTheWholeBootstrapReinstallNotJustTheFileSwap() {
    final ContextConfiguration config = configuration();
    final FakeArcadeDBServer server = stubbedServer(config);
    final ArcadeStateMachine sm = new ArcadeStateMachine();

    final AtomicReference<String> reasonInsideTheInstall = new AtomicReference<>();
    final AtomicReference<List<String>> namesInsideTheInstall = new AtomicReference<>();
    server.on("getBackupCoordinator", args -> {
      reasonInsideTheInstall.set(sm.bootstrapWindowReason());
      namesInsideTheInstall.set(sm.getBootstrapInstallsInFlight());
      return null; // no coordinator: install() documents null as "no slot to take, and none to release"
    });

    sm.setServer(server);

    // lastTxId above the local one so the "local is fresher" refusal does not fire: only the fingerprint
    // mismatch matters here, which is the arm that reinstalls.
    assertThatNoException().isThrownBy(
        () -> sm.applyBootstrapFingerprintEntry(baselineOfAnotherCopy(Long.MAX_VALUE), 7L));

    assertThat(namesInsideTheInstall.get())
        .as("the database being replaced is registered as in flight before a single byte is downloaded")
        .containsExactly(DB_NAME);
    assertThat(reasonInsideTheInstall.get())
        .as("the readiness probe reports the node unfit to serve for the whole install, not just the swap")
        .isNotNull()
        .contains("replacing 1 database(s) on this node");
    assertThat(reasonInsideTheInstall.get())
        .as("GET /api/v1/ready is unauthenticated, so the body counts the databases and never names them")
        .doesNotContain(DB_NAME);
  }

  // Regression for #7519: the peer half of the bootstrap window takes the node out of the Service
  /**
   * A failed install keeps the readiness holder through the retry, and keeps it in NOTHING ELSE than the in-flight
   * depth. This is the likely path at a real first formation - the bootstrap entry is applied while leader election
   * on this peer may still be settling, so the very first download often has no leader to pull from.
   * <p>
   * The half that is easy to get wrong is the second one. An earlier revision of this fix held readiness across
   * the retry by setting the durable {@code bootstrapUnreconciled} mark, which is the issue #6124 set: it means
   * "this node kept a copy FRESHER than the baseline and an operator must choose a side", and {@code
   * ClusterAlerts} publishes it as a CRITICAL telling them to stop the cluster and copy a database directory to
   * every peer. Raising that on an ordinary formation is a false alarm with a drastic remedy, so the holder is an
   * in-flight depth and the mark is left to the branch that actually means it.
   */
  @Test
  void aFailedInstallHoldsReadinessThroughTheRetryWithoutRaisingTheCriticalDivergenceAlert() throws Exception {
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    sm.setServer(stubbedServer(configuration()));

    assertThatNoException().isThrownBy(
        () -> sm.applyBootstrapFingerprintEntry(baselineOfAnotherCopy(Long.MAX_VALUE), 7L));

    assertThat(sm.getBootstrapUnreconciledDatabases())
        .as("an ordinary first-download failure is not the #6124 'kept a fresher copy' condition and must not "
            + "raise its CRITICAL alert")
        .isEmpty();

    // The retry runs on the lifecycleExecutor: let it finish. Before issue #8367 it released the holder whatever it
    // decided to do, so a retry that failed as well put the node back in the Service serving the rejected copy. The
    // holder is now released where the copy is actually replaced - ArcadeStateMachinePendingBootstrapReplacementTest drives
    // those paths - and nothing in this unit test ever replaces it.
    sm.awaitLifecycleTasksForTesting(30_000);
    assertThat(sm.getBootstrapInstallsInFlight())
        .as("a retry that could not replace the copy keeps the node out of the Service")
        .containsExactly(DB_NAME);
    assertThat(sm.getPendingBootstrapReplacements()).containsExactly(DB_NAME);
    assertThat(sm.getBootstrapUnreconciledDatabases()).as("and still raises no CRITICAL").isEmpty();
  }

  // Regression for #7519: the peer half of the bootstrap window takes the node out of the Service
  /**
   * Both conditions at once - an install replacing one database while another sits unreconciled - are reported
   * together. An operator who read only the install would go on believing the node comes back by itself once the
   * install finishes, which is the one case where it does not. Observed from inside the install, the only moment
   * at which both are true.
   */
  @Test
  void bothHalvesOfTheWindowAreReportedWhenBothHold() {
    final FakeArcadeDBServer server = stubbedServer(configuration());
    final ArcadeStateMachine sm = new ArcadeStateMachine();

    final AtomicReference<String> reasonInsideTheInstall = new AtomicReference<>();
    server.on("getBackupCoordinator", args -> {
      reasonInsideTheInstall.set(sm.bootstrapWindowReason());
      return null;
    });

    sm.setServer(server);
    // A second database left unreconciled by an earlier pass, which no install in this test will clear. It has to
    // be a copy this node HOLDS: a marked database that is not here is the #7298 missing half, which does not hold
    // readiness at all (issue #8045).
    server.on("existsDatabase", args -> Objects.equals(args[0], DB_NAME) || Objects.equals(args[0], "another-database"));
    sm.markBootstrapUnreconciled("another-database");

    assertThatNoException().isThrownBy(
        () -> sm.applyBootstrapFingerprintEntry(baselineOfAnotherCopy(Long.MAX_VALUE), 7L));

    final String reason = reasonInsideTheInstall.get();
    assertThat(reason).as("the probe was taken while the install was in flight").isNotNull();
    assertThat(reason).contains("is replacing 1 database(s) on this node");
    assertThat(reason).contains("1 database(s) on this node hold a copy");
    assertThat(reason).as("the route that names them is said once, not once per condition")
        .endsWith(" GET /api/v1/cluster names them.");
    assertThat(reason).doesNotContain("another-database");
  }

  // Regression for #7519: the peer half of the bootstrap window takes the node out of the Service
  /**
   * The state the window leaves behind (issue #6124). The peer whose copy was FRESHER than the chosen baseline
   * keeps it rather than lose data, and from then on its file ids are assigned by a history no other peer shares.
   * It is durable and it is not transient, and until it is reconciled every read it serves for that database is
   * data the cluster never adopted - so it stays out of the Service, and the reason names the way back.
   */
  @Test
  void aCopyTheBaselineDidNotAdoptKeepsTheNodeOutOfTheServiceUntilItIsResynced() {
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    sm.setServer(stubbedServer(configuration()));

    assertThat(localDb.getLastTransactionId()).isGreaterThan(0L);
    // Baseline below the local transaction id: the "local is fresher, refuse to overwrite" arm.
    assertThatNoException().isThrownBy(() -> sm.applyBootstrapFingerprintEntry(baselineOfAnotherCopy(0L), 7L));

    assertThat(sm.getBootstrapUnreconciledDatabases()).containsExactly(DB_NAME);
    assertThat(sm.getBootstrapInstallsInFlight()).as("nothing was installed on this arm").isEmpty();

    final String reason = sm.bootstrapWindowReason();
    assertThat(reason).isNotNull();
    assertThat(reason).contains("1 database(s) on this node hold a copy");
    assertThat(reason).as("the reason has to name the one command that ends it").contains("/api/v1/cluster/resync/");
    assertThat(reason)
        .as("GET /api/v1/ready is unauthenticated, so the body points at the authenticated route for the names")
        .doesNotContain(DB_NAME);
  }

  // Regression for #7519: the peer half of the bootstrap window takes the node out of the Service
  /**
   * The ordinary case has to stay ordinary: a peer whose copy IS the committed baseline installs nothing, is
   * marked nothing, and reports itself ready. A gate that held a healthy first formation out of the Service
   * would be worse than the hole it closes.
   */
  @Test
  void aPeerThatMatchesTheBaselineIsNeverHeldOutOfTheService() {
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    sm.setServer(stubbedServer(configuration()));

    // Sampled from the settled copy (issue #8177): setUp's commit may still be in flight to the disk, and a sample
    // taken before the flush lands hashes different bytes than the state machine's recomputation after it.
    final ByteString encoded = RaftLogEntryCodec.encodeBootstrapFingerprintEntry(
        DB_NAME, BootstrapFingerprint.computeSettled(localDb), localDb.getLastTransactionId());

    assertThatNoException().isThrownBy(() -> sm.applyBootstrapFingerprintEntry(RaftLogEntryCodec.decode(encoded), 7L));

    assertThat(sm.getBootstrapInstallsInFlight()).isEmpty();
    assertThat(sm.getBootstrapUnreconciledDatabases()).isEmpty();
    assertThat(sm.bootstrapWindowReason())
        .as("a matching peer is serving the cluster's own copy and belongs in the Service")
        .isNull();
  }

  // Regression for #7519: the peer half of the bootstrap window takes the node out of the Service
  /**
   * Two overlapping installs of the same database must not have the first one to finish declare the node ready
   * while the second is still moving files. That is why the registration is a depth per database rather than a
   * set - the same shape {@code SnapshotInstaller.INSTALLS_IN_FLIGHT} settled on for its own overlap guard, and
   * for the same reason: with a set the failure is silent, with no log line and nothing to assert on.
   * <p>
   * Driven through the real apply path twice, reentrantly: the inner apply runs from inside the outer install's
   * {@code getBackupCoordinator()} call, so when the inner one returns and releases its holder, the outer install
   * is still in flight and the node must still be reporting itself unfit to serve.
   */
  @Test
  void anOverlappingInstallOfTheSameDatabaseDoesNotReleaseTheGateEarly() throws Exception {
    final FakeArcadeDBServer server = stubbedServer(configuration());
    final ArcadeStateMachine sm = new ArcadeStateMachine();

    final AtomicReference<List<String>> inFlightAfterTheInnerInstallReturned = new AtomicReference<>();
    final AtomicBoolean reentered = new AtomicBoolean();
    server.on("getBackupCoordinator", args -> {
      if (reentered.compareAndSet(false, true)) {
        // A second install of the SAME database, started and finished while this one is still in flight.
        sm.applyBootstrapFingerprintEntry(baselineOfAnotherCopy(Long.MAX_VALUE), 8L);
        inFlightAfterTheInnerInstallReturned.set(sm.getBootstrapInstallsInFlight());
      }
      return null;
    });

    sm.setServer(server);

    assertThatNoException().isThrownBy(
        () -> sm.applyBootstrapFingerprintEntry(baselineOfAnotherCopy(Long.MAX_VALUE), 7L));

    assertThat(reentered).as("the reentrant install really did run").isTrue();
    // Asserted on the in-flight registration, NOT on bootstrapWindowReason(): the failed inner install also
    // records the unreconciled mark, so the reason string is non-null either way and a test written against it
    // passes whether the depth works or not. Checked by reverting endBootstrapInstall to a plain remove, which
    // left the reason assertion green and only this one red.
    assertThat(inFlightAfterTheInnerInstallReturned.get())
        .as("the inner install releasing its holder must not deregister the outer one, which is still running")
        .containsExactly(DB_NAME);
    // After both retries have run (review of PR #7964: they are queued on the real single-threaded lifecycleExecutor,
    // so this waits for it rather than racing it). Both installs failed, and since issue #8367 a failed replacement
    // keeps ONE holder until the copy is actually replaced: the second failure of the same database must not stack a
    // second holder that no replacement would ever release.
    sm.awaitLifecycleTasksForTesting(30_000);
    assertThat(sm.getPendingBootstrapReplacements()).containsExactly(DB_NAME);
    assertThat(sm.getBootstrapInstallsInFlight()).containsExactly(DB_NAME);
    // One settle - here the DROP of the database, the cheapest path that settles one in this harness - releases it
    // entirely: with a stacked second holder the entry would outlive the database it names.
    // Absent from the registry AND from disk: since issue #8451 a deregistered name whose directory is still there is
    // a closed database the drop has to delete, which is not the already-absent path this harness relies on.
    server.returns("existsDatabase", false);
    localDb.close();
    FileUtils.deleteRecursively(new File(DB_PATH));
    sm.applyDropDatabaseEntry(RaftLogEntryCodec.decode(RaftLogEntryCodec.encodeDropDatabaseEntry(DB_NAME)), 1L);
    assertThat(sm.getPendingBootstrapReplacements()).isEmpty();
    assertThat(sm.getBootstrapInstallsInFlight())
        .as("and once the last holder is gone the entry is removed rather than left behind at zero")
        .isEmpty();
  }

  // Regression for #7519: the peer half of the bootstrap window takes the node out of the Service
  /** No bootstrap has happened at all: nothing to report, and nothing allocated on the readiness-probe path. */
  @Test
  void aNodeThatNeverBootstrappedReportsNoReason() {
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    sm.setServer(stubbedServer(configuration()));

    assertThat(sm.bootstrapWindowReason()).isNull();
    assertThat(sm.getBootstrapInstallsInFlight()).isEmpty();
  }

  // Regression for #8408: a pass still deciding on a database is published in the status document
  /**
   * Regression test for issue #8408: the third arm of {@code ArcadeStateMachine.bootstrapWindowReason()} - a first-formation
   * bootstrap pass still deciding which copy of a database the cluster keeps - was the one arm of the window that
   * {@code GET /api/v1/cluster} did not publish, so the readiness body deliberately left out its "GET /api/v1/cluster names
   * them" sentence. It is now a {@code bootstrap-deciding-databases} alert and a {@code bootstrapDeciding} member, sampled
   * from the same {@link ClusterAlerts.NodeStatus} as the installs, and the sentence is said for it too.
   */
  @Test
  void aPassDecidingOnADatabaseIsPublishedInTheStatusDocument() {
    final ArcadeDBServer server = serverWithRegisteredDatabase();
    final ArcadeStateMachine sm = stateMachine(server);
    assertThat(sm.getBootstrapPassesDeciding()).as("nothing is running yet").isEmpty();

    announcePassForTheDatabase(sm, "pass-1");

    assertThat(sm.getBootstrapPassesDeciding()).containsExactly(DB_NAME);
    assertThat(sm.bootstrapWindowReason())
        .as("the readiness body points at the document for this arm too, now that it names it")
        .contains("deciding which copy of 1 database(s)")
        .endsWith(" GET /api/v1/cluster names them.")
        .doesNotContain(DB_NAME);

    final ClusterAlerts.NodeStatus nodeStatus = ClusterAlerts.NodeStatus.of(sm);
    final JSONArray alerts = ClusterAlerts.scan(server, sm, List.of(), null, null, null, sm.getLocalResyncState(), nodeStatus,
        false);
    final JSONObject alert = alertWithId(alerts, "bootstrap-deciding-databases");
    assertThat(alert).as("before #8408 the alert list was empty while the readiness probe answered 503").isNotNull();
    assertThat(alert.getString("severity")).isEqualTo(ClusterAlerts.SEVERITY_INFO);
    assertThat(alert.getJSONObject("details").getInt("count")).isEqualTo(1);
    assertThat(alert.getJSONObject("details").getJSONArray("databases").getString(0)).isEqualTo(DB_NAME);

    final JSONObject member = GetClusterHandler.buildBootstrapDeciding(nodeStatus.bootstrapDeciding(), null);
    assertThat(member.getBoolean("inProgress")).isTrue();
    assertThat(member.getInt("count")).isEqualTo(1);
    assertThat(member.getJSONArray("databases").getString(0)).isEqualTo(DB_NAME);
    assertThat(member.keySet()).containsExactlyInAnyOrder("inProgress", "count", "databases");
  }

  // Regression for #8408: a pass still deciding on a database is published in the status document
  /**
   * Whether this node is out of the Service is a node-level fact: a caller authorized on no database still sees the alert
   * and the count, and only the name is scoped.
   */
  @Test
  void aCallerWhoMayNotSeeTheDatabaseLearnsTheCountAndNotTheName() {
    final ArcadeDBServer server = serverWithRegisteredDatabase();
    final ArcadeStateMachine sm = stateMachine(server);
    announcePassForTheDatabase(sm, "pass-1");

    final ClusterAlerts.NodeStatus nodeStatus = ClusterAlerts.NodeStatus.of(sm);
    final JSONArray alerts = ClusterAlerts.scan(server, sm, List.of(), Set.of(), null, null, sm.getLocalResyncState(),
        nodeStatus, false);
    final JSONObject alert = alertWithId(alerts, "bootstrap-deciding-databases");
    assertThat(alert).isNotNull();
    assertThat(alert.getJSONObject("details").getInt("count")).isEqualTo(1);
    assertThat(alert.getJSONObject("details").getJSONArray("databases")).isEmpty();

    final JSONObject member = GetClusterHandler.buildBootstrapDeciding(nodeStatus.bootstrapDeciding(), Set.of());
    assertThat(member.getBoolean("inProgress")).isTrue();
    assertThat(member.getInt("count")).isEqualTo(1);
    assertThat(member.getJSONArray("databases")).isEmpty();
  }

  // Regression for #8408: a pass still deciding on a database is published in the status document
  @Test
  void theDocumentIsCleanOnceThePassConcludes() {
    final ArcadeDBServer server = serverWithRegisteredDatabase();
    final ArcadeStateMachine sm = stateMachine(server);
    announcePassForTheDatabase(sm, "pass-1");
    PostBootstrapStateHandler.applyPassMarker(new JSONObject(BootstrapElection.concludePassBody("pass-1", List.of())), sm,
        HOLD_MS);

    final ClusterAlerts.NodeStatus nodeStatus = ClusterAlerts.NodeStatus.of(sm);
    assertThat(nodeStatus.bootstrapDeciding()).isEmpty();
    assertThat(alertWithId(ClusterAlerts.scan(server, sm, List.of(), null, null, null, sm.getLocalResyncState(), nodeStatus,
        false), "bootstrap-deciding-databases")).isNull();
    assertThat(GetClusterHandler.buildBootstrapDeciding(nodeStatus.bootstrapDeciding(), null).getBoolean("inProgress"))
        .isFalse();
    assertThat(sm.bootstrapWindowReason()).isNull();
  }

  // ------------------------------------------------------------------------------------------------------------

  private ArcadeDBServer serverWithRegisteredDatabase() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, DB_DIR);
    config.setValue(GlobalConfiguration.HA_SNAPSHOT_INSTALL_RETRIES, 0);
    config.setValue(GlobalConfiguration.HA_SNAPSHOT_INSTALL_RETRY_BASE_MS, 0L);
    config.setValue(GlobalConfiguration.NETWORK_USE_SSL, false);
    final FakeArcadeDBServer server = FakeArcadeDBServer.create((String) null, config);
    server.databaseNames(DB_NAME);
    server.database(DB_NAME, new ServerDatabase(null, localDb));
    // The single-bucket check walks the registry; nothing in it is what this test is about.
    return server;
  }

  private static ArcadeStateMachine stateMachine(final ArcadeDBServer server) {
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    sm.setServer(server);
    return sm;
  }

  private static void announcePassForTheDatabase(final ArcadeStateMachine sm, final String passId) {
    PostBootstrapStateHandler.applyPassMarker(new JSONObject(BootstrapElection.announcePassBody(passId, List.of(DB_NAME))), sm,
        HOLD_MS);
  }

  private static JSONObject alertWithId(final JSONArray alerts, final String id) {
    for (int i = 0; i < alerts.length(); i++) {
      final JSONObject alert = alerts.getJSONObject(i);
      if (id.equals(alert.getString("id", null)))
        return alert;
    }
    return null;
  }
}
