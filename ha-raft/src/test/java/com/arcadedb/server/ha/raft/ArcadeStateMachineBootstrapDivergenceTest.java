/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
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
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ha.raft.ArcadeStateMachine.BootstrapBaseline;
import com.arcadedb.utility.FileUtils;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issue #6124.
 * <p>
 * When a peer's local copy of a database is fresher than the cluster's committed bootstrap baseline,
 * {@link ArcadeStateMachine#applyBootstrapFingerprintEntry} refuses to overwrite it. That refusal is
 * correct, but it used to leave nothing behind: the node's file-id space stayed assigned by an
 * independent history, no code path ever looked at it again, and the only trace was a single SEVERE
 * line at bootstrap. These tests pin the durable mark the refusal now records, the periodic
 * verification that clears it once the copies converge, and the paths that clear it because this
 * node's copy was actually replaced by the leader's.
 * <p>
 * Driven against a real (unstarted) {@link ArcadeDBServer} and a real {@link LocalDatabase}, no
 * mocking framework - the same harness as {@code ArcadeStateMachineBootstrapBaselinePersistenceTest}.
 */
class ArcadeStateMachineBootstrapDivergenceTest {

  private static final String DB_A     = "db-a";
  private static final String DB_OTHER = "db-other";

  @TempDir
  private Path           serverDir;
  private ArcadeDBServer server;
  private LocalDatabase  localDbA;
  private String         dbAPath;

  @BeforeEach
  void setUp() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, serverDir.toString());
    server = new ArcadeDBServer(config);

    dbAPath = serverDir.resolve(DB_A).toString();
    localDbA = (LocalDatabase) new DatabaseFactory(dbAPath).create();
    server.registerDatabase(DB_A, localDbA);
  }

  @AfterEach
  void tearDown() {
    if (localDbA != null && localDbA.isOpen())
      localDbA.close();
    if (dbAPath != null)
      FileUtils.deleteRecursively(new File(dbAPath));
  }

  private ArcadeStateMachine newStateMachine() {
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    sm.setServer(server);
    return sm;
  }

  private ByteString encodeBaseline(final String dbName, final String fingerprint, final long lastTxId) {
    return RaftLogEntryCodec.encodeBootstrapFingerprintEntry(dbName, fingerprint, lastTxId);
  }

  /** Applies a baseline strictly older than the local copy, which is the "refuse to overwrite" branch. */
  private ArcadeStateMachine applyStaleBaseline(final ArcadeStateMachine sm) {
    final long localLastTxId = localDbA.getLastTransactionId();
    sm.applyBootstrapFingerprintEntry(
        RaftLogEntryCodec.decode(encodeBaseline(DB_A, "f".repeat(64), localLastTxId - 1)), 50L);
    return sm;
  }

  private String localFingerprint() {
    return BootstrapFingerprint.compute(new File(dbAPath));
  }

  /**
   * The refusal is recorded, not merely logged: the database is reported as diverged from the cluster
   * so the condition can be surfaced and re-checked instead of being forgotten the moment the log line
   * scrolls away.
   */
  @Test
  void refusingToOverwriteALocallyFresherCopyRecordsTheDivergence() {
    final ArcadeStateMachine sm = applyStaleBaseline(newStateMachine());

    assertThat(sm.getBootstrapUnreconciledDatabases())
        .as("the database kept by the overwrite guard is reported as diverged")
        .containsExactly(DB_A);
  }

  /**
   * A local copy that matches the committed baseline is NOT diverged: the mark must be specific to the
   * refusal branch, or every healthy node would raise the alert.
   */
  @Test
  void aLocalCopyThatMatchesTheBaselineIsNotRecordedAsDiverged() {
    final ArcadeStateMachine sm = newStateMachine();
    sm.applyBootstrapFingerprintEntry(
        RaftLogEntryCodec.decode(encodeBaseline(DB_A, localFingerprint(), localDbA.getLastTransactionId())), 50L);

    assertThat(sm.getBootstrapUnreconciledDatabases()).isEmpty();
  }

  /**
   * The mark survives a restart. This is the whole reason it is persisted rather than kept in memory:
   * the per-database replay-skip stops {@code applyBootstrapFingerprintEntry} from re-evaluating the
   * refusal after a restart, so an in-memory mark would silently disappear on the first bounce and the
   * node would go back to being diverged with nobody tracking it.
   */
  @Test
  void theDivergenceMarkSurvivesARestart() {
    applyStaleBaseline(newStateMachine());

    final ArcadeStateMachine reopened = newStateMachine();
    assertThat(reopened.getBootstrapUnreconciledDatabases())
        .as("a fresh state machine recovers the mark from disk")
        .containsExactly(DB_A);
  }

  /** The persisted shape is pinned independently of the reader: an extra flag on the existing entry. */
  @Test
  void theDivergenceMarkIsPersistedAsAFlagOnTheBaselineEntry() throws Exception {
    applyStaleBaseline(newStateMachine());

    final JSONObject json = new JSONObject(
        Files.readString(serverDir.resolve(".raft").resolve("bootstrap-baselines")).trim());
    assertThat(json.getJSONObject(DB_A).getBoolean("unreconciled", false)).isTrue();
  }

  /**
   * A healthy database's persisted entry keeps exactly the shape it had before this change - the flag is
   * written only when set, so an older build reading the file sees nothing new.
   */
  @Test
  void aReconciledDatabaseCarriesNoFlagInThePersistedFile() throws Exception {
    final ArcadeStateMachine sm = newStateMachine();
    sm.applyBootstrapFingerprintEntry(
        RaftLogEntryCodec.decode(encodeBaseline(DB_A, localFingerprint(), localDbA.getLastTransactionId())), 50L);

    final JSONObject json = new JSONObject(
        Files.readString(serverDir.resolve(".raft").resolve("bootstrap-baselines")).trim());
    assertThat(json.getJSONObject(DB_A).has("unreconciled")).isFalse();
  }

  /**
   * The verification's confirming outcome: once the leader reports the same fingerprint, the two copies
   * are byte-identical over the persisted content and the divergence is over. The mark is dropped, and
   * durably - a restart must not resurrect an alert that has been answered.
   */
  @Test
  void aLeaderFingerprintThatMatchesClearsTheDivergence() {
    final ArcadeStateMachine sm = applyStaleBaseline(newStateMachine());

    sm.reconcileBootstrapDivergence(Map.of(DB_A, new BootstrapBaseline(localFingerprint(), 99L)));

    assertThat(sm.getBootstrapUnreconciledDatabases()).isEmpty();
    assertThat(newStateMachine().getBootstrapUnreconciledDatabases())
        .as("clearing the mark is durable")
        .isEmpty();
  }

  /**
   * The verification's escalating outcome: a leader whose copy still differs leaves the mark raised, so
   * the alert keeps being reported until an operator decides which copy the cluster keeps.
   */
  @Test
  void aLeaderFingerprintThatDiffersKeepsTheDivergenceRaised() {
    final ArcadeStateMachine sm = applyStaleBaseline(newStateMachine());

    sm.reconcileBootstrapDivergence(Map.of(DB_A, new BootstrapBaseline("a".repeat(64), 12345L)));

    assertThat(sm.getBootstrapUnreconciledDatabases()).containsExactly(DB_A);
  }

  /**
   * A leader that reports no state for the database proves nothing: absence is not convergence, so the
   * mark stays. Without this the first probe answered by a leader that had not opened the database yet
   * would silently retire the alert.
   */
  @Test
  void aLeaderThatDoesNotReportTheDatabaseKeepsTheDivergenceRaised() {
    final ArcadeStateMachine sm = applyStaleBaseline(newStateMachine());

    sm.reconcileBootstrapDivergence(Map.of(DB_OTHER, new BootstrapBaseline("b".repeat(64), 1L)));

    assertThat(sm.getBootstrapUnreconciledDatabases()).containsExactly(DB_A);
  }

  /** Replacing this node's copy with the leader's ends the divergence, durably. */
  @Test
  void resyncingTheDatabaseFromTheLeaderClearsTheDivergence() {
    final ArcadeStateMachine sm = applyStaleBaseline(newStateMachine());

    sm.clearBootstrapUnreconciled(DB_A);

    assertThat(sm.getBootstrapUnreconciledDatabases()).isEmpty();
    assertThat(newStateMachine().getBootstrapUnreconciledDatabases()).isEmpty();
  }

  /** A full resync reinstalls every present database, so it ends every divergence at once. */
  @Test
  void aFullResyncClearsEveryDivergence() {
    final ArcadeStateMachine sm = applyStaleBaseline(newStateMachine());

    sm.clearAllBootstrapUnreconciled();

    assertThat(sm.getBootstrapUnreconciledDatabases()).isEmpty();
    assertThat(newStateMachine().getBootstrapUnreconciledDatabases()).isEmpty();
  }

  /**
   * Dropping the database takes its mark with it: the alert must not outlive the database it names, and
   * the flag lives in the same persisted entry the drop evicts.
   */
  @Test
  void droppingTheDatabaseClearsItsDivergence() {
    final ArcadeStateMachine sm = applyStaleBaseline(newStateMachine());
    assertThat(sm.getBootstrapUnreconciledDatabases()).containsExactly(DB_A);

    sm.applyDropDatabaseEntry(RaftLogEntryCodec.decode(RaftLogEntryCodec.encodeDropDatabaseEntry(DB_A)));

    assertThat(sm.getBootstrapUnreconciledDatabases()).isEmpty();
    assertThat(newStateMachine().getBootstrapUnreconciledDatabases())
        .as("the dropped database's mark does not linger in the persisted file")
        .isEmpty();
  }

  /**
   * One database's divergence does not disturb another database's baseline: the mark is written into the
   * shared baselines file, which is rewritten whole on every change.
   */
  @Test
  void markingOneDatabasePreservesAnotherDatabasesBaseline() {
    final ArcadeStateMachine sm = newStateMachine();
    sm.applyBootstrapFingerprintEntry(RaftLogEntryCodec.decode(encodeBaseline(DB_OTHER, "c".repeat(64), 7L)), 49L);
    applyStaleBaseline(sm);

    final ArcadeStateMachine reopened = newStateMachine();
    assertThat(reopened.getBootstrapUnreconciledDatabases()).containsExactly(DB_A);
    assertThat(reopened.getBootstrapBaseline(DB_OTHER)).isNotNull();
    assertThat(reopened.getBootstrapBaseline(DB_OTHER).fingerprint()).isEqualTo("c".repeat(64));
    assertThat(reopened.getBootstrapBaseline(DB_A))
        .as("the refused database still records the committed baseline it refused")
        .isNotNull();
  }

  /**
   * A marked database that is not loaded on this node keeps its mark. {@code existsDatabase} answers
   * "is it in the registry", not "is it on disk", so treating absence as convergence would silently
   * retire a real divergence for a database an operator merely left closed. The mark is also read back
   * from a hand-written file here, which pins the persisted flag's reader independently of its writer.
   */
  @Test
  void aDatabaseThatIsNotLoadedKeepsItsDivergence() throws Exception {
    final Path raftDir = serverDir.resolve(".raft");
    Files.createDirectories(raftDir);
    Files.writeString(raftDir.resolve("bootstrap-baselines"),
        "{\"" + DB_OTHER + "\":{\"fingerprint\":\"" + "e".repeat(64) + "\",\"lastTxId\":4,\"unreconciled\":true}}");

    final ArcadeStateMachine sm = newStateMachine();
    assertThat(sm.getBootstrapUnreconciledDatabases()).containsExactly(DB_OTHER);

    sm.reconcileBootstrapDivergence(Map.of(DB_OTHER, new BootstrapBaseline("e".repeat(64), 4L)));

    assertThat(sm.getBootstrapUnreconciledDatabases())
        .as("a database that is not loaded here is neither cleared nor opened to be fingerprinted")
        .containsExactly(DB_OTHER);
  }

  /**
   * A mark is only ever loaded for an entry that also carries a baseline. That is what makes "every
   * marked database has a baseline" an invariant of the in-memory state rather than a property of the
   * current call graph, and therefore what lets the writer iterate the baselines alone without dropping
   * a mark on the floor.
   */
  @Test
  void aMarkWithoutABaselineIsNotLoaded() throws Exception {
    final Path raftDir = serverDir.resolve(".raft");
    Files.createDirectories(raftDir);
    Files.writeString(raftDir.resolve("bootstrap-baselines"), "{\"" + DB_OTHER + "\":{\"unreconciled\":true}}");

    final ArcadeStateMachine sm = newStateMachine();

    assertThat(sm.getBootstrapUnreconciledDatabases()).isEmpty();
    assertThat(sm.getBootstrapBaseline(DB_OTHER)).isNull();
  }

  /** With nothing diverged the verification is a no-op that cannot invent an alert. */
  @Test
  void reconcilingWithNoDivergenceDoesNothing() {
    final ArcadeStateMachine sm = newStateMachine();

    sm.reconcileBootstrapDivergence(Map.of(DB_A, new BootstrapBaseline("d".repeat(64), 3L)));

    assertThat(sm.getBootstrapUnreconciledDatabases()).isEmpty();
  }

  /**
   * The probe throttle: one attempt per window, and a second attempt inside it stands down. Without this
   * the health tick (seconds apart) would make the leader hash every database directory it holds on every
   * tick, forever, to reach a conclusion that only changes when an operator acts.
   */
  @Test
  void theProbeIsThrottledToOneAttemptPerWindow() {
    final ArcadeStateMachine sm = newStateMachine();

    assertThat(sm.claimBootstrapDivergenceCheckSlot(1_000_000L, 300_000L))
        .as("the first attempt claims the slot").isTrue();
    assertThat(sm.claimBootstrapDivergenceCheckSlot(1_000_001L, 300_000L))
        .as("an attempt inside the window stands down").isFalse();
    assertThat(sm.claimBootstrapDivergenceCheckSlot(1_299_999L, 300_000L))
        .as("still inside the window").isFalse();
    assertThat(sm.claimBootstrapDivergenceCheckSlot(1_300_000L, 300_000L))
        .as("the next window re-arms").isTrue();
  }

  /**
   * The whole verification stands down when there is no Raft server to name a leader, and does so
   * WITHOUT burning the throttle slot: a node that spends its first ticks without a leader must still get
   * a full-rate first probe once one appears.
   */
  @Test
  void verificationWithoutARaftServerIsANoOpThatKeepsTheThrottleSlot() {
    final ArcadeStateMachine sm = applyStaleBaseline(newStateMachine());

    sm.verifyBootstrapDivergence();

    assertThat(sm.getBootstrapUnreconciledDatabases())
        .as("the mark is untouched when the check cannot run").containsExactly(DB_A);
    assertThat(sm.claimBootstrapDivergenceCheckSlot(1_000_000L, 300_000L))
        .as("no throttle slot was consumed").isTrue();
  }
}
