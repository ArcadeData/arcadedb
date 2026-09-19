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
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerDatabase;
import com.arcadedb.utility.FileUtils;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7519BootstrapWindowGateTest {

  private static final String DB_DIR  = "./target/databases";
  private static final String DB_NAME = "test-7519-bootstrap-window";
  private static final String DB_PATH = DB_DIR + "/" + DB_NAME;

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
    if (localDb != null && localDb.isOpen())
      localDb.close();
    FileUtils.deleteRecursively(new File(DB_PATH));
    FileUtils.deleteRecursively(new File(DB_DIR + "/.raft"));
  }

  private ContextConfiguration configuration() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, DB_DIR);
    // Fail the download on the first attempt: there is no leader to pull from in a unit test, and the
    // exponential backoff would only make the test slow.
    config.setValue(GlobalConfiguration.HA_SNAPSHOT_INSTALL_RETRIES, 0);
    config.setValue(GlobalConfiguration.HA_SNAPSHOT_INSTALL_RETRY_BASE_MS, 0L);
    config.setValue(GlobalConfiguration.NETWORK_USE_SSL, false);
    return config;
  }

  private ArcadeDBServer stubbedServer(final ContextConfiguration config) {
    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.getConfiguration()).thenReturn(config);
    when(server.existsDatabase(DB_NAME)).thenReturn(true);
    // The server arg is null: the paths reached here dereference the wrapped localDb, not the server.
    when(server.getDatabase(DB_NAME)).thenReturn(new ServerDatabase(null, localDb));
    return server;
  }

  /** The baseline of a copy this node does not have: never matches, so the mismatch arm reinstalls. */
  private static RaftLogEntryCodec.DecodedEntry baselineOfAnotherCopy(final long lastTxId) {
    final ByteString encoded = RaftLogEntryCodec.encodeBootstrapFingerprintEntry(DB_NAME, "0".repeat(64), lastTxId);
    return RaftLogEntryCodec.decode(encoded);
  }

  /**
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
    final ArcadeDBServer server = stubbedServer(config);
    final ArcadeStateMachine sm = new ArcadeStateMachine();

    final AtomicReference<String> reasonInsideTheInstall = new AtomicReference<>();
    final AtomicReference<List<String>> namesInsideTheInstall = new AtomicReference<>();
    when(server.getBackupCoordinator()).thenAnswer(invocation -> {
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

  /**
   * A failed install hands over to a durable mark rather than to nothing. This is the path the gate leaks on if
   * the handover is missing, and it is the LIKELY path during a real first formation: the bootstrap entry is
   * applied while leader election on this peer may not have settled, so the very first download often has no
   * leader to pull from.
   * <p>
   * The in-flight registration is released - the node is genuinely not installing any more, and holding readiness
   * on a condition nothing clears would wedge it out of the Service for good. What replaces it is the
   * unreconciled mark, which is persisted in {@code .raft/bootstrap-baselines}, so unlike the in-flight set it
   * survives the {@code restartRatis} that rebuilds a state machine - and the condition it reports (a copy the
   * committed baseline rejected is still on disk) survives a restart too.
   */
  @Test
  void aFailedBootstrapInstallHandsReadinessOverToTheDurableUnreconciledMark() {
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    sm.setServer(stubbedServer(configuration()));

    assertThatNoException().isThrownBy(
        () -> sm.applyBootstrapFingerprintEntry(baselineOfAnotherCopy(Long.MAX_VALUE), 7L));

    assertThat(sm.getBootstrapInstallsInFlight())
        .as("the node is not installing any more, so it is not held out on that ground")
        .isEmpty();
    assertThat(sm.getBootstrapUnreconciledDatabases())
        .as("but the copy the committed baseline rejected is still on disk, and only this records that")
        .containsExactly(DB_NAME);
    assertThat(sm.bootstrapWindowReason())
        .as("so the node stays out of the Service across the scheduled retry, not only during the install")
        .isNotNull();
  }

  /**
   * Both conditions at once - an install replacing one database while another sits unreconciled - are reported
   * together. An operator who read only the install would go on believing the node comes back by itself once the
   * install finishes, which is the one case where it does not. Observed from inside the install, the only moment
   * at which both are true.
   */
  @Test
  void bothHalvesOfTheWindowAreReportedWhenBothHold() {
    final ArcadeDBServer server = stubbedServer(configuration());
    final ArcadeStateMachine sm = new ArcadeStateMachine();

    final AtomicReference<String> reasonInsideTheInstall = new AtomicReference<>();
    when(server.getBackupCoordinator()).thenAnswer(invocation -> {
      reasonInsideTheInstall.set(sm.bootstrapWindowReason());
      return null;
    });

    sm.setServer(server);
    // A second database left unreconciled by an earlier pass, which no install in this test will clear.
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

  /**
   * The ordinary case has to stay ordinary: a peer whose copy IS the committed baseline installs nothing, is
   * marked nothing, and reports itself ready. A gate that held a healthy first formation out of the Service
   * would be worse than the hole it closes.
   */
  @Test
  void aPeerThatMatchesTheBaselineIsNeverHeldOutOfTheService() {
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    sm.setServer(stubbedServer(configuration()));

    final ByteString encoded = RaftLogEntryCodec.encodeBootstrapFingerprintEntry(
        DB_NAME, BootstrapFingerprint.compute(new File(DB_PATH)), localDb.getLastTransactionId());

    assertThatNoException().isThrownBy(() -> sm.applyBootstrapFingerprintEntry(RaftLogEntryCodec.decode(encoded), 7L));

    assertThat(sm.getBootstrapInstallsInFlight()).isEmpty();
    assertThat(sm.getBootstrapUnreconciledDatabases()).isEmpty();
    assertThat(sm.bootstrapWindowReason())
        .as("a matching peer is serving the cluster's own copy and belongs in the Service")
        .isNull();
  }

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
  void anOverlappingInstallOfTheSameDatabaseDoesNotReleaseTheGateEarly() {
    final ArcadeDBServer server = stubbedServer(configuration());
    final ArcadeStateMachine sm = new ArcadeStateMachine();

    final AtomicReference<List<String>> inFlightAfterTheInnerInstallReturned = new AtomicReference<>();
    final AtomicBoolean reentered = new AtomicBoolean();
    when(server.getBackupCoordinator()).thenAnswer(invocation -> {
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
    assertThat(sm.getBootstrapInstallsInFlight())
        .as("and once both holders are gone the entry is removed rather than left behind at zero")
        .isEmpty();
  }

  /** No bootstrap has happened at all: nothing to report, and nothing allocated on the readiness-probe path. */
  @Test
  void aNodeThatNeverBootstrappedReportsNoReason() {
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    sm.setServer(stubbedServer(configuration()));

    assertThat(sm.bootstrapWindowReason()).isNull();
    assertThat(sm.getBootstrapInstallsInFlight()).isEmpty();
  }
}
