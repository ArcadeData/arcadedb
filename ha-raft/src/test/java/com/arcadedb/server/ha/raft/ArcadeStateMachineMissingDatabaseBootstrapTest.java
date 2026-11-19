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
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.FakeArcadeDBServer;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.awaitility.Awaitility.await;

/**
 * Unit tests of how the {@link ArcadeStateMachine} bootstrap-baseline path treats a database this node does not have:
 * the replay of a baseline entry for a wiped database, and the alert that must still be published for it.
 */
class ArcadeStateMachineMissingDatabaseBootstrapTest {

  private static final String DB_NAME     = "wiped-and-resynced";
  private static final long   ENTRY_INDEX = 50L;

  @TempDir
  private Path serverDir;

  /**
   * Every state machine this class builds, closed in {@link #closeStateMachines()}.
   * <p>
   * A state machine owns two executors, and an install that fails - which is what half of these tests drive -
   * leaves work queued on one of them. Left running, those threads outlive the test method and race JUnit's
   * {@code @TempDir} cleanup for {@code serverDir/.raft}, which surfaces as a {@code DirectoryNotEmptyException}
   * out of the extension rather than as any test failing: the CI run of PR #7941 hit exactly that with all six
   * tests green. Closing them is what makes the directory quiet before JUnit deletes it.
   */
  private final List<ArcadeStateMachine> stateMachines = new ArrayList<>();
  private static final String MISSING_DB = "wiped-and-not-back";
  private static final String KEPT_DB    = "fresher-than-the-cluster";

  /**
   * {@code SnapshotInstaller.install} asks the server for its backup coordinator before it does anything else, so
   * that call is the proof the install was actually reached - independent of the log lines, which is what the old
   * guard got wrong in the first place.
   */
  private FakeArcadeDBServer mockServerWithDatabaseRegistered(final boolean registered) {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, serverDir.toString());
    // 0 retries with no backoff: the leader is unknown in this unit test, so the install fails immediately
    // instead of sleeping through an exponential-backoff budget.
    config.setValue(GlobalConfiguration.HA_SNAPSHOT_INSTALL_RETRIES, 0);
    config.setValue(GlobalConfiguration.HA_SNAPSHOT_INSTALL_RETRY_BASE_MS, 0L);
    config.setValue(GlobalConfiguration.NETWORK_USE_SSL, false);

    final FakeArcadeDBServer server = FakeArcadeDBServer.create();
    server.returns("getConfiguration", config);
    server.on("existsDatabase", args -> Objects.equals(args[0], DB_NAME) ? registered : false);
    return server;
  }

  @AfterEach
  void closeStateMachines() {
    for (final ArcadeStateMachine sm : stateMachines)
      try {
        sm.close();
      } catch (final IOException e) {
        // Teardown of a unit-test fixture: a close that fails must not replace the test's own verdict.
      }
    stateMachines.clear();
  }

  private ArcadeStateMachine stateMachineOn(final ArcadeDBServer server) {
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    sm.setServer(server);
    stateMachines.add(sm);
    return sm;
  }

  private static RaftLogEntryCodec.DecodedEntry bootstrapEntry() throws Exception {
    final ByteString encoded = RaftLogEntryCodec.encodeBootstrapFingerprintEntry(DB_NAME, "0".repeat(64), 7L);
    return RaftLogEntryCodec.decode(encoded);
  }

  // Regression for #7298: the baseline replay does not skip a database that is no longer on disk
  /**
   * Regression test for issue #7298, the sibling of #7221.
   * <p>
   * #7221 fixed the {@code forceSnapshot} arm of {@code applyInstallDatabaseEntry}, which returned on a persisted
   * applied index without asking whether the database was still on disk.
   * {@link ArcadeStateMachine#applyBootstrapFingerprintEntry} had the identical guard 150 lines away in the same
   * class, with its {@code existsDatabase} check sitting one line PAST the early return that skipped it.
   * <p>
   * The input is the same and so is the consequence. {@code .raft/applied-index} is a SIBLING of the per-database
   * directories, so the wipe-and-resync recovery this repo's runbook prescribes - stop the follower, delete the bad
   * copy, start it again - leaves the entry behind while the database it describes is gone. On replay the guard
   * fired, logged that verification had completed, and returned. Nothing else brought the database back either: a
   * bootstrap-baselined database predates the cluster, so there is no {@code INSTALL_DATABASE_ENTRY} in the log for
   * the "late joiner" arm below the guard to be waiting for, and {@code triggerSnapshotDownload} only reinstalls
   * the databases the server already has REGISTERED - which is precisely the one it does not have.
   * <p>
   * The premise is now proved before the skip, and the not-registered case reinstalls from the leader, which is the
   * action #7221's own fix takes in the same situation on the install path.
   * <p>
   * The defect. The applied index says a previous session applied this very entry, the database is not registered
   * now, and before the fix the guard returned anyway - claiming a completed verification for a database that is
   * not there, and leaving the node running permanently short of it.
   */
  @Test
  void anAppliedEntryWhoseDatabaseIsGoneReinstallsItInsteadOfSkipping() throws Exception {
    final FakeArcadeDBServer server = mockServerWithDatabaseRegistered(false);
    final ArcadeStateMachine sm = stateMachineOn(server);
    sm.writePersistedAppliedIndex(ENTRY_INDEX, DB_NAME);

    // The install fails (no leader is reachable from a unit test) and that failure must stay contained: this runs
    // on the Raft StateMachineUpdater thread, where an escaping exception trips the critical-error halt.
    assertThatNoException().isThrownBy(() -> sm.applyBootstrapFingerprintEntry(bootstrapEntry(), ENTRY_INDEX));

    assertThat(server.calls("getBackupCoordinator")).isNotEmpty();
    // The failed install hands off to a retry on the lifecycle executor, which persists the unreconciled mark under
    // .raft before it releases its holder. Waiting for the release keeps that write from racing the @TempDir
    // cleanup, which surfaced as "Failed to close extension context" with the test itself green.
    await().atMost(Duration.ofSeconds(30)).until(() -> sm.getBootstrapInstallsInFlight().isEmpty());
  }

  // Regression for #7298: the baseline replay does not skip a database that is no longer on disk
  /**
   * The legitimate replay-skip is preserved: with the database registered, the same input still returns without
   * touching the install machinery. This is what #4824's own test asserts, restated against the install path so a
   * fix for #7298 that simply deleted the guard fails here.
   */
  @Test
  void anAppliedEntryWhoseDatabaseIsStillHereStillSkips() throws Exception {
    final FakeArcadeDBServer server = mockServerWithDatabaseRegistered(true);
    final ArcadeStateMachine sm = stateMachineOn(server);
    sm.writePersistedAppliedIndex(ENTRY_INDEX, DB_NAME);

    sm.applyBootstrapFingerprintEntry(bootstrapEntry(), ENTRY_INDEX);

    assertThat(server.calls("getBackupCoordinator")).isEmpty();
    // The baseline is recorded before the skip either way, as it always was.
    assertThat(sm.getBootstrapBaseline(DB_NAME)).isNotNull();
  }

  // Regression for #7298: the baseline replay does not skip a database that is no longer on disk
  /**
   * The failed reinstall must not be a log line and nothing else. {@code applyTransaction} persists this
   * database's applied index whatever happens in this method, so the entry reads as applied while the database is
   * absent, and the replay that would retry it is not guaranteed to survive the next Ratis snapshot. The mark is
   * what outlives that: it is persisted with the baselines, published by {@code ClusterAlerts}, and re-verified
   * on the health tick.
   * <p>
   * Awaited rather than asserted outright: the mark is written by the one-shot retry, which runs on the
   * lifecycleExecutor so that a failing install never blocks the Raft apply thread.
   */
  @Test
  void aFailedReinstallIsRecordedDurablyRatherThanOnlyLogged() throws Exception {
    final FakeArcadeDBServer server = mockServerWithDatabaseRegistered(false);
    final ArcadeStateMachine sm = stateMachineOn(server);
    sm.writePersistedAppliedIndex(ENTRY_INDEX, DB_NAME);

    sm.applyBootstrapFingerprintEntry(bootstrapEntry(), ENTRY_INDEX);

    await().atMost(Duration.ofSeconds(30))
        .untilAsserted(() -> assertThat(sm.getBootstrapUnreconciledDatabases())
            .as("a database this node had, lost, and could not pull back must be visible in the cluster status")
            .contains(DB_NAME));
  }

  // Regression for #7298: the baseline replay does not skip a database that is no longer on disk
  /**
   * The bounded retry: the periodic bootstrap-divergence check reinstalls a marked database whose files are gone.
   * Its absent-database arm used to {@code continue} unconditionally, which was right for the #6124 case it was
   * written for - a database that is merely CLOSED still has files worth protecting - and wrong for this one,
   * where there is nothing local to protect and nothing else retries.
   * <p>
   * The mark is set directly rather than by replaying an entry: this test is about what the periodic check does
   * with a marked-and-missing database, and going through the apply path would race its asynchronous retry
   * against the synchronous one being measured.
   */
  @Test
  void thePeriodicCheckRetriesTheInstallWhenTheDirectoryIsGone() {
    final FakeArcadeDBServer server = mockServerWithDatabaseRegistered(false);
    final ArcadeStateMachine sm = stateMachineOn(server);
    sm.markBootstrapUnreconciled(DB_NAME);

    sm.reconcileBootstrapDivergence(Map.of(DB_NAME, new ArcadeStateMachine.BootstrapBaseline("0".repeat(64), 7L)));

    assertThat(server.calls("getBackupCoordinator")).isNotEmpty();
    assertThat(sm.getBootstrapUnreconciledDatabases())
        .as("the install failed again, so the mark stays for the next tick")
        .contains(DB_NAME);
  }

  // Regression for #7298: the baseline replay does not skip a database that is no longer on disk
  /**
   * The other half of that rule: a marked database whose DIRECTORY is still on disk is only closed, and the check
   * must leave it exactly as it was rather than reinstall over an operator's copy.
   */
  @Test
  void thePeriodicCheckLeavesAClosedButPresentDatabaseAlone() throws Exception {
    final FakeArcadeDBServer server = mockServerWithDatabaseRegistered(false);
    final ArcadeStateMachine sm = stateMachineOn(server);
    sm.markBootstrapUnreconciled(DB_NAME);
    // A closed database, files and all: an EMPTY directory is what a failed install leaves behind, and holds no
    // copy of anything (issue #8045).
    Files.writeString(Files.createDirectories(serverDir.resolve(DB_NAME)).resolve(LocalSchema.SCHEMA_FILE_NAME), "{}");

    sm.reconcileBootstrapDivergence(Map.of(DB_NAME, new ArcadeStateMachine.BootstrapBaseline("0".repeat(64), 7L)));

    assertThat(server.calls("getBackupCoordinator")).isEmpty();
    assertThat(sm.getBootstrapUnreconciledDatabases())
        .as("a closed database keeps its mark: absence from the registry is not evidence of convergence")
        .contains(DB_NAME);
  }

  // Regression for #7298: the baseline replay does not skip a database that is no longer on disk
  /**
   * A genuine late joiner - no local copy and NO evidence it ever had one - must keep waiting for the follow-on
   * install entry rather than pulling a snapshot on its own. That is the first-formation path, and turning it
   * into an install would make every peer of a forming cluster race the leader for a database that is about to
   * be shipped to it anyway.
   */
  @Test
  void aGenuineLateJoinerStillWaitsForTheFollowOnInstallEntry() throws Exception {
    final FakeArcadeDBServer server = mockServerWithDatabaseRegistered(false);
    final ArcadeStateMachine sm = stateMachineOn(server);
    // No persisted applied index for this database at all: readPersistedAppliedIndex(dbName) answers -1.

    sm.applyBootstrapFingerprintEntry(bootstrapEntry(), ENTRY_INDEX);

    assertThat(server.calls("getBackupCoordinator")).isEmpty();
    assertThat(sm.getBootstrapBaseline(DB_NAME)).isNotNull();
  }

  /**
   * A server that has NO database registered, which is the state a node is in for a database it lost: the
   * authorization filter {@code GetClusterHandler} builds from the registry is therefore empty, exactly as it is
   * in production on this path.
   */
  private ArcadeStateMachine stateMachineOnAnEmptyRegistry() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, serverDir.toString());

    final FakeArcadeDBServer server = FakeArcadeDBServer.create((String) null, config);

    final ArcadeStateMachine sm = new ArcadeStateMachine();
    sm.setServer(server);
    stateMachines.add(sm);
    return sm;
  }

  private static JSONObject alertWithId(final JSONArray alerts, final String id) {
    for (int i = 0; i < alerts.length(); i++) {
      final JSONObject alert = alerts.getJSONObject(i);
      if (id.equals(alert.getString("id", null)))
        return alert;
    }
    return null;
  }

  // Regression for #7902: the missing-database mark is published as an alert and not filtered by the registry
  /**
   * Regression test for issue #7902: the durable mark the #7298 fix hangs its whole guarantee on was never
   * published, because the alert builder filtered it through the databases this node still HAS.
   * <p>
   * {@code ClusterAlerts.checkBootstrapDivergedDatabases} passed the marked set through {@code visible(...)}, and
   * {@code GetClusterHandler} builds that filter from {@code ArcadeDBServer.getDatabaseNames()} - the in-memory
   * database registry. A database that is missing from this node is, by definition, not in it. So the filter
   * removed precisely the databases the #7298 mark exists for, the builder received an empty list and emitted
   * nothing, and {@code GET /api/v1/cluster} answered {@code alerts: []} on a node knowingly running without a
   * database the cluster has. The filter is right for what it was written for - a tenant must not learn another
   * tenant's database name from a status poll - and the #6124 case it was written against always has the database
   * registered, so it never removed anything that mattered until now.
   * <p>
   * The second defect was behind the same call. If the database ever WAS registered again while the mark stood,
   * the alert that finally fired described the opposite condition: "Database(s) kept a local copy the cluster never
   * adopted", "the copies are otherwise intact - nothing has been lost", and a recommendation to copy this node's
   * database directory to every peer. Following that for a database this node is missing would overwrite every good
   * copy in the cluster.
   * <p>
   * Both are fixed by splitting the marked set on where the database is NOW, not on why it was marked: a mark is
   * durable and the condition under it is not, so an operator who restores a directory by hand moves the database
   * from one half to the other with nothing re-running the branch that marked it.
   * <p>
   * The defect, at the seam it actually failed at: the caller's visible set is the registry, the registry does not
   * contain a database this node is missing, and the alert has to fire anyway. Whether this node is serving a
   * database the cluster has is a node-level fact, like {@code localResync.inProgress}, and the filter reduces its
   * NAMES rather than deciding whether it is reported.
   */
  @Test
  void aMissingDatabaseIsReportedAlthoughTheRegistryFilterCannotContainIt() {
    final ArcadeStateMachine sm = stateMachineOnAnEmptyRegistry();
    sm.markBootstrapUnreconciled(MISSING_DB);

    final JSONArray alerts = new JSONArray();
    // Set.of() is precisely what filterAuthorizedDatabases(user, server.getDatabaseNames()) answers here.
    ClusterAlerts.checkBootstrapDivergedDatabases(sm, alerts, Set.of());

    final JSONObject missing = alertWithId(alerts, "bootstrap-database-missing");
    assertThat(missing)
        .as("the one case the registry-derived filter silently swallowed")
        .isNotNull();
    assertThat(missing.getJSONObject("details").getInt("count"))
        .as("the count is the raw figure, so a caller who may be told no name still learns the node is short")
        .isEqualTo(1);
    assertThat(missing.getJSONObject("details").getJSONArray("databases"))
        .as("but the name itself stays scoped")
        .isEmpty();
  }

  // Regression for #7902: the missing-database mark is published as an alert and not filtered by the registry
  /**
   * And it is not reported under the alert whose text says the opposite. The kept-copy alert promises the data is
   * intact and tells an operator to copy this node's directory to every peer, which for a database this node does
   * not have would destroy every good copy in the cluster.
   */
  @Test
  void aMissingDatabaseIsNotReportedAsAKeptLocalCopy() {
    final ArcadeStateMachine sm = stateMachineOnAnEmptyRegistry();
    sm.markBootstrapUnreconciled(MISSING_DB);

    final JSONArray alerts = new JSONArray();
    ClusterAlerts.checkBootstrapDivergedDatabases(sm, alerts, null);

    assertThat(alertWithId(alerts, "bootstrap-diverged-databases"))
        .as("a node with no copy has kept nothing")
        .isNull();
    final JSONObject missing = alertWithId(alerts, "bootstrap-database-missing");
    assertThat(missing.getJSONObject("details").getJSONArray("databases").getString(0)).isEqualTo(MISSING_DB);
    assertThat(missing.getString("recommendation"))
        .as("the remedy must not be the one that overwrites every good copy in the cluster")
        .doesNotContain("copy this node's database directory to every peer");
  }

  // Regression for #7902: the missing-database mark is published as an alert and not filtered by the registry
  /**
   * The #6124 case is untouched: a marked database whose directory is on disk is a copy this node KEPT - merely
   * closed, not gone - and it keeps the alert whose text was written for it, filter and all.
   */
  @Test
  void aKeptLocalCopyStillGetsTheDivergenceAlert() throws Exception {
    final ArcadeStateMachine sm = stateMachineOnAnEmptyRegistry();
    sm.markBootstrapUnreconciled(KEPT_DB);
    // A closed database, files and all: an EMPTY directory is what a failed install leaves behind, and holds no
    // copy of anything (issue #8045).
    Files.writeString(Files.createDirectories(serverDir.resolve(KEPT_DB)).resolve(LocalSchema.SCHEMA_FILE_NAME), "{}");

    final JSONArray alerts = new JSONArray();
    ClusterAlerts.checkBootstrapDivergedDatabases(sm, alerts, null);

    final JSONObject diverged = alertWithId(alerts, "bootstrap-diverged-databases");
    assertThat(diverged).isNotNull();
    assertThat(diverged.getJSONObject("details").getJSONArray("databases").getString(0)).isEqualTo(KEPT_DB);
    assertThat(alertWithId(alerts, "bootstrap-database-missing"))
        .as("nothing is missing here")
        .isNull();
  }

  // Regression for #7902: the missing-database mark is published as an alert and not filtered by the registry
  /**
   * Both conditions at once, which is the state a node reaches by losing one database and refusing to overwrite
   * another. Each database must appear in exactly one of the two alerts, or an operator reading either one acts
   * on the wrong database.
   */
  @Test
  void theTwoConditionsAreReportedSeparatelyWhenBothHold() throws Exception {
    final ArcadeStateMachine sm = stateMachineOnAnEmptyRegistry();
    sm.markBootstrapUnreconciled(MISSING_DB);
    sm.markBootstrapUnreconciled(KEPT_DB);
    // A closed database, files and all: an EMPTY directory is what a failed install leaves behind, and holds no
    // copy of anything (issue #8045).
    Files.writeString(Files.createDirectories(serverDir.resolve(KEPT_DB)).resolve(LocalSchema.SCHEMA_FILE_NAME), "{}");

    final JSONArray alerts = new JSONArray();
    ClusterAlerts.checkBootstrapDivergedDatabases(sm, alerts, null);

    assertThat(alertWithId(alerts, "bootstrap-diverged-databases").getJSONObject("details").getJSONArray("databases"))
        .map(Object::toString).containsExactly(KEPT_DB);
    assertThat(alertWithId(alerts, "bootstrap-database-missing").getJSONObject("details").getJSONArray("databases"))
        .map(Object::toString).containsExactly(MISSING_DB);
  }

  // Regression for #7902: the missing-database mark is published as an alert and not filtered by the registry
  /**
   * The kept-copy alert keeps its old scoping, and that is deliberate rather than an oversight: it is a statement
   * about specific databases whose remedy is per database, so a caller authorized on none of them has nothing to
   * read and nothing to do. Only the missing half is node-scoped.
   */
  @Test
  void theKeptCopyAlertIsStillSuppressedForACallerThatMayNotSeeIt() throws Exception {
    final ArcadeStateMachine sm = stateMachineOnAnEmptyRegistry();
    sm.markBootstrapUnreconciled(KEPT_DB);
    // A closed database, files and all: an EMPTY directory is what a failed install leaves behind, and holds no
    // copy of anything (issue #8045).
    Files.writeString(Files.createDirectories(serverDir.resolve(KEPT_DB)).resolve(LocalSchema.SCHEMA_FILE_NAME), "{}");

    final JSONArray alerts = new JSONArray();
    ClusterAlerts.checkBootstrapDivergedDatabases(sm, alerts, Set.of("some-other-tenants-database"));

    assertThat(alerts).isEmpty();
  }

  // Regression for #7902: the missing-database mark is published as an alert and not filtered by the registry
  /**
   * An empty marked set costs nothing, which matters because this runs on every Studio status poll: no allocation
   * and, in particular, no directory stat per database.
   */
  @Test
  void anUnmarkedNodeEmitsNothingAndTouchesNoDisk() {
    final ArcadeStateMachine sm = stateMachineOnAnEmptyRegistry();

    final ArcadeStateMachine.BootstrapUnreconciled unreconciled = sm.getBootstrapUnreconciled(null);

    assertThat(unreconciled.keptLocalCopy()).isEmpty();
    assertThat(unreconciled.missingLocally()).isEmpty();
    assertThat(unreconciled.missingCount()).isZero();

    final JSONArray alerts = new JSONArray();
    ClusterAlerts.checkBootstrapDivergedDatabases(sm, alerts, null);
    assertThat(alerts).isEmpty();
  }
}
