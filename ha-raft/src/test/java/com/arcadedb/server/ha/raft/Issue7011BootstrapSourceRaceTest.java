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
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issue #7011: the formation-time bootstrap election racing the leader's own database creation
 * and seeding.
 * <p>
 * The election samples every local database at collect time, so a database the leader created milliseconds
 * earlier is baselined at {@code lastTxId=0}; by the time the leader's own state machine applies the committed
 * {@code BOOTSTRAP_FINGERPRINT_ENTRY} the seeding has advanced the copy, and the "local is fresher, refusing to
 * overwrite" guard fired on the very peer that sourced the baseline. Two apply-side rules close it deterministically:
 * the bootstrap source is exempt from the refusal (its copy IS the baseline), and a baseline for a database that
 * already has Raft history on the node is superseded and ignored on every peer alike.
 * <p>
 * Same harness as {@code ArcadeStateMachineBootstrapDivergenceTest}: a real unstarted {@link ArcadeDBServer} and a
 * real {@link LocalDatabase}, no mocking framework.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7011BootstrapSourceRaceTest {

  private static final String DB = "config-db";

  @TempDir
  private Path           serverDir;
  private ArcadeDBServer server;
  private LocalDatabase  localDb;
  private String         dbPath;

  @BeforeEach
  void setUp() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, serverDir.toString());
    server = new ArcadeDBServer(config);

    dbPath = serverDir.resolve(DB).toString();
    localDb = (LocalDatabase) new DatabaseFactory(dbPath).create();
    // The application seeds the database right after creating it: the copy moves past the sampled baseline.
    localDb.getSchema().createDocumentType("Seed");
    localDb.transaction(() -> localDb.newDocument("Seed").set("k", 1).save());
    server.registerDatabase(DB, localDb);
    assertThat(localDb.getLastTransactionId()).isGreaterThan(0);
  }

  @AfterEach
  void tearDown() {
    if (localDb != null && localDb.isOpen())
      localDb.close();
    if (dbPath != null)
      FileUtils.deleteRecursively(new File(dbPath));
  }

  private ArcadeStateMachine newStateMachine() {
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    sm.setServer(server);
    return sm;
  }

  /** The baseline the election sampled when the database was still empty. */
  private RaftLogEntryCodec.DecodedEntry emptyBaseline() {
    return RaftLogEntryCodec.decode(RaftLogEntryCodec.encodeBootstrapFingerprintEntry(DB, "0".repeat(64), 0L));
  }

  /**
   * The reported failure: the leader that committed the baseline applies it after its own seeding advanced the
   * copy. The source's copy is the baseline by definition, so the refusal must not fire on it.
   */
  @Test
  void theBootstrapSourceIsNotPoisonedByItsOwnConcurrentWrites() {
    final ArcadeStateMachine sm = newStateMachine();

    sm.applyBootstrapFingerprintEntry(emptyBaseline(), 3L, true);

    assertThat(sm.getBootstrapUnreconciledDatabases())
        .as("the source's copy advancing past the sampled baseline is the expected outcome, not divergence")
        .isEmpty();
    assertThat(sm.getBootstrapBaseline(DB)).as("the committed baseline is still recorded for status export").isNotNull();
    assertThat(sm.getBootstrapBaseline(DB).lastTxId()).isZero();
    assertThat(localDb.isOpen()).isTrue();
  }

  /**
   * The source never falls through to the mismatch branch, whatever its copy reads: a baseline above the local
   * transaction id is impossible for the copy it was sampled from, and the answer is still "keep the copy".
   */
  @Test
  void theBootstrapSourceKeepsItsCopyEvenBehindItsOwnBaseline() {
    final ArcadeStateMachine sm = newStateMachine();
    final RaftLogEntryCodec.DecodedEntry aheadOfLocal = RaftLogEntryCodec.decode(
        RaftLogEntryCodec.encodeBootstrapFingerprintEntry(DB, "0".repeat(64), localDb.getLastTransactionId() + 100));

    sm.applyBootstrapFingerprintEntry(aheadOfLocal, 3L, true);

    assertThat(sm.getBootstrapUnreconciledDatabases()).isEmpty();
    assertThat(localDb.isOpen()).as("the copy is kept open, not closed for a reinstall from itself").isTrue();
    assertThat(sm.getBootstrapBaseline(DB)).isNotNull();
  }

  /**
   * Regression guard for the #6124 behaviour this fix must not weaken: a peer that did NOT source the baseline and
   * holds a fresher copy with no Raft history is still refused and marked diverged.
   */
  @Test
  void aNonSourcePeerWithAFresherStrayCopyIsStillRefused() {
    final ArcadeStateMachine sm = newStateMachine();

    sm.applyBootstrapFingerprintEntry(emptyBaseline(), 3L, false);

    assertThat(sm.getBootstrapUnreconciledDatabases()).containsExactly(DB);
  }

  /**
   * A database that an application entry earlier in the log already touched on this node has its history inside
   * the Raft log: the baseline sampled for it is stale by construction and must be ignored, not refused. This is
   * what keeps a follower that acquired the database through replication from being marked diverged, and it is
   * decided from the log order, so every peer reaches the same verdict.
   */
  @Test
  void aBaselineCommittedAfterTheDatabaseGotRaftHistoryIsSuperseded() {
    final ArcadeStateMachine sm = newStateMachine();
    // A schema/tx entry for this database applied at index 2, then the baseline lands at index 3.
    sm.writePersistedAppliedIndex(2L, DB);

    sm.applyBootstrapFingerprintEntry(emptyBaseline(), 3L, false);

    assertThat(sm.getBootstrapUnreconciledDatabases()).as("replication, not bootstrap, owns this database").isEmpty();
    assertThat(sm.getBootstrapBaseline(DB)).as("a baseline the cluster never adopted is not recorded").isNull();
    assertThat(localDb.isOpen()).isTrue();
  }

  /**
   * The superseded rule is per database: history on ANOTHER database does not suppress this one's baseline.
   */
  @Test
  void historyOnAnotherDatabaseDoesNotSupersedeThisBaseline() {
    final ArcadeStateMachine sm = newStateMachine();
    sm.writePersistedAppliedIndex(2L, "other-db");

    sm.applyBootstrapFingerprintEntry(emptyBaseline(), 3L, false);

    assertThat(sm.getBootstrapBaseline(DB)).isNotNull();
    assertThat(sm.getBootstrapUnreconciledDatabases()).containsExactly(DB);
  }

  /**
   * A genuine first formation is untouched: the source's copy equals the baseline and no history precedes it.
   */
  @Test
  void aMatchingBaselineOnTheSourceIsBootstrappedLocally() {
    final ArcadeStateMachine sm = newStateMachine();
    final RaftLogEntryCodec.DecodedEntry matching = RaftLogEntryCodec.decode(RaftLogEntryCodec.encodeBootstrapFingerprintEntry(
        DB, BootstrapFingerprint.compute(new File(dbPath)), localDb.getLastTransactionId()));

    sm.applyBootstrapFingerprintEntry(matching, 3L, true);

    assertThat(sm.getBootstrapUnreconciledDatabases()).isEmpty();
    assertThat(sm.getBootstrapBaseline(DB)).isNotNull();
  }
}
