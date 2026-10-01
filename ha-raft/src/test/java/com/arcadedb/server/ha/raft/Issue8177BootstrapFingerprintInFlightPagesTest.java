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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Regression tests for issue #8177: {@code Issue7519BootstrapWindowGateTest.aPeerThatMatchesTheBaselineIsNeverHeldOutOfTheService}
 * intermittently took the mismatch arm on a peer whose copy IS the baseline.
 * <p>
 * The test sampled the baseline with {@link BootstrapFingerprint#compute(File)} right after its setup committed a
 * transaction, and the state machine recomputed it from the same directory a moment later. A commit hands its pages
 * to the asynchronous flush thread, so the two reads could straddle the flush and hash different bytes. These tests
 * make the window deterministic by holding the flush with {@code PageManager.suspendFlushAndExecute} - the pages of
 * the commit inside the callback are deferred until it returns - instead of waiting for the flush thread to lose a
 * race.
 * <p>
 * Same harness as {@code Issue7519BootstrapWindowGateTest}: a real {@link LocalDatabase} and a stubbed
 * {@link ArcadeDBServer}, zero snapshot-install retries so a mismatch fails its download immediately.
 */
class Issue8177BootstrapFingerprintInFlightPagesTest {

  private static final String DB_DIR  = "./target/databases";
  private static final String DB_NAME = "test-8177-in-flight-pages";
  private static final String DB_PATH = DB_DIR + "/" + DB_NAME;

  private LocalDatabase localDb;

  @BeforeEach
  void setUp() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    FileUtils.deleteRecursively(new File(DB_DIR + "/.raft"));
    localDb = (LocalDatabase) new DatabaseFactory(DB_PATH).create();
    localDb.getSchema().createDocumentType("Seed");
  }

  @AfterEach
  void tearDown() {
    if (localDb != null && localDb.isOpen())
      localDb.close();
    FileUtils.deleteRecursively(new File(DB_PATH));
    FileUtils.deleteRecursively(new File(DB_DIR + "/.raft"));
  }

  private ArcadeStateMachine stateMachine() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, DB_DIR);
    config.setValue(GlobalConfiguration.HA_SNAPSHOT_INSTALL_RETRIES, 0);
    config.setValue(GlobalConfiguration.HA_SNAPSHOT_INSTALL_RETRY_BASE_MS, 0L);
    config.setValue(GlobalConfiguration.NETWORK_USE_SSL, false);

    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.getConfiguration()).thenReturn(config);
    when(server.existsDatabase(DB_NAME)).thenReturn(true);
    when(server.getDatabase(DB_NAME)).thenReturn(new ServerDatabase(null, localDb));

    final ArcadeStateMachine sm = new ArcadeStateMachine();
    sm.setServer(server);
    return sm;
  }

  /**
   * Commits one transaction while the flush of this database is held, and returns the fingerprint of the directory
   * as it stands INSIDE that window - the bytes a sample taken right after a commit can see. The commit's pages land
   * on disk when the window closes.
   */
  private String commitAndSampleWhileItsPagesAreInFlight() throws Exception {
    final AtomicReference<String> sampled = new AtomicReference<>();
    localDb.getPageManager().suspendFlushAndExecute(localDb, () -> {
      commitOneSeed();
      sampled.set(BootstrapFingerprint.compute(new File(DB_PATH)));
    });
    return sampled.get();
  }

  /** The same commit with the flush held, and no sample: only the in-flight pages are wanted. */
  private void commitWhileTheFlushIsHeld() throws Exception {
    localDb.getPageManager().suspendFlushAndExecute(localDb, this::commitOneSeed);
  }

  private void commitOneSeed() {
    localDb.transaction(() -> localDb.newDocument("Seed").set("k", 1).save());
  }

  private static RaftLogEntryCodec.DecodedEntry baseline(final String fingerprint, final long lastTxId) {
    return RaftLogEntryCodec.decode(RaftLogEntryCodec.encodeBootstrapFingerprintEntry(DB_NAME, fingerprint, lastTxId));
  }

  /**
   * The mechanism, pinned: a sample taken while a commit's pages are still in flight is not the copy the state
   * machine fingerprints once they land, and on a peer that turns a matching copy into a reinstall. This is the
   * shape of the old sampling in {@code Issue7519BootstrapWindowGateTest}, with the race won on purpose.
   */
  @Test
  void aBaselineSampledWhileTheLastCommitIsStillInFlightIsNotTheCopyOnDisk() throws Exception {
    final String inFlight = commitAndSampleWhileItsPagesAreInFlight();

    assertThat(SettledBootstrapFingerprint.of(localDb))
        .as("the commit's pages reached the disk after the sample was taken, so the directory hashes differently now. "
            + "If this fails, suspendFlushAndExecute no longer defers the pages of a commit made inside its window, and "
            + "this test no longer reproduces the race")
        .isNotEqualTo(inFlight);

    final ArcadeStateMachine sm = stateMachine();
    assertThatNoException().isThrownBy(() -> sm.applyBootstrapFingerprintEntry(baseline(inFlight, localDb.getLastTransactionId()), 7L));
    sm.awaitLifecycleTasksForTesting(30_000);

    assertThat(sm.getBootstrapInstallsInFlight())
        .as("the state machine read a different copy than the one sampled, and took the mismatch arm")
        .containsExactly(DB_NAME);
  }

  /**
   * The fix: the baseline is sampled from the settled copy, so the state machine's recomputation reads the same bytes
   * and a peer whose copy IS the baseline installs nothing, is marked nothing and stays in the Service. A guard for
   * the helper on the same in-flight commit, deterministic by construction: the drain is explicit.
   */
  @Test
  void aBaselineSampledFromTheSettledCopyMatchesThePeer() throws Exception {
    commitWhileTheFlushIsHeld();

    final String settled = SettledBootstrapFingerprint.of(localDb);
    final ArcadeStateMachine sm = stateMachine();
    assertThatNoException().isThrownBy(() -> sm.applyBootstrapFingerprintEntry(baseline(settled, localDb.getLastTransactionId()), 7L));

    assertThat(sm.getBootstrapInstallsInFlight()).isEmpty();
    assertThat(sm.getBootstrapUnreconciledDatabases()).isEmpty();
    assertThat(sm.bootstrapWindowReason())
        .as("a matching peer is serving the cluster's own copy and belongs in the Service")
        .isNull();
  }
}
