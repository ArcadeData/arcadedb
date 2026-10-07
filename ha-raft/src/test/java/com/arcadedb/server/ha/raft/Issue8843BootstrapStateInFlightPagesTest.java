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
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerDatabase;
import com.arcadedb.utility.FileUtils;
import org.apache.ratis.protocol.RaftPeerId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Regression tests for issue #8843: every production reader of a node's own bootstrap state fingerprinted the OPEN
 * database straight off the disk, so a commit whose pages were still queued in the asynchronous page flush made two
 * samples of one unchanged copy differ - a peer whose copy IS the baseline could take the mismatch arm and reinstall it.
 * <p>
 * One test per reader, each driving its own entry point while the last commit's pages are held in the pipeline:
 * <ul>
 *   <li>{@link ArcadeStateMachine#readLocalBootstrapState} - the peer verification of a committed baseline and the
 *   periodic divergence re-check;</li>
 *   <li>{@link BootstrapElection#computeLocalStates} - the source's own sample during the election;</li>
 *   <li>{@link PostBootstrapStateHandler#localDatabaseStates} - the {@code bootstrap-state} answer to a peer.</li>
 * </ul>
 * The hold is {@code PageManager.suspendFlushAndExecute} on a background thread, released only once the reader is
 * parked waiting for the flush, so the reader can only answer with the settled copy if it really waited.
 */
class Issue8843BootstrapStateInFlightPagesTest {

  private static final String DB_DIR  = "./target/databases";
  private static final String DB_NAME = "test-8843-in-flight-pages";
  private static final String DB_PATH = DB_DIR + "/" + DB_NAME;

  private LocalDatabase   localDb;
  private ExecutorService executor;

  @BeforeEach
  void setUp() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    localDb = (LocalDatabase) new DatabaseFactory(DB_PATH).create();
    localDb.getSchema().createDocumentType("Seed");
    executor = Executors.newFixedThreadPool(2);
  }

  @AfterEach
  void tearDown() throws InterruptedException {
    executor.shutdownNow();
    executor.awaitTermination(30, TimeUnit.SECONDS);
    if (localDb != null && localDb.isOpen())
      localDb.close();
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  private ArcadeDBServer stubbedServer() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, DB_DIR);
    config.setValue(GlobalConfiguration.NETWORK_USE_SSL, false);

    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.getConfiguration()).thenReturn(config);
    when(server.getDatabaseNames()).thenReturn(Set.of(DB_NAME));
    when(server.existsDatabase(DB_NAME)).thenReturn(true);
    when(server.getDatabase(DB_NAME)).thenReturn(new ServerDatabase(null, localDb));
    return server;
  }

  @Test
  void theStateMachineVerifiesTheSettledCopy() throws Exception {
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    sm.setServer(stubbedServer());

    final Sample sample = sampleWhileTheLastCommitIsInFlight(() -> sm.readLocalBootstrapState(DB_NAME).fingerprint());

    assertThat(sample.whileInFlight).isEqualTo(sample.settled);
    assertThat(sample.rawWhileInFlight)
        .as("control: the raw files did not hold the commit while the pages were held")
        .isNotEqualTo(sample.settled);
  }

  @Test
  void theElectionSamplesTheSettledCopy() throws Exception {
    final BootstrapElection election = new BootstrapElection(mock(RaftHAServer.class), stubbedServer());

    final Sample sample = sampleWhileTheLastCommitIsInFlight(() -> {
      final Map<String, BootstrapElection.PeerState> states = election.computeLocalStates(RaftPeerId.valueOf("peer-0"),
          Set.of(DB_NAME));
      return states.get(DB_NAME).fingerprint();
    });

    assertThat(sample.whileInFlight).isEqualTo(sample.settled);
    assertThat(sample.rawWhileInFlight).isNotEqualTo(sample.settled);
  }

  @Test
  void theBootstrapStateAnswerCarriesTheSettledCopy() throws Exception {
    final ArcadeDBServer server = stubbedServer();

    final Sample sample = sampleWhileTheLastCommitIsInFlight(() -> {
      final JSONArray dbs = PostBootstrapStateHandler.localDatabaseStates(server);
      assertThat(dbs.length()).isEqualTo(1);
      final JSONObject db = dbs.getJSONObject(0);
      assertThat(db.getString("name")).isEqualTo(DB_NAME);
      assertThat(db.getLong("lastTxId")).isEqualTo(localDb.getLastTransactionId());
      return db.getString("fingerprint");
    });

    assertThat(sample.whileInFlight).isEqualTo(sample.settled);
    assertThat(sample.rawWhileInFlight).isNotEqualTo(sample.settled);
  }

  /** A sweep over several databases shares one settle deadline: what is left of it, never negative once spent. */
  @Test
  void aSweepSharesOneSettleDeadline() {
    assertThat(ArcadeStateMachine.settleBudget(System.currentTimeMillis() - 1_000L)).isZero();
    assertThat(ArcadeStateMachine.settleBudget(System.currentTimeMillis() + 60_000L)).isBetween(1L, 60_000L);
  }

  /**
   * A spent sweep budget does not wait at all: with the last commit held in the pipeline the reader answers at once,
   * from the files as they stand, instead of charging the next database another full bound.
   */
  @Test
  void aSpentSweepBudgetHashesWithoutWaiting() throws Exception {
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    sm.setServer(stubbedServer());

    final Sample sample = sampleWhileTheLastCommitIsInFlight(
        () -> sm.readLocalBootstrapState(DB_NAME, ArcadeStateMachine.settleBudget(0L)).fingerprint());

    assertThat(sample.whileInFlight).isEqualTo(sample.rawWhileInFlight);
  }

  private record Sample(String whileInFlight, String rawWhileInFlight, String settled) {
  }

  /**
   * Commits one transaction with the flush of the database held, asks {@code reader} for the fingerprint while the
   * commit's pages are still in the pipeline, and releases the hold only once the reader is parked (or has already
   * answered, which is the bug). Returns what the reader answered, what a raw compute read while the pages were held,
   * and the fingerprint of the copy once everything landed.
   */
  private Sample sampleWhileTheLastCommitIsInFlight(final Callable<String> reader) throws Exception {
    assertThat(localDb.getPageManager().waitAllPagesOfDatabaseAreFlushed(localDb)).isTrue();

    final CountDownLatch committed = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    final AtomicReference<String> rawInFlight = new AtomicReference<>();
    final Future<?> hold = executor.submit(() -> {
      localDb.getPageManager().suspendFlushAndExecute(localDb, () -> {
        localDb.transaction(() -> localDb.newDocument("Seed").set("k", 1).save());
        rawInFlight.set(BootstrapFingerprint.compute(new File(DB_PATH)));
        committed.countDown();
        release.await(60, TimeUnit.SECONDS);
      });
      return null;
    });
    try {
      assertThat(committed.await(60, TimeUnit.SECONDS)).isTrue();

      final AtomicReference<Thread> readerThread = new AtomicReference<>();
      final Future<String> answered = executor.submit(() -> {
        readerThread.set(Thread.currentThread());
        return reader.call();
      });

      final long deadline = System.currentTimeMillis() + 60_000L;
      boolean parked = false;
      while (!parked && !answered.isDone() && System.currentTimeMillis() < deadline) {
        final Thread t = readerThread.get();
        parked = t != null && (t.getState() == Thread.State.TIMED_WAITING || t.getState() == Thread.State.WAITING);
        if (!parked)
          Thread.sleep(5);
      }
      if (!parked && !answered.isDone())
        fail("the reader neither parked on the flush drain nor answered within 60 s");
      release.countDown();
      hold.get(60, TimeUnit.SECONDS);

      final String whileInFlight = answered.get(60, TimeUnit.SECONDS);
      return new Sample(whileInFlight, rawInFlight.get(), BootstrapFingerprint.computeSettled(localDb));
    } finally {
      release.countDown();
    }
  }
}
