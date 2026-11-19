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
import com.arcadedb.database.Database;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.utility.FileUtils;
import com.sun.net.httpserver.HttpServer;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.proto.RaftProtos.StateMachineLogEntryProto;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #11919: a node restarting after a database was dropped and created again under the same
 * name replays its log from the last snapshot marker, and when the marker is older than the drop it re-applied the
 * entries of the dropped incarnation to the recreated copy. The old create was skipped (the name exists), the entries
 * after it were applied to the new files - a TIMESERIES type then met the sealed-store owner of the old type - and the
 * old drop renamed the recreated copy away, to be rebuilt from the log by the later create.
 * <p>
 * The harness drives a real (unstarted) {@link ArcadeDBServer} through {@link ArcadeStateMachine#applyTransaction}, the
 * path a replay takes, and models the restart with a second state machine over the same server and the same
 * {@code .raft/applied-index} file: Ratis hands it the entries again from the marker on.
 */
class Issue11919ReplayOfDroppedIncarnationTest {
  private static final String DB          = "db-incarnations";
  private static final long   TERM        = 3L;
  private static final long   FIRST       = 10L;
  private static final long   OLD_TX      = 11L;
  private static final long   DROP        = 12L;
  private static final long   SECOND      = 13L;
  private static final String CURRENT     = "CreatedAfterTheSecondCreate";

  @TempDir
  private Path            serverDir;
  private ArcadeDBServer  server;
  private ExecutorService executor;
  private DeferredDatabaseDeleter deleter;

  @BeforeEach
  void setUp() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, serverDir.toString());
    server = new ArcadeDBServer(config);
    executor = Executors.newSingleThreadExecutor();
    deleter = new DeferredDatabaseDeleter(executor);
  }

  @AfterEach
  void tearDown() {
    deleter.close();
    executor.shutdownNow();
    for (final String name : new String[] { DB, OTHER })
      if (server.existsDatabase(name))
        server.getDatabase(name).getEmbedded().close();
    FileUtils.deleteRecursively(new File(serverDir.toString()));
  }

  /**
   * The history the cluster committed and this node applied: create, a transaction of that first incarnation, drop,
   * create again. The current copy then holds a type of its own, standing for everything the entries after the second
   * create put in it.
   */
  private ArcadeStateMachine applyHistory() {
    final ArcadeStateMachine sm = newStateMachine();
    apply(sm, RaftLogEntryCodec.encodeInstallDatabaseEntry(DB), FIRST);
    // The transaction of the first incarnation is not applied by this node: an empty WAL fails the apply, which is how
    // the test tells, on the replay below, whether an entry of that incarnation reached the copy
    apply(sm, RaftLogEntryCodec.encodeDropDatabaseEntry(DB), DROP);
    apply(sm, RaftLogEntryCodec.encodeInstallDatabaseEntry(DB), SECOND);
    assertThat(server.existsDatabase(DB)).as("the second create made the current copy").isTrue();

    final Database current = server.getDatabase(DB);
    current.transaction(() -> current.getSchema().createDocumentType(CURRENT));
    return sm;
  }

  /** The restart: a fresh state machine fed the log again from a snapshot marker below the first create. */
  private ArcadeStateMachine replayFromBeforeTheDrop() {
    final ArcadeStateMachine restarted = newStateMachine();
    apply(restarted, RaftLogEntryCodec.encodeInstallDatabaseEntry(DB), FIRST);
    apply(restarted, RaftLogEntryCodec.encodeTxEntry(DB, new byte[0], Collections.emptyMap()), OLD_TX);
    apply(restarted, RaftLogEntryCodec.encodeDropDatabaseEntry(DB), DROP);
    apply(restarted, RaftLogEntryCodec.encodeInstallDatabaseEntry(DB), SECOND);
    return restarted;
  }

  @Test
  void theReplayedDropOfTheFirstIncarnationLeavesTheRecreatedCopyInPlace() {
    applyHistory();

    replayFromBeforeTheDrop();

    assertThat(server.existsDatabase(DB)).isTrue();
    assertThat(server.getDatabase(DB).getSchema().existsType(CURRENT))
        .as("the drop at index %d belongs to the incarnation the create at index %d replaced: replayed, it renamed the "
            + "current copy away and the replayed create made an empty one", DROP, SECOND).isTrue();
  }

  @Test
  void anEntryOfTheFirstIncarnationIsNotAppliedToTheRecreatedCopy() {
    applyHistory();

    final ArcadeStateMachine restarted = newStateMachine();
    apply(restarted, RaftLogEntryCodec.encodeInstallDatabaseEntry(DB), FIRST);
    apply(restarted, RaftLogEntryCodec.encodeTxEntry(DB, new byte[0], Collections.emptyMap()), OLD_TX);

    // Read before the replayed drop, which clears a quarantine along with the database it drops
    assertThat(restarted.isDatabaseDiverged(DB))
        .as("the transaction at index %d belongs to the dropped incarnation and must not reach the copy created at index %d",
            OLD_TX, SECOND).isFalse();
    assertThat(restarted.readAppliedIndexCounter()).as("a skipped entry still advances the applied position")
        .isEqualTo(OLD_TX);
  }

  @Test
  void aDatabaseDroppedForGoodIsStillDroppedByTheReplay() {
    final ArcadeStateMachine sm = newStateMachine();
    apply(sm, RaftLogEntryCodec.encodeInstallDatabaseEntry(DB), FIRST);
    apply(sm, RaftLogEntryCodec.encodeDropDatabaseEntry(DB), DROP);
    assertThat(server.existsDatabase(DB)).isFalse();

    final ArcadeStateMachine restarted = newStateMachine();
    apply(restarted, RaftLogEntryCodec.encodeInstallDatabaseEntry(DB), FIRST);
    assertThat(server.existsDatabase(DB)).as("the replayed create of a database with no later create runs").isTrue();
    apply(restarted, RaftLogEntryCodec.encodeDropDatabaseEntry(DB), DROP);

    assertThat(server.existsDatabase(DB)).as("the replayed drop of the same incarnation still drops it").isFalse();
  }

  // A database created after the history, by an entry whose applied-index write is not fsynced
  private static final String OTHER = "db-other";

  @Test
  void theCreationBoundarySurvivesAPowerLossOfTheAppliedIndexFile() throws Exception {
    final ArcadeStateMachine sm = applyHistory();
    // A later entry rewrites the applied-index file without an fsync (issue #11517): a power loss can leave it empty
    apply(sm, RaftLogEntryCodec.encodeInstallDatabaseEntry(OTHER), SECOND + 1);
    Files.writeString(raftDir().resolve("applied-index"), "");

    replayFromBeforeTheDrop();

    assertThat(server.getDatabase(DB).getSchema().existsType(CURRENT))
        .as("the boundary recorded at index %d must not depend on the unsynced per-entry write", SECOND).isTrue();
  }

  @Test
  void aCreationBoundaryThatCouldNotBeWrittenIsWrittenByTheNextEntry() throws Exception {
    // Nothing can be written over a non-empty directory: the write of the boundary at the second create fails
    final Path blocker = raftDir().resolve("created-index");
    Files.createDirectories(blocker);
    Files.writeString(blocker.resolve("x"), "x");
    final ArcadeStateMachine sm = applyHistory();

    FileUtils.deleteRecursively(blocker.toFile());
    // An entry that records no boundary of its own: only the retry writes the one that failed
    apply(sm, RaftLogEntryCodec.encodeDropDatabaseEntry("db-never-created"), SECOND + 1);
    Files.writeString(raftDir().resolve("applied-index"), "");

    replayFromBeforeTheDrop();

    assertThat(server.getDatabase(DB).getSchema().existsType(CURRENT))
        .as("the boundary whose write failed at index %d is written again once the volume takes writes", SECOND).isTrue();
  }

  @Test
  void theDropOfAnEarlierIncarnationLeavesACopyAcquiredFromTheLeader() throws Exception {
    // The leader served the copy at index 20, after the drop at index 12 and the create at index 13 of the incarnation
    // the copy holds; this node, a follower installing a Raft snapshot, still applies the entries above its install boundary
    final HttpServer leader = leaderServingTheCopyAt(20);
    final ArcadeStateMachine sm = newStateMachine();
    try {
      sm.runUnderInstallGate(DB, FIRST - 1, () -> acquireTheCopy(leader));
    } finally {
      leader.stop(0);
    }

    apply(sm, RaftLogEntryCodec.encodeInstallDatabaseEntry(DB), FIRST);
    apply(sm, RaftLogEntryCodec.encodeDropDatabaseEntry(DB), DROP);
    apply(sm, RaftLogEntryCodec.encodeInstallDatabaseEntry(DB), SECOND);

    assertThat(server.existsDatabase(DB)).isTrue();
    assertThat(server.getDatabase(DB).getSchema().existsType(CURRENT))
        .as("the drop at index %d is of an incarnation the copy served at index 20 already replaced", DROP).isTrue();
  }

  @Test
  void aDropWaitingForTheAcquisitionReadsTheBoundaryItRecords() throws Exception {
    // The drop of the earlier incarnation arrives while the copy is being acquired: it waits on the install gate, and must
    // read the boundary the acquisition records before it releases the gate, not the one it would have read before waiting
    final HttpServer leader = leaderServingTheCopyAt(20);
    final ArcadeStateMachine sm = newStateMachine();
    final CountDownLatch installing = new CountDownLatch(1);
    final CountDownLatch dropWaiting = new CountDownLatch(1);
    final ExecutorService threads = Executors.newFixedThreadPool(2);
    ArcadeStateMachine.applyWaitsForInstallForTesting = name -> dropWaiting.countDown();
    try {
      final Future<?> install = threads.submit(() -> {
        sm.runUnderInstallGate(DB, FIRST - 1, () -> {
          installing.countDown();
          awaitLatch(dropWaiting);
          acquireTheCopy(leader);
        });
        return null;
      });
      assertThat(installing.await(30, TimeUnit.SECONDS)).isTrue();
      final Future<?> drop = threads.submit(() -> apply(sm, RaftLogEntryCodec.encodeDropDatabaseEntry(DB), DROP));
      install.get(60, TimeUnit.SECONDS);
      drop.get(60, TimeUnit.SECONDS);
    } finally {
      ArcadeStateMachine.applyWaitsForInstallForTesting = null;
      threads.shutdownNow();
      leader.stop(0);
    }

    assertThat(server.existsDatabase(DB)).isTrue();
    assertThat(server.getDatabase(DB).getSchema().existsType(CURRENT))
        .as("the drop at index %d waited for the copy served at index 20, which already replaced its incarnation", DROP).isTrue();
  }

  /** A leader that serves a copy of {@link #DB} it reports as of applied index {@code servedAt}. */
  private static HttpServer leaderServingTheCopyAt(final long servedAt) throws IOException {
    final HttpServer leader = HttpServer.create(new InetSocketAddress(0), 0);
    leader.createContext("/api/v1/ha/snapshot/" + DB, exchange -> {
      final byte[] zip = zipWith("data.dat", "copy");
      exchange.getResponseHeaders().add(SnapshotManager.APPLIED_INDEX_HEADER, Long.toString(servedAt));
      exchange.sendResponseHeaders(200, zip.length);
      exchange.getResponseBody().write(zip);
      exchange.close();
    });
    leader.start();
    return leader;
  }

  /** The install: downloads the leader's copy and stands in for it with a database holding {@link #CURRENT}. */
  private void acquireTheCopy(final HttpServer leader) throws IOException {
    final Path staging = serverDir.resolve(".staging");
    Files.createDirectories(staging);
    SnapshotInstaller.downloadWithRetry(DB, staging, "localhost:" + leader.getAddress().getPort(), null, 1, 10);
    final Database acquired = server.createDatabase(DB, ComponentFile.MODE.READ_WRITE);
    acquired.transaction(() -> acquired.getSchema().createDocumentType(CURRENT));
  }

  private static void awaitLatch(final CountDownLatch latch) throws IOException {
    try {
      if (!latch.await(30, TimeUnit.SECONDS))
        throw new IOException("timed out waiting for the drop to wait on the install gate");
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException(e);
    }
  }

  private Path raftDir() throws IOException {
    return Files.createDirectories(serverDir.resolve(".raft"));
  }

  private static byte[] zipWith(final String name, final String content) throws IOException {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (final ZipOutputStream zip = new ZipOutputStream(baos)) {
      zip.putNextEntry(new ZipEntry(name));
      zip.write(content.getBytes(StandardCharsets.UTF_8));
      zip.closeEntry();
    }
    return baos.toByteArray();
  }

  private ArcadeStateMachine newStateMachine() {
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    sm.setServer(server);
    sm.setDeferredDatabaseDeleter(deleter);
    return sm;
  }

  private static void apply(final ArcadeStateMachine sm, final ByteString payload, final long index) {
    final LogEntryProto logEntry = LogEntryProto.newBuilder().setTerm(TERM).setIndex(index)
        .setStateMachineLogEntry(StateMachineLogEntryProto.newBuilder().setLogData(payload).build()).build();
    // A failed apply (the empty WAL above) is the outcome some assertions look at, not a test failure
    sm.applyTransaction(TransactionContext.newBuilder().setStateMachine(sm).setLogEntry(logEntry).build())
        .exceptionally(t -> null).join();
  }
}
