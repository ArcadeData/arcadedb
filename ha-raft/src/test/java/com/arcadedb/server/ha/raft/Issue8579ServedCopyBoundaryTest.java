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

import com.sun.net.httpserver.HttpServer;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.proto.RaftProtos.StateMachineLogEntryProto;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for issue #8579: #8577 gave only the Ratis-initiated notify-install a boundary on the database's
 * {@link ArcadeStateMachine.InstallApplyGate}. Every other install of a leader copy - the operator and #8490 targeted
 * resyncs, the full resync, the bootstrap-mismatch retry - released the gate with nothing recorded, so an entry
 * waiting behind it that the installed copy already carried was applied again to that copy.
 * <p>
 * The fix threads the applied index the leader reported serving the copy at ({@link SnapshotManager#APPLIED_INDEX_HEADER},
 * issue #8454) back out of the download to {@code ArcadeStateMachine.runUnderInstallGate}, which records it on the gate
 * before releasing it. An entry at or below it is not applied again, but - unlike the #8577 skip - still advances the
 * applied position, because such an install moves neither {@code lastAppliedIndex} nor the Ratis applied position.
 * <p>
 * Driven against a bare {@link ArcadeStateMachine} with a local HTTP server standing in for the leader, the way
 * {@code Issue8454SnapshotSourceAppliedIndexTest} drives the download. An empty-payload {@code TX_ENTRY} fails
 * {@code applyTxEntry} on its first read: an entry that completes with "OK" was therefore not applied, and one that
 * fails reached the apply path.
 */
class Issue8579ServedCopyBoundaryTest {

  private static final String DB = "chaos";

  private HttpServer httpServer;
  private int        port;

  @TempDir
  Path tempDir;

  @BeforeEach
  void startServer() throws IOException {
    httpServer = HttpServer.create(new InetSocketAddress(0), 0);
    port = httpServer.getAddress().getPort();
  }

  @AfterEach
  void stopServer() {
    ArcadeStateMachine.applyWaitsForInstallForTesting = null;
    if (httpServer != null)
      httpServer.stop(0);
  }

  @Test
  void anEntryTheServedCopyCarriesIsNotReappliedButAdvancesTheAppliedPosition() throws Exception {
    serveSnapshotsAt(40L);
    final ArcadeStateMachine sm = new ArcadeStateMachine();

    sm.runUnderInstallGate(DB, this::downloadFromLeader);

    final CompletableFuture<Message> carried = sm.applyTransaction(txEntry(sm, 3L, 30L));
    assertThat(carried.isCompletedExceptionally()).as("an entry the served copy carries must not be applied again")
        .isFalse();
    assertThat(carried.get().getContent().toStringUtf8()).isEqualTo("OK");
    assertThat(sm.readAppliedIndexCounter())
        .as("the entry is the next one in order: it must still advance the applied position").isEqualTo(30L);
    assertThat(sm.getLastAppliedTermIndex().getIndex()).isEqualTo(30L);

    final CompletableFuture<Message> atServedIndex = sm.applyTransaction(txEntry(sm, 3L, 40L));
    assertThat(atServedIndex.isCompletedExceptionally()).isFalse();
    assertThat(sm.readAppliedIndexCounter()).isEqualTo(40L);

    assertThat(sm.applyTransaction(txEntry(sm, 3L, 41L)).isCompletedExceptionally())
        .as("an entry past the served index is not in the copy and must reach the apply path").isTrue();
  }

  @Test
  void aSchemaEntryTheServedCopyCarriesIsNotReappliedEither() throws Exception {
    serveSnapshotsAt(40L);
    final ArcadeStateMachine sm = new ArcadeStateMachine();

    // Without the fix this entry reaches applySchemaEntry, which finds no open database in this server-less harness.
    assertThat(sm.applyTransaction(schemaEntry(sm, 25L)).isCompletedExceptionally())
        .as("sanity: a schema entry with no copy in place reaches the apply path and fails here").isTrue();

    sm.runUnderInstallGate(DB, this::downloadFromLeader);

    final CompletableFuture<Message> carried = sm.applyTransaction(schemaEntry(sm, 30L));
    assertThat(carried.isCompletedExceptionally())
        .as("a schema change the served copy carries must not be written over it again").isFalse();
    assertThat(sm.readAppliedIndexCounter()).isEqualTo(30L);
  }

  @Test
  @Timeout(60)
  void anEntryAlreadyWaitingOnTheGateWhenAResyncFinishesIsNotReapplied() throws Exception {
    serveSnapshotsAt(40L);
    final ArcadeStateMachine sm = new ArcadeStateMachine();

    final CountDownLatch installHoldsGate = new CountDownLatch(1);
    final CountDownLatch releaseInstall = new CountDownLatch(1);
    final CountDownLatch applyParked = new CountDownLatch(1);
    ArcadeStateMachine.applyWaitsForInstallForTesting = name -> {
      if (DB.equals(name))
        applyParked.countDown();
    };

    // A resync on another thread (the lifecycleExecutor, an HTTP worker) holds the gate for its whole install.
    final AtomicReference<Throwable> installFailure = new AtomicReference<>();
    final Thread installer = new Thread(() -> {
      try {
        sm.runUnderInstallGate(DB, () -> {
          installHoldsGate.countDown();
          try {
            releaseInstall.await();
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
          }
          downloadFromLeader();
        });
      } catch (final Throwable t) {
        installFailure.set(t);
      }
    }, "issue8579-resync");
    installer.start();
    assertThat(installHoldsGate.await(30, TimeUnit.SECONDS)).isTrue();

    final AtomicReference<CompletableFuture<Message>> result = new AtomicReference<>();
    final Thread applier = new Thread(() -> result.set(sm.applyTransaction(txEntry(sm, 3L, 30L))), "issue8579-apply");
    applier.start();
    assertThat(applyParked.await(30, TimeUnit.SECONDS)).as("the entry must be parked on the resync's gate").isTrue();

    releaseInstall.countDown();
    installer.join(30_000);
    applier.join(30_000);

    assertThat(installer.isAlive()).as("the resync must finish").isFalse();
    assertThat(applier.isAlive()).as("the parked entry must be released").isFalse();
    assertThat(installFailure.get()).isNull();
    assertThat(result.get().isCompletedExceptionally())
        .as("the parked entry the installed copy carries must not be applied to it once the gate is released").isFalse();
    assertThat(sm.readAppliedIndexCounter()).isEqualTo(30L);
  }

  @Test
  void anEntryTypeThatActsBeyondTheDatabaseFilesIsStillApplied() throws Exception {
    serveSnapshotsAt(40L);
    final ArcadeStateMachine sm = new ArcadeStateMachine();

    sm.runUnderInstallGate(DB, this::downloadFromLeader);

    // An INSTALL_DATABASE_ENTRY carries node-local bookkeeping a copy of the files does not: it must reach its apply
    // path (which fails in this server-less harness) even below the served index.
    assertThat(sm.applyTransaction(entry(sm, RaftLogEntryCodec.encodeInstallDatabaseEntry(DB, true), 30L, null))
        .isCompletedExceptionally()).as("only TX and schema entries may be left out").isTrue();
  }

  @Test
  void anEntryThisNodeOriginatedIsStillApplied() throws Exception {
    serveSnapshotsAt(40L);
    final ArcadeStateMachine sm = new ArcadeStateMachine();

    sm.runUnderInstallGate(DB, this::downloadFromLeader);

    assertThat(sm.applyTransaction(entry(sm, emptyTxPayload(), 30L, Boolean.TRUE)).isCompletedExceptionally())
        .as("a locally originated entry publishes a commit a caller waits on: it must reach the apply path").isTrue();
  }

  @Test
  void noEntryIsLeftOutWhileALocalCommitIsPending() throws Exception {
    serveSnapshotsAt(40L);
    final ArcadeStateMachine sm = new ArcadeStateMachine();

    sm.runUnderInstallGate(DB, this::downloadFromLeader);
    assertThat(sm.registerLocalCommit(new LocalCommit("other", 7L, null, null, new byte[] { 1 }))).isTrue();
    assertThat(sm.pendingLocalCommits()).isEqualTo(1);

    assertThat(sm.applyTransaction(txEntry(sm, 3L, 30L)).isCompletedExceptionally())
        .as("with a local commit waiting to be claimed, the entry must reach the apply path").isTrue();
  }

  @Test
  void aLeaderThatDoesNotReportItsAppliedIndexRecordsNoBoundary() throws Exception {
    serveSnapshotsAt(null);
    final ArcadeStateMachine sm = new ArcadeStateMachine();

    sm.runUnderInstallGate(DB, this::downloadFromLeader);

    assertThat(sm.applyTransaction(txEntry(sm, 3L, 30L)).isCompletedExceptionally())
        .as("with no reported index nothing is known to be in the copy: the entry is applied as before").isTrue();
  }

  @Test
  void aFailedInstallRecordsNoBoundary() {
    serveSnapshotsAt(40L);
    final ArcadeStateMachine sm = new ArcadeStateMachine();

    // The download succeeds, then the swap fails: the old copy stays, and the waiting entries still belong to it.
    assertThatThrownBy(() -> sm.runUnderInstallGate(DB, () -> {
      downloadFromLeader();
      throw new IOException("swap failed");
    })).isInstanceOf(IOException.class);

    assertThat(sm.applyTransaction(txEntry(sm, 3L, 30L)).isCompletedExceptionally())
        .as("a failed install replaced nothing, so the entry must go to the copy still in place").isTrue();
  }

  @Test
  void aLaterCopyServedAtALowerIndexReplacesTheBoundary() throws Exception {
    final AtomicInteger calls = new AtomicInteger();
    // A first leader serves the copy at 40; a later install gets a copy from a new leader that has applied only to 20.
    serveSnapshots(() -> calls.incrementAndGet() == 1 ? 40L : 20L);
    final ArcadeStateMachine sm = new ArcadeStateMachine();

    sm.runUnderInstallGate(DB, this::downloadFromLeader);
    sm.runUnderInstallGate(DB, this::downloadFromLeader);

    assertThat(sm.applyTransaction(txEntry(sm, 3L, 30L)).isCompletedExceptionally())
        .as("the copy on disk is the one served at 20: entry 30 is not in it and must be applied").isTrue();
  }

  @Test
  void theServedIndexIsReportedToTheInstallThatDownloadedIt() throws Exception {
    serveSnapshotsAt(40L);

    assertThat(SnapshotInstaller.runRequiringSourceAppliedIndex(-1L, () -> {
    })).as("an install that downloads nothing reports nothing").isEqualTo(-1L);

    final AtomicLong inner = new AtomicLong();
    final long outer = SnapshotInstaller.runRequiringSourceAppliedIndex(-1L,
        () -> inner.set(SnapshotInstaller.runRequiringSourceAppliedIndex(-1L, this::downloadFromLeader)));
    assertThat(inner.get()).isEqualTo(40L);
    assertThat(outer).as("a nested install reports to its own caller only").isEqualTo(-1L);

    assertThat(SnapshotInstaller.parseAppliedIndex(null)).isEqualTo(-1L);
    assertThat(SnapshotInstaller.parseAppliedIndex(" ")).isEqualTo(-1L);
    assertThat(SnapshotInstaller.parseAppliedIndex("lots")).isEqualTo(-1L);
    assertThat(SnapshotInstaller.parseAppliedIndex("-7")).isEqualTo(-1L);
    assertThat(SnapshotInstaller.parseAppliedIndex(" 12 ")).isEqualTo(12L);
  }

  private void downloadFromLeader() throws IOException {
    final Path snapshotDir = tempDir.resolve(".snapshot-" + System.nanoTime());
    Files.createDirectories(snapshotDir);
    SnapshotInstaller.downloadWithRetry(DB, snapshotDir, "localhost:" + port, null, 1, 10);
  }

  private void serveSnapshotsAt(final Long appliedIndex) {
    serveSnapshots(() -> appliedIndex);
  }

  private void serveSnapshots(final Supplier<Long> appliedIndex) {
    httpServer.createContext("/api/v1/ha/snapshot/" + DB, exchange -> {
      final Long applied = appliedIndex.get();
      final byte[] zip = zipWith("data.dat", "applied-" + applied);
      if (applied != null)
        exchange.getResponseHeaders().add(SnapshotManager.APPLIED_INDEX_HEADER, String.valueOf(applied));
      exchange.sendResponseHeaders(200, zip.length);
      exchange.getResponseBody().write(zip);
      exchange.close();
    });
    httpServer.start();
  }

  private static TransactionContext txEntry(final ArcadeStateMachine sm, final long term, final long index) {
    final LogEntryProto logEntry = LogEntryProto.newBuilder()
        .setTerm(term)
        .setIndex(index)
        .setStateMachineLogEntry(StateMachineLogEntryProto.newBuilder().setLogData(emptyTxPayload()).build())
        .build();
    return TransactionContext.newBuilder().setStateMachine(sm).setLogEntry(logEntry).build();
  }

  private static ByteString emptyTxPayload() {
    return RaftLogEntryCodec.encodeTxEntry(DB, new byte[0], Collections.emptyMap());
  }

  private static TransactionContext entry(final ArcadeStateMachine sm, final ByteString payload, final long index,
      final Object stateMachineContext) {
    final LogEntryProto logEntry = LogEntryProto.newBuilder()
        .setTerm(3L)
        .setIndex(index)
        .setStateMachineLogEntry(StateMachineLogEntryProto.newBuilder().setLogData(payload).build())
        .build();
    // Set on the built context: a builder given a log entry does not carry a state machine context over.
    final TransactionContext trx = TransactionContext.newBuilder().setStateMachine(sm).setLogEntry(logEntry).build();
    if (stateMachineContext != null)
      trx.setStateMachineContext(stateMachineContext);
    return trx;
  }

  private static TransactionContext schemaEntry(final ArcadeStateMachine sm, final long index) {
    final ByteString payload = RaftLogEntryCodec.encodeSchemaEntry(DB, "{}", Collections.emptyMap(), Collections.emptyMap(),
        Collections.emptyList(), Collections.emptyList());
    final LogEntryProto logEntry = LogEntryProto.newBuilder()
        .setTerm(3L)
        .setIndex(index)
        .setStateMachineLogEntry(StateMachineLogEntryProto.newBuilder().setLogData(payload).build())
        .build();
    return TransactionContext.newBuilder().setStateMachine(sm).setLogEntry(logEntry).build();
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
}
