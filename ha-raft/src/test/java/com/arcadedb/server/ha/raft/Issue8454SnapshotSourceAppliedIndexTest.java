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
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for issue #8454: a snapshot install refuses a leader copy served at an applied index below the entries
 * this node already applied to the copy it is replacing.
 * <p>
 * Three pieces are covered here without a cluster: the floor the install lock derives (including the entry the apply
 * thread applied just before releasing the lock, which {@code lastAppliedIndex} does not yet show), the refusal on the
 * download path with a retry that accepts a caught-up copy, and the leader side's "no Raft, nothing to report".
 * {@code Issue8454SnapshotSourceBehindFollowerIT} drives the whole path on a real cluster.
 */
class Issue8454SnapshotSourceAppliedIndexTest {

  private HttpServer httpServer;
  private int        port;

  @BeforeEach
  void startServer() throws IOException {
    httpServer = HttpServer.create(new InetSocketAddress(0), 0);
    port = httpServer.getAddress().getPort();
  }

  @AfterEach
  void stopServer() {
    SnapshotInstaller.sourceBehindForTesting = null;
    if (httpServer != null)
      httpServer.stop(0);
  }

  @Test
  void aCopyBehindTheFloorIsRefusedAndOneAtOrPastItAccepted() {
    assertThatThrownBy(() -> SnapshotInstaller.checkSourceAppliedIndex("db", "4", 5L))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("applied index 4")
        .hasMessageContaining("behind index 5");
    assertThatThrownBy(() -> SnapshotInstaller.checkSourceAppliedIndex("db", "-1", 0L)).isInstanceOf(IOException.class);
    assertThatCode(() -> SnapshotInstaller.checkSourceAppliedIndex("db", "5", 5L)).doesNotThrowAnyException();
    assertThatCode(() -> SnapshotInstaller.checkSourceAppliedIndex("db", " 9 ", 5L)).doesNotThrowAnyException();
  }

  @Test
  void noFloorOrNoIndexFromAnOlderLeaderIsAccepted() {
    // No install lock, no floor: exactly the behavior before the fix.
    assertThatCode(() -> SnapshotInstaller.checkSourceAppliedIndex("db", "0", -1L)).doesNotThrowAnyException();
    // A leader predating #8454 during a rolling upgrade: refusing it would fail every install until the upgrade ends.
    assertThatCode(() -> SnapshotInstaller.checkSourceAppliedIndex("db", null, 5L)).doesNotThrowAnyException();
    assertThatCode(() -> SnapshotInstaller.checkSourceAppliedIndex("db", "", 5L)).doesNotThrowAnyException();
    assertThatCode(() -> SnapshotInstaller.checkSourceAppliedIndex("db", "lots", 5L)).doesNotThrowAnyException();
  }

  @Test
  void theFloorIsScopedToTheInstallAndRestoredWhenNested() throws Exception {
    assertThat(SnapshotInstaller.requiredSourceAppliedIndex()).isEqualTo(-1L);
    final AtomicLong outer = new AtomicLong();
    final AtomicLong inner = new AtomicLong();
    final AtomicLong outerAfterInner = new AtomicLong();
    SnapshotInstaller.runRequiringSourceAppliedIndex(7L, () -> {
      outer.set(SnapshotInstaller.requiredSourceAppliedIndex());
      SnapshotInstaller.runRequiringSourceAppliedIndex(3L, () -> inner.set(SnapshotInstaller.requiredSourceAppliedIndex()));
      outerAfterInner.set(SnapshotInstaller.requiredSourceAppliedIndex());
    });
    assertThat(outer.get()).isEqualTo(7L);
    assertThat(inner.get()).isEqualTo(3L);
    assertThat(outerAfterInner.get()).isEqualTo(7L);
    assertThat(SnapshotInstaller.requiredSourceAppliedIndex()).isEqualTo(-1L);
  }

  @Test
  void downloadRetriesUntilTheLeaderHasAppliedWhatThisNodeApplied(@TempDir final Path tempDir) throws Exception {
    final AtomicInteger calls = new AtomicInteger();
    httpServer.createContext("/api/v1/ha/snapshot/testdb", exchange -> {
      // The first copy is served before the leader's apply thread reached index 5; the second after.
      final long applied = calls.incrementAndGet() == 1 ? 4L : 5L;
      final byte[] zip = zipWith("data.dat", "applied-" + applied);
      exchange.getResponseHeaders().add(SnapshotManager.APPLIED_INDEX_HEADER, String.valueOf(applied));
      exchange.sendResponseHeaders(200, zip.length);
      exchange.getResponseBody().write(zip);
      exchange.close();
    });
    httpServer.start();

    final AtomicInteger refusals = new AtomicInteger();
    SnapshotInstaller.sourceBehindForTesting = name -> refusals.incrementAndGet();
    final Path snapshotDir = tempDir.resolve(".snapshot-new");
    Files.createDirectories(snapshotDir);

    SnapshotInstaller.runRequiringSourceAppliedIndex(5L,
        () -> SnapshotInstaller.downloadWithRetry("testdb", snapshotDir, "localhost:" + port, null, 2, 10));

    assertThat(calls.get()).isEqualTo(2);
    assertThat(refusals.get()).isEqualTo(1);
    assertThat(Files.readString(snapshotDir.resolve("data.dat"))).as("the installed copy is the caught-up one")
        .isEqualTo("applied-5");
  }

  @Test
  void aLeaderThatNeverCatchesUpFailsTheDownloadAndExtractsNothing(@TempDir final Path tempDir) throws Exception {
    final AtomicInteger calls = new AtomicInteger();
    httpServer.createContext("/api/v1/ha/snapshot/testdb", exchange -> {
      calls.incrementAndGet();
      final byte[] zip = zipWith("data.dat", "stale");
      exchange.getResponseHeaders().add(SnapshotManager.APPLIED_INDEX_HEADER, "4");
      exchange.sendResponseHeaders(200, zip.length);
      exchange.getResponseBody().write(zip);
      exchange.close();
    });
    httpServer.start();

    final Path snapshotDir = tempDir.resolve(".snapshot-new");
    Files.createDirectories(snapshotDir);

    assertThatThrownBy(() -> SnapshotInstaller.runRequiringSourceAppliedIndex(5L,
        () -> SnapshotInstaller.downloadWithRetry("testdb", snapshotDir, "localhost:" + port, null, 1, 10)))
        .isInstanceOf(IOException.class)
        .rootCause().hasMessageContaining("behind index 5");
    assertThat(calls.get()).isEqualTo(2);
    assertThat(snapshotDir.resolve("data.dat")).as("a copy behind the floor is never extracted").doesNotExist();
  }

  @Test
  void theInstallLockFloorCoversTheEntryAppliedJustBeforeItWasReleased() throws Exception {
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    final AtomicLong untouched = new AtomicLong();
    sm.runUnderInstallGate("db-A", () -> untouched.set(SnapshotInstaller.requiredSourceAppliedIndex()));
    assertThat(untouched.get()).as("a node that applied nothing has no floor").isEqualTo(-1L);

    // The apply thread takes db-A's install lock for entry 5 and releases it. The entry fails in this server-less
    // harness (it quarantines db-A), so lastAppliedIndex never moves to 5 - the same position an install sees when it
    // takes the lock in the gap between the apply thread releasing it and advancing lastAppliedIndex.
    sm.applyTransaction(txEntryForDatabase(sm, "db-A", 1L, 5L)).exceptionally(t -> null);
    assertThat(sm.readAppliedIndexCounter()).isLessThan(5L);

    final AtomicLong floorA = new AtomicLong();
    sm.runUnderInstallGate("db-A", () -> floorA.set(SnapshotInstaller.requiredSourceAppliedIndex()));
    assertThat(floorA.get()).as("the floor covers the entry that went to db-A's copy under the lock").isEqualTo(5L);

    final AtomicLong floorB = new AtomicLong();
    sm.runUnderInstallGate("db-B", () -> floorB.set(SnapshotInstaller.requiredSourceAppliedIndex()));
    assertThat(floorB.get()).as("an entry for db-A does not raise db-B's floor above the node's applied index")
        .isEqualTo(sm.readAppliedIndexCounter());
    assertThat(SnapshotInstaller.requiredSourceAppliedIndex()).as("the floor does not outlive the install").isEqualTo(-1L);
  }

  @Test
  void aNodeWithoutRaftReportsNoAppliedIndex() {
    assertThat(SnapshotHttpHandler.servedAppliedIndex(null)).isEqualTo(Long.MIN_VALUE);
  }

  private static TransactionContext txEntryForDatabase(final ArcadeStateMachine sm, final String databaseName,
      final long term, final long index) {
    // An empty payload fails applyTxEntry on its first read, without needing a server (see
    // ArcadeStateMachinePerDatabaseHaltTest).
    final ByteString payload = RaftLogEntryCodec.encodeTxEntry(databaseName, new byte[0], Collections.emptyMap());
    final LogEntryProto logEntry = LogEntryProto.newBuilder()
        .setTerm(term)
        .setIndex(index)
        .setStateMachineLogEntry(StateMachineLogEntryProto.newBuilder().setLogData(payload).build())
        .build();
    return TransactionContext.newBuilder().setStateMachine(sm).setLogEntry(logEntry).build();
  }

  private static byte[] zipWith(final String name, final String content) throws IOException {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (final ZipOutputStream zip = new ZipOutputStream(baos)) {
      zip.putNextEntry(new ZipEntry(name));
      zip.write(content.getBytes());
      zip.closeEntry();
    }
    return baos.toByteArray();
  }
}
