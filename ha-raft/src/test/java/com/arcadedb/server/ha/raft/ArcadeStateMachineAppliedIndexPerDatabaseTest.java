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
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerDatabase;
import com.arcadedb.utility.FileUtils;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Regression tests for issue #4824.
 * <p>
 * {@link ArcadeStateMachine} multiplexes every database onto one Raft group but used to persist a
 * single global {@code .raft/applied-index} scalar. The replay-skip in
 * {@link ArcadeStateMachine#applyBootstrapFingerprintEntry} ("this database's bootstrap baseline was
 * already applied in a prior session, skip verification") consulted that GLOBAL value even though the
 * decision is per-database. A co-located database that advanced the global index past this database's
 * {@code BOOTSTRAP_FINGERPRINT_ENTRY} index would silently suppress the verification of a database
 * that was never bootstrapped - the "value that mixes databases" defect.
 * <p>
 * The fix makes applied-index tracking per-database-aware: the skip now consults this database's own
 * applied index and only skips when there is positive per-database evidence.
 */
class ArcadeStateMachineAppliedIndexPerDatabaseTest {

  private static final String DB_A     = "db-a";
  private static final String DB_OTHER = "db-other";

  @TempDir
  private Path        serverDir;
  private LocalDatabase localDbA;
  private String        dbAPath;

  @BeforeEach
  void setUp() {
    dbAPath = serverDir.resolve(DB_A).toString();
    localDbA = (LocalDatabase) new DatabaseFactory(dbAPath).create();
  }

  @AfterEach
  void tearDown() {
    if (localDbA != null && localDbA.isOpen())
      localDbA.close();
    FileUtils.deleteRecursively(new File(dbAPath));
  }

  private ArcadeStateMachine newStateMachine(final ArcadeDBServer server) {
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    sm.setServer(server);
    return sm;
  }

  private ArcadeDBServer mockServer() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, serverDir.toString());

    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.getConfiguration()).thenReturn(config);
    when(server.existsDatabase(DB_A)).thenReturn(true);
    when(server.getDatabase(DB_A)).thenReturn(new ServerDatabase(null, localDbA));
    return server;
  }

  /**
   * Regression: another database advancing the GLOBAL applied index past this database's bootstrap
   * entry index must NOT skip this database's bootstrap verification. Before the fix the skip fired
   * (global 100 &gt;= 50) and {@code applyBootstrapFingerprintEntry} returned before ever consulting
   * the local database, so {@code server.getDatabase(DB_A)} was never called.
   */
  @Test
  void bootstrapSkipNotTriggeredByAnotherDatabasesAppliedIndex() throws Exception {
    final ArcadeDBServer server = mockServer();
    final ArcadeStateMachine sm = newStateMachine(server);

    // A co-located database advanced the shared Raft log far ahead of db-a's bootstrap entry.
    sm.writePersistedAppliedIndex(100L, DB_OTHER);

    // Build a MATCHING bootstrap entry for db-a so the verification path returns cleanly once reached
    // (no snapshot install). The discriminator is purely "was the local database consulted?".
    final String realFingerprint = BootstrapFingerprint.compute(new File(dbAPath));
    final long realLastTxId = localDbA.getLastTransactionId();
    final ByteString encoded = RaftLogEntryCodec.encodeBootstrapFingerprintEntry(DB_A, realFingerprint, realLastTxId);
    final RaftLogEntryCodec.DecodedEntry decoded = RaftLogEntryCodec.decode(encoded);

    // db-a's own bootstrap entry sits at index 50, below the global 100.
    sm.applyBootstrapFingerprintEntry(decoded, 50L);

    // Verification must have run for db-a (its local state was read), proving the global index of a
    // different database did not suppress it.
    verify(server, atLeastOnce()).getDatabase(DB_A);

    // The baseline is recorded regardless of the skip decision.
    assertThat(sm.getBootstrapBaseline(DB_A)).isNotNull();
    assertThat(sm.getBootstrapBaseline(DB_A).fingerprint()).isEqualTo(realFingerprint);
  }

  /**
   * A database's OWN per-database applied index at or beyond the bootstrap entry index still skips its
   * verification (the legitimate replay-skip on restart is preserved). When skipped,
   * {@code applyBootstrapFingerprintEntry} returns before consulting the local database.
   */
  @Test
  void bootstrapSkipHonorsPerDatabaseAppliedIndex() throws Exception {
    final ArcadeDBServer server = mockServer();
    final ArcadeStateMachine sm = newStateMachine(server);

    // Positive per-database evidence that db-a was already applied past its bootstrap entry index.
    sm.writePersistedAppliedIndex(50L, DB_A);

    final ByteString encoded = RaftLogEntryCodec.encodeBootstrapFingerprintEntry(DB_A, "0".repeat(64), Long.MAX_VALUE);
    final RaftLogEntryCodec.DecodedEntry decoded = RaftLogEntryCodec.decode(encoded);

    sm.applyBootstrapFingerprintEntry(decoded, 50L);

    // Skipped: the local database was never consulted.
    verify(server, never()).getDatabase(DB_A);
    // Baseline is still recorded before the skip check.
    assertThat(sm.getBootstrapBaseline(DB_A)).isNotNull();
  }

  /**
   * A legacy plain-number {@code .raft/applied-index} file (written by an older version) is honoured
   * as the GLOBAL value but yields NO per-database evidence: a per-database read returns -1, so it can
   * never falsely skip a per-database decision.
   */
  @Test
  void legacyPlainNumberFileReadAsGlobalNotPerDatabase() throws Exception {
    final ArcadeDBServer server = mockServer();
    final ArcadeStateMachine sm = newStateMachine(server);

    final Path raftDir = serverDir.resolve(".raft");
    Files.createDirectories(raftDir);
    Files.writeString(raftDir.resolve("applied-index"), "77");

    assertThat(sm.readPersistedAppliedIndex())
        .as("legacy plain-number file is honoured as the global applied index")
        .isEqualTo(77L);
    assertThat(sm.readPersistedAppliedIndex(DB_A))
        .as("legacy file carries no per-database evidence")
        .isEqualTo(-1L);
  }

  /**
   * Per-database and global values round-trip through the persisted file and are recovered by a fresh
   * state machine instance (simulating a process restart).
   */
  @Test
  void perDatabaseValuesRoundTripAcrossRestart() {
    final ArcadeDBServer server = mockServer();

    final ArcadeStateMachine writer = newStateMachine(server);
    writer.writePersistedAppliedIndex(10L, DB_A);
    writer.writePersistedAppliedIndex(20L, DB_OTHER);

    final ArcadeStateMachine reopened = newStateMachine(server);
    assertThat(reopened.readPersistedAppliedIndex(DB_A)).isEqualTo(10L);
    assertThat(reopened.readPersistedAppliedIndex(DB_OTHER)).isEqualTo(20L);
    assertThat(reopened.readPersistedAppliedIndex())
        .as("global tracks the last applied index across all databases")
        .isEqualTo(20L);
    assertThat(reopened.readPersistedAppliedIndex("never-seen")).isEqualTo(-1L);
  }
}
